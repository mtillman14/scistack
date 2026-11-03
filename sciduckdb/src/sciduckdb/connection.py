"""The locked DuckDB connection every SciStack layer shares.

``DuckConnection`` serialises every statement under one lock, groups paired
work with the re-entrant ``transaction()`` context manager, and offers the
locked execute/fetch and vectorized bulk methods listed in the README. It
knows nothing about which tables exist.
"""

import logging
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger("sciduck")


class TransactionRolledBackError(RuntimeError):
    """The outermost ``_commit`` found that an inner level had rolled back.

    DuckDB has no savepoints, so a rollback at any nesting level dooms the
    whole transaction: the outermost level rolls back instead of committing,
    and raises this so a caller that swallowed the inner failure cannot
    report a save that never happened.
    """

# A lock wait at least this long is logged by _log_contended; shorter,
# uncontended statements are not (they flooded the debug log and the GUI output).
_SLOW_LOCK_WAIT_S = 0.1


def _truncate_sql(sql: str, limit: int = 200) -> str:
    """Collapse whitespace and cap length for log lines."""
    flat = " ".join(sql.split())
    if len(flat) > limit:
        return flat[:limit] + "...(truncated)"
    return flat


class DuckConnection:
    """One DuckDB connection, safe to share across threads.

    Parameters
    ----------
    db_path : str or Path
        Path to the DuckDB database file.  Use ":memory:" for in-memory.
    read_only : bool
        Open the connection read-only; DuckDB then rejects every write, so a
        read-only connection never contends for the write lock beyond
        DuckDB's shared read lock.
    """

    def __init__(self, db_path: str | Path, read_only: bool = False):
        self.db_path = str(db_path)
        self.read_only = bool(read_only)
        self._lock = threading.Lock()
        # Thread id currently holding an open BEGIN...COMMIT/ROLLBACK transaction
        # on `self.con`, or None. DuckDB connections have exactly one transaction
        # context; `_lock` only serializes individual statements (see `execute`
        # docstring), so a statement from another thread can still land *inside*
        # this transaction between `_begin()` and `_commit()`. Tracking the owner
        # lets us log that interleaving instead of just seeing the downstream
        # "Current transaction is aborted" failure with no context.
        self._tx_owner: int | None = None
        # Nesting depth of the open transaction (0 = none). Only the outermost
        # _begin/_commit/_rollback issues SQL: DuckDB cannot nest BEGIN, and a
        # for_each save wraps save_batch + record_run (each of which opens its
        # own level) in one transaction. ``_tx_doomed``: an inner level rolled
        # back, so the outermost must roll back too (no savepoints in DuckDB).
        self._tx_depth: int = 0
        self._tx_doomed: bool = False
        # Called (no arguments) after the outermost ROLLBACK, so a layer above
        # can drop Python-side caches filled by statements that never landed.
        self._rollback_listeners: list = []
        logger.debug(
            "DuckDB lock ACQUIRED (read_only=%s): %s", self.read_only, self.db_path
        )
        self.con = duckdb.connect(self.db_path, read_only=self.read_only)
        self._open = True

    # ------------------------------------------------------------------
    # Locked statements
    # ------------------------------------------------------------------

    def _recover_from_autocommit_failure(self) -> None:
        """Best-effort ROLLBACK after a failed autocommit statement.

        DuckDB requires an explicit ROLLBACK once any statement fails —
        even a plain autocommit call outside an explicit BEGIN/COMMIT —
        or every later statement on this connection fails with "Current
        transaction is aborted", regardless of what it does. Observed in
        production: a migration's ``ALTER TABLE ... ADD COLUMN`` failed
        with CatalogException (column already existed) on every restart;
        the exception was caught by the caller, but the connection stayed
        aborted, so the next *unguarded* statement — a completely
        unrelated ``CREATE TABLE IF NOT EXISTS`` — failed too, and the
        shared connection was stuck that way for the rest of the process.
        Must be called only while still holding ``_lock``, and only skips
        the rollback when an explicit transaction is open (``_tx_owner``
        set) — that transaction's rollback is the owning caller's call,
        not ours to issue out from under them.
        """
        if self._tx_owner is not None:
            return
        try:
            self.con.execute("ROLLBACK")
            logger.debug("auto-ROLLBACK issued after autocommit statement failure")
        except Exception:
            pass

    def _unregister_frame(self, view: str) -> None:
        """Drop a registered pandas view without masking the caller's error.

        The one owner of the ``finally`` cleanup for every register/unregister
        site. Inside an explicit transaction a failed statement leaves the
        transaction aborted, and DuckDB then refuses ``unregister`` too
        ("Current transaction is aborted"). Raised from a ``finally``, that
        second error REPLACES the real one: on 2026-10-22 a save failed with
        ConversionException ('Y' -> DOUBLE) but every scidb log line and the
        GUI only said TransactionException. A leaked view is harmless — the
        next ``register`` of the same name replaces it — so the failure is
        logged and swallowed.
        """
        try:
            self.con.unregister(view)
        except Exception as exc:
            logger.debug(
                "unregister(%s) failed (%s: %s); the view is replaced on next "
                "register", view, type(exc).__name__, exc,
            )

    def _log_contended(
        self, op: str, thread: int, waited: float, foreign_tx: bool, sql: str,
        detail: str = "",
    ) -> None:
        """Debug-log a locked statement only when it was contended.

        Every routine statement used to log here (~90% of scistack-debug.log
        on 2026-10-27, flooding the GUI output too). Only the cases these
        lines were added to catch remain: a lock wait of at least
        ``_SLOW_LOCK_WAIT_S``, or a statement run inside another thread's
        open transaction. Callers log failures separately, regardless.
        Call with ``_lock`` held.
        """
        if not (foreign_tx or waited >= _SLOW_LOCK_WAIT_S):
            return
        logger.debug(
            "%s thread=%d waited=%.4fs tx_owner=%s foreign_tx=%s %ssql=%s",
            op, thread, waited, self._tx_owner, foreign_tx, detail,
            _truncate_sql(sql),
        )

    def execute(self, sql: str, params=None) -> None:
        """Run one statement that returns no rows (DDL, INSERT, UPDATE, DELETE).

        Returns nothing, on purpose. DuckDB's connection returns itself from
        ``execute()``, and fetching from it after this method releases
        ``_lock`` lets a concurrent statement tear the pending result down
        mid-fetch ("INTERNAL Error: Attempted to dereference shared_ptr that
        is NULL!"; a GUI graph build, 2026-08-25). Queries that return rows
        use ``fetchall`` / ``fetchone`` / ``fetchdf``, which hold the lock
        across execute and fetch.
        """
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            self._log_contended("execute", thread, waited, foreign_tx, sql)
            try:
                if params:
                    self.con.execute(sql, params)
                else:
                    self.con.execute(sql)
            except Exception:
                logger.exception(
                    "execute FAILED thread=%d tx_owner=%s foreign_tx=%s sql=%s",
                    thread, self._tx_owner, foreign_tx, _truncate_sql(sql),
                )
                self._recover_from_autocommit_failure()
                raise

    def fetch_table(self, sql: str, params=None) -> tuple[list[str], list[tuple]]:
        """``(column_names, rows)`` with SQL NULL preserved as ``None``.

        ``fetchdf`` goes through pandas, which has no NULL for a float column:
        every NULL DOUBLE arrives as NaN and becomes indistinguishable from a
        stored NaN. That is fine for computation and wrong for a diagnostic —
        the `scidb sql` renderer printed `nan` for a NULL and sent an
        investigation chasing a save-path difference that did not exist
        (2026-09-15). Callers that must report what the database actually holds
        use this instead.
        """
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            self._log_contended("fetch_table", thread, waited, foreign_tx, sql)
            try:
                cur = self.con.execute(sql, params) if params else self.con.execute(sql)
                columns = [d[0] for d in (cur.description or [])]
                return columns, cur.fetchall()
            except Exception:
                logger.exception(
                    "fetch_table FAILED thread=%d tx_owner=%s foreign_tx=%s sql=%s",
                    thread, self._tx_owner, foreign_tx, _truncate_sql(sql),
                )
                self._recover_from_autocommit_failure()
                raise

    def _executemany(self, sql: str, params_list):
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            logger.debug(
                "_executemany thread=%d waited=%.4fs tx_owner=%s foreign_tx=%s "
                "rows=%d sql=%s",
                thread, waited, self._tx_owner, foreign_tx, len(params_list),
                _truncate_sql(sql),
            )
            try:
                return self.con.executemany(sql, params_list)
            except Exception:
                logger.exception(
                    "_executemany FAILED thread=%d tx_owner=%s foreign_tx=%s sql=%s",
                    thread, self._tx_owner, foreign_tx, _truncate_sql(sql),
                )
                self._recover_from_autocommit_failure()
                raise

    def insert_frame(self, table: str, frame, *, conflict=None) -> int:
        """Insert every row of *frame* (a pandas DataFrame or a pyarrow Table)
        into *table* with one ``INSERT ... SELECT``, under the lock.

        THE one way to insert a frame: register, insert, unregister on this
        connection, with the same contention log and autocommit-failure
        recovery as :meth:`execute`. Before 2026-11-02 scidb's bulk Arrow
        inserts (``RecordStore``) reached past the connection to the raw
        ``duckdb`` handle and skipped both (design review F14). Columns are
        the frame's, by name. ``conflict`` (column names) appends
        ``ON CONFLICT (...) DO NOTHING``. Safe inside an open transaction.
        Returns the number of rows handed in.
        """
        columns = list(getattr(frame, "column_names", None) or frame.columns)
        n_rows = frame.num_rows if hasattr(frame, "num_rows") else len(frame)
        if n_rows == 0:
            return 0
        view = "_insert_frame"
        col_str = ", ".join(f'"{c}"' for c in columns)
        sql = f'INSERT INTO "{table}" ({col_str}) SELECT {col_str} FROM {view}'
        if conflict:
            conflict_str = ", ".join(f'"{c}"' for c in conflict)
            sql += f" ON CONFLICT ({conflict_str}) DO NOTHING"
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            self._log_contended("insert_frame", thread, waited, foreign_tx, sql,
                                detail=f"table={table} rows={n_rows} ")
            t0 = time.perf_counter()
            self.con.register(view, frame)
            try:
                self.con.execute(sql)
            except Exception:
                logger.exception(
                    "insert_frame FAILED thread=%d tx_owner=%s foreign_tx=%s "
                    "table=%s rows=%d",
                    thread, self._tx_owner, foreign_tx, table, n_rows,
                )
                self._recover_from_autocommit_failure()
                raise
            finally:
                self._unregister_frame(view)
            logger.debug(
                "[insert_frame] table=%s rows=%d ms=%.1f", table, n_rows,
                (time.perf_counter() - t0) * 1000.0,
            )
        return n_rows

    def bulk_insert(self, table: str, columns, rows, conflict_cols=None) -> None:
        """Insert many rows with a single vectorized ``INSERT ... SELECT``.

        DuckDB's ``executemany`` re-runs the prepared single-row statement once
        per row; against a PRIMARY-KEY / indexed table that is pathologically
        slow (per-row index probe + maintenance), e.g. ~497s for 8k rows into
        ``_record``. This builds the rows into one DataFrame and inserts it
        through :meth:`insert_frame` (sub-second for the same data).

        ``rows`` is an iterable of value tuples in ``columns`` order. When
        ``conflict_cols`` is given, appends ``ON CONFLICT (...) DO NOTHING`` so
        the insert stays idempotent. Safe to call inside an open transaction.
        """
        rows = list(rows)
        if not rows:
            return
        self.insert_frame(
            table, pd.DataFrame(rows, columns=list(columns)), conflict=conflict_cols
        )

    def bulk_update(self, table: str, key_cols, set_cols, rows) -> int:
        """Update many rows with a single vectorized ``UPDATE ... FROM``.

        The update counterpart of :meth:`bulk_insert`, for the same reason
        (per-row statements against a PK table are pathologically slow).
        ``rows`` are value tuples in ``key_cols + set_cols`` order; a row
        whose key matches nothing is ignored, and a matched row whose
        ``set_cols`` already hold the given values is left untouched
        (``IS DISTINCT FROM``), so re-running is a no-op. Returns the number
        of rows handed in (DuckDB does not report the changed count here).
        Safe inside an open transaction, like ``bulk_insert``.
        """
        rows = list(rows)
        if not rows:
            return 0
        key_cols, set_cols = list(key_cols), list(set_cols)
        df = pd.DataFrame(rows, columns=key_cols + set_cols)
        set_str = ", ".join(f'"{c}" = d."{c}"' for c in set_cols)
        match = " AND ".join(f'"{table}"."{c}" = d."{c}"' for c in key_cols)
        changed = " OR ".join(
            f'"{table}"."{c}" IS DISTINCT FROM d."{c}"' for c in set_cols
        )
        sql = (
            f'UPDATE "{table}" SET {set_str} FROM _bulk_update_df AS d '
            f"WHERE {match} AND ({changed})"
        )
        with self._lock:
            logger.debug(
                "bulk_update thread=%d table=%s rows=%d",
                threading.get_ident(), table, len(rows),
            )
            self.con.register("_bulk_update_df", df)
            try:
                self.con.execute(sql)
            except Exception:
                logger.exception("bulk_update FAILED table=%s", table)
                self._recover_from_autocommit_failure()
                raise
            finally:
                self._unregister_frame("_bulk_update_df")
        return len(rows)

    def add_rollback_listener(self, callback) -> None:
        """Call ``callback()`` after every outermost ROLLBACK on this
        connection. A failing listener is logged, never raised."""
        self._rollback_listeners.append(callback)

    def _reset_tx_state(self) -> None:
        self._tx_owner = None
        self._tx_depth = 0
        self._tx_doomed = False

    def _notify_rollback(self) -> None:
        for callback in list(self._rollback_listeners):
            try:
                callback()
            except Exception:
                logger.exception("rollback listener %r failed", callback)

    @contextmanager
    def transaction(self, label: str = ""):
        """One transaction around the block; re-entrant on the owning thread.

        Only the outermost level issues BEGIN/COMMIT/ROLLBACK. An exception
        at any level rolls back the outermost (DuckDB has no savepoints); an
        inner level that rolled back while the outer one finishes normally
        makes the outer commit raise :class:`TransactionRolledBackError`.
        """
        self._begin()
        depth = self._tx_depth
        logger.debug("transaction(%s) BEGIN depth=%d", label, depth)
        try:
            yield self
        except BaseException as exc:
            logger.debug(
                "transaction(%s) ROLLBACK depth=%d: %s: %s",
                label, depth, type(exc).__name__, exc,
            )
            try:
                self._rollback()
            except Exception:
                logger.exception("transaction(%s) ROLLBACK failed", label)
            raise
        else:
            self._commit()
            logger.debug("transaction(%s) COMMIT depth=%d", label, depth)

    def _begin(self):
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            if self._tx_depth > 0 and self._tx_owner == thread:
                self._tx_depth += 1
                logger.debug(
                    "_begin thread=%d nested depth=%d (no SQL)",
                    thread, self._tx_depth,
                )
                return
            if self._tx_owner is not None and self._tx_owner != thread:
                logger.warning(
                    "_begin thread=%d waited=%.4fs OVERLAPPING existing "
                    "tx_owner=%d — this connection allows only one open "
                    "transaction; the new BEGIN will nest into the same "
                    "connection context",
                    thread, waited, self._tx_owner,
                )
            else:
                logger.debug("_begin thread=%d waited=%.4fs", thread, waited)
            self.con.execute("BEGIN TRANSACTION")
            self._tx_owner = thread
            self._tx_depth = 1
            self._tx_doomed = False

    def _commit(self):
        thread = threading.get_ident()
        rolled_back = False
        with self._lock:
            logger.debug(
                "_commit thread=%d tx_owner=%s depth=%d doomed=%s",
                thread, self._tx_owner, self._tx_depth, self._tx_doomed,
            )
            if self._tx_owner is not None and self._tx_owner != thread:
                logger.warning(
                    "_commit thread=%d committing a transaction opened by "
                    "thread=%d — statements from either thread may have been "
                    "interleaved into it",
                    thread, self._tx_owner,
                )
            if self._tx_depth > 1:
                self._tx_depth -= 1
                return
            if self._tx_doomed:
                logger.warning(
                    "_commit thread=%d: an inner level rolled back; rolling "
                    "back the whole transaction instead of committing",
                    thread,
                )
                try:
                    self.con.execute("ROLLBACK")
                finally:
                    self._reset_tx_state()
                rolled_back = True
            else:
                try:
                    self.con.execute("COMMIT")
                except Exception:
                    logger.exception(
                        "_commit FAILED thread=%d tx_owner=%s",
                        thread, self._tx_owner,
                    )
                    raise
                finally:
                    self._reset_tx_state()
        if rolled_back:
            self._notify_rollback()
            raise TransactionRolledBackError(
                "an inner level of this transaction rolled back; nothing was committed"
            )

    def _rollback(self):
        thread = threading.get_ident()
        with self._lock:
            logger.debug(
                "_rollback thread=%d tx_owner=%s depth=%d",
                thread, self._tx_owner, self._tx_depth,
            )
            if self._tx_depth > 1:
                # No savepoints: mark the transaction doomed; the outermost
                # level issues the ROLLBACK.
                self._tx_depth -= 1
                self._tx_doomed = True
                logger.debug(
                    "_rollback thread=%d inner level; outermost (depth=1) "
                    "will roll back",
                    thread,
                )
                return
            try:
                self.con.execute("ROLLBACK")
            finally:
                self._reset_tx_state()
        self._notify_rollback()

    def fetchall(self, sql: str, params=None) -> list:
        """All result rows as tuples, with execute and fetch under one lock.

        Right for results with few rows (metadata, aggregates). For many rows
        use ``fetchdf``: it skips building one Python object per value.
        """
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            self._log_contended("fetchall", thread, waited, foreign_tx, sql)
            try:
                if params:
                    return self.con.execute(sql, params).fetchall()
                return self.con.execute(sql).fetchall()
            except Exception:
                logger.exception(
                    "fetchall FAILED thread=%d tx_owner=%s foreign_tx=%s sql=%s",
                    thread, self._tx_owner, foreign_tx, _truncate_sql(sql),
                )
                self._recover_from_autocommit_failure()
                raise

    def _fetchall_with_frame(
        self, sql: str, frame, *, view: str = "_query_frame", params=None
    ) -> list:
        """``fetchall`` with a pandas DataFrame visible to the query as ``view``.

        The read-side twin of ``bulk_insert``'s register/unregister idiom: the
        frame is registered under the lock, the query runs against it, and it is
        unregistered in ``finally`` so a failing query cannot leak a view into
        the next caller's namespace.

        Exists so a caller can hand DuckDB a small mapping it holds in memory —
        which rows belong to which group, say — and have DuckDB do the heavy
        reduction against the stored columns by joining to it. The alternative,
        pulling the stored columns OUT to reduce them next to the mapping, is
        exactly the pattern that boxed 174 million floats into Python on
        2026-09-13 (scistackplot's ``.claude/plan-duckdb-native-reduction.md``).

        ``view`` is a plain identifier; callers that could run concurrently
        against one connection should pick distinct names, though the lock
        already serialises them.
        """
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            self._log_contended(
                "_fetchall_with_frame", thread, waited, foreign_tx, sql,
                detail=f"view={view} rows={len(frame)} ",
            )
            self.con.register(view, frame)
            try:
                if params:
                    return self.con.execute(sql, params).fetchall()
                return self.con.execute(sql).fetchall()
            except Exception:
                logger.exception(
                    "_fetchall_with_frame FAILED thread=%d tx_owner=%s foreign_tx=%s "
                    "view=%s sql=%s",
                    thread, self._tx_owner, foreign_tx, view, _truncate_sql(sql),
                )
                self._recover_from_autocommit_failure()
                raise
            finally:
                self._unregister_frame(view)

    def _fetchdf_with_frame(
        self, sql: str, frame, *, view: str = "_query_frame", params=None
    ) -> "pd.DataFrame":
        """``_fetchall_with_frame``, returning a DataFrame instead of rows.

        **The difference is the whole point, not a convenience.** ``fetchall()``
        materialises one Python object per value — a boxed float per sample —
        which is the cost this exists to avoid: on 2026-09-13, exploding 174
        million samples into Python took 153 s
        (``.claude/plan-duckdb-native-reduction.md`` §2). ``.df()`` goes through
        Arrow into numpy, so a numeric column arrives as one contiguous buffer
        and never passes through a Python float at all. Use this for any query
        whose result has many ROWS; ``fetchall`` remains right for results with
        few rows (metadata, aggregates).
        """
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            self._log_contended(
                "_fetchdf_with_frame", thread, waited, foreign_tx, sql,
                detail=f"view={view} rows={len(frame)} ",
            )
            self.con.register(view, frame)
            try:
                if params:
                    return self.con.execute(sql, params).df()
                return self.con.execute(sql).df()
            except Exception:
                logger.exception(
                    "_fetchdf_with_frame FAILED thread=%d tx_owner=%s foreign_tx=%s "
                    "view=%s sql=%s",
                    thread, self._tx_owner, foreign_tx, view, _truncate_sql(sql),
                )
                self._recover_from_autocommit_failure()
                raise
            finally:
                self._unregister_frame(view)

    def fetchone(self, sql: str, params=None):
        """First result row, or ``None`` — the locked counterpart to
        ``fetchall`` for single-row lookups.

        Exists so callers never have to write ``execute(...).fetchone()``,
        which fetches after the lock has already been released. See the NOTE
        on ``execute``.
        """
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            self._log_contended("fetchone", thread, waited, foreign_tx, sql)
            try:
                result = self.con.execute(sql, params) if params else self.con.execute(sql)
                row = result.fetchone()
                if row is not None:
                    # Drain the rest: a half-read result stays OPEN on the
                    # shared connection, and newer DuckDB refuses the next
                    # register() (``bulk_insert``) with "connection has an
                    # unfinished result from execute()" (CI, Python
                    # 3.12/3.13, 2026-10-23). Callers ask single-row
                    # questions, so this is at most a few rows.
                    result.fetchall()
                return row
            except Exception:
                logger.exception(
                    "fetchone FAILED thread=%d tx_owner=%s foreign_tx=%s sql=%s",
                    thread, self._tx_owner, foreign_tx, _truncate_sql(sql),
                )
                self._recover_from_autocommit_failure()
                raise

    def fetchdf(self, sql: str, params=None) -> pd.DataFrame:
        """The result as a DataFrame, with execute and fetch under one lock.

        Goes through Arrow into numpy, so numeric columns never pass through
        Python objects. NULL in a float column arrives as NaN; use
        ``fetch_table`` when a NULL must stay distinguishable.
        """
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            self._log_contended("fetchdf", thread, waited, foreign_tx, sql)
            try:
                if params:
                    return self.con.execute(sql, params).fetchdf()
                return self.con.execute(sql).fetchdf()
            except Exception:
                logger.exception(
                    "fetchdf FAILED thread=%d tx_owner=%s foreign_tx=%s sql=%s",
                    thread, self._tx_owner, foreign_tx, _truncate_sql(sql),
                )
                self._recover_from_autocommit_failure()
                raise

    def fetchdf_if_open(self, sql: str, params=None) -> pd.DataFrame | None:
        """``fetchdf``, or None when the connection is closed.

        The open check runs under ``_lock`` — the same lock ``close()``
        takes — so check and query are atomic. Checking ``is_open`` and
        then calling ``fetchdf`` leaves a window a concurrent close can
        hit, which ``fetchdf`` then logs as an ERROR with a traceback.
        """
        thread = threading.get_ident()
        with self._lock:
            if not self._open:
                logger.debug(
                    "fetchdf_if_open thread=%d: connection closed, skipped sql=%s",
                    thread, _truncate_sql(sql),
                )
                return None
            foreign_tx = self._tx_owner is not None and self._tx_owner != thread
            try:
                if params:
                    return self.con.execute(sql, params).fetchdf()
                return self.con.execute(sql).fetchdf()
            except Exception:
                logger.exception(
                    "fetchdf_if_open FAILED thread=%d tx_owner=%s foreign_tx=%s sql=%s",
                    thread, self._tx_owner, foreign_tx, _truncate_sql(sql),
                )
                self._recover_from_autocommit_failure()
                raise

    def table_exists(self, name: str) -> bool:
        """Whether a table or view called ``name`` exists."""
        rows = self.fetchall(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [name],
        )
        return rows[0][0] > 0

    # ------------------------------------------------------------------
    # Direct query access
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Context manager / cleanup
    # ------------------------------------------------------------------

    @property
    def is_open(self) -> bool:
        """Whether the connection is open: False after close(), True again
        after reopen(). The one owner of that state. A caller that can do
        without the database (a best-effort read) asks this rather than
        querying a closed connection, which fetchdf would log as an ERROR."""
        return getattr(self, "_open", False)

    def close(self):
        """Close the DuckDB connection.

        Takes ``_lock`` so a close never lands between another thread's
        is-open check and its statement (see ``fetchdf_if_open``): two
        overlapping GUI requests sharing one refcounted connection hit that
        window on 2026-10-19 (scistack.log 15:08:27, "Connection already
        closed!" 4 ms after ``is_open`` said True).
        """
        thread = threading.get_ident()
        wait_start = time.monotonic()
        with self._lock:
            waited = time.monotonic() - wait_start
            self.con.close()
            self._open = False
        logger.debug(
            "DuckDB lock RELEASED: %s (thread=%d waited=%.4fs for in-flight statement)",
            self.db_path, thread, waited,
        )

    def reopen(self):
        """Reopen the DuckDB connection after close(), preserving read_only mode."""
        logger.debug(
            "DuckDB lock ACQUIRED (reopen, read_only=%s): %s",
            getattr(self, "read_only", False),
            self.db_path,
        )
        with self._lock:
            self.con = duckdb.connect(
                str(self.db_path), read_only=getattr(self, "read_only", False)
            )
            self._open = True

    def __enter__(self):
        """Enter context manager."""
        return self

    def __exit__(self, *args):
        """Exit context manager, closing the DuckDB connection."""
        self.close()

