"""
Shared database connection for the GUI backend.

The DatabaseManager instance is created once at startup (in __main__.py)
and shared by all API endpoints.
"""

import logging
import re
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import duckdb
from scidb.database import DatabaseManager

import scidb

logger = logging.getLogger("scistack_gui.db")

_db: DatabaseManager | None = None
_db_path: Path | None = None

# ---------------------------------------------------------------------------
# Connection lifecycle — allows MATLAB to access the DB between GUI requests.
#
# The DuckDB file lock is held only while a request is being serviced (or a
# Python run is in progress). Between requests the lock is released so MATLAB
# can open the same file.
# ---------------------------------------------------------------------------
_db_open = False  # is the DuckDB connection currently held?
_db_refcount = 0  # number of concurrent callers holding the connection
_db_lifecycle_lock = threading.Lock()

# How this process manages the connection. The two GUI transports genuinely
# differ and nothing else in this module can tell them apart:
#
#   "persistent"   — standalone/FastAPI. The connection is opened at startup and
#                    held for the life of the process; no request acquires or
#                    releases (grep: acquire_db_connection has no callers
#                    outside server.py). Narrowing a hold here is meaningless,
#                    and an acquire/release pair would be actively harmful — the
#                    release drops the refcount to 0 and CLOSES the connection
#                    that every later request expects to find open, since
#                    get_db() returns the manager without reopening it.
#   "per_request"  — the JSON-RPC server. It deliberately drops the lock between
#                    requests so MATLAB can open the same file.
#
# Set by server.py at startup. Declared here rather than sniffed, because a
# wrong guess is a closed connection in a live process.
_connection_policy = "persistent"
# Name of the external process the database has been deliberately handed to
# (see external_db_access), or None. While set, acquiring is refused up
# front instead of racing MATLAB for the file lock.
_external_holder: "str | None" = None

# How long to keep retrying a reopen that lost the race to another process.
# MATLAB grabs the file for a single write and lets go again, so a short
# backoff turns most conflicts into a barely-noticeable pause instead of a
# user-visible error. A MATLAB session that keeps the DB open for a whole
# pipeline run will still exceed this — that case has to be *reported*, not
# waited out, which is what DatabaseLockedError is for.
ACQUIRE_RETRY_TIMEOUT = 5.0
ACQUIRE_RETRY_INTERVAL = 0.25

# DuckDB's conflict message names the process holding the lock, but the
# wording is PLATFORM-SPECIFIC and both forms must be recognized.
#
# POSIX:
#   "Could not set lock on file ...: Conflicting lock is held in
#    /path/to/python (PID 12345)"
#
# Windows (this is the one that bit us — it shares no phrase with the POSIX
# form, so a POSIX-only matcher classifies it as a generic I/O error, skips
# the retry loop entirely, and surfaces a raw duckdb.IOException):
#   'Cannot open file "...": The process cannot access the file because it
#    is being used by another process.
#
#    File is already open in
#    C:\\Program Files\\MATLAB\\R2023b\\bin\\win64\\MATLAB.exe (PID 53772)'
#
# The PID is the single most useful thing we can tell the user, so pull it
# out rather than dumping the whole multi-line IOException at them.
_LOCK_PID_RE = re.compile(r"\(PID (\d+)\)")
_LOCK_HOLDER_RE = re.compile(
    r"(?:Conflicting lock is held in|File is already open in)\s*"
    r"(.*?)(?: \(PID \d+\))?[.\n]",
    re.DOTALL,
)
# Any one of these marks a message as a lock conflict rather than a genuine
# I/O error. Kept as a tuple so adding a platform means adding a phrase.
_LOCK_CONFLICT_MARKERS = (
    "Conflicting lock",  # POSIX
    "set lock on file",  # POSIX
    "File is already open in",  # Windows
    "being used by another process",  # Windows
)


class DatabaseLockedError(RuntimeError):
    """The DuckDB file is open in another process (typically MATLAB).

    Distinct from a generic failure because it is *expected* and
    *recoverable*: the GUI deliberately drops its file lock between
    requests (see the module-level note above) precisely so MATLAB can take
    it. Callers need to tell the user "MATLAB currently owns the database"
    rather than surfacing a raw ``duckdb.IOException`` — and the JSON-RPC
    dispatcher maps it to its own error code so the extension can treat it
    as transient.

    Note this is NOT an ``OSError``, which is what DuckDB itself raises for
    a lock conflict. Nothing catches ``OSError`` around an acquire (only
    ``server.py`` calls one, and it handles this class explicitly), and
    subclassing ``OSError`` with a custom multi-argument ``__init__`` runs
    into its errno-parsing constructor — not worth the risk for
    compatibility no caller needs.
    """

    def __init__(
        self,
        db_path,
        holder: "str | None",
        pid: "str | None",
        raw: str,
        retryable: bool = True,
    ):
        self.db_path = str(db_path)
        self.holder = holder
        self.pid = pid
        self.raw = raw
        # False when WE handed the file over on purpose (external_db_access):
        # that lasts as long as the MATLAB run does, so backing off for a few
        # seconds only delays an answer we already know.
        self.retryable = retryable
        who = holder or "another process"
        if pid:
            who = f"{who} (PID {pid})"
        super().__init__(
            f"The database is currently open in {who}. SciStack releases its "
            f"own lock between requests so MATLAB can use the database, so "
            f"this means MATLAB (or another tool) still has it open. Close it "
            f"there — or wait for the MATLAB run to finish — and retry."
        )


def _as_locked_error(exc: Exception) -> "DatabaseLockedError | None":
    """Classify a ``reopen()`` failure as a lock conflict, or ``None``.

    Matches on the message rather than the exception type: DuckDB reports
    this as a plain ``IOException`` shared with unrelated I/O problems, and
    a genuine I/O error must NOT be retried or reported as "MATLAB has it".

    Both the POSIX and Windows wordings count — see
    ``_LOCK_CONFLICT_MARKERS``. They share no common phrase, so a matcher
    written against one platform silently fails open on the other.
    """
    text = str(exc)
    if not any(marker in text for marker in _LOCK_CONFLICT_MARKERS):
        return None
    pid_match = _LOCK_PID_RE.search(text)
    holder_match = _LOCK_HOLDER_RE.search(text)
    return DatabaseLockedError(
        _db_path,
        holder_match.group(1).strip() if holder_match else None,
        pid_match.group(1) if pid_match else None,
        text,
    )


def acquire_db_connection(timeout: float = ACQUIRE_RETRY_TIMEOUT) -> None:
    """Increment the holder count and reopen the connection if needed.

    If ``reopen()`` raises (typically because another process still holds
    the DuckDB file lock), the refcount is **not** incremented — the caller
    must not call :func:`release_db_connection`.  This keeps the refcount
    consistent with the number of live holders, so a transient lock
    conflict doesn't leak the count upward and keep the lock permanently
    held on subsequent successful acquires.

    A lock conflict is retried for up to ``timeout`` seconds before being
    raised as :class:`DatabaseLockedError`; any other failure is raised
    immediately.

    Contention is logged as one *episode*, not one line per attempt: the
    first blocked attempt warns, the rest are DEBUG, and a successful acquire
    after any wait logs a single INFO summary. Per-attempt logging made a
    single 2.8s wait emit 66 lines (33 WARN/INFO pairs) in the 2026-09-01
    session while never stating the one thing worth knowing — how long it
    actually took. The window must stay visible at default level, though: it
    being invisible is how a MATLAB-held database turned into an unexplained
    GUI hang (see .claude/plan-matlab-run-hang-fix.md).
    """
    global _db_open, _db_refcount
    started = time.monotonic()
    deadline = started + max(0.0, timeout)
    attempt = 0
    last_holder: "str | None" = None
    while True:
        attempt += 1
        try:
            _try_acquire_db_connection(attempt)
        except DatabaseLockedError as locked:
            remaining = deadline - time.monotonic()
            if not locked.retryable:
                logger.info(
                    "[db] acquire_db_connection: %s owns the database for the "
                    "duration of its run — refusing immediately rather than "
                    "backing off",
                    locked.holder,
                )
                raise
            if remaining <= 0:
                logger.warning(
                    "[db] acquire_db_connection: giving up after %d attempt(s) "
                    "over %.1fs — %s still holds %s",
                    attempt,
                    timeout,
                    locked.holder or "another process",
                    locked.db_path,
                )
                raise
            last_holder = locked.holder or "another process"
            logger.debug(
                "[db] acquire_db_connection: locked by %s%s, retrying in %.2fs "
                "(%.1fs left)",
                last_holder,
                f" (PID {locked.pid})" if locked.pid else "",
                ACQUIRE_RETRY_INTERVAL,
                remaining,
            )
            time.sleep(min(ACQUIRE_RETRY_INTERVAL, remaining))
            continue
        if attempt > 1:
            # Closes the episode opened by the first blocked attempt's WARN.
            # With the per-attempt lines at DEBUG this is the only thing a
            # default-level log has to answer "was there contention, and how
            # bad was it?" — so it carries the elapsed time and the count.
            logger.info(
                "[db] acquire_db_connection: acquired after %.1fs and %d "
                "attempt(s) — %s had the lock",
                time.monotonic() - started,
                attempt,
                last_holder or "another process",
            )
        return


def _try_acquire_db_connection(attempt: int) -> None:
    """One acquire attempt. Raises :class:`DatabaseLockedError` on a lock
    conflict so :func:`acquire_db_connection` can decide whether to retry."""
    global _db_open, _db_refcount
    with _db_lifecycle_lock:
        logger.debug(
            "[db] acquire_db_connection: current state - open=%s, refcount=%d, "
            "attempt=%d",
            _db_open,
            _db_refcount,
            attempt,
        )
        if _external_holder is not None and not _db_open:
            # We handed the file over on purpose. Reopening here would race
            # the holder for the lock, and winning would be worse than
            # losing — it would break the run we just dispatched.
            raise DatabaseLockedError(
                _db_path,
                _external_holder,
                None,
                f"database deliberately handed to {_external_holder}",
                retryable=False,
            )
        reopened = False
        if not _db_open and _db is not None:
            logger.info("[db] acquire_db_connection: connection closed, reopening")
            try:
                _db.reopen()
                logger.info(
                    "[db] acquire_db_connection: successfully reopened connection"
                )
            except Exception as exc:
                locked = _as_locked_error(exc)
                if locked is not None:
                    # The first blocked attempt announces the contention at
                    # WARN so it stays visible at default level; the rest are
                    # DEBUG, and acquire_db_connection's INFO summary reports
                    # how the episode ended. Warning on every attempt buried
                    # the rest of the log — see its docstring.
                    logger.log(
                        logging.WARNING if attempt == 1 else logging.DEBUG,
                        "[db] acquire_db_connection: reopen blocked by a "
                        "conflicting lock (refcount stays at %d): %s",
                        _db_refcount,
                        locked.raw.replace("\n", " ")[:300],
                    )
                    raise locked from exc
                logger.exception(
                    "[db] acquire_db_connection: reopen failed (refcount stays at %d)",
                    _db_refcount,
                )
                raise
            _db_open = True
            reopened = True
        _db_refcount += 1
        logger.debug(
            "[db] acquire_db_connection complete: refcount=%d, reopened=%s",
            _db_refcount,
            reopened,
        )


def release_db_connection() -> None:
    """Decrement the holder count and close the connection when idle."""
    global _db_open, _db_refcount
    with _db_lifecycle_lock:
        logger.debug(
            "[db] release_db_connection: current refcount=%d, open=%s",
            _db_refcount,
            _db_open,
        )
        _db_refcount = max(0, _db_refcount - 1)
        closed = False
        if _db_refcount == 0 and _db_open and _db is not None:
            logger.info(
                "[db] release_db_connection: refcount reached 0, closing connection"
            )
            _db._duck.close()
            _db_open = False
            closed = True
        logger.debug(
            "[db] release_db_connection complete: refcount=%d, closed=%s",
            _db_refcount,
            closed,
        )


def set_connection_policy(policy: str) -> None:
    """Declare how this process manages the DuckDB connection.

    See :data:`_connection_policy`. Called once, by the JSON-RPC server at
    startup; the default suits the standalone/FastAPI process.
    """
    global _connection_policy
    if policy not in ("persistent", "per_request"):
        raise ValueError(
            f"Unknown connection policy {policy!r} — "
            f"expected 'persistent' or 'per_request'."
        )
    _connection_policy = policy
    logger.info("[db] connection policy set to %r", policy)


def connection_policy() -> str:
    """The current policy — for tests and for logging."""
    return _connection_policy


@contextmanager
def db_connection(label: str = "", *, needed: bool = True):
    """Hold the DuckDB connection for the narrowest window a caller can manage.

    Plot work is the motivating case and states the problem well: resolving a
    figure spent 25-27s in pandas and matplotlib while
    :func:`~scistack_gui.server._handle_request` held the file lock across the
    *whole* RPC, so MATLAB could not open the database for the full 27 seconds
    (2026-09-11 log, 12:27:39 acquire -> 12:28:10 release). The database is only
    needed while the variable frames load; everything after that is in memory.

    Under the ``persistent`` policy this is a no-op — see
    :data:`_connection_policy` for why acquiring there would close the very
    connection the next request needs.

    ``needed=False`` skips the acquire entirely, for a caller that turns out not
    to touch the database at all (plotting a CSV, say). The context still works,
    so callers don't need two code paths.

    The hold time is logged, because "the lock is held for less time now" is the
    entire point of this and an unmeasured claim about it is worthless.
    """
    if not needed or _connection_policy != "per_request":
        yield
        return

    started = time.monotonic()
    acquire_db_connection()
    try:
        yield
    finally:
        # Mirrors _handle_request: a failed acquire raises before the try, so
        # release is only ever reached for a hold we actually took.
        release_db_connection()
        held = time.monotonic() - started
        logger.info(
            "[db] %s: held the DuckDB connection for %.3fs",
            label or "db_connection",
            held,
        )


@contextmanager
def external_db_access(holder: str = "MATLAB"):
    """Hand the DuckDB file to another process for the duration of the block.

    The JSON-RPC server drops its lock between requests, but nothing else
    does — in browser/standalone mode (FastAPI) the connection stays open
    for the life of the process. A MATLAB sidecar run started from there
    would therefore find the database locked by *us* and fail on its first
    ``scihist.configure_database`` call. So: close the connection, and mark
    the database externally owned so a concurrent request can't quietly
    reopen it and steal the lock back mid-run — such a request gets a
    :class:`DatabaseLockedError` naming the holder, which is both true and
    actionable, rather than a race.

    The connection is restored on exit only if we had it on entry.
    """
    global _db_open, _external_holder
    with _db_lifecycle_lock:
        had_connection = _db_open
        if _db_open and _db is not None:
            logger.info(
                "[db] external_db_access: releasing the DuckDB lock for %s", holder
            )
            _db._duck.close()
            _db_open = False
        _external_holder = holder
    try:
        yield
    finally:
        with _db_lifecycle_lock:
            _external_holder = None
            # Only reopen if callers are still holding, or we had it open
            # before. A reopen here can still lose to a MATLAB process that
            # has not fully exited; that is reported by the next acquire
            # rather than raised into whatever finished the run.
            if (had_connection or _db_refcount > 0) and not _db_open and _db is not None:
                try:
                    _db.reopen()
                    _db_open = True
                    logger.info(
                        "[db] external_db_access: reacquired the DuckDB lock "
                        "after %s",
                        holder,
                    )
                except Exception:
                    logger.warning(
                        "[db] external_db_access: could not reacquire the "
                        "DuckDB lock after %s — the next request will retry",
                        holder,
                        exc_info=True,
                    )


def close_initial_connection() -> None:
    """Release the connection held since startup.

    Called once after the server sends its 'ready' notification so that
    MATLAB (or any other process) can open the DB immediately.  The lock
    is reacquired automatically on the first incoming request.
    """
    global _db_open
    with _db_lifecycle_lock:
        if _db_open and _db is not None:
            logger.debug("close_initial_connection: releasing startup lock")
            _db._duck.close()
            _db_open = False


def read_schema_keys(db_path: Path) -> list[str]:
    """
    Read the schema keys from an existing SciStack database without needing
    to know them in advance. The schema keys are stored as columns in the
    _schema table (all columns except schema_id and schema_level).
    """
    logger.debug("read_schema_keys: opening read-only connection to %s", db_path)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = '_schema' "
            "AND column_name NOT IN ('schema_id', 'schema_level') "
            "ORDER BY ordinal_position"
        ).fetchall()
        return [row[0] for row in rows]
    finally:
        con.close()
        logger.debug("read_schema_keys: closed read-only connection to %s", db_path)


def init_db(db_path: Path) -> DatabaseManager:
    """
    Open an existing SciStack database. Called once at startup.
    Reads schema keys from the DB itself so the user doesn't need to supply them.
    """
    logger.info("[db] init_db: initializing database from %s", db_path)
    global _db, _db_path, _db_open

    logger.info("[db] reading schema keys from database")
    schema_keys = read_schema_keys(db_path)
    logger.info("[db] found %d schema key(s): %s", len(schema_keys), schema_keys)

    logger.info("[db] configuring database connection")
    _db = scidb.configure_database(db_path, schema_keys)
    _db_path = db_path
    _db_open = True
    logger.info("[db] database connection established")

    # Migrate manual_nodes / manual_edges from JSON into DuckDB (one-time, idempotent).
    logger.info("[db] migrating legacy JSON layout to DuckDB (if needed)")
    from scistack_gui import pipeline_store

    layout_path = db_path.with_suffix(".layout.json")
    pipeline_store.migrate_from_json(_db, layout_path)
    logger.info("[db] migration complete")

    logger.info("[db] init_db complete: database ready at %s", db_path)
    return _db


def create_db(db_path: Path, schema_keys: list[str]) -> DatabaseManager:
    """
    Create a new SciStack database at db_path with the given schema keys.
    The parent directory must already exist. Fails if the file already exists.
    """
    logger.info(
        "[db] create_db: creating new database at %s with schema keys: %s",
        db_path,
        schema_keys,
    )
    global _db, _db_path, _db_open

    logger.info("[db] validating database does not exist")
    if db_path.exists():
        raise FileExistsError(f"Database already exists: {db_path}")

    logger.info("[db] validating schema keys")
    if not schema_keys:
        raise ValueError("schema_keys must not be empty")

    logger.info("[db] configuring new database with %d schema key(s)", len(schema_keys))
    _db = scidb.configure_database(db_path, schema_keys)
    _db_path = db_path
    _db_open = True

    logger.info("[db] create_db complete: new database created at %s", db_path)
    return _db


def is_loaded() -> bool:
    """Whether a database has been opened or created yet (via init_db/create_db)."""
    return _db is not None


def get_db_path() -> Path:
    """Returns the path to the open database file."""
    if _db_path is None:
        raise RuntimeError("Database not initialised. Call init_db() first.")
    return _db_path


def get_db() -> DatabaseManager:
    """FastAPI dependency: returns the shared db instance."""
    if _db is None:
        raise RuntimeError("Database not initialised. Call init_db() first.")
    return _db
