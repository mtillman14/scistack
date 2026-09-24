"""
Shared database connection for the GUI backend.

The DatabaseManager instance is created once at startup (in __main__.py)
and shared by all API endpoints.
"""

import logging
import os
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


#: Backoff ceiling while a MATLAB run is known to be in flight.
#:
#: ACQUIRE_RETRY_TIMEOUT's five seconds are right for what it was written
#: for: MATLAB's own short writes clear well under a second, so retrying
#: turns most conflicts into a pause nobody notices. They are wrong against
#: a twenty-minute run, where every GUI click that touches the database
#: burns five seconds and *then* reports a conflict that was knowable at
#: t=0. Clicking around during a run cost ~5 s per click.
#:
#: Not zero. If the hint is ever wrong, a 200 ms MATLAB write that would
#: have succeeded must not start reporting failures — this is "5 s to
#: 0.5 s", not "5 s to instant". And it only changes how long we WAIT,
#: never whether we ATTEMPT: the acquire always tries at least once, so a
#: database that is actually free is always opened.
ACQUIRE_RETRY_TIMEOUT_TRACKED = 0.5


def _matlab_run_in_flight() -> bool:
    """Whether a watched MATLAB run currently believes it owns a database.

    Never raises: this only tunes a timeout, and a failure to answer must
    fall back to the patient default rather than break an acquire.
    """
    try:
        from scistack_gui.matlab_run_watch import any_run_in_flight

        return any_run_in_flight()
    except Exception:  # noqa: BLE001 — a hint is not worth an exception
        return False


def _effective_acquire_timeout(timeout: float) -> float:
    """How long to keep retrying, given what we know about MATLAB."""
    if not _matlab_run_in_flight():
        return timeout
    shortened = min(timeout, ACQUIRE_RETRY_TIMEOUT_TRACKED)
    if shortened < timeout:
        logger.debug(
            "[db] a MATLAB run is in flight — backing off %.1fs instead of %.1fs",
            shortened,
            timeout,
        )
    return shortened


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
    timeout = _effective_acquire_timeout(timeout)
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
    (2026-09-11 log, 12:27:39 acquire -> 12:28:10 release). The database is
    needed while the variable frames load and, since the DuckDB reducer, while
    they are reduced (``plot_service._loaded``); rendering is in memory.

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


# ---------------------------------------------------------------------------
# Who has the database, and are they still alive?
#
# This is the half of MATLAB run tracking that needs no cooperation from
# MATLAB. A marker file tells us how a run ENDED (see
# ``scidb.run_markers``), but a killed or Ctrl-C'd MATLAB writes nothing, so
# a missing marker is ambiguous on its own: still running, or gone.
#
# DuckDB answers it for us. Its lock-conflict message names the process
# holding the file — ``_LOCK_HOLDER_RE`` / ``_LOCK_PID_RE`` above already
# parse both the POSIX and the Windows spelling — so a failed open tells us
# *which* process owns the database, and the OS tells us whether that
# process still exists.
# ---------------------------------------------------------------------------


def pid_alive(pid: int) -> bool:
    """Whether process *pid* currently exists.

    **When in doubt, say alive.** Every caller uses this to decide whether a
    run that has not reported is dead, and a false "dead" ends a run that is
    still working — the worse error by far, since the user then re-runs work
    that is already in flight against the same database. A false "alive"
    only delays the verdict until the ceiling.
    """
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        STILL_ACTIVE = 259
        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            # Could be "gone", could be "access denied". A denied handle for
            # a live process is possible when MATLAB runs elevated, so ask
            # once more in a way that does not need the handle.
            return _nt_pid_in_snapshot(pid)
        try:
            code = ctypes.c_ulong()
            if kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return code.value == STILL_ACTIVE
            return True
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # It exists; it just is not ours to signal.
        return True
    except OSError as exc:
        logger.debug(
            "[db] pid_alive(%d): undecidable (%s) — assuming alive", pid, exc
        )
        return True
    return True


def _nt_pid_in_snapshot(pid: int) -> bool:
    """Windows fallback: is *pid* in the process list?

    Used only when ``OpenProcess`` fails, which conflates "gone" with
    "access denied" — and MATLAB started elevated is a real case.
    """
    try:
        import subprocess

        out = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True,
            text=True,
            timeout=5,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        return str(pid) in (out.stdout or "")
    except Exception as exc:  # noqa: BLE001 — a probe must never raise
        logger.debug(
            "[db] _nt_pid_in_snapshot(%d) failed (%s) — assuming alive", pid, exc
        )
        return True


class LockHolder:
    """The answer to "who has this database, and are they still there?"."""

    __slots__ = ("free", "holder", "pid", "alive", "raw")

    def __init__(
        self,
        free: bool,
        holder: "str | None" = None,
        pid: "int | None" = None,
        alive: bool = False,
        raw: str = "",
    ):
        #: Nothing holds the file — we were able to open it.
        self.free = free
        #: The executable path DuckDB named, when it named one.
        self.holder = holder
        self.pid = pid
        #: Whether that process still exists. Meaningless when ``free``.
        self.alive = alive
        self.raw = raw

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        if self.free:
            return "LockHolder(free)"
        return (
            f"LockHolder(holder={self.holder!r}, pid={self.pid}, alive={self.alive})"
        )


def probe_lock_holder(db_path=None) -> LockHolder:
    """Who currently holds *db_path*, without disturbing our own connection.

    Opens a throwaway **read-only** connection: DuckDB allows many readers
    or one writer, so this succeeds exactly when no other process is writing
    — and fails with the lock message we already know how to parse when one
    is. It touches neither ``_db`` nor the refcount, so it is safe to call
    from a watcher thread at any time.

    Returns ``free`` when *we* are the holder: this process having the
    database open is not an answer to "is MATLAB still running", and
    reporting ourselves would make every probe look busy.
    """
    path = Path(db_path) if db_path is not None else _db_path
    if path is None:
        return LockHolder(free=True)
    if _db_open:
        # We hold it. Nothing to learn, and the probe would only conflict
        # with ourselves.
        return LockHolder(free=True)
    try:
        con = duckdb.connect(str(path), read_only=True)
        con.close()
        return LockHolder(free=True)
    except Exception as exc:  # noqa: BLE001 — classified below
        locked = _as_locked_error(exc)
        if locked is None:
            # Not a lock conflict (a missing file, a corrupt database). That
            # is not this function's question, and guessing "busy" from it
            # would keep a finished run pending for ever.
            logger.debug("[db] probe_lock_holder: %s is not a lock conflict", exc)
            return LockHolder(free=True)
        pid: "int | None"
        try:
            pid = int(locked.pid) if locked.pid else None
        except (TypeError, ValueError):
            pid = None
        # No PID in the message means we know it is held but not by whom —
        # "held" is the safe reading, since something clearly has it.
        alive = pid_alive(pid) if pid is not None else True
        return LockHolder(
            free=False,
            holder=locked.holder,
            pid=pid,
            alive=alive,
            raw=locked.raw,
        )
