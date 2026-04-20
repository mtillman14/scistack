"""
Shared database connection for the GUI backend.

The DatabaseManager instance is created once at startup (in __main__.py)
and shared by all API endpoints and the Jupyter kernel.
"""

import logging
from pathlib import Path
import threading
import duckdb
import scidb
from scidb.database import DatabaseManager

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
_db_open = False          # is the DuckDB connection currently held?
_db_refcount = 0          # number of concurrent callers holding the connection
_db_lifecycle_lock = threading.Lock()


def acquire_db_connection() -> None:
    """Increment the holder count and reopen the connection if needed.

    If ``reopen()`` raises (typically because another process still holds
    the DuckDB file lock), the refcount is **not** incremented — the caller
    must not call :func:`release_db_connection`.  This keeps the refcount
    consistent with the number of live holders, so a transient lock
    conflict doesn't leak the count upward and keep the lock permanently
    held on subsequent successful acquires.
    """
    global _db_open, _db_refcount
    with _db_lifecycle_lock:
        reopened = False
        if not _db_open and _db is not None:
            try:
                _db.reopen()
            except Exception:
                logger.exception(
                    "acquire_db_connection: reopen failed (refcount stays at %d)",
                    _db_refcount,
                )
                raise
            _db_open = True
            reopened = True
        _db_refcount += 1
        logger.debug("acquire_db_connection: refcount=%d, reopened=%s", _db_refcount, reopened)


def release_db_connection() -> None:
    """Decrement the holder count and close the connection when idle."""
    global _db_open, _db_refcount
    with _db_lifecycle_lock:
        _db_refcount = max(0, _db_refcount - 1)
        closed = False
        if _db_refcount == 0 and _db_open and _db is not None:
            _db._duck.close()
            _db_open = False
            closed = True
        logger.debug("release_db_connection: refcount=%d, closed=%s", _db_refcount, closed)


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
    global _db, _db_path, _db_open
    schema_keys = read_schema_keys(db_path)
    _db = scidb.configure_database(db_path, schema_keys)
    _db_path = db_path
    _db_open = True

    # Migrate manual_nodes / manual_edges from JSON into DuckDB (one-time, idempotent).
    from scistack_gui import pipeline_store
    layout_path = db_path.with_suffix(".layout.json")
    pipeline_store.migrate_from_json(_db, layout_path)

    return _db


def create_db(db_path: Path, schema_keys: list[str]) -> DatabaseManager:
    """
    Create a new SciStack database at db_path with the given schema keys.
    The parent directory must already exist. Fails if the file already exists.
    """
    global _db, _db_path, _db_open
    if db_path.exists():
        raise FileExistsError(f"Database already exists: {db_path}")
    if not schema_keys:
        raise ValueError("schema_keys must not be empty")
    _db = scidb.configure_database(db_path, schema_keys)
    _db_path = db_path
    _db_open = True
    return _db


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
