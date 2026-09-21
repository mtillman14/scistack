"""
JSON-RPC over stdin/stdout server for the VS Code extension.

Usage:
    python -m scistack_gui.server --db experiment.duckdb [--module pipeline.py]

Protocol:
    - Reads newline-delimited JSON-RPC requests from stdin
    - Writes newline-delimited JSON-RPC responses/notifications to stdout
    - Stderr is used for logging (forwarded to VS Code Output Channel)

This replaces __main__.py + app.py for the extension mode. The standalone
FastAPI mode (scistack-gui CLI) is unchanged and still works.
"""

import argparse
import json
import logging
import os
import sys
import threading
import time
from pathlib import Path

# Logging: the scistacklog facade owns the scistack layer loggers — its
# console sink already writes to stderr (never stdout, which carries the
# JSON-RPC stream). Raise the console sink to DEBUG so the VS Code Output
# Channel gets full detail. A root basicConfig would double-print every
# layer record (propagate=True), so only non-scistack loggers get a plain
# stderr handler of their own.
from scidb.log import Log as _Log

_Log.attach()
_Log.set_level("DEBUG", sink="console")
# Explicit name: under `python -m scistack_gui.server` __name__ is
# "__main__", which would fall outside the scistack_gui layer logger.
logger = logging.getLogger("scistack_gui.server")

# Optional: start a debugpy listener so VS Code can attach and hit breakpoints
# inside user functions executed by /api/run. Enable by setting
# SCISTACK_GUI_DEBUG=1 (optionally SCISTACK_GUI_DEBUG_PORT=<port>,
# SCISTACK_GUI_DEBUG_WAIT=1 to block until the debugger attaches).
if os.environ.get("SCISTACK_GUI_DEBUG"):
    try:
        import debugpy

        _port = int(os.environ.get("SCISTACK_GUI_DEBUG_PORT", "5678"))
        debugpy.listen(("127.0.0.1", _port))
        logger.info(f"debugpy listening on 127.0.0.1:{_port} (attach from VS Code)")
        if os.environ.get("SCISTACK_GUI_DEBUG_WAIT"):
            logger.info(
                "SCISTACK_GUI_DEBUG_WAIT set — blocking until debugger attaches..."
            )
            debugpy.wait_for_client()
            logger.info("debugger attached")
    except Exception as e:
        logger.warning(f"failed to start debugpy listener: {e}")


def _send(obj: dict) -> None:
    """Write a JSON-RPC message to stdout (thread-safe with notify._lock)."""
    from scistack_gui.notify import _lock

    msg = json.dumps(obj)
    with _lock:
        sys.stdout.write(msg + "\n")
        sys.stdout.flush()


def _respond(req_id, result):
    """Send a JSON-RPC success response."""
    _send({"jsonrpc": "2.0", "id": req_id, "result": result})


def _respond_error(req_id, code: int, message: str):
    """Send a JSON-RPC error response."""
    _send({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}})


def _send_progress(message: str) -> None:
    """Emit a startup progress notification. Uses _send directly because
    notify.enable() has not been called yet during startup."""
    _send({"jsonrpc": "2.0", "method": "progress", "params": {"message": message}})


# ---------------------------------------------------------------------------
# Method dispatch table
# ---------------------------------------------------------------------------
# Every method is declared ONCE, as a row in one of the handler tables under
# ``api/`` (``api/handlers.py`` for the shape, ``api/tables.py`` for the
# union). The JSON-RPC methods and the lock policy below are built from
# those rows; the FastAPI routes are built from the same rows; the
# frontend's route map is checked against them (tests/test_api_handlers.py).
# Nothing about a method is written here.

from scistack_gui.api.handlers import rpc_methods, self_managed  # noqa: E402
from scistack_gui.api.tables import ALL_HANDLERS  # noqa: E402

METHODS = rpc_methods(ALL_HANDLERS)

#: Methods that acquire the DuckDB connection themselves, for as long as they
#: actually need it, instead of letting :func:`_handle_request` hold it across
#: the whole call — every row whose ``holds_db_lock`` is False.
#:
#: They exist for one measured reason: a plot resolve spends nearly all of
#: its time in pandas and matplotlib, with the database touched only while
#: the variable frames load. Holding the file lock for the rest of it blocked
#: MATLAB for the full duration — the 2026-09-11 log shows one 31-second hold
#: (12:27:39 acquire, 12:28:10 release) for work that needed the database for
#: well under a second of it.
#:
#: A method declared so that then forgets to wrap its own database access
#: will fail with a closed connection rather than silently working, because
#: this server closes the connection whenever the refcount hits zero. The
#: single-function run (``api/run.start_run``) is a different arrangement:
#: it acquires *in addition to* the blanket hold and hands the hold off to
#: its thread, so it is not in this set.
SELF_MANAGED_DB_METHODS = self_managed(ALL_HANDLERS)


# ---------------------------------------------------------------------------
# MATLAB support
# ---------------------------------------------------------------------------


def _find_scimatlab_matlab_dir() -> str | None:
    """Return the scimatlab MATLAB package directory, or None if not found.

    For editable installs (``pip install -e``), the dist-info's
    ``direct_url.json`` records the project root; the Python package (and its
    ``matlab/`` subdirectory) is found inside that tree via ``find_spec``.
    For regular wheel installs, ``matlab/`` sits directly inside the installed
    package directory. Both paths are handled by ``find_spec`` alone, but the
    editable check is kept explicit for clarity and robustness.

    The returned path must be on MATLAB's ``addpath`` so that the
    ``+scihist``, ``+scidb``, and ``+scifor`` package folders resolve.
    """
    import importlib.metadata
    import importlib.util
    import json
    from pathlib import Path

    # Editable installs: direct_url.json in the dist-info points to the
    # project root.  find_spec still resolves to the right location, but
    # we check explicitly so the intent is visible in logs.
    try:
        dist = importlib.metadata.distribution("scimatlab")
        direct_url_text = dist.read_text("direct_url.json")
        if direct_url_text:
            info = json.loads(direct_url_text)
            if info.get("dir_info", {}).get("editable", False):
                url = info.get("url", "")
                logger.info("_find_scimatlab_matlab_dir: editable install at %s", url)
    except Exception:
        pass  # dist not found or JSON parse error — fall through to find_spec

    # Works for both editable and regular installs: find_spec resolves to the
    # actual package __init__.py in either case.
    try:
        spec = importlib.util.find_spec("scimatlab")
        if spec and spec.origin:
            d = Path(spec.origin).parent / "matlab"
            if d.is_dir():
                logger.info("_find_scimatlab_matlab_dir: found %s", d)
                return str(d)
            logger.warning("_find_scimatlab_matlab_dir: matlab/ not found at %s", d)
    except Exception as exc:
        logger.warning("_find_scimatlab_matlab_dir: find_spec failed: %s", exc)

    return None



# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def _summarize_params(params: dict, max_len: int = 120) -> str:
    """Return a compact one-line summary of RPC params for logging."""
    if not params:
        return ""
    parts = []
    for k, v in params.items():
        if isinstance(v, str) and len(v) > 40:
            v = v[:37] + "..."
        elif isinstance(v, (list, dict)) and len(str(v)) > 40:
            v = f"{type(v).__name__}[{len(v)}]"
        parts.append(f"{k}={v}")
    s = ", ".join(parts)
    return s[:max_len] + "..." if len(s) > max_len else s


def _summarize_result(result, max_len: int = 160) -> "str | None":
    """Compact size summary of an RPC result, or ``None`` if there is
    nothing worth counting.

    Sized so a read RPC's answer is legible at INFO: ``list[3]`` for a bare
    list, ``nodes[3], edges[0]`` for a dict of collections. This exists
    because an empty list is a *silent* failure — the sidebar's PathInput
    list rendered empty while the canvas (fed by ``get_pipeline``, an
    independent RPC over the same registry) showed three nodes, and the log
    could not say whether ``get_path_inputs`` had even been called, let
    alone what it returned. Counts on the way out settle that in one line.
    """
    if isinstance(result, list):
        return f"list[{len(result)}]"
    if isinstance(result, dict):
        parts = [
            f"{k}[{len(v)}]"
            for k, v in result.items()
            if isinstance(v, (list, dict, set, tuple))
        ]
        if not parts:
            return None
        s = ", ".join(parts)
        return s[:max_len] + "..." if len(s) > max_len else s
    return None


# JSON-RPC error code for "the DuckDB file is open in another process".
# Distinct from the catch-all -32000 so the extension can treat it as the
# transient, self-explanatory condition it is rather than a crash.
ERR_DATABASE_LOCKED = -32010


def _handle_request(req: dict) -> None:
    """Process a single JSON-RPC request.

    **Every request must produce exactly one response frame.** This function
    is a per-request thread target (see the dispatch loop in :func:`main`),
    and the extension's ``pythonProcess.request()`` keeps a promise pending
    until a frame with the matching id arrives. An exception that escapes
    this function kills its thread silently and leaves that promise pending
    forever — the GUI hangs with no error anywhere.

    That is not hypothetical: ``acquire_db_connection()`` used to be called
    *outside* the try below, and it raises whenever MATLAB holds the DuckDB
    file lock (which the GUI deliberately allows — see ``db.py``'s
    connection-lifecycle note). Clicking Run on a MATLAB node with MATLAB
    attached therefore hung the whole GUI. Hence the acquire inside the try
    and the outer safety net: a response is emitted no matter what fails.
    """
    from scidb.log import Log

    from scistack_gui.db import (
        DatabaseLockedError,
        acquire_db_connection,
        release_db_connection,
    )

    req_id = req.get("id")
    method = req.get("method", "")
    params = req.get("params", {})

    handler = METHODS.get(method)
    if handler is None:
        if req_id is not None:
            _respond_error(req_id, -32601, f"Method not found: {method}")
        return

    summary = _summarize_params(params)
    Log.debug(f"RPC >> {method}({summary})")
    t0 = time.monotonic()

    # Exactly one frame per request: the safety net below must not answer a
    # request the normal path already answered, or the caller sees a stray
    # frame for an id it has already settled.
    responded = False

    def _answer(send) -> None:
        # `responded` flips only after the frame is actually out. _send
        # serialises before it writes, so a failed send wrote nothing — the
        # request is still unanswered and the fallback below must be free to
        # try again with a simpler payload.
        nonlocal responded
        if req_id is None or responded:
            return
        send()
        responded = True

    try:
        # A failed acquire must not be released — acquire_db_connection only
        # increments the refcount on success (see its docstring).
        acquired = False
        try:
            if method not in SELF_MANAGED_DB_METHODS:
                acquire_db_connection()
                acquired = True
            result = handler(params)
            elapsed_ms = (time.monotonic() - t0) * 1000
            # Read RPCs answer at INFO with what they actually returned; a
            # list that came back empty is otherwise invisible in the log
            # (see _summarize_result). Everything else stays at DEBUG so
            # writes and per-frame chatter don't flood the file.
            counts = (
                _summarize_result(result)
                if method.startswith(("get_", "list_"))
                else None
            )
            if counts is not None:
                Log.info(f"RPC << {method} -> {counts} ({elapsed_ms:.1f}ms)")
            else:
                Log.debug(f"RPC << {method} OK ({elapsed_ms:.1f}ms)")
            _answer(lambda: _respond(req_id, result))
        except DatabaseLockedError as locked:
            elapsed_ms = (time.monotonic() - t0) * 1000
            Log.error(f"RPC << {method} DB LOCKED ({elapsed_ms:.1f}ms): {locked}")
            logger.warning(
                "Database locked while handling %s (holder=%s pid=%s)",
                method,
                locked.holder,
                locked.pid,
            )
            _answer(
                lambda: _respond_error(req_id, ERR_DATABASE_LOCKED, str(locked))
            )
        except Exception as e:
            elapsed_ms = (time.monotonic() - t0) * 1000
            Log.error(f"RPC << {method} FAILED ({elapsed_ms:.1f}ms): {e}")
            logger.exception("Error handling %s", method)
            _answer(lambda: _respond_error(req_id, -32000, str(e)))
        finally:
            if acquired:
                release_db_connection()
    except BaseException as e:  # noqa: BLE001 — last line of defence, see docstring
        # Anything at all that got past the handlers above — a failure
        # inside _respond itself, or in release_db_connection. Losing the
        # response is strictly worse than any error we could report, so try
        # once more on a clean path (and only if nothing was sent yet).
        logger.exception("Unhandled failure dispatching %s", method)
        try:
            Log.error(f"RPC !! {method} NO RESPONSE: {e}")
        except Exception:
            pass
        try:
            _answer(
                lambda: _respond_error(
                    req_id, -32000, f"Internal dispatch failure: {e}"
                )
            )
        except Exception:
            logger.exception(
                "Could not send an error response for %s (id=%s) — the "
                "caller will time out",
                method,
                req_id,
            )


def main():
    t0 = time.monotonic()
    parser = argparse.ArgumentParser(prog="scistack-gui-server")
    parser.add_argument("--db", type=Path, required=True, help="Path to .duckdb file")
    parser.add_argument(
        "--module",
        "-m",
        type=Path,
        default=None,
        help="Path to pipeline .py file (single-file mode)",
    )
    parser.add_argument(
        "--project",
        "-p",
        type=Path,
        default=None,
        help="Path to pyproject.toml or directory containing one "
        "(project mode — reads [tool.scistack] config)",
    )
    parser.add_argument(
        "--schema-keys",
        type=str,
        default=None,
        help="Comma-separated schema keys; if provided and --db "
        "does not exist, a new database is created.",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=None,
        help="Directory to treat as the project root when no "
        "pyproject.toml/scistack.toml exists yet (the VS Code workspace "
        "folder). Determines where a new scistack.toml and entities file "
        "are written; without it the database's own directory is the last "
        "resort, which is usually a datasets folder.",
    )
    args = parser.parse_args()

    # This process drops the DuckDB lock between requests (see the dispatch
    # loop below and close_initial_connection). Declaring it lets db.py's
    # narrow-hold helper be a real acquire here and a no-op in the standalone
    # FastAPI process, where the connection is held for the life of the process
    # and releasing it would close the one every later request depends on.
    from scistack_gui.db import set_connection_policy

    set_connection_policy("per_request")

    if args.project_root is not None:
        from scistack_gui.config import set_project_root_hint

        set_project_root_hint(args.project_root)

    if args.module and args.project:
        print(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "error",
                    "params": {
                        "message": "--module and --project are mutually exclusive."
                    },
                }
            )
        )
        sys.exit(1)

    db_path = args.db.resolve()
    create_new = not db_path.exists()
    if create_new and not args.schema_keys:
        print(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "error",
                    "params": {"message": f"Database not found: {db_path}"},
                }
            )
        )
        sys.exit(1)

    # Open the log file BEFORE any discovery runs. configure_database()
    # attaches the same sink further down, but that is after the registry
    # scan below — which is the single most diagnostically valuable part of
    # startup (it decides which functions, variables and PathInputs exist,
    # and it is where load errors are produced). Everything it logged used
    # to reach stderr only, so scidb.log began mid-startup at
    # "configure_database:" and a question like "why is this PathInput
    # missing?" had no record to answer it. attach_log_file is idempotent,
    # so the later configure_database() call is a no-op.
    from scidb.log import attach_log_file

    attach_log_file(db_path)
    logger.info(
        "[startup] log file attached: db=%s create_new=%s — discovery follows",
        db_path,
        create_new,
    )

    # Make the project's declaration surfaces exist BEFORE the config is
    # read, exactly as bootstrap.open_or_create_project does for the browser
    # entry points -- this path duplicates that sequence inline and had
    # simply never been given this step, so creating a database from VS Code
    # in a folder that already held a scistack_entities.toml left the project
    # in folder-scan mode with no entities_file, and the file's Variables /
    # Parameters / PathInputs never reached the registry. Skipped in
    # single-file (--module) mode, which has no project root to initialize.
    if not args.module:
        from scistack_gui.services.project_init_service import ensure_project_files

        try:
            init = ensure_project_files(db_path, args.project)
            if init.created:
                logger.info("[startup] project init created: %s", init.created)
            for warning in init.warnings:
                logger.warning("[startup] project init: %s", warning)
        except Exception:
            logger.exception("[startup] project file initialization failed")

    # Import user code first (same order as __main__.py) so that
    # configure_database() can auto-register the user's variable classes.
    from scistack_gui import registry

    if args.project:
        # Project mode: load from [tool.scistack] in pyproject.toml
        from scistack_gui.config import load_config

        try:
            _send_progress("Loading project config...")
            config = load_config(args.project, db_path)
            result = registry.load_from_config(config)
            logger.info(
                "Project mode: %d functions, %d variables",
                len(result["functions"]),
                len(result["variables"]),
            )
            _send_progress(
                f"Loaded {len(result['functions'])} Python functions, "
                f"{len(result['variables'])} variables"
            )
            # Load MATLAB registry if MATLAB config is present.
            if config.has_matlab:
                from scistack_gui import matlab_registry

                _send_progress(
                    f"Loading MATLAB registry ({len(config.matlab_functions)} "
                    f"functions, {len(config.matlab_variables)} variables)..."
                )
                matlab_result = matlab_registry.load_from_config(config)
                logger.info(
                    "MATLAB: %d functions, %d variables",
                    len(matlab_result["matlab_functions"]),
                    len(matlab_result["matlab_variables"]),
                )
                _send_progress("MATLAB registry loaded")
        except (FileNotFoundError, ValueError) as e:
            print(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "method": "error",
                        "params": {"message": f"Config error: {e}"},
                    }
                )
            )
            sys.exit(1)
        except Exception as e:
            print(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "method": "error",
                        "params": {"message": f"Error loading project: {e}"},
                    }
                )
            )
            sys.exit(1)
    elif args.module:
        # Single-file mode (legacy)
        module_path = args.module.resolve()
        if not module_path.exists():
            print(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "method": "error",
                        "params": {"message": f"Module not found: {module_path}"},
                    }
                )
            )
            sys.exit(1)
        import importlib.util

        spec = importlib.util.spec_from_file_location("user_pipeline", module_path)
        user_mod = importlib.util.module_from_spec(spec)
        try:
            with registry._suppress_user_code_output():
                spec.loader.exec_module(user_mod)
        except Exception as e:
            print(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "method": "error",
                        "params": {"message": f"Error importing module: {e}"},
                    }
                )
            )
            sys.exit(1)
        registry.register_module(user_mod, module_path=module_path)
        logger.info("Loaded module: %s", module_path)
    else:
        # No --module/--project given: best-effort auto-discovery, either
        # from a pyproject.toml/scistack.toml found near the database, or
        # (more commonly, for a loose-scripts project) a folder scan of the
        # database's directory. Never fatal — an empty registry here is no
        # worse than today's default of not discovering anything at all.
        from scistack_gui.config import load_config

        try:
            _send_progress("Auto-discovering pipeline code...")
            config = load_config(None, db_path)
            result = registry.load_from_config(config)
            logger.info(
                "Auto-discovered: %d functions, %d variables",
                len(result["functions"]),
                len(result["variables"]),
            )
            _send_progress(
                f"Auto-discovered {len(result['functions'])} Python functions, "
                f"{len(result['variables'])} variables"
            )
            if config.has_matlab:
                from scistack_gui import matlab_registry

                matlab_result = matlab_registry.load_from_config(config)
                logger.info(
                    "MATLAB: %d functions, %d variables",
                    len(matlab_result["matlab_functions"]),
                    len(matlab_result["matlab_variables"]),
                )
                _send_progress(
                    f"MATLAB: {len(matlab_result['matlab_functions'])} functions, "
                    f"{len(matlab_result['matlab_variables'])} variables"
                )
        except Exception as e:
            logger.warning("Auto-discovery failed (%s); starting with an empty registry.", e)

    # Initialise the database (create if missing and schema keys supplied)
    from scistack_gui.db import create_db, init_db

    _send_progress("Opening database...")
    try:
        if create_new:
            schema_keys = [k.strip() for k in args.schema_keys.split(",") if k.strip()]
            db = create_db(db_path, schema_keys)
            logger.info("Created database: %s (schema_keys=%s)", db_path, schema_keys)
        else:
            db = init_db(db_path)
            logger.info("Opened database: %s", db_path)
    except Exception as e:
        print(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "error",
                    "params": {"message": f"Error opening database: {e}"},
                }
            )
        )
        sys.exit(1)

    # Restore any manually-declared builtin function references (e.g.
    # numpy.mean, a MATLAB builtin) from a previous session — they have no
    # file on disk to be rediscovered from otherwise.
    try:
        from scistack_gui.services.builtin_function_service import (
            replay_persisted_builtins,
        )

        replay_persisted_builtins(db)
    except Exception:
        logger.exception("Failed to restore builtin function references")

    # Bridge Python logging → scidb.log so that scihist/scistack_gui logger
    # calls appear in the unified log file.
    from scidb.log import Log

    Log.bridge_python_logging()

    # Enable JSON-RPC notifications on stdout
    from scistack_gui.notify import enable

    enable()

    # Phase 8: Stale lockfile detection on project open.
    # If pyproject.toml exists next to the db, check whether uv.lock is
    # out of date and silently sync if so. On failure, the error is
    # recorded in scistack_gui.startup; the frontend picks it up via the
    # next `get_info` call.
    from scistack_gui import startup as _startup

    if registry._config is not None:
        _startup.check_windows_config_paths(registry._config)
    _startup.check_lockfile_staleness(db_path.parent)

    # Signal readiness
    logger.info("Startup complete in %.2fs", time.monotonic() - t0)
    _send(
        {
            "jsonrpc": "2.0",
            "method": "ready",
            "params": {
                "db_name": db_path.name,
                "schema_keys": db.dataset_schema_keys,
            },
        }
    )

    # Release the DuckDB file lock now that startup is complete. It will be
    # reacquired automatically when the first request arrives. This allows
    # MATLAB (or any other process) to open the same database immediately.
    from scistack_gui.db import close_initial_connection

    close_initial_connection()
    logger.info("DB connection released after startup — MATLAB can now access the file")

    # Main request loop — read one JSON-RPC request per line from stdin
    logger.info("Server ready, waiting for requests on stdin...")
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError as e:
            logger.warning("Invalid JSON on stdin: %s", e)
            continue

        # Handle each request in a thread so long-running calls (like start_run)
        # don't block the main loop from reading the next request.
        threading.Thread(target=_handle_request, args=(req,), daemon=True).start()

    logger.info("stdin closed, shutting down.")


if __name__ == "__main__":
    main()
