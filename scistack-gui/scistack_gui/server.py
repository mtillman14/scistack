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
import os
import sys
import threading
import logging
from pathlib import Path

# Configure logging to stderr so it doesn't corrupt the JSON-RPC stream.
logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="[scistack] %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

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
            logger.info("SCISTACK_GUI_DEBUG_WAIT set — blocking until debugger attaches...")
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


# ---------------------------------------------------------------------------
# Method handlers
# ---------------------------------------------------------------------------
# Each handler takes (params: dict) and returns a JSON-serialisable result.
# They call into the same business logic as the FastAPI route handlers.

def _h_get_pipeline(params):
    from scistack_gui.api.pipeline import _build_graph
    from scistack_gui.db import get_db
    return _build_graph(get_db())


def _h_get_layout(params):
    from scistack_gui import layout as layout_store
    return layout_store.read_layout()


def _h_get_schema(params):
    from scistack_gui.db import get_db
    db = get_db()
    keys = db.dataset_schema_keys
    values = {key: db.distinct_schema_values(key) for key in keys}
    return {"keys": keys, "values": values}


def _h_get_info(params):
    from scistack_gui.db import get_db_path
    return {"db_name": get_db_path().name}


def _h_get_registry(params):
    from scistack_gui import registry
    from scidb import BaseVariable
    return {
        "functions": sorted(registry._functions.keys()),
        "variables": sorted(BaseVariable._all_subclasses.keys()),
    }


def _h_get_function_params(params):
    from scistack_gui.api.pipeline import _fn_params_from_registry
    return {"params": _fn_params_from_registry(params["name"])}


def _h_get_function_source(params):
    """Return the source file path and line number for a registered function."""
    import inspect
    from scistack_gui import registry
    name = params["name"]
    fn = registry._functions.get(name)
    if fn is None:
        return {"ok": False, "error": f"Function '{name}' is not registered (pass --module at startup)."}
    try:
        file = inspect.getsourcefile(fn) or inspect.getfile(fn)
        _, line = inspect.getsourcelines(fn)
    except (TypeError, OSError) as e:
        return {"ok": False, "error": f"Could not locate source for '{name}': {e}"}
    return {"ok": True, "file": file, "line": line}


def _h_get_variable_records(params):
    from scistack_gui.db import get_db
    # Reuse the logic from api/variables.py but without FastAPI dependencies.
    from scistack_gui.api.variables import get_variable_records as _get_var_records
    # The FastAPI handler uses Depends(get_db), we pass it directly.
    return _get_var_records(params["name"], get_db())


def _h_get_constants(params):
    from scistack_gui import layout as layout_store
    return layout_store.read_all_constant_names()


def _h_get_path_inputs(params):
    from scistack_gui import layout as layout_store
    return layout_store.read_all_path_input_names()


def _h_put_layout(params):
    from scistack_gui import layout as layout_store
    node_id = params["node_id"]
    x, y = params["x"], params["y"]
    node_type = params.get("node_type")
    label = params.get("label")
    if node_type and label:
        layout_store.write_manual_node(node_id, x, y, node_type, label)
    else:
        layout_store.write_node_position(node_id, x, y)
    return {"ok": True}


def _h_delete_layout(params):
    from scistack_gui import layout as layout_store
    layout_store.delete_node(params["node_id"])
    return {"ok": True}


def _h_put_edge(params):
    from scistack_gui import layout as layout_store
    layout_store.write_manual_edge({
        "id": params["edge_id"],
        "source": params["source"],
        "target": params["target"],
        "sourceHandle": params.get("source_handle"),
        "targetHandle": params.get("target_handle"),
    })
    return {"ok": True}


def _h_delete_edge(params):
    from scistack_gui import layout as layout_store
    layout_store.delete_manual_edge(params["edge_id"])
    return {"ok": True}


def _h_put_pending_constant(params):
    from scistack_gui import layout as layout_store
    from scistack_gui.notify import notify
    layout_store.add_pending_constant(params["name"], params["value"])
    notify("dag_updated", {})
    return {"ok": True}


def _h_delete_pending_constant(params):
    from scistack_gui import layout as layout_store
    from scistack_gui.notify import notify
    layout_store.remove_pending_constant(params["name"], params["value"])
    notify("dag_updated", {})
    return {"ok": True}


def _h_create_constant(params):
    from scistack_gui import layout as layout_store
    layout_store.write_constant(params["name"])
    return {"ok": True}


def _h_delete_constant(params):
    from scistack_gui import layout as layout_store
    layout_store.delete_constant(params["name"])
    return {"ok": True}


def _h_create_path_input(params):
    from scistack_gui import layout as layout_store
    layout_store.write_path_input(
        params["name"], params.get("template", ""), params.get("root_folder"))
    return {"ok": True}


def _h_update_path_input(params):
    from scistack_gui import layout as layout_store
    layout_store.write_path_input(
        params["name"], params.get("template", ""), params.get("root_folder"))
    return {"ok": True}


def _h_delete_path_input(params):
    from scistack_gui import layout as layout_store
    layout_store.delete_path_input(params["name"])
    return {"ok": True}


def _h_put_node_config(params):
    from scistack_gui.db import get_db
    from scistack_gui import pipeline_store
    node_id = params["node_id"]
    config = params.get("config", {})
    pipeline_store.update_node_config(get_db(), node_id, config)
    return {"ok": True}


def _h_start_run(params):
    import uuid
    from scistack_gui.db import get_db
    from scistack_gui.api.run import _run_in_thread

    run_id = params.get("run_id") or str(uuid.uuid4())[:8]
    function_name = params["function_name"]
    variants = params.get("variants", [])
    schema_filter = params.get("schema_filter")
    schema_level = params.get("schema_level")
    run_options = params.get("run_options")
    db = get_db()

    thread = threading.Thread(
        target=_run_in_thread,
        args=(run_id, function_name, variants, db, schema_filter, schema_level, run_options),
        daemon=True,
    )
    thread.start()
    return {"run_id": run_id}


def _h_refresh_module(params):
    # NOTE: The VS Code extension does NOT call this RPC. It uses a full
    # subprocess restart ("SciStack: Restart Python Process") instead, which
    # also picks up edits to scistack_gui server code. This handler is kept
    # for other JSON-RPC clients and internal callers (e.g. variable creation
    # in api/variables.py) that only need to re-import the user module.
    from scistack_gui import registry
    from scistack_gui.notify import notify
    try:
        # Project mode refreshes all sources; single-file mode refreshes one.
        if registry._config is not None:
            result = registry.refresh_all()
        else:
            result = registry.refresh_module()
    except RuntimeError as e:
        return {"ok": False, "error": str(e)}
    except Exception as e:
        logger.exception("Failed to refresh module")
        return {"ok": False, "error": f"Import error: {e}"}
    notify("dag_updated", {})
    return {"ok": True, **result}


def _h_create_variable(params):
    import keyword
    from scidb import BaseVariable
    from scistack_gui import registry
    from scistack_gui.notify import notify

    name = params.get("name", "").strip()
    docstring = params.get("docstring")

    if not name.isidentifier() or keyword.iskeyword(name):
        return {"ok": False, "error": f"'{name}' is not a valid Python class name."}
    if name.startswith("_"):
        return {"ok": False, "error": "Variable names must not start with an underscore."}
    if not name[0].isupper():
        return {"ok": False, "error": "Variable names should start with an uppercase letter."}
    if name in BaseVariable._all_subclasses:
        return {"ok": False, "error": f"A variable named '{name}' already exists."}

    # Determine the target file for the new class definition.
    target_file: Path | None = None
    if registry._config is not None and registry._config.variable_file is not None:
        target_file = registry._config.variable_file
    elif registry._module_path is not None:
        target_file = registry._module_path
    if target_file is None:
        return {"ok": False, "error": "No module file was loaded at startup."}

    lines = ["\n"]
    if docstring:
        escaped = docstring.replace('"""', '\\"\\"\\"')
        lines.append(f'class {name}(BaseVariable):\n    """{escaped}"""\n    pass\n')
    else:
        lines.append(f"class {name}(BaseVariable):\n    pass\n")

    try:
        with open(target_file, "a") as f:
            f.writelines(lines)
    except OSError as e:
        return {"ok": False, "error": f"Failed to write to module file: {e}"}

    try:
        if registry._config is not None:
            registry.refresh_all()
        else:
            registry.refresh_module()
    except Exception as e:
        return {"ok": False, "error": f"Class was written but refresh failed: {e}"}

    notify("dag_updated", {})
    return {"ok": True, "name": name}


# ---------------------------------------------------------------------------
# Method dispatch table
# ---------------------------------------------------------------------------

METHODS = {
    "get_pipeline": _h_get_pipeline,
    "get_layout": _h_get_layout,
    "get_schema": _h_get_schema,
    "get_info": _h_get_info,
    "get_registry": _h_get_registry,
    "get_function_params": _h_get_function_params,
    "get_function_source": _h_get_function_source,
    "get_variable_records": _h_get_variable_records,
    "get_constants": _h_get_constants,
    "get_path_inputs": _h_get_path_inputs,
    "put_layout": _h_put_layout,
    "put_node_config": _h_put_node_config,
    "delete_layout": _h_delete_layout,
    "put_edge": _h_put_edge,
    "delete_edge": _h_delete_edge,
    "put_pending_constant": _h_put_pending_constant,
    "delete_pending_constant": _h_delete_pending_constant,
    "create_constant": _h_create_constant,
    "delete_constant": _h_delete_constant,
    "create_path_input": _h_create_path_input,
    "update_path_input": _h_update_path_input,
    "delete_path_input": _h_delete_path_input,
    "start_run": _h_start_run,
    "refresh_module": _h_refresh_module,
    "create_variable": _h_create_variable,
}


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def _handle_request(req: dict) -> None:
    """Process a single JSON-RPC request."""
    req_id = req.get("id")
    method = req.get("method", "")
    params = req.get("params", {})

    handler = METHODS.get(method)
    if handler is None:
        if req_id is not None:
            _respond_error(req_id, -32601, f"Method not found: {method}")
        return

    try:
        result = handler(params)
        if req_id is not None:
            _respond(req_id, result)
    except Exception as e:
        logger.exception("Error handling %s", method)
        if req_id is not None:
            _respond_error(req_id, -32000, str(e))


def main():
    parser = argparse.ArgumentParser(prog="scistack-gui-server")
    parser.add_argument("--db", type=Path, required=True, help="Path to .duckdb file")
    parser.add_argument("--module", "-m", type=Path, default=None,
                        help="Path to pipeline .py file (single-file mode)")
    parser.add_argument("--project", "-p", type=Path, default=None,
                        help="Path to pyproject.toml or directory containing one "
                             "(project mode — reads [tool.scistack] config)")
    parser.add_argument("--schema-keys", type=str, default=None,
                        help="Comma-separated schema keys; if provided and --db "
                             "does not exist, a new database is created.")
    args = parser.parse_args()

    if args.module and args.project:
        print(json.dumps({
            "jsonrpc": "2.0", "method": "error",
            "params": {"message": "--module and --project are mutually exclusive."}
        }))
        sys.exit(1)

    db_path = args.db.resolve()
    create_new = not db_path.exists()
    if create_new and not args.schema_keys:
        print(json.dumps({
            "jsonrpc": "2.0", "method": "error",
            "params": {"message": f"Database not found: {db_path}"}
        }))
        sys.exit(1)

    # Import user code first (same order as __main__.py) so that
    # configure_database() can auto-register the user's variable classes.
    from scistack_gui import registry

    if args.project:
        # Project mode: load from [tool.scistack] in pyproject.toml
        from scistack_gui.config import load_config
        try:
            config = load_config(args.project, db_path)
            result = registry.load_from_config(config)
            logger.info(
                "Project mode: %d functions, %d variables",
                len(result["functions"]), len(result["variables"]),
            )
        except (FileNotFoundError, ValueError) as e:
            print(json.dumps({
                "jsonrpc": "2.0", "method": "error",
                "params": {"message": f"Config error: {e}"}
            }))
            sys.exit(1)
        except Exception as e:
            print(json.dumps({
                "jsonrpc": "2.0", "method": "error",
                "params": {"message": f"Error loading project: {e}"}
            }))
            sys.exit(1)
    elif args.module:
        # Single-file mode (legacy)
        module_path = args.module.resolve()
        if not module_path.exists():
            print(json.dumps({
                "jsonrpc": "2.0", "method": "error",
                "params": {"message": f"Module not found: {module_path}"}
            }))
            sys.exit(1)
        import importlib.util
        spec = importlib.util.spec_from_file_location("user_pipeline", module_path)
        user_mod = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(user_mod)
        except Exception as e:
            print(json.dumps({
                "jsonrpc": "2.0", "method": "error",
                "params": {"message": f"Error importing module: {e}"}
            }))
            sys.exit(1)
        registry.register_module(user_mod, module_path=module_path)
        logger.info("Loaded module: %s", module_path)

    # Initialise the database (create if missing and schema keys supplied)
    from scistack_gui.db import init_db, create_db
    try:
        if create_new:
            schema_keys = [k.strip() for k in args.schema_keys.split(",") if k.strip()]
            db = create_db(db_path, schema_keys)
            logger.info("Created database: %s (schema_keys=%s)", db_path, schema_keys)
        else:
            db = init_db(db_path)
            logger.info("Opened database: %s", db_path)
    except Exception as e:
        print(json.dumps({
            "jsonrpc": "2.0", "method": "error",
            "params": {"message": f"Error opening database: {e}"}
        }))
        sys.exit(1)

    # Enable JSON-RPC notifications on stdout
    from scistack_gui.notify import enable
    enable()

    # Signal readiness
    _send({
        "jsonrpc": "2.0",
        "method": "ready",
        "params": {
            "db_name": db_path.name,
            "schema_keys": db.dataset_schema_keys,
        },
    })

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
        threading.Thread(
            target=_handle_request, args=(req,), daemon=True
        ).start()

    logger.info("stdin closed, shutting down.")


if __name__ == "__main__":
    main()
