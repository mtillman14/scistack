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
import time
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


def _send_progress(message: str) -> None:
    """Emit a startup progress notification. Uses _send directly because
    notify.enable() has not been called yet during startup."""
    _send({"jsonrpc": "2.0", "method": "progress",
           "params": {"message": message}})


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
    from scistack_gui import startup as _startup
    return {
        "db_name": get_db_path().name,
        "startup_errors": [e.to_dict() for e in _startup.get_startup_errors()],
    }


def _h_get_registry(params):
    from scistack_gui import registry
    from scistack_gui import matlab_registry
    from scidb import BaseVariable
    matlab_fns = matlab_registry.get_all_function_names()
    logger.info(
        "get_registry: %d python fns, %d matlab fns, %d vars",
        len(registry._functions), len(matlab_fns),
        len(BaseVariable._all_subclasses),
    )
    if matlab_fns:
        logger.info("matlab_functions: %s", matlab_fns)
    return {
        "functions": sorted(registry._functions.keys()),
        "variables": sorted(BaseVariable._all_subclasses.keys()),
        "matlab_functions": matlab_fns,
    }


def _h_get_function_params(params):
    from scistack_gui.api.pipeline import _fn_params_from_registry
    from scistack_gui import matlab_registry
    name = params["name"]
    # Check MATLAB registry first for MATLAB functions.
    if matlab_registry.is_matlab_function(name):
        info = matlab_registry.get_matlab_function(name)
        return {"params": info.params}
    return {"params": _fn_params_from_registry(name)}


def _h_get_function_source(params):
    """Return the source file path and line number for a registered function."""
    import inspect
    from scistack_gui import registry
    from scistack_gui import matlab_registry
    name = params["name"]
    # Check MATLAB registry first.
    if matlab_registry.is_matlab_function(name):
        info = matlab_registry.get_matlab_function(name)
        return {"ok": True, "file": str(info.file_path), "line": 1}
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


def _h_get_variables_list(params):
    from scidb import BaseVariable
    return [{"variable_name": name} for name in sorted(BaseVariable._all_subclasses.keys())]


def _h_start_run(params):
    import uuid
    from scistack_gui.db import get_db, acquire_db_connection, release_db_connection
    from scistack_gui.api.run import _run_in_thread, WhereFilterSpec

    run_id = params.get("run_id") or str(uuid.uuid4())[:8]
    function_name = params["function_name"]
    variants = params.get("variants", [])
    schema_filter = params.get("schema_filter")
    schema_level = params.get("schema_level")
    run_options = params.get("run_options")
    raw_where = params.get("where_filters")
    language = params.get("language", "python")
    where_filters = [WhereFilterSpec(**f) for f in raw_where] if raw_where else None
    db = get_db()

    # Surface the start_run request to the SciStack Output channel (via stderr).
    # This is the Python-side counterpart to the extension's outputChannel log.
    logger.info(
        "start_run[%s]: function=%s, language=%s, variants=%d, "
        "schema_filter=%s, schema_level=%s, run_options=%s, where_filters=%d",
        run_id, function_name, language, len(variants),
        list(schema_filter.keys()) if schema_filter else None,
        schema_level, run_options,
        len(where_filters) if where_filters else 0,
    )

    # Hold the DB connection open for the full duration of the run (the
    # _handle_request wrapper will release its reference when this handler
    # returns, but we acquire an extra one here so the count stays ≥ 1 until
    # the run thread finishes).
    acquire_db_connection()

    def _run_wrapper():
        try:
            _run_in_thread(run_id, function_name, variants, db, schema_filter,
                           schema_level, run_options, where_filters)
        finally:
            release_db_connection()

    thread = threading.Thread(target=_run_wrapper, daemon=True)
    thread.start()
    return {"run_id": run_id}


def _h_cancel_run(params):
    """Cooperatively cancel an in-flight run."""
    from scistack_gui.api.run import cancel_run
    run_id = params["run_id"]
    logger.info("cancel_run[%s]: cooperative cancel requested", run_id)
    return cancel_run(run_id)


def _h_force_cancel_run(params):
    """Force-cancel an in-flight run by injecting KeyboardInterrupt."""
    from scistack_gui.api.run import force_cancel_run
    run_id = params["run_id"]
    logger.info("force_cancel_run[%s]: force cancel requested", run_id)
    return force_cancel_run(run_id)


def _h_refresh_module(params):
    # NOTE: The VS Code extension does NOT call this RPC. It uses a full
    # subprocess restart ("SciStack: Restart Python Process") instead, which
    # also picks up edits to scistack_gui server code. This handler is kept
    # for other JSON-RPC clients and internal callers (e.g. variable creation
    # in api/variables.py) that only need to re-import the user module.
    from scistack_gui import registry
    from scistack_gui import matlab_registry
    from scistack_gui.notify import notify
    try:
        # Project mode refreshes all sources; single-file mode refreshes one.
        if registry._config is not None:
            result = registry.refresh_all()
        else:
            result = registry.refresh_module()
        # Also refresh MATLAB registry if configured.
        matlab_registry.refresh_all()
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
    from scistack_gui import matlab_registry
    from scistack_gui.notify import notify

    name = params.get("name", "").strip()
    docstring = params.get("docstring")
    language = params.get("language", "python")

    if not name.isidentifier() or keyword.iskeyword(name):
        return {"ok": False, "error": f"'{name}' is not a valid class name."}
    if name.startswith("_"):
        return {"ok": False, "error": "Variable names must not start with an underscore."}
    if not name[0].isupper():
        return {"ok": False, "error": "Variable names should start with an uppercase letter."}
    if name in BaseVariable._all_subclasses:
        return {"ok": False, "error": f"A variable named '{name}' already exists."}

    # MATLAB variable creation: write a .m classdef file.
    if language == "matlab":
        return _create_matlab_variable(name, docstring, matlab_registry, notify)

    # Python variable creation (original path).
    target_file: Path | None = None
    if registry._config is not None and registry._config.variable_file is not None:
        target_file = registry._config.variable_file
    elif registry._module_path is not None:
        target_file = registry._module_path
    if target_file is None:
        # No Python target available — fall back to MATLAB if configured.
        if matlab_registry.has_matlab_config() and matlab_registry._config is not None and matlab_registry._config.matlab_variable_dir is not None:
            return _create_matlab_variable(name, docstring, matlab_registry, notify)
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


def _create_matlab_variable(name, docstring, matlab_registry, notify):
    """Create a MATLAB classdef variable file and register the surrogate."""
    if matlab_registry._config is None or matlab_registry._config.matlab_variable_dir is None:
        return {"ok": False, "error": "No matlab.variable_dir configured in [tool.scistack.matlab]."}

    target_dir = matlab_registry._config.matlab_variable_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / f"{name}.m"

    if target_file.exists():
        return {"ok": False, "error": f"File already exists: {target_file}"}

    # Build the .m classdef content.
    m_lines = [f"classdef {name} < scidb.BaseVariable"]
    if docstring:
        m_lines.append(f"    % {docstring}")
    m_lines.append("end")
    m_lines.append("")  # trailing newline

    try:
        target_file.write_text("\n".join(m_lines), encoding="utf-8")
    except OSError as e:
        return {"ok": False, "error": f"Failed to write .m file: {e}"}

    # Create the Python surrogate and refresh the MATLAB registry.
    try:
        from sci_matlab.bridge import register_matlab_variable
        register_matlab_variable(name)
        matlab_registry.refresh_all()
    except Exception as e:
        return {"ok": False, "error": f"File written but registration failed: {e}"}

    notify("dag_updated", {})
    return {"ok": True, "name": name}


# ---------------------------------------------------------------------------
# Stale lockfile handling (Phase 8)
# ---------------------------------------------------------------------------
# The actual check lives in :mod:`scistack_gui.startup` so both FastAPI and
# JSON-RPC entry points share one implementation. Errors are stored in the
# startup module and surfaced to the frontend via the ``get_info`` handler,
# which the React app polls once on mount — that's a more reliable delivery
# channel than firing a notification at a webview that may not be listening
# yet.


# ---------------------------------------------------------------------------
# Project config panel (Phase 6)
# ---------------------------------------------------------------------------

def _h_get_project_code(params):
    from scistack_gui.api.project import get_project_code
    return get_project_code()


def _h_get_project_libraries(params):
    from scistack_gui.api.project import get_project_libraries
    return get_project_libraries()


def _h_refresh_project(params):
    from scistack_gui.api.project import refresh_project
    return refresh_project()


# ---------------------------------------------------------------------------
# Index & library management (Phase 7)
# ---------------------------------------------------------------------------

def _h_get_indexes(params):
    from scistack_gui.api.indexes import list_indexes
    return list_indexes()


def _h_search_index_packages(params):
    from scistack_gui.api.indexes import search_index_packages
    return search_index_packages(params.get("name", ""), q=params.get("q", ""))


def _h_add_library(params):
    from scistack_gui.api.indexes import add_library
    return add_library(params)


def _h_remove_library(params):
    from scistack_gui.api.indexes import remove_library
    return remove_library(params.get("name", ""))


# ---------------------------------------------------------------------------
# MATLAB support
# ---------------------------------------------------------------------------

def _find_sci_matlab_matlab_dir() -> str | None:
    """Return the sci-matlab MATLAB package directory, or None if not found.

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
        dist = importlib.metadata.distribution("sci_matlab")
        direct_url_text = dist.read_text("direct_url.json")
        if direct_url_text:
            info = json.loads(direct_url_text)
            if info.get("dir_info", {}).get("editable", False):
                url = info.get("url", "")
                logger.info(
                    "_find_sci_matlab_matlab_dir: editable install at %s", url
                )
    except Exception:
        pass  # dist not found or JSON parse error — fall through to find_spec

    # Works for both editable and regular installs: find_spec resolves to the
    # actual package __init__.py in either case.
    try:
        spec = importlib.util.find_spec("sci_matlab")
        if spec and spec.origin:
            d = Path(spec.origin).parent / "matlab"
            if d.is_dir():
                logger.info("_find_sci_matlab_matlab_dir: found %s", d)
                return str(d)
            logger.warning(
                "_find_sci_matlab_matlab_dir: matlab/ not found at %s", d
            )
    except Exception as exc:
        logger.warning("_find_sci_matlab_matlab_dir: find_spec failed: %s", exc)

    return None


def _h_generate_matlab_command(params):
    """Generate a ready-to-paste MATLAB command for a pipeline function."""
    from scistack_gui.api.matlab_command import generate_matlab_command
    from scistack_gui.db import get_db, get_db_path
    from scistack_gui import matlab_registry

    db = get_db()
    db_path = str(get_db_path())

    # Collect addpath directories from MATLAB config.
    addpath_dirs: list[str] = []
    if matlab_registry._config is not None:
        addpath_dirs = [str(p) for p in matlab_registry._config.matlab_addpath]

    # Prepend the sci-matlab MATLAB package directory so that +scihist,
    # +scidb, and +scifor resolve regardless of what the user configured.
    sci_matlab_dir = _find_sci_matlab_matlab_dir()
    if sci_matlab_dir:
        addpath_dirs = [sci_matlab_dir] + addpath_dirs
        logger.info("generate_matlab_command: prepended sci-matlab dir: %s", sci_matlab_dir)
    else:
        logger.warning(
            "generate_matlab_command: sci-matlab MATLAB directory not found; "
            "scihist.* / scidb.* may be unavailable in MATLAB"
        )

    # Resolve variants for this function from DB history.
    function_name = params["function_name"]
    all_variants = db.list_pipeline_variants()
    fn_variants = [v for v in all_variants if v["function_name"] == function_name]

    # Collect PathInput param mappings: param_name → {template, root_folder}.
    # Source 1: DB variants — PathInput values are stored in input_types as JSON.
    from scistack_gui.api.pipeline import _parse_path_input
    from scistack_gui import layout as layout_store
    path_input_params: dict[str, dict] = {}
    for v in fn_variants:
        for param_name, type_val in (v.get("input_types") or {}).items():
            pi = _parse_path_input(str(type_val))
            if pi is not None:
                path_input_params[param_name] = pi

    # Source 2: layout manual edges — for functions not yet in the DB.
    # Edge: source=pathInput__<name>, target=fn__<funcname>[__hash],
    #        targetHandle=in__<param>
    saved_pis = {pi["name"]: pi for pi in layout_store.read_all_path_input_names()}
    for edge in layout_store.read_manual_edges():
        src = edge.get("source", "")
        tgt = edge.get("target", "")
        th = edge.get("targetHandle", "")
        if not (src.startswith("pathInput__") and th.startswith("in__")):
            continue
        # Match target to this function by name (strip "fn__" prefix and hash suffix).
        tgt_parts = tgt.split("__")
        if len(tgt_parts) < 2 or tgt_parts[0] != "fn":
            continue
        tgt_fn_name = tgt_parts[1]
        if tgt_fn_name != function_name:
            continue
        pi_name = src.split("__")[1] if len(src.split("__")) >= 2 else src[len("pathInput__"):]
        param_name = th[len("in__"):]
        if pi_name in saved_pis:
            # Prefer the saved template (most up-to-date) over whatever is in the variant.
            path_input_params[param_name] = {
                "template": saved_pis[pi_name].get("template", ""),
                "root_folder": saved_pis[pi_name].get("root_folder"),
            }

    # Overlay saved templates onto DB-variant-derived PathInput params.
    for param_name, pi in path_input_params.items():
        # Find which saved PathInput name maps to this param via manual edges.
        for edge in layout_store.read_manual_edges():
            th = edge.get("targetHandle", "")
            if th == f"in__{param_name}":
                src = edge.get("source", "")
                pi_name = src.split("__")[1] if len(src.split("__")) >= 2 else ""
                if pi_name in saved_pis and saved_pis[pi_name].get("template"):
                    pi["template"] = saved_pis[pi_name]["template"]
                    pi["root_folder"] = saved_pis[pi_name].get("root_folder")

    return {
        "command": generate_matlab_command(
            function_name=function_name,
            db_path=db_path,
            schema_keys=list(db.dataset_schema_keys),
            variants=fn_variants if fn_variants else params.get("variants"),
            schema_filter=params.get("schema_filter"),
            schema_level=params.get("schema_level"),
            addpath_dirs=addpath_dirs if addpath_dirs else None,
            python_executable=sys.executable,
            path_inputs=path_input_params if path_input_params else None,
        )
    }


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
    "get_variables_list": _h_get_variables_list,
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
    "cancel_run": _h_cancel_run,
    "force_cancel_run": _h_force_cancel_run,
    "refresh_module": _h_refresh_module,
    "create_variable": _h_create_variable,
    "get_project_code": _h_get_project_code,
    "get_project_libraries": _h_get_project_libraries,
    "refresh_project": _h_refresh_project,
    "get_indexes": _h_get_indexes,
    "search_index_packages": _h_search_index_packages,
    "add_library": _h_add_library,
    "remove_library": _h_remove_library,
    "generate_matlab_command": _h_generate_matlab_command,
}


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def _handle_request(req: dict) -> None:
    """Process a single JSON-RPC request."""
    from scistack_gui.db import acquire_db_connection, release_db_connection

    req_id = req.get("id")
    method = req.get("method", "")
    params = req.get("params", {})

    handler = METHODS.get(method)
    if handler is None:
        if req_id is not None:
            _respond_error(req_id, -32601, f"Method not found: {method}")
        return

    acquire_db_connection()
    try:
        result = handler(params)
        if req_id is not None:
            _respond(req_id, result)
    except Exception as e:
        logger.exception("Error handling %s", method)
        if req_id is not None:
            _respond_error(req_id, -32000, str(e))
    finally:
        release_db_connection()


def main():
    t0 = time.monotonic()
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
            _send_progress("Loading project config...")
            config = load_config(args.project, db_path)
            result = registry.load_from_config(config)
            logger.info(
                "Project mode: %d functions, %d variables",
                len(result["functions"]), len(result["variables"]),
            )
            _send_progress(
                f"Loaded {len(result['functions'])} Python functions, "
                f"{len(result['variables'])} variables"
            )
            # Load MATLAB registry if MATLAB config is present.
            if config.matlab_functions or config.matlab_variables:
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
        print(json.dumps({
            "jsonrpc": "2.0", "method": "error",
            "params": {"message": f"Error opening database: {e}"}
        }))
        sys.exit(1)

    # Enable JSON-RPC notifications on stdout
    from scistack_gui.notify import enable
    enable()

    # Phase 8: Stale lockfile detection on project open.
    # If pyproject.toml exists next to the db, check whether uv.lock is
    # out of date and silently sync if so. On failure, the error is
    # recorded in scistack_gui.startup; the frontend picks it up via the
    # next /api/info call (see _h_get_info).
    from scistack_gui import startup as _startup
    _startup.check_lockfile_staleness(db_path.parent)

    # Signal readiness
    logger.info("Startup complete in %.2fs", time.monotonic() - t0)
    _send({
        "jsonrpc": "2.0",
        "method": "ready",
        "params": {
            "db_name": db_path.name,
            "schema_keys": db.dataset_schema_keys,
        },
    })

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
        threading.Thread(
            target=_handle_request, args=(req,), daemon=True
        ).start()

    logger.info("stdin closed, shutting down.")


if __name__ == "__main__":
    main()
