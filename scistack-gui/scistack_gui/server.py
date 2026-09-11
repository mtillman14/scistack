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
# Method handlers
# ---------------------------------------------------------------------------
# Each handler takes (params: dict) and returns a JSON-serialisable result.
# They call into the same business logic as the FastAPI route handlers.


def _h_get_pipeline(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    return get_pipeline_graph(get_db(), params.get("pipeline_id", "main"))


def _h_get_layout(params):
    from scistack_gui.services.layout_service import get_layout

    return get_layout(params.get("pipeline_id", "main"))


# --- Nested-pipeline scopes (plan-gui-nested-pipelines.md Part A) ---
# Store-level ValueErrors propagate; the dispatch loop's error mapping
# reports them as JSON-RPC errors with the store's message.


def _h_list_pipelines(params):
    from scistack_gui.services.scope_service import list_pipelines

    return list_pipelines()


def _h_create_pipeline(params):
    from scistack_gui.services.scope_service import create_pipeline

    return create_pipeline(params["name"])


def _h_rename_pipeline(params):
    from scistack_gui.services.scope_service import rename_pipeline

    return rename_pipeline(params["pipeline_id"], params["name"])


def _h_delete_pipeline(params):
    from scistack_gui.services.scope_service import hide_pipeline

    return hide_pipeline(params["pipeline_id"])


def _h_unhide_pipeline(params):
    from scistack_gui.services.scope_service import unhide_pipeline

    return unhide_pipeline(params["pipeline_id"])


def _h_get_hidden_pipelines(params):
    from scistack_gui.services.scope_service import list_hidden_pipelines

    return list_hidden_pipelines()


def _h_list_hypotheses(params):
    from scistack_gui.services.scope_service import list_hypotheses

    return list_hypotheses()


def _h_create_hypothesis(params):
    from scistack_gui.services.scope_service import create_hypothesis

    return create_hypothesis(params["name"])


def _h_update_hypothesis(params):
    from scistack_gui.services.scope_service import update_hypothesis

    return update_hypothesis(
        params["pipeline_id"],
        research_question=params.get("research_question"),
        hypothesis_statement=params.get("hypothesis_statement"),
        evidence_for=params.get("evidence_for"),
        evidence_against=params.get("evidence_against"),
    )


def _h_delete_hypothesis(params):
    from scistack_gui.services.scope_service import hide_hypothesis

    return hide_hypothesis(params["pipeline_id"])


def _h_get_pipeline_interface(params):
    from scistack_gui.services.scope_service import pipeline_interface

    return pipeline_interface(params["pipeline_id"])


def _h_get_hidden_ports(params):
    from scistack_gui.services.scope_service import get_hidden_ports

    return get_hidden_ports(params["pipeline_id"])


def _h_hide_port(params):
    from scistack_gui.services.scope_service import hide_port

    return hide_port(params["pipeline_id"], params["direction"], params["var_type"])


def _h_unhide_port(params):
    from scistack_gui.services.scope_service import unhide_port

    return unhide_port(params["pipeline_id"], params["direction"], params["var_type"])


def _h_extract_to_submodule(params):
    from scistack_gui.services.scope_service import extract_to_submodule

    return extract_to_submodule(
        params["pipeline_id"], params["node_ids"], params["name"]
    )


def _h_duplicate_pipeline(params):
    from scistack_gui.services.scope_service import duplicate_pipeline

    return duplicate_pipeline(params["pipeline_id"], params["name"])


def _h_duplicate_hypothesis(params):
    from scistack_gui.services.scope_service import duplicate_hypothesis

    return duplicate_hypothesis(params["pipeline_id"], params["name"])


def _h_export_pipeline(params):
    from scistack_gui.services.scope_service import export_pipeline

    return export_pipeline(params["pipeline_id"])


def _h_import_pipeline(params):
    from scistack_gui.services.scope_service import import_pipeline

    return import_pipeline(params["document"])


def _h_export_pipeline_code(params):
    from scistack_gui.services.scope_service import export_pipeline_code

    return export_pipeline_code(params["pipeline_id"])


def _h_paste_nodes(params):
    from scistack_gui.services.scope_service import paste_nodes

    return paste_nodes(
        params["source_pipeline_id"],
        params["node_ids"],
        params["pipeline_id"],
        params.get("x", 0.0),
        params.get("y", 0.0),
    )


def _h_add_pipeline_use(params):
    from scistack_gui.services.scope_service import add_pipeline_use

    return add_pipeline_use(
        params["parent_pipeline_id"],
        params["child_pipeline_id"],
        params.get("binding"),
        params.get("x", 0.0),
        params.get("y", 0.0),
    )


def _h_update_use_binding(params):
    from scistack_gui.services.scope_service import update_use_binding

    return update_use_binding(params["use_id"], params["binding"])


def _h_remove_pipeline_use(params):
    from scistack_gui.services.scope_service import remove_pipeline_use

    return remove_pipeline_use(params["use_id"])


def _h_get_pipeline_plan(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.execution_service import plan_pipeline

    return plan_pipeline(get_db(), params["pipeline_id"], params.get("target", ""))


def _h_start_pipeline_run(params):
    from scistack_gui.api.run import start_pipeline_run

    # JSON-RPC callers go through the VS Code extension's dagPanel.ts — a
    # privileged host that can itself generate + dispatch a MATLAB script
    # to the MathWorks terminal (Stage 2). HTTP callers (api/scopes.py, no
    # such host) get the standalone sidecar instead (Stage 3) — see
    # start_pipeline_run's docstring.
    return start_pipeline_run(
        params["pipeline_id"],
        params.get("mode", "all"),
        params.get("target", ""),
        params.get("finalized"),
        params.get("skip_computed", True),
        params.get("run_id"),
        host_can_dispatch_matlab=True,
    )


def _h_get_endpoint_artifacts(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.endpoint_service import endpoint_artifacts

    return endpoint_artifacts(get_db(), params["fn_name"])


def _h_write_report(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.endpoint_service import write_report

    return write_report(get_db())


def _h_get_schema(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.pipeline_service import get_schema

    return get_schema(get_db())


def _h_get_info(params):
    from scistack_gui.services.pipeline_service import get_info

    return get_info()


def _h_get_registry(params):
    from scistack_gui.services.pipeline_service import get_registry

    return get_registry()


def _h_get_function_params(params):
    from scistack_gui.services.pipeline_service import get_function_full_info

    return get_function_full_info(params["name"])


def _h_get_function_source(params):
    from scistack_gui.services.pipeline_service import get_function_source

    return get_function_source(params["name"])


def _h_get_function_doc(params):
    from scistack_gui.services.pipeline_service import get_function_doc

    return get_function_doc(params["name"])


def _h_get_notes(params):
    from scistack_gui.services.layout_service import get_notes

    return get_notes()


def _h_set_note(params):
    from scistack_gui.services.layout_service import set_note

    return set_note(params["key"], params["text"])


def _h_get_variable_records(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.variable_service import get_variable_records

    return get_variable_records(params["name"], get_db())


def _h_get_variable_plot_data(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.variable_service import get_variable_plot_data

    return get_variable_plot_data(params["name"], get_db())


def _h_get_parameters(params):
    from scistack_gui.services.layout_service import get_parameters

    return get_parameters()


def _h_get_path_inputs(params):
    from scistack_gui.services.layout_service import get_path_inputs

    return get_path_inputs()


def _h_deep_copy_path_input(params):
    from scistack_gui.services.layout_service import deep_copy_path_input

    return deep_copy_path_input(params["node_id"])


def _h_put_layout(params):
    from scistack_gui.services.layout_service import put_layout

    return put_layout(
        params["node_id"],
        params["x"],
        params["y"],
        params.get("node_type"),
        params.get("label"),
        params.get("pipeline_id", "main"),
    )


def _h_delete_layout(params):
    from scistack_gui.services.layout_service import delete_layout

    return delete_layout(params["node_id"])


def _h_put_edge(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.layout_service import put_edge

    return put_edge(
        get_db(),
        params["edge_id"],
        params["source"],
        params["target"],
        params.get("source_handle"),
        params.get("target_handle"),
    )


def _h_delete_edge(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.layout_service import delete_edge

    return delete_edge(
        get_db(),
        params["edge_id"],
        params.get("source", ""),
        params.get("target", ""),
        params.get("source_handle"),
        params.get("target_handle"),
    )


def _h_unhide_edge(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.layout_service import unhide_edge

    return unhide_edge(get_db(), params["edge_id"], params.get("pipeline_id", "main"))


def _h_get_hidden_edges(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.layout_service import get_hidden_edges

    return get_hidden_edges(get_db(), params.get("pipeline_id"))


def _h_put_pending_constant(params):
    from scistack_gui.notify import notify
    from scistack_gui.services.layout_service import put_pending_constant

    result = put_pending_constant(params["name"], params["value"])
    notify("dag_updated", {})
    return result


def _h_delete_pending_constant(params):
    from scistack_gui.notify import notify
    from scistack_gui.services.layout_service import delete_pending_constant

    result = delete_pending_constant(params["name"], params["value"])
    notify("dag_updated", {})
    return result


def _h_hide_combo(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.layout_service import hide_variant_combo

    return hide_variant_combo(
        get_db(), params["function_name"], params.get("node_id"), params["variant_key"]
    )


def _h_unhide_combo(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.layout_service import unhide_variant_combo

    return unhide_variant_combo(get_db(), params["node_id"])


def _h_list_hidden_combos(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.layout_service import get_hidden_combos

    return get_hidden_combos(get_db(), params["function_name"])


def _h_hide_parameter_value(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.layout_service import hide_parameter_value

    return hide_parameter_value(
        get_db(),
        params["name"],
        params["value"],
        params.get("pipeline_id", "main"),
    )


def _h_unhide_parameter_value(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.layout_service import unhide_parameter_value

    return unhide_parameter_value(
        get_db(),
        params["name"],
        params["value"],
        params.get("pipeline_id", "main"),
    )


def _h_set_parameter_group_checked(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.layout_service import set_parameter_group_checked

    return set_parameter_group_checked(
        get_db(),
        params["name"],
        params["values"],
        params["checked"],
        params.get("pipeline_id", "main"),
    )


def _h_list_hidden_parameter_values(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.layout_service import get_hidden_constant_values

    return get_hidden_constant_values(get_db(), params.get("pipeline_id", "main"))


def _h_create_parameter(params):
    from scistack_gui.services.layout_service import create_parameter

    return create_parameter(params["name"], params.get("values"))


def _h_list_glue(params):
    from scistack_gui.services import glue_service

    return {"nodes": glue_service.list_glue_nodes()}


def _h_get_glue(params):
    from scistack_gui.services import glue_service

    return glue_service.read_glue_source(params["name"])


def _h_get_glue_columns(params):
    from scistack_gui.services import glue_service

    variable_type = params.get("variable_type") or ""
    if not variable_type:
        return {
            "ok": False,
            "error": (
                "This glue node is not wired to a variable yet, so there are "
                "no columns to show."
            ),
        }
    return glue_service.input_columns(variable_type)


def _h_create_glue(params):
    from scistack_gui.services import glue_service

    return glue_service.create_glue_node(
        params["name"],
        param=params.get("param", "value"),
        language=params.get("language", "python"),
    )


def _h_save_glue(params):
    from scistack_gui.services import glue_service

    return glue_service.update_glue_source(params["name"], params["source"])


def _h_delete_glue(params):
    from scistack_gui.services import glue_service

    return glue_service.delete_glue_node(params["name"])


def _h_update_parameter(params):
    from scistack_gui.services.layout_service import update_parameter

    return update_parameter(
        params["name"],
        params["values"],
        params.get("description", ""),
        params.get("group"),
    )


def _h_delete_parameter(params):
    from scistack_gui.services.layout_service import delete_parameter

    return delete_parameter(params["name"])


def _h_refresh_parameter_source(params):
    from scistack_gui.services.layout_service import refresh_parameter_source

    return refresh_parameter_source(params.get("name"))


def _h_create_path_input(params):
    from scistack_gui.services.layout_service import create_path_input

    return create_path_input(
        params["name"], params.get("template", ""), params.get("root_folder")
    )


def _h_update_path_input(params):
    from scistack_gui.services.layout_service import update_path_input

    return update_path_input(
        params["name"],
        params["template"],
        params.get("root_folder"),
        params.get("alternate_templates"),
    )


def _h_delete_path_input(params):
    from scistack_gui.services.layout_service import delete_path_input

    return delete_path_input(params["name"], params.get("pipeline_id", "main"))


def _h_put_node_config(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.layout_service import put_node_config

    return put_node_config(get_db(), params["node_id"], params.get("config", {}))


def _h_get_variables_list(params):
    from scistack_gui.services.pipeline_service import get_variables_list

    return get_variables_list()


def _h_start_run(params):
    import uuid

    from scistack_gui.api.run import (
        WhereFilterSpec,
        _run_in_thread,
        route_matlab_single_run,
    )
    from scistack_gui.db import acquire_db_connection, get_db, release_db_connection

    logger.info("[server] JSON-RPC start_run handler called")
    logger.debug("[server] Request params: %s", params)

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

    logger.info(
        "[server] Parsed request: run_id=%s, function=%s, language=%s, variants=%d, "
        "schema_filter=%s, schema_level=%s, run_options=%s, where_filters=%d",
        run_id,
        function_name,
        language,
        len(variants),
        list(schema_filter.keys()) if schema_filter else None,
        schema_level,
        run_options,
        len(where_filters) if where_filters else 0,
    )

    # MATLAB functions can't run through _run_in_thread's Python registry at
    # all. Decide here, from matlab_registry — not from the caller's
    # `language` hint — and hand host-dispatchable runs back to dagPanel.ts
    # (which owns the MathWorks-terminal path where breakpoints work). Must
    # happen BEFORE acquire_db_connection below: MATLAB needs the DuckDB
    # file lock for its own run, and holding it here for a run this process
    # will never execute would block the very thing we just dispatched.
    routed = route_matlab_single_run(
        function_name, params, run_id, db, host_can_dispatch_matlab=True
    )
    if routed is not None:
        return routed

    logger.debug("[server] Acquiring DB connection for run thread")
    acquire_db_connection()

    def _run_wrapper():
        logger.debug("[server] Run wrapper thread started (run_id=%s)", run_id)
        try:
            _run_in_thread(
                run_id,
                function_name,
                variants,
                db,
                schema_filter,
                schema_level,
                run_options,
                where_filters,
            )
        finally:
            logger.debug("[server] Releasing DB connection (run_id=%s)", run_id)
            release_db_connection()
            logger.debug("[server] Run wrapper thread finished (run_id=%s)", run_id)

    logger.info("[server] Spawning run wrapper thread (run_id=%s)", run_id)
    thread = threading.Thread(target=_run_wrapper, daemon=True)
    thread.start()
    logger.info("[server] Run wrapper thread started (run_id=%s)", run_id)
    return {"run_id": run_id}


def _h_cancel_run(params):
    from scistack_gui.services.run_service import cancel_run

    run_id = params["run_id"]
    logger.info("[server] JSON-RPC cancel_run handler called for run_id=%s", run_id)
    result = cancel_run(run_id)
    logger.debug("[server] cancel_run result: %s (run_id=%s)", result, run_id)
    return result


def _h_force_cancel_run(params):
    from scistack_gui.services.run_service import force_cancel_run

    run_id = params["run_id"]
    logger.info(
        "[server] JSON-RPC force_cancel_run handler called for run_id=%s", run_id
    )
    result = force_cancel_run(run_id)
    logger.debug("[server] force_cancel_run result: %s (run_id=%s)", result, run_id)
    return result


def _h_refresh_module(params):
    from scistack_gui.notify import notify
    from scistack_gui.services.pipeline_service import refresh_module

    result = refresh_module()
    if result.get("ok"):
        notify("dag_updated", {})
    return result


def _h_create_builtin_function(params):
    from scistack_gui.services.builtin_function_service import create_builtin_function

    return create_builtin_function(params["language"], params["reference"])


def _h_create_variable(params):
    from scistack_gui.services.variable_service import create_variable

    # No dag_updated notify: a bare type declaration has no DB records yet,
    # so it can't appear as a canvas node — see api/variables.py's
    # create_variable for the full reasoning (same fix, both transports).
    return create_variable(
        params.get("name", ""),
        params.get("docstring"),
        params.get("language", "python"),
    )


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
    from scistack_gui.services.project_service import get_project_code

    return get_project_code()


def _h_refresh_project(params):
    from scistack_gui.notify import notify
    from scistack_gui.services.project_service import refresh_project

    result = refresh_project()
    if result.get("ok"):
        notify("dag_updated", {})
    return result


def _h_get_project_paths(params):
    from scistack_gui.services.project_service import get_project_paths

    return get_project_paths()


def _h_add_project_path(params):
    from scistack_gui.notify import notify
    from scistack_gui.services.project_service import add_project_path

    result = add_project_path(params.get("path", ""))
    if result.get("ok"):
        notify("dag_updated", {})
    return result


def _h_remove_project_path(params):
    from scistack_gui.notify import notify
    from scistack_gui.services.project_service import remove_project_path

    result = remove_project_path(params.get("path", ""))
    if result.get("ok"):
        notify("dag_updated", {})
    return result


def _h_set_entities_file(params):
    from scistack_gui.notify import notify
    from scistack_gui.services.project_service import set_entities_file

    result = set_entities_file(params.get("path"))
    if result.get("ok"):
        notify("dag_updated", {})
    return result


def _h_clear_entities_file(params):
    from scistack_gui.notify import notify
    from scistack_gui.services.project_service import clear_entities_file

    result = clear_entities_file()
    if result.get("ok"):
        notify("dag_updated", {})
    return result


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


def _h_generate_matlab_command(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.matlab_command_service import generate_matlab_command

    return generate_matlab_command(params["function_name"], get_db(), params)


def _h_generate_matlab_pipeline_command(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.matlab_command_service import (
        generate_matlab_pipeline_command,
    )

    return generate_matlab_pipeline_command(params["pipeline_id"], get_db(), params)


def _h_start_matlab_sidecar_run(params):
    from scistack_gui.api.run import start_matlab_sidecar_run

    return start_matlab_sidecar_run(
        params["command"], params["run_id"], params.get("warnings")
    )


def _h_get_matlab_engine_status(params):
    from scistack_gui.api.run import get_matlab_engine_status

    return get_matlab_engine_status()


def _h_restart_matlab_engine(params):
    from scistack_gui.api.run import restart_matlab_engine

    return restart_matlab_engine()


# --- Plot Studio (docs/claude/plotting-library-design.md) ---
# These call the same service functions as the HTTP routes in api/plot.py, so
# the browser GUI and the extension can't drift. They deliberately reuse the
# ONE DatabaseManager the server already holds: a second DuckDB connection
# would reintroduce the write-lock contention the MATLAB run-ownership work
# resolved (docs/claude/matlab-run-database-ownership.md).


def _h_plot_describe(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.plot_service import describe

    return describe(
        get_db(),
        params.get("variable"),
        refresh=bool(params.get("refresh")),
        csv_path=params.get("csv_path"),
    )


def _h_plot_capabilities(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.plot_service import capabilities_for

    return capabilities_for(
        get_db(), params["spec"], csv_path=params.get("csv_path")
    )


def _h_plot_variant_graph(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.plot_service import variant_graph

    return variant_graph(
        get_db(),
        params["variable"],
        functions=params.get("functions") or [],
        csv_path=params.get("csv_path"),
    )


def _h_plot_resolve(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.plot_service import resolve_figures

    return resolve_figures(
        get_db(),
        params["spec"],
        max_points=params.get("max_points"),
        figure_index=params.get("figure_index"),
        csv_path=params.get("csv_path"),
    )


def _h_plot_export(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.plot_service import export_code

    return export_code(
        get_db(),
        params["spec"],
        function_name=params.get("function_name"),
        output_variable=params.get("output_variable"),
        path_template=params.get("path_template"),
        finalized=params.get("finalized", True),
        csv_path=params.get("csv_path"),
    )


def _h_plot_add_to_pipeline(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.plot_service import add_to_pipeline

    return add_to_pipeline(
        get_db(),
        params["spec"],
        function_name=params.get("function_name"),
        output_variable=params.get("output_variable"),
        path_template=params.get("path_template"),
        finalized=params.get("finalized", True),
    )


def _h_plot_save_figure(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.plot_service import save_figure

    return save_figure(
        get_db(),
        params["spec"],
        params["path"],
        dpi=params.get("dpi", 200),
        figure_index=params.get("figure_index"),
        csv_path=params.get("csv_path"),
    )


def _h_plot_save_all(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.plot_service import start_save_job

    return start_save_job(
        get_db(),
        params["spec"],
        params["path"],
        dpi=params.get("dpi", 200),
        csv_path=params.get("csv_path"),
    )


def _h_plot_invalidate(params):
    from scistack_gui.db import get_db
    from scistack_gui.services.plot_service import invalidate

    return invalidate(get_db())


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
    "get_function_doc": _h_get_function_doc,
    "get_notes": _h_get_notes,
    "set_note": _h_set_note,
    "get_variable_records": _h_get_variable_records,
    "get_variable_plot_data": _h_get_variable_plot_data,
    "get_parameters": _h_get_parameters,
    "get_variables_list": _h_get_variables_list,
    "get_path_inputs": _h_get_path_inputs,
    "put_layout": _h_put_layout,
    "put_node_config": _h_put_node_config,
    "delete_layout": _h_delete_layout,
    "put_edge": _h_put_edge,
    "delete_edge": _h_delete_edge,
    "unhide_edge": _h_unhide_edge,
    "get_hidden_edges": _h_get_hidden_edges,
    "put_pending_constant": _h_put_pending_constant,
    "delete_pending_constant": _h_delete_pending_constant,
    "hide_combo": _h_hide_combo,
    "unhide_combo": _h_unhide_combo,
    "list_hidden_combos": _h_list_hidden_combos,
    "hide_parameter_value": _h_hide_parameter_value,
    "unhide_parameter_value": _h_unhide_parameter_value,
    "set_parameter_group_checked": _h_set_parameter_group_checked,
    "list_hidden_parameter_values": _h_list_hidden_parameter_values,
    "create_parameter": _h_create_parameter,
    # Glue nodes. Deliberately no "run_glue": a glue node is never
    # independently executable (docs/claude/free-code-glue-nodes.md §5).
    "list_glue": _h_list_glue,
    "get_glue": _h_get_glue,
    "get_glue_columns": _h_get_glue_columns,
    "create_glue": _h_create_glue,
    "save_glue": _h_save_glue,
    "delete_glue": _h_delete_glue,
    "update_parameter": _h_update_parameter,
    "delete_parameter": _h_delete_parameter,
    "refresh_parameter_source": _h_refresh_parameter_source,
    "create_path_input": _h_create_path_input,
    "update_path_input": _h_update_path_input,
    "delete_path_input": _h_delete_path_input,
    "deep_copy_path_input": _h_deep_copy_path_input,
    "start_run": _h_start_run,
    "cancel_run": _h_cancel_run,
    "force_cancel_run": _h_force_cancel_run,
    "refresh_module": _h_refresh_module,
    "create_variable": _h_create_variable,
    "create_builtin_function": _h_create_builtin_function,
    "get_project_code": _h_get_project_code,
    "refresh_project": _h_refresh_project,
    "get_project_paths": _h_get_project_paths,
    "add_project_path": _h_add_project_path,
    "remove_project_path": _h_remove_project_path,
    "set_entities_file": _h_set_entities_file,
    "clear_entities_file": _h_clear_entities_file,
    "list_pipelines": _h_list_pipelines,
    "create_pipeline": _h_create_pipeline,
    "rename_pipeline": _h_rename_pipeline,
    "delete_pipeline": _h_delete_pipeline,
    "unhide_pipeline": _h_unhide_pipeline,
    "get_hidden_pipelines": _h_get_hidden_pipelines,
    "list_hypotheses": _h_list_hypotheses,
    "create_hypothesis": _h_create_hypothesis,
    "update_hypothesis": _h_update_hypothesis,
    "delete_hypothesis": _h_delete_hypothesis,
    "get_pipeline_interface": _h_get_pipeline_interface,
    "get_hidden_ports": _h_get_hidden_ports,
    "hide_port": _h_hide_port,
    "unhide_port": _h_unhide_port,
    "extract_to_submodule": _h_extract_to_submodule,
    "duplicate_pipeline": _h_duplicate_pipeline,
    "duplicate_hypothesis": _h_duplicate_hypothesis,
    "export_pipeline": _h_export_pipeline,
    "import_pipeline": _h_import_pipeline,
    "export_pipeline_code": _h_export_pipeline_code,
    "paste_nodes": _h_paste_nodes,
    "add_pipeline_use": _h_add_pipeline_use,
    "update_use_binding": _h_update_use_binding,
    "remove_pipeline_use": _h_remove_pipeline_use,
    "get_pipeline_plan": _h_get_pipeline_plan,
    "start_pipeline_run": _h_start_pipeline_run,
    "get_endpoint_artifacts": _h_get_endpoint_artifacts,
    "write_report": _h_write_report,
    "generate_matlab_command": _h_generate_matlab_command,
    "generate_matlab_pipeline_command": _h_generate_matlab_pipeline_command,
    "start_matlab_sidecar_run": _h_start_matlab_sidecar_run,
    "get_matlab_engine_status": _h_get_matlab_engine_status,
    "restart_matlab_engine": _h_restart_matlab_engine,
    "plot_describe": _h_plot_describe,
    "plot_capabilities": _h_plot_capabilities,
    "plot_variant_graph": _h_plot_variant_graph,
    "plot_resolve": _h_plot_resolve,
    "plot_export": _h_plot_export,
    "plot_add_to_pipeline": _h_plot_add_to_pipeline,
    "plot_save_figure": _h_plot_save_figure,
    "plot_save_all": _h_plot_save_all,
    "plot_invalidate": _h_plot_invalidate,
}


#: Methods that acquire the DuckDB connection themselves, for as long as they
#: actually need it, instead of letting :func:`_handle_request` hold it across
#: the whole call.
#:
#: Every entry is plot work, and they are here for one measured reason: a plot
#: resolve spends nearly all of its time in pandas and matplotlib, with the
#: database touched only while the variable frames load. Holding the file lock
#: for the rest of it blocked MATLAB for the full duration — the 2026-09-11 log
#: shows one 31-second hold (12:27:39 acquire, 12:28:10 release) for work that
#: needed the database for well under a second of it.
#:
#: The precedent is :func:`_h_start_run`, which has owned its own connection
#: since the MATLAB run-ownership work (docs/claude/matlab-run-database-ownership.md).
#: It is NOT listed here: it acquires *in addition to* the blanket hold and
#: hands off to a thread, which is a different arrangement from these.
#:
#: A method added here that then forgets to wrap its own database access will
#: fail with a closed connection rather than silently working, because the
#: JSON-RPC server closes the connection whenever the refcount hits zero. That
#: is why the list is the plot methods whose cost is CPU-bound reduction and
#: rendering, and not simply every ``plot_*`` handler:
#:
#: * ``plot_add_to_pipeline`` writes source files and reloads the registry
#:   through services that reach the database by their own routes
#:   (``target_file_service``), so it keeps the blanket hold.
#: * ``plot_invalidate`` only drops a dict; there is no hold worth narrowing.
SELF_MANAGED_DB_METHODS = frozenset(
    {
        "plot_describe",
        "plot_capabilities",
        "plot_variant_graph",
        "plot_resolve",
        "plot_export",
        "plot_save_figure",
        # Spawns a thread and returns; the HANDLER touches nothing. Its worker
        # takes the connection through `save_figure` -> `_load` on its own
        # schedule, which is the point — a save of thirty figures must not hold
        # the DuckDB file for the minutes it spends in matplotlib.
        "plot_save_all",
    }
)


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
    # next /api/info call (see _h_get_info).
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
