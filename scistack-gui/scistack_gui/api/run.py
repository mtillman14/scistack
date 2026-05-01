"""
POST /api/run

Triggers a for_each call in a background thread and streams stdout
back to the frontend via WebSocket.

Payload:
  {
    "function_name": "compute_rolling_vo2",
    "variants": [
      {"window_seconds": 30, "sample_interval": 5},
      {"window_seconds": 60, "sample_interval": 5}
    ]
  }

Each entry in `variants` is a constants dict. We run one for_each call
per variant. If `variants` is empty we run all known variants from the DB.
"""

import ctypes
import logging
import sys
import time
import uuid
import threading
from io import StringIO
from contextlib import redirect_stdout

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from scihist import for_each
from scidb.database import DatabaseManager

from scistack_gui.db import get_db
from scistack_gui import registry
from scistack_gui.api.ws import push_message

# This logger is configured in server.py (FastAPI) / __main__.py (JSON-RPC)
# to write to stderr with the "[scistack] …" prefix. The extension forwards
# stderr to the SciStack Output channel, so .info() calls here show up in
# VS Code's UI in addition to being captured by scidb.log.Log for the on-disk
# scidb.log file.
logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Per-run cancellation registry
# ---------------------------------------------------------------------------
#
# Each entry: {
#   "event": threading.Event,        # set by cancel_run / force_cancel_run
#   "thread": threading.Thread,      # the worker thread running _run_in_thread
#   "cancelled": bool,               # True after cooperative cancel requested
#   "force_cancelled": bool,         # True after force cancel requested
# }
#
# The registry is module-level (process-wide); lookups by run_id are O(1).
# Mutated only from the FastAPI/JSON-RPC handler thread and the worker
# thread's entry/exit, so a plain dict is sufficient.
_active_runs: dict[str, dict] = {}
_active_runs_lock = threading.Lock()


class WhereFilterSpec(BaseModel):
    variable: str              # variable type name, e.g. "Side"
    op: str                    # "==", "!=", "<", "<=", ">", ">=", "IN"
    value: str                 # always string, coerced on backend


class RunRequest(BaseModel):
    function_name: str
    variants: list[dict] = []   # list of constants dicts; empty = run all known
    run_id: str | None = None   # frontend-generated ID; we generate one if absent
    schema_filter: dict[str, list] | None = None   # {key: [selected values]}; None = all
    schema_level: list[str] | None = None          # which schema keys to iterate; None = all
    run_options: dict | None = None   # {dry_run, save, distribute}; all optional
    where_filters: list[WhereFilterSpec] | None = None  # data filters for where= param


def _run_in_thread(run_id: str, function_name: str, variants: list[dict], db: DatabaseManager,
                    schema_filter: dict[str, list] | None = None,
                    schema_level: list[str] | None = None,
                    run_options: dict | None = None,
                    where_filters: list[WhereFilterSpec] | None = None):
    """
    Executed in a background thread. Runs for_each for each variant,
    captures stdout line-by-line, and pushes it to the WebSocket queue.
    """
    logger.info(
        "run[%s]: thread started for function=%s, variants=%d, "
        "schema_level=%s, schema_filter=%s, where_filters=%s, run_options=%s",
        run_id, function_name, len(variants or []),
        schema_level, _summarize_schema_filter(schema_filter),
        len(where_filters) if where_filters else 0,
        run_options,
    )

    def emit(text: str):
        push_message({"type": "run_output", "run_id": run_id, "text": text})

    # Register this run so cancel_run/force_cancel_run can find it.
    cancel_event = threading.Event()
    with _active_runs_lock:
        _active_runs[run_id] = {
            "event": cancel_event,
            "thread": threading.current_thread(),
            "cancelled": False,
            "force_cancelled": False,
        }

    def _is_cancelled() -> bool:
        return cancel_event.is_set()

    # The DatabaseManager is stored in thread-local storage by configure_database().
    # Background threads don't inherit that local, so we re-register it here.
    db.set_current_db()

    try:
        fn = registry.get_function(function_name)
    except KeyError as e:
        logger.warning("run[%s]: function not found: %s", run_id, e)
        push_message({"type": "run_done", "run_id": run_id, "success": False,
                      "error": str(e), "cancelled": False})
        with _active_runs_lock:
            _active_runs.pop(run_id, None)
        return

    # Look up input/output types for this function from the DB.
    all_variants = db.list_pipeline_variants()
    fn_variants = [v for v in all_variants if v["function_name"] == function_name]

    from scidb.log import Log
    Log.info(f"run: fn_variants for '{function_name}' = {fn_variants}")

    # --- Check manual edges for current output wiring ---
    # If the user has rewired the function's outputs (e.g. deleted an old output
    # node and connected a new one), manual edges should override DB-derived
    # output types.  This prevents stale DB history from resurrecting old nodes.
    from scistack_gui import pipeline_store, layout as layout_store
    from scistack_gui.api.pipeline import _fn_params_from_registry
    from scistack_gui.domain.edge_resolver import (
        resolve_function_edges, infer_manual_fn_output_types,
    )

    all_edges = pipeline_store.get_manual_edges(db)
    manual_nodes = pipeline_store.get_manual_nodes(db)

    # Collect all node IDs for this function: every DB-derived call site
    # (composite fn__{fn}__{call_id}), every manual node sharing the label,
    # and the legacy fn__{fn} form for backward-compat manual edges.
    from scistack_gui.domain.graph_builder import fn_node_id
    fn_node_ids = set()
    fn_node_ids.add(f"fn__{function_name}")  # legacy/manual edges
    for v in fn_variants:
        cid = v.get("call_id")
        if cid:
            fn_node_ids.add(fn_node_id(function_name, cid))
    for nid, meta in manual_nodes.items():
        if meta["type"] == "functionNode" and meta["label"] == function_name:
            fn_node_ids.add(nid)

    manual_output_types = infer_manual_fn_output_types(
        fn_node_ids, all_edges, manual_nodes, existing_node_labels={})

    if fn_variants and manual_output_types:
        # Override output types in DB-derived variants with the current wiring.
        Log.info(f"run: overriding DB output types with manual edge outputs: {manual_output_types}")
        overridden = []
        seen_constants = set()
        for v in fn_variants:
            key = tuple(sorted(v["constants"].items()))
            if key in seen_constants:
                continue
            seen_constants.add(key)
            for out in manual_output_types:
                overridden.append({
                    **v,
                    "output_type": out,
                })
        fn_variants = overridden

    if not fn_variants:
        # No DB history yet — try to infer inputs/outputs from manual edges.
        Log.info(f"run: all_edges = {all_edges}")
        Log.info(f"run: manual_nodes = {manual_nodes}")
        Log.info(f"run: fn_node_ids = {fn_node_ids}")

        sig_params = _fn_params_from_registry(function_name)
        resolved = resolve_function_edges(
            fn_node_ids=fn_node_ids,
            manual_edges=all_edges,
            manual_nodes=manual_nodes,
            existing_node_labels={},
            sig_params=sig_params,
        )
        input_types = resolved.input_types
        output_types = resolved.output_types
        constant_names = resolved.constant_names

        Log.info(f"run: after edge scan: input_types={input_types}, output_types={output_types}, "
                 f"constant_names={constant_names}")

        if not output_types:
            Log.warn(f"run: no outputs found for '{function_name}' from DB or edges")
            push_message({"type": "run_done", "run_id": run_id, "success": False,
                          "error": f"No pipeline history or output connections found for '{function_name}'. "
                                    "Connect it to an output variable node first."})
            return

        # Collect constant values from pending constants for wired constant nodes.
        import ast as _ast
        inferred_constants: dict[str, list] = {}  # const_name → list of typed values
        if constant_names:
            pending = pipeline_store.get_pending_constants(db)
            Log.info(f"run: pending constants = {pending}")
            for cname in constant_names:
                vals = pending.get(cname, set())
                typed_vals = []
                for v in vals:
                    try:
                        typed_vals.append(_ast.literal_eval(v))
                    except (ValueError, SyntaxError):
                        typed_vals.append(v)
                if typed_vals:
                    inferred_constants[cname] = typed_vals
                else:
                    Log.warn(f"run: constant '{cname}' wired to '{function_name}' but has no pending values")

        Log.info(f"run: inferred inputs={input_types} outputs={output_types} "
                 f"constants={list(inferred_constants.keys())} for '{function_name}' from edges")

        # Build synthetic variants: cross-product of output types × constant combos.
        if inferred_constants:
            # Build all combinations of constant values.
            from itertools import product as _product
            const_names_list = sorted(inferred_constants.keys())
            const_value_lists = [inferred_constants[c] for c in const_names_list]
            fn_variants = []
            for combo in _product(*const_value_lists):
                constants = dict(zip(const_names_list, combo))
                for out in output_types:
                    fn_variants.append({
                        "input_types": input_types,
                        "output_type": out,
                        "constants": constants,
                    })
        else:
            fn_variants = [
                {"input_types": input_types, "output_type": out, "constants": {}}
                for out in output_types
            ]

    # --- Variant resolution via domain layer ---
    from scistack_gui.domain.variant_resolver import (
        filter_variants, deduplicate_variants,
        merge_pending_constants, build_schema_kwargs,
    )

    # Determine which variants to run.
    if variants:
        targets = filter_variants(fn_variants, variants)
    else:
        targets = fn_variants

    unique_targets = deduplicate_variants(targets)

    # Add synthetic targets for pending constant values.
    if fn_variants:
        from scistack_gui import pipeline_store as _ps
        pending_consts = _ps.get_pending_constants(db)
        unique_targets = merge_pending_constants(unique_targets, pending_consts)

    # Build schema kwargs.
    iterate_keys = schema_level if schema_level is not None else list(db.dataset_schema_keys)
    distinct_values = {key: db.distinct_schema_values(key) for key in iterate_keys}
    schema_kwargs = build_schema_kwargs(
        schema_level, list(db.dataset_schema_keys),
        schema_filter, distinct_values,
    )

    # Extract run options (dry_run, save, distribute, as_table).
    opts = run_options or {}
    opt_dry_run = opts.get("dry_run", False)
    opt_save = opts.get("save", True)
    opt_distribute = opts.get("distribute", False)
    opt_as_table = opts.get("as_table", False)

    success = True
    run_started_at = time.time()
    # Build where= argument from where_filters.
    where_arg = _build_where(where_filters)

    logger.info(
        "run[%s]: executing %d target(s) for '%s' "
        "(dry_run=%s, save=%s, distribute=%s, as_table=%s, schema_keys=%s)",
        run_id, len(unique_targets), function_name,
        opt_dry_run, opt_save, opt_distribute, opt_as_table,
        list(schema_kwargs.keys()),
    )

    cancelled = False
    try:
        for v in unique_targets:
            # Cooperative cancel: stop before launching the next variant.
            if _is_cancelled():
                logger.info("run[%s]: cancel detected between variants — stopping",
                            run_id)
                cancelled = True
                emit("⛔ Cancelled\n")
                break
            # Build inputs dict: variable class inputs + scalar constants
            try:
                inputs = {}
                for param, type_names in v["input_types"].items():
                    # type_names may be a list (new) or a string (from DB history).
                    if isinstance(type_names, list):
                        if len(type_names) > 1:
                            from scidb import EachOf
                            inputs[param] = EachOf(*(registry.get_variable_class(t) for t in type_names))
                        else:
                            inputs[param] = registry.get_variable_class(type_names[0])
                    else:
                        inputs[param] = registry.get_variable_class(type_names)
                inputs.update(v["constants"])   # add constants

                OutputCls = registry.get_variable_class(v["output_type"])
            except KeyError as e:
                emit(f"Error: {e}\n")
                success = False
                continue

            label = f"{function_name}({', '.join(f'{k}={val}' for k, val in v['constants'].items())})" \
                    if v["constants"] else function_name
            logger.info(
                "run[%s]: target -> %s, inputs=%s, output=%s",
                run_id, label,
                {k: (type_names if isinstance(type_names, list) else [type_names])
                 for k, type_names in v["input_types"].items()},
                v["output_type"],
            )
            emit(f"▶ Running {label}\n")

            # Emit structured run_start message for the frontend.
            started_at = time.time()
            push_message({
                "type": "run_start",
                "run_id": run_id,
                "function_name": function_name,
                "constants": v["constants"],
                "input_types": {k: str(vt) for k, vt in v["input_types"].items()},
                "output_type": v["output_type"],
                "started_at": started_at,
            })

            # Progress callback: relay structured progress to the frontend.
            def _progress_fn(info: dict):
                # Convert metadata values to strings for JSON serialization.
                meta = {str(k): str(val) for k, val in info.get("metadata", {}).items()}
                push_message({
                    "type": "run_progress",
                    "run_id": run_id,
                    "event": info["event"],
                    "current": info["current"],
                    "total": info["total"],
                    "completed": info["completed"],
                    "skipped": info["skipped"],
                    "metadata": meta,
                    "error": info.get("error"),
                })

            # Capture for_each stdout and relay it line-by-line.
            buf = StringIO()
            try:
                with redirect_stdout(buf):
                    for_each(fn, inputs=inputs, outputs=[OutputCls],
                             dry_run=opt_dry_run, save=opt_save,
                             distribute=opt_distribute,
                             as_table=opt_as_table,
                             where=where_arg,
                             skip_computed=False,
                             _progress_fn=_progress_fn,
                             _cancel_check=_is_cancelled,
                             **schema_kwargs)
                output = buf.getvalue()
                if output:
                    emit(output)
                target_ms = int((time.time() - started_at) * 1000)
                logger.info("run[%s]: target %s completed in %d ms",
                            run_id, label, target_ms)
            except KeyboardInterrupt:
                # Force-cancel injected an interrupt into this thread (or the
                # user pressed Ctrl-C in CLI mode).  Treat as cancel and stop.
                logger.warning(
                    "run[%s]: target %s interrupted by KeyboardInterrupt "
                    "(force-cancel)", run_id, label,
                )
                output = buf.getvalue()
                if output:
                    emit(output)
                cancelled = True
                emit("⛔ Force-cancelled\n")
                break
            except Exception as exc:
                logger.exception("run[%s]: target %s failed", run_id, label)
                emit(f"Error: {exc}\n")
                success = False
    except KeyboardInterrupt:
        # Defence in depth: if KeyboardInterrupt slips past the per-target
        # handler (e.g. fired between targets), still cancel cleanly.
        logger.warning("run[%s]: interrupted by KeyboardInterrupt at top level",
                       run_id)
        cancelled = True
        emit("⛔ Force-cancelled\n")
    finally:
        duration_ms = int((time.time() - run_started_at) * 1000)
        # Read the final cancel flags from the registry before popping it.
        with _active_runs_lock:
            entry = _active_runs.pop(run_id, None)
        was_force = bool(entry and entry.get("force_cancelled"))
        if cancel_event.is_set():
            cancelled = True
        logger.info(
            "run[%s]: finished (success=%s, cancelled=%s, force=%s) in %d ms",
            run_id, success, cancelled, was_force, duration_ms,
        )
        push_message({"type": "run_done", "run_id": run_id, "success": success,
                      "duration_ms": duration_ms,
                      "cancelled": cancelled, "force_cancelled": was_force})
        push_message({"type": "dag_updated"})


def _build_where(where_filters: list[WhereFilterSpec] | None):
    """Convert frontend WhereFilterSpec list into scidb filter objects.

    Returns None (no filter), a single Filter, or EachOf(filter1, filter2, ...).
    """
    if not where_filters:
        return None

    import ast
    from scidb.filters import VariableFilter

    def _coerce(s: str):
        try:
            return ast.literal_eval(s)
        except (ValueError, SyntaxError):
            return s

    scidb_filters = []
    for f in where_filters:
        var_cls = registry.get_variable_class(f.variable)
        val = _coerce(f.value)
        scidb_filters.append(VariableFilter(var_cls, f.op, val))

    if len(scidb_filters) == 1:
        return scidb_filters[0]

    from scidb import EachOf
    return EachOf(*scidb_filters)


def _summarize_schema_filter(schema_filter: dict[str, list] | None) -> str:
    """Compact one-line summary of a schema_filter for logging."""
    if not schema_filter:
        return "none"
    return ", ".join(f"{k}={len(v)}v" for k, v in schema_filter.items())


@router.post("/run")
def start_run(req: RunRequest, db: DatabaseManager = Depends(get_db)):
    run_id = req.run_id or str(uuid.uuid4())[:8]
    thread = threading.Thread(
        target=_run_in_thread,
        args=(run_id, req.function_name, req.variants, db,
              req.schema_filter, req.schema_level, req.run_options,
              req.where_filters),
        daemon=True,
    )
    thread.start()
    return {"run_id": run_id}


# ---------------------------------------------------------------------------
# Cancel APIs (called from server.py JSON-RPC handlers)
# ---------------------------------------------------------------------------

def cancel_run(run_id: str) -> dict:
    """Cooperatively cancel a running for_each.

    Sets the cancel event so the worker thread breaks between combos.
    Safe: completed combos are saved, in-flight combo finishes normally.

    Returns:
        ``{"ok": True, "cancelled": True}`` on success,
        ``{"ok": False, "error": "unknown run_id"}`` if the run isn't active.
    """
    with _active_runs_lock:
        entry = _active_runs.get(run_id)
        if entry is None:
            logger.warning("cancel_run: unknown run_id=%s", run_id)
            return {"ok": False, "error": f"unknown run_id: {run_id}"}
        entry["cancelled"] = True
        entry["event"].set()
    logger.info("run[%s]: cancel requested (cooperative)", run_id)
    return {"ok": True, "cancelled": True, "force": False}


def force_cancel_run(run_id: str) -> dict:
    """Force-cancel a running for_each by injecting KeyboardInterrupt.

    Sets the cooperative cancel event AND calls
    ``ctypes.pythonapi.PyThreadState_SetAsyncExc`` to raise
    ``KeyboardInterrupt`` in the worker thread. Best-effort:

    - Won't interrupt code blocked in C extensions, native syscalls,
      or threading primitives that don't poll for interrupts.
    - When that fails, the user must restart the Python subprocess via
      the existing ``scistack.restartPython`` command.

    Returns:
        ``{"ok": True, "cancelled": True, "force": True, "best_effort": True}``
        on success,
        ``{"ok": False, "error": "..."}`` if the run isn't active or the
        ctypes injection failed unexpectedly.
    """
    with _active_runs_lock:
        entry = _active_runs.get(run_id)
        if entry is None:
            logger.warning("force_cancel_run: unknown run_id=%s", run_id)
            return {"ok": False, "error": f"unknown run_id: {run_id}"}
        entry["cancelled"] = True
        entry["force_cancelled"] = True
        entry["event"].set()
        thread = entry["thread"]

    tid = thread.ident
    if tid is None:
        logger.warning(
            "run[%s]: force-cancel could not resolve thread id (thread not started?)",
            run_id,
        )
        return {
            "ok": True,
            "cancelled": True,
            "force": True,
            "best_effort": True,
            "injected": False,
            "warning": "thread id not available",
        }

    # PyThreadState_SetAsyncExc takes (long thread_id, PyObject* exc) and
    # returns the number of threads modified. Returns:
    #   0  → invalid thread id (worker likely already exited)
    #   1  → success
    #  >1  → catastrophic; immediately undo by passing NULL
    n = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(tid),
        ctypes.py_object(KeyboardInterrupt),
    )
    if n == 0:
        logger.warning(
            "run[%s]: force-cancel injection failed (thread tid=%s no longer exists)",
            run_id, tid,
        )
        return {
            "ok": True,
            "cancelled": True,
            "force": True,
            "best_effort": True,
            "injected": False,
            "warning": "thread no longer running",
        }
    if n > 1:
        # Undo the over-broad injection per Python docs.
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(tid), ctypes.c_long(0))
        logger.error(
            "run[%s]: force-cancel injection affected %d threads — rolled back",
            run_id, n,
        )
        return {
            "ok": False,
            "error": f"PyThreadState_SetAsyncExc affected {n} threads (rolled back)",
        }

    logger.info(
        "run[%s]: force-cancel injected KeyboardInterrupt into tid=%s",
        run_id, tid,
    )
    return {
        "ok": True,
        "cancelled": True,
        "force": True,
        "best_effort": True,
        "injected": True,
    }
