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
        "[run_thread] Step 1: Thread started for run_id=%s, function=%s, variants=%d, "
        "schema_level=%s, schema_filter=%s, where_filters=%s, run_options=%s",
        run_id, function_name, len(variants or []),
        schema_level, _summarize_schema_filter(schema_filter),
        len(where_filters) if where_filters else 0,
        run_options,
    )

    def emit(text: str):
        logger.debug("[run_thread] Emitting output for run_id=%s: %s", run_id, text.rstrip())
        push_message({"type": "run_output", "run_id": run_id, "text": text})

    # Register this run so cancel_run/force_cancel_run can find it.
    logger.info("[run_thread] Step 2: Registering run in active runs registry (run_id=%s)", run_id)
    cancel_event = threading.Event()
    with _active_runs_lock:
        _active_runs[run_id] = {
            "event": cancel_event,
            "thread": threading.current_thread(),
            "cancelled": False,
            "force_cancelled": False,
        }
    logger.debug("[run_thread] Run registered successfully (run_id=%s)", run_id)

    def _is_cancelled() -> bool:
        return cancel_event.is_set()

    # The DatabaseManager is stored in thread-local storage by configure_database().
    # Background threads don't inherit that local, so we re-register it here.
    logger.info("[run_thread] Step 3: Setting current database for thread (run_id=%s)", run_id)
    db.set_current_db()

    logger.info("[run_thread] Step 4: Looking up function '%s' in registry (run_id=%s)", function_name, run_id)
    try:
        fn = registry.get_function(function_name)
        logger.debug("[run_thread] Function found: %s (run_id=%s)", fn, run_id)
    except KeyError as e:
        logger.warning("[run_thread] Function not found: %s (run_id=%s)", e, run_id)
        push_message({"type": "run_done", "run_id": run_id, "success": False,
                      "error": str(e), "cancelled": False})
        with _active_runs_lock:
            _active_runs.pop(run_id, None)
        logger.info("[run_thread] Thread exiting due to function not found (run_id=%s)", run_id)
        return

    # Look up input/output types for this function from the DB.
    logger.info("[run_thread] Step 5: Querying DB for pipeline variants (run_id=%s)", run_id)
    all_variants = db.list_pipeline_variants()
    fn_variants = [v for v in all_variants if v["function_name"] == function_name]
    logger.debug("[run_thread] Found %d variants for function '%s' (run_id=%s)",
                 len(fn_variants), function_name, run_id)

    from scidb.log import Log
    Log.info(f"run: fn_variants for '{function_name}' = {fn_variants}")

    # --- Check manual edges for current output wiring ---
    # If the user has rewired the function's outputs (e.g. deleted an old output
    # node and connected a new one), manual edges should override DB-derived
    # output types.  This prevents stale DB history from resurrecting old nodes.
    logger.info("[run_thread] Step 6: Checking manual edges for output wiring (run_id=%s)", run_id)
    from scistack_gui import pipeline_store, layout as layout_store
    from scistack_gui.api.pipeline import _fn_params_from_registry
    from scistack_gui.domain.edge_resolver import (
        resolve_function_edges, infer_manual_fn_output_types,
    )

    all_edges = pipeline_store.get_manual_edges(db)
    manual_nodes = pipeline_store.get_manual_nodes(db)
    logger.debug("[run_thread] Found %d manual edges, %d manual nodes (run_id=%s)",
                 len(all_edges), len(manual_nodes), run_id)

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
    logger.debug("[run_thread] Inferred manual output types: %s (run_id=%s)",
                 manual_output_types, run_id)

    if fn_variants and manual_output_types:
        # Override output types in DB-derived variants with the current wiring.
        logger.info("[run_thread] Step 7: Overriding DB output types with manual edge outputs: %s (run_id=%s)",
                    manual_output_types, run_id)
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
        logger.debug("[run_thread] Overridden to %d variants (run_id=%s)", len(fn_variants), run_id)

    if not fn_variants:
        # No DB history yet — try to infer inputs/outputs from manual edges.
        logger.info("[run_thread] Step 8: No DB variants found, inferring from manual edges (run_id=%s)", run_id)
        Log.info(f"run: all_edges = {all_edges}")
        Log.info(f"run: manual_nodes = {manual_nodes}")
        Log.info(f"run: fn_node_ids = {fn_node_ids}")

        sig_params = _fn_params_from_registry(function_name)
        logger.debug("[run_thread] Function signature params: %s (run_id=%s)", sig_params, run_id)
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

        logger.info("[run_thread] Edge resolution complete: inputs=%s, outputs=%s, constants=%s (run_id=%s)",
                    input_types, output_types, constant_names, run_id)
        Log.info(f"run: after edge scan: input_types={input_types}, output_types={output_types}, "
                 f"constant_names={constant_names}")

        if not output_types:
            logger.warning("[run_thread] No outputs found for '%s' from DB or edges (run_id=%s)",
                          function_name, run_id)
            Log.warn(f"run: no outputs found for '{function_name}' from DB or edges")
            push_message({"type": "run_done", "run_id": run_id, "success": False,
                          "error": f"No pipeline history or output connections found for '{function_name}'. "
                                    "Connect it to an output variable node first."})
            logger.info("[run_thread] Thread exiting due to no outputs (run_id=%s)", run_id)
            return

        # Collect constant values from pending constants for wired constant nodes.
        logger.info("[run_thread] Step 9: Collecting pending constants for wired nodes (run_id=%s)", run_id)
        import ast as _ast
        inferred_constants: dict[str, list] = {}  # const_name → list of typed values
        if constant_names:
            pending = pipeline_store.get_pending_constants(db)
            logger.debug("[run_thread] Pending constants from DB: %s (run_id=%s)", pending, run_id)
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
                    logger.debug("[run_thread] Constant '%s' has %d values (run_id=%s)",
                                cname, len(typed_vals), run_id)
                else:
                    logger.warning("[run_thread] Constant '%s' wired to '%s' has no pending values (run_id=%s)",
                                  cname, function_name, run_id)
                    Log.warn(f"run: constant '{cname}' wired to '{function_name}' but has no pending values")

        logger.info("[run_thread] Inferred constants: %s (run_id=%s)",
                    list(inferred_constants.keys()), run_id)
        Log.info(f"run: inferred inputs={input_types} outputs={output_types} "
                 f"constants={list(inferred_constants.keys())} for '{function_name}' from edges")

        # Build synthetic variants: cross-product of output types × constant combos.
        logger.info("[run_thread] Step 10: Building synthetic variants from inferred data (run_id=%s)", run_id)
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
            logger.debug("[run_thread] Built %d synthetic variants from constant combinations (run_id=%s)",
                        len(fn_variants), run_id)
        else:
            fn_variants = [
                {"input_types": input_types, "output_type": out, "constants": {}}
                for out in output_types
            ]
            logger.debug("[run_thread] Built %d synthetic variants without constants (run_id=%s)",
                        len(fn_variants), run_id)

    # --- Variant resolution via domain layer ---
    logger.info("[run_thread] Step 11: Resolving variants to execute (run_id=%s)", run_id)
    from scistack_gui.domain.variant_resolver import (
        filter_variants, deduplicate_variants,
    )

    # Determine which variants to run.
    if variants:
        logger.debug("[run_thread] Filtering %d DB variants to requested %d variants (run_id=%s)",
                    len(fn_variants), len(variants), run_id)
        targets = filter_variants(fn_variants, variants)
    else:
        logger.debug("[run_thread] Using all %d DB variants (run_id=%s)", len(fn_variants), run_id)
        targets = fn_variants

    unique_targets = deduplicate_variants(targets)
    logger.debug("[run_thread] After deduplication: %d unique targets (run_id=%s)",
                len(unique_targets), run_id)

    # Get pending constants to override during execution.
    # Note: Pending constants are applied per-variant during input construction
    # rather than creating synthetic cross-product variants.
    from scistack_gui import pipeline_store as _ps
    pending_consts = _ps.get_pending_constants(db)
    if pending_consts:
        logger.info("[run_thread] Pending constants will override DB values: %s (run_id=%s)",
                    list(pending_consts.keys()), run_id)

    # Schema iteration will be handled directly by for_each via schema_filter and schema_level.
    logger.info("[run_thread] Step 12: Schema iteration parameters will be handled by for_each (run_id=%s)", run_id)
    if schema_level:
        logger.debug("[run_thread] Schema level: %s (run_id=%s)", schema_level, run_id)
    if schema_filter:
        logger.debug("[run_thread] Schema filter: %s (run_id=%s)",
                     {k: f"{len(v)} values" for k, v in schema_filter.items()}, run_id)

    # Extract run options (dry_run, save, distribute, as_table).
    logger.info("[run_thread] Step 13: Extracting run options (run_id=%s)", run_id)
    opts = run_options or {}
    opt_dry_run = opts.get("dry_run", False)
    opt_save = opts.get("save", True)
    opt_distribute = opts.get("distribute", False)
    opt_as_table = opts.get("as_table", False)
    logger.debug("[run_thread] Run options: dry_run=%s, save=%s, distribute=%s, as_table=%s (run_id=%s)",
                opt_dry_run, opt_save, opt_distribute, opt_as_table, run_id)

    success = True
    run_started_at = time.time()
    # Build where= argument from where_filters.
    logger.info("[run_thread] Step 14: Building where filters (run_id=%s)", run_id)
    where_arg = _build_where(where_filters)
    if where_arg:
        logger.debug("[run_thread] Where filters built: %s (run_id=%s)", where_arg, run_id)

    logger.info(
        "[run_thread] Step 15: Starting execution of %d target(s) for '%s' "
        "(dry_run=%s, save=%s, distribute=%s, as_table=%s, schema_level=%s, schema_filter=%s) (run_id=%s)",
        len(unique_targets), function_name,
        opt_dry_run, opt_save, opt_distribute, opt_as_table,
        schema_level, _summarize_schema_filter(schema_filter), run_id,
    )

    cancelled = False
    try:
        for idx, v in enumerate(unique_targets, 1):
            # Cooperative cancel: stop before launching the next variant.
            if _is_cancelled():
                logger.info("[run_thread] Cancel detected between variants — stopping (run_id=%s, target=%d/%d)",
                            run_id, idx, len(unique_targets))
                cancelled = True
                emit("⛔ Cancelled\n")
                break
            # Build inputs dict: variable class inputs + scalar constants
            logger.info("[run_thread] Step 16.%d: Processing target %d/%d (run_id=%s)",
                       idx, idx, len(unique_targets), run_id)
            try:
                logger.debug("[run_thread] Building inputs for target %d (run_id=%s)", idx, run_id)
                inputs = {}
                for param, type_names in v["input_types"].items():
                    # type_names may be a list (new) or a string (from DB history).
                    if isinstance(type_names, list):
                        if len(type_names) > 1:
                            from scidb import EachOf
                            inputs[param] = EachOf(*(registry.get_variable_class(t) for t in type_names))
                            logger.debug("[run_thread] Input '%s' is EachOf with %d types (run_id=%s)",
                                       param, len(type_names), run_id)
                        else:
                            inputs[param] = registry.get_variable_class(type_names[0])
                            logger.debug("[run_thread] Input '%s' is single type: %s (run_id=%s)",
                                       param, type_names[0], run_id)
                    else:
                        inputs[param] = registry.get_variable_class(type_names)
                        logger.debug("[run_thread] Input '%s' is type: %s (run_id=%s)",
                                   param, type_names, run_id)
                inputs.update(v["constants"])   # add constants
                logger.debug("[run_thread] Added constants to inputs: %s (run_id=%s)",
                           v["constants"], run_id)

                # Override with pending constants if any match this variant's constants.
                if pending_consts:
                    import ast as _ast
                    for const_name, pending_values in pending_consts.items():
                        if const_name in v["constants"]:
                            # Use the first pending value (Strategy 2: simplest approach)
                            pending_str = next(iter(pending_values))
                            try:
                                pending_typed = _ast.literal_eval(pending_str)
                            except (ValueError, SyntaxError):
                                pending_typed = pending_str
                            inputs[const_name] = pending_typed
                            logger.info("[run_thread] Overriding constant '%s' with pending value: %s (run_id=%s)",
                                       const_name, pending_typed, run_id)

                OutputCls = registry.get_variable_class(v["output_type"])
                logger.debug("[run_thread] Output class: %s (run_id=%s)", v["output_type"], run_id)
            except KeyError as e:
                logger.error("[run_thread] Failed to resolve input/output types for target %d: %s (run_id=%s)",
                           idx, e, run_id)
                emit(f"Error: {e}\n")
                success = False
                continue

            # Build label from actual constants that will be used (after pending overrides)
            actual_constants = {k: val for k, val in inputs.items()
                               if k in v["constants"] or (pending_consts and k in pending_consts)}
            label = f"{function_name}({', '.join(f'{k}={val}' for k, val in actual_constants.items())})" \
                    if actual_constants else function_name
            logger.info(
                "[run_thread] Target %d/%d -> %s, inputs=%s, output=%s (run_id=%s)",
                idx, len(unique_targets), label,
                {k: (type_names if isinstance(type_names, list) else [type_names])
                 for k, type_names in v["input_types"].items()},
                v["output_type"], run_id,
            )
            emit(f"▶ Running {label}\n")

            # Emit structured run_start message for the frontend.
            logger.debug("[run_thread] Emitting run_start message for target %d (run_id=%s)", idx, run_id)
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
                logger.debug("[run_thread] Progress update: event=%s, current=%d, total=%d (run_id=%s)",
                           info["event"], info["current"], info["total"], run_id)
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
            logger.info("[run_thread] Step 17.%d: Executing for_each for target %d (run_id=%s)",
                       idx, idx, run_id)
            buf = StringIO()
            try:
                logger.debug("[run_thread] Redirecting stdout to buffer (run_id=%s)", run_id)
                with redirect_stdout(buf):
                    for_each(fn, inputs=inputs, outputs=[OutputCls],
                             dry_run=opt_dry_run, save=opt_save,
                             distribute=opt_distribute,
                             as_table=opt_as_table,
                             where=where_arg,
                             skip_computed=False,
                             _progress_fn=_progress_fn,
                             _cancel_check=_is_cancelled,
                             schema_filter=schema_filter,
                             schema_level=schema_level)
                output = buf.getvalue()
                if output:
                    logger.debug("[run_thread] Captured %d bytes of stdout (run_id=%s)",
                               len(output), run_id)
                    emit(output)
                target_ms = int((time.time() - started_at) * 1000)
                logger.info("[run_thread] Target %d/%d (%s) completed successfully in %d ms (run_id=%s)",
                            idx, len(unique_targets), label, target_ms, run_id)
            except KeyboardInterrupt:
                # Force-cancel injected an interrupt into this thread (or the
                # user pressed Ctrl-C in CLI mode).  Treat as cancel and stop.
                logger.warning(
                    "[run_thread] Target %d/%d (%s) interrupted by KeyboardInterrupt (force-cancel) (run_id=%s)",
                    idx, len(unique_targets), label, run_id,
                )
                output = buf.getvalue()
                if output:
                    logger.debug("[run_thread] Emitting %d bytes of partial stdout (run_id=%s)",
                               len(output), run_id)
                    emit(output)
                cancelled = True
                emit("⛔ Force-cancelled\n")
                break
            except Exception as exc:
                logger.exception("[run_thread] Target %d/%d (%s) failed with exception (run_id=%s)",
                                idx, len(unique_targets), label, run_id)
                emit(f"Error: {exc}\n")
                success = False
    except KeyboardInterrupt:
        # Defence in depth: if KeyboardInterrupt slips past the per-target
        # handler (e.g. fired between targets), still cancel cleanly.
        logger.warning("[run_thread] Interrupted by KeyboardInterrupt at top level (run_id=%s)",
                       run_id)
        cancelled = True
        emit("⛔ Force-cancelled\n")
    finally:
        logger.info("[run_thread] Step 18: Cleanup and completion (run_id=%s)", run_id)
        duration_ms = int((time.time() - run_started_at) * 1000)
        # Read the final cancel flags from the registry before popping it.
        logger.debug("[run_thread] Removing run from active registry (run_id=%s)", run_id)
        with _active_runs_lock:
            entry = _active_runs.pop(run_id, None)
        was_force = bool(entry and entry.get("force_cancelled"))
        if cancel_event.is_set():
            cancelled = True
        logger.info(
            "[run_thread] Thread finished (success=%s, cancelled=%s, force=%s) in %d ms (run_id=%s)",
            success, cancelled, was_force, duration_ms, run_id,
        )
        logger.debug("[run_thread] Emitting run_done message (run_id=%s)", run_id)
        push_message({"type": "run_done", "run_id": run_id, "success": success,
                      "duration_ms": duration_ms,
                      "cancelled": cancelled, "force_cancelled": was_force})
        logger.debug("[run_thread] Emitting dag_updated message (run_id=%s)", run_id)
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
    logger.info("[api/run] POST /api/run - Validating request")
    logger.debug("[api/run] Request: function_name=%s, variants=%d, run_id=%s, schema_filter=%s, "
                "schema_level=%s, run_options=%s, where_filters=%d",
                req.function_name, len(req.variants), req.run_id,
                _summarize_schema_filter(req.schema_filter), req.schema_level,
                req.run_options, len(req.where_filters) if req.where_filters else 0)

    run_id = req.run_id or str(uuid.uuid4())[:8]
    logger.info("[api/run] Generated run_id: %s", run_id)

    logger.info("[api/run] Spawning background thread for run_id=%s", run_id)
    thread = threading.Thread(
        target=_run_in_thread,
        args=(run_id, req.function_name, req.variants, db,
              req.schema_filter, req.schema_level, req.run_options,
              req.where_filters),
        daemon=True,
    )
    thread.start()
    logger.info("[api/run] Background thread started for run_id=%s", run_id)
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
    logger.info("[cancel_run] Attempting cooperative cancel for run_id=%s", run_id)
    with _active_runs_lock:
        entry = _active_runs.get(run_id)
        if entry is None:
            logger.warning("[cancel_run] Unknown run_id=%s (not in active runs)", run_id)
            return {"ok": False, "error": f"unknown run_id: {run_id}"}
        logger.debug("[cancel_run] Setting cancelled flag and event for run_id=%s", run_id)
        entry["cancelled"] = True
        entry["event"].set()
    logger.info("[cancel_run] Cooperative cancel requested for run_id=%s", run_id)
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
    logger.info("[force_cancel_run] Attempting force cancel for run_id=%s", run_id)
    with _active_runs_lock:
        entry = _active_runs.get(run_id)
        if entry is None:
            logger.warning("[force_cancel_run] Unknown run_id=%s (not in active runs)", run_id)
            return {"ok": False, "error": f"unknown run_id: {run_id}"}
        logger.debug("[force_cancel_run] Setting cancelled and force_cancelled flags for run_id=%s", run_id)
        entry["cancelled"] = True
        entry["force_cancelled"] = True
        entry["event"].set()
        thread = entry["thread"]

    tid = thread.ident
    if tid is None:
        logger.warning(
            "[force_cancel_run] Could not resolve thread id for run_id=%s (thread not started?)",
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

    logger.info("[force_cancel_run] Injecting KeyboardInterrupt into thread tid=%s (run_id=%s)", tid, run_id)
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
            "[force_cancel_run] Injection failed - thread tid=%s no longer exists (run_id=%s)",
            tid, run_id,
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
        logger.error(
            "[force_cancel_run] Injection affected %d threads - rolling back (run_id=%s)",
            n, run_id,
        )
        ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(tid), ctypes.c_long(0))
        return {
            "ok": False,
            "error": f"PyThreadState_SetAsyncExc affected {n} threads (rolled back)",
        }

    logger.info(
        "[force_cancel_run] Successfully injected KeyboardInterrupt into tid=%s (run_id=%s)",
        tid, run_id,
    )
    return {
        "ok": True,
        "cancelled": True,
        "force": True,
        "best_effort": True,
        "injected": True,
    }
