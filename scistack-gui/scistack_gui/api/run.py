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
import threading
import time
import uuid
from collections.abc import Callable
from contextlib import redirect_stdout
from io import StringIO

from fastapi import APIRouter
from pydantic import BaseModel
from scidb.database import DatabaseManager
from scidb.foreach_config import RunOptions

from scidb import for_each
from scistack_gui import registry
from scistack_gui.api.handlers import Handler, install_routes
from scistack_gui.api.ws import push_message
from scistack_gui.db import external_db_access
from scistack_gui.domain.schema_selection import is_empty

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
    variable: str  # variable type name, e.g. "Side"
    op: str  # "==", "!=", "<", "<=", ">", ">=", "IN"
    value: str  # always string, coerced on backend


class RunRequest(BaseModel):
    function_name: str
    variants: list[dict] = []  # list of constants dicts; empty = run all known
    run_id: str | None = None  # frontend-generated ID; we generate one if absent
    # The picker's pair: ragged `include` prefixes + a standing
    # `exclude_levels` rule. Replaces the old per-key `schema_filter`, which
    # could only express a Cartesian product. None/empty = everything.
    schema_selection: dict | None = None
    schema_level: list[str] | None = None  # which schema keys to iterate; None = all
    run_options: dict | None = None  # {dry_run, save, distribute}; all optional
    where_filters: list[WhereFilterSpec] | None = None  # data filters for where= param
    # The canvas node the user actually clicked Run on. Optional for back-
    # compat, but required to disambiguate once >1 node shares function_name
    # with a different wiring (see execution_service.derive_target_for_node)
    # — without it, targets are derived by NAME alone and a click on one
    # node's Run button can silently execute a DIFFERENT node's real history.
    node_id: str | None = None
    # A hint from the canvas node's data.language. NOT authoritative — the
    # backend asks matlab_registry itself (see route_matlab_single_run), so
    # a browser client that never sends this still gets MATLAB routing.
    language: str | None = None
    # MATLAB output parameter names for command generation when the function
    # has no DB history yet. Consumed by generate_matlab_command; ignored on
    # the Python path.
    output_types: list[str] | None = None


def _run_in_thread(
    run_id: str,
    function_name: str,
    variants: list[dict],
    db: DatabaseManager,
    schema_selection: dict | None = None,
    schema_level: list[str] | None = None,
    run_options: dict | None = None,
    where_filters: list[WhereFilterSpec] | None = None,
    node_id: str | None = None,
):
    """
    Executed in a background thread. Runs for_each for each variant,
    captures stdout line-by-line, and pushes it to the WebSocket queue.
    """
    logger.info(
        "[run_thread] Thread started for run_id=%s, function=%s, variants=%d, "
        "schema_level=%s, schema_selection=%s, where_filters=%s, run_options=%s",
        run_id,
        function_name,
        len(variants or []),
        schema_level,
        _summarize_selection(schema_selection),
        len(where_filters) if where_filters else 0,
        run_options,
    )

    def emit(text: str):
        logger.debug(
            "[run_thread] Emitting output for run_id=%s: %s", run_id, text.rstrip()
        )
        push_message({"type": "run_output", "run_id": run_id, "text": text})

    class _RunLogRelay(logging.Handler):
        """Relays scifor/scidb INFO+ log records to the frontend run console.

        The pipeline narrative (banner, progress, run summary, failures) is
        log records since the logging redesign — stdout only carries dry-run
        output — so the frontend gets it from a scoped handler instead.
        Attached to the "scifor"/"scidb" loggers only (never "scistack_gui",
        whose records include emit()'s own debug line — a feedback loop).
        """

        _RELAY_LOGGERS = ("scifor", "scidb")

        def __init__(self):
            super().__init__(level=logging.INFO)
            self.setFormatter(logging.Formatter("%(message)s"))

        def emit(self, record):
            try:
                emit(self.format(record) + "\n")
            except Exception:
                pass

        def __enter__(self):
            for name in self._RELAY_LOGGERS:
                logging.getLogger(name).addHandler(self)
            return self

        def __exit__(self, *exc):
            for name in self._RELAY_LOGGERS:
                logging.getLogger(name).removeHandler(self)
            return False

    # Register this run so cancel_run/force_cancel_run can find it.
    logger.info(
        "[run_thread] Registering run in active runs registry (run_id=%s)", run_id
    )
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
    logger.info("[run_thread] Setting current database for thread (run_id=%s)", run_id)
    db.set_current_db()

    logger.info(
        "[run_thread] Looking up function '%s' in registry (run_id=%s)",
        function_name,
        run_id,
    )
    try:
        fn = registry.get_function(function_name)
        logger.debug("[run_thread] Function found: %s (run_id=%s)", fn, run_id)
    except KeyError as e:
        logger.warning("[run_thread] Function not found: %s (run_id=%s)", e, run_id)
        push_message(
            {
                "type": "run_done",
                "run_id": run_id,
                "success": False,
                "error": str(e),
                "cancelled": False,
            }
        )
        with _active_runs_lock:
            _active_runs.pop(run_id, None)
        logger.info(
            "[run_thread] Thread exiting due to function not found (run_id=%s)", run_id
        )
        return

    # Derive the for_each targets for this function node — DB history with
    # manual-wiring overrides, or the manual-edge fallback. Shared with the
    # pipeline compiler (execution_service) so per-node and pipeline runs
    # derive identically. When node_id is given, scope derivation to that
    # EXACT node's own wiring (derive_target_for_node) rather than every
    # node/call site sharing function_name — required once more than one
    # wiring of the same function name can coexist on a canvas.
    logger.info(
        "[run_thread] Deriving targets for '%s' (node_id=%s, run_id=%s)",
        function_name,
        node_id,
        run_id,
    )
    if node_id:
        from scistack_gui.services.execution_service import derive_target_for_node

        fn_variants = derive_target_for_node(db, node_id)
    else:
        from scistack_gui.services.execution_service import derive_fn_targets

        fn_variants = derive_fn_targets(db, function_name)
    logger.debug(
        "[run_thread] Derived %d target(s) for '%s' (run_id=%s)",
        len(fn_variants),
        function_name,
        run_id,
    )
    if not fn_variants:
        from scistack_gui.services.execution_service import disconnected_reason

        reason = disconnected_reason(db, function_name, node_id)
        error = (
            f"'{function_name}' can't run: {reason}."
            if reason
            else f"No pipeline history or output connections found for '{function_name}'. "
            "Connect it to an output variable node first."
        )
        logger.info(
            "[run_thread] no targets for '%s' (run_id=%s): %s",
            function_name,
            run_id,
            error,
        )
        push_message(
            {
                "type": "run_done",
                "run_id": run_id,
                "success": False,
                "error": error,
            }
        )
        with _active_runs_lock:
            _active_runs.pop(run_id, None)
        logger.info("[run_thread] Thread exiting due to no targets (run_id=%s)", run_id)
        return

    # --- Variant resolution via domain layer ---
    logger.info("[run_thread] Resolving variants to execute (run_id=%s)", run_id)
    from scistack_gui.domain.variant_resolver import (
        deduplicate_variants,
        filter_variants,
    )

    # Determine which variants to run.
    if variants:
        logger.debug(
            "[run_thread] Filtering %d DB variants to requested %d variants (run_id=%s)",
            len(fn_variants),
            len(variants),
            run_id,
        )
        targets = filter_variants(fn_variants, variants)
    else:
        logger.debug(
            "[run_thread] Using all %d DB variants (run_id=%s)",
            len(fn_variants),
            run_id,
        )
        targets = fn_variants

    # Staged pending constants override DB values on the derived targets —
    # shared helper with the pipeline compiler so eager and pull runs
    # materialize staged values identically. Deduplicate AFTER overriding:
    # targets differing only in the overridden constant collapse together.
    from scistack_gui import pipeline_store as _ps
    from scistack_gui.services.execution_service import apply_pending_overrides

    pending_consts = _ps.get_pending_constants(db)
    if pending_consts:
        logger.info(
            "[run_thread] Pending constants will override DB values: %s (run_id=%s)",
            list(pending_consts.keys()),
            run_id,
        )
    unique_targets = deduplicate_variants(
        apply_pending_overrides(targets, pending_consts)
    )
    logger.debug(
        "[run_thread] After override + deduplication: %d unique targets (run_id=%s)",
        len(unique_targets),
        run_id,
    )

    # Extract run options (dry_run, save, distribute, as_table).
    logger.info("[run_thread] Extracting run options (run_id=%s)", run_id)
    opts = run_options or {}
    opt_dry_run = opts.get("dry_run", False)
    opt_save = opts.get("save", True)
    opt_distribute = opts.get("distribute", False)
    opt_as_table = opts.get("as_table", False)
    logger.debug(
        "[run_thread] Run options: dry_run=%s, save=%s, distribute=%s, as_table=%s (run_id=%s)",
        opt_dry_run,
        opt_save,
        opt_distribute,
        opt_as_table,
        run_id,
    )

    # Hidden combos (see plan-combo-hiding.md) — never delete data, just
    # exclude a specific already-hidden Cartesian-product row from actually
    # running, using THIS chokepoint's own distribute/as_table so a hidden
    # pending combo's stored call_id stays consistent with what would
    # actually execute here.
    from scistack_gui.domain.variant_resolver import (
        filter_hidden_targets,
        hidden_call_ids_for_fn,
    )

    # The hides of the canvas the click came from; a run with no node id
    # (the name-scoped fallback) still sees every scope's.
    from scistack_gui import intent_store

    hidden_ids = _ps.get_hidden_node_ids(
        db, intent_store.scope_of_node(db, node_id) if node_id else None
    )
    before_hidden_filter = len(unique_targets)
    unique_targets = filter_hidden_targets(
        unique_targets,
        function_name,
        hidden_call_ids_for_fn(hidden_ids, function_name),
        pending_consts,
        RunOptions(distribute=opt_distribute, as_table=opt_as_table),
    )
    if len(unique_targets) != before_hidden_filter:
        logger.info(
            "[run_thread] %d target(s) excluded as hidden (run_id=%s)",
            before_hidden_filter - len(unique_targets),
            run_id,
        )

    # Schema iteration is handled by for_each via schema_level — but for_each
    # ONLY auto-iterates when it is set. Left None it pools every schema row
    # into a single call, which is never what a canvas Run means for per-combo
    # functions (the seed scripts pass explicit iterables). Default to
    # iterating ALL schema keys — EXCEPT when the user explicitly chose
    # as_table, which means "pool the rows".
    #
    # `schema_selection` does NOT suppress this, and the old `schema_filter`
    # did: a filtered run then established no iteration at all and silently
    # pooled. A location selection says WHICH combos to run, never whether to
    # iterate, so the two are now independent.
    if schema_level is None and not opt_as_table:
        # One owner for the default (execution_service.default_schema_level):
        # the node's own level, else where the function last ran, else the
        # level its inputs imply, else every key. See
        # docs/claude/intent-and-fact.md rule 4.
        from scistack_gui.services.execution_service import default_schema_level

        schema_level, why = default_schema_level(db, function_name, unique_targets)
        logger.info(
            "[run_thread] No schema iteration requested — iterating %s (%s) "
            "(run_id=%s)",
            schema_level if schema_level is not None else "nothing: one call",
            why,
            run_id,
        )
    if schema_level:
        logger.debug("[run_thread] Schema level: %s (run_id=%s)", schema_level, run_id)
    if not is_empty(schema_selection):
        # INFO, not DEBUG: this changes which combos run, and a run that does
        # less than expected is read as a broken pipeline unless the log says
        # a selection was in force. scifor logs the resulting counts.
        logger.info(
            "[run_thread] Schema selection: %s (run_id=%s)",
            _summarize_selection(schema_selection),
            run_id,
        )

    success = True
    run_started_at = time.time()
    # Accumulated across targets from scifor's authoritative "summary"
    # progress events: for_each never raises on iteration failures
    # (continue-and-report), so success must be decided from these counts —
    # NOT from "the for_each call returned". skip_computed skips are
    # removed before the loop and never inflate 'failed'.
    combo_totals = {"completed": 0, "failed": 0, "no_data": 0}
    # Build where= argument from where_filters.
    logger.info("[run_thread] Building where filters (run_id=%s)", run_id)
    where_arg = _build_where(where_filters)
    if where_arg:
        logger.debug(
            "[run_thread] Where filters built: %s (run_id=%s)", where_arg, run_id
        )

    logger.info(
        "[run_thread] Starting execution of %d target(s) for '%s' "
        "(dry_run=%s, save=%s, distribute=%s, as_table=%s, schema_level=%s, schema_selection=%s) (run_id=%s)",
        len(unique_targets),
        function_name,
        opt_dry_run,
        opt_save,
        opt_distribute,
        opt_as_table,
        schema_level,
        _summarize_selection(schema_selection),
        run_id,
    )

    cancelled = False
    try:
        for idx, v in enumerate(unique_targets, 1):
            # Cooperative cancel: stop before launching the next variant.
            if _is_cancelled():
                logger.info(
                    "[run_thread] Cancel detected between variants — stopping (run_id=%s, target=%d/%d)",
                    run_id,
                    idx,
                    len(unique_targets),
                )
                cancelled = True
                emit("⛔ Cancelled\n")
                break
            # Build inputs dict: variable class inputs + scalar constants
            logger.info(
                "[run_thread] Processing target %d/%d (run_id=%s)",
                idx,
                len(unique_targets),
                run_id,
            )
            try:
                logger.debug(
                    "[run_thread] Building inputs for target %d (run_id=%s)",
                    idx,
                    run_id,
                )
                # Variable-class inputs + scalar constants (pending overrides
                # already applied above the loop) + any PathInput-backed
                # params — shared with the pipeline-compiler path so this
                # logic (including PathInput construction) lives in one
                # place. See execution_service.build_run_inputs.
                from scistack_gui.services.execution_service import (
                    build_run_glue,
                    build_run_inputs,
                )

                inputs = build_run_inputs(v, function_name)
                # Glue is a property of this step's input binding, resolved
                # right beside the inputs it reshapes — never a step, never a
                # run of its own.
                glue_arg = build_run_glue(v, function_name) or None
                logger.debug(
                    "[run_thread] Built inputs for %d param(s): %s (run_id=%s)",
                    len(inputs),
                    list(inputs),
                    run_id,
                )

                OutputCls = registry.get_variable_class(v["output_type"])
                logger.debug(
                    "[run_thread] Output class: %s (run_id=%s)",
                    v["output_type"],
                    run_id,
                )
            except KeyError as e:
                logger.error(
                    "[run_thread] Failed to resolve input/output types for target %d: %s (run_id=%s)",
                    idx,
                    e,
                    run_id,
                )
                emit(f"Error: {e}\n")
                success = False
                continue

            # Target constants are final (pending overrides already applied).
            label = (
                f"{function_name}({', '.join(f'{k}={val}' for k, val in v['constants'].items())})"
                if v["constants"]
                else function_name
            )
            # Every binding kind, tagged by origin. This used to print
            # ``input_types`` alone — variables only — so a healthy
            # PathInput-fed run logged ``inputs={}``, which is the exact
            # signature the zero-combo warning below tells you to look for.
            _binding_summary = {
                param: (
                    b["ref"]
                    if b["kind"] == "variable"
                    else f"{b['kind']}:{b['ref']}"
                )
                for param, b in (v.get("bindings") or {}).items()
            }
            logger.info(
                "[run_thread] Target %d/%d -> %s, inputs=%s, constants=%s, "
                "output=%s (run_id=%s)",
                idx,
                len(unique_targets),
                label,
                _binding_summary or "{} (nothing wired)",
                v.get("constants") or {},
                v["output_type"],
                run_id,
            )
            emit(f"▶ Running {label}\n")

            # Emit structured run_start message for the frontend.
            logger.debug(
                "[run_thread] Emitting run_start message for target %d (run_id=%s)",
                idx,
                run_id,
            )
            started_at = time.time()
            push_message(
                {
                    "type": "run_start",
                    "run_id": run_id,
                    "function_name": function_name,
                    "constants": v["constants"],
                    "input_types": {k: str(vt) for k, vt in v["input_types"].items()},
                    "output_type": v["output_type"],
                    "started_at": started_at,
                }
            )

            # Progress callback: relay structured progress to the frontend.
            def _progress_fn(info: dict):
                # The end-of-run summary carries this target's final counts.
                if info.get("event") == "summary":
                    combo_totals["completed"] += info.get("completed", 0)
                    no_data = info.get("no_data", 0)
                    combo_totals["failed"] += info.get(
                        "failed", info.get("skipped", 0) - no_data
                    )
                    combo_totals["no_data"] += no_data
                # Convert metadata values to strings for JSON serialization.
                meta = {str(k): str(val) for k, val in info.get("metadata", {}).items()}
                logger.debug(
                    "[run_thread] Progress update: event=%s, current=%d, total=%d (run_id=%s)",
                    info["event"],
                    info["current"],
                    info["total"],
                    run_id,
                )
                push_message(
                    {
                        "type": "run_progress",
                        "run_id": run_id,
                        "event": info["event"],
                        "current": info["current"],
                        "total": info["total"],
                        "completed": info["completed"],
                        "skipped": info["skipped"],
                        "no_data": info.get("no_data"),
                        "metadata": meta,
                        "error": info.get("error"),
                    }
                )

            # Relay the run narrative (log records) plus any stdout (dry-run
            # output) to the frontend console.
            logger.info(
                "[run_thread] Executing for_each for target %d (run_id=%s)", idx, run_id
            )
            buf = StringIO()
            target_snapshot = dict(combo_totals)
            try:
                # This run reads the intent store (rule 3): label it so the
                # `_run` row says so, and a later script run of the same
                # function can be told apart from it on the canvas.
                from scidb.intent import ORIGIN_GUI, run_origin

                with _RunLogRelay(), redirect_stdout(buf), run_origin(ORIGIN_GUI):
                    for_each(
                        fn,
                        inputs=inputs,
                        outputs=[OutputCls],
                        dry_run=opt_dry_run,
                        save=opt_save,
                        distribute=opt_distribute,
                        as_table=opt_as_table,
                        where=where_arg,
                        skip_computed=False,
                        glue=glue_arg,
                        _progress_fn=_progress_fn,
                        _cancel_check=_is_cancelled,
                        # The picker's pair, applied to the COMBO list by
                        # scifor. Distinct from schema_keys below, which says
                        # which keys to iterate at all.
                        locations=schema_selection,
                        # NOTE: scidb.for_each's real parameter is
                        # `schema_keys`, not `schema_level` — `schema_level`
                        # is this module's own GUI-facing name for "which
                        # schema keys to iterate" (RunRequest.schema_level,
                        # etc.). Passing `schema_level=` here used to land
                        # in for_each's **metadata_iterables catch-all
                        # instead of actually requesting iteration: it
                        # silently created a bogus metadata axis literally
                        # named "schema_level", leaving the REAL schema key
                        # (e.g. "subject") un-iterated. for_each then
                        # entered aggregation mode and pooled every row
                        # for a given key into one call — functions written
                        # per-combo (e.g. gui_test_data.compute_max_vo2)
                        # then crash on the multi-row table. See
                        # execution_service.build_backend_pipeline's
                        # schema_iterables comment for the same failure
                        # mode, found earlier via a different call path.
                        schema_keys=schema_level,
                    )
                output = buf.getvalue()
                if output:
                    logger.debug(
                        "[run_thread] Captured %d bytes of stdout (run_id=%s)",
                        len(output),
                        run_id,
                    )
                    emit(output)
                target_ms = int((time.time() - started_at) * 1000)
                target_failed = combo_totals["failed"] - target_snapshot["failed"]
                target_no_data = combo_totals["no_data"] - target_snapshot["no_data"]
                target_completed = (
                    combo_totals["completed"] - target_snapshot["completed"]
                )
                if target_failed:
                    logger.warning(
                        "[run_thread] Target %d/%d (%s) finished in %d ms with "
                        "%d failed combo(s) (completed=%d, no_data=%d) (run_id=%s)",
                        idx,
                        len(unique_targets),
                        label,
                        target_ms,
                        target_failed,
                        target_completed,
                        target_no_data,
                        run_id,
                    )
                    emit(
                        f"⚠ {label}: {target_failed} combo(s) failed "
                        f"({target_completed} completed) — see log above\n"
                    )
                else:
                    logger.info(
                        "[run_thread] Target %d/%d (%s) completed successfully in %d ms (run_id=%s)",
                        idx,
                        len(unique_targets),
                        label,
                        target_ms,
                        run_id,
                    )
            except KeyboardInterrupt:
                # Force-cancel injected an interrupt into this thread (or the
                # user pressed Ctrl-C in CLI mode).  Treat as cancel and stop.
                logger.warning(
                    "[run_thread] Target %d/%d (%s) interrupted by KeyboardInterrupt (force-cancel) (run_id=%s)",
                    idx,
                    len(unique_targets),
                    label,
                    run_id,
                )
                output = buf.getvalue()
                if output:
                    logger.debug(
                        "[run_thread] Emitting %d bytes of partial stdout (run_id=%s)",
                        len(output),
                        run_id,
                    )
                    emit(output)
                cancelled = True
                emit("⛔ Force-cancelled\n")
                break
            except Exception as exc:
                logger.exception(
                    "[run_thread] Target %d/%d (%s) failed with exception (run_id=%s)",
                    idx,
                    len(unique_targets),
                    label,
                    run_id,
                )
                emit(f"Error: {exc}\n")
                success = False
    except KeyboardInterrupt:
        # Defence in depth: if KeyboardInterrupt slips past the per-target
        # handler (e.g. fired between targets), still cancel cleanly.
        logger.warning(
            "[run_thread] Interrupted by KeyboardInterrupt at top level (run_id=%s)",
            run_id,
        )
        cancelled = True
        emit("⛔ Force-cancelled\n")
    finally:
        logger.info("[run_thread] Cleanup and completion (run_id=%s)", run_id)
        duration_ms = int((time.time() - run_started_at) * 1000)
        # Honest verdict: iteration failures never raise out of for_each,
        # so a run with any failed combos is NOT a success.
        if combo_totals["failed"] > 0:
            success = False
        # Read the final cancel flags from the registry before popping it.
        logger.debug(
            "[run_thread] Removing run from active registry (run_id=%s)", run_id
        )
        with _active_runs_lock:
            entry = _active_runs.pop(run_id, None)
        was_force = bool(entry and entry.get("force_cancelled"))
        if cancel_event.is_set():
            cancelled = True
        logger.info(
            "[run_thread] Thread finished (success=%s, cancelled=%s, force=%s, "
            "completed_combos=%d, failed_combos=%d, no_data_combos=%d) in %d ms "
            "(run_id=%s)",
            success,
            cancelled,
            was_force,
            combo_totals["completed"],
            combo_totals["failed"],
            combo_totals["no_data"],
            duration_ms,
            run_id,
        )
        if (
            success
            and not cancelled
            and sum(combo_totals[k] for k in ("completed", "failed", "no_data")) == 0
        ):
            # A run that iterated zero times wrote nothing, so every node it
            # touches stays red while the run itself reports success. Said
            # out loud because the alternative is inferring it from the
            # target's binding line several hundred entries earlier.
            logger.warning(
                "[run_thread] '%s' completed without running a single "
                "combination — nothing was computed and no records were "
                "written, so its nodes will stay red. Check that the "
                "function's inputs are wired to the right handles and that "
                "any PathInput resolves to real files (run_id=%s)",
                function_name,
                run_id,
            )
        logger.debug("[run_thread] Emitting run_done message (run_id=%s)", run_id)
        push_message(
            {
                "type": "run_done",
                "run_id": run_id,
                "success": success,
                "duration_ms": duration_ms,
                "cancelled": cancelled,
                "force_cancelled": was_force,
                "completed_combos": combo_totals["completed"],
                "failed_combos": combo_totals["failed"],
                "no_data_combos": combo_totals["no_data"],
            }
        )
        logger.debug("[run_thread] Emitting dag_updated message (run_id=%s)", run_id)
        _notify_records_changed()


def _notify_records_changed() -> None:
    """Announce that a run has finished and may have written records.

    Two things have to happen together, which is why they are one call:

    1. the canvas re-fetches the DAG (``dag_updated``);
    2. Plot Studio drops its cached frames.

    ``ScidbSource`` caches whole variable frames, so skipping (2) leaves the
    plot panel serving PRE-RUN data — a figure that silently disagrees with the
    database it claims to show. The invalidation RPC existed and was wired end
    to end, but nothing ever called it; this is that missing call, put on the
    backend so the web GUI and the VS Code extension cannot drift (CLAUDE.md
    NOTE 3).

    Takes no database handle deliberately. The cache is keyed by file path, and
    the MATLAB run threads have released their connection to the sidecar by the
    time they get here — so there is nothing to pass, and nothing that needs
    reopening just to drop a cache entry.
    """
    from scistack_gui.db import get_db_path
    from scistack_gui.services import plot_service

    try:
        plot_service.invalidate(get_db_path())
    except Exception:
        # A cache we failed to drop is a stale figure, not a failed run: the
        # user's data is already written. Never let this bury the run result.
        logger.exception("[run] could not invalidate the plot source cache")
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


def _summarize_selection(selection: dict | None) -> str:
    """Compact one-line summary of a schema selection for logging."""
    from scistack_gui.domain.schema_selection import as_selection, is_empty

    if is_empty(selection):
        return "none"
    pair = as_selection(selection)
    parts = []
    if pair["include"]:
        parts.append(f"{len(pair['include'])} location(s)")
    for key, values in pair["exclude_levels"].items():
        parts.append(f"-{len(values)} {key}")
    return ", ".join(parts)




def get_matlab_engine_status() -> dict:
    """Engine state for the GUI indicator: ``{state, pid, error}``.

    ``state`` is ``unavailable`` / ``stopped`` / ``ready`` / ``busy``.
    ``error`` carries a health-probe failure (e.g. no ``pyenv``), which is a
    SETUP problem worth surfacing before a run turns it into a confusing
    scidb traceback.
    """
    from scistack_gui import matlab_sidecar

    return matlab_sidecar.get_sidecar().status()


def restart_matlab_engine() -> dict:
    """Kill and relaunch the kept-warm engine — the manual recovery for a
    wedged session. Also runs the health probe, so a restart reports a
    misconfigured ``pyenv`` immediately rather than at the next run."""
    from scistack_gui import matlab_sidecar

    sidecar = matlab_sidecar.get_sidecar()
    if not sidecar.restart():
        return {"ok": False, "error": "'matlab' not found on PATH."}
    health = sidecar.check_health()
    if health:
        return {"ok": False, "error": health, **sidecar.status()}
    return {"ok": True, **sidecar.status()}


def route_matlab_single_run(
    function_name: str,
    params: dict,
    run_id: str,
    db: DatabaseManager,
    *,
    host_can_dispatch_matlab: bool = False,
) -> "dict | None":
    """Route a single-node Run for a MATLAB function, or return ``None``
    when ``function_name`` is a Python function and the caller should
    continue down its normal ``_run_in_thread`` path.

    This is the single-node counterpart to the ladder
    :func:`start_pipeline_run` already applies to whole pipelines, and it
    lives **here** rather than in the VS Code extension for a reason: the
    extension used to be the only place that knew a run was MATLAB, keyed
    on a ``language`` field the webview happened to send
    (``dagPanel.ts``). Browser/standalone clients never went through that
    host, so a MATLAB node clicked in a browser fell through to the Python
    registry and failed with "Function '…' not found in registry" (todo
    #5). ``matlab_registry`` is the authority now, so every transport
    reaches the same decision.

    - ``host_can_dispatch_matlab=True`` — set only by ``server.py``'s
      JSON-RPC handler, which has the privileged ``dagPanel.ts`` host in
      front of it — returns ``host_execution_required`` so that host can
      generate the script and hand it to the MathWorks terminal (where
      breakpoints actually work). No thread is spawned here.
    - Otherwise the standalone sidecar drives it in-process, emitting real
      ``run_output``/``run_done`` on the same ``run_id``.
    """
    from scistack_gui import matlab_registry

    if not matlab_registry.is_matlab_function(function_name):
        return None

    if host_can_dispatch_matlab:
        logger.info(
            "[api/run] '%s' is a MATLAB function — routing to host-side "
            "execution (run_id=%s)",
            function_name,
            run_id,
        )
        return {
            "run_id": run_id,
            "host_execution_required": True,
            "language": "matlab",
        }

    logger.info(
        "[api/run] '%s' is a MATLAB function — routing to standalone MATLAB "
        "sidecar (run_id=%s)",
        function_name,
        run_id,
    )
    thread = threading.Thread(
        target=_run_matlab_function_in_thread,
        args=(run_id, function_name, params, db),
        daemon=True,
    )
    thread.start()
    return {"run_id": run_id, "language": "matlab"}


def _refuse_glue_node(node_id: "str | None", function_name: str, db) -> "str | None":
    """A message if this run targets a glue node, else ``None``.

    Matched on the node id first (the canvas always sends one) and on the
    ``glue_`` name second, so a run request assembled without a node id is
    refused too.
    """
    from scidb import function_role
    from scistack_gui import pipeline_store
    from scistack_gui.domain.edge_resolver import GLUE_NODE_TYPE
    from scistack_gui.ids import strip_placement

    is_glue = function_role(function_name or "") == "glue"
    if not is_glue and node_id:
        nodes = pipeline_store.get_manual_nodes(db)
        meta = nodes.get(node_id) or nodes.get(strip_placement(node_id)) or {}
        is_glue = meta.get("type") == GLUE_NODE_TYPE
    if not is_glue:
        return None
    return (
        f"'{function_name}' is a glue node, which is never run on its own. It "
        f"reshapes an input in memory as part of the run of whichever function "
        f"consumes it — run that function instead."
    )


def start_run(db: DatabaseManager, req: "RunRequest", *, transport: str) -> dict:
    """Start a single-function run in a background thread — one function
    for both transports.

    Until the table (2026-09-21) the HTTP route and the RPC handler were
    two copies that had drifted: only HTTP refused a glue node and passed
    the clicked ``node_id`` through to the run thread (so the extension
    derived targets by NAME and could run a sibling wiring's history — the
    exact bug ``derive_target_for_node`` documents), and only RPC held the
    DuckDB connection across the run (a ``per_request``-policy need, no-op
    under the standalone process's ``persistent`` policy).

    The one thing the transport legitimately decides: the extension's
    dagPanel.ts can dispatch a MATLAB script to the MathWorks terminal, the
    browser cannot (``host_can_dispatch_matlab``).
    """
    from scistack_gui.db import (
        acquire_db_connection,
        connection_policy,
        release_db_connection,
    )

    logger.info("[api/run] start_run (%s) - Validating request", transport)
    logger.debug(
        "[api/run] Request: function_name=%s, node_id=%s, variants=%d, run_id=%s, "
        "schema_selection=%s, schema_level=%s, run_options=%s, where_filters=%d, "
        "language=%s",
        req.function_name,
        req.node_id,
        len(req.variants),
        req.run_id,
        _summarize_selection(req.schema_selection),
        req.schema_level,
        req.run_options,
        len(req.where_filters) if req.where_filters else 0,
        req.language,
    )

    # A glue node is not a step (D5): it is transient by construction, so
    # there is nothing for a standalone run to produce. Refusing here is the
    # point — compiling an empty pipeline instead would report a *successful*
    # run that did nothing, the documented succeeds-while-doing-no-work
    # failure mode. See docs/claude/free-code-glue-nodes.md §5.
    glue_error = _refuse_glue_node(req.node_id, req.function_name, db)
    if glue_error is not None:
        logger.info("[api/run] refused: %s", glue_error)
        raise ValueError(glue_error)

    run_id = req.run_id or str(uuid.uuid4())[:8]
    logger.info("[api/run] Generated run_id: %s", run_id)

    # MATLAB functions can't run through _run_in_thread's Python registry at
    # all. Decide here, from matlab_registry — not from the caller's
    # `language` hint. Must happen BEFORE the connection is held below:
    # MATLAB needs the DuckDB file lock for its own run, and holding it here
    # for a run this process will never execute would block the very thing
    # we just dispatched.
    routed = route_matlab_single_run(
        req.function_name,
        req.model_dump(),
        run_id,
        db,
        host_can_dispatch_matlab=(transport == "rpc"),
    )
    if routed is not None:
        return routed

    # Under the JSON-RPC server's per-request policy the connection closes
    # whenever its refcount hits zero, so the run thread must hold it for
    # its whole life — acquired HERE, while the dispatch loop still holds
    # it, so there is no window in which it closes and MATLAB takes the
    # file. The standalone process keeps one connection open: no-op.
    hold = connection_policy() == "per_request"
    if hold:
        logger.debug("[api/run] Acquiring DB connection for run thread")
        acquire_db_connection()

    def _run_wrapper():
        try:
            _run_in_thread(
                run_id,
                req.function_name,
                req.variants,
                db,
                req.schema_selection,
                req.schema_level,
                req.run_options,
                req.where_filters,
                req.node_id,
            )
        finally:
            if hold:
                logger.debug("[api/run] Releasing DB connection (run_id=%s)", run_id)
                release_db_connection()

    logger.info("[api/run] Spawning background thread for run_id=%s", run_id)
    thread = threading.Thread(target=_run_wrapper, daemon=True)
    thread.start()
    logger.info("[api/run] Background thread started for run_id=%s", run_id)
    return {"run_id": run_id}


def _run_pipeline_in_thread(
    run_id: str,
    pipeline_id: str,
    mode: str,
    target: str,
    finalized,
    skip_computed: bool,
    db: DatabaseManager,
):
    """Background execution of a document pipeline through the backend
    verbs (G2). Reuses the per-node run's registry (force-cancel works —
    KeyboardInterrupt between/inside steps; cooperative cancel is a no-op
    for pipeline runs in v1: Pipeline._run has no between-step hook yet)
    and its message contract (run_output / run_done / dag_updated).
    """
    from scistack_gui.services.execution_service import run_pipeline

    def emit(text: str):
        push_message({"type": "run_output", "run_id": run_id, "text": text})

    class _RunLogRelay(logging.Handler):
        _RELAY_LOGGERS = ("scifor", "scidb")

        def __init__(self):
            super().__init__(level=logging.INFO)
            self.setFormatter(logging.Formatter("%(message)s"))

        def emit(self, record):
            try:
                emit(self.format(record) + "\n")
            except Exception:
                pass

        def __enter__(self):
            for name in self._RELAY_LOGGERS:
                logging.getLogger(name).addHandler(self)
            return self

        def __exit__(self, *exc):
            for name in self._RELAY_LOGGERS:
                logging.getLogger(name).removeHandler(self)
            return False

    cancel_event = threading.Event()
    with _active_runs_lock:
        _active_runs[run_id] = {
            "event": cancel_event,
            "thread": threading.current_thread(),
            "cancelled": False,
            "force_cancelled": False,
        }
    db.set_current_db()
    logger.info(
        "[pipeline_run] run_id=%s scope=%s mode=%s target=%r",
        run_id,
        pipeline_id,
        mode,
        target,
    )
    emit(
        f"▶ Running pipeline scope {pipeline_id} (mode={mode}"
        + (f", target={target}" if target else "")
        + ")\n"
    )

    success, cancelled = True, False
    run_started_at = time.time()
    buf = StringIO()
    report: list = []
    try:
        with _RunLogRelay(), redirect_stdout(buf):
            result = run_pipeline(
                db,
                pipeline_id,
                mode=mode,
                target=target,
                finalized=finalized,
                skip_computed=skip_computed,
            )
        report = (result or {}).get("report") or []
        # Draft outputs of a show run exist ONLY in this return value (no
        # records are written) — push them to the preview panel before
        # run_done. Payloads may be non-JSON scalars; stringify defensively.
        if mode == "show":
            rendered = (result or {}).get("rendered") or []
            safe = [
                r if isinstance(r, (str, int, float, bool, dict, list)) else str(r)
                for r in rendered
            ]
            push_message(
                {
                    "type": "show_rendered",
                    "run_id": run_id,
                    "step": target,
                    "rendered": safe,
                }
            )
    except KeyboardInterrupt:
        cancelled = True
        emit("⛔ Force-cancelled\n")
    except Exception as exc:
        logger.exception("[pipeline_run] failed (run_id=%s)", run_id)
        emit(f"Error: {exc}\n")
        success = False
    finally:
        output = buf.getvalue()
        if output:
            emit(output)
        # Honest verdict: step for_each calls never raise on iteration
        # failures (continue-and-report), so success comes from the
        # pipeline's per-step run report.
        completed_combos = sum(e.get("completed", 0) for e in report)
        failed_combos = sum(e.get("failed", 0) for e in report)
        no_data_combos = sum(e.get("no_data", 0) for e in report)
        for e in report:
            if e.get("failed"):
                emit(
                    f"⚠ {e.get('label', e.get('step'))}: "
                    f"{e['failed']} combo(s) failed "
                    f"({e.get('completed', 0)} completed)\n"
                )
        if failed_combos > 0:
            success = False
            emit(
                f"✗ Pipeline run finished with {failed_combos} failed "
                f"combo(s) across {sum(1 for e in report if e.get('failed'))}"
                f" step(s) — see log above\n"
            )
        duration_ms = int((time.time() - run_started_at) * 1000)
        with _active_runs_lock:
            entry = _active_runs.pop(run_id, None)
        was_force = bool(entry and entry.get("force_cancelled"))
        if cancel_event.is_set():
            cancelled = True
        logger.info(
            "[pipeline_run] finished (run_id=%s success=%s "
            "cancelled=%s completed_combos=%d failed_combos=%d "
            "no_data_combos=%d) in %d ms",
            run_id,
            success,
            cancelled,
            completed_combos,
            failed_combos,
            no_data_combos,
            duration_ms,
        )
        push_message(
            {
                "type": "run_done",
                "run_id": run_id,
                "success": success,
                "duration_ms": duration_ms,
                "cancelled": cancelled,
                "force_cancelled": was_force,
                "completed_combos": completed_combos,
                "failed_combos": failed_combos,
                "no_data_combos": no_data_combos,
            }
        )
        _notify_records_changed()


def _run_matlab_pipeline_in_thread(
    run_id: str,
    pipeline_id: str,
    mode: str,
    target: str,
    finalized,
    skip_computed: bool,
    db: DatabaseManager,
) -> None:
    """Background execution of a MATLAB-containing GUI pipeline scope
    through the standalone sidecar (Stage 3) — the browser/standalone
    counterpart to ``_run_pipeline_in_thread``, for callers with no
    host-side terminal integration (no VS Code + MathWorks extension).
    Generates the whole-pipeline script (Stage 1's
    ``generate_matlab_pipeline_command``) and drives it through
    ``MatlabSidecar``, relaying real stdout lines as ``run_output`` and a
    real ``run_done`` once the sentinel is seen — unlike the VS Code
    terminal path (Stage 2), this path KNOWS exactly when the run
    finished, no DB file-watcher guessing needed.
    """
    from scistack_gui import matlab_sidecar
    from scistack_gui.services.matlab_command_service import (
        generate_matlab_pipeline_command,
    )

    def emit(text: str):
        push_message({"type": "run_output", "run_id": run_id, "text": text})

    cancel_event = threading.Event()
    with _active_runs_lock:
        _active_runs[run_id] = {
            "event": cancel_event,
            "thread": threading.current_thread(),
            "cancelled": False,
            "force_cancelled": False,
            # force_cancel_run checks this to kill the MATLAB process
            # itself instead of (uselessly) ctypes-injecting into a thread
            # that's just waiting on a queue — see force_cancel_run.
            "matlab_sidecar": True,
        }
    db.set_current_db()
    run_started_at = time.time()
    logger.info(
        "[pipeline_run][matlab] run_id=%s scope=%s mode=%s target=%r",
        run_id,
        pipeline_id,
        mode,
        target,
    )
    emit(
        f"▶ Running pipeline scope {pipeline_id} via MATLAB sidecar (mode={mode}"
        + (f", target={target}" if target else "")
        + ")\n"
    )

    success = False
    cancelled = False
    try:
        result = generate_matlab_pipeline_command(
            pipeline_id,
            db,
            {
                "mode": mode,
                "target": target,
                "finalized": finalized,
                "skip_computed": skip_computed,
            },
        )
        # Two channels: "warnings" is composition (steps excluded from the
        # script), "diagnostics" is why a variable type may not resolve in
        # MATLAB. Both belong in front of the user before the wait starts.
        for w in (result.get("warnings") or []) + (result.get("diagnostics") or []):
            emit(f"⚠ {w}\n")
        command = result["command"]

        # MATLAB opens the same .duckdb file itself — hand it over for the
        # duration, or its scihist.configure_database call is locked out by
        # this process (see external_db_access).
        with external_db_access("MATLAB"):
            success = _drive_sidecar(matlab_sidecar, command, emit)
    except KeyboardInterrupt:
        # force_cancel_run killed the sidecar process, which unblocks
        # run_command's queue wait with a RuntimeError (not
        # KeyboardInterrupt) — this branch exists defensively in case a
        # future caller injects one directly, matching the Python-thread
        # run path's shape.
        cancelled = True
        emit("⛔ Force-cancelled\n")
    except Exception as exc:
        logger.exception("[pipeline_run][matlab] failed (run_id=%s)", run_id)
        emit(f"Error: {exc}\n")
    finally:
        duration_ms = int((time.time() - run_started_at) * 1000)
        with _active_runs_lock:
            entry = _active_runs.pop(run_id, None)
        was_force = bool(entry and entry.get("force_cancelled"))
        if cancel_event.is_set():
            cancelled = True
        if was_force:
            # A force-cancel that killed the sidecar surfaces here as a
            # plain RuntimeError from run_command (process exited before
            # completion) — reclassify it as a cancellation, not a failure.
            cancelled = True
        logger.info(
            "[pipeline_run][matlab] finished (run_id=%s success=%s cancelled=%s) "
            "in %d ms",
            run_id,
            success,
            cancelled,
            duration_ms,
        )
        push_message(
            {
                "type": "run_done",
                "run_id": run_id,
                "success": success and not cancelled,
                "duration_ms": duration_ms,
                "cancelled": cancelled,
                "force_cancelled": was_force,
            }
        )
        _notify_records_changed()


def _run_matlab_command_in_thread(
    run_id: str,
    command: str,
    warnings: list[str],
) -> None:
    """Background execution of an ALREADY-GENERATED MATLAB command (single
    function or whole pipeline — this is transport-agnostic, just text)
    through the standalone sidecar. The Stage 4 fallback-ladder tier: used
    when a caller has some OTHER host-side dispatch option too (VS Code's
    dagPanel.ts trying the MathWorks terminal first) but that option isn't
    available right now, before finally falling back to clipboard-copy.
    Pushes real run_output/run_done — the caller doesn't synthesize
    anything for this tier (unlike the terminal-dispatch tier, which must,
    since nothing else drives that path)."""
    _drive_matlab_in_thread(run_id, lambda: (command, list(warnings)))


def _run_matlab_function_in_thread(
    run_id: str,
    function_name: str,
    params: dict,
    db: DatabaseManager,
) -> None:
    """Background execution of a SINGLE MATLAB function node through the
    standalone sidecar — the browser/standalone counterpart to the VS Code
    host's generate-then-dispatch path in ``dagPanel.ts::handleMatlabRun``,
    and the single-node sibling of ``_run_matlab_pipeline_in_thread``.

    Command generation happens *inside* the thread on purpose: it reads the
    DB and the canvas wiring and can fail, and a failure has to reach the
    user as ``run_output``/``run_done`` on this ``run_id``. Raising it out
    of the ``start_run`` RPC instead would leave the node's Run button stuck
    on "⏳ Running…" with no console output.
    """

    def make_command() -> "tuple[str, list[str]]":
        from scistack_gui.services.matlab_command_service import (
            generate_matlab_command,
        )

        result = generate_matlab_command(function_name, db, params)
        return result["command"], list(
            (result.get("warnings") or []) + (result.get("diagnostics") or [])
        )

    db.set_current_db()
    logger.info(
        "[run_thread][matlab] single-node run via sidecar (run_id=%s fn=%s)",
        run_id,
        function_name,
    )
    _drive_matlab_in_thread(run_id, make_command)


def _drive_matlab_in_thread(
    run_id: str,
    make_command: "Callable[[], tuple[str, list[str]]]",
) -> None:
    """Shared body for every sidecar-driven MATLAB run: register the run so
    it can be cancelled, obtain the script, drive it through the kept-warm
    engine relaying stdout as ``run_output``, and always finish with exactly
    one ``run_done`` + ``dag_updated``.

    ``make_command`` is a callable rather than a string so callers that must
    *generate* the script (per-function, per-pipeline) get their generation
    failures reported through the same run console as an execution failure,
    instead of as an RPC error the frontend can't attribute to the run.
    """
    from scistack_gui import matlab_sidecar

    def emit(text: str):
        push_message({"type": "run_output", "run_id": run_id, "text": text})

    cancel_event = threading.Event()
    with _active_runs_lock:
        _active_runs[run_id] = {
            "event": cancel_event,
            "thread": threading.current_thread(),
            "cancelled": False,
            "force_cancelled": False,
            "matlab_sidecar": True,
        }
    run_started_at = time.time()

    success = False
    try:
        # Generate BEFORE handing the database over: generation reads the DB
        # (variants, wiring) and needs our own connection.
        command, warnings = make_command()
        for w in warnings:
            emit(f"⚠ {w}\n")
        # MATLAB opens the same .duckdb file itself. In browser/standalone
        # mode nothing otherwise releases our connection (only the JSON-RPC
        # server drops it between requests), so without this hand-off MATLAB
        # would fail on its first scihist.configure_database call — locked
        # out by us.
        with external_db_access("MATLAB"):
            success = _drive_sidecar(matlab_sidecar, command, emit)
    except Exception as exc:
        logger.exception("[run_thread][matlab] failed (run_id=%s)", run_id)
        emit(f"Error: {exc}\n")
    finally:
        duration_ms = int((time.time() - run_started_at) * 1000)
        with _active_runs_lock:
            entry = _active_runs.pop(run_id, None)
        was_force = bool(entry and entry.get("force_cancelled"))
        cancelled = was_force or cancel_event.is_set()
        logger.info(
            "[run_thread][matlab] finished (run_id=%s success=%s cancelled=%s) "
            "in %d ms",
            run_id,
            success,
            cancelled,
            duration_ms,
        )
        push_message(
            {
                "type": "run_done",
                "run_id": run_id,
                "success": success and not cancelled,
                "duration_ms": duration_ms,
                "cancelled": cancelled,
                "force_cancelled": was_force,
            }
        )
        _notify_records_changed()


def _drive_sidecar(matlab_sidecar, command: str, emit) -> bool:
    """Start (or reuse) the kept-warm engine and run ``command`` on it,
    relaying stdout through ``emit``. Returns whether MATLAB succeeded.

    Called with the DuckDB file already handed over to MATLAB — see
    ``_drive_matlab_in_thread``.
    """
    sidecar = matlab_sidecar.get_sidecar()
    if not sidecar.start():
        emit(
            "Error: MATLAB is required here, but 'matlab' is not on PATH. "
            "Add it, or run this from VS Code with the MathWorks MATLAB "
            "extension installed.\n"
        )
        return False
    health = sidecar.check_health()
    if health:
        # A setup problem, not a pipeline failure — say so before the first
        # py.* call turns it into a confusing scidb traceback.
        emit(f"Error: {health}\n")
        raise RuntimeError(health)
    success = sidecar.run_command(command, emit)
    if not success:
        emit("✗ MATLAB reported an error — see log above\n")
    return success


def start_matlab_sidecar_run(
    command: str, run_id: str, warnings: "list[str] | None" = None
) -> dict:
    """Drive an already-generated MATLAB command (single function or
    whole pipeline) through the standalone sidecar. Called from the VS
    Code extension's dagPanel.ts as the fallback-ladder's Tier 3, when
    Tier 2 (the MathWorks terminal) isn't available — see
    plan-matlab-pipeline-execution.md Stage 4.

    Cheaply reports unavailability up front (``sidecar_available=False``,
    no thread spawned) when ``matlab`` isn't on PATH, so the caller can
    fall through to its clipboard-copy fallback instead of waiting.
    """
    from scistack_gui import matlab_sidecar

    if not matlab_sidecar.sidecar_capable():
        logger.info(
            "[api/run] MATLAB sidecar unavailable ('matlab' not on PATH) "
            "for run_id=%s",
            run_id,
        )
        return {"run_id": run_id, "sidecar_available": False}

    thread = threading.Thread(
        target=_run_matlab_command_in_thread,
        args=(run_id, command, warnings or []),
        daemon=True,
    )
    thread.start()
    logger.info("[api/run] MATLAB sidecar run thread started (run_id=%s)", run_id)
    return {"run_id": run_id, "sidecar_available": True}


def start_pipeline_run(
    pipeline_id: str,
    mode: str = "all",
    target: str = "",
    finalized=None,
    skip_computed: bool = True,
    run_id: "str | None" = None,
    host_can_dispatch_matlab: bool = False,
) -> dict:
    """Spawn a background pipeline run (called from api/scopes and the
    JSON-RPC handler). Validates the mode/target shape up front so bad
    requests fail synchronously.

    MATLAB routing: if ``pipeline_id``'s scope (or any pipeline it uses)
    contains a MATLAB function node, Python's own ``Pipeline._run`` can't
    execute it (see ``execution_service.pipeline_has_matlab_steps`` — it
    can only silently skip such steps, not run them).

    ``host_can_dispatch_matlab=True`` (set only by ``server.py``'s
    JSON-RPC handler — the VS Code extension has a privileged host,
    ``dagPanel.ts``, sitting between the webview and this process) skips
    spawning any thread here and instead returns
    ``host_execution_required=True`` (with the SAME ``run_id`` the caller
    already has), so ``dagPanel.ts`` generates the script itself and
    dispatches it to the MathWorks terminal (Stage 2) or clipboard.

    Plain HTTP callers (``api/scopes.py`` — browser/standalone mode, no
    such privileged host exists) get ``host_can_dispatch_matlab=False``
    (the default): MATLAB steps are driven directly here through the
    standalone sidecar (Stage 3, ``_run_matlab_pipeline_in_thread``). Both
    paths emit ``run_output``/``run_done`` tagged with the same
    ``run_id``, so the frontend's run console doesn't need to know which
    path served it.
    """
    from scistack_gui.db import get_db
    from scistack_gui.services.execution_service import pipeline_has_matlab_steps

    if mode not in ("all", "until", "endpoints", "show"):
        raise ValueError(f"unknown run mode {mode!r}")
    if mode in ("until", "show") and not target:
        raise ValueError(f"mode={mode!r} requires a target step name")

    from scistack_gui.db import get_db

    rid = run_id or str(uuid.uuid4())[:8]
    db = get_db()
    if pipeline_has_matlab_steps(db, pipeline_id):
        if mode == "show":
            raise ValueError(
                "mode='show' is not supported for a MATLAB-containing "
                "pipeline (MATLAB Pipeline.m has no show() equivalent)"
            )
        if host_can_dispatch_matlab:
            logger.info(
                "[api/run] pipeline %s contains MATLAB step(s) — routing to "
                "host-side execution (run_id=%s)",
                pipeline_id,
                rid,
            )
            return {
                "run_id": rid,
                "host_execution_required": True,
                "language": "matlab",
            }
        logger.info(
            "[api/run] pipeline %s contains MATLAB step(s) — routing to "
            "standalone MATLAB sidecar (run_id=%s)",
            pipeline_id,
            rid,
        )
        thread = threading.Thread(
            target=_run_matlab_pipeline_in_thread,
            args=(rid, pipeline_id, mode, target, finalized, skip_computed, db),
            daemon=True,
        )
        thread.start()
        return {"run_id": rid}

    thread = threading.Thread(
        target=_run_pipeline_in_thread,
        args=(rid, pipeline_id, mode, target, finalized, skip_computed, db),
        daemon=True,
    )
    thread.start()
    logger.info("[api/run] pipeline run thread started (run_id=%s)", rid)
    return {"run_id": rid}


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
            logger.warning(
                "[cancel_run] Unknown run_id=%s (not in active runs)", run_id
            )
            return {"ok": False, "error": f"unknown run_id: {run_id}"}
        logger.debug(
            "[cancel_run] Setting cancelled flag and event for run_id=%s", run_id
        )
        entry["cancelled"] = True
        entry["event"].set()
    logger.info("[cancel_run] Cooperative cancel requested for run_id=%s", run_id)
    return {"ok": True, "cancelled": True, "force": False}


def force_cancel_run(run_id: str) -> dict:
    """Force-cancel a running for_each by injecting KeyboardInterrupt (or,
    for a MATLAB-sidecar pipeline run, killing the MATLAB process itself).

    Sets the cooperative cancel event AND calls
    ``ctypes.pythonapi.PyThreadState_SetAsyncExc`` to raise
    ``KeyboardInterrupt`` in the worker thread. Best-effort:

    - Won't interrupt code blocked in C extensions, native syscalls,
      or threading primitives that don't poll for interrupts.
    - When that fails, the user must restart the Python subprocess via
      the existing ``scistack.restartPython`` command.

    A MATLAB-sidecar run (``_run_matlab_pipeline_in_thread``, tagged
    ``matlab_sidecar: True`` in the registry) is a different shape: the
    worker thread is just blocked waiting on a queue for output from a
    SEPARATE MATLAB process — injecting an exception into that thread would
    only stop Python from waiting, not stop MATLAB from computing. Real
    cancellation there means killing the MATLAB process directly
    (``matlab_sidecar.MatlabSidecar.stop()``), which closes its stdout and
    unblocks the worker thread's queue wait with a clean "process exited"
    error — no ctypes injection needed or attempted.

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
            logger.warning(
                "[force_cancel_run] Unknown run_id=%s (not in active runs)", run_id
            )
            return {"ok": False, "error": f"unknown run_id: {run_id}"}
        logger.debug(
            "[force_cancel_run] Setting cancelled and force_cancelled flags for run_id=%s",
            run_id,
        )
        entry["cancelled"] = True
        entry["force_cancelled"] = True
        entry["event"].set()
        thread = entry["thread"]
        is_matlab_sidecar = bool(entry.get("matlab_sidecar"))

    if is_matlab_sidecar:
        from scistack_gui import matlab_sidecar

        logger.info(
            "[force_cancel_run] MATLAB sidecar run — killing the MATLAB "
            "process directly (run_id=%s)",
            run_id,
        )
        matlab_sidecar.get_sidecar().stop()
        return {
            "ok": True,
            "cancelled": True,
            "force": True,
            "best_effort": True,
            "injected": False,
        }

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

    logger.info(
        "[force_cancel_run] Injecting KeyboardInterrupt into thread tid=%s (run_id=%s)",
        tid,
        run_id,
    )
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
            tid,
            run_id,
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
            n,
            run_id,
        )
        ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), ctypes.c_long(0))
        return {
            "ok": False,
            "error": f"PyThreadState_SetAsyncExc affected {n} threads (rolled back)",
        }

    logger.info(
        "[force_cancel_run] Successfully injected KeyboardInterrupt into tid=%s (run_id=%s)",
        tid,
        run_id,
    )
    return {
        "ok": True,
        "cancelled": True,
        "force": True,
        "best_effort": True,
        "injected": True,
    }


# ---------------------------------------------------------------------------
# The handler table — both transports (api/handlers.py)
# ---------------------------------------------------------------------------
#
#     POST /api/run                          start_run
#     POST /api/run/{run_id}/cancel          cancel_run
#     POST /api/run/{run_id}/force-cancel    force_cancel_run
#     GET  /api/matlab-engine                get_matlab_engine_status
#     POST /api/matlab-engine/restart        restart_matlab_engine
#     (RPC only)                             generate_matlab_command,
#                                            generate_matlab_pipeline_command,
#                                            start_matlab_sidecar_run
#
# The three RPC-only methods exist for the VS Code extension's dagPanel.ts,
# which owns the MathWorks-terminal dispatch and the sidecar fallback
# ladder; the browser has no host that could act on them.


class RunRef(BaseModel):
    run_id: str


class MatlabCommandRequest(BaseModel):
    """The command generators read the raw request dict (``params``) as the
    single-run/pipeline-run requests do — every field the picker sends is
    forwarded. Declared open so the model does not have to repeat them."""

    model_config = {"extra": "allow"}

    function_name: str | None = None
    pipeline_id: str | None = None


class SidecarRunRequest(BaseModel):
    command: str
    run_id: str
    warnings: list[str] | None = None


def _start_run(db, req: RunRequest, *, transport: str) -> dict:
    return start_run(db, req, transport=transport)


def _cancel_run(req: RunRef) -> dict:
    logger.info("[api/run] cancel_run for run_id=%s", req.run_id)
    result = cancel_run(req.run_id)
    logger.debug("[api/run] cancel_run result: %s (run_id=%s)", result, req.run_id)
    return result


def _force_cancel_run(req: RunRef) -> dict:
    logger.info("[api/run] force_cancel_run for run_id=%s", req.run_id)
    result = force_cancel_run(req.run_id)
    logger.debug("[api/run] force_cancel_run result: %s (run_id=%s)", result, req.run_id)
    return result


def _get_matlab_engine_status() -> dict:
    return get_matlab_engine_status()


def _restart_matlab_engine() -> dict:
    return restart_matlab_engine()


def _generate_matlab_command(db, req: MatlabCommandRequest) -> dict:
    from scistack_gui.services.matlab_command_service import generate_matlab_command

    if not req.function_name:
        raise ValueError("generate_matlab_command needs a function_name")
    return generate_matlab_command(req.function_name, db, req.model_dump())


def _generate_matlab_pipeline_command(db, req: MatlabCommandRequest) -> dict:
    from scistack_gui.services.matlab_command_service import (
        generate_matlab_pipeline_command,
    )

    if not req.pipeline_id:
        raise ValueError("generate_matlab_pipeline_command needs a pipeline_id")
    return generate_matlab_pipeline_command(req.pipeline_id, db, req.model_dump())


def _start_matlab_sidecar_run(req: SidecarRunRequest) -> dict:
    return start_matlab_sidecar_run(req.command, req.run_id, req.warnings)


_NO_DB = {"needs_db": False}

RUN_HANDLERS: tuple[Handler, ...] = (
    Handler("start_run", "/run", RunRequest, _start_run, http_errors={ValueError: 400}, wants_transport=True),
    Handler("cancel_run", "/run/{run_id}/cancel", RunRef, _cancel_run, body=False, **_NO_DB),
    Handler("force_cancel_run", "/run/{run_id}/force-cancel", RunRef, _force_cancel_run, body=False, **_NO_DB),
    Handler("get_matlab_engine_status", "/matlab-engine", None, _get_matlab_engine_status, http_method="GET", **_NO_DB),
    Handler("restart_matlab_engine", "/matlab-engine/restart", None, _restart_matlab_engine, **_NO_DB),
    Handler("generate_matlab_command", None, MatlabCommandRequest, _generate_matlab_command),
    Handler("generate_matlab_pipeline_command", None, MatlabCommandRequest, _generate_matlab_pipeline_command),
    Handler("start_matlab_sidecar_run", None, SidecarRunRequest, _start_matlab_sidecar_run, **_NO_DB),
)

install_routes(router, RUN_HANDLERS)
