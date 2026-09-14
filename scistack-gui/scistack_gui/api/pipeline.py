"""
GET /pipeline

Returns the pipeline graph as React Flow nodes and edges.

Node types:
  - "variableNode": a named variable type (RawEMG, FilteredEMG, ...)
  - "functionNode": a pipeline function (bandpass_filter, ...)

Positions are set to (0, 0) here; the layout endpoint overwrites them with
saved positions, and the frontend assigns dagre positions for new nodes.
"""

import inspect
import logging
import time

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from scidb.database import DatabaseManager

from scistack_gui import layout as layout_store
from scistack_gui import registry
from scistack_gui.api import ws
from scistack_gui.db import get_db

logger = logging.getLogger(__name__)
router = APIRouter()


def _parse_path_input(value: str) -> dict | None:
    """If *value* (from __inputs) represents a PathInput, return parsed info.

    Delegates to domain.graph_builder.parse_path_input.
    """
    from scistack_gui.domain.graph_builder import parse_path_input

    return parse_path_input(value)


def _fn_params_from_registry(fn_name: str) -> list[str]:
    """Return non-private parameter names from the registered function's signature.

    ``lookup_function`` covers both discovered user code and library
    references (``pandas.read_csv``), which are imported on demand rather
    than stored in the registry. Falls back to the MATLAB registry if the
    function isn't a Python function.
    """
    fn = registry.lookup_function(fn_name)
    if fn is not None:
        try:
            return [
                name
                for name in inspect.signature(fn).parameters
                if not name.startswith("_")
            ]
        except (ValueError, TypeError):
            return []
    # Check MATLAB registry.
    from scistack_gui import matlab_registry

    if matlab_registry.is_matlab_function(fn_name):
        return list(matlab_registry.get_matlab_function(fn_name).params)
    return []


def _node_id_to_var_label(
    node_id: str,
    existing_ids: set[str],
    nodes: list[dict],
    manual_nodes: dict[str, dict],
) -> str | None:
    """Resolve a node ID to its variable label, or None if not a variable node.

    Thin wrapper around domain.edge_resolver.node_id_to_var_label that
    builds the existing_node_labels dict from the nodes list.
    """
    from scistack_gui.domain.edge_resolver import node_id_to_var_label

    existing_node_labels = {n["id"]: n["data"]["label"] for n in nodes}
    return node_id_to_var_label(node_id, existing_node_labels, manual_nodes)


def _get_record_counts(db: DatabaseManager, var_types: set[str]) -> dict[str, int]:
    """
    Query the row count of each variable type's table directly.
    Used for nodes that have data but no for_each variants (e.g. raw .save() calls).
    Returns 0 for types whose table doesn't exist yet.
    """
    counts: dict[str, int] = {}
    for vtype in var_types:
        try:
            row = db._duck._fetchall(f'SELECT COUNT(*) FROM "{vtype}"')
            counts[vtype] = int(row[0][0]) if row else 0
        except Exception:
            counts[vtype] = 0
    return counts


def _build_matlab_fn_proxy(fn_name: str):
    """Build a MatlabLineageFcn proxy for use in check_node_state.

    Uses the source hash from the MATLAB registry so the proxy's ``.hash``
    matches what was stored at save time.
    """
    from scimatlab.bridge import MatlabLineageFcn

    from scistack_gui import matlab_registry

    info = matlab_registry.get_matlab_function(fn_name)
    # unpack_output MUST match scimatlab/.../+scihist/for_each.m's default.
    # Native MATLAB multi-output (`[a,b,c] = fn(...)`) uses unpack_output=False
    # and is unpacked at runtime via LineageFcn's n_out>1 branch. unpack_output=True
    # is only for the rarer single-cell-array-return pattern.
    proxy = MatlabLineageFcn(info.source_hash, fn_name, unpack_output=False)
    logger.debug(
        "[pipeline] matlab proxy fn=%s source_hash=%s unpack=False hash=%s",
        fn_name,
        info.source_hash[:12],
        proxy.hash[:12],
    )
    proxy.__name__ = fn_name
    return proxy


def _own_state_for_function(
    db: DatabaseManager,
    fn_name: str,
    fn_out_types: set[str],
    call_id: str | None = None,
) -> str:
    """
    Return the own run state ("green"/"grey"/"red") for a single function
    by calling scidb.check_node_state.

    When ``call_id`` is provided, restricts the state computation to records
    produced by that specific for_each call site. Used for manual (not yet
    graduated) function nodes, which fall outside the batched
    ``_compute_run_states`` pass since they have no recorded call site yet.

    Falls back to "red" for unregistered functions (never executed or not
    importable in this session).
    """
    from scidb import BaseVariable, check_node_state

    # lookup_function also covers library references, which resolve by
    # import — without it a pandas.read_csv node reads as permanently red.
    fn_obj = registry.lookup_function(fn_name)
    if fn_obj is None:
        # Try MATLAB registry — build a proxy with the right hash.
        from scistack_gui import matlab_registry

        if matlab_registry.is_matlab_function(fn_name):
            fn_obj = _build_matlab_fn_proxy(fn_name)
        else:
            # Function not registered in this session — can't run state check.
            return "red"

    output_classes = [
        BaseVariable._all_subclasses[t]
        for t in fn_out_types
        if t in BaseVariable._all_subclasses
    ]
    if not output_classes:
        return "red"

    try:
        result = check_node_state(fn_obj, output_classes, db=db, call_id=call_id)
        state = result["state"]
        counts = result.get("counts", {})
        logger.debug(
            "state(%s call_id=%s): %s (up_to_date=%d, stale=%d, missing=%d)",
            fn_name,
            call_id,
            state,
            counts.get("up_to_date", 0),
            counts.get("stale", 0),
            counts.get("missing", 0),
        )
        return state
    except Exception:
        logger.exception(
            "check_node_state failed for %s call_id=%s — falling back to red",
            fn_name,
            call_id,
        )
        return "red"


def _wiring_conflicts_with_candidate(
    inferred_inputs: dict[str, str],
    output_types: list[str],
    candidate_input_params: dict,
    candidate_output_types,
) -> bool:
    """Whether a manual function node's OWN resolved wiring actively
    contradicts a candidate DB call site's real wiring.

    Absence of wiring info is NOT a conflict — a freshly-placed, still
    unwired function node has no basis to distinguish "this is the same
    call site" from "this is a different one", so it graduates into a
    single matching candidate immediately (existing UX — see
    test_graduation_preserves_sub_scope_membership, which places a bare
    functionNode with no edges at all and expects it to graduate). A
    conflict is only genuine once the manual node is ACTIVELY wired to a
    variable type that differs from the candidate's for that same param
    or output (e.g. compute_rolling_vo2 fed by RawHeartRate instead of
    the candidate's RawVO2).
    """
    for param, var_type in inferred_inputs.items():
        real_type = candidate_input_params.get(param)
        if real_type and real_type != var_type:
            return True
    if output_types and candidate_output_types:
        if not set(output_types) & set(candidate_output_types):
            return True
    return False


def _find_db_fn_candidate(
    agg, fn_label: str, wiring: str
) -> tuple[dict, set] | None:
    """Real (input_params, output_types) for a DB-derived (fn_label,
    wiring) call site group, or None if it doesn't exist.

    ``agg`` here must be the WIRING-GROUPED aggregate (post
    ``group_call_sites_by_wiring`` — see ``_build_graph``), whose keys are
    ``(fn_name, wiring_id)``, matching what a DB-derived function node id
    (``fn__{fn_name}__{wiring_id}``) encodes.
    """
    key = (fn_label, wiring)
    if key not in agg.fn_input_params and key not in agg.fn_outputs:
        return None
    return agg.fn_input_params.get(key, {}), agg.fn_outputs.get(key, set())


def _compute_run_states(
    db: DatabaseManager,
    fn_input_params: dict[tuple, dict],
    fn_outputs: dict[tuple, set],
    disconnected_fkeys: set[tuple] | None = None,
) -> dict[str, str]:
    """
    Compute run_state for every function and variable node.

    Function-keyed inputs use FnKey = (fn_name, call_id) so the same fn
    reused across multiple for_each call sites gets a distinct state per
    call site.

    Pass 1 — own state per function-call-site:
      Calls scidb.check_multiple_nodes_state() for all nodes in batch.

    Pass 2 — propagate staleness through the DAG (delegated to domain layer).
      ``disconnected_fkeys`` (call sites with a user-hidden required inbound
      edge — see domain.graph_builder.hidden_wirings/wiring_disconnected_fkeys)
      forces those call sites red regardless of DB freshness, cascading
      downstream through the same propagation.

    Returns {node_id: "green"|"red"} for fn__ and var__ nodes — real,
    recorded call sites only. "pending" (an unrun staged constant value)
    is a display-only concept layered on top by
    domain.graph_builder.group_call_sites_by_wiring, which synthesizes a
    separate row for the not-yet-existing combo rather than downgrading
    any real call site's own state (see pending_value_group_coverage).
    """
    from scidb import BaseVariable, check_multiple_nodes_state
    from scistack_gui.domain.run_state import propagate_run_states

    t0 = time.monotonic()

    # --- Pass 1: Build function registry and nodes list ---
    # Build registry combining Python and MATLAB functions
    fn_registry = dict(registry._functions)  # Copy Python functions

    from scistack_gui import matlab_registry

    # Fill in the two kinds of function that aren't in registry._functions:
    # library references (pandas.read_csv — resolved by import) and MATLAB
    # functions (a proxy carrying the right hash). Only for names actually
    # on this canvas, so neither lookup is paid for the whole registry.
    for fn_name in fn_input_params.keys():
        fn_name_str, _ = fn_name
        if fn_name_str in fn_registry:
            continue
        library_fn = registry.lookup_function(fn_name_str)
        if library_fn is not None:
            fn_registry[fn_name_str] = library_fn
        elif matlab_registry.is_matlab_function(fn_name_str):
            fn_registry[fn_name_str] = _build_matlab_fn_proxy(fn_name_str)

    # Build nodes list for batched state checking
    nodes = []
    for fkey in fn_input_params:
        fn_name, cid = fkey
        fn_out_types = fn_outputs.get(fkey, set())

        # Convert output type names to classes
        output_classes = [
            BaseVariable._all_subclasses[t]
            for t in fn_out_types
            if t in BaseVariable._all_subclasses
        ]

        if output_classes:  # Only add if we have valid output classes
            nodes.append(
                {
                    "fn_name": fn_name,
                    "call_id": cid,
                    "outputs": output_classes,
                }
            )

    # Batch call to check states for all nodes
    state_results = check_multiple_nodes_state(nodes, fn_registry=fn_registry, db=db)

    # Convert results to fn_own_state format (FnKey → state)
    fn_own_state: dict[tuple, str] = {}
    for fkey in fn_input_params:
        fn_name, cid = fkey
        node_id = f"fn__{fn_name}__{cid or ''}"
        if node_id in state_results:
            fn_own_state[fkey] = state_results[node_id]["state"]
            counts = state_results[node_id].get("counts", {})
            logger.debug(
                "state(%s call_id=%s): %s (up_to_date=%d, stale=%d, missing=%d)",
                fn_name,
                cid,
                fn_own_state[fkey],
                counts.get("up_to_date", 0),
                counts.get("stale", 0),
                counts.get("missing", 0),
            )
        else:
            # Function not in results (no outputs or error) → mark as red
            fn_own_state[fkey] = "red"

    # --- Pass 2: DAG propagation (pure) ---
    result = propagate_run_states(
        fn_own_state,
        fn_input_params,
        fn_outputs,
        disconnected_fkeys,
    )

    elapsed_ms = (time.monotonic() - t0) * 1000
    counts = {"green": 0, "pending": 0, "red": 0}
    for nid, s in result.items():
        if nid.startswith("fn__"):
            counts[s] = counts.get(s, 0) + 1
    logger.debug(
        "run_states complete: %d call sites in %.1fms (%d green, %d pending, %d red)",
        len([k for k in result if k.startswith("fn__")]),
        elapsed_ms,
        counts["green"],
        counts["pending"],
        counts["red"],
    )
    return result


def _matlab_param_to_class_from_db(
    aggregated_functions: dict,
    matlab_functions: set,
    matlab_output_order: dict,
) -> dict[str, dict[str, str]]:
    """``{fn_name: {output_param_name: class_name}}`` derived from DB variants.

    Maps each variant's ``output_num`` (its slot in the fn signature) onto the
    fn's declared output name. This is the DB-derived half of
    ``matlab_param_to_class``; ``infer_manual_fn_param_to_class`` supplies the
    manual-edge half. When BOTH come up empty the fn node renders handle
    ``out__{param}`` while build_edges points its edge at ``out__{Class}``, and
    React Flow silently drops an edge whose sourceHandle does not exist — the
    fn appears disconnected from an output variable that run_state marks green.
    """
    from_db: dict[str, dict[str, str]] = {}
    for (fn_name, _call_id), fn_data in aggregated_functions.items():
        if fn_name not in matlab_functions:
            continue
        for variant in fn_data.get("variants", []):
            onum = variant.get("output_num")
            out_type = variant.get("output_type")
            if onum is None or out_type is None:
                # A variant with no output_num contributes nothing here, so the
                # manual-edge fallback becomes load-bearing. Say so — this is
                # what surfaced get_aggregated_variants() dropping output_num.
                logger.info(
                    "[pipeline] matlab_param_to_class: fn=%s output_type=%r has "
                    "output_num=%r — DB source contributes nothing for this variant",
                    fn_name,
                    out_type,
                    onum,
                )
                continue
            names = matlab_output_order.get(fn_name) or []
            if 0 <= int(onum) < len(names):
                from_db.setdefault(fn_name, {})[names[int(onum)]] = out_type
            else:
                logger.info(
                    "[pipeline] matlab_param_to_class: fn=%s output_num=%s is out of "
                    "range for its %d declared output name(s) %s — DB source skipped",
                    fn_name,
                    onum,
                    len(names),
                    names,
                )
    return from_db


def _build_graph(db: DatabaseManager, pipeline_id: str = "main") -> dict:
    """
    Build nodes and edges from list_pipeline_variants() and list_variables(),
    restricted to one pipeline SCOPE.

    Delegates pure logic to domain.graph_builder and domain.edge_resolver;
    this function orchestrates data fetching and side effects. The full
    graph is built once, then scope-filtered (domain.scope_filter): manual
    nodes belong by pipeline_id, DB-derived nodes by where their position
    is saved (unsaved -> root), edges by both-endpoints-kept. pipelineNode
    entries for the scope's use edges are appended last (scope_service).
    """
    logger.info("[pipeline] Starting graph build orchestration (scope=%s)", pipeline_id)

    from scistack_gui import matlab_registry as _mr
    from scistack_gui import pipeline_store as _ps
    from scistack_gui.domain import graph_builder as gb
    from scistack_gui.domain.edge_resolver import resolve_function_edges

    hidden_ids = _ps.get_hidden_node_ids(db, pipeline_id)
    logger.debug(
        "[pipeline] loaded %d hidden node ID(s) for scope=%s", len(hidden_ids), pipeline_id
    )
    hidden_edge_ids = _ps.get_hidden_edge_ids(db, pipeline_id)
    logger.debug(
        "[pipeline] loaded %d hidden edge ID(s) for scope=%s",
        len(hidden_edge_ids),
        pipeline_id,
    )

    # --- Fetch aggregated data from scidb (replaces steps 2-5) ---
    logger.info("[pipeline] Fetching aggregated variants from scidb")
    scidb_agg = db.get_aggregated_variants()
    logger.info(
        "[pipeline] fetched data for %d functions, %d variables, %d constants, %d path inputs",
        len(scidb_agg["functions"]),
        len(scidb_agg["variables"]),
        len(scidb_agg["constants"]),
        len(scidb_agg["path_inputs"]),
    )

    # Convert scidb format to AggregatedData format for compatibility
    logger.info("[pipeline] Converting to AggregatedData format")
    agg = gb.AggregatedData()

    # Convert functions dict
    for (fn_name, call_id), fn_data in scidb_agg["functions"].items():
        fkey = (fn_name, call_id)
        agg.fn_input_params[fkey] = fn_data["input_params"]
        agg.fn_outputs[fkey] = set(fn_data["outputs"])
        # Convert constants from list to dict for const_counts
        for const_name, values in fn_data["constants"].items():
            agg.fn_constants[fkey].add(const_name)
            for val in values:
                # Note: we don't have per-value record counts from scidb_agg,
                # but const_counts is used for display, so we can approximate
                agg.const_counts[const_name][str(val)] = 1
        agg.fn_variants_map[fkey] = fn_data["variants"]

    # Convert constants
    for const_name, const_data in scidb_agg["constants"].items():
        for val_entry in const_data["values"]:
            agg.const_counts[const_name][val_entry["value"]] = val_entry["record_count"]
        for fkey in const_data["functions"]:
            agg.const_fns[const_name].add(tuple(fkey))

    # Convert variables
    agg.all_var_types = set(scidb_agg["variables"].keys())

    # Convert path_inputs — scidb_agg is keyed by PARAM NAME (it's a raw
    # DB-history extraction with no knowledge of source code), but a
    # PathInput's real identity is its source-declared name (see
    # docs/claude/code-discovery-categories.md), which can differ from the
    # parameter it happens to fill. Resolve each by content match against
    # the registry (shared with execution_service.disconnected_report_entries
    # — see graph_builder.convert_scidb_path_inputs).
    path_input_registry = registry.get_path_inputs_registry()
    agg.path_inputs = gb.convert_scidb_path_inputs(
        scidb_agg["path_inputs"],
        path_input_registry,
        _ps.path_input_history_index(db),
        registry.get_project_root(),
    )
    gb.seed_undiscovered_path_inputs(agg.path_inputs, path_input_registry)

    logger.info("[pipeline] Filtering hidden nodes")
    # strip_var_type_values=False: this pre-grouping pass must NOT scrub
    # hidden variable types out of fn_outputs/fn_input_params VALUES — those
    # feed wiring_id (fn name + input/output var types) below, and a
    # function's wiring identity (hence its canvas node id and saved scope
    # placement) must stay stable regardless of which of its own outputs the
    # user has hidden, or the node loses its placement and vanishes from
    # non-root scopes (see graph_builder.filter_hidden docstring). The
    # post-grouping filter_hidden call further down still strips those
    # values (default True) for display, once identity is already fixed.
    gb.filter_hidden(agg, hidden_ids, strip_var_type_values=False)

    logger.info("[pipeline] Using record counts from scidb")
    record_counts = {
        vtype: vdata["record_count"] for vtype, vdata in scidb_agg["variables"].items()
    }

    pending_constants = layout_store.get_pending_constants()
    logger.debug("[pipeline] loaded %d pending constant(s)", len(pending_constants))
    pending_constants, removals = gb.auto_clean_pending_constants(
        pending_constants, agg
    )
    for const_name, pval in removals:
        layout_store.remove_pending_constant(const_name, pval)
    if removals:
        logger.debug(
            "[pipeline] removed %d pending constant value(s) that are now in database",
            len(removals),
        )

    # --- Disconnected wirings (hidden required inbound edges) ---
    # Computed on the PRE-GROUPING agg — every call site sharing a wiring
    # recomputes the same wiring_id regardless of grouping (see
    # graph_builder.hidden_wirings). Drives both run-state forcing below
    # and the per-node "disconnected" flag attached to function nodes.
    # manual_edges is fetched here (rather than further down, where it used
    # to be the first read) so a manual reconnect onto a previously-hidden
    # handle can clear the disconnected state in the same pass — see
    # graph_builder.hidden_wirings' manual_edges param.
    manual_edges_for_fn_lookup = layout_store.read_manual_edges()
    disconnected_wirings = gb.hidden_wirings(
        agg.fn_input_params,
        agg.fn_outputs,
        agg.fn_constants,
        agg.path_inputs,
        hidden_edge_ids,
        manual_edges=manual_edges_for_fn_lookup,
    )
    disconnected_fkeys = gb.wiring_disconnected_fkeys(
        agg.fn_input_params, agg.fn_outputs, disconnected_wirings, agg.path_inputs
    )
    if disconnected_wirings:
        logger.info(
            "[pipeline] %d wiring(s) disconnected: %s",
            len(disconnected_wirings),
            sorted(disconnected_wirings),
        )

    # --- Compute run states (per call site — state never blurs) ---
    logger.info("[pipeline] Computing run states (delegating to run_state)")
    run_states = _compute_run_states(
        db,
        agg.fn_input_params,
        agg.fn_outputs,
        disconnected_fkeys,
    )
    logger.info("[pipeline] computed run states for %d nodes", len(run_states))

    # --- Group call sites by wiring (one canvas node per fn + IO shape) ---
    # Constant-value call sites become variant rows (with their own state
    # chips) inside one node; staged pending values get synthesized rows.
    logger.info("[pipeline] Grouping call sites by wiring")
    agg, run_states, wiring_member_map = gb.group_call_sites_by_wiring(
        agg, run_states, pending_constants
    )
    # Hidden-id filtering ran pre-grouping for LEGACY per-call-site ids;
    # run it again now so deletions of wiring-grouped nodes (hidden id =
    # fn__{fn}__{wiring_id}) also apply.
    gb.filter_hidden(agg, hidden_ids)

    # --- Build fn_params_map and saved_configs ---
    # fn_params_map and saved_configs are keyed by fn_name (the signature
    # and saved settings don't vary across call sites).
    logger.info("[pipeline] Building function parameter maps and saved configs")
    fn_names = {fn for fn, _ in agg.fn_input_params.keys()}
    logger.debug(
        "[pipeline] building parameter maps for %d unique function(s)", len(fn_names)
    )
    fn_params_map: dict[str, list[str]] = {}
    for fn in fn_names:
        if _mr.is_matlab_function(fn):
            fn_params_map[fn] = list(_mr.get_matlab_function(fn).params)
        else:
            fn_params_map[fn] = _fn_params_from_registry(fn)

    manual_nodes = _ps.get_manual_nodes(db)
    logger.debug("[pipeline] loaded %d manual node(s)", len(manual_nodes))

    # Config keyed by node_id -- the authoritative store, and the only one that
    # can hold a setting for a node that has already run (a DB-derived node has
    # no _pipeline_nodes row at all). Passed alongside the fn_name-keyed map
    # below, which stays as the fallback for manual nodes.
    node_configs = _ps.get_node_configs(db)

    saved_configs: dict[str, dict | None] = {}
    for fn in fn_names:
        # Manual nodes can use either the legacy `fn__{fn}` ID or the
        # composite `fn__{fn}__{call_id}` ID.  Look up the legacy form
        # first (matches the pre-call-id node), then any composite manual
        # node for this fn_name as a fallback.
        cfg = node_configs.get(f"fn__{fn}") or manual_nodes.get(f"fn__{fn}", {}).get(
            "config"
        )
        if cfg is None:
            for _nid, meta in manual_nodes.items():
                if (
                    meta.get("type") == "functionNode"
                    and meta.get("label") == fn
                    and meta.get("config")
                ):
                    cfg = meta["config"]
                    break
        saved_configs[fn] = cfg

    matlab_functions = set(_mr.get_all_function_names())
    matlab_output_order = {
        name: _mr.get_matlab_function(name).output_names for name in matlab_functions
    }

    # Build matlab_param_to_class from DB variants' output_num and, as a
    # fallback for ungraduated fns with no DB history yet, from persisted
    # manual edges.
    # Source 1: DB variants' output_num → the fn's signature output name.
    from_db = _matlab_param_to_class_from_db(
        scidb_agg["functions"], matlab_functions, matlab_output_order
    )
    matlab_param_to_class: dict[str, dict[str, str]] = {
        fn: dict(mapping) for fn, mapping in from_db.items()
    }

    from scistack_gui.domain.edge_resolver import infer_manual_fn_param_to_class
    from scistack_gui.domain.graph_builder import fn_node_id

    existing_node_labels_pre = {f"var__{t}": t for t in agg.all_var_types}
    for fn in matlab_functions:
        # Collect all DB-derived node IDs for this fn (one per call site)
        # plus any manual nodes that share the label.
        fn_ids = {
            fn_node_id(fn_name, cid)
            for (fn_name, cid) in agg.fn_input_params.keys()
            if fn_name == fn
        }
        fn_ids |= {
            nid
            for nid, meta in manual_nodes.items()
            if meta.get("type") == "functionNode" and meta.get("label") == fn
        }
        edge_map = infer_manual_fn_param_to_class(
            fn_node_ids=fn_ids,
            manual_edges=manual_edges_for_fn_lookup,
            manual_nodes=manual_nodes,
            existing_node_labels=existing_node_labels_pre,
        )
        if edge_map:
            existing = matlab_param_to_class.setdefault(fn, {})
            for p, c in edge_map.items():
                existing.setdefault(p, c)
        # INFO, not DEBUG, and attributed per source. An empty merged map is
        # the precondition for the fn↔output-variable edge disappearing from
        # the canvas, and at DEBUG the log showed only the empty result with
        # no way to tell which of the two sources failed.
        if fn in matlab_param_to_class or edge_map or fn in from_db:
            logger.info(
                "[pipeline] matlab_param_to_class: fn=%s from_db_output_num=%s "
                "from_manual_edges=%s merged=%s",
                fn,
                from_db.get(fn, {}),
                edge_map,
                matlab_param_to_class.get(fn, {}),
            )

    # --- Load sweeps (source-scanned — see docs/claude/code-discovery-categories.md).
    # "Delete" only hides the node (layout_service.delete_parameter) — the source
    # declaration is never touched — so this must filter by hidden_ids
    # explicitly; unlike var__/pathInput__ nodes, a Parameter's declared
    # values have no DB-derived aggregation path that filter_hidden covers.
    source_parameters = {
        name: p
        for name, p in registry.get_parameters_registry().items()
        if f"{gb.PARAM_ID_PREFIX}{name}" not in hidden_ids
    }
    logger.debug(
        "[pipeline] loaded %d parameter(s) from registry", len(source_parameters)
    )

    # --- Build nodes (pure) ---
    logger.info("[pipeline] Building nodes (delegating to graph_builder)")
    nodes = gb.build_variable_nodes(agg.all_var_types, record_counts, run_states)
    var_node_count = len(nodes)
    hidden_const_values: dict[str, set] = {}
    for row in _ps.list_hidden_parameter_values(db, pipeline_id):
        hidden_const_values.setdefault(row["const_name"], set()).add(row["value"])
    # Constants and Sweeps are ONE node kind (Parameters, D6) — built
    # together so a Parameter never changes node type or id when a second
    # value turns its declaration from a Constant into a Sweep.
    entities_file = (
        str(registry._config.entities_file)
        if registry._config is not None and registry._config.entities_file is not None
        else None
    )
    nodes += gb.build_parameter_nodes(
        agg.const_counts,
        pending_constants,
        source_parameters,
        hidden_const_values,
        _ps.get_parameter_value_groups(db),
        entities_file,
    )
    const_node_count = len(nodes) - var_node_count
    nodes += gb.build_path_input_nodes(agg.path_inputs)
    path_input_node_count = len(nodes) - var_node_count - const_node_count
    sweep_node_count = 0
    nodes += gb.build_function_nodes(
        agg.fn_input_params,
        agg.fn_outputs,
        agg.fn_constants,
        agg.fn_variants_map,
        fn_params_map,
        run_states,
        matlab_functions,
        saved_configs,
        matlab_output_order=matlab_output_order,
        matlab_param_to_class=matlab_param_to_class,
        node_configs=node_configs,
    )
    fn_node_count = (
        len(nodes)
        - var_node_count
        - const_node_count
        - path_input_node_count
        - sweep_node_count
    )
    logger.info(
        "[pipeline] built %d nodes: %d variable, %d constant, %d path input, "
        "%d sweep, %d function",
        len(nodes),
        var_node_count,
        const_node_count,
        path_input_node_count,
        sweep_node_count,
        fn_node_count,
    )

    # --- Tag disconnected function nodes ---
    # By this point agg is wiring-grouped, so function node ids are exactly
    # fn__{fn_name}__{wiring_id} — directly comparable to disconnected_wirings
    # with no further translation. Visual/state only (see run_state above
    # for the actual color); execution_service enforces un-runnability
    # independently at run time.
    if disconnected_wirings:
        tagged = 0
        for node in nodes:
            if node["type"] != "functionNode":
                continue
            parsed = gb.parse_fn_node_id(node["id"])
            if parsed is not None and parsed in disconnected_wirings:
                node["data"]["disconnected"] = True
                tagged += 1
        logger.debug("[pipeline] tagged %d function node(s) disconnected", tagged)

    # --- Build edges (pure) ---
    logger.info("[pipeline] Building edges (delegating to graph_builder)")
    manual_edges_list = manual_edges_for_fn_lookup
    edges = gb.build_edges(
        agg.fn_input_params,
        agg.fn_outputs,
        agg.const_fns,
        agg.path_inputs,
        manual_edges_list,
        hidden_ids,
        matlab_param_to_class=matlab_param_to_class,
        hidden_edge_ids=hidden_edge_ids,
    )
    logger.info("[pipeline] built %d edges", len(edges))

    # --- Merge manual nodes ---
    # pipelineNode entries are NOT generic manual nodes: they are built by
    # scope_service (ports + binding) after filtering, so exclude them here.
    logger.info("[pipeline] Merging manual nodes (delegating to graph_builder)")
    positions_by_scope = layout_store.read_positions_by_scope()
    saved_positions: dict = {}
    for _scope_positions_map in positions_by_scope.values():
        saved_positions.update(_scope_positions_map)
    logger.debug(
        "[pipeline] loaded %d saved position(s) across %d scope(s)",
        len(saved_positions),
        len(positions_by_scope),
    )
    mergeable_manual_nodes = {
        nid: meta
        for nid, meta in manual_nodes.items()
        if meta.get("type") != "pipelineNode"
    }
    to_add, graduations = gb.merge_manual_nodes(
        nodes, mergeable_manual_nodes, saved_positions
    )

    # Function-node graduation refinement — merge_manual_nodes decides by
    # (type, label) alone, which is no longer sufficient now that multiple
    # real call sites can share one function name (e.g. compute_rolling_vo2
    # fed by RawVO2 in one node, RawHeartRate in another). Two passes, both
    # keyed off each manual function node's OWN resolved wiring:
    #
    # Pass 1 (reject): merge_manual_nodes proposed graduation because
    # exactly one label-matched candidate exists — but "exactly one"
    # candidate isn't necessarily the RIGHT one once other wirings share
    # the label. Absence of wiring info (never wired yet) is NOT treated
    # as a conflict — see test_graduation_preserves_sub_scope_membership,
    # a bare unwired node with a single real candidate must still graduate
    # immediately, the original one-candidate-wins UX.
    #
    # Pass 2 (promote): merge_manual_nodes REFUSED to graduate at all
    # because 0 or >1 candidates share the label — but if this manual
    # node's own wiring uniquely matches exactly one of them (however many
    # OTHER same-named candidates also exist), it should still graduate.
    # Without this, a manual node that's already been run successfully
    # (and shows green) never merges with its own real counterpart once a
    # second same-named wiring exists: a permanent duplicate "replica"
    # node stays on the canvas forever (found via a real GUI session).
    existing_node_labels = {n["id"]: n["data"]["label"] for n in nodes}

    def _resolve_manual_fn_wiring(node_id: str, fn_label: str):
        resolved = resolve_function_edges(
            fn_node_ids={node_id},
            manual_edges=manual_edges_list,
            manual_nodes=manual_nodes,
            existing_node_labels=existing_node_labels,
        )
        inferred_inputs = {p: ts[0] for p, ts in resolved.input_types.items() if ts}
        return resolved, inferred_inputs

    validated_graduations = []
    for action in graduations:
        meta = manual_nodes[action.old_id]
        if meta["type"] != "functionNode":
            validated_graduations.append(action)
            continue
        resolved, inferred_inputs = _resolve_manual_fn_wiring(action.old_id, meta["label"])
        candidate_parsed = gb.parse_fn_node_id(action.new_id)
        candidate = (
            _find_db_fn_candidate(agg, meta["label"], candidate_parsed[1])
            if candidate_parsed is not None
            else None
        )
        conflict = candidate is not None and _wiring_conflicts_with_candidate(
            inferred_inputs, resolved.output_types, candidate[0], candidate[1]
        )
        if not conflict:
            validated_graduations.append(action)
        else:
            logger.warning(
                "[pipeline] graduation candidate %s -> %s rejected: manual "
                "node's own wiring (inputs=%s, outputs=%s) conflicts with "
                "the candidate's real wiring (inputs=%s, outputs=%s) — "
                "keeping %s as a separate manual node",
                action.old_id,
                action.new_id,
                inferred_inputs,
                resolved.output_types,
                candidate[0] if candidate else None,
                candidate[1] if candidate else None,
                action.old_id,
            )
            to_add.append(action.old_id)
    graduations = validated_graduations

    still_to_add = []
    for node_id in to_add:
        meta = manual_nodes[node_id]
        if meta["type"] != "functionNode":
            still_to_add.append(node_id)
            continue
        resolved, inferred_inputs = _resolve_manual_fn_wiring(node_id, meta["label"])
        if not resolved.output_types:
            still_to_add.append(node_id)
            continue
        my_wiring = gb.wiring_id(
            meta["label"],
            inferred_inputs,
            set(resolved.output_types),
            resolved.path_input_params,
        )
        matches = [
            n["id"]
            for n in nodes
            if n["type"] == "functionNode"
            and n["data"]["label"] == meta["label"]
            and (gb.parse_fn_node_id(n["id"]) or (None, None))[1] == my_wiring
        ]
        if len(matches) != 1:
            still_to_add.append(node_id)
            continue
        target_id = gb.placement_id(matches[0], meta.get("pipeline_id") or "main")
        if target_id in saved_positions:
            still_to_add.append(node_id)
            continue
        logger.debug(
            "[pipeline] wiring-matched graduation: %s -> %s (inputs=%s, outputs=%s)",
            node_id,
            target_id,
            inferred_inputs,
            resolved.output_types,
        )
        graduations.append(gb.GraduationAction(old_id=node_id, new_id=target_id))
    to_add = still_to_add

    # Collision guard — the passes above each decide, per manual node,
    # "does THIS ONE graduate" without knowing about siblings. Two manual
    # nodes sharing a label can independently resolve to the SAME target
    # (e.g. one wired-and-run, one left completely unwired: Pass 1's
    # "absence of wiring is not a conflict" rule lets the unwired one
    # graduate too, since from its own perspective there's no evidence it's
    # different). graduate_manual_node only deletes the manual row and
    # never creates the target (it already exists from real DB data), so a
    # second graduation to the same target silently deletes that manual
    # node with nothing left to show for it — found via a real GUI session
    # (two compute_rolling_vo2 placeholders, one wired to RawVO2 and run,
    # one left completely disconnected; running the first made the second
    # vanish, see plan-duplicate-manual-node-graduation-collision.md).
    #
    # Resolve by preferring the graduation with actual wiring evidence (a
    # non-empty inferred input/output type — i.e. it matched its candidate
    # on more than "you were the only option") over one that only passed
    # because it had no wiring to contradict anything. Ties (or all-unwired
    # collisions) keep one deterministically; the rest are demoted back to
    # a normal (red, unrun) manual node instead of being deleted.
    def _has_wiring_evidence(action: gb.GraduationAction) -> bool:
        meta = manual_nodes[action.old_id]
        if meta["type"] != "functionNode":
            return True
        resolved, inferred_inputs = _resolve_manual_fn_wiring(action.old_id, meta["label"])
        return bool(inferred_inputs) or bool(resolved.output_types)

    by_target: dict[str, list[gb.GraduationAction]] = {}
    for action in graduations:
        by_target.setdefault(action.new_id, []).append(action)

    deduped_graduations = []
    for target_id, actions in by_target.items():
        if len(actions) == 1:
            deduped_graduations.append(actions[0])
            continue
        ranked = sorted(actions, key=lambda a: not _has_wiring_evidence(a))
        winner, losers = ranked[0], ranked[1:]
        deduped_graduations.append(winner)
        for loser in losers:
            logger.warning(
                "[pipeline] graduation collision on target %s: %s and %s "
                "both resolved to the same target — keeping %s, demoting "
                "%s back to a separate manual node",
                target_id,
                winner.old_id,
                loser.old_id,
                winner.old_id,
                loser.old_id,
            )
            to_add.append(loser.old_id)
    graduations = deduped_graduations

    # Execute graduation side effects.
    logger.info("[pipeline] Executing %d graduation action(s)", len(graduations))
    for action in graduations:
        layout_store.graduate_manual_node(action.old_id, action.new_id)
        logger.debug(
            "[pipeline] graduated manual node: %s -> %s", action.old_id, action.new_id
        )
        # graduate_manual_node rewrites edge endpoints in the DB
        # (pipeline_store.rename_edge_endpoints), but `edges` here was
        # already built earlier in this same call — patch it in-memory too
        # so this response isn't missing edges that just got graduated.
        for e in edges:
            if e["source"] == action.old_id:
                e["source"] = action.new_id
            if e["target"] == action.old_id:
                e["target"] = action.new_id

    # Build and append manual nodes that should be added.
    logger.info("[pipeline] Building %d manual node(s) to add", len(to_add))
    for node_id in to_add:
        meta = manual_nodes[node_id]
        # For function nodes, resolve edges and compute state.
        resolved_input_params = None
        resolved_output_types = None
        manual_fn_state = None
        if meta["type"] == "glueNode":
            # Handles only. A glue node has no run state to compute (D5) and
            # no output type to resolve — its output is fused into whatever
            # consumes it and is never saved, so a glue node feeding nothing
            # renders inert rather than red.
            fn_label = meta["label"]
            resolved_input_params = {
                p: "" for p in _fn_params_from_registry(fn_label)
            }
            glue_resolved = resolve_function_edges(
                fn_node_ids={node_id},
                manual_edges=manual_edges_list,
                manual_nodes=manual_nodes,
                existing_node_labels=existing_node_labels,
            )
            for p, ts in glue_resolved.input_types.items():
                if ts:
                    resolved_input_params[p] = ts[0]
        elif meta["type"] == "functionNode":
            fn_label = meta["label"]
            sig_params = _fn_params_from_registry(fn_label)
            resolved = resolve_function_edges(
                fn_node_ids={node_id},
                manual_edges=manual_edges_list,
                manual_nodes=manual_nodes,
                existing_node_labels=existing_node_labels,
            )
            inferred_inputs = {p: ts[0] for p, ts in resolved.input_types.items() if ts}
            resolved_input_params = {p: inferred_inputs.get(p, "") for p in sig_params}
            for p, t in inferred_inputs.items():
                if p not in resolved_input_params:
                    resolved_input_params[p] = t
            resolved_output_types = resolved.output_types
            # For state computation, always use the edge-resolved output types
            # (the actual variable class names like 'XSENSLoaded'), NOT the
            # MATLAB declared output parameter names (like 'extracted_data').
            state_output_types = resolved_output_types
            # For manual MATLAB function nodes, always use the declared output
            # names from the function signature as handles. Connected edges carry
            # the actual var-label mapping via sourceHandle, so the handle set
            # must always match the full signature regardless of what's wired up.
            if _mr.is_matlab_function(fn_label):
                info = _mr.get_matlab_function(fn_label)
                resolved_output_types = list(info.output_names)
                logger.debug(
                    "manual fn %s (MATLAB): using declared output_names=%s, "
                    "edge-resolved output_types=%s",
                    fn_label,
                    resolved_output_types,
                    state_output_types,
                )
            if state_output_types:
                # _own_state_for_function checks scidb.check_node_state
                # without a call_id, so it answers "has THIS FUNCTION NAME
                # ever produced these outputs" — blind to which inputs fed
                # it. Two manual nodes can share a function name while
                # being wired to different inputs (e.g. compute_rolling_vo2
                # fed by RawVO2 vs. by RawHeartRate); without this guard,
                # the second would read the first's completed run as its
                # own and show green despite never having been run. Same
                # conflict check the graduation-candidate validation above
                # uses — an unwired/partially-wired node is still trusted
                # (no basis to say it's different), only an ACTIVE mismatch
                # forces red.
                found_any_history = False
                compatible_with_some_history = False
                for fn_name, wid in agg.fn_input_params:
                    if fn_name != fn_label:
                        continue
                    found_any_history = True
                    real_inputs, real_outputs = _find_db_fn_candidate(
                        agg, fn_label, wid
                    )
                    if not _wiring_conflicts_with_candidate(
                        inferred_inputs, state_output_types, real_inputs, real_outputs
                    ):
                        compatible_with_some_history = True
                        break
                trust_history = not found_any_history or compatible_with_some_history
                if trust_history:
                    manual_fn_state = _own_state_for_function(
                        db, fn_label, set(state_output_types)
                    )
                else:
                    manual_fn_state = "red"
                logger.debug(
                    "manual fn %s: computed state=%s (outputs=%s, "
                    "trust_history=%s)",
                    fn_label,
                    manual_fn_state,
                    state_output_types,
                    trust_history,
                )
            else:
                manual_fn_state = "red"
                logger.debug(
                    "manual fn %s: no inferred outputs, defaulting to red", fn_label
                )

        node = gb.build_manual_node(
            node_id,
            meta,
            pending_constants,
            manual_fn_state,
            resolved_input_params,
            resolved_output_types,
            matlab_functions,
            node_config=node_configs.get(node_id),
        )
        nodes.append(node)
        logger.debug(
            "[pipeline] built manual node: %s (type=%s, label=%s)",
            node_id,
            meta["type"],
            meta["label"],
        )

    # --- Scope filtering (nested pipelines) ---
    # Graduations above MOVE positions (the scope-membership record for
    # DB-derived nodes) and delete manual-node rows; filtering on the
    # pre-graduation snapshots would place a just-graduated node in the
    # root scope (no position found) while its position actually lives in
    # a sub scope — so refresh both inputs when any graduation ran.
    if graduations:
        positions_by_scope = layout_store.read_positions_by_scope()
        manual_nodes = _ps.get_manual_nodes(db)
        logger.debug(
            "[pipeline] refreshed scope-membership inputs after %d graduation(s)",
            len(graduations),
        )

    # --- One-time wiring migration ---
    # Pre-grouping documents keyed positions (= scope membership) and manual
    # edges by per-call-site node ids; adopt them onto the group node ids.
    # Idempotent: legacy keys are dropped after adoption.
    adoptions, drop_ids = gb.legacy_position_adoptions(
        wiring_member_map, positions_by_scope
    )
    for action in adoptions:
        layout_store.write_node_position(
            action["new_id"], action["x"], action["y"], pipeline_id=action["scope"]
        )
        logger.info(
            "[pipeline] wiring migration: adopted position of "
            "legacy call-site node into %s (scope=%s)",
            action["new_id"],
            action["scope"],
        )
    for old_id in drop_ids:
        layout_store.drop_node_positions(old_id)
    edge_rewrites = gb.legacy_edge_rewrites(wiring_member_map, manual_edges_list)
    for rewritten in edge_rewrites:
        _ps.write_manual_edge(db, rewritten)
        # Patch the already-built in-memory edge too so THIS response is
        # correct without a second fetch.
        for e in edges:
            if e["id"] == rewritten["id"]:
                e["source"] = rewritten["source"]
                e["target"] = rewritten["target"]
        logger.info(
            "[pipeline] wiring migration: rewrote manual edge %s "
            "endpoints to group node ids",
            rewritten["id"],
        )
    if adoptions or drop_ids:
        positions_by_scope = layout_store.read_positions_by_scope()

    # --- Re-dedup after endpoint rewrites ---
    # build_edges deduped manual edges against DB-derived ones, but that ran
    # BEFORE graduation and before the wiring migration above — both of which
    # rewrite manual-edge endpoints onto DB-derived node ids, which is exactly
    # what turns a manual edge into a duplicate of a DB-derived one. Without
    # this pass the FIRST build after a run returns both copies of every
    # just-graduated wire (the next, unrelated rebuild returns the correct
    # set), so the canvas draws doubled edges until something else refreshes
    # it. Unconditional and idempotent: with no rewrites it drops nothing.
    edges, superseded_edges = gb.drop_superseded_manual_edges(edges)
    if superseded_edges:
        logger.info(
            "[pipeline] dropped %d manual edge(s) superseded by DB-derived "
            "edges after endpoint rewriting: %s",
            len(superseded_edges),
            ", ".join(
                f"{e['id']} ({e['source']} -> {e['target']})"
                for e in superseded_edges
            ),
        )

    logger.info("[pipeline] Filtering graph to scope %s", pipeline_id)
    from scistack_gui.domain.scope_filter import resolve_scope_view
    from scistack_gui.services.scope_service import build_pipeline_nodes

    nodes, edges = resolve_scope_view(
        nodes, edges, pipeline_id, manual_nodes, positions_by_scope
    )
    # Config saved under a placement-qualified id (``...::scope``) is only
    # findable once the ids have been resolved -- build_function_nodes above
    # ran on bare ids. See gb.apply_placement_configs.
    applied = gb.apply_placement_configs(nodes, node_configs)
    if applied:
        logger.info(
            "[pipeline] rehydrated placement-qualified config on %d node(s)", applied
        )
    nodes += build_pipeline_nodes(db, pipeline_id)

    # --- Endpoint classification (plot_/stat_ prefixes) ---
    # Detection lives in scidb (_endpoint_kind — same source of truth as
    # Pipeline.endpoints()/for_each's endpoint policy); the GUI only tags.
    from scidb.foreach import _endpoint_kind

    endpoint_count = 0
    for n in nodes:
        if n["type"] == "functionNode":
            kind = _endpoint_kind(n["data"]["label"])
            if kind is not None:
                n["data"]["endpoint_kind"] = kind
                endpoint_count += 1
    if endpoint_count:
        logger.info("[pipeline] tagged %d endpoint node(s)", endpoint_count)

    logger.info("[pipeline] Graph build complete - assembling final result")
    node_types = {}
    for n in nodes:
        t = n["type"]
        node_types[t] = node_types.get(t, 0) + 1
    logger.info(
        "[pipeline] graph built successfully (scope=%s): %d total nodes (%s), %d edges",
        pipeline_id,
        len(nodes),
        ", ".join(f"{c} {t}" for t, c in sorted(node_types.items())),
        len(edges),
    )

    return {"nodes": nodes, "edges": edges, "pipeline_id": pipeline_id}


@router.get("/pipeline")
def get_pipeline(pipeline_id: str = "main", db: DatabaseManager = Depends(get_db)):
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    return get_pipeline_graph(db, pipeline_id)


@router.get("/function/{fn_name}/params")
def get_function_params(fn_name: str):
    from scistack_gui.services.pipeline_service import get_function_full_info

    return get_function_full_info(fn_name)


@router.get("/function/{fn_name}/source")
def get_function_source(fn_name: str):
    from scistack_gui.services.pipeline_service import (
        get_function_source as _get_source,
    )

    return _get_source(fn_name)


@router.get("/function/{fn_name}/doc")
def get_function_doc(fn_name: str):
    from scistack_gui.services.pipeline_service import get_function_doc as _get_doc

    return _get_doc(fn_name)


@router.put("/parameters/{name}/pending/{value}")
async def add_pending_constant_value(name: str, value: str):
    from scistack_gui.services.layout_service import put_pending_constant

    put_pending_constant(name, value)
    await ws.broadcast({"type": "dag_updated"})
    return {"ok": True}


@router.delete("/parameters/{name}/pending/{value}")
async def remove_pending_constant_value(name: str, value: str):
    from scistack_gui.services.layout_service import delete_pending_constant

    delete_pending_constant(name, value)
    await ws.broadcast({"type": "dag_updated"})
    return {"ok": True}


class HideComboRequest(BaseModel):
    node_id: str | None = None
    variant_key: dict


@router.post("/functions/{function_name}/hidden_combos")
def hide_combo(
    function_name: str,
    body: HideComboRequest,
    db: DatabaseManager = Depends(get_db),
):
    from scistack_gui.services.layout_service import hide_variant_combo

    return hide_variant_combo(db, function_name, body.node_id, body.variant_key)


@router.delete("/functions/hidden_combos/{node_id}")
def unhide_combo(node_id: str, db: DatabaseManager = Depends(get_db)):
    from scistack_gui.services.layout_service import unhide_variant_combo

    return unhide_variant_combo(db, node_id)


@router.get("/functions/{function_name}/hidden_combos")
def list_hidden_combos(function_name: str, db: DatabaseManager = Depends(get_db)):
    from scistack_gui.services.layout_service import get_hidden_combos

    return get_hidden_combos(db, function_name)


@router.post("/parameters/{name}/hidden_values/{value}")
def hide_parameter_value(
    name: str,
    value: str,
    pipeline_id: str = "main",
    db: DatabaseManager = Depends(get_db),
):
    from scistack_gui.services.layout_service import hide_parameter_value as _hide

    return _hide(db, name, value, pipeline_id)


@router.delete("/parameters/{name}/hidden_values/{value}")
def unhide_parameter_value(
    name: str,
    value: str,
    pipeline_id: str = "main",
    db: DatabaseManager = Depends(get_db),
):
    from scistack_gui.services.layout_service import unhide_parameter_value as _unhide

    return _unhide(db, name, value, pipeline_id)


class ParameterGroupChecked(BaseModel):
    values: list[str] = []
    checked: bool = True


@router.post("/parameters/{name}/group_checked")
def set_parameter_group_checked(
    name: str,
    body: ParameterGroupChecked,
    pipeline_id: str = "main",
    db: DatabaseManager = Depends(get_db),
):
    """Check/uncheck every member of a generated value set in one call —
    the per-value route above would be one request per value."""
    from scistack_gui.services.layout_service import (
        set_parameter_group_checked as _set,
    )

    return _set(db, name, body.values, body.checked, pipeline_id)


@router.get("/parameters/hidden_values")
def list_hidden_parameter_values(
    pipeline_id: str = "main", db: DatabaseManager = Depends(get_db)
):
    from scistack_gui.services.layout_service import get_hidden_constant_values

    return get_hidden_constant_values(db, pipeline_id)
