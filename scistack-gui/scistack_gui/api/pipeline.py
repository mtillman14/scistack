"""
GET /pipeline

Returns the pipeline graph as React Flow nodes and edges.

Node types:
  - "variableNode": a named variable type (RawEMG, FilteredEMG, ...)
  - "functionNode": a pipeline function (bandpass_filter, ...)

Positions are set to (0, 0) here; the layout endpoint overwrites them with
saved positions, and the frontend assigns dagre positions for new nodes.
"""

import logging
import time
import inspect

logger = logging.getLogger(__name__)
from fastapi import APIRouter, Depends
from scidb.database import DatabaseManager
from scistack_gui.db import get_db
from scistack_gui import layout as layout_store
from scistack_gui import registry
from scistack_gui.api import ws

router = APIRouter()


def _parse_path_input(value: str) -> dict | None:
    """If *value* (from __inputs) represents a PathInput, return parsed info.

    Delegates to domain.graph_builder.parse_path_input.
    """
    from scistack_gui.domain.graph_builder import parse_path_input
    return parse_path_input(value)


def _fn_params_from_registry(fn_name: str) -> list[str]:
    """Return non-private parameter names from the registered function's signature.

    Falls back to the MATLAB registry if the function isn't a Python function.
    """
    fn = registry._functions.get(fn_name)
    if fn is not None:
        try:
            return [
                name for name in inspect.signature(fn).parameters
                if not name.startswith('_')
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
    from scistack_gui import matlab_registry
    from sci_matlab.bridge import MatlabLineageFcn

    info = matlab_registry.get_matlab_function(fn_name)
    # unpack_output MUST match sci-matlab/.../+scihist/for_each.m's default.
    # Native MATLAB multi-output (`[a,b,c] = fn(...)`) uses unpack_output=False
    # and is unpacked at runtime via LineageFcn's n_out>1 branch. unpack_output=True
    # is only for the rarer single-cell-array-return pattern.
    proxy = MatlabLineageFcn(info.source_hash, fn_name, unpack_output=False)
    logger.debug(
        "[pipeline] matlab proxy fn=%s source_hash=%s unpack=False hash=%s",
        fn_name, info.source_hash[:12], proxy.hash[:12],
    )
    proxy.__name__ = fn_name
    return proxy


def _compute_run_states(
    db: DatabaseManager,
    fn_input_params: dict[tuple, dict],
    fn_outputs: dict[tuple, set],
    fn_constants: dict[tuple, set] | None = None,
    pending_constants: dict[str, set] | None = None,
) -> dict[str, str]:
    """
    Compute run_state for every function and variable node.

    Function-keyed inputs use FnKey = (fn_name, call_id) so the same fn
    reused across multiple for_each call sites gets a distinct state per
    call site.

    Pass 1 — own state per function-call-site:
      Calls scihist.check_multiple_nodes_state() for all nodes in batch.

    Pass 2 — propagate staleness through the DAG (delegated to domain layer).

    Returns {node_id: "green"|"grey"|"red"} for fn__ and var__ nodes.
    """
    from scistack_gui.domain.run_state import propagate_run_states
    from scihist import check_multiple_nodes_state
    from scidb import BaseVariable

    t0 = time.monotonic()

    # --- Pass 1: Build function registry and nodes list ---
    # Build registry combining Python and MATLAB functions
    fn_registry = dict(registry._functions)  # Copy Python functions

    from scistack_gui import matlab_registry
    for fn_name in fn_input_params.keys():
        fn_name_str, _ = fn_name
        if fn_name_str not in fn_registry and matlab_registry.is_matlab_function(fn_name_str):
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
            nodes.append({
                "fn_name": fn_name,
                "call_id": cid,
                "outputs": output_classes,
            })

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
                fn_name, cid, fn_own_state[fkey],
                counts.get("up_to_date", 0),
                counts.get("stale", 0),
                counts.get("missing", 0),
            )
        else:
            # Function not in results (no outputs or error) → mark as red
            fn_own_state[fkey] = "red"

    # --- Pass 2: DAG propagation (pure) ---
    result = propagate_run_states(
        fn_own_state, fn_input_params, fn_outputs,
        fn_constants, pending_constants,
    )

    elapsed_ms = (time.monotonic() - t0) * 1000
    counts = {"green": 0, "grey": 0, "red": 0}
    for nid, s in result.items():
        if nid.startswith("fn__"):
            counts[s] = counts.get(s, 0) + 1
    logger.debug(
        "run_states complete: %d call sites in %.1fms (%d green, %d grey, %d red)",
        len([k for k in result if k.startswith("fn__")]), elapsed_ms,
        counts["green"], counts["grey"], counts["red"],
    )
    return result


def _build_graph(db: DatabaseManager) -> dict:
    """
    Build nodes and edges from list_pipeline_variants() and list_variables().

    Delegates pure logic to domain.graph_builder and domain.edge_resolver;
    this function orchestrates data fetching and side effects.
    """
    logger.info("[pipeline] Step 1: Starting graph build orchestration")

    from scistack_gui import pipeline_store as _ps
    from scistack_gui import matlab_registry as _mr
    from scistack_gui.domain import graph_builder as gb
    from scistack_gui.domain.edge_resolver import resolve_function_edges

    hidden_ids = _ps.get_hidden_node_ids(db)
    logger.debug("[pipeline] loaded %d hidden node IDs", len(hidden_ids))

    # --- Fetch aggregated data from scidb (replaces steps 2-5) ---
    logger.info("[pipeline] Step 2: Fetching aggregated variants from scidb")
    scidb_agg = db.get_aggregated_variants()
    logger.info("[pipeline] fetched data for %d functions, %d variables, %d constants, %d path inputs",
                len(scidb_agg["functions"]), len(scidb_agg["variables"]),
                len(scidb_agg["constants"]), len(scidb_agg["path_inputs"]))

    # Convert scidb format to AggregatedData format for compatibility
    logger.info("[pipeline] Step 3: Converting to AggregatedData format")
    from collections import defaultdict
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

    # Convert path_inputs
    for param_name, pi_data in scidb_agg["path_inputs"].items():
        agg.path_inputs[param_name] = {
            "template": pi_data["template"],
            "root_folder": pi_data["root_folder"],
            "functions": set(tuple(f) for f in pi_data["functions"]),
        }

    logger.info("[pipeline] Step 4: Filtering hidden nodes")
    gb.filter_hidden(agg, hidden_ids)

    logger.info("[pipeline] Step 5: Using record counts from scidb")
    record_counts = {vtype: vdata["record_count"]
                     for vtype, vdata in scidb_agg["variables"].items()}

    pending_constants = layout_store.get_pending_constants()
    logger.debug("[pipeline] loaded %d pending constant(s)", len(pending_constants))
    pending_constants, removals = gb.auto_clean_pending_constants(
        pending_constants, agg.const_counts)
    for const_name, pval in removals:
        layout_store.remove_pending_constant(const_name, pval)
    if removals:
        logger.debug("[pipeline] removed %d pending constant value(s) that are now in database", len(removals))

    # --- Compute run states ---
    logger.info("[pipeline] Step 6: Computing run states (delegating to run_state)")
    run_states = _compute_run_states(
        db, agg.fn_input_params, agg.fn_outputs,
        agg.fn_constants, pending_constants,
    )
    logger.info("[pipeline] computed run states for %d nodes", len(run_states))

    # --- Build fn_params_map and saved_configs ---
    # fn_params_map and saved_configs are keyed by fn_name (the signature
    # and saved settings don't vary across call sites).
    logger.info("[pipeline] Step 7: Building function parameter maps and saved configs")
    fn_names = {fn for fn, _ in agg.fn_input_params.keys()}
    logger.debug("[pipeline] building parameter maps for %d unique function(s)", len(fn_names))
    fn_params_map: dict[str, list[str]] = {}
    for fn in fn_names:
        if _mr.is_matlab_function(fn):
            fn_params_map[fn] = list(_mr.get_matlab_function(fn).params)
        else:
            fn_params_map[fn] = _fn_params_from_registry(fn)

    manual_nodes = _ps.get_manual_nodes(db)
    logger.debug("[pipeline] loaded %d manual node(s)", len(manual_nodes))
    saved_configs: dict[str, dict | None] = {}
    for fn in fn_names:
        # Manual nodes can use either the legacy `fn__{fn}` ID or the
        # composite `fn__{fn}__{call_id}` ID.  Look up the legacy form
        # first (matches the pre-call-id node), then any composite manual
        # node for this fn_name as a fallback.
        cfg = manual_nodes.get(f"fn__{fn}", {}).get("config")
        if cfg is None:
            for nid, meta in manual_nodes.items():
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
        name: _mr.get_matlab_function(name).output_names
        for name in matlab_functions
    }

    # Build matlab_param_to_class from DB variants' __output_num (written by
    # _build_lineage_version_keys) and, as a fallback for ungraduated fns with
    # no DB history yet, from persisted manual edges.
    matlab_param_to_class: dict[str, dict[str, str]] = {}
    # Collect all variants from the aggregated data
    all_variants = []
    for (fn_name, call_id), fn_data in scidb_agg["functions"].items():
        for v in fn_data.get("variants", []):
            # Add function_name to variant dict for compatibility
            variant = {"function_name": fn_name, **v}
            all_variants.append(variant)

    for v in all_variants:
        fn = v.get("function_name")
        if fn not in matlab_functions:
            continue
        onum = v.get("output_num")
        out_type = v.get("output_type")
        if onum is None or out_type is None:
            continue
        names = matlab_output_order.get(fn) or []
        if 0 <= int(onum) < len(names):
            matlab_param_to_class.setdefault(fn, {})[names[int(onum)]] = out_type
    from scistack_gui.domain.edge_resolver import infer_manual_fn_param_to_class
    from scistack_gui.domain.graph_builder import fn_node_id
    manual_edges_for_fn_lookup = layout_store.read_manual_edges()
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
            nid for nid, meta in manual_nodes.items()
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
    logger.debug(
        "[pipeline] matlab_param_to_class=%s",
        {k: dict(v) for k, v in matlab_param_to_class.items()},
    )

    # --- Overlay saved path inputs ---
    logger.info("[pipeline] Step 8: Overlaying saved path inputs")
    saved_path_inputs = layout_store.read_all_path_input_names()
    logger.debug("[pipeline] loaded %d saved path input(s)", len(saved_path_inputs))
    gb.overlay_saved_path_inputs(agg.path_inputs, saved_path_inputs)

    # --- Build nodes (pure) ---
    logger.info("[pipeline] Step 9: Building nodes (delegating to graph_builder)")
    nodes = gb.build_variable_nodes(agg.all_var_types, record_counts, run_states)
    var_node_count = len(nodes)
    nodes += gb.build_constant_nodes(agg.const_counts, pending_constants)
    const_node_count = len(nodes) - var_node_count
    nodes += gb.build_path_input_nodes(agg.path_inputs)
    path_input_node_count = len(nodes) - var_node_count - const_node_count
    nodes += gb.build_function_nodes(
        agg.fn_input_params, agg.fn_outputs, agg.fn_constants,
        agg.fn_variants_map, fn_params_map, run_states,
        matlab_functions, saved_configs,
        matlab_output_order=matlab_output_order,
        matlab_param_to_class=matlab_param_to_class,
    )
    fn_node_count = len(nodes) - var_node_count - const_node_count - path_input_node_count
    logger.info("[pipeline] built %d nodes: %d variable, %d constant, %d path input, %d function",
                len(nodes), var_node_count, const_node_count, path_input_node_count, fn_node_count)

    # --- Build edges (pure) ---
    logger.info("[pipeline] Step 10: Building edges (delegating to graph_builder)")
    manual_edges_list = manual_edges_for_fn_lookup
    edges = gb.build_edges(
        agg.fn_input_params, agg.fn_outputs, agg.const_fns,
        agg.path_inputs, manual_edges_list, hidden_ids,
        matlab_param_to_class=matlab_param_to_class,
    )
    logger.info("[pipeline] built %d edges", len(edges))

    # --- Merge manual nodes ---
    logger.info("[pipeline] Step 11: Merging manual nodes (delegating to graph_builder)")
    saved_positions = layout_store.read_layout()["positions"]
    logger.debug("[pipeline] loaded %d saved position(s)", len(saved_positions))
    to_add, graduations = gb.merge_manual_nodes(nodes, manual_nodes, saved_positions)

    # Execute graduation side effects.
    logger.info("[pipeline] Step 12: Executing %d graduation action(s)", len(graduations))
    for action in graduations:
        layout_store.graduate_manual_node(action.old_id, action.new_id)
        logger.debug("[pipeline] graduated manual node: %s -> %s", action.old_id, action.new_id)

    # Build and append manual nodes that should be added.
    logger.info("[pipeline] Step 13: Building %d manual node(s) to add", len(to_add))
    existing_node_labels = {n["id"]: n["data"]["label"] for n in nodes}
    for node_id in to_add:
        meta = manual_nodes[node_id]
        # For function nodes, resolve edges and compute state.
        resolved_input_params = None
        resolved_output_types = None
        manual_fn_state = None
        if meta["type"] == "functionNode":
            fn_label = meta["label"]
            sig_params = _fn_params_from_registry(fn_label)
            resolved = resolve_function_edges(
                fn_node_ids={node_id},
                manual_edges=manual_edges_list,
                manual_nodes=manual_nodes,
                existing_node_labels=existing_node_labels,
                sig_params=sig_params,
            )
            inferred_inputs = {
                p: ts[0] for p, ts in resolved.input_types.items() if ts
            }
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
                    fn_label, resolved_output_types, state_output_types,
                )
            if state_output_types:
                manual_fn_state = _own_state_for_function(
                    db, fn_label, set(state_output_types))
                logger.debug("manual fn %s: computed state=%s (outputs=%s)",
                             fn_label, manual_fn_state, state_output_types)
            else:
                manual_fn_state = "red"
                logger.debug("manual fn %s: no inferred outputs, defaulting to red", fn_label)

        node = gb.build_manual_node(
            node_id, meta, pending_constants,
            manual_fn_state, resolved_input_params, resolved_output_types,
            matlab_functions,
        )
        nodes.append(node)
        logger.debug("[pipeline] built manual node: %s (type=%s, label=%s)",
                     node_id, meta["type"], meta["label"])

    logger.info("[pipeline] Step 14: Graph build complete - assembling final result")
    node_types = {}
    for n in nodes:
        t = n["type"]
        node_types[t] = node_types.get(t, 0) + 1
    logger.info(
        "[pipeline] graph built successfully: %d total nodes (%s), %d edges",
        len(nodes),
        ", ".join(f"{c} {t}" for t, c in sorted(node_types.items())),
        len(edges),
    )

    return {"nodes": nodes, "edges": edges}


@router.get("/pipeline")
def get_pipeline(db: DatabaseManager = Depends(get_db)):
    from scistack_gui.services.pipeline_service import get_pipeline_graph
    return get_pipeline_graph(db)


@router.get("/function/{fn_name}/params")
def get_function_params(fn_name: str):
    from scistack_gui.services.pipeline_service import get_function_full_info
    return get_function_full_info(fn_name)


@router.get("/function/{fn_name}/source")
def get_function_source(fn_name: str):
    from scistack_gui.services.pipeline_service import get_function_source as _get_source
    return _get_source(fn_name)


@router.put("/constants/{name}/pending/{value}")
async def add_pending_constant_value(name: str, value: str):
    from scistack_gui.services.layout_service import put_pending_constant
    put_pending_constant(name, value)
    await ws.broadcast({"type": "dag_updated"})
    return {"ok": True}


@router.delete("/constants/{name}/pending/{value}")
async def remove_pending_constant_value(name: str, value: str):
    from scistack_gui.services.layout_service import delete_pending_constant
    delete_pending_constant(name, value)
    await ws.broadcast({"type": "dag_updated"})
    return {"ok": True}
