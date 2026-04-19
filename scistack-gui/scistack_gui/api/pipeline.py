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

    Uses the source hash and output count from the MATLAB registry so
    the proxy's ``.hash`` matches what was stored at save time.
    """
    from scistack_gui import matlab_registry
    from sci_matlab.bridge import MatlabLineageFcn

    info = matlab_registry.get_matlab_function(fn_name)
    unpack = info.n_outputs >= 2
    proxy = MatlabLineageFcn(info.source_hash, fn_name, unpack_output=unpack)
    # check_node_state reads getattr(fn, "__name__") to match lineage records.
    proxy.__name__ = fn_name
    return proxy


def _own_state_for_function(
    db: DatabaseManager,
    fn_name: str,
    fn_out_types: set[str],
) -> str:
    """
    Return the own run state ("green"/"grey"/"red") for a single function by
    calling scihist.check_node_state when the function and its output classes
    are available in the registry.

    Falls back to "red" for unregistered functions (never executed or not
    importable in this session).
    """
    from scihist.state import check_node_state
    from scidb import BaseVariable

    fn_obj = registry._functions.get(fn_name)
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
        result = check_node_state(fn_obj, output_classes, db=db)
        state = result["state"]
        counts = result.get("counts", {})
        logger.debug(
            "state(%s): %s (up_to_date=%d, stale=%d, missing=%d)",
            fn_name, state,
            counts.get("up_to_date", 0),
            counts.get("stale", 0),
            counts.get("missing", 0),
        )
        return state
    except Exception:
        logger.exception(
            "check_node_state failed for %s — falling back to red", fn_name
        )
        return "red"


def _compute_run_states(
    db: DatabaseManager,
    fn_input_params: dict[str, dict],
    fn_outputs: dict[str, set],
    fn_constants: dict[str, set] | None = None,
    pending_constants: dict[str, set] | None = None,
) -> dict[str, str]:
    """
    Compute run_state for every function and variable node.

    Pass 1 — own state per function:
      Calls scihist.check_node_state for each function that is registered in
      this session.  Uses lineage records + function hash when available,
      falling back to __fn_hash + timestamps for scidb.for_each outputs.
      Unregistered functions default to "red".

    Pass 2 — propagate staleness through the DAG (delegated to domain layer).

    Returns {node_id: "green"|"grey"|"red"} for fn__ and var__ nodes.
    """
    from scistack_gui.domain.run_state import propagate_run_states

    t0 = time.monotonic()

    # --- Pass 1: own state per function ---
    fn_own_state: dict[str, str] = {}
    for fn_name in fn_input_params:
        fn_own_state[fn_name] = _own_state_for_function(
            db, fn_name, fn_outputs.get(fn_name, set())
        )

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
        "run_states complete: %d functions in %.1fms (%d green, %d grey, %d red)",
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
    from scistack_gui import pipeline_store as _ps
    from scistack_gui import matlab_registry as _mr
    from scistack_gui.domain import graph_builder as gb
    from scistack_gui.domain.edge_resolver import resolve_function_edges

    hidden_ids = _ps.get_hidden_node_ids(db)

    # --- Fetch data ---
    variants: list[dict] = db.list_pipeline_variants()
    logger.debug("_build_graph: %d pipeline variants from DB", len(variants))

    listed_var_names: set[str] = set()
    try:
        listed = db.list_variables()
        for _, row in listed.iterrows():
            listed_var_names.add(row["variable_name"])
    except Exception:
        pass

    # --- Aggregate and filter (pure) ---
    agg = gb.aggregate_variants(variants, listed_var_names)
    gb.filter_hidden(agg, hidden_ids)

    record_counts = _get_record_counts(db, agg.all_var_types)

    pending_constants = layout_store.get_pending_constants()
    pending_constants, removals = gb.auto_clean_pending_constants(
        pending_constants, agg.const_counts)
    for const_name, pval in removals:
        layout_store.remove_pending_constant(const_name, pval)

    # --- Compute run states ---
    run_states = _compute_run_states(
        db, agg.fn_input_params, agg.fn_outputs,
        agg.fn_constants, pending_constants,
    )

    # --- Build fn_params_map and saved_configs ---
    fn_params_map: dict[str, list[str]] = {}
    for fn in agg.fn_input_params:
        if _mr.is_matlab_function(fn):
            fn_params_map[fn] = list(_mr.get_matlab_function(fn).params)
        else:
            fn_params_map[fn] = _fn_params_from_registry(fn)

    manual_nodes = _ps.get_manual_nodes(db)
    saved_configs: dict[str, dict | None] = {}
    for fn in agg.fn_input_params:
        node_id = f"fn__{fn}"
        saved_configs[fn] = manual_nodes.get(node_id, {}).get("config")

    matlab_functions = set(_mr.get_all_function_names())
    matlab_output_order = {
        name: _mr.get_matlab_function(name).output_names
        for name in matlab_functions
    }

    # --- Overlay saved path inputs ---
    saved_path_inputs = layout_store.read_all_path_input_names()
    gb.overlay_saved_path_inputs(agg.path_inputs, saved_path_inputs)

    # --- Build nodes (pure) ---
    nodes = gb.build_variable_nodes(agg.all_var_types, record_counts, run_states)
    nodes += gb.build_constant_nodes(agg.const_counts, pending_constants)
    nodes += gb.build_path_input_nodes(agg.path_inputs)
    nodes += gb.build_function_nodes(
        agg.fn_input_params, agg.fn_outputs, agg.fn_constants,
        agg.fn_variants_map, fn_params_map, run_states,
        matlab_functions, saved_configs,
        matlab_output_order=matlab_output_order,
    )

    # --- Build edges (pure) ---
    manual_edges_list = layout_store.read_manual_edges()
    edges = gb.build_edges(
        agg.fn_input_params, agg.fn_outputs, agg.const_fns,
        agg.path_inputs, manual_edges_list, hidden_ids,
    )

    # --- Merge manual nodes ---
    saved_positions = layout_store.read_layout()["positions"]
    to_add, graduations = gb.merge_manual_nodes(nodes, manual_nodes, saved_positions)

    # Execute graduation side effects.
    for action in graduations:
        layout_store.graduate_manual_node(action.old_id, action.new_id)

    # Build and append manual nodes that should be added.
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
            # For manual MATLAB function nodes, always use the declared output
            # names from the function signature as handles. Connected edges carry
            # the actual var-label mapping via sourceHandle, so the handle set
            # must always match the full signature regardless of what's wired up.
            if _mr.is_matlab_function(fn_label):
                info = _mr.get_matlab_function(fn_label)
                resolved_output_types = list(info.output_names)
                logger.debug(
                    "manual fn %s (MATLAB): using declared output_names=%s",
                    fn_label, resolved_output_types,
                )
            if resolved_output_types:
                manual_fn_state = _own_state_for_function(
                    db, fn_label, set(resolved_output_types))
                logger.debug("manual fn %s: computed state=%s (outputs=%s)",
                             fn_label, manual_fn_state, resolved_output_types)
            else:
                manual_fn_state = "red"
                logger.debug("manual fn %s: no inferred outputs, defaulting to red", fn_label)

        node = gb.build_manual_node(
            node_id, meta, pending_constants,
            manual_fn_state, resolved_input_params, resolved_output_types,
            matlab_functions,
        )
        nodes.append(node)

    node_types = {}
    for n in nodes:
        t = n["type"]
        node_types[t] = node_types.get(t, 0) + 1
    logger.debug(
        "graph built: %d nodes (%s), %d edges",
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
