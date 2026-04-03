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
from collections import defaultdict
from fastapi import APIRouter, Depends
from scidb.database import DatabaseManager
from scistack_gui.db import get_db
from scistack_gui import layout as layout_store
from scistack_gui import registry

router = APIRouter()


def _fn_params_from_registry(fn_name: str) -> list[str]:
    """Return non-private parameter names from the registered function's signature."""
    fn = registry._functions.get(fn_name)
    if fn is None:
        return []
    try:
        return [
            name for name in inspect.signature(fn).parameters
            if not name.startswith('_')
        ]
    except (ValueError, TypeError):
        return []


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


def _build_graph(db: DatabaseManager) -> dict:
    """
    Build nodes and edges from list_pipeline_variants() and list_variables().

    list_pipeline_variants() is the primary source — it covers every for_each
    run and does not require scilineage. list_variables() fills in any variable
    types that exist in the DB but have never been run through for_each.
    """
    variants: list[dict] = db.list_pipeline_variants()
    all_var_types: set[str] = set()
    # function_name → dict of param_name → type_name (variable inputs only)
    fn_input_params: dict[str, dict] = defaultdict(dict)
    fn_outputs: dict[str, set] = defaultdict(set)
    # constant_name → {str(value): total_record_count}
    const_counts: dict[str, dict] = defaultdict(lambda: defaultdict(int))
    # constant_name → set of function names that use it
    const_fns: dict[str, set] = defaultdict(set)
    # function_name → set of constant param names
    fn_constants: dict[str, set] = defaultdict(set)

    for v in variants:
        fn = v["function_name"]
        out = v["output_type"]
        inputs = v["input_types"]   # dict: param_name → variable_type_name
        constants = v["constants"]  # dict: param_name → scalar value
        count = v["record_count"]

        all_var_types.add(out)
        all_var_types.update(inputs.values())

        fn_input_params[fn].update(inputs)  # param_name → type_name
        fn_outputs[fn].add(out)

        for k, val in constants.items():
            const_counts[k][str(val)] += count
            const_fns[k].add(fn)
            fn_constants[fn].add(k)

    # Add any variables in the DB that weren't in any for_each run
    try:
        listed = db.list_variables()
        for _, row in listed.iterrows():
            all_var_types.add(row["variable_name"])
    except Exception:
        pass

    # Get raw record counts for each variable type
    record_counts = _get_record_counts(db, all_var_types)

    # --- Build nodes ---
    nodes = []

    for vtype in sorted(all_var_types):
        nodes.append({
            "id": f"var__{vtype}",
            "type": "variableNode",
            "position": {"x": 0, "y": 0},   # overwritten by layout endpoint
            "data": {
                "label": vtype,
                "total_records": record_counts.get(vtype, 0),
            },
        })

    for const_name in sorted(const_counts.keys()):
        values = [
            {"value": val, "record_count": cnt}
            for val, cnt in sorted(const_counts[const_name].items())
        ]
        nodes.append({
            "id": f"const__{const_name}",
            "type": "constantNode",
            "position": {"x": 0, "y": 0},
            "data": {"label": const_name, "values": values},
        })

    # Build per-function variant list for the settings panel.
    fn_variants: dict[str, list] = defaultdict(list)
    for v in variants:
        fn_variants[v["function_name"]].append({
            "constants": v["constants"],
            "input_types": v["input_types"],
            "output_type": v["output_type"],
            "record_count": v["record_count"],
        })

    for fn in sorted(fn_input_params.keys()):
        input_params = dict(sorted(fn_input_params[fn].items()))
        constant_params = sorted(fn_constants[fn])
        # Fill in any params the DB didn't capture (e.g. never run via for_each)
        known = set(input_params) | set(constant_params)
        for name in _fn_params_from_registry(fn):
            if name not in known:
                input_params[name] = ""
        nodes.append({
            "id": f"fn__{fn}",
            "type": "functionNode",
            "position": {"x": 0, "y": 0},
            "data": {
                "label": fn,
                "variants": fn_variants.get(fn, []),
                "input_params": input_params,
                "output_types": sorted(fn_outputs[fn]),
                "constant_params": constant_params,
            },
        })

    # --- Build edges (deduplicated) ---
    edges = []
    seen_edges: set[tuple] = set()

    for fn, params in fn_input_params.items():
        for param_name, in_type in params.items():
            key = (f"var__{in_type}", f"fn__{fn}")
            if key not in seen_edges:
                seen_edges.add(key)
                edges.append({
                    "id": f"e__{in_type}__{fn}",
                    "source": f"var__{in_type}",
                    "target": f"fn__{fn}",
                    "targetHandle": f"in__{param_name}",
                })

    for fn, out_types in fn_outputs.items():
        for out_type in out_types:
            key = (f"fn__{fn}", f"var__{out_type}")
            if key not in seen_edges:
                seen_edges.add(key)
                edges.append({
                    "id": f"e__{fn}__{out_type}",
                    "source": f"fn__{fn}",
                    "target": f"var__{out_type}",
                    "sourceHandle": f"out__{out_type}",
                })

    for const_name, fns in const_fns.items():
        for fn in fns:
            key = (f"const__{const_name}", f"fn__{fn}")
            if key not in seen_edges:
                seen_edges.add(key)
                edges.append({
                    "id": f"e__{const_name}__{fn}",
                    "source": f"const__{const_name}",
                    "target": f"fn__{fn}",
                    "targetHandle": f"const__{const_name}",
                })

    # Merge in manually-created edges (tagged so the frontend can delete them).
    for me in layout_store.read_manual_edges():
        if any(e["id"] == me["id"] for e in edges):
            continue
        edge: dict = {
            "id": me["id"],
            "source": me["source"],
            "target": me["target"],
            "data": {"manual": True},
        }
        if me.get("sourceHandle"):
            edge["sourceHandle"] = me["sourceHandle"]
        if me.get("targetHandle"):
            edge["targetHandle"] = me["targetHandle"]
        edges.append(edge)

    # Merge in manually-placed nodes that aren't already present from DB data.
    existing_ids = {n["id"] for n in nodes}
    # Map (type, label) → canonical DB node ID so we can detect graduated nodes.
    db_node_by_label: dict[tuple, str] = {
        (n["type"], n["data"]["label"]): n["id"] for n in nodes
    }
    # Snapshot positions once so we can tell whether a canonical node was already
    # an established canvas node before this refresh.
    saved_positions = layout_store.read_layout()["positions"]
    for node_id, meta in layout_store.get_manual_nodes().items():
        if node_id in existing_ids:
            continue
        key = (meta["type"], meta["label"])
        if key in db_node_by_label:
            canonical_id = db_node_by_label[key]
            # Only graduate (transfer position → canonical, remove manual entry)
            # when the canonical node has NO saved position yet — meaning it just
            # appeared in the DB for the first time and this manual node was its
            # placeholder.  If the canonical node already has a saved position the
            # user intentionally placed an extra instance; keep it on the canvas.
            if canonical_id not in saved_positions:
                layout_store.graduate_manual_node(node_id, canonical_id)
                continue
            # Intentional extra instance — fall through to add it to the canvas.
        fn_label = meta["label"]
        extra: dict = {}
        if meta["type"] == "variableNode":
            extra = {"total_records": 0}
        elif meta["type"] == "constantNode":
            extra = {"values": []}
        elif meta["type"] == "functionNode":
            sig_params = _fn_params_from_registry(fn_label)
            extra = {
                "input_params": {p: "" for p in sig_params},
                "output_types": [],
                "constant_params": [],
            }
        nodes.append({
            "id": node_id,
            "type": meta["type"],
            "position": {"x": 0, "y": 0},
            "data": {"label": fn_label, **extra},
        })

    return {"nodes": nodes, "edges": edges}


@router.get("/pipeline")
def get_pipeline(db: DatabaseManager = Depends(get_db)):
    return _build_graph(db)
