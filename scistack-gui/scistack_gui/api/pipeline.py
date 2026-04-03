"""
GET /pipeline

Returns the pipeline graph as React Flow nodes and edges.

Node types:
  - "variableNode": a named variable type (RawEMG, FilteredEMG, ...)
  - "functionNode": a pipeline function (bandpass_filter, ...)

Positions are set to (0, 0) here; the layout endpoint overwrites them with
saved positions, and the frontend assigns dagre positions for new nodes.
"""

from collections import defaultdict
from fastapi import APIRouter, Depends
from scidb.database import DatabaseManager
from scistack_gui.db import get_db
from scistack_gui import layout as layout_store

router = APIRouter()


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
    # function_name → set of (input_var_type, ...) tuples (for deduplicating edges)
    fn_inputs: dict[str, set] = defaultdict(set)
    fn_outputs: dict[str, set] = defaultdict(set)
    # variable_type → list of variant dicts {constants, record_count}
    var_variants: dict[str, list] = defaultdict(list)

    for v in variants:
        fn = v["function_name"]
        out = v["output_type"]
        inputs = v["input_types"]   # dict: param_name → variable_type_name
        constants = v["constants"]  # dict: param_name → scalar value
        count = v["record_count"]

        all_var_types.add(out)
        all_var_types.update(inputs.values())

        for in_type in inputs.values():
            fn_inputs[fn].add(in_type)
        fn_outputs[fn].add(out)

        # Record this variant on the output variable node (full constants for now;
        # we'll trim to distinguishing-only keys below)
        var_variants[out].append({"constants": constants, "record_count": count})

    # Add any variables in the DB that weren't in any for_each run
    try:
        listed = db.list_variables()
        for _, row in listed.iterrows():
            all_var_types.add(row["variable_name"])
    except Exception:
        pass

    # Get raw record counts for nodes that have no for_each variants
    record_counts = _get_record_counts(db, all_var_types)

    # Trim variant constants to only the keys that differ across variants of
    # the same variable type. Constants shared by all variants are noise.
    for vtype, variant_list in var_variants.items():
        if len(variant_list) <= 1:
            continue
        all_keys = set().union(*(v["constants"].keys() for v in variant_list))
        # A key is distinguishing if its values differ across any two variants
        distinguishing = {
            k for k in all_keys
            if len({str(v["constants"].get(k)) for v in variant_list}) > 1
        }
        for v in variant_list:
            v["constants"] = {k: val for k, val in v["constants"].items()
                              if k in distinguishing}

    # --- Build nodes ---
    nodes = []

    for vtype in sorted(all_var_types):
        nodes.append({
            "id": f"var__{vtype}",
            "type": "variableNode",
            "position": {"x": 0, "y": 0},   # overwritten by layout endpoint
            "data": {
                "label": vtype,
                "variants": var_variants.get(vtype, []),
                "total_records": record_counts.get(vtype, 0),
            },
        })

    for fn in sorted(fn_inputs.keys()):
        nodes.append({
            "id": f"fn__{fn}",
            "type": "functionNode",
            "position": {"x": 0, "y": 0},
            "data": {"label": fn},
        })

    # --- Build edges (deduplicated) ---
    edges = []
    seen_edges: set[tuple] = set()

    for fn, in_types in fn_inputs.items():
        for in_type in in_types:
            key = (f"var__{in_type}", f"fn__{fn}")
            if key not in seen_edges:
                seen_edges.add(key)
                edges.append({
                    "id": f"e__{in_type}__{fn}",
                    "source": f"var__{in_type}",
                    "target": f"fn__{fn}",
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
                })

    # Merge in manually-placed nodes that aren't already present from DB data.
    existing_ids = {n["id"] for n in nodes}
    for node_id, meta in layout_store.get_manual_nodes().items():
        if node_id not in existing_ids:
            nodes.append({
                "id": node_id,
                "type": meta["type"],
                "position": {"x": 0, "y": 0},
                "data": {"label": meta["label"],
                         **({"variants": [], "total_records": 0}
                            if meta["type"] == "variableNode" else {})},
            })

    return {"nodes": nodes, "edges": edges}


@router.get("/pipeline")
def get_pipeline(db: DatabaseManager = Depends(get_db)):
    return _build_graph(db)
