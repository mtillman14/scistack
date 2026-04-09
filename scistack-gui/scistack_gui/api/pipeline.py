"""
GET /pipeline

Returns the pipeline graph as React Flow nodes and edges.

Node types:
  - "variableNode": a named variable type (RawEMG, FilteredEMG, ...)
  - "functionNode": a pipeline function (bandpass_filter, ...)

Positions are set to (0, 0) here; the layout endpoint overwrites them with
saved positions, and the frontend assigns dagre positions for new nodes.
"""

import json
import inspect
from collections import defaultdict
from fastapi import APIRouter, Depends
from scidb.database import DatabaseManager
from scistack_gui.db import get_db
from scistack_gui import layout as layout_store
from scistack_gui import registry
from scistack_gui.api import ws

router = APIRouter()


def _parse_path_input(value: str) -> dict | None:
    """If *value* (from __inputs) represents a PathInput, return parsed info.

    Handles two formats:
    - New: JSON with ``__type: "PathInput"`` (from PathInput.to_key())
    - Legacy: repr string like ``PathInput('{subject}/...', root_folder=...)``

    Returns ``{"template": ..., "root_folder": ...}`` or ``None``.
    """
    import re

    # New JSON format
    if value.startswith("{"):
        try:
            parsed = json.loads(value)
            if parsed.get("__type") == "PathInput":
                return {
                    "template": parsed["template"],
                    "root_folder": parsed.get("root_folder"),
                }
        except (json.JSONDecodeError, KeyError):
            pass

    # Legacy repr format: PathInput('...', root_folder=PosixPath('...'))
    if value.startswith("PathInput("):
        m = re.match(r"PathInput\('([^']*)'", value)
        if m:
            template = m.group(1)
            root_match = re.search(
                r"root_folder=(?:Posix|Windows|Pure\w*)?Path\('([^']*)'\)", value
            )
            root = root_match.group(1) if root_match else None
            return {"template": template, "root_folder": root}

    return None


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


def _node_id_to_var_label(
    node_id: str,
    existing_ids: set[str],
    nodes: list[dict],
    manual_nodes: dict[str, dict],
) -> str | None:
    """Resolve a node ID to its variable label, or None if not a variable node."""
    # DB-derived nodes use the convention "var__TypeName".
    if node_id.startswith("var__"):
        # Could be a canonical DB node or a manual node with that prefix.
        for n in nodes:
            if n["id"] == node_id:
                return n["data"]["label"]
        # Manual node not yet appended — check raw ID.
        parts = node_id.split("__")
        if len(parts) >= 2:
            return parts[1]
    # Check the manual_nodes dict.
    meta = manual_nodes.get(node_id)
    if meta and meta["type"] == "variableNode":
        return meta["label"]
    return None


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


_STATE_ORDER = {"red": 0, "grey": 1, "green": 2}


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

    from scihist.state import check_node_state
    from scidb import BaseVariable

    fn_obj = registry._functions.get(fn_name)
    if fn_obj is None:
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
        return result["state"]
    except Exception:
        import logging
        logging.getLogger(__name__).exception(
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

    Pass 2 — propagate staleness through the DAG (topological order):
      effective_state(fn) = min(own_state, state of each input variable)
      variable state      = upstream function's effective state

    Returns {node_id: "green"|"grey"|"red"} for fn__ and var__ nodes.
    """
    # --- Pass 1: own state per function ---
    fn_own_state: dict[str, str] = {}
    for fn_name in fn_input_params:
        fn_own_state[fn_name] = _own_state_for_function(
            db, fn_name, fn_outputs.get(fn_name, set())
        )

    # Downgrade "green" → "grey" for functions that have unrun pending constant values.
    if fn_constants and pending_constants:
        for fn_name in fn_own_state:
            if fn_own_state[fn_name] == "green":
                for const_name in fn_constants.get(fn_name, set()):
                    if pending_constants.get(const_name):
                        fn_own_state[fn_name] = "grey"
                        break

    # --- Pass 2: DAG propagation ---
    var_producer: dict[str, str] = {}
    for fn_name, out_types in fn_outputs.items():
        for ot in out_types:
            var_producer[ot] = fn_name

    fn_effective_state: dict[str, str] = {}
    var_state: dict[str, str] = {}

    fn_input_types: dict[str, set] = {
        fn: set(params.values()) for fn, params in fn_input_params.items()
    }

    remaining = set(fn_own_state.keys())
    for _ in range(len(remaining) + 1):
        if not remaining:
            break
        progress = False
        for fn_name in list(remaining):
            input_var_states: list[str] = []
            all_resolved = True
            for vtype in fn_input_types.get(fn_name, set()):
                if vtype in var_state:
                    input_var_states.append(var_state[vtype])
                elif vtype not in var_producer:
                    # Root variable — no upstream producer, treat as green.
                    input_var_states.append("green")
                else:
                    all_resolved = False
                    break
            if not all_resolved:
                continue

            all_states = [fn_own_state[fn_name]] + input_var_states
            fn_effective_state[fn_name] = min(all_states, key=lambda s: _STATE_ORDER[s])
            for vtype in fn_outputs.get(fn_name, set()):
                var_state[vtype] = fn_effective_state[fn_name]
            remaining.remove(fn_name)
            progress = True

        if not progress:
            # Cycle or unresolvable — mark remaining as red.
            for fn_name in remaining:
                fn_effective_state[fn_name] = "red"
                for vtype in fn_outputs.get(fn_name, set()):
                    var_state[vtype] = "red"
            break

    result: dict[str, str] = {}
    for fn_name, state in fn_effective_state.items():
        result[f"fn__{fn_name}"] = state
    for vtype, state in var_state.items():
        result[f"var__{vtype}"] = state
    return result


def _build_graph(db: DatabaseManager) -> dict:
    """
    Build nodes and edges from list_pipeline_variants() and list_variables().

    list_pipeline_variants() is the primary source — it covers every for_each
    run and does not require scilineage. list_variables() fills in any variable
    types that exist in the DB but have never been run through for_each.
    """
    from scistack_gui import pipeline_store as _ps
    hidden_ids = _ps.get_hidden_node_ids(db)

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
    # param_name → {"template": ..., "root_folder": ..., "functions": set}
    path_inputs: dict[str, dict] = {}

    for v in variants:
        fn = v["function_name"]
        out = v["output_type"]
        inputs = v["input_types"]   # dict: param_name → variable_type_name
        constants = v["constants"]  # dict: param_name → scalar value
        count = v["record_count"]

        all_var_types.add(out)

        # Separate PathInput entries from normal variable inputs.
        for param_name, type_val in inputs.items():
            pi = _parse_path_input(type_val)
            if pi is not None:
                existing = path_inputs.get(param_name)
                if existing is None:
                    path_inputs[param_name] = {**pi, "functions": {fn}}
                else:
                    existing["functions"].add(fn)
            else:
                all_var_types.add(type_val)
                fn_input_params[fn][param_name] = type_val

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

    # Pending constant values — user-declared variants not yet in the DB.
    pending_constants = layout_store.get_pending_constants()

    # Auto-clean pending values that are now in const_counts (they've been run).
    # This must happen BEFORE computing run states so resolved values don't
    # incorrectly keep the function grey/red.
    for const_name in list(pending_constants.keys()):
        still_pending: set[str] = set()
        for pval in pending_constants[const_name]:
            if pval in const_counts.get(const_name, {}):
                layout_store.remove_pending_constant(const_name, pval)
            else:
                still_pending.add(pval)
        pending_constants[const_name] = still_pending

    # --- Filter out user-hidden nodes ---
    # Remove variable types, functions, constants, and path inputs that the
    # user explicitly deleted from the canvas.  Done before computing run
    # states so hidden nodes don't affect state propagation.
    hidden_var_types = {nid.replace("var__", "", 1) for nid in hidden_ids
                        if nid.startswith("var__")}
    hidden_fn_names = {nid.replace("fn__", "", 1) for nid in hidden_ids
                       if nid.startswith("fn__")}
    hidden_const_names = {nid.replace("const__", "", 1) for nid in hidden_ids
                          if nid.startswith("const__")}
    hidden_path_names = {nid.replace("pathInput__", "", 1) for nid in hidden_ids
                         if nid.startswith("pathInput__")}
    all_var_types -= hidden_var_types
    # Also remove hidden var types from fn_outputs so edges aren't created.
    for fn_name in list(fn_outputs.keys()):
        fn_outputs[fn_name] -= hidden_var_types
    # Remove hidden var types from fn_input_params values.
    for fn_name in list(fn_input_params.keys()):
        fn_input_params[fn_name] = {
            p: t for p, t in fn_input_params[fn_name].items()
            if t not in hidden_var_types
        }
    for fn_name in hidden_fn_names:
        fn_input_params.pop(fn_name, None)
        fn_outputs.pop(fn_name, None)
        fn_constants.pop(fn_name, None)
    for cname in hidden_const_names:
        const_counts.pop(cname, None)
        const_fns.pop(cname, None)
    for pname in hidden_path_names:
        path_inputs.pop(pname, None)

    # Compute run states for all function and variable nodes
    run_states = _compute_run_states(db, fn_input_params, fn_outputs, fn_constants, pending_constants)

    # --- Build nodes ---
    nodes = []

    for vtype in sorted(all_var_types):
        data: dict = {
            "label": vtype,
            "total_records": record_counts.get(vtype, 0),
        }
        # Root variable nodes (no upstream function) are always up to date.
        state = run_states.get(f"var__{vtype}", "green")
        data["run_state"] = state
        nodes.append({
            "id": f"var__{vtype}",
            "type": "variableNode",
            "position": {"x": 0, "y": 0},   # overwritten by layout endpoint
            "data": data,
        })

    for const_name in sorted(const_counts.keys()):
        values = [
            {"value": val, "record_count": cnt}
            for val, cnt in sorted(const_counts[const_name].items())
        ]
        # Add any still-pending values (not yet run) to the display list.
        existing_values = {v["value"] for v in values}
        for pval in sorted(pending_constants.get(const_name, set())):
            if pval not in existing_values:
                values.append({"value": pval, "record_count": 0})
        nodes.append({
            "id": f"const__{const_name}",
            "type": "constantNode",
            "position": {"x": 0, "y": 0},
            "data": {"label": const_name, "values": values},
        })

    # --- PathInput nodes ---
    for param_name in sorted(path_inputs.keys()):
        pi = path_inputs[param_name]
        nodes.append({
            "id": f"pathInput__{param_name}",
            "type": "pathInputNode",
            "position": {"x": 0, "y": 0},
            "data": {
                "label": param_name,
                "template": pi["template"],
                "root_folder": pi.get("root_folder"),
            },
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

    # Load saved node configs (schema filter, run options) from manual nodes.
    manual_nodes = _ps.get_manual_nodes(db)

    for fn in sorted(fn_input_params.keys()):
        input_params = dict(sorted(fn_input_params[fn].items()))
        constant_params = sorted(fn_constants[fn])
        # Fill in any params the DB didn't capture (e.g. never run via for_each)
        known = set(input_params) | set(constant_params)
        for name in _fn_params_from_registry(fn):
            if name not in known:
                input_params[name] = ""
        fn_data: dict = {
            "label": fn,
            "variants": fn_variants.get(fn, []),
            "input_params": input_params,
            "output_types": sorted(fn_outputs[fn]),
            "constant_params": constant_params,
        }
        state = run_states.get(f"fn__{fn}")
        if state:
            fn_data["run_state"] = state
        # Apply saved config (schemaFilter, runOptions) if present.
        node_id = f"fn__{fn}"
        saved = manual_nodes.get(node_id, {}).get("config")
        if saved:
            if "schemaFilter" in saved:
                fn_data["schemaFilter"] = saved["schemaFilter"]
            if "schemaLevel" in saved:
                fn_data["schemaLevel"] = saved["schemaLevel"]
            if "runOptions" in saved:
                fn_data["runOptions"] = saved["runOptions"]
        nodes.append({
            "id": node_id,
            "type": "functionNode",
            "position": {"x": 0, "y": 0},
            "data": fn_data,
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

    for param_name, pi in path_inputs.items():
        for fn in pi["functions"]:
            key = (f"pathInput__{param_name}", f"fn__{fn}")
            if key not in seen_edges:
                seen_edges.add(key)
                edges.append({
                    "id": f"e__{param_name}__{fn}",
                    "source": f"pathInput__{param_name}",
                    "target": f"fn__{fn}",
                    "targetHandle": f"in__{param_name}",
                })

    # Merge in manually-created edges (tagged so the frontend can delete them).
    # Skip edges that reference hidden nodes.
    for me in layout_store.read_manual_edges():
        if me["source"] in hidden_ids or me["target"] in hidden_ids:
            continue
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
            extra = {"total_records": 0, "run_state": "red"}
        elif meta["type"] == "constantNode":
            # Include any pending (user-declared, not yet in DB) values.
            pending_vals = [
                {"value": pval, "record_count": 0}
                for pval in sorted(pending_constants.get(fn_label, set()))
            ]
            extra = {"values": pending_vals}
        elif meta["type"] == "pathInputNode":
            extra = {"template": "", "root_folder": None}
        elif meta["type"] == "functionNode":
            sig_params = _fn_params_from_registry(fn_label)
            # Infer output_types and input_params from manual edges so that
            # newly-wired functions show their connections before the first run.
            inferred_outputs: list[str] = []
            inferred_inputs: dict[str, str] = {}
            unmatched_inputs: list[str] = []
            manual_nodes_snapshot = layout_store.get_manual_nodes()
            for me in layout_store.read_manual_edges():
                if me["source"] == node_id:
                    # Edge from this function → a variable node (output).
                    tgt = me["target"]
                    var_label = _node_id_to_var_label(tgt, existing_ids, nodes,
                                                      manual_nodes_snapshot)
                    if var_label and var_label not in inferred_outputs:
                        inferred_outputs.append(var_label)
                elif me["target"] == node_id:
                    # Edge from a variable node → this function (input).
                    src = me["source"]
                    var_label = _node_id_to_var_label(src, existing_ids, nodes,
                                                      manual_nodes_snapshot)
                    if var_label:
                        th = me.get("targetHandle") or ""
                        if th.startswith("in__"):
                            inferred_inputs[th.replace("in__", "")] = var_label
                        else:
                            unmatched_inputs.append(var_label)
            # Match unmatched inputs to signature params by position.
            remaining_params = [p for p in sig_params if p not in inferred_inputs]
            for param, var_type in zip(remaining_params, unmatched_inputs):
                inferred_inputs[param] = var_type
            input_params = {p: inferred_inputs.get(p, "") for p in sig_params}
            for p, t in inferred_inputs.items():
                if p not in input_params:
                    input_params[p] = t
            extra = {
                "input_params": input_params,
                "output_types": sorted(inferred_outputs),
                "constant_params": [],
                "run_state": "red",
            }
        node_data: dict = {"label": fn_label, **extra}
        # Apply saved config for manually-placed function nodes.
        saved_cfg = meta.get("config")
        if saved_cfg and meta["type"] == "functionNode":
            if "schemaFilter" in saved_cfg:
                node_data["schemaFilter"] = saved_cfg["schemaFilter"]
            if "schemaLevel" in saved_cfg:
                node_data["schemaLevel"] = saved_cfg["schemaLevel"]
            if "runOptions" in saved_cfg:
                node_data["runOptions"] = saved_cfg["runOptions"]
        nodes.append({
            "id": node_id,
            "type": meta["type"],
            "position": {"x": 0, "y": 0},
            "data": node_data,
        })

    return {"nodes": nodes, "edges": edges}


@router.get("/pipeline")
def get_pipeline(db: DatabaseManager = Depends(get_db)):
    return _build_graph(db)


@router.get("/function/{fn_name}/params")
def get_function_params(fn_name: str):
    """Return parameter names from the registered function's signature."""
    params = _fn_params_from_registry(fn_name)
    return {"params": params}


@router.get("/function/{fn_name}/source")
def get_function_source(fn_name: str):
    """Return the source file path and first line number for a registered function."""
    fn = registry._functions.get(fn_name)
    if fn is None:
        return {"ok": False, "error": f"Function '{fn_name}' is not registered."}
    try:
        file = inspect.getsourcefile(fn) or inspect.getfile(fn)
        _, line = inspect.getsourcelines(fn)
    except (TypeError, OSError) as e:
        return {"ok": False, "error": f"Could not locate source for '{fn_name}': {e}"}
    return {"ok": True, "file": file, "line": line}


@router.put("/constants/{name}/pending/{value}")
async def add_pending_constant_value(name: str, value: str):
    layout_store.add_pending_constant(name, value)
    await ws.broadcast({"type": "dag_updated"})
    return {"ok": True}


@router.delete("/constants/{name}/pending/{value}")
async def remove_pending_constant_value(name: str, value: str):
    layout_store.remove_pending_constant(name, value)
    await ws.broadcast({"type": "dag_updated"})
    return {"ok": True}
