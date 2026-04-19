"""
Pure graph-building logic for the pipeline DAG.

Builds React Flow nodes and edges from pre-fetched data. No I/O — works
entirely on plain Python data structures (dicts, lists, sets, strings).
"""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class AggregatedData:
    """Aggregated pipeline data from DB variants."""
    all_var_types: set[str] = field(default_factory=set)
    fn_input_params: dict[str, dict] = field(default_factory=lambda: defaultdict(dict))
    fn_outputs: dict[str, set] = field(default_factory=lambda: defaultdict(set))
    const_counts: dict[str, dict] = field(default_factory=lambda: defaultdict(lambda: defaultdict(int)))
    const_fns: dict[str, set] = field(default_factory=lambda: defaultdict(set))
    fn_constants: dict[str, set] = field(default_factory=lambda: defaultdict(set))
    path_inputs: dict[str, dict] = field(default_factory=dict)
    fn_variants_map: dict[str, list] = field(default_factory=lambda: defaultdict(list))


@dataclass
class GraduationAction:
    """Side-effect to execute after merge_manual_nodes (pure return value)."""
    old_id: str
    new_id: str


def parse_path_input(value: str) -> dict | None:
    """If *value* (from __inputs) represents a PathInput, return parsed info.

    Handles two formats:
    - New: JSON with ``__type: "PathInput"`` (from PathInput.to_key())
    - Legacy: repr string like ``PathInput('{subject}/...', root_folder=...)``

    Returns ``{"template": ..., "root_folder": ...}`` or ``None``.
    """
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


def aggregate_variants(
    variants: list[dict],
    listed_var_names: set[str],
) -> AggregatedData:
    """Parse DB variants into aggregated data structures.

    Args:
        variants: From db.list_pipeline_variants().
        listed_var_names: Variable names from db.list_variables() to fill in
            types that exist but haven't been run through for_each.

    Returns:
        AggregatedData with all parsed fields.
    """
    agg = AggregatedData()

    for v in variants:
        fn = v["function_name"]
        out = v["output_type"]
        inputs = v["input_types"]
        constants = v["constants"]
        count = v["record_count"]

        agg.all_var_types.add(out)

        for param_name, type_val in inputs.items():
            pi = parse_path_input(type_val)
            if pi is not None:
                existing = agg.path_inputs.get(param_name)
                if existing is None:
                    agg.path_inputs[param_name] = {**pi, "functions": {fn}}
                else:
                    existing["functions"].add(fn)
            else:
                agg.all_var_types.add(type_val)
                agg.fn_input_params[fn][param_name] = type_val

        agg.fn_outputs[fn].add(out)

        for k, val in constants.items():
            agg.const_counts[k][str(val)] += count
            agg.const_fns[k].add(fn)
            agg.fn_constants[fn].add(k)

        # Per-function variant list for the settings panel.
        agg.fn_variants_map[fn].append({
            "constants": constants,
            "input_types": inputs,
            "output_type": out,
            "record_count": count,
        })

    # Add variable types from the DB that weren't in any for_each run.
    agg.all_var_types |= listed_var_names

    logger.debug(
        "aggregate_variants: %d variants → %d var types, %d functions, %d constants, %d path inputs",
        len(variants), len(agg.all_var_types), len(agg.fn_outputs),
        len(agg.const_counts), len(agg.path_inputs),
    )
    return agg


def filter_hidden(agg: AggregatedData, hidden_ids: set[str]) -> AggregatedData:
    """Remove hidden nodes from the aggregated data (mutates in place).

    Args:
        agg: Aggregated data to filter.
        hidden_ids: Set of node IDs the user has explicitly deleted.

    Returns:
        The same AggregatedData, mutated.
    """
    hidden_var_types = {nid.replace("var__", "", 1) for nid in hidden_ids
                        if nid.startswith("var__")}
    hidden_fn_names = {nid.replace("fn__", "", 1) for nid in hidden_ids
                       if nid.startswith("fn__")}
    hidden_const_names = {nid.replace("const__", "", 1) for nid in hidden_ids
                          if nid.startswith("const__")}
    hidden_path_names = {nid.replace("pathInput__", "", 1) for nid in hidden_ids
                         if nid.startswith("pathInput__")}

    agg.all_var_types -= hidden_var_types

    for fn_name in list(agg.fn_outputs.keys()):
        agg.fn_outputs[fn_name] -= hidden_var_types

    for fn_name in list(agg.fn_input_params.keys()):
        agg.fn_input_params[fn_name] = {
            p: t for p, t in agg.fn_input_params[fn_name].items()
            if t not in hidden_var_types
        }

    for fn_name in hidden_fn_names:
        agg.fn_input_params.pop(fn_name, None)
        agg.fn_outputs.pop(fn_name, None)
        agg.fn_constants.pop(fn_name, None)

    for cname in hidden_const_names:
        agg.const_counts.pop(cname, None)
        agg.const_fns.pop(cname, None)

    for pname in hidden_path_names:
        agg.path_inputs.pop(pname, None)

    if hidden_ids:
        logger.debug(
            "filter_hidden: removed var=%s fn=%s const=%s pathInput=%s",
            hidden_var_types, hidden_fn_names, hidden_const_names, hidden_path_names,
        )
    return agg


def auto_clean_pending_constants(
    pending_constants: dict[str, set[str]],
    const_counts: dict[str, dict],
) -> tuple[dict[str, set[str]], list[tuple[str, str]]]:
    """Remove pending values that are now in const_counts (they've been run).

    Returns:
        Tuple of (cleaned pending_constants, list of (name, value) to remove from DB).
    """
    removals: list[tuple[str, str]] = []
    for const_name in list(pending_constants.keys()):
        still_pending: set[str] = set()
        for pval in pending_constants[const_name]:
            if pval in const_counts.get(const_name, {}):
                removals.append((const_name, pval))
            else:
                still_pending.add(pval)
        pending_constants[const_name] = still_pending
    if removals:
        logger.debug("auto_clean_pending_constants: removing %s", removals)
    return pending_constants, removals


def build_variable_nodes(
    all_var_types: set[str],
    record_counts: dict[str, int],
    run_states: dict[str, str],
) -> list[dict]:
    """Build React Flow variable nodes."""
    nodes = []
    for vtype in sorted(all_var_types):
        data: dict = {
            "label": vtype,
            "total_records": record_counts.get(vtype, 0),
        }
        state = run_states.get(f"var__{vtype}", "green")
        data["run_state"] = state
        nodes.append({
            "id": f"var__{vtype}",
            "type": "variableNode",
            "position": {"x": 0, "y": 0},
            "data": data,
        })
    return nodes


def build_constant_nodes(
    const_counts: dict[str, dict],
    pending_constants: dict[str, set[str]],
) -> list[dict]:
    """Build React Flow constant nodes."""
    nodes = []
    for const_name in sorted(const_counts.keys()):
        values = [
            {"value": val, "record_count": cnt}
            for val, cnt in sorted(const_counts[const_name].items())
        ]
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
    return nodes


def overlay_saved_path_inputs(
    path_inputs: dict[str, dict],
    saved_path_inputs: list[dict],
) -> dict[str, dict]:
    """Overlay saved template/root_folder from layout.json onto path_inputs.

    Mutates path_inputs in place and returns it.
    """
    for saved_pi in saved_path_inputs:
        pname = saved_pi["name"]
        if pname in path_inputs:
            if saved_pi.get("template"):
                path_inputs[pname]["template"] = saved_pi["template"]
            if saved_pi.get("root_folder") is not None:
                path_inputs[pname]["root_folder"] = saved_pi["root_folder"]
        else:
            path_inputs[pname] = {
                "template": saved_pi.get("template", ""),
                "root_folder": saved_pi.get("root_folder"),
                "functions": set(),
            }
    return path_inputs


def build_path_input_nodes(path_inputs: dict[str, dict]) -> list[dict]:
    """Build React Flow path input nodes."""
    nodes = []
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
    return nodes


def build_function_nodes(
    fn_input_params: dict[str, dict],
    fn_outputs: dict[str, set],
    fn_constants: dict[str, set],
    fn_variants_map: dict[str, list],
    fn_params_map: dict[str, list[str]],
    run_states: dict[str, str],
    matlab_functions: set[str],
    saved_configs: dict[str, dict | None],
    matlab_output_order: dict[str, list[str]] | None = None,
) -> list[dict]:
    """Build React Flow function nodes.

    Args:
        fn_input_params: {fn_name: {param: var_type}}.
        fn_outputs: {fn_name: {output_types}}.
        fn_constants: {fn_name: {constant_param_names}}.
        fn_variants_map: {fn_name: [variant_dicts]} for settings panel.
        fn_params_map: {fn_name: [all_sig_params]} from registry.
        run_states: {node_id: state}.
        matlab_functions: Set of MATLAB function names.
        saved_configs: {fn_name: config_dict or None} from manual nodes.
        matlab_output_order: {fn_name: [output_names in signature order]}.
    """
    nodes = []
    for fn in sorted(fn_input_params.keys()):
        input_params = dict(sorted(fn_input_params[fn].items()))
        constant_params = sorted(fn_constants.get(fn, set()))

        # Fill in any params the DB didn't capture.
        known = set(input_params) | set(constant_params)
        for name in fn_params_map.get(fn, []):
            if name not in known:
                input_params[name] = ""

        # Order output types: MATLAB functions use declared signature order;
        # any DB outputs not in the signature are appended sorted.
        actual_outputs = fn_outputs.get(fn, set())
        if fn in matlab_functions and matlab_output_order:
            declared = matlab_output_order.get(fn, [])
            ordered = [t for t in declared if t in actual_outputs]
            extras = sorted(t for t in actual_outputs if t not in declared)
            out_types = ordered + extras
        else:
            out_types = sorted(actual_outputs)

        fn_data: dict = {
            "label": fn,
            "variants": fn_variants_map.get(fn, []),
            "input_params": input_params,
            "output_types": out_types,
            "constant_params": constant_params,
        }
        state = run_states.get(f"fn__{fn}")
        if state:
            fn_data["run_state"] = state
        if fn in matlab_functions:
            fn_data["language"] = "matlab"

        # Apply saved config (schemaFilter, runOptions) if present.
        saved = saved_configs.get(fn)
        if saved:
            if "schemaFilter" in saved:
                fn_data["schemaFilter"] = saved["schemaFilter"]
            if "schemaLevel" in saved:
                fn_data["schemaLevel"] = saved["schemaLevel"]
            if "runOptions" in saved:
                fn_data["runOptions"] = saved["runOptions"]

        nodes.append({
            "id": f"fn__{fn}",
            "type": "functionNode",
            "position": {"x": 0, "y": 0},
            "data": fn_data,
        })
    return nodes


def build_edges(
    fn_input_params: dict[str, dict],
    fn_outputs: dict[str, set],
    const_fns: dict[str, set],
    path_inputs: dict[str, dict],
    manual_edges: list[dict],
    hidden_ids: set[str],
) -> list[dict]:
    """Build React Flow edges (DB-derived + manual).

    Args:
        fn_input_params: {fn_name: {param: var_type}}.
        fn_outputs: {fn_name: {output_types}}.
        const_fns: {const_name: {fn_names}}.
        path_inputs: {param_name: {"functions": set, ...}}.
        manual_edges: List of manual edge dicts from pipeline_store.
        hidden_ids: Set of hidden node IDs.
    """
    edges = []
    seen_edges: set[tuple] = set()

    # Variable → function edges.
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

    # Function → variable edges.
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

    # Constant → function edges.
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

    # PathInput → function edges.
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

    # Merge manually-created edges.
    for me in manual_edges:
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

    return edges


def _apply_saved_config(node_data: dict, config: dict | None) -> None:
    """Apply saved config (schemaFilter, runOptions) to a function node."""
    if not config:
        return
    if "schemaFilter" in config:
        node_data["schemaFilter"] = config["schemaFilter"]
    if "schemaLevel" in config:
        node_data["schemaLevel"] = config["schemaLevel"]
    if "runOptions" in config:
        node_data["runOptions"] = config["runOptions"]


def build_manual_node(
    node_id: str,
    meta: dict,
    pending_constants: dict[str, set[str]],
    manual_fn_state: str | None,
    resolved_input_params: dict[str, str] | None,
    resolved_output_types: list[str] | None,
    matlab_functions: set[str],
) -> dict:
    """Build a single manual node dict.

    Args:
        node_id: The manual node ID.
        meta: {"type": ..., "label": ..., "config": ...} from pipeline_store.
        pending_constants: {const_name: {pending_values}}.
        manual_fn_state: Pre-computed run state for function nodes (or None).
        resolved_input_params: Pre-resolved {param: var_type} for function nodes.
        resolved_output_types: Pre-resolved output types for function nodes.
        matlab_functions: Set of MATLAB function names.
    """
    fn_label = meta["label"]
    extra: dict = {}

    if meta["type"] == "variableNode":
        extra = {"total_records": 0, "run_state": "red"}
    elif meta["type"] == "constantNode":
        pending_vals = [
            {"value": pval, "record_count": 0}
            for pval in sorted(pending_constants.get(fn_label, set()))
        ]
        extra = {"values": pending_vals}
    elif meta["type"] == "pathInputNode":
        extra = {"template": "", "root_folder": None}
    elif meta["type"] == "functionNode":
        extra = {
            "input_params": resolved_input_params or {},
            "output_types": list(resolved_output_types or []),
            "constant_params": [],
            "run_state": manual_fn_state or "red",
        }
        if fn_label in matlab_functions:
            extra["language"] = "matlab"

    node_data: dict = {"label": fn_label, **extra}
    _apply_saved_config(node_data, meta.get("config") if meta["type"] == "functionNode" else None)

    return {
        "id": node_id,
        "type": meta["type"],
        "position": {"x": 0, "y": 0},
        "data": node_data,
    }


def merge_manual_nodes(
    existing_nodes: list[dict],
    manual_nodes: dict[str, dict],
    saved_positions: dict[str, dict],
) -> tuple[list[str], list[GraduationAction]]:
    """Determine which manual nodes to add and which to graduate.

    Returns:
        Tuple of:
        - List of manual node IDs that should be added to the graph.
        - List of GraduationAction objects (side-effects for the service layer).
    """
    existing_ids = {n["id"] for n in existing_nodes}
    db_node_by_label: dict[tuple, str] = {
        (n["type"], n["data"]["label"]): n["id"] for n in existing_nodes
    }

    to_add: list[str] = []
    graduations: list[GraduationAction] = []

    for node_id, meta in manual_nodes.items():
        if node_id in existing_ids:
            continue
        key = (meta["type"], meta["label"])
        if key in db_node_by_label:
            canonical_id = db_node_by_label[key]
            if canonical_id not in saved_positions:
                graduations.append(GraduationAction(old_id=node_id, new_id=canonical_id))
                continue
        to_add.append(node_id)

    if graduations:
        logger.debug(
            "merge_manual_nodes: graduating %d node(s): %s",
            len(graduations),
            [(g.old_id, g.new_id) for g in graduations],
        )
    return to_add, graduations
