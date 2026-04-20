"""
Pure edge resolution logic for pipeline graphs.

Resolves input types, output types, and constant names from manual edges
and node metadata. No I/O — works entirely on plain Python data structures.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ResolvedEdges:
    """Result of scanning edges for a function node."""
    input_types: dict[str, list[str]]   # param_name → [variable type names]
    output_types: list[str]             # ordered list of output variable labels
    constant_names: set[str]            # constant param names wired to this fn


def node_id_to_var_label(
    node_id: str,
    existing_node_labels: dict[str, str],
    manual_nodes: dict[str, dict],
) -> str | None:
    """Resolve a node ID to its variable label, or None if not a variable node.

    Args:
        node_id: The node ID to resolve (e.g. "var__RawEMG" or a manual node UUID).
        existing_node_labels: {node_id: label} for all DB-derived nodes already built.
        manual_nodes: {node_id: {"type": ..., "label": ...}} from pipeline_store.
    """
    # DB-derived nodes use the convention "var__TypeName".
    if node_id.startswith("var__"):
        # Check existing DB nodes first.
        if node_id in existing_node_labels:
            return existing_node_labels[node_id]
        # Fall back to extracting from the ID itself.
        parts = node_id.split("__")
        if len(parts) >= 2:
            return parts[1]
    # Check the manual_nodes dict.
    meta = manual_nodes.get(node_id)
    if meta and meta.get("type") == "variableNode":
        return meta.get("label")
    return None


def resolve_function_edges(
    fn_node_ids: set[str],
    manual_edges: list[dict],
    manual_nodes: dict[str, dict],
    existing_node_labels: dict[str, str],
    sig_params: list[str],
) -> ResolvedEdges:
    """Resolve input/output/constant connections for a function from manual edges.

    This is the single source of truth for edge inference, replacing the
    duplicated logic that was in api/pipeline.py, api/run.py, and server.py.

    Args:
        fn_node_ids: Set of node IDs that represent this function
            (e.g. {"fn__my_func", "fn__my_func__abc123"}).
        manual_edges: List of edge dicts with id, source, target,
            sourceHandle, targetHandle.
        manual_nodes: {node_id: {"type": ..., "label": ...}} from pipeline_store.
        existing_node_labels: {node_id: label} for all DB-derived variable nodes.
        sig_params: Parameter names from the function signature, in order.

    Returns:
        ResolvedEdges with input_types, output_types, and constant_names.
    """
    input_types: dict[str, list[str]] = {}
    output_types: list[str] = []
    constant_names: set[str] = set()
    unmatched_inputs: list[str] = []

    for edge in manual_edges:
        source = edge.get("source", "")
        target = edge.get("target", "")

        if source in fn_node_ids:
            # Edge from this function → a variable node (output).
            var_label = node_id_to_var_label(
                target, existing_node_labels, manual_nodes)
            if var_label and var_label not in output_types:
                output_types.append(var_label)

        elif target in fn_node_ids:
            # Edge into this function (input or constant).
            th = edge.get("targetHandle") or ""

            # Check if source is a constant node.
            is_const = False
            const_label = None
            if source.startswith("const__"):
                is_const = True
                src_meta = manual_nodes.get(source)
                if src_meta:
                    const_label = src_meta["label"]
                else:
                    # DB-derived constant: ID is "const__<name>".
                    const_label = source.replace("const__", "", 1)
            else:
                src_meta = manual_nodes.get(source)
                if src_meta and src_meta.get("type") == "constantNode":
                    is_const = True
                    const_label = src_meta["label"]

            if is_const and const_label is not None:
                # Determine param name from targetHandle or fall back to label.
                if th.startswith("const__"):
                    constant_names.add(th.replace("const__", "", 1))
                elif th.startswith("in__"):
                    constant_names.add(th.replace("in__", "", 1))
                else:
                    constant_names.add(const_label)
                continue

            # Not a constant — check if it's a variable input.
            var_label = node_id_to_var_label(
                source, existing_node_labels, manual_nodes)
            if var_label:
                if th.startswith("in__"):
                    param = th.replace("in__", "")
                    input_types.setdefault(param, [])
                    if var_label not in input_types[param]:
                        input_types[param].append(var_label)
                else:
                    unmatched_inputs.append(var_label)

    # Match unmatched inputs to signature params by position.
    if unmatched_inputs:
        remaining_params = [p for p in sig_params if p not in input_types]
        for param, var_type in zip(remaining_params, unmatched_inputs):
            input_types.setdefault(param, [])
            if var_type not in input_types[param]:
                input_types[param].append(var_type)

    logger.debug(
        "resolve_function_edges: inputs=%s outputs=%s constants=%s unmatched=%d",
        list(input_types), output_types, constant_names, len(unmatched_inputs),
    )
    return ResolvedEdges(
        input_types=input_types,
        output_types=output_types,
        constant_names=constant_names,
    )


def infer_manual_fn_output_types(
    fn_node_ids: set[str],
    manual_edges: list[dict],
    manual_nodes: dict[str, dict],
    existing_node_labels: dict[str, str],
) -> list[str]:
    """Infer output types from manual edges for a function (no positional matching needed).

    Used by the run path when DB variants exist but the user has rewired outputs.
    """
    output_types: list[str] = []
    for edge in manual_edges:
        if edge.get("source", "") in fn_node_ids:
            var_label = node_id_to_var_label(
                edge.get("target", ""), existing_node_labels, manual_nodes)
            if var_label and var_label not in output_types:
                output_types.append(var_label)
    return output_types


def infer_manual_fn_param_to_class(
    fn_node_ids: set[str],
    manual_edges: list[dict],
    manual_nodes: dict[str, dict],
    existing_node_labels: dict[str, str],
) -> dict[str, str]:
    """Extract {param_name: class_name} for a fn from its outgoing manual edges.

    Each manual edge from a function node carries the MATLAB signature param
    name in ``sourceHandle`` (``out__{param_name}``) and the downstream
    Variable class via the edge target. This gives an explicit mapping that
    does not rely on naming conventions — e.g. ``output1 → Result`` works.

    Edges without an ``out__`` prefix or without a resolvable target class
    are skipped. On duplicate param names the first wins.
    """
    mapping: dict[str, str] = {}
    for edge in manual_edges:
        if edge.get("source", "") not in fn_node_ids:
            continue
        sh = edge.get("sourceHandle") or ""
        if not sh.startswith("out__"):
            continue
        param = sh[len("out__"):]
        if not param or param in mapping:
            continue
        class_name = node_id_to_var_label(
            edge.get("target", ""), existing_node_labels, manual_nodes)
        if class_name:
            mapping[param] = class_name
    return mapping
