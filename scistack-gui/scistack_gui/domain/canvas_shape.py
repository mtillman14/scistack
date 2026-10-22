"""
What a canvas shows, independent of node ids (plan
``.claude/plan-canvas-as-document.md`` Stage 0).

Node ids change across the canvas-as-document stages (a node drawn from
history today becomes a stored row with a fresh id), so the regression oracle
for those stages cannot compare ids. ``canvas_shape`` describes a built graph
by what a person sees instead: nodes by type and label, edges by their two
endpoints' type and label plus the input port, each Parameter's values,
saved positions and run state. Two builds that draw the same pipeline have
equal shapes; :func:`diff_shapes` says how two shapes differ, one line each.

:func:`canvas_sources` answers the diagnostic question behind the whole plan:
how much of this canvas is stored rows, and how much is derived from run
history on every build.

Pure: no database, no registry. The caller passes the built graph, the
manual-node rows and the scope's saved positions.
"""

from __future__ import annotations

from collections import Counter

from scistack_gui import ids

#: Node types whose label is the whole identity of what they show.
PIPELINE_NODE = "pipelineNode"


def _ref(node: dict) -> str:
    """``type:label``: how a person would name the node."""
    return f"{node.get('type')}:{(node.get('data') or {}).get('label', '')}"


def _position_for(node_id: str, positions: dict, pipeline_id: str) -> "dict | None":
    # The placement-id lookup trap (ids.py): a position may be keyed by the
    # graph id, the bare id or the placed id. Same order as
    # canvas_snapshot.capture.
    bare = ids.strip_placement(node_id)
    return (
        positions.get(node_id)
        or positions.get(bare)
        or positions.get(ids.placement_id(bare, pipeline_id))
    )


def _parameter_values(node: dict) -> list[str]:
    values = (node.get("data") or {}).get("values") or []
    return sorted(str(v.get("value")) for v in values if isinstance(v, dict))


def canvas_shape(graph: dict, positions: "dict | None" = None) -> dict:
    """The id-free description of one scope's built *graph*.

    ``positions`` is that scope's saved ``{node_id: {x, y}}``
    (``layout.read_positions_by_scope()[pipeline_id]``); a node with no saved
    position records none (the frontend lays it out).

    Several nodes may share a ``type:label`` (two call sites of one
    function), so every list is a sorted multiset rather than a map.
    """
    pipeline_id = graph.get("pipeline_id") or ids.ROOT_SCOPE
    positions = positions or {}
    by_id = {n["id"]: n for n in graph.get("nodes", [])}

    nodes = []
    for n in graph.get("nodes", []):
        data = n.get("data") or {}
        entry: dict = {"ref": _ref(n), "run_state": data.get("run_state")}
        if n.get("type") == "parameterNode":
            entry["values"] = _parameter_values(n)
        pos = _position_for(n["id"], positions, pipeline_id)
        entry["position"] = (
            [round(float(pos["x"]), 1), round(float(pos["y"]), 1)] if pos else None
        )
        nodes.append(entry)
    nodes.sort(key=lambda e: (e["ref"], str(e.get("values")), str(e["position"])))

    edges = []
    for e in graph.get("edges", []):
        src, tgt = by_id.get(e.get("source")), by_id.get(e.get("target"))
        edges.append(
            [
                _ref(src) if src else f"?{e.get('source')}",
                _ref(tgt) if tgt else f"?{e.get('target')}",
                e.get("targetHandle") or "",
            ]
        )
    edges.sort()
    return {"pipeline_id": pipeline_id, "nodes": nodes, "edges": edges}


def diff_shapes(before: dict, after: dict) -> list[str]:
    """Human-readable differences, empty when the two shapes are equal."""
    out: list[str] = []

    def multiset(items, key):
        return Counter(key(i) for i in items)

    def node_key(n):
        return (n["ref"], tuple(n.get("values") or ()))

    b_nodes = multiset(before["nodes"], node_key)
    a_nodes = multiset(after["nodes"], node_key)
    for k in sorted(set(b_nodes) | set(a_nodes)):
        if b_nodes[k] != a_nodes[k]:
            out.append(f"node {k[0]} values={list(k[1])}: {b_nodes[k]} -> {a_nodes[k]}")

    b_edges = multiset(before["edges"], tuple)
    a_edges = multiset(after["edges"], tuple)
    for k in sorted(set(b_edges) | set(a_edges)):
        if b_edges[k] != a_edges[k]:
            out.append(f"edge {k[0]} -> {k[1]} [{k[2]}]: {b_edges[k]} -> {a_edges[k]}")

    for field in ("run_state", "position"):
        b_f = multiset(before["nodes"], lambda n: (n["ref"], str(n.get(field))))
        a_f = multiset(after["nodes"], lambda n: (n["ref"], str(n.get(field))))
        for k in sorted(set(b_f) | set(a_f)):
            if b_f[k] != a_f[k]:
                out.append(f"{field} of {k[0]} = {k[1]}: {b_f[k]} -> {a_f[k]}")
    return out


def canvas_sources(graph: dict, manual_node_ids: "set[str]") -> dict:
    """How much of *graph* is stored rows vs. derived from run history.

    A node is ``stored`` when its id (or bare id) is a manual-node row or a
    placed submodule, else ``derived``. An edge is ``drawn`` when it came
    from a manual edge row (``graph_builder.is_manual_edge``), else
    ``derived``.
    """
    from scistack_gui.domain.graph_builder import is_manual_edge

    stored, derived = 0, Counter()
    for n in graph.get("nodes", []):
        nid = n["id"]
        if (
            n.get("type") == PIPELINE_NODE
            or nid in manual_node_ids
            or ids.strip_placement(nid) in manual_node_ids
        ):
            stored += 1
        else:
            derived[n.get("type")] += 1
    drawn = sum(1 for e in graph.get("edges", []) if is_manual_edge(e))
    return {
        "nodes_stored": stored,
        "nodes_derived": sum(derived.values()),
        "nodes_derived_by_type": dict(sorted(derived.items())),
        "edges_drawn": drawn,
        "edges_derived": len(graph.get("edges", [])) - drawn,
    }
