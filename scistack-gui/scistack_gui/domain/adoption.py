"""Canvas adoption and the document view: pure halves.

Plan ``.claude/plan-canvas-as-document.md`` Stages 2 and 3; the plain-language
walkthrough is ``docs/claude/canvas-as-document-explained.md``.

**Adoption** (:func:`plan_adoption`) turns what run history would draw, and
the document does not store yet, into document rows, ONCE: a ``_pipeline_nodes``
row per node (same id the build already gives it), a drawn edge per connection
(id = its connection id), a stated value per recorded Parameter value. Same
ids mean the build after adoption draws exactly the picture the build before
it did. The ``_adopted`` marker (``node_wiring.adopted_subjects``) makes it
once-only, so a node or edge the user removes is never adopted back.

**The document view** (:func:`document_view`) is Stage 3: of everything the
build assembled, keep only stored rows. History then only annotates what is
kept (run state, counts, variant rows), joined by node id, and never decides
what exists.

Pure: no database. ``services/adoption.py`` is the one writer.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from scistack_gui.ids import ROOT_SCOPE, param_node_id, strip_placement

logger = logging.getLogger(__name__)

#: Node types adoption stores. Glue and pipeline nodes are only ever created
#: by a person, so they are always rows already.
ADOPTABLE_NODE_TYPES = frozenset(
    {"variableNode", "functionNode", "parameterNode", "pathInputNode"}
)


def node_subject(node_id: str) -> str:
    return f"node:{strip_placement(node_id)}"


def edge_subject(connection: str) -> str:
    return f"edge:{connection}"


def value_subject(param: str, value: str) -> str:
    return f"value:{param}={value}"


def combo_subject(fn_name: str, call_id: str) -> str:
    return f"combo:{fn_name}:{call_id}"


@dataclass
class AdoptionPlan:
    """What one build would store. Empty on every build but the first after
    new history appears."""

    #: ``(node_id, node_type, label, scope)``
    nodes: list[tuple[str, str, str, str]] = field(default_factory=list)
    #: drawn-edge dicts: ``id`` (the connection id), ``source``, ``target``,
    #: ``sourceHandle``, ``targetHandle``
    edges: list[dict] = field(default_factory=list)
    #: ``(Parameter, value)`` to state
    values: list[tuple[str, str]] = field(default_factory=list)
    #: ``(subject, node_id, scope)`` marker rows, one per thing above
    subjects: list[tuple[str, str, str]] = field(default_factory=list)

    @property
    def empty(self) -> bool:
        return not (self.nodes or self.edges or self.values)

    @property
    def node_ids(self) -> set[str]:
        return {n[0] for n in self.nodes}

    @property
    def edge_ids(self) -> set[str]:
        return {e["id"] for e in self.edges}

    def functions(self) -> list[str]:
        """Labels of the function nodes adopted, for the notification."""
        return sorted({label for _nid, t, label, _s in self.nodes if t == "functionNode"})

    def summary(self) -> str:
        return (
            f"{len(self.nodes)} node(s) ({len(self.functions())} step(s): "
            f"{', '.join(self.functions()) or '-'}), {len(self.edges)} edge(s), "
            f"{len(self.values)} value(s)"
        )


def visibility_by_node(
    nodes: list[dict],
    edges: list[dict],
    scopes,
    manual_nodes: dict,
    positions_by_scope: dict,
) -> tuple[dict[str, list[str]], set[str]]:
    """``({bare node id: [scopes it is drawn in]}, {edge ids drawn anywhere})``
    under today's membership rules (``scope_filter.resolve_scope_view``), the
    rules adoption must reproduce exactly. Root comes first in each list."""
    from scistack_gui.domain.scope_filter import resolve_scope_view

    ordered = [ROOT_SCOPE] + sorted(s for s in set(scopes) if s != ROOT_SCOPE)
    visible: dict[str, list[str]] = {}
    edge_ids: set[str] = set()
    for scope in ordered:
        kept_nodes, kept_edges = resolve_scope_view(
            nodes, edges, scope, manual_nodes, positions_by_scope
        )
        for n in kept_nodes:
            visible.setdefault(str(strip_placement(n["id"])), []).append(scope)
        edge_ids.update(e["id"] for e in kept_edges)
    return visible, edge_ids


def plan_adoption(
    nodes: list[dict],
    edges: list[dict],
    *,
    manual_nodes: dict,
    drawn_edges: "list[dict] | tuple",
    adopted: "set[str] | frozenset[str]",
    visible: dict[str, list[str]],
    visible_edge_ids: "set[str] | frozenset[str]",
    recorded_values: dict[str, dict],
    stated_values: dict[str, set[str]],
) -> AdoptionPlan:
    """What to store so the document holds everything this build would draw.

    * ``nodes`` / ``edges``: the FULL (pre-scope) graph the build assembled,
      history-derived and stored alike.
    * ``manual_nodes`` / ``drawn_edges``: the document as it is.
    * ``adopted``: marker subjects already written (never re-adopted).
    * ``visible`` / ``visible_edge_ids``: :func:`visibility_by_node`. A node
      or edge no canvas draws (an unplaced declared-only Parameter, say) is
      not adopted: it is not on the canvas today either.
    * ``recorded_values``: ``AggregatedData.const_counts``
      (``{Parameter: {value: record count}}``); ``stated_values``: every
      value that already has a statement (pending or stated).

    A node's scope is the first canvas it is drawn on, root first. Its other
    placements keep resolving through their ``{id}::{scope}`` positions
    (``scope_filter._resolve_in_scope``).
    """
    from scistack_gui.domain.graph_builder import edge_connection_id, is_manual_edge

    plan = AdoptionPlan()
    rows = {str(strip_placement(nid)) for nid in manual_nodes}

    for n in nodes:
        if n.get("type") not in ADOPTABLE_NODE_TYPES:
            continue
        bare = str(strip_placement(n["id"]))
        if bare in rows or bare in plan.node_ids:
            continue
        scopes = visible.get(bare)
        if not scopes:
            continue
        subject = node_subject(bare)
        if subject in adopted:
            logger.debug(
                "[adoption] %s was adopted before and is no longer a row; "
                "not adopting it again",
                bare,
            )
            continue
        label = (n.get("data") or {}).get("label") or bare
        plan.nodes.append((bare, n["type"], label, scopes[0]))
        plan.subjects.append((subject, bare, scopes[0]))

    stored = rows | plan.node_ids
    drawn = {edge_connection_id(e, manual_nodes) for e in drawn_edges}
    for e in edges:
        if is_manual_edge(e) or e["id"] not in visible_edge_ids:
            continue
        src, tgt = str(strip_placement(e["source"])), str(strip_placement(e["target"]))
        if src not in stored or tgt not in stored:
            continue
        connection = edge_connection_id(e, manual_nodes)
        if connection in drawn or connection in plan.edge_ids:
            continue
        subject = edge_subject(connection)
        if subject in adopted:
            continue
        edge = {"id": connection, "source": e["source"], "target": e["target"]}
        for handle in ("sourceHandle", "targetHandle"):
            if e.get(handle):
                edge[handle] = e[handle]
        plan.edges.append(edge)
        plan.subjects.append((subject, tgt, (visible.get(tgt) or [ROOT_SCOPE])[0]))

    for param, counts in sorted((recorded_values or {}).items()):
        have = stated_values.get(param, set())
        for value in sorted(counts or {}):
            if value in have:
                continue
            subject = value_subject(param, value)
            if subject in adopted:
                continue
            plan.values.append((param, value))
            plan.subjects.append((subject, str(param_node_id(param)), ROOT_SCOPE))
    return plan


def document_view(
    nodes: list[dict],
    edges: list[dict],
    *,
    manual_nodes: dict,
    drawn_edges: "list[dict] | tuple",
    pending: "AdoptionPlan | None" = None,
) -> tuple[list[dict], list[dict], dict]:
    """Stage 3: keep only what the document stores.

    A node is kept when its bare id is a stored row (or is about to be, when
    adoption was deferred: ``pending``). An edge is kept when it is a drawn
    edge, or a history edge whose CONNECTION is drawn: ``build_edges`` keeps
    the history copy of a connection both carry, so filtering on
    ``data.manual`` alone would drop the wire entirely. One edge per
    connection survives, the drawn copy preferred.

    Returns ``(nodes, edges, counts)``; ``counts`` is for the INFO line.
    """
    from scistack_gui.domain.graph_builder import edge_connection_id, is_manual_edge

    rows = {str(strip_placement(nid)) for nid in manual_nodes}
    if pending is not None:
        rows |= pending.node_ids
    kept_nodes = [
        n
        for n in nodes
        if n.get("type") not in ADOPTABLE_NODE_TYPES
        or str(strip_placement(n["id"])) in rows
    ]
    kept_ids = {id(n) for n in kept_nodes}
    dropped_nodes = sorted(n["id"] for n in nodes if id(n) not in kept_ids)

    drawn = {edge_connection_id(e, manual_nodes) for e in drawn_edges}
    if pending is not None:
        drawn |= pending.edge_ids
    by_connection: dict[str, dict] = {}
    order: list[str] = []
    dropped_edges: list[str] = []
    for e in edges:
        connection = edge_connection_id(e, manual_nodes)
        if not (is_manual_edge(e) or connection in drawn):
            dropped_edges.append(e["id"])
            continue
        if connection not in by_connection:
            order.append(connection)
            by_connection[connection] = e
        elif is_manual_edge(e) and not is_manual_edge(by_connection[connection]):
            by_connection[connection] = e
    # Every kept edge IS a stored edge (the history copy only stands in for a
    # drawn one over the same connection), so each is tagged drawn: the
    # `canvas_sources` count and anything else reading `data.manual` see the
    # document as it is.
    kept_edges = [
        by_connection[c]
        if is_manual_edge(by_connection[c])
        else {**by_connection[c], "data": {**(by_connection[c].get("data") or {}), "manual": True}}
        for c in order
    ]
    counts = {
        "nodes_kept": len(kept_nodes),
        "nodes_dropped": len(dropped_nodes),
        "edges_kept": len(kept_edges),
        "edges_dropped": len(dropped_edges),
        "edges_merged": len(edges) - len(dropped_edges) - len(kept_edges),
    }
    if dropped_nodes or dropped_edges:
        logger.info(
            "[adoption] document view: %d history-only node(s) and %d history-only "
            "edge(s) not drawn (not stored; removed by the user, or never on a "
            "canvas): nodes=%s edges=%s",
            len(dropped_nodes),
            len(dropped_edges),
            dropped_nodes[:20],
            dropped_edges[:20],
        )
    return kept_nodes, kept_edges, counts
