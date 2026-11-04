"""
Pure scope logic for nested pipelines (plan-gui-nested-pipelines.md Part A).

Two concerns, both side-effect-free (a submodule's ports: ``domain.ports``):

- **Membership**: which pipeline scope a node belongs to. Manual nodes carry
  a ``pipeline_id`` in the document; DB-derived nodes (var__/fn__/param__/
  pathInput__ built from history) belong to the scope their POSITION is
  saved in — dragging a node onto a sub-pipeline's canvas writes its
  position into that scope, which IS the membership record. A node with no
  saved position anywhere defaults to the root scope — EXCEPT a
  declared-only node (see :data:`DECLARED_ONLY`), which is on no canvas
  until it is placed or wired.
- **Filtering**: restrict a fully-built graph to one scope (nodes by
  membership, edges by both-endpoints-kept).
"""

from __future__ import annotations

import logging

from scistack_gui.ids import (
    ROOT_SCOPE,
    parse_placement_id,
    placement_id,
)

logger = logging.getLogger(__name__)

#: Node-data flag, set by ``graph_builder`` on a Parameter/PathInput node
#: that exists only because source declares it (no run history, no
#: GUI-added value). History is what earns an unplaced node the root-canvas
#: default; a bare declaration lives in the sidebar, and joins a canvas only
#: once dragged there (a position) or wired (a manual edge). Without this a
#: brand-new database opened with every declared Parameter and PathInput
#: already strewn across the root canvas.
DECLARED_ONLY = "declared_only"


def node_scope(node_id: str, manual_nodes: dict, positions_by_scope: dict) -> str:
    """The pipeline scope a node belongs to (see module docstring).

    A placement-qualified id (``{canonical}::{scope}``) carries its scope
    directly — no position scan needed, and no ambiguity possible. The
    position-scan fallback stays for bare ids (not-yet-migrated documents,
    or a canonical id with no explicit placement at all, which defaults to
    root — see :func:`_resolve_in_scope`).
    """
    meta = manual_nodes.get(node_id)
    if meta is not None:
        # A bare position wins over the row's scope, as in _resolve_in_scope.
        for scope_id, positions in positions_by_scope.items():
            if node_id in positions:
                return scope_id
        return meta.get("pipeline_id") or ROOT_SCOPE
    parsed = parse_placement_id(node_id)
    if parsed is not None:
        return parsed[1]
    for scope_id, positions in positions_by_scope.items():
        if node_id in positions:
            return scope_id
    return ROOT_SCOPE


def _resolve_in_scope(
    node_id: str,
    scope_id: str,
    manual_nodes: dict,
    positions_by_scope: dict,
    default_to_root: bool = True,
) -> str | None:
    """Resolve ``node_id`` (manual, bare canonical, or already placement-
    qualified) to its id WITHIN ``scope_id``, or None if not visible there.

    A DB-derived canonical id can have independent placements in more than
    one scope (see ids.placement_id) — this is the one
    shared "does X belong to scope Y" answer, so scope membership is judged
    identically everywhere.

    A bare id can ALSO be positioned directly, with no placement suffix at
    all — dragging an existing DB-derived node straight onto a sub-canvas
    writes its position under the literal bare id, bypassing graduation
    entirely (this is the original "position IS the membership record"
    mechanism, still single-scope-exclusive by construction: a bare key
    can only physically live in one scope bucket at a time). Both forms
    are checked so a canonical id can carry an old-style bare placement in
    one scope while independently graduating into a new-style qualified
    placement in another.
    """
    meta = manual_nodes.get(node_id)
    if meta is not None:
        # A hand-placed node that has run keeps its (allocated) id, so it is
        # ALSO a DB node, and can be placed in another scope by placement id:
        # duplicating a hypothesis does exactly that. Checking only the row's
        # own scope dropped every such node from the copy (scistack.log
        # 2026-10-01 23:03, "placed in [..., 'pipe_bf2a723782c5']" yet not in
        # that view).
        #
        # The placed spelling wins in the row's OWN scope too (2026-10-22). An
        # adopted node keeps the id it had before it was stored, and a node
        # positioned under `{id}::{scope}` was drawn under that id: answering
        # with the bare id would orphan its position and every edge naming it
        # (plan-canvas-as-document Stage 3).
        candidate = placement_id(node_id, scope_id)
        if candidate in positions_by_scope.get(scope_id, {}):
            return candidate
        # A bare position is still the membership record: dragging a node
        # onto another canvas writes its position there. Since adoption a node
        # from a run is a stored row too, and its row's scope is where it was
        # FIRST drawn, so a bare position elsewhere must win over it or a move
        # would not take (test_position_moves_db_derived_node_between_scopes).
        if node_id in positions_by_scope.get(scope_id, {}):
            return node_id
        if any(
            node_id in positions
            for sid, positions in positions_by_scope.items()
            if sid != scope_id
        ):
            return None
        if (meta.get("pipeline_id") or ROOT_SCOPE) == scope_id:
            return node_id
        return None

    parsed = parse_placement_id(node_id)
    if parsed is not None:
        return node_id if parsed[1] == scope_id else None

    candidate = placement_id(node_id, scope_id)
    if candidate in positions_by_scope.get(scope_id, {}):
        return candidate
    if node_id in positions_by_scope.get(scope_id, {}):
        return node_id

    # No placement (qualified or bare) for this id in scope_id — check
    # whether it's placed anywhere else at all before defaulting to root.
    for positions in positions_by_scope.values():
        if node_id in positions:
            return None  # bare placement, but in a DIFFERENT scope
        for pid in positions:
            parsed_pid = parse_placement_id(pid)
            if parsed_pid is not None and parsed_pid[0] == node_id:
                return None  # qualified placement, but in a DIFFERENT scope
    return node_id if scope_id == ROOT_SCOPE and default_to_root else None


def resolve_scope_view(
    nodes: list[dict],
    edges: list[dict],
    scope_id: str,
    manual_nodes: dict,
    positions_by_scope: dict,
) -> tuple[list, list]:
    """Resolve the full (bare-id) built graph into ONE scope's view.

    Replaces the old ``filter_graph_to_scope`` now that a DB-derived
    canonical node can have independent placements in more than one scope:
    a node is kept, with its id rewritten to the resolved (placement-
    qualified, where applicable) form, when :func:`_resolve_in_scope`
    finds it visible here; an edge is kept, with its endpoints rewritten,
    only when BOTH resolve within this scope — so a dangling reference (an
    endpoint that matches no built node) still defaults to root and stays
    on the root canvas exactly as it did pre-scoping.
    """
    # A declared-only node an edge already touches is in use (the edge can
    # only be one the user drew), so it keeps the root default — hiding it
    # would strand that edge.
    wired = {e["source"] for e in edges} | {e["target"] for e in edges}
    unplaced_declared = {
        n["id"]
        for n in nodes
        if n.get("data", {}).get(DECLARED_ONLY) and n["id"] not in wired
    }

    def _resolve(node_id: str) -> str | None:
        return _resolve_in_scope(
            node_id,
            scope_id,
            manual_nodes,
            positions_by_scope,
            default_to_root=node_id not in unplaced_declared,
        )

    # An edge endpoint naming a placement IN THIS SCOPE (``X::scope``) places
    # X here. Graduation writes exactly that id into a drawn edge, and a
    # manual node that had no saved position leaves no other placement record
    # behind (positions are the membership record, and a position-less manual
    # node -- pipeline_discovery seeds them -- has none to move). Without this
    # the edge survived (its endpoint resolves within the scope) while the
    # node did not: an edge to nothing (test_graph_edge_endpoints, 2026-10-09).
    edge_placed: set[str] = set()
    for e in edges:
        for end in (e["source"], e["target"]):
            parsed = parse_placement_id(end or "")
            if parsed is not None and parsed[1] == scope_id:
                edge_placed.add(end)

    id_map: dict[str, str] = {}
    kept_nodes = []
    placed_by_edge = []
    for n in nodes:
        resolved = _resolve(n["id"])
        if parse_placement_id(n["id"]) is None:
            placed = placement_id(n["id"], scope_id)
            if placed in edge_placed and resolved != placed:
                # A stored row drawn here under its bare id (its own scope,
                # nothing positioned under the placed spelling): the edge
                # names the same node, so map it rather than rename the node.
                # Since adoption (2026-10-22) a node that ran is a stored row
                # too, so this case is no longer only DB-derived nodes.
                if resolved is not None and (
                    resolved in positions_by_scope.get(scope_id, {})
                    or n["id"] in manual_nodes
                ):
                    # Positioned here under its bare id: the edge names the
                    # same node by its placed spelling -- map, don't move it
                    # (moving would orphan its saved position).
                    id_map[placed] = resolved
                else:
                    resolved = placed
                    placed_by_edge.append(placed)
        if resolved is not None:
            id_map[n["id"]] = resolved
            kept_nodes.append({**n, "id": resolved} if resolved != n["id"] else n)
    if placed_by_edge:
        logger.info(
            "[scope_filter] %s: placed by their drawn edges (no saved position): %s",
            scope_id,
            placed_by_edge,
        )

    kept_edges = []
    for e in edges:
        src = id_map.get(e["source"]) or _resolve(e["source"])
        tgt = id_map.get(e["target"]) or _resolve(e["target"])
        if src is not None and tgt is not None:
            kept_edges.append(
                {**e, "source": src, "target": tgt}
                if (src, tgt) != (e["source"], e["target"])
                else e
            )

    # Diagnostic: an edge kept with an endpoint no kept node has is drawn by
    # nothing (the canvas silently loses it). Say so, naming both ends.
    kept_ids = {n["id"] for n in kept_nodes}
    dangling = [
        (e["source"], e["target"]) for e in kept_edges
        if e["source"] not in kept_ids or e["target"] not in kept_ids
    ]
    if dangling:
        logger.warning(
            "[scope_filter] %s: %d edge(s) point at a node not on this canvas: %s",
            scope_id,
            len(dangling),
            dangling[:5],
        )

    # A FUNCTION node missing from a scope's view is what "the canvas lost my
    # nodes" looks like (scistack.log 2026-10-01 22:51, a duplicated hypothesis
    # drew 1 of 4 function nodes). Name each, with where it IS placed.
    dropped_fns = [
        n["id"]
        for n in nodes
        if n.get("type") == "functionNode" and n["id"] not in id_map
    ]
    if dropped_fns:
        from scistack_gui.ids import strip_placement as _strip

        def _placed_in(node_id: str) -> list:
            bare = _strip(node_id)
            return sorted(
                scope
                for scope, positions in (positions_by_scope or {}).items()
                if any(_strip(k) == bare for k in positions)
            )

        logger.info(
            "[scope_filter] scope %s: %d function node(s) not in this view: %s",
            scope_id,
            len(dropped_fns),
            [f"{nid} (placed in {_placed_in(nid) or 'no scope'})" for nid in dropped_fns[:20]],
        )

    logger.debug(
        "[scope_filter] scope %s: kept %d/%d node(s), %d/%d edge(s)",
        scope_id,
        len(kept_nodes),
        len(nodes),
        len(kept_edges),
        len(edges),
    )
    off_canvas = sorted(nid for nid in unplaced_declared if nid not in id_map)
    if off_canvas:
        logger.debug(
            "[scope_filter] scope %s: %d declared-only node(s) never placed "
            "or wired, left off the canvas: %s",
            scope_id,
            len(off_canvas),
            off_canvas,
        )
    return kept_nodes, kept_edges
