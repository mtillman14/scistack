"""
Pure scope logic for nested pipelines (plan-gui-nested-pipelines.md Part A).

Three concerns, all side-effect-free:

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
- **Document interface**: a pipeline scope's ports — variable types consumed
  inside but not produced inside (inputs) and types produced inside
  (outputs) — the GUI-document mirror of scidb's ``Pipeline.interface()``,
  recursing through nested pipeline uses (acyclic by store guard).
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
    shared "does X belong to scope Y" answer used by both
    :func:`resolve_scope_view` and :func:`document_interface`, so scope
    membership is judged identically everywhere.

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
        return node_id if (meta.get("pipeline_id") or ROOT_SCOPE) == scope_id else None

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

    id_map: dict[str, str] = {}
    kept_nodes = []
    for n in nodes:
        resolved = _resolve(n["id"])
        if resolved is not None:
            id_map[n["id"]] = resolved
            kept_nodes.append({**n, "id": resolved} if resolved != n["id"] else n)

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


def _var_label(node_id: str, manual_nodes: dict) -> str | None:
    """Variable-type label for a node id, or None if not a variable node.

    ``edge_resolver.node_id_to_var_label`` owns the rule (it also handles a
    manual variable id's random suffix, ``var__{label}__{rand}``, which a
    plain prefix strip gets wrong).
    """
    from scistack_gui.domain.edge_resolver import node_id_to_var_label

    return node_id_to_var_label(node_id, {}, manual_nodes)


def document_interface(
    scope_id: str,
    manual_nodes: dict,
    edges: list[dict],
    uses_by_parent: dict,
    positions_by_scope: dict | None = None,
    hidden_ports: dict | None = None,
    _visiting: frozenset = frozenset(),
) -> dict:
    """A scope's ports from the DOCUMENT graph: ``{"inputs": [...],
    "outputs": [...]}`` as sorted variable-type labels.

    consumed = variable→function edges inside the scope; produced =
    function→variable edges inside the scope. Nested pipeline uses recurse:
    a child's inputs join consumed, its outputs join produced (matching how
    the composed backend graph would union). ``uses_by_parent`` maps
    ``parent_pipeline_id -> [{"use_id", "child_pipeline_id", ...}]``.

    ``hidden_ports`` (see ``pipeline_store.get_hidden_ports_by_scope``) is
    a manual override — ``{pipeline_id: {"input": {type, ...}, "output":
    {type, ...}}}`` — suppressing one type's port on ONE specific scope,
    toggled by right-clicking a variable node inside that scope's own
    canvas (to-do #9). Applied last, after the automatic union (including
    whatever bubbled up from nested ``uses``), and only against THIS
    scope's own entry — a hide made two levels down doesn't silently also
    hide the parent's re-export of that type, and a hide made here doesn't
    retroactively change what the child scope itself reports.
    """
    if scope_id in _visiting:  # defensive; the store rejects cycles
        return {"inputs": [], "outputs": []}
    positions_by_scope = positions_by_scope or {}
    hidden_ports = hidden_ports or {}

    # Scope membership judged per-edge-endpoint via the same resolution
    # resolve_scope_view uses — a bare id and a placement-qualified id for
    # the SAME canonical node must both correctly test "in scope_id" here,
    # since edges can carry either form (bare, pre-graduation; placement-
    # qualified, after graduate_manual_node rewrites them).
    consumed: set[str] = set()
    produced: set[str] = set()
    for e in edges:
        src, tgt = e["source"], e["target"]
        src_label = _var_label(src, manual_nodes)
        tgt_label = _var_label(tgt, manual_nodes)
        tgt_in_scope = (
            _resolve_in_scope(tgt, scope_id, manual_nodes, positions_by_scope)
            is not None
        )
        src_in_scope = (
            _resolve_in_scope(src, scope_id, manual_nodes, positions_by_scope)
            is not None
        )
        # var -> fn (consumption): source is a variable, target in scope.
        if src_label is not None and tgt_in_scope:
            consumed.add(src_label)
        # fn -> var (production): target is a variable, source in scope.
        if tgt_label is not None and src_in_scope:
            produced.add(tgt_label)

    for use in uses_by_parent.get(scope_id, []):
        child_iface = document_interface(
            use["child_pipeline_id"],
            manual_nodes,
            edges,
            uses_by_parent,
            positions_by_scope,
            hidden_ports,
            _visiting | {scope_id},
        )
        consumed.update(child_iface["inputs"])
        produced.update(child_iface["outputs"])

    scope_hidden = hidden_ports.get(scope_id, {})
    hidden_inputs = scope_hidden.get("input", set())
    hidden_outputs = scope_hidden.get("output", set())

    return {
        "inputs": sorted((consumed - produced) - hidden_inputs),
        "outputs": sorted(produced - hidden_outputs),
    }
