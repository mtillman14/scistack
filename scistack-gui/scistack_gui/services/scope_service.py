"""
Scope service — nested-pipeline operations shared by both protocol adapters.

Wraps pipeline_store's scope/use CRUD with layout-side effects (positions
live in the JSON file per scope) and computes pipeline-node data (document
interface = ports) for the graph endpoint. Store-level ValueErrors (cycle,
root guards, unknown ids, duplicate names) pass through for the adapters to
map to protocol errors.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def list_pipelines() -> dict:
    """All scopes + all use edges — the sidebar / breadcrumb data."""
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    db = get_db()
    return {
        "pipelines": ps.list_pipelines(db),
        "uses": ps.get_pipeline_uses(db),
    }


def create_pipeline(name: str) -> dict:
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    pipeline_id = ps.create_pipeline(get_db(), name)
    return {"ok": True, "pipeline_id": pipeline_id, "name": name}


def rename_pipeline(pipeline_id: str, name: str) -> dict:
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    ps.rename_pipeline(get_db(), pipeline_id, name)
    return {"ok": True}


def hide_pipeline(pipeline_id: str) -> dict:
    """Hide a scope (store guards apply). Positions are left intact —
    never delete data; unhide_pipeline fully restores it."""
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    ps.hide_pipeline(get_db(), pipeline_id)
    return {"ok": True}


def unhide_pipeline(pipeline_id: str) -> dict:
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    ps.unhide_pipeline(get_db(), pipeline_id)
    return {"ok": True}


def list_hidden_pipelines() -> dict:
    """Hidden pipelines — the restore panel's data."""
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    return {"pipelines": ps.list_hidden_pipelines(get_db())}


def list_hypotheses() -> dict:
    """Hypothesis-tagged pipelines — the tab strip's data."""
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    return {"hypotheses": ps.list_hypotheses(get_db())}


def create_hypothesis(name: str) -> dict:
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    pipeline_id = ps.create_hypothesis(get_db(), name)
    return {"ok": True, "pipeline_id": pipeline_id, "name": name}


def update_hypothesis(
    pipeline_id: str,
    research_question: str | None = None,
    hypothesis_statement: str | None = None,
    evidence_for: list | None = None,
    evidence_against: list | None = None,
) -> dict:
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    ps.update_hypothesis(
        get_db(),
        pipeline_id,
        research_question=research_question,
        hypothesis_statement=hypothesis_statement,
        evidence_for=evidence_for,
        evidence_against=evidence_against,
    )
    return {"ok": True}


def hide_hypothesis(pipeline_id: str) -> dict:
    """Hide a hypothesis tab (store guards apply). Positions and hypothesis
    metadata are left intact — unhide_pipeline fully restores the tab."""
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    ps.hide_hypothesis(get_db(), pipeline_id)
    return {"ok": True}


def add_pipeline_use(
    parent_pipeline_id: str,
    child_pipeline_id: str,
    binding: dict | None = None,
    x: float = 0.0,
    y: float = 0.0,
) -> dict:
    """Place a pipeline node on a parent canvas (use row + node + position)."""
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    use_id = ps.add_pipeline_use(
        get_db(), parent_pipeline_id, child_pipeline_id, binding
    )
    layout_store.write_node_position(use_id, x, y, pipeline_id=parent_pipeline_id)
    return {"ok": True, "use_id": use_id}


def remove_pipeline_use(use_id: str) -> dict:
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    ps.remove_pipeline_use(get_db(), use_id)
    layout_store.drop_node_positions(use_id)
    return {"ok": True}


def update_use_binding(use_id: str, binding: dict) -> dict:
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    ps.update_use_binding(get_db(), use_id, binding)
    return {"ok": True}


def extract_to_submodule(pipeline_id: str, node_ids: list[str], name: str) -> dict:
    """Select existing nodes on ``pipeline_id``'s canvas and turn them into
    a new, immediately-reusable submodule pipeline.

    This is a MOVE (the selected nodes disappear from the parent canvas),
    replaced by one pipeline node placing the new submodule at the
    selection's centroid.

    Boundary edges (exactly one endpoint moved) are deliberately left
    UNCHANGED, not deleted — ``domain.scope_filter.document_interface``
    computes a scope's ports purely from which edges touch nodes IN that
    scope, regardless of where the other endpoint lives, so the original
    edge is exactly what makes the new submodule's interface (inputs/
    outputs) compute correctly. It simply stops rendering on either
    canvas once its endpoints split across two scopes (``resolve_scope_view``
    requires both endpoints to resolve within the same scope) — which
    is fine, since the moved node no longer renders on the parent canvas
    either. What the parent canvas needs INSTEAD is a new, additional edge
    from the kept node to the placed pipeline node's port, purely for
    visual continuity (matching how a hand-built submodule is normally
    wired: variable nodes connect to `in__{type}`/`out__{type}` handles).

    Every boundary edge is var<->fn (never var<->var or fn<->fn), but
    EITHER side may be the one that moved — a selection can carry a
    variable node into the submodule while leaving a downstream function
    behind (e.g. a leaf variable used by one function outside the
    extracted set), not just the reverse. So the variable label is looked
    up on the KEPT side first, falling back to the MOVED side when the
    kept side isn't a variable-type node itself. The in__/out__ direction
    on the placed node's port still follows which side moved (moved-as-
    source -> out__, moved-as-target -> in__), since that describes flow
    across the new scope boundary regardless of which endpoint supplied
    the label. A boundary edge where NEITHER side is a variable-type node
    has no expressible port and is left without a visual replacement
    (rare/unsupported wiring) — it still contributes to the interface via
    the mechanism above.
    """
    import uuid

    from scistack_gui import ids
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db
    from scistack_gui.domain import graph_builder
    from scistack_gui.domain.edge_resolver import node_id_to_var_label
    from scistack_gui.domain.scope_filter import node_scope

    db = get_db()
    node_id_set = set(node_ids)
    if not node_id_set:
        raise ValueError("select at least one node to extract")

    manual_nodes = ps.get_manual_nodes(db)
    positions_by_scope = layout_store.read_positions_by_scope()
    for nid in node_id_set:
        scope = node_scope(nid, manual_nodes, positions_by_scope)
        if scope != pipeline_id:
            raise ValueError(f"node '{nid}' is not on pipeline '{pipeline_id}'")

    all_edges = ps.get_manual_edges(db)

    # Classify boundary edges (exactly one endpoint moved) — see docstring
    # for why the originals are kept rather than deleted.
    new_edges: list[dict] = []  # {source, target, sourceHandle, targetHandle}
    for e in all_edges:
        src, tgt = e["source"], e["target"]
        src_moved, tgt_moved = src in node_id_set, tgt in node_id_set
        if src_moved == tgt_moved:
            continue  # both moved (internal) or neither (irrelevant) — untouched

        if src_moved:
            kept_id, moved_id = tgt, src
        else:
            kept_id, moved_id = src, tgt
        # Kept side first (the common case: a function moved, its
        # connected variable stayed put); fall back to the moved side
        # (e.g. a variable itself moved, leaving a downstream function
        # behind) — either endpoint may be the variable-type one.
        var_label = node_id_to_var_label(kept_id, {}, manual_nodes)
        if var_label is None:
            var_label = node_id_to_var_label(moved_id, {}, manual_nodes)
        if var_label is None:
            logger.warning(
                "[scope_service] extract_to_submodule: boundary edge "
                "%s -> %s has no variable-type node on either side; "
                "no visual replacement added", src, tgt,
            )
            continue
        if src_moved:
            new_edges.append({
                "source": "__NEW_USE__", "target": tgt,
                "sourceHandle": f"out__{var_label}", "targetHandle": e.get("targetHandle"),
            })
        else:
            new_edges.append({
                "source": src, "target": "__NEW_USE__",
                "sourceHandle": e.get("sourceHandle"), "targetHandle": f"in__{var_label}",
            })

    # Centroid of the selection's CURRENT positions, for the placed node.
    parent_positions = positions_by_scope.get(pipeline_id, {})
    selected_positions = [parent_positions[n] for n in node_id_set if n in parent_positions]
    cx = sum(p["x"] for p in selected_positions) / len(selected_positions) if selected_positions else 0.0
    cy = sum(p["y"] for p in selected_positions) / len(selected_positions) if selected_positions else 0.0

    new_pid = ps.create_pipeline(db, name)

    for nid in node_id_set:
        # A selected node may itself be an existing submodule placement —
        # its rendering scope is the USE row's parent, not just its
        # _pipeline_nodes row (see move_pipeline_use_parent's docstring);
        # both must move together.
        if manual_nodes.get(nid, {}).get("type") == "pipelineNode":
            ps.move_pipeline_use_parent(db, nid, new_pid)
        ps.move_node_scope(db, nid, new_pid)

        # A graduated (placement-qualified) DB-derived node's id embeds
        # its OLD scope — node_scope trusts that suffix over which bucket
        # holds the position, so simply re-writing the position under the
        # unchanged key would go stale. Re-key it for the new scope and
        # rewrite any internal edges that referenced the old key.
        parsed = ids.parse_placement_id(nid)
        new_node_id = (
            ids.placement_id(parsed[0], new_pid) if parsed else None
        )
        layout_store.move_node_position(nid, new_pid, new_node_id=new_node_id)
        if new_node_id:
            ps.rename_edge_endpoints(db, nid, new_node_id)

    use_result = add_pipeline_use(pipeline_id, new_pid, None, cx, cy)
    use_id = use_result["use_id"]

    seen: set[tuple] = set()
    for spec in new_edges:
        source = use_id if spec["source"] == "__NEW_USE__" else spec["source"]
        target = use_id if spec["target"] == "__NEW_USE__" else spec["target"]
        key = (source, target, spec["sourceHandle"], spec["targetHandle"])
        if key in seen:
            continue  # de-dupe: multiple original edges collapsing onto one port
        seen.add(key)
        ps.write_manual_edge(db, {
            "id": f"edge_{uuid.uuid4().hex[:12]}",
            "source": source,
            "target": target,
            "sourceHandle": spec["sourceHandle"],
            "targetHandle": spec["targetHandle"],
        })

    logger.info(
        "[scope_service] extract_to_submodule: %d node(s) from '%s' -> new "
        "pipeline '%s' (%s), %d boundary edge(s) added for visual continuity",
        len(node_id_set), pipeline_id, name, new_pid, len(seen),
    )
    return {"ok": True, "pipeline_id": new_pid, "use_id": use_id}


def _clone_nodes(
    db,
    source_pid: str,
    node_ids: "list[str] | None",
    target_pid: str,
    anchor: "tuple[float, float] | None" = None,
) -> "tuple[dict[str, str], int, int]":
    """Copy a set of ``source_pid``'s nodes (``node_ids=None`` means every
    node in the scope) into ``target_pid`` with fresh ids. Shared by
    ``duplicate_pipeline`` (whole scope, fixed +40/+40 offset from each
    node's own original position) and ``paste_nodes`` (an explicit
    selection, translated so its bounding box's top-left lands at
    ``anchor`` instead).

    Includes already-executed ("graduated") DB-derived nodes too, now that
    graduation is scope-aware (placement-qualified ids — see
    ids.placement_id and plan-placement-qualified-node-
    ids.md): a fresh manual copy of a graduated node safely graduates to
    its OWN independent placement in the new scope, rather than colliding
    with and stealing the original's.

    What's copied is the per-node GUI config (schemaSelection/schemaLevel/
    runOptions, and eventually whereFilters) and the wiring, since each
    copy gets its own node_id — copying those verbatim is what makes the
    copy compute IDENTICALLY to the original until the user changes
    something (at which point scidb naturally creates an independent
    variant, no special handling needed). Function/variable-type identity
    stays shared for free (a fresh node with the same label just resolves
    against the same real function/DB table); PathInput/Sweep nodes are
    shared by name by default too — copying the node (same name
    reference) is exactly the desired "shared ground truth" behavior.

    Submodule placements (pipelineNode) get a fresh use_id pointing at the
    SAME child_pipeline_id — the shared submodule itself is never
    duplicated, since factoring something out as a submodule is precisely
    what makes it reusable across hypotheses.

    An edge with either endpoint OUTSIDE the copied set is dropped (only
    possible for an explicit partial selection — a whole-scope copy has no
    such edges by construction, since every node is included).

    Returns ``(old_id -> new_id map, nodes copied, edges copied)``.
    """
    import uuid

    from scistack_gui import ids
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store as ps
    from scistack_gui.domain import graph_builder
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    graph = get_pipeline_graph(db, source_pid)
    manual_nodes = ps.get_manual_nodes(db, source_pid)  # config source
    old_positions = layout_store.read_positions_by_scope().get(source_pid, {})
    uses_by_id = {u["use_id"]: u for u in ps.get_pipeline_uses(db, source_pid)}
    prefix_by_type = {
        "functionNode": "fn",
        "variableNode": "var",
        "parameterNode": "param",
        "pathInputNode": "pathInput",
    }

    node_id_set = None if node_ids is None else set(node_ids)
    selected = [n for n in graph["nodes"] if node_id_set is None or n["id"] in node_id_set]

    # anchor=None (duplicate_pipeline): keep each node's own position,
    # offset by a fixed amount. anchor=(x, y) (paste_nodes): translate the
    # WHOLE selection so its bounding-box top-left lands there, preserving
    # the copied nodes' relative layout to one another.
    if anchor is None:
        translation = {"x": 40.0, "y": 40.0}
    else:
        real_positions = [old_positions[n["id"]] for n in selected if n["id"] in old_positions]
        min_x = min((p["x"] for p in real_positions), default=0.0)
        min_y = min((p["y"] for p in real_positions), default=0.0)
        translation = {"x": anchor[0] - min_x, "y": anchor[1] - min_y}

    old_to_new: dict[str, str] = {}
    # Counts nodes solidified below with no prior real position, so each
    # gets a distinct fallback instead of all landing on the same point.
    unpositioned_solidified = 0

    for node in selected:
        old_id = node["id"]
        node_type = node["type"]

        if node_type == "pipelineNode":
            # Submodule placement: fresh use_id, same child_pipeline_id,
            # binding copied (the copy's binding becomes independently
            # editable from here on — nothing special needed, it's just a
            # normal column value).
            use = uses_by_id.get(old_id)
            if use is None:
                continue  # defensive; shouldn't happen
            pos = old_positions.get(old_id, {"x": 0.0, "y": 0.0})
            result = add_pipeline_use(
                target_pid, use["child_pipeline_id"], dict(use["binding"]),
                pos["x"] + translation["x"], pos["y"] + translation["y"],
            )
            old_to_new[old_id] = result["use_id"]
            continue

        # Fresh node_id + config copied verbatim. Uses the FULL resolved
        # graph (not just get_manual_nodes) so already-executed nodes are
        # copied too — each gets its own node_id and, if its label matches
        # real DB history, independently graduates to its own placement in
        # target_pid on the next graph build.
        label = manual_nodes.get(old_id, {}).get("label") or node.get("data", {}).get("label", "")
        prefix = prefix_by_type.get(node_type, node_type)
        new_id = f"{prefix}__{label}__{uuid.uuid4().hex[:8]}"
        old_to_new[old_id] = new_id

        ps.write_manual_node(db, new_id, node_type, label, target_pid)
        config = manual_nodes.get(old_id, {}).get("config")
        if config:
            ps.update_node_config(db, new_id, config)
        # Statements about the original (column selections, run options, the
        # location) are COPIED — as resolved on the source canvas, written at
        # the target's scope — so the duplicate owns them and configuring
        # either can never reach the other. The line above copies only the
        # legacy config column, which never held them.
        from scistack_gui import intent_store

        intent_store.copy_subject(
            db, old_id, new_id, src_scope=source_pid, dst_scope=target_pid
        )

        real_pos = old_positions.get(old_id)
        pos = real_pos or {"x": 0.0, "y": 0.0}
        layout_store.write_node_position(
            new_id, pos["x"] + translation["x"], pos["y"] + translation["y"], pipeline_id=target_pid
        )

        # old_id may be a BARE canonical id relying on the implicit root
        # default (never explicitly placed anywhere) rather than an
        # explicit placement. Once the fresh copy above independently
        # graduates to its OWN placement in target_pid on the next graph
        # build, a bare id's "no placement anywhere" fallback and a
        # "moved away to another scope" state become indistinguishable
        # from position data alone (domain.scope_filter._resolve_in_scope
        # can't tell "copied" from "relocated") — the source's implicit
        # default would silently disappear from its own canvas. Affirm the
        # source's own explicit placement now, before that ambiguity can
        # arise.
        if old_id not in manual_nodes and ids.parse_placement_id(old_id) is None:
            solidified_id = ids.placement_id(old_id, source_pid)
            if real_pos is not None:
                solidify_pos = real_pos
            else:
                # No saved position ever existed for old_id — the frontend
                # (frontend/src/layout.ts) auto-arranges such nodes via
                # dagre on every load. Writing a REAL saved position is
                # still required (it's the only way to record "this
                # canonical node is explicitly claimed by `source_pid`",
                # avoiding the ambiguity above) but it must not be the
                # same shared fallback for every node solidified here —
                # the frontend treats any saved position as authoritative
                # and stops auto-laying it out, so identical coordinates
                # collapse every such node onto the same point (nodes
                # appear to vanish, edges between them render as
                # zero-length stubs). Spread them out instead.
                solidify_pos = {
                    "x": unpositioned_solidified * 60.0,
                    "y": unpositioned_solidified * 60.0,
                }
                unpositioned_solidified += 1
            logger.info(
                "[_clone_nodes] solidifying %s -> %s at %r (had_real_pos=%s)",
                old_id, solidified_id, solidify_pos, real_pos is not None,
            )
            layout_store.write_node_position(solidified_id, solidify_pos["x"], solidify_pos["y"], pipeline_id=source_pid)

    # get_pipeline_graph is already scope-resolved (both endpoints belong
    # to source_pid), so an edge is dropped here only when at least one
    # endpoint wasn't part of the copied selection.
    n_edges = 0
    for e in graph["edges"]:
        src, tgt = old_to_new.get(e["source"]), old_to_new.get(e["target"])
        if src is None or tgt is None:
            continue
        ps.write_manual_edge(db, {
            "id": f"edge_{uuid.uuid4().hex[:12]}",
            "source": src,
            "target": tgt,
            "sourceHandle": e.get("sourceHandle"),
            "targetHandle": e.get("targetHandle"),
        })
        n_edges += 1

    return old_to_new, len(old_to_new), n_edges


def duplicate_pipeline(pipeline_id: str, new_name: str) -> dict:
    """Fork ``pipeline_id``'s own nodes into a brand-new, independent
    pipeline the user can freely edit (e.g. "gait symmetry" -> "gait
    speed") — see ``_clone_nodes`` for what "fork" means at the node
    level. Unlike a partial paste (``paste_nodes``), a whole-scope copy is
    expected to be immediately self-consistent, so this validates the
    result compiles and rolls back if not.
    """
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    db = get_db()
    new_pid = ps.create_pipeline(db, new_name)
    _old_to_new, n_nodes, n_edges = _clone_nodes(db, pipeline_id, None, new_pid)

    # Sanity-check: the copy must compile like any other pipeline. A
    # failure here means something in the copy was inconsistent — clean up
    # rather than leave a broken pipeline in the document.
    from scistack_gui.services.execution_service import (
        _discard_compiled,
        build_backend_pipeline,
    )

    built: dict = {}
    try:
        build_backend_pipeline(db, new_pid, built)
    except Exception as exc:
        # Real delete, not hide: this pipeline never became valid, so there
        # is no user-visible content to preserve.
        ps._hard_delete_pipeline(db, new_pid)
        layout_store.drop_scope_positions(new_pid)
        raise ValueError(f"duplicated pipeline failed to compile: {exc}") from exc
    finally:
        _discard_compiled(built)

    logger.info(
        "[scope_service] duplicate_pipeline: '%s' -> '%s' (%s): %d node(s), "
        "%d edge(s)",
        pipeline_id, new_name, new_pid, n_nodes, n_edges,
    )
    return {"ok": True, "pipeline_id": new_pid}


def paste_nodes(
    source_pipeline_id: str,
    node_ids: list,
    target_pipeline_id: str,
    x: float,
    y: float,
) -> dict:
    """Copy a selection (Cmd/Ctrl+C -> Cmd/Ctrl+V, or the canvas toolbar's
    Copy/Paste buttons) from one scope into another — or the same scope
    twice — via ``_clone_nodes`` at selection granularity (to-do #5).

    Unlike ``duplicate_pipeline``, this does NOT validate that the result
    compiles: a pasted subgraph is expected to often be a dangling
    fragment right after paste (edges to nodes outside the selection are
    dropped — meaningless once the target scope may not even contain
    them), exactly like manually dragging in a few nodes one at a time.
    The user re-wires boundaries same as any other partially-wired state
    the GUI already tolerates (matches the project's "reversible, not
    restrictive" stance used for hidden edges/ports).

    Returns the old->new id map so the caller can select the freshly
    pasted nodes.
    """
    from scistack_gui.db import get_db

    if not node_ids:
        return {"ok": True, "node_id_map": {}}

    db = get_db()
    old_to_new, n_nodes, n_edges = _clone_nodes(
        db, source_pipeline_id, node_ids, target_pipeline_id, anchor=(x, y)
    )
    logger.info(
        "[scope_service] paste_nodes: %d node(s) from '%s' -> '%s' (%d "
        "internal edge(s) kept; boundary edges to non-copied nodes dropped)",
        n_nodes, source_pipeline_id, target_pipeline_id, n_edges,
    )
    return {"ok": True, "node_id_map": old_to_new}


def duplicate_hypothesis(pipeline_id: str, new_name: str) -> dict:
    """Duplicate a hypothesis's pipeline AND tag the copy as its own
    hypothesis (see duplicate_pipeline) — used by the tab strip's
    "Duplicate" action so the result shows up as a new tab, e.g. "gait
    symmetry" -> "gait speed"."""
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    result = duplicate_pipeline(pipeline_id, new_name)
    ps.tag_as_hypothesis(get_db(), result["pipeline_id"])
    return result


def pipeline_interface(pipeline_id: str) -> dict:
    """A scope's ports from the document graph (see domain.scope_filter)."""
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db
    from scistack_gui.domain.scope_filter import document_interface

    db = get_db()
    manual_nodes = ps.get_manual_nodes(db)
    edges = ps.get_manual_edges(db)
    uses_by_parent: dict = {}
    for use in ps.get_pipeline_uses(db):
        uses_by_parent.setdefault(use["parent_pipeline_id"], []).append(use)
    positions_by_scope = layout_store.read_positions_by_scope()
    hidden_ports = ps.get_hidden_ports_by_scope(db)
    return document_interface(
        pipeline_id, manual_nodes, edges, uses_by_parent, positions_by_scope, hidden_ports
    )


def build_pipeline_nodes(db, scope_id: str) -> list[dict]:
    """The pipelineNode entries for one canvas: one per use row on
    ``scope_id``, carrying the child's name, binding, and ports."""
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store as ps
    from scistack_gui.domain.scope_filter import document_interface

    all_uses = ps.get_pipeline_uses(db)
    uses_by_parent: dict = {}
    for use in all_uses:
        uses_by_parent.setdefault(use["parent_pipeline_id"], []).append(use)
    if not uses_by_parent.get(scope_id):
        return []

    manual_nodes = ps.get_manual_nodes(db)
    edges = ps.get_manual_edges(db)
    positions_by_scope = layout_store.read_positions_by_scope()
    hidden_ports = ps.get_hidden_ports_by_scope(db)
    names = {p["pipeline_id"]: p["name"] for p in ps.list_pipelines(db)}

    nodes = []
    for use in uses_by_parent[scope_id]:
        child_id = use["child_pipeline_id"]
        iface = document_interface(
            child_id, manual_nodes, edges, uses_by_parent, positions_by_scope, hidden_ports
        )
        nodes.append(
            {
                "id": use["use_id"],
                "type": "pipelineNode",
                "position": {"x": 0, "y": 0},
                "data": {
                    "label": names.get(child_id, child_id),
                    "child_pipeline_id": child_id,
                    "binding": use["binding"],
                    "inputs": iface["inputs"],
                    "outputs": iface["outputs"],
                },
            }
        )
    logger.info(
        "[scope_service] built %d pipelineNode(s) for scope %s", len(nodes), scope_id
    )
    return nodes


def hide_port(pipeline_id: str, direction: str, var_type: str) -> dict:
    """Suppress a variable type's exposed port on ``pipeline_id``'s
    interface (to-do #9) — see pipeline_store.hide_port /
    domain.scope_filter.document_interface."""
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    ps.hide_port(get_db(), pipeline_id, direction, var_type)
    return {"ok": True}


def unhide_port(pipeline_id: str, direction: str, var_type: str) -> dict:
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    ps.unhide_port(get_db(), pipeline_id, direction, var_type)
    return {"ok": True}


def get_hidden_ports(pipeline_id: str) -> dict:
    """One scope's hidden ports as JSON-friendly lists (the frontend
    context menu's Show/Hide label needs to know current state)."""
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    hidden = ps.get_hidden_ports(get_db(), pipeline_id)
    return {"input": sorted(hidden["input"]), "output": sorted(hidden["output"])}


def export_pipeline(pipeline_id: str) -> dict:
    """Write a portable document for ``pipeline_id`` (+ everything it
    uses) to ``exports/`` and return its path + the document itself
    (to-do #7) — see services.portability_service."""
    from scistack_gui.db import get_db
    from scistack_gui.services.portability_service import export_pipeline_to_file

    return export_pipeline_to_file(get_db(), pipeline_id)


def import_pipeline(document: dict) -> dict:
    """Recreate an exported document with fresh ids (to-do #7) — see
    services.portability_service."""
    from scistack_gui.db import get_db
    from scistack_gui.services.portability_service import import_pipeline_document

    return import_pipeline_document(get_db(), document)


def export_pipeline_code(pipeline_id: str) -> dict:
    """Translate ``pipeline_id`` (+ everything it uses) into a standalone
    Python or MATLAB script, written to ``exports/`` (to-do #6) — see
    services.code_export_service."""
    from scistack_gui.db import get_db
    from scistack_gui.services.code_export_service import export_pipeline_to_code

    return export_pipeline_to_code(get_db(), pipeline_id)
