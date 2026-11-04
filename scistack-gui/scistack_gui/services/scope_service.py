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

from scifor.failures import ActionableError

from scistack_gui.domain import ports
from scistack_gui.ids import in_handle, out_handle

logger = logging.getLogger(__name__)

#: Where extract places input/output nodes around the moved nodes.
PORT_NODE_MARGIN = 220.0
PORT_NODE_SPACING = 100.0


def list_pipelines() -> dict:
    """All scopes + all use edges — the sidebar / breadcrumb data."""
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db

    db = get_db()
    return {
        "pipelines": ps.list_pipelines(db),
        "uses": ps.get_pipeline_uses(db),
    }


def create_pipeline(name: str) -> dict:
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db

    pipeline_id = ps.create_pipeline(get_db(), name)
    return {"ok": True, "pipeline_id": pipeline_id, "name": name}


def rename_pipeline(pipeline_id: str, name: str) -> dict:
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db

    ps.rename_pipeline(get_db(), pipeline_id, name)
    return {"ok": True}


def hide_pipeline(pipeline_id: str) -> dict:
    """Hide a scope (store guards apply). Positions are left intact —
    never delete data; unhide_pipeline fully restores it."""
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db

    ps.hide_pipeline(get_db(), pipeline_id)
    return {"ok": True}


def unhide_pipeline(pipeline_id: str) -> dict:
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db

    ps.unhide_pipeline(get_db(), pipeline_id)
    return {"ok": True}


def list_hidden_pipelines() -> dict:
    """Hidden pipelines — the restore panel's data."""
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db

    return {"pipelines": ps.list_hidden_pipelines(get_db())}


def list_hypotheses() -> dict:
    """Hypothesis-tagged pipelines — the tab strip's data."""
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db

    return {"hypotheses": ps.list_hypotheses(get_db())}


def create_hypothesis(name: str) -> dict:
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db

    pipeline_id = ps.create_hypothesis(get_db(), name)
    return {"ok": True, "pipeline_id": pipeline_id, "name": name}


def update_hypothesis(
    pipeline_id: str,
    research_question: str | None = None,
    hypothesis_statement: str | None = None,
    evidence_for: list | None = None,
    evidence_against: list | None = None,
) -> dict:
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db

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
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db

    ps.hide_hypothesis(get_db(), pipeline_id)
    return {"ok": True}


def add_pipeline_use(
    parent_pipeline_id: str,
    child_pipeline_id: str,
    binding: dict | None = None,
    x: float = 0.0,
    y: float = 0.0,
) -> dict:
    """Place a pipeline node on a parent canvas (use row + node + position).

    Placing a LIBRARY pipeline also declares the Parameters / PathInputs the
    project lacks, by itself (E7, ``library_service.on_placed``, the one
    path: there is no second "declare" click to forget)."""
    from scistack_gui.stores import layout as layout_store
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db
    from scistack_gui.services.library_service import on_placed

    use_id = ps.add_pipeline_use(
        get_db(), parent_pipeline_id, child_pipeline_id, binding
    )
    layout_store.write_node_position(use_id, x, y, pipeline_id=parent_pipeline_id)
    declared = on_placed(get_db(), child_pipeline_id)
    return {"ok": True, "use_id": use_id, "declared": declared}


def remove_pipeline_use(use_id: str) -> dict:
    from scistack_gui.stores import layout as layout_store
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db

    ps.remove_pipeline_use(get_db(), use_id)
    layout_store.drop_node_positions(use_id)
    return {"ok": True}


def update_use_binding(use_id: str, binding: dict) -> dict:
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db

    ps.update_use_binding(get_db(), use_id, binding)
    return {"ok": True}


class SubmoduleSelectionError(ActionableError, ValueError):
    """A selection can't become a submodule. A refusal whose message says what
    to select instead, so the journal files it as a setup problem."""


class PortError(ActionableError, ValueError):
    """A port can't be created, renamed or moved as asked."""


def extract_to_submodule(pipeline_id: str, node_ids: list[str], name: str) -> dict:
    """Turn nodes selected on ``pipeline_id``'s canvas into a new submodule.

    A MOVE: the nodes leave the parent canvas, replaced by one placement at the
    selection's centroid. Each connection crossing the boundary is rewritten
    into an inner edge and an outer edge through a port
    (``domain.ports``, ``docs/claude/submodule-ports.md``), so both canvases
    only ever store edges with both ends on them. Reading through the ports
    gives back the original connection, so nothing about what ran or what is
    current changes.

    A crossing edge hidden on this canvas is left as it is: it binds nothing,
    and redrawing it restores it. A selection a path leaves and re-enters is
    refused (:func:`domain.ports.find_reentry`).
    """
    from scistack_gui import ids
    from scistack_gui.stores import layout as layout_store
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db
    from scistack_gui.domain.edge_resolver import node_id_to_var_label
    from scistack_gui.domain.edge_view import effective_edges
    from scistack_gui.domain.scope_filter import node_scope

    db = get_db()
    node_id_set = set(node_ids)
    if not node_id_set:
        raise ValueError("select at least one node to extract")

    view = effective_edges(db, pipeline_id, caller="extract_to_submodule")
    manual_nodes = view.manual_nodes
    positions_by_scope = layout_store.read_positions_by_scope()
    for nid in node_id_set:
        scope = node_scope(nid, manual_nodes, positions_by_scope)
        if scope != pipeline_id:
            raise ValueError(f"node '{nid}' is not on pipeline '{pipeline_id}'")

    bare_selected = {ids.strip_placement(n) for n in node_id_set}

    def is_moved(end: str) -> bool:
        # An edge may name a selected node by its bare or its placed spelling.
        if end in node_id_set:
            return True
        parsed = ids.parse_placement_id(end)
        if parsed is not None and parsed[1] != pipeline_id:
            return False
        return ids.strip_placement(end) in bare_selected

    def is_here(end: str) -> bool:
        return node_scope(end, manual_nodes, positions_by_scope) == pipeline_id

    def label_of(end: str) -> str:
        meta = manual_nodes.get(end) or manual_nodes.get(ids.strip_placement(end)) or {}
        return meta.get("label") or ids.strip_placement(end)

    def var_label_of(end: str) -> "str | None":
        return node_id_to_var_label(end, {}, manual_nodes)

    here = [
        e for e in view.drawn_list
        if is_here(e.get("source") or "") and is_here(e.get("target") or "")
    ]
    crossing = [e for e in here if is_moved(e["source"]) != is_moved(e["target"])]

    reentry = ports.find_reentry(crossing, here, is_moved)
    if reentry is not None:
        path = " -> ".join(label_of(n) for n in reentry)
        logger.warning(
            "[scope_service] extract_to_submodule refused: %d selected node(s) on "
            "'%s', a path leaves and re-enters the selection: %s",
            len(node_id_set), pipeline_id, path,
        )
        raise SubmoduleSelectionError(
            f"Can't make a submodule from this selection: {path} leaves the "
            "selection and comes back into it. Select the nodes in between too, "
            "or leave out the node it returns to."
        )

    planned = ports.plan_boundary(crossing, is_moved, var_label_of, label_of)

    # Centroid of the selection's CURRENT positions, for the placed node.
    parent_positions = positions_by_scope.get(pipeline_id, {})
    selected_positions = [parent_positions[n] for n in node_id_set if n in parent_positions]
    cx = sum(p["x"] for p in selected_positions) / len(selected_positions) if selected_positions else 0.0
    cy = sum(p["y"] for p in selected_positions) / len(selected_positions) if selected_positions else 0.0
    min_x = min((p["x"] for p in selected_positions), default=0.0)
    max_x = max((p["x"] for p in selected_positions), default=0.0)
    min_y = min((p["y"] for p in selected_positions), default=0.0)

    # One transaction: a failure partway must not leave nodes half-moved. A call
    # that raises records no undo step (history.recording), so anything it had
    # already written could never be undone.
    n_rewritten = 0
    try:
        with ps.transaction(db, "extract_to_submodule"):
            new_pid = ps.create_pipeline(db, name)
            use_id = add_pipeline_use(pipeline_id, new_pid, None, cx, cy)["use_id"]

            rows = {ports.INPUT_PORT_NODE: 0, ports.OUTPUT_PORT_NODE: 0}
            for port in planned:
                is_input = port.direction == ports.INPUT_PORT_NODE
                order = rows[port.direction]
                rows[port.direction] += 1
                port_id = ids.new_manual_node_id(port.direction, port.name)
                layout_store.write_manual_node(
                    port_id,
                    min_x - PORT_NODE_MARGIN if is_input else max_x + PORT_NODE_MARGIN,
                    min_y + order * PORT_NODE_SPACING,
                    port.direction,
                    port.name,
                    pipeline_id=new_pid,
                )

                outer_written: set[tuple] = set()
                for e in port.edges:
                    inner = {"id": ids.new_manual_edge_id()}
                    if is_input:
                        inner.update(source=port_id, target=e["target"])
                        if e.get("targetHandle"):
                            inner["targetHandle"] = e["targetHandle"]
                        outer = {
                            "source": e["source"],
                            "target": use_id,
                            "sourceHandle": e.get("sourceHandle"),
                            "targetHandle": in_handle(port.name),
                        }
                    else:
                        inner.update(source=e["source"], target=port_id)
                        if e.get("sourceHandle"):
                            inner["sourceHandle"] = e["sourceHandle"]
                        outer = {
                            "source": use_id,
                            "target": e["target"],
                            "sourceHandle": out_handle(port.name),
                            "targetHandle": e.get("targetHandle"),
                        }
                    ps.delete_manual_edge(db, e["id"])
                    # Fan-out: a source feeding several inner nodes is one port
                    # and one outer edge; an output feeding several outer nodes
                    # is one port and one inner edge.
                    side = (outer["source"], outer["target"], outer["targetHandle"])
                    inner_side = (inner["source"], inner["target"], inner.get("targetHandle"))
                    if inner_side not in outer_written:
                        outer_written.add(inner_side)
                        ps.write_manual_edge(db, inner)
                    if side not in outer_written:
                        outer_written.add(side)
                        ps.write_manual_edge(db, {"id": ids.new_manual_edge_id(), **outer})
                    n_rewritten += 1

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
    except Exception as exc:
        logger.warning(
            "[scope_service] extract_to_submodule: rolled back, nothing moved "
            "(%d node(s) from '%s', name=%r): %s: %s",
            len(node_id_set), pipeline_id, name, type(exc).__name__, exc,
        )
        raise

    logger.info(
        "[scope_service] extract_to_submodule: %d node(s) from '%s' -> new "
        "pipeline '%s' (%s), %d port(s) %s, %d crossing edge(s) rewritten",
        len(node_id_set), pipeline_id, name, new_pid, len(planned),
        [(p.direction, p.name) for p in planned], n_rewritten,
    )
    return {"ok": True, "pipeline_id": new_pid, "use_id": use_id}


def check_new_port(db, node_type: str, label: str, pipeline_id: str) -> None:
    """Refuse a port on a canvas nothing can place, or a duplicate name.

    One rule for ``put_layout`` (the user inserting a node); extract names its
    own ports through ``ports.unique_port_name``.
    """
    from scistack_gui.stores import pipeline_store as ps

    if pipeline_id in {h["pipeline_id"] for h in ps.list_hypotheses(db)}:
        raise PortError(
            "Input and output nodes belong inside a submodule. This canvas is a "
            "hypothesis, which nothing places, so its ports could never connect."
        )
    existing = ports.ports_of_scope(
        pipeline_id, ps.get_manual_nodes(db), ps.get_hidden_node_ids(db, pipeline_id)
    )
    direction = "inputs" if node_type == ports.INPUT_PORT_NODE else "outputs"
    if label in existing[direction]:
        raise PortError(
            f"This submodule already has an {direction[:-1]} port named '{label}'. "
            "Choose another name."
        )


def rename_port(node_id: str, name: str) -> dict:
    """Rename an input/output node, and the outer edges on every placement
    that connect to its port. Wiring and identity don't change."""
    from scistack_gui.runtime.db import get_db
    from scistack_gui.stores import pipeline_store as ps

    db = get_db()
    name = name.strip()
    meta = ps.get_manual_nodes(db).get(node_id)
    if meta is None or meta.get("type") not in ports.PORT_NODE_TYPES:
        raise PortError(f"'{node_id}' is not an input or output node.")
    if not name:
        raise PortError("A port needs a name.")
    if name == meta["label"]:
        return {"ok": True}
    check_new_port(db, meta["type"], name, meta["pipeline_id"])

    is_input = meta["type"] == ports.INPUT_PORT_NODE
    old_handle = in_handle(meta["label"]) if is_input else out_handle(meta["label"])
    new_handle = in_handle(name) if is_input else out_handle(name)
    placements = {
        u["use_id"]
        for u in ps.get_pipeline_uses(db)
        if u["child_pipeline_id"] == meta["pipeline_id"]
    }
    field_end, field_handle = (
        ("target", "targetHandle") if is_input else ("source", "sourceHandle")
    )
    n_edges = 0
    with ps.transaction(db, "rename_port"):
        ps.write_manual_node(db, node_id, meta["type"], name, meta["pipeline_id"])
        for edge in ps.get_manual_edges(db):
            if edge.get(field_end) in placements and edge.get(field_handle) == old_handle:
                ps.write_manual_edge(db, {**edge, field_handle: new_handle})
                n_edges += 1
    logger.info(
        "[scope_service] rename_port: %s '%s' -> '%s' on '%s' (%d outer edge(s))",
        meta["type"], meta["label"], name, meta["pipeline_id"], n_edges,
    )
    _notify_scope_changed("rename_port")
    return {"ok": True}


def _notify_scope_changed(source: str) -> None:
    from scistack_gui.services.layout_service import _notify_dag_updated

    _notify_dag_updated(source)


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

    One owner for the copy itself: ``services.canvas_snapshot`` (capture,
    then apply), which export/import also use, so a duplicate and an
    exported-then-imported pipeline cannot copy different things. What is
    left here is what only a same-database copy needs: the anchor rule and
    the source-side placement step below.

    Returns ``(old_id -> new_id map, nodes copied, edges copied)``.
    """
    from scistack_gui import ids
    from scistack_gui.stores import layout as layout_store
    from scistack_gui.services import canvas_snapshot

    snap = canvas_snapshot.capture(db, [source_pid], node_ids)

    # anchor=None (duplicate_pipeline): keep each node's own position,
    # offset by a fixed amount. anchor=(x, y) (paste_nodes): translate the
    # WHOLE selection so its bounding-box top-left lands there, preserving
    # the copied nodes' relative layout to one another.
    if anchor is None:
        translation = (40.0, 40.0)
    else:
        real = [(n.x, n.y) for n in snap.nodes if n.has_position]
        min_x = min((x for x, _ in real), default=0.0)
        min_y = min((y for _, y in real), default=0.0)
        translation = (anchor[0] - min_x, anchor[1] - min_y)

    old_to_new = canvas_snapshot.apply(
        db, snap, {source_pid: target_pid}, translation=translation
    )

    # A node drawn from history may be a BARE canonical id relying on the
    # implicit root default (never explicitly placed anywhere) rather than an
    # explicit placement. Once the fresh copy independently graduates to its
    # OWN placement in target_pid on the next graph build, a bare id's "no
    # placement anywhere" fallback and a "moved away to another scope" state
    # become indistinguishable from position data alone
    # (domain.scope_filter._resolve_in_scope can't tell "copied" from
    # "relocated") — the source's implicit default would silently disappear
    # from its own canvas. Affirm the source's own explicit placement now,
    # before that ambiguity can arise.
    unpositioned_solidified = 0
    for n in snap.nodes:
        if n.manual or ids.parse_placement_id(n.node_id) is not None:
            continue
        solidified_id = ids.placement_id(n.node_id, source_pid)
        if n.has_position:
            solidify_pos = {"x": n.x, "y": n.y}
        else:
            # No saved position ever existed — the frontend
            # (frontend/src/layout.ts) auto-arranges such nodes via dagre on
            # every load. Writing a REAL saved position is still required
            # (the only way to record "this canonical node is explicitly
            # claimed by `source_pid`") but it must not be one shared
            # fallback for every node solidified here — the frontend treats
            # any saved position as authoritative, so identical coordinates
            # collapse every such node onto one point. Spread them out.
            solidify_pos = {
                "x": unpositioned_solidified * 60.0,
                "y": unpositioned_solidified * 60.0,
            }
            unpositioned_solidified += 1
        logger.info(
            "[_clone_nodes] solidifying %s -> %s at %r (had_real_pos=%s)",
            n.node_id, solidified_id, solidify_pos, n.has_position,
        )
        layout_store.write_node_position(
            solidified_id, solidify_pos["x"], solidify_pos["y"], pipeline_id=source_pid
        )

    n_edges = sum(1 for e in snap.edges if e.source in old_to_new and e.target in old_to_new)
    return old_to_new, len(old_to_new), n_edges


def duplicate_pipeline(pipeline_id: str, new_name: str) -> dict:
    """Fork ``pipeline_id``'s own nodes into a brand-new, independent
    pipeline the user can freely edit (e.g. "gait symmetry" -> "gait
    speed") — see ``_clone_nodes`` for what "fork" means at the node
    level. Unlike a partial paste (``paste_nodes``), a whole-scope copy is
    expected to be immediately self-consistent, so this validates the
    result compiles and rolls back if not.
    """
    from scistack_gui.stores import layout as layout_store
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db

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


def duplicate_placement(use_id: str, new_name: str) -> dict:
    """Give one placement its own independent copy of its submodule: the copy
    replaces the submodule on this placement, in place. Its connections stay (the
    copy has the same ports), and from here they can differ from the original's,
    which is how a submodule with one wiring gets connected differently."""
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db

    db = get_db()
    use = next((u for u in ps.get_pipeline_uses(db) if u["use_id"] == use_id), None)
    if use is None:
        raise ValueError(f"no placement {use_id!r}")
    new_pid = duplicate_pipeline(use["child_pipeline_id"], new_name)["pipeline_id"]
    ps.set_use_child(db, use_id, new_pid, new_name)
    logger.info(
        "[scope_service] duplicate_placement: %s now places '%s' (%s), a copy of %s",
        use_id, new_name, new_pid, use["child_pipeline_id"],
    )
    _notify_scope_changed("duplicate_placement")
    return {"ok": True, "pipeline_id": new_pid, "use_id": use_id}


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
    from scistack_gui.runtime.db import get_db

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
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db

    result = duplicate_pipeline(pipeline_id, new_name)
    ps.tag_as_hypothesis(get_db(), result["pipeline_id"])
    return result


def pipeline_interface(pipeline_id: str) -> dict:
    """A submodule's ports, in order, from its input/output nodes (the one
    owner: ``domain.ports.ports_of_scope``)."""
    from scistack_gui.stores import layout as layout_store
    from scistack_gui.stores import pipeline_store as ps
    from scistack_gui.runtime.db import get_db

    db = get_db()
    return ports.ports_of_scope(
        pipeline_id,
        ps.get_manual_nodes(db),
        ps.get_hidden_node_ids(db, pipeline_id),
        layout_store.read_positions_by_scope().get(pipeline_id),
    )


def build_pipeline_nodes(db, scope_id: str) -> list[dict]:
    """The pipelineNode entries for one canvas: one per use row on
    ``scope_id``, carrying the child's name, binding, and ports."""
    from scistack_gui.stores import layout as layout_store
    from scistack_gui.stores import pipeline_store as ps

    uses = ps.get_pipeline_uses(db, scope_id)
    if not uses:
        return []
    manual_nodes = ps.get_manual_nodes(db)
    positions_by_scope = layout_store.read_positions_by_scope()
    names = {p["pipeline_id"]: p["name"] for p in ps.list_pipelines(db)}

    nodes = []
    for use in uses:
        child_id = use["child_pipeline_id"]
        iface = ports.ports_of_scope(
            child_id,
            manual_nodes,
            ps.get_hidden_node_ids(db, child_id),
            positions_by_scope.get(child_id),
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


def export_pipeline(pipeline_id: str) -> dict:
    """Write a portable document for ``pipeline_id`` (+ everything it
    uses) to ``exports/`` and return its path + the document itself
    (to-do #7) — see services.portability_service."""
    from scistack_gui.runtime.db import get_db
    from scistack_gui.services.portability_service import export_pipeline_to_file

    return export_pipeline_to_file(get_db(), pipeline_id)


def import_pipeline(document: dict) -> dict:
    """Recreate an exported document with fresh ids (to-do #7) — see
    services.portability_service."""
    from scistack_gui.runtime.db import get_db
    from scistack_gui.services.portability_service import import_pipeline_document

    return import_pipeline_document(get_db(), document)


def export_pipeline_code(pipeline_id: str) -> dict:
    """Translate ``pipeline_id`` (+ everything it uses) into a standalone
    Python or MATLAB script, written to ``exports/`` (to-do #6) — see
    services.code_export_service."""
    from scistack_gui.runtime.db import get_db
    from scistack_gui.services.code_export_service import export_pipeline_to_code

    return export_pipeline_to_code(get_db(), pipeline_id)
