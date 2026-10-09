"""
A canvas as data: the one owner of "copy what a pipeline's canvas shows".

``capture`` reads one or more scopes (or a selection inside one) into a
:class:`CanvasSnapshot` of plain data; ``apply`` writes a snapshot into a
target scope with fresh ids. Every copy of canvas state goes through this
pair (plan ``.claude/plan-portability.md`` Stage 2):

* duplicate a pipeline / paste a selection (``scope_service._clone_nodes``):
  capture + apply in one database;
* export a pipeline (``portability_service``): capture + :meth:`to_dict`;
* import it, here or in another database: :meth:`from_dict` + apply.

Before this module, export/import re-implemented a weaker copy of the
duplicate path: settings on nodes that had run were dropped, intent
statements and hides never travelled, and ids were minted ad hoc.

**What a snapshot holds is what the canvas shows**, read from the RESOLVED
graph (``pipeline_service.get_pipeline_graph``), so a node that exists only
because of run history is captured like any other. ``apply`` always writes
manual nodes: the copy never depends on history the target may not have
(a copy in the same database graduates onto that history again on its next
build, exactly as duplicating always did).

Settings travel as intent STATEMENTS (``intent_store.resolved_statements``:
as resolved in the source scope, written at the target's), plus whatever is
left in a node's legacy config blob. A snapshot is pure data, so a later
transformation (Stage 6's schema-key remap) is a function over it, applied
before ``apply``.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from typing import Any, Callable

logger = logging.getLogger(__name__)

PIPELINE_NODE = "pipelineNode"


@dataclass
class NodeSnap:
    node_id: str
    pipeline_id: str
    node_type: str
    label: str
    x: float = 0.0
    y: float = 0.0
    #: Whether the source had a SAVED position (else the frontend lays the
    #: node out itself, and (x, y) is only a fallback).
    has_position: bool = False
    #: Whether the source node was a manual row (vs. drawn from history).
    manual: bool = False
    #: What is left in the node's legacy config blob once every aspect that
    #: lives as a statement is removed (:func:`_blob_leftovers`). Statements
    #: are the authority for everything else.
    config: dict = field(default_factory=dict)
    #: Intent statements about the node, minus subject and scope:
    #: ``{subject_kind, aspect, key, value, surface, stated_at}``.
    statements: list[dict] = field(default_factory=list)


@dataclass
class UseSnap:
    """A placed submodule (a ``pipelineNode`` on the parent's canvas)."""

    use_id: str
    parent_pipeline_id: str
    child_pipeline_id: str
    binding: dict = field(default_factory=dict)
    x: float = 0.0
    y: float = 0.0


@dataclass
class EdgeSnap:
    source: str
    target: str
    source_handle: "str | None" = None
    target_handle: "str | None" = None


@dataclass
class HideSnap:
    """A hidden edge, as stored (endpoints are the SOURCE's ids: a hide
    matches a connection by its stored endpoints, so a same-database copy
    takes them verbatim -- see ``scope_service._clone_nodes``)."""

    pipeline_id: str
    edge_id: str
    source: str
    target: str
    source_handle: "str | None" = None
    target_handle: "str | None" = None


@dataclass
class CanvasSnapshot:
    nodes: list[NodeSnap] = field(default_factory=list)
    uses: list[UseSnap] = field(default_factory=list)
    edges: list[EdgeSnap] = field(default_factory=list)
    hides: list[HideSnap] = field(default_factory=list)
    #: ``(pipeline_id, direction, var_type)``
    hidden_ports: list[tuple[str, str, str]] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "nodes": [asdict(n) for n in self.nodes],
            "uses": [asdict(u) for u in self.uses],
            "edges": [asdict(e) for e in self.edges],
            "hides": [asdict(h) for h in self.hides],
            "hidden_ports": [list(hp) for hp in self.hidden_ports],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CanvasSnapshot":
        return cls(
            nodes=[NodeSnap(**n) for n in data.get("nodes", [])],
            uses=[UseSnap(**u) for u in data.get("uses", [])],
            edges=[EdgeSnap(**e) for e in data.get("edges", [])],
            hides=[HideSnap(**h) for h in data.get("hides", [])],
            hidden_ports=[tuple(hp) for hp in data.get("hidden_ports", [])],
        )

    def describe(self) -> str:
        return (
            f"{len(self.nodes)} node(s), {len(self.uses)} use(s), "
            f"{len(self.edges)} edge(s), {len(self.hides)} hide(s), "
            f"{len(self.hidden_ports)} hidden port(s), "
            f"{sum(len(n.statements) for n in self.nodes)} statement(s)"
        )


# ---------------------------------------------------------------------------
# Capture
# ---------------------------------------------------------------------------


def _blob_leftovers(config: dict) -> dict:
    """*config* minus every key whose aspect lives in the intent store
    (``intent_store.NODE_CONFIG_KEYS``, ``SCHEMA_LOCATION_KEYS``): those are
    captured as statements, resolved for the source scope, and replaying the
    blob's overlay too could write a different scope's value."""
    from scistack_gui.intent_store import NODE_CONFIG_KEYS, SCHEMA_LOCATION_KEYS

    graduated = set(NODE_CONFIG_KEYS.values()) | set(SCHEMA_LOCATION_KEYS)
    return {k: v for k, v in (config or {}).items() if k not in graduated}


def _statement_dict(st) -> dict:
    return {
        "subject_kind": st.subject_kind,
        "aspect": st.aspect,
        "key": st.key,
        "value": st.value,
        "surface": st.surface,
        "stated_at": st.stated_at,
    }


def capture(db, pipeline_ids: list[str], node_ids: "list[str] | None" = None) -> CanvasSnapshot:
    """Read the canvas of each pipeline in *pipeline_ids* (its OWN content,
    never recursing; pass the closure to get submodules too).

    *node_ids* restricts the capture to a selection; only valid with a single
    pipeline. An edge is kept only when both endpoints are captured, and a
    hide only when both its endpoints are among the captured nodes.
    """
    from scistack_gui import ids
    from scistack_gui import intent_store
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store as ps
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    if node_ids is not None and len(pipeline_ids) != 1:
        raise ValueError("capture: a node selection needs exactly one pipeline")
    wanted = None if node_ids is None else set(node_ids)

    snap = CanvasSnapshot()
    for pid in pipeline_ids:
        graph = get_pipeline_graph(db, pid)
        # Read AFTER the graph build: building graduates manual nodes onto
        # their history-derived twins and MOVES their saved positions (and
        # manual rows) to the placed id (layout.graduate_manual_node). A read
        # taken before the build still has them under the old manual id.
        manual = ps.get_manual_nodes(db, pid)
        positions = layout_store.read_positions_by_scope().get(pid, {})
        uses_by_id = {u["use_id"]: u for u in ps.get_pipeline_uses(db, pid)}
        # One batched read per pipeline (keyed by bare id), never one per node:
        # get_node_config rebuilds the whole statement overlay on every call.
        configs = ps.get_node_configs(db, pid)
        captured: set[str] = set()

        for node in graph["nodes"]:
            nid = node["id"]
            if wanted is not None and nid not in wanted:
                continue
            # A position may be keyed by the graph's id, the bare id, or the
            # placed id a graduation wrote (`{bare}::{pipeline}`) -- the
            # placement-id lookup trap (ids.py). Try all three.
            bare = ids.strip_placement(nid)
            pos = (
                positions.get(nid)
                or positions.get(bare)
                or positions.get(ids.placement_id(bare, pid))
            )
            if node["type"] == PIPELINE_NODE:
                use = uses_by_id.get(nid)
                if use is None:
                    logger.warning("[canvas_snapshot] %s: placed submodule %s has no use row", pid, nid)
                    continue
                p = pos or {"x": 0.0, "y": 0.0}
                snap.uses.append(
                    UseSnap(nid, pid, use["child_pipeline_id"], dict(use["binding"] or {}), p["x"], p["y"])
                )
                captured.add(nid)
                continue
            label = (
                manual.get(nid, {}).get("label")
                or manual.get(bare, {}).get("label")
                or node.get("data", {}).get("label", "")
            )
            p = pos or {"x": 0.0, "y": 0.0}
            snap.nodes.append(
                NodeSnap(
                    node_id=nid,
                    pipeline_id=pid,
                    node_type=node["type"],
                    label=label,
                    x=p["x"],
                    y=p["y"],
                    has_position=pos is not None,
                    manual=nid in manual or bare in manual,
                    config=_blob_leftovers(configs.get(bare, {})),
                    statements=[
                        _statement_dict(st)
                        for st in intent_store.resolved_statements(db, nid, pid)
                    ],
                )
            )
            captured.add(nid)

        for e in graph["edges"]:
            if e["source"] in captured and e["target"] in captured:
                snap.edges.append(
                    EdgeSnap(e["source"], e["target"], e.get("sourceHandle"), e.get("targetHandle"))
                )

        captured_bare = {ids.strip_placement(n) for n in captured}
        for hide in ps.list_hidden_edges(db, pid):
            src = ids.strip_placement(hide.get("source") or "")
            tgt = ids.strip_placement(hide.get("target") or "")
            if wanted is not None and not (src in captured_bare and tgt in captured_bare):
                continue
            snap.hides.append(
                HideSnap(
                    pid,
                    hide["edge_id"],
                    hide.get("source") or "",
                    hide.get("target") or "",
                    hide.get("source_handle"),
                    hide.get("target_handle"),
                )
            )

        if wanted is None:
            hp = ps.get_hidden_ports(db, pid)
            snap.hidden_ports += [(pid, d, t) for d in ("input", "output") for t in hp.get(d, [])]

    unpositioned = [n.node_id for n in snap.nodes if not n.has_position]
    logger.info(
        "[canvas_snapshot] capture %s: %s; %d node(s) without a saved position%s",
        pipeline_ids,
        snap.describe(),
        len(unpositioned),
        f" (e.g. {unpositioned[:3]})" if unpositioned else "",
    )
    return snap


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------


def apply(
    db,
    snap: CanvasSnapshot,
    pipeline_map: "dict[str, str]",
    *,
    translation: "tuple[float, float]" = (0.0, 0.0),
    include_hides: bool = True,
    node_id_for: "Callable[[NodeSnap], str] | None" = None,
    use_id_for: "Callable[[UseSnap], str] | None" = None,
) -> dict[str, str]:
    """Write *snap* with fresh ids; return ``{old_id: new_id}``.

    *pipeline_map* sends each captured pipeline id to the target pipeline id;
    a node, use, edge or hidden port whose pipeline is not in the map is
    skipped (an import that REUSES an identical local pipeline leaves that
    pipeline's own content alone). Use children are mapped too, falling back
    to the captured child id (a same-database paste places the SAME
    submodule). *translation* offsets every position. *include_hides* writes
    the hidden edges (meaningful only where the hidden connections exist,
    i.e. in the same database). *node_id_for* / *use_id_for* give each node
    / placed submodule a DERIVED id instead of a fresh one (a library
    pipeline's re-sync must write the same ids every time:
    ``ids.library_node_id`` / ``ids.library_use_id``).
    """
    from scidb.intent import SURFACE_STORE, Statement

    from scistack_gui import ids
    from scistack_gui import intent_store
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store as ps

    dx, dy = translation
    old_to_new: dict[str, str] = {}
    n_statements = 0

    for n in snap.nodes:
        target_pid = pipeline_map.get(n.pipeline_id)
        if target_pid is None:
            continue
        new_id = node_id_for(n) if node_id_for else ids.new_manual_node_id(n.node_type, n.label)
        old_to_new[n.node_id] = new_id
        ps.write_manual_node(db, new_id, n.node_type, n.label, target_pid)
        if n.config:
            ps.update_node_config(db, new_id, n.config)
        rows = [
            Statement(
                subject_kind=s["subject_kind"],
                subject_ref=new_id,
                aspect=s["aspect"],
                value=s["value"],
                key=s.get("key"),
                scope=target_pid,
                surface=s.get("surface") or SURFACE_STORE,
                stated_at=s.get("stated_at"),
            )
            for s in n.statements
        ]
        if rows:
            intent_store.put_statements(db, rows)
            n_statements += len(rows)
        layout_store.write_node_position(new_id, n.x + dx, n.y + dy, pipeline_id=target_pid)

    for u in snap.uses:
        parent = pipeline_map.get(u.parent_pipeline_id)
        if parent is None:
            continue
        child = pipeline_map.get(u.child_pipeline_id, u.child_pipeline_id)
        new_use = ps.add_pipeline_use(
            db, parent, child, dict(u.binding), use_id=use_id_for(u) if use_id_for else None
        )
        old_to_new[u.use_id] = new_use
        layout_store.write_node_position(new_use, u.x + dx, u.y + dy, pipeline_id=parent)

    n_edges = 0
    for e in snap.edges:
        src, tgt = old_to_new.get(e.source), old_to_new.get(e.target)
        if src is None or tgt is None:
            continue
        ps.write_manual_edge(
            db,
            {
                "id": ids.new_manual_edge_id(),
                "source": src,
                "target": tgt,
                "sourceHandle": e.source_handle,
                "targetHandle": e.target_handle,
            },
        )
        n_edges += 1

    n_hides = 0
    if include_hides:
        for h in snap.hides:
            target_pid = pipeline_map.get(h.pipeline_id)
            if target_pid is None:
                continue
            ps.hide_edge(
                db, h.edge_id, h.source, h.target, h.source_handle, h.target_handle, target_pid
            )
            n_hides += 1

    n_ports = 0
    for pid, direction, var_type in snap.hidden_ports:
        target_pid = pipeline_map.get(pid)
        if target_pid is None:
            continue
        ps.hide_port(db, target_pid, direction, var_type)
        n_ports += 1

    logger.info(
        "[canvas_snapshot] apply -> %s: %d node/use id(s), %d statement(s), "
        "%d edge(s), %d hide(s), %d hidden port(s)",
        sorted(set(pipeline_map.values())),
        len(old_to_new),
        n_statements,
        n_edges,
        n_hides,
        n_ports,
    )
    return old_to_new


# ---------------------------------------------------------------------------
# Content signature (import's reuse-or-fork decision)
# ---------------------------------------------------------------------------


def _canon(value: Any) -> str:
    import json

    return json.dumps(value if value is not None else {}, sort_keys=True, default=str)


def signature(snap: CanvasSnapshot, pipeline_id: str, child_identity: "dict[str, str]") -> tuple:
    """An order-independent fingerprint of one pipeline's OWN content in
    *snap*, comparable across databases: nodes by (type, label, settings,
    statements), edges by their endpoints' (type, label), uses by the
    RESOLVED child identity (*child_identity*: captured child id -> local id),
    hidden ports. Ids, scopes and timestamps are left out -- they never match
    across a database boundary.

    Both sides of import's comparison are snapshots (the document's, and a
    fresh capture of the local pipeline), so the two can never be computed
    by different rules.
    """
    label_of: dict[str, tuple] = {}
    nodes = set()
    for n in snap.nodes:
        if n.pipeline_id != pipeline_id:
            continue
        label_of[n.node_id] = (n.node_type, n.label)
        stmts = sorted(
            _canon({k: s.get(k) for k in ("subject_kind", "aspect", "key", "value")})
            for s in n.statements
        )
        nodes.add((n.node_type, n.label, _canon(n.config), tuple(stmts)))
    uses = set()
    for u in snap.uses:
        if u.parent_pipeline_id != pipeline_id:
            continue
        child = child_identity.get(u.child_pipeline_id, u.child_pipeline_id)
        label_of[u.use_id] = (PIPELINE_NODE, child)
        uses.add((child, _canon(u.binding)))
    edges = frozenset(
        (label_of[e.source], label_of[e.target], e.source_handle, e.target_handle)
        for e in snap.edges
        if e.source in label_of and e.target in label_of
    )
    hidden = frozenset((d, t) for pid, d, t in snap.hidden_ports if pid == pipeline_id)
    return (frozenset(nodes), edges, hidden, frozenset(uses))


# ---------------------------------------------------------------------------
# Which tables travel how
# ---------------------------------------------------------------------------

PORTABILITY_CLASSES = ("canvas", "global", "history")


def table_portability() -> dict[str, str]:
    """``{table: class}`` for every GUI table, read from each owner module's
    ``PORTABILITY`` (one owner per table, as ``history.tracked_tables`` reads
    ``UNDOABLE_TABLES``). The guard test ``test_every_gui_table_is_classified``
    fails when a new table is not classified, so nothing is left out of an
    export by accident."""
    from scistack_gui import intent_store, node_wiring, pipeline_store

    out: dict[str, str] = {}
    for module in (pipeline_store, intent_store, node_wiring):
        out.update(module.PORTABILITY)
    return out


# ---------------------------------------------------------------------------
# Into another schema (portability Stage 6)
# ---------------------------------------------------------------------------


def remap_schema_keys(snap: CanvasSnapshot, key_map, report) -> CanvasSnapshot:
    """*snap* with every schema key renamed through *key_map*
    (``scidb.schema_map.KeyMap``) -- a pure function over the data, applied
    before :func:`apply`, so the canvas lands in the recipient's schema.

    Two places name schema keys: a node's location statement (its stated
    iteration level and its location selection) and a placed submodule's
    binding (``key_map`` both sides, since child and parent are both
    exporter pipelines, and the ``iterate`` overrides keyed by key). What the map drops or flags goes to
    *report* (``schema_map.MapReport``).
    """
    from dataclasses import replace

    from scidb.intent import ASPECT_SCHEMA_LOCATION

    if key_map is None or key_map.is_identity:
        return snap

    nodes = []
    for n in snap.nodes:
        statements = []
        for s in n.statements:
            if s.get("aspect") == ASPECT_SCHEMA_LOCATION and isinstance(s.get("value"), dict):
                value = dict(s["value"])
                where = f"node {n.label} ({n.pipeline_id})"
                if "schemaLevel" in value:
                    value["schemaLevel"] = key_map.level(value["schemaLevel"], where, report)
                if value.get("schemaSelection"):
                    value["schemaSelection"] = key_map.locations(
                        value["schemaSelection"], where, report
                    )
                s = {**s, "value": value}
            statements.append(s)
        nodes.append(replace(n, statements=statements))

    uses = []
    for u in snap.uses:
        binding = dict(u.binding or {})
        where = f"submodule use {u.use_id} ({u.parent_pipeline_id})"
        if binding.get("key_map"):
            mapped = {}
            for child_key, parent_key in binding["key_map"].items():
                c, p = key_map.key(child_key), key_map.key(parent_key)
                if c is None or p is None:
                    report.dropped.append((where, f"{child_key}->{parent_key}"))
                    continue
                mapped[c] = p
            binding["key_map"] = mapped
        if binding.get("iterate"):
            # {key: values}: keys renamed; the values are the exporter's data.
            before = set(binding["iterate"])
            binding["iterate"] = key_map.table(binding["iterate"], where, report) or {}
            if any(key_map.key(k) != k for k in before):
                report.flagged.append(
                    (where, "iteration-value overrides name the exporter's levels; review them")
                )
        uses.append(replace(u, binding=binding))

    out = replace(snap, nodes=nodes, uses=uses)
    logger.info(
        "[canvas_snapshot] remapped into another schema (%s)", key_map.describe()
    )
    return out
