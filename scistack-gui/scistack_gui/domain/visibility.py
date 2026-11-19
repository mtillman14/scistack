"""THE answer to "is this hidden?", for every canvas at once.

Concept: ``docs/claude/visibility-model.md``. Plan: ``.claude/plan-visibility-model.md``.

**Everything you can see is an occurrence on exactly one canvas, and a hide
belongs to that occurrence, on that canvas.** A reader never chooses whose
hides apply: it asks about a node or an edge, and the occurrence's own canvas
decides. So the answer for one edge is the same whichever canvas asks:

- a node occurrence's canvas: :meth:`Visibility.canvas_of_node`
  (``scope_filter.node_scope``, plus a placement's parent canvas);
- an edge's canvas: the canvas of its ends (:meth:`Visibility.canvases_of_edge`);
- a connection traced through submodule ports has no canvas of its own: it is
  live when every segment is live, and a hide of the whole connection
  (adoption retires rewired connections on the function node's canvas) counts
  on the canvas of either of its ends (:meth:`Visibility.connection_hidden`);
- a hidden Parameter value belongs to the Parameter occurrence that feeds the
  argument (:meth:`Visibility.hidden_values_for_node`).

Hypothesis canvases share nothing (a duplicate copies hides when it is made).
A submodule's inside is one canvas every placement shares, so a hide inside it
applies to every placement (user decision D2, 2026-11-19). The lines into and
out of a placement's ports are occurrences on the parent canvas.

Hidden combos are the one exception, global by design: a combo is a call_id,
the same computation on every canvas (``hide_combo``). They are part of every
canvas's hidden node ids, as they always were.

Before this module, about fifteen readers each picked "whose hides" for
themselves (the asking canvas with a ``strip_placement`` fallback, every
canvas unioned, the function node's canvas), and a hide inside a submodule cut
the parent's lines into its ports (scistack-debug.log 2026-11-19 12:38:43, "no
runnable MATLAB step").
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from scistack_gui.ids import ROOT_SCOPE, parse_placement_id, strip_placement

logger = logging.getLogger(__name__)

#: Node types whose stored row is ONE occurrence: its id is never drawn on
#: another canvas, so the row's canvas is definite. Variables, Parameters,
#: PathInputs and Merge nodes are shared entities: a bare id can be drawn on
#: several canvases (as ``id::canvas``), so a bare end of theirs only says
#: which canvas when nothing else does.
_ONE_OCCURRENCE_TYPES = frozenset(
    {"functionNode", "glueNode", "inputPortNode", "outputPortNode", "pipelineNode"}
)

#: Problems already reported (the view is loaded many times per build).
_REPORTED: set[str] = set()


def _warn_once(key: str, message: str, *args) -> None:
    if key not in _REPORTED:
        _REPORTED.add(key)
        logger.warning(message, *args)


@dataclass(frozen=True)
class Visibility:
    """Every hide on every canvas, loaded once (:meth:`load`)."""

    #: canvas -> bare node ids hidden there.
    node_hides: dict
    #: canvas -> hidden connection ids (and own ids of port-less hides).
    edge_hides: dict
    #: canvas -> {declared Parameter name: {hidden values}}.
    value_hides: dict
    #: Hidden combos: global by design (a call_id is one computation everywhere).
    combos: frozenset
    manual_nodes: dict
    positions_by_scope: dict
    #: use_id -> the canvas the placement is drawn on.
    use_parent: dict
    #: canvas -> {connection id: [(source, target) the hide recorded]}. Which
    #: OCCURRENCES a whole-connection hide names (:meth:`connection_hidden`).
    edge_hide_ends: dict = field(default_factory=dict)
    _canvas_cache: dict = field(default_factory=dict, compare=False, repr=False)

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    @classmethod
    def load(cls, db, manual_nodes: "dict | None" = None) -> "Visibility":
        """Every hide statement, grouped by the canvas it belongs to."""
        from scistack_gui.stores import intent_store
        from scistack_gui.stores import layout as layout_store
        from scistack_gui.stores import pipeline_store

        pipeline_store._ensure_tables(db)
        if manual_nodes is None:
            manual_nodes = pipeline_store.get_manual_nodes(db)
        hides = intent_store.hides_by_scope(db)
        node_hides: dict[str, set] = {}
        for scope, node_id in hides["nodes"]:
            canvas = scope
            parsed = parse_placement_id(node_id)
            if parsed is not None and parsed[1] != scope:
                # The id names its own canvas; it wins over the statement's.
                _warn_once(
                    f"node-hide-scope:{scope}:{node_id}",
                    "[visibility] node hide %s was stated on canvas %s but names "
                    "canvas %s; it applies on %s",
                    node_id, scope, parsed[1], parsed[1],
                )
                canvas = parsed[1]
            node_hides.setdefault(canvas, set()).add(str(strip_placement(node_id)))
        edge_hides: dict[str, set] = {}
        edge_hide_ends: dict[str, dict[str, list]] = {}
        for scope, hide in hides["edges"]:
            connection = pipeline_store._hidden_connection_id(hide, manual_nodes)
            edge_hides.setdefault(scope, set()).add(connection)
            edge_hide_ends.setdefault(scope, {}).setdefault(connection, []).append(
                (hide.get("source") or "", hide.get("target") or "")
            )
        value_hides: dict[str, dict[str, set]] = {}
        for scope, name, value in hides["values"]:
            value_hides.setdefault(scope, {}).setdefault(name, set()).add(value)
        uses = pipeline_store.get_pipeline_uses(db)
        vis = cls(
            node_hides={k: frozenset(v) for k, v in node_hides.items()},
            edge_hides={k: frozenset(v) for k, v in edge_hides.items()},
            value_hides=value_hides,
            combos=frozenset(hides["combos"]),
            manual_nodes=manual_nodes,
            positions_by_scope=layout_store._positions_by_scope(db._duck),
            use_parent={u["use_id"]: u["parent_pipeline_id"] for u in uses},
            edge_hide_ends=edge_hide_ends,
        )
        logger.debug(
            "[visibility] loaded: node hides %s, edge hides %s, value hides %s, "
            "%d combo(s)",
            {k: len(v) for k, v in vis.node_hides.items()},
            {k: len(v) for k, v in vis.edge_hides.items()},
            {k: sum(len(s) for s in v.values()) for k, v in vis.value_hides.items()},
            len(vis.combos),
        )
        return vis

    # ------------------------------------------------------------------
    # Where an occurrence is
    # ------------------------------------------------------------------

    def canvas_of_node(self, node_id: str) -> str:
        """The canvas this node id is drawn on (``scope_filter.node_scope``,
        the one owner of membership; a placement is on its parent canvas)."""
        node_id = node_id or ""
        cached = self._canvas_cache.get(node_id)
        if cached is not None:
            return cached
        if node_id in self.use_parent:
            canvas = self.use_parent[node_id] or ROOT_SCOPE
        else:
            from scistack_gui.domain.scope_filter import node_scope

            canvas = node_scope(node_id, self.manual_nodes, self.positions_by_scope)
        self._canvas_cache[node_id] = canvas
        return canvas

    def _definite(self, end: str) -> bool:
        """Whether *end* alone names its canvas: a placed id, a placement, or a
        node type that is only ever one occurrence."""
        if parse_placement_id(end) is not None or end in self.use_parent:
            return True
        meta = self.manual_nodes.get(end) or {}
        return meta.get("type") in _ONE_OCCURRENCE_TYPES

    def canvases_of_edge(self, edge: dict) -> tuple[str, ...]:
        """The canvas an edge is on, as a 1-tuple; two canvases only for a
        stored row whose ends are on different canvases (a line left spanning
        a boundary, which binds nothing on its own: a hide on either counts).

        A shared entity's bare end (``var__X``) is drawn on several canvases,
        so the other end decides; with neither end definite, both ends' own
        canvases are used."""
        source = edge.get("source") or ""
        target = edge.get("target") or ""
        definite = [self.canvas_of_node(e) for e in (source, target) if self._definite(e)]
        if not definite:
            definite = [self.canvas_of_node(source), self.canvas_of_node(target)]
        canvases = tuple(dict.fromkeys(definite))
        if len(canvases) > 1:
            logger.debug(
                "[visibility] edge %s (%s -> %s) spans canvases %s; a hide on "
                "either applies",
                edge.get("id"), source, target, list(canvases),
            )
        return canvases

    # ------------------------------------------------------------------
    # Nodes
    # ------------------------------------------------------------------

    def node_hidden_on(self, canvas: str, node_id: str) -> bool:
        """Whether the occurrence of *node_id* on *canvas* is hidden. A placed
        id names its own canvas, which wins over *canvas*."""
        node_id = node_id or ""
        parsed = parse_placement_id(node_id)
        if parsed is not None:
            canvas = parsed[1]
        bare = str(strip_placement(node_id))
        if bare in self.combos or node_id in self.combos:
            return True
        return bare in self.node_hides.get(canvas, ())

    def node_hidden(self, node_id: str) -> bool:
        """Whether *node_id*, on its own canvas, is hidden."""
        return self.node_hidden_on(self.canvas_of_node(node_id), node_id)

    def hidden_node_ids(self, canvas: str) -> set[str]:
        """The bare ids hidden on *canvas*, plus the (global) hidden combos:
        what a reader that draws or runs exactly that canvas filters by."""
        return set(self.node_hides.get(canvas, ())) | set(self.combos)

    def hidden_node_ids_any_canvas(self) -> set[str]:
        """Every canvas's hidden ids, unioned. Only for a run that names no
        node, so has no canvas (the name-scoped fallback)."""
        out = set(self.combos)
        for ids_ in self.node_hides.values():
            out |= ids_
        return out

    def hidden_rows(self) -> set[str]:
        """Stored node rows hidden on their own canvas, plus hidden combos.
        For a question about rows themselves (may this node claim a wiring?),
        not about what one canvas draws."""
        out = {nid for nid in self.manual_nodes if self.node_hidden(nid)}
        return out | set(self.combos)

    # ------------------------------------------------------------------
    # Edges
    # ------------------------------------------------------------------

    def hidden_edge_ids(self, canvas: str) -> set[str]:
        """Connection ids hidden on *canvas*: what the build of that canvas
        matches its history edges against."""
        return set(self.edge_hides.get(canvas, ()))

    def edge_hidden(self, edge: dict) -> bool:
        """The edge itself is hidden on its canvas, by
        ``graph_builder.manual_edge_is_hidden`` (own id or connection), the one
        matching rule."""
        from scistack_gui.domain.graph_builder import manual_edge_is_hidden

        return any(
            manual_edge_is_hidden(edge, self.edge_hides.get(canvas, ()), self.manual_nodes)
            for canvas in self.canvases_of_edge(edge)
        )

    def hidden_end(self, edge: dict) -> "str | None":
        """An end of *edge* that is hidden on the edge's canvas, or None."""
        canvases = self.canvases_of_edge(edge)
        for end in (edge.get("source") or "", edge.get("target") or ""):
            if any(self.node_hidden_on(c, end) for c in canvases):
                return end
        return None

    def edge_live(self, edge: dict) -> bool:
        """Neither the edge nor an end of it is hidden on its canvas."""
        return not self.edge_hidden(edge) and self.hidden_end(edge) is None

    def connection_hidden(self, connection: str, source: str, target: str) -> bool:
        """A connection traced through ports (``source`` -> ``target``), hidden
        as a whole: a hide of *connection* on either end's canvas that names
        the SAME occurrences, i.e. its recorded ends are on the same canvases
        as *source* and *target*. Adoption's retired connections are recorded
        with exactly the traced ends (``retire_rewired_edges``).

        A connection id says what the ends are, not which occurrence, so a
        removed direct line inside a submodule (from the copy of a PathInput
        on the submodule's canvas) has the SAME id as the live route from the
        parent's PathInput through the port. Matching by id alone dropped that
        route and turned every step it fed red (scistack-debug.log 2026-11-19
        13:55, load_filter_emg). A hide with no recorded ends can't be placed,
        so it matches by id, as before.
        """
        source_canvas = self.canvas_of_node(source)
        target_canvas = self.canvas_of_node(target)
        for canvas in dict.fromkeys((source_canvas, target_canvas)):
            if connection not in self.edge_hides.get(canvas, ()):
                continue
            ends = self.edge_hide_ends.get(canvas, {}).get(connection) or [("", "")]
            for hide_source, hide_target in ends:
                if not (hide_source and hide_target):
                    return True
                if (
                    self.canvas_of_node(hide_source) == source_canvas
                    and self.canvas_of_node(hide_target) == target_canvas
                ):
                    return True
            logger.debug(
                "[visibility] connection %s (%s -> %s) is hidden on %s only as "
                "another occurrence's line %s; the traced route stays",
                connection, source, target, canvas, ends[:3],
            )
        return False

    def hidden_port_ids(self) -> set[str]:
        """Input/output nodes hidden on their own canvas: removed ports."""
        from scistack_gui.domain.ports import PORT_NODE_TYPES

        return {
            nid
            for nid, meta in self.manual_nodes.items()
            if meta.get("type") in PORT_NODE_TYPES and self.node_hidden(nid)
        }

    # ------------------------------------------------------------------
    # Parameter values
    # ------------------------------------------------------------------

    def hidden_values_on(self, canvas: str) -> dict[str, set[str]]:
        """{declared Parameter name: hidden values} stated on *canvas*."""
        return {k: set(v) for k, v in self.value_hides.get(canvas, {}).items()}

    def hidden_values_any_canvas(self) -> dict[str, set[str]]:
        """Every canvas's hidden values, unioned. Only for a run that names no
        node, so has no canvas (the name-scoped fallback)."""
        out: dict[str, set[str]] = {}
        for by_name in self.value_hides.values():
            for name, values in by_name.items():
                out.setdefault(name, set()).update(values)
        return out

    def hidden_values_for_node(self, node_id: str, wiring_edges) -> dict[str, set[str]]:
        """Hidden values for the run of one function node: each Parameter's
        hides come from the canvas of the Parameter occurrence that feeds it
        (*wiring_edges* is ``EdgeView.wiring_list``, traced through ports).
        A Parameter no edge feeds (a recorded constant) takes the node's own
        canvas."""
        from scistack_gui.domain.graph_builder import endpoint_ref

        own = self.canvas_of_node(node_id)
        out = self.hidden_values_on(own)
        bare = str(strip_placement(node_id))
        from_elsewhere: dict[str, str] = {}
        for edge in wiring_edges:
            target = edge.get("target") or ""
            if target != node_id and str(strip_placement(target)) != bare:
                continue
            source = edge.get("source") or ""
            ref = endpoint_ref(source, self.manual_nodes)
            if not ref.startswith("param:"):
                continue
            name = ref[len("param:"):]
            canvas = self.canvas_of_node(source)
            if canvas == own:
                continue
            values = self.value_hides.get(canvas, {}).get(name, set())
            out[name] = set(values)
            from_elsewhere[name] = canvas
        if from_elsewhere:
            logger.debug(
                "[visibility] %s: Parameter value hides read from the feeding "
                "Parameter's canvas: %s",
                node_id, from_elsewhere,
            )
        return out
