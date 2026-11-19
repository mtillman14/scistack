"""The edges a scope's canvas draws, as one value every reader uses.

Step 1 of the unified edge model (.claude/plan-unified-edge-model.md,
D-2026-10-01-1). Before this, each reader (graph build, run targets, the
"why can't this run" check, the pipeline-run skip report, the MATLAB command)
loaded the raw stored manual edges and hidden edge ids itself and filtered
them its own way. They disagreed five times on 2026-10-01 alone. The run
path also unioned every scope's hidden edges, so an edge hidden in one
hypothesis tab disconnected the node in another.

``effective_edges(db, scope)`` is now the ONE place a reader gets:

- ``drawn``: the stored manual edges that are live, each judged on ITS OWN
  canvas (``domain.visibility``: neither the edge nor an end hidden there).
  This does not depend on *scope*: a hide inside a submodule never cuts the
  parent's lines into its ports, whichever canvas asks
  (docs/claude/visibility-model.md);
- ``hidden_edge_ids``: the connections hidden on *scope* (what its history
  edges are matched against), plus connections severed at a removed segment;
- ``manual_nodes``;
- ``stale_port``: edges left out of ``drawn`` because their target port is
  no longer an argument of the node's function (:func:`missing_port`);
- ``wiring_list``: the same connections with submodule ports traced through
  (``domain.ports``). ``drawn`` is what a canvas draws; ``wiring_list`` is what
  a node is wired to. They are the same list when no canvas has a port.

Only this module and the edge MUTATORS (layout_service, scope_service,
layout.py, pipeline_store) may call ``pipeline_store.get_manual_edges`` or
``get_hidden_edge_ids``. ``tests/test_edge_view_guard.py`` enforces that.

``scope=None`` (no asking canvas: the name-scoped run fallback, and readers
that want every canvas's edges) affects only ``hidden_edge_ids``, which is
then every canvas's hidden connections.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from scistack_gui.domain import ports
from scistack_gui.domain.ports import Wiring
from scistack_gui.domain.visibility import Visibility
from scistack_gui.ids import PARAM_ID_PREFIX

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EdgeView:
    scope: "str | None"
    drawn: tuple
    hidden_edge_ids: frozenset
    manual_nodes: dict = field(default_factory=dict)
    #: Stored, not-hidden edges set aside because their target port is no
    #: longer an argument of the node's function (:func:`missing_port`).
    #: Refresh Code hides them (layout_service.disconnect_removed_arguments).
    stale_port: tuple = ()
    #: Connections traced through submodule ports (``domain.ports.Wiring``),
    #: or ``None`` when no canvas has an input/output node.
    port_wiring: "Wiring | None" = None
    #: The hides this view was judged by (``domain.visibility.Visibility``).
    visibility: "Visibility | None" = field(default=None, compare=False, repr=False)

    @property
    def drawn_list(self) -> list[dict]:
        """``drawn`` as the list the domain functions take. What a CANVAS
        draws: a connection crossing a submodule is two edges here. A reader
        asking what a node is wired to wants :attr:`wiring_list`."""
        return list(self.drawn)

    @property
    def wiring_list(self) -> list[dict]:
        """Every connection, with submodule ports traced through: the edges a
        canvas with no submodules would store. With no port on any canvas it
        is exactly :attr:`drawn_list`."""
        return self.port_wiring.edges if self.port_wiring else list(self.drawn)

    def open_arguments(self, node_id: str) -> list[str]:
        """Arguments of *node_id* fed by a port with nothing connected."""
        if self.port_wiring is None:
            return []
        return self.port_wiring.open_arguments(node_id)


def effective_edges(db, scope: "str | None", *, caller: str) -> EdgeView:
    """The edges *scope*'s canvas draws: see the module docstring.

    *caller* names the reader in the INFO line, so a run's view can be matched
    against the canvas's in scistack.log.
    """
    from scistack_gui.stores import pipeline_store

    stored = pipeline_store.get_manual_edges(db)
    manual_nodes = pipeline_store.get_manual_nodes(db)
    vis = Visibility.load(db, manual_nodes)
    # What THIS canvas's history edges are matched against. With no asking
    # canvas, every canvas's hidden connections.
    if scope is not None:
        hidden = frozenset(vis.hidden_edge_ids(scope))
    else:
        hidden = frozenset().union(*vis.edge_hides.values()) if vis.edge_hides else frozenset()
    signatures: dict = {}
    hidden_edge_rows: list[dict] = []
    # An edge into or out of a HIDDEN node is not drawn and binds nothing
    # (canvas-as-document, 2026-10-22): hiding a PathInput and drawing another
    # onto the same port must not run an EachOf of both. Judged on the EDGE's
    # canvas (domain.visibility), never the asking canvas's.
    on_hidden_node: list[tuple[str, str]] = []
    stale_port: list[dict] = []
    drawn_list = []
    for e in stored:
        if vis.edge_hidden(e):
            hidden_edge_rows.append(e)
            continue
        if _on_retired_port(e):
            continue
        if _on_missing_port(e, manual_nodes, signatures):
            stale_port.append(e)
            continue
        hidden_end = vis.hidden_end(e)
        if hidden_end is not None:
            on_hidden_node.append((e.get("id"), hidden_end))
            continue
        drawn_list.append(e)
    _log_left_out(caller, hidden_edge_rows, on_hidden_node, vis)
    drawn = tuple(drawn_list)
    port_wiring, severed = _trace_ports(db, stored, drawn_list, manual_nodes, vis)
    if severed:
        hidden = hidden | severed
        from scistack_gui.domain.graph_builder import edge_connection_id

        # A severed connection runs THROUGH a removed port, so no edge drawn
        # directly on one canvas should share its id. One that does is two
        # different wires sharing an id (2026-11-19: glue nodes were keyed by
        # their function, so the fresh line vanished from the build).
        for e in drawn_list:
            if edge_connection_id(e, manual_nodes) in severed:
                _warn_once(
                    f"severed-drawn:{e.get('id')}",
                    "[edge_view] drawn edge %s (%s -> %s) shares its connection "
                    "id %s with a connection severed at a removed port; the "
                    "graph build will leave it out",
                    e.get("id"),
                    e.get("source"),
                    e.get("target"),
                    edge_connection_id(e, manual_nodes),
                )
    logger.info(
        "[edge_view] %s: scope=%s, %d drawn of %d stored manual edge(s), "
        "%d hidden edge id(s), %d edge(s) on a hidden node, %d on a port that "
        "is no longer an argument",
        caller,
        scope if scope is not None else "(all scopes)",
        len(drawn),
        len(stored),
        len(hidden),
        len(on_hidden_node),
        len(stale_port),
    )
    return EdgeView(
        scope=scope,
        drawn=drawn,
        hidden_edge_ids=hidden,
        manual_nodes=manual_nodes,
        stale_port=tuple(stale_port),
        port_wiring=port_wiring,
        visibility=vis,
    )


def _log_left_out(caller: str, hidden_rows: list, on_hidden_node: list, vis: Visibility) -> None:
    """DEBUG: which stored edges a hide left out, and on which canvas."""
    if not (hidden_rows or on_hidden_node) or not logger.isEnabledFor(logging.DEBUG):
        return
    logger.debug(
        "[edge_view] %s: %d stored edge(s) hidden on their canvas %s; %d on a "
        "node hidden on the edge's canvas %s",
        caller,
        len(hidden_rows),
        [(e.get("id"), list(vis.canvases_of_edge(e))) for e in hidden_rows[:20]],
        len(on_hidden_node),
        [
            (edge_id, end, vis.canvas_of_node(end))
            for edge_id, end in on_hidden_node[:20]
        ],
    )


def _trace_ports(
    db, stored: list, drawn: list, manual_nodes: dict, vis: Visibility
) -> "tuple[Wiring | None, frozenset]":
    """``(domain.ports.Wiring, severed connection ids)``, or ``(None, empty)``
    when no canvas has an input/output node.

    A segment (an edge touching an input/output node or a placement) lives on
    exactly one canvas, and its hides are that canvas's (``domain.visibility``),
    so a run of a node inside a submodule sees an outer edge removed on the
    parent, and a node hidden inside never cuts the parent's segment. Each
    hidden segment also hides the connection it carried (*severed*), which is
    what disconnects a node that has recorded history: removing the line
    ``[submodule] -> C`` removes ``B -> C``.
    """
    from scistack_gui.domain.graph_builder import edge_connection_id
    from scistack_gui.stores import pipeline_store

    if not ports.has_port_nodes(manual_nodes):
        return None, frozenset()
    uses = pipeline_store.get_pipeline_uses(db)
    wiring = trace_through_ports(db, drawn, manual_nodes, uses, vis)
    is_segment = _segment_test(manual_nodes, uses)
    drawn_ids = {e.get("id") for e in drawn}
    unhidden = list(drawn) + [
        e for e in stored if e.get("id") not in drawn_ids and is_segment(e)
    ]
    possible = ports.resolve_wiring(unhidden, manual_nodes, uses)
    traced = {
        edge_connection_id(w.edge, manual_nodes): w for w in wiring.wires if w.traced
    }
    severed = frozenset(
        edge_connection_id(w.edge, manual_nodes) for w in possible.wires if w.traced
    ) - set(traced)
    # A traced connection hidden as a whole (adoption retires a rewired node's
    # old connections on the node's canvas) counts on either end's canvas,
    # never the asking canvas's.
    kept = tuple(
        w
        for w in wiring.wires
        if not w.traced
        or not vis.connection_hidden(
            edge_connection_id(w.edge, manual_nodes),
            w.edge.get("source") or "",
            w.edge.get("target") or "",
        )
    )
    if len(kept) != len(wiring.wires):
        logger.debug(
            "[edge_view] %d traced connection(s) hidden as a whole: %s",
            len(wiring.wires) - len(kept),
            [
                edge_connection_id(w.edge, manual_nodes)
                for w in wiring.wires
                if w not in kept
            ][:20],
        )
        wiring.wires = kept
    for source, target, reason, via in wiring.illegal:
        _warn_once(
            f"illegal:{source}:{target}",
            "[edge_view] connection %s -> %s through ports binds nothing: %s "
            "(edges %s)",
            source,
            target,
            reason,
            list(via),
        )
    for edge in wiring.dangling:
        _warn_once(
            f"dangling:{edge.get('id')}",
            "[edge_view] edge %s (%s -> %s, port %s) names a port that does not "
            "exist; binds nothing",
            edge.get("id"),
            edge.get("source"),
            edge.get("target"),
            edge.get("targetHandle"),
        )
    logger.info(
        "[edge_view] ports: %d connection(s) (%d traced), %d severed, %d open "
        "input(s), %d dangling, %d illegal",
        len(wiring.wires),
        sum(1 for w in wiring.wires if w.traced),
        len(severed),
        len(wiring.open_inputs),
        len(wiring.dangling),
        len(wiring.illegal),
    )
    return wiring, severed


def _segment_test(manual_nodes: dict, uses: list):
    """``is_segment(edge)``: whether *edge* touches an input/output node or a
    placement, i.e. is one half of a connection through a port."""
    use_ids = {u["use_id"] for u in uses}

    def is_segment(edge: dict) -> bool:
        return any(
            end in use_ids or (manual_nodes.get(end) or {}).get("type") in ports.PORT_NODE_TYPES
            for end in (edge.get("source") or "", edge.get("target") or "")
        )

    return is_segment


def trace_through_ports(
    db, edges, manual_nodes: dict, uses: list, vis: "Visibility | None" = None
) -> Wiring:
    """THE trace of *edges* through submodule ports, as every reader must see
    it: the canvas (via :func:`_trace_ports`) and put_edge's legality check.

    A segment lives on exactly one canvas, and only that canvas's hides apply
    to it (``domain.visibility``), whichever canvas is asking. A caller's edge
    list may hold segments that are not live (put_edge adds its own edge to a
    view), so they are judged again here, and a removed port (an input/output
    node hidden on its own canvas) ends the trace. Before this was the one
    owner, put_edge traced the raw view and refused a connection through a
    segment the user had already removed (scistack-debug.log 2026-11-18 11:41,
    GitHub #24).
    """
    if vis is None:
        vis = Visibility.load(db, manual_nodes)
    is_segment = _segment_test(manual_nodes, uses)
    live = []
    removed = []
    for e in edges:
        if is_segment(e) and not vis.edge_live(e):
            removed.append(e.get("id"))
        else:
            live.append(e)
    if removed:
        logger.debug(
            "[edge_view] trace_through_ports: %d removed segment(s) left out: %s",
            len(removed),
            removed[:20],
        )
    return ports.resolve_wiring(live, manual_nodes, uses, vis.hidden_port_ids())


#: Messages already logged by :func:`_warn_once`.
_REPORTED_PORT_PROBLEMS: set[str] = set()


def _warn_once(key: str, message: str, *args) -> None:
    """Once per process: the view is read many times per graph build."""
    if key not in _REPORTED_PORT_PROBLEMS:
        _REPORTED_PORT_PROBLEMS.add(key)
        logger.warning(message, *args)


#: The Parameter port, retired 2026-10-01 (step 2): every input port is in__X.
#: It reused the Parameter node-id prefix, so ids is still the one spelling.
_RETIRED_PORT_PREFIX = PARAM_ID_PREFIX

#: Stored edge ids already reported by _on_retired_port, so the WARN is once per
#: process rather than once per graph build.
_REPORTED_RETIRED: set[str] = set()


def _on_retired_port(edge: dict) -> bool:
    """A stored edge drawn onto a `param__X` port, which no node renders since
    step 2. Clean break (beta rule, no migration): it is not drawn and binds
    nothing. The user redraws it onto in__X. History's own Parameter edges
    moved to in__X with the same ids, so a run that recorded the Parameter
    still shows it."""
    handle = edge.get("targetHandle") or edge.get("target_handle") or ""
    if not handle.startswith(_RETIRED_PORT_PREFIX):
        return False
    if edge.get("id") not in _REPORTED_RETIRED:
        _REPORTED_RETIRED.add(edge.get("id"))
        logger.warning(
            "[edge_view] stored edge %s (%s -> %s) targets the retired port %r; "
            "not drawn and binds nothing. Redraw it onto the in__ port.",
            edge.get("id"),
            edge.get("source"),
            edge.get("target"),
            handle,
        )
    return True


#: Stored edge ids already reported by _on_missing_port (once per process).
_REPORTED_MISSING_PORT: set[str] = set()

#: The node types whose ``in__X`` ports are their function's arguments.
_SIGNATURE_NODE_TYPES = ("functionNode", "glueNode")


def missing_port(
    edge: dict, manual_nodes: dict, signatures: dict
) -> "tuple[str, list[str]] | None":
    """THE rule for "this edge's target port is no longer an argument".

    A stored edge into a function or glue node's ``in__X`` where the node's
    function no longer takes ``X`` returns ``(function label, its current
    argument names)``; any other edge returns ``None``.

    Renaming an argument removes its port, so the canvas cannot draw the edge
    and the user cannot select it to delete it, yet a stored row used to keep
    binding it: glue 2026-10-21 (``in__value`` beside the live
    ``in__filepath``), function 2026-10-27 (``in__df_in`` after renaming to
    ``data_path`` -> "unexpected keyword argument 'df_in'"). Two readers use
    this: :func:`effective_edges` (not drawn, binds nothing) and Refresh Code's
    ``layout_service.disconnect_removed_arguments`` (hides the edge, so the
    user redraws it).

    *signatures* memoizes ``{label: arguments}`` for one pass. An edge is kept
    when nothing is known to be stale (:func:`removed_argument`).
    """
    from scistack_gui.ids import IN_HANDLE_PREFIX

    target = edge.get("target") or ""
    handle = edge.get("targetHandle") or edge.get("target_handle") or ""
    if not handle.startswith(IN_HANDLE_PREFIX):
        return None
    return removed_argument(
        target, handle[len(IN_HANDLE_PREFIX):], manual_nodes, signatures
    )


def removed_argument(
    node_id: str, port: str, manual_nodes: dict, signatures: dict
) -> "tuple[str, list[str]] | None":
    """The port form of :func:`missing_port`: is argument *port* of node
    *node_id* gone from its function? ``(function label, its current argument
    names)`` when it is, else ``None``.

    The second reader is the disconnected-port check
    (``execution_service.document_disconnected_ports``): a HIDDEN edge on a
    removed argument is not a disconnected input, there is no input left to
    reconnect. Before 2026-10-27 it was, so renaming ``df_in`` to
    ``data_path`` and deleting the old edge left the node refusing every run
    with "input 'df_in' is disconnected".

    Nothing is known to be stale, so ``None``, when the node is not a function
    or glue node, the signature cannot be read (no arguments: MATLAB not
    scanned, an import error) or the function takes ``**kwargs``, which
    accepts any name.
    """
    import inspect

    from scistack_gui.ids import strip_placement

    meta = manual_nodes.get(node_id) or manual_nodes.get(strip_placement(node_id))
    if not meta or meta.get("type") not in _SIGNATURE_NODE_TYPES:
        return None
    label = meta.get("label") or strip_placement(node_id)
    if label not in signatures:
        from scistack_gui.services.graph_service import fn_arguments

        signatures[label] = fn_arguments(label)
    arguments = signatures[label]
    if not arguments or any(
        a.kind == inspect.Parameter.VAR_KEYWORD for a in arguments
    ):
        return None
    params = [a.name for a in arguments if not a.private]
    if port in params:
        return None
    return label, params


def _on_missing_port(edge: dict, manual_nodes: dict, signatures: dict) -> bool:
    """:func:`missing_port`, warned once per edge: not drawn, binds nothing."""
    stale = missing_port(edge, manual_nodes, signatures)
    if stale is None:
        return False
    if edge.get("id") not in _REPORTED_MISSING_PORT:
        _REPORTED_MISSING_PORT.add(edge.get("id"))
        label, params = stale
        logger.warning(
            "[edge_view] stored edge %s (%s -> %s) targets port %r, but %r now "
            "takes %s; not drawn and binds nothing (the argument was renamed "
            "or removed). Wire the source to the current port.",
            edge.get("id"),
            edge.get("source"),
            edge.get("target"),
            edge.get("targetHandle") or edge.get("target_handle"),
            label,
            params,
        )
    return True


def run_scope(db, node_id: "str | None") -> "str | None":
    """The canvas a run asks from: the clicked node's canvas
    (``intent_store.scope_of_node``), or ``None`` when no node is named. It
    decides which history-edge hides the node's own wiring is matched against;
    stored edges are judged on their own canvases whatever it is
    (docs/claude/visibility-model.md)."""
    if not node_id:
        return None
    from scistack_gui.stores import intent_store

    return intent_store.scope_of_node(db, node_id)
