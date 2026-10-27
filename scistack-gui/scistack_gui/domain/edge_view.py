"""The edges a scope's canvas draws, as one value every reader uses.

Step 1 of the unified edge model (.claude/plan-unified-edge-model.md,
D-2026-10-01-1). Before this, each reader (graph build, run targets, the
"why can't this run" check, the pipeline-run skip report, the MATLAB command)
loaded the raw stored manual edges and hidden edge ids itself and filtered
them its own way. They disagreed five times on 2026-10-01 alone. The run
path also unioned every scope's hidden edges, so an edge hidden in one
hypothesis tab disconnected the node in another.

``effective_edges(db, scope)`` is now the ONE place a reader gets:

- ``drawn``: the stored manual edges the canvas can draw
  (``graph_builder.visible_manual_edges``: own id not hidden, history twin not
  hidden);
- ``hidden_edge_ids``: the history edges hidden in *scope*;
- ``manual_nodes``;
- ``stale_port``: edges left out of ``drawn`` because their target port is
  no longer an argument of the node's function (:func:`missing_port`).

Only this module and the edge MUTATORS (layout_service, scope_service,
layout.py, pipeline_store) may call ``pipeline_store.get_manual_edges`` or
``get_hidden_edge_ids``. ``tests/test_edge_view_guard.py`` enforces that.

``scope=None`` means every scope unioned. That is used only by the name-scoped
run fallback (a run request with no node), the same rule hidden NODES follow
(``pipeline_store.get_hidden_node_ids``).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

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

    @property
    def drawn_list(self) -> list[dict]:
        """``drawn`` as the list the domain functions take."""
        return list(self.drawn)


def effective_edges(db, scope: "str | None", *, caller: str) -> EdgeView:
    """The edges *scope*'s canvas draws: see the module docstring.

    *caller* names the reader in the INFO line, so a run's view can be matched
    against the canvas's in scistack.log.
    """
    from scistack_gui import pipeline_store
    from scistack_gui.domain.graph_builder import visible_manual_edges

    from scistack_gui.ids import strip_placement

    stored = pipeline_store.get_manual_edges(db)
    manual_nodes = pipeline_store.get_manual_nodes(db)
    hidden = frozenset(pipeline_store.get_hidden_edge_ids(db, scope))
    # An edge into or out of a HIDDEN node is not drawn and binds nothing
    # (canvas-as-document, 2026-10-22). Before adoption, a hidden node's
    # edges were history edges and vanished with it; now they are stored
    # rows, so the hide has to reach them here, the one view every reader
    # uses. Otherwise hiding a PathInput and drawing another onto the same
    # port ran an EachOf of both.
    hidden_nodes = pipeline_store.get_hidden_node_ids(db, scope)
    signatures: dict = {}
    on_hidden_node: list[str] = []
    stale_port: list[dict] = []
    drawn_list = []
    for e in visible_manual_edges(stored, hidden, manual_nodes):
        if _on_retired_port(e):
            continue
        if _on_missing_port(e, manual_nodes, signatures):
            stale_port.append(e)
            continue
        ends = {e.get("source") or "", e.get("target") or ""}
        if hidden_nodes and any(
            end in hidden_nodes or strip_placement(end) in hidden_nodes for end in ends
        ):
            on_hidden_node.append(e.get("id"))
            continue
        drawn_list.append(e)
    drawn = tuple(drawn_list)
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
    )


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
        from scistack_gui.api.pipeline import _fn_arguments

        signatures[label] = _fn_arguments(label)
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
    """The scope a run's hides come from: the clicked node's canvas
    (``intent_store.scope_of_node``), or every scope when no node is named.
    Decision (a) of the plan, matching hidden nodes since 2026-09-20."""
    if not node_id:
        return None
    from scistack_gui import intent_store

    return intent_store.scope_of_node(db, node_id)
