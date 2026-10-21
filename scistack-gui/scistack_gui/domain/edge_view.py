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
- ``manual_nodes``.

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

    @property
    def drawn_list(self) -> list[dict]:
        """``drawn`` as the list the domain functions take."""
        return list(self.drawn)


def effective_edges(db, scope: "str | None", *, caller: str) -> EdgeView:
    """The edges *scope*'s canvas draws: see the module docstring.

    *caller* names the reader in the INFO line, so a run's view can be matched
    against the canvas's in scidb.log.
    """
    from scistack_gui import pipeline_store
    from scistack_gui.domain.graph_builder import visible_manual_edges

    stored = pipeline_store.get_manual_edges(db)
    manual_nodes = pipeline_store.get_manual_nodes(db)
    hidden = frozenset(pipeline_store.get_hidden_edge_ids(db, scope))
    glue_ports: dict[str, "list[str]"] = {}
    drawn = tuple(
        e for e in visible_manual_edges(stored, hidden, manual_nodes)
        if not _on_retired_port(e)
        and not _on_missing_glue_port(e, manual_nodes, glue_ports)
    )
    logger.info(
        "[edge_view] %s: scope=%s, %d drawn of %d stored manual edge(s), "
        "%d hidden edge id(s)",
        caller,
        scope if scope is not None else "(all scopes)",
        len(drawn),
        len(stored),
        len(hidden),
    )
    return EdgeView(
        scope=scope,
        drawn=drawn,
        hidden_edge_ids=hidden,
        manual_nodes=manual_nodes,
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


#: Stored edge ids already reported by _on_missing_glue_port (once per process).
_REPORTED_MISSING_GLUE_PORT: set[str] = set()


def _on_missing_glue_port(
    edge: dict, manual_nodes: dict, glue_ports: dict
) -> bool:
    """A stored edge into a glue node's ``in__X`` where the glue's function no
    longer has a parameter ``X``.

    Renaming the glue's argument (the new-glue body starts as ``value``)
    removes the port, so the canvas cannot draw the edge and the user cannot
    select it to delete it. It stayed in storage, though, and the glue chain
    followed the first incoming edge, which was this invisible one
    (2026-10-21: ``in__value`` beside the live ``in__filepath``). Not drawn
    and binds nothing, like a retired port. The stored row is left alone:
    renaming the argument back brings the edge back.

    *glue_ports* memoizes ``{glue label: params}`` for one view. A glue whose
    signature cannot be read (no params) keeps all its edges, since the
    canvas then renders a single unnamed handle and nothing is known to be
    stale.
    """
    from scistack_gui.domain.edge_resolver import GLUE_NODE_TYPE
    from scistack_gui.ids import IN_HANDLE_PREFIX, strip_placement

    target = edge.get("target") or ""
    meta = manual_nodes.get(target) or manual_nodes.get(strip_placement(target))
    if not meta or meta.get("type") != GLUE_NODE_TYPE:
        return False
    handle = edge.get("targetHandle") or edge.get("target_handle") or ""
    if not handle.startswith(IN_HANDLE_PREFIX):
        return False
    label = meta.get("label") or strip_placement(target)
    if label not in glue_ports:
        from scistack_gui.api.pipeline import _fn_params_from_registry

        glue_ports[label] = _fn_params_from_registry(label)
    params = glue_ports[label]
    port = handle[len(IN_HANDLE_PREFIX):]
    if not params or port in params:
        return False
    if edge.get("id") not in _REPORTED_MISSING_GLUE_PORT:
        _REPORTED_MISSING_GLUE_PORT.add(edge.get("id"))
        logger.warning(
            "[edge_view] stored edge %s (%s -> %s) targets glue port %r, but "
            "glue %r now takes %s; not drawn and binds nothing (the glue's "
            "argument was renamed). Wire the source to the current port.",
            edge.get("id"),
            edge.get("source"),
            target,
            handle,
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
