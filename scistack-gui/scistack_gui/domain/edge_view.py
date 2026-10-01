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
    drawn = tuple(visible_manual_edges(stored, hidden, manual_nodes))
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


def run_scope(db, node_id: "str | None") -> "str | None":
    """The scope a run's hides come from: the clicked node's canvas
    (``intent_store.scope_of_node``), or every scope when no node is named.
    Decision (a) of the plan, matching hidden nodes since 2026-09-20."""
    if not node_id:
        return None
    from scistack_gui import intent_store

    return intent_store.scope_of_node(db, node_id)
