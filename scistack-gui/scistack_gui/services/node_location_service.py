"""Per-location status for a FUNCTION NODE — the processing tab's picker data.

The plotting tab asks about one variable (``plot_service.location_tree``). A
function node has no single variable: it has inputs, and the locations it will
actually run are the ones EVERY input has. That inner join lives in scidb
(``locations.intersect_location_states``, see
docs/claude/schema-location-status.md §"The fifth question") — this module only
answers "which variables is this node fed by", which is a GUI-graph question
and nothing else's business.

Kept out of ``plot_service`` deliberately: nothing here is about plotting, and
folding it in would put a canvas concern behind a name that says otherwise.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def input_variables_for_node(db, node_id: str) -> list[str]:
    """Distinct input variable type names this node is wired to.

    Resolved through ``derive_target_for_node`` — by the exact node clicked,
    never by function NAME — because one name can have several independent
    wirings on a canvas, and resolving by name shows the other node's inputs
    (the failure ``derive_target_for_node`` exists to prevent, found in a real
    session).

    A parameter bound to several types (a multi-type input) contributes all of
    them: every one is an input the node can run on, so every one must have the
    location for the node to be runnable there.
    """
    from scistack_gui.services.execution_service import derive_target_for_node

    names: list[str] = []
    for target in derive_target_for_node(db, node_id):
        for value in (target.get("input_types") or {}).values():
            for name in value if isinstance(value, (list, tuple)) else [value]:
                if isinstance(name, str) and name and name not in names:
                    names.append(name)
    return names


def node_location_tree(
    db,
    node_id: str,
    *,
    problems_only: bool = False,
) -> dict:
    """The intersected location tree for one function node.

    Returns the same payload shape as ``plot_service.location_tree`` — the
    picker draws one component for both tabs, so a second shape here would be a
    second renderer.

    A node with no resolvable inputs (a PathInput-only loader, or one whose
    edges were deleted) returns an empty tree carrying a note rather than an
    error: the panel should open and say why it is empty.
    """
    from scidb.locations import intersect_location_states, prune_to_problems

    from scistack_gui.db import db_connection

    with db_connection("node_location_tree"):
        variables = input_variables_for_node(db, node_id)
        if not variables:
            logger.info(
                "[node_locations] %s has no resolvable input variables — "
                "empty tree",
                node_id,
            )
            return {
                "variable": "",
                "schema_keys": list(db.dataset_schema_keys),
                "variant": {},
                "counts": {"green": 0, "amber": 0, "red": 0, "grey": 0},
                "total": 0,
                "green": 0,
                "verdict": "grey",
                "basis": "present_only",
                "notes": [
                    "This node has no input variables the canvas can resolve — "
                    "a loader that reads files, or a node whose input edges "
                    "were removed. There are no saved locations to intersect, "
                    "so every combination of the schema keys is a candidate."
                ],
                "roots": [],
                "selection": {},
            }

        # variant=None: a node runs on whatever its inputs currently are. A
        # per-variable variant pin is the plotting layer's question.
        tree = intersect_location_states(variables, variant=None, db=db)
        if problems_only:
            tree = prune_to_problems(tree)

        payload = tree.to_dict()
        payload["selection"] = {}
        logger.info(
            "[node_locations] %s over %s: %d/%d green (amber=%d, red=%d, "
            "excluded=%d)",
            node_id,
            variables,
            payload["green"],
            payload["total"],
            payload["counts"].get("amber", 0),
            payload["counts"].get("red", 0),
            payload["counts"].get("grey", 0),
        )
        return payload
