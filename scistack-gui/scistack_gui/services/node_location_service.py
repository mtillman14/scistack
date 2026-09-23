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


def _names_in(value) -> list[str]:
    """Variable type names in one ``input_types`` / ``output_type`` value.

    A parameter bound to several types (a multi-type input) arrives as a LIST,
    and the never-run fallback (``resolve_function_edges``) returns a list even
    for a single candidate — so both shapes have to be unwrapped or a
    freshly-placed node looks like it has no inputs at all.
    """
    values = value if isinstance(value, (list, tuple)) else [value]
    return [name for name in values if isinstance(name, str) and name]


def output_variables_for_node(db, node_id: str) -> list[str]:
    """Variable type names this node PRODUCES.

    The fallback for a node with no variable inputs — a loader reading files.
    Its locations are not the intersection of anything; they are wherever its
    own output exists or is expected, which is exactly what
    ``location_states`` answers for that variable (with the discovery basis for
    a PathInput loader, so un-loaded files show as red rather than being
    invisible).
    """
    from scistack_gui.services.execution_service import derive_target_for_node

    names: list[str] = []
    for target in derive_target_for_node(db, node_id):
        for name in _names_in(target.get("output_type")):
            if name not in names:
                names.append(name)
    return names


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

    targets = derive_target_for_node(db, node_id)
    names: list[str] = []
    for target in targets:
        for value in (target.get("input_types") or {}).values():
            for name in _names_in(value):
                if name not in names:
                    names.append(name)
    # The two empty cases have completely different causes and used to produce
    # the same blank pane: NO TARGETS means the canvas could not resolve this
    # node at all (a stale id, a hidden input edge), while targets with no
    # variable inputs is an ordinary loader. Say which, with what was actually
    # seen — guessing from a blank pane is what this line exists to stop.
    if not names:
        logger.info(
            "[node_locations] %s: %d target(s), no variable inputs among %s",
            node_id,
            len(targets),
            [sorted((t.get("input_types") or {})) for t in targets] or "nothing",
        )
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

    Three cases, in order — a LOADER is the common one and must not come back
    blank:

    1. **Variable inputs** — the inner join of their locations: where this node
       can run.
    2. **No variable inputs, but an output** — a loader reading files. Its
       locations are not an intersection of anything; they are wherever its own
       output exists or is expected, which is what ``location_states`` answers
       for that variable (with the discovery basis, so a file on disk that was
       never loaded shows red rather than being invisible).
    3. **Nothing resolvable** — the canvas cannot place this node at all. The
       note then carries the SAME explanation the Run button would give, since
       it is the same question: a hidden input edge, or a node that has never
       run and has nothing wired.
    """
    from scidb.locations import (
        intersect_location_states,
        location_states,
        prune_to_problems,
    )

    from scistack_gui.db import db_connection

    with db_connection("node_location_tree"):
        variables = input_variables_for_node(db, node_id)
        note: str | None = None

        if not variables:
            outputs = output_variables_for_node(db, node_id)
            if outputs:
                variables = outputs
                note = (
                    f"This node reads files rather than variables, so this is "
                    f"where its output ({', '.join(outputs)}) exists — red "
                    f"means a location the loader has not written yet."
                )
                logger.info(
                    "[node_locations] %s has no variable inputs; falling back "
                    "to its output(s) %s",
                    node_id,
                    outputs,
                )

        if not variables:
            return _unresolvable(db, node_id)

        if len(variables) == 1:
            tree = location_states(variables[0], variant=None, db=db)
        else:
            # variant=None: a node runs on whatever its inputs currently are. A
            # per-variable variant pin is the plotting layer's question.
            tree = intersect_location_states(variables, variant=None, db=db)
        if note:
            tree.notes.insert(0, note)
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


def _unresolvable(db, node_id: str) -> dict:
    """The empty payload for a node the canvas cannot resolve, explained.

    The explanation comes from ``disconnected_reason`` — the same sentence the
    Run button shows for the same node — rather than a second guess written
    here. When it has nothing to say, the node simply has no wiring yet, and
    the note says that instead of implying something is broken.
    """
    from scistack_gui.services.execution_service import disconnected_reason
    from scistack_gui.ids import FN_ID_PREFIX, parse_fn_node_id, strip_placement

    parsed = parse_fn_node_id(node_id)
    function_name = parsed[0] if parsed else strip_placement(node_id).removeprefix(FN_ID_PREFIX)
    reason = None
    try:
        reason = disconnected_reason(db, function_name, node_id)
    except Exception:  # pragma: no cover - diagnosis must not raise
        logger.debug("[node_locations] disconnected_reason failed", exc_info=True)

    if reason:
        note = f"This node cannot run: {reason}. Reconnect it to see its locations."
    else:
        note = (
            "Nothing is wired into this node yet, and it has produced nothing, "
            "so there are no schema locations to show. Connect its inputs (or "
            "run it once) and reopen this."
        )
    logger.info("[node_locations] %s is unresolvable: %s", node_id, note)
    return {
        "variable": "",
        "schema_keys": list(db.dataset_schema_keys),
        "variant": {},
        "counts": {"green": 0, "amber": 0, "red": 0, "grey": 0},
        "total": 0,
        "green": 0,
        "verdict": "grey",
        "basis": "present_only",
        "notes": [note],
        "roots": [],
        "selection": {},
    }
