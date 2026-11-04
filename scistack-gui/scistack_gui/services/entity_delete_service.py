"""
The palette's Delete for Variables, Parameters and PathInputs
(``.claude/plan-palette-order-and-delete.md``).

Delete removes the entity's DECLARATION from the TOML entities file and
nothing else: its records stay in the database (declaring the name again
brings them back), and its palette note is kept. It is a deliberate,
user-requested exception (2026-11-03) to "never delete, mark hidden",
scoped to entities-file declarations.

Three rules, each with one owner:

* **Only the entities file is written** --
  ``target_file_service.entity_editability`` (via ``remove_declaration``).
* **Nothing on a canvas changes behind the user's back** -- a delete is
  refused while the entity is on any pipeline's canvas, hidden pipelines
  included (restoring one must not reveal a node whose declaration vanished).
  "On a canvas" is what the canvas SHOWS: the resolved graph from
  ``pipeline_service.get_pipeline_graph``, so a node that is there only
  because of run history counts, and a node the user hid does not.
* **Undo** is the generic history (``scistack_gui/history``): the
  ``delete_entity`` handler is ``undoable`` and the file write snapshots
  the bytes first, so Undo restores the file exactly. No second mechanism.
"""

from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

#: Palette kind -> the React Flow node type that kind is drawn as.
NODE_TYPE_BY_KIND = {
    "variable": "variableNode",
    "parameter": "parameterNode",
    "path_input": "pathInputNode",
}


def entity_placements(kind: str, name: str) -> list[dict]:
    """Every pipeline whose canvas shows *name* as a *kind* node:
    ``[{pipeline_id, pipeline_name, hidden_pipeline, node_ids}]``, root first.

    One graph build per pipeline -- this is asked only by Delete, which is
    rare, and the build is the one owner of what a canvas shows. Timed and
    logged so a slow check is visible.
    """
    from scistack_gui.runtime.db import get_db, is_loaded
    from scistack_gui.services.pipeline_service import get_pipeline_graph
    from scistack_gui.stores import pipeline_store as ps

    node_type = NODE_TYPE_BY_KIND.get(kind)
    if node_type is None:
        raise ValueError(f"Unknown entity kind {kind!r}")
    if not is_loaded():
        logger.info("[entity_delete] entity_placements: no database open; none")
        return []

    db = get_db()
    t0 = time.perf_counter()
    out: list[dict] = []
    pipelines = ps.list_all_pipelines(db)
    for p in pipelines:
        graph = get_pipeline_graph(db, p["pipeline_id"], run_states=False)
        node_ids = [
            n["id"]
            for n in graph.get("nodes", [])
            if n.get("type") == node_type
            and (n.get("data") or {}).get("label") == name
        ]
        if node_ids:
            out.append(
                {
                    "pipeline_id": p["pipeline_id"],
                    "pipeline_name": p["name"],
                    "hidden_pipeline": bool(p.get("hidden")),
                    "node_ids": node_ids,
                }
            )
    logger.info(
        "[entity_delete] entity_placements: %s '%s' on %d of %d pipeline(s) "
        "(%.0f ms): %s",
        kind,
        name,
        len(out),
        len(pipelines),
        (time.perf_counter() - t0) * 1000.0,
        [(p["pipeline_name"], len(p["node_ids"])) for p in out],
    )
    return out


def delete_entity(kind: str, name: str, *, preview: bool = False) -> dict:
    """Delete *name*'s declaration, or (``preview=True``) say what Delete
    would remove without writing.

    Checks, in order -- the first refusal wins and nothing is written:

    1. declared in the entities file (``remove_declaration``'s preview);
    2. not on any canvas (``entity_placements``);
    3. the write itself (``remove_declaration``).

    Returns ``remove_declaration``'s dict, or a refusal
    ``{"ok": False, "reason": "placed", "placements": [...], "error"}``.
    """
    from scistack_gui.services.target_file_service import remove_declaration

    if kind not in NODE_TYPE_BY_KIND:
        return {"ok": False, "error": f"Cannot delete a {kind!r}.", "reason": "unsupported_kind"}

    logger.info("[entity_delete] delete_entity: %s '%s' preview=%s", kind, name, preview)

    check = remove_declaration(kind, name, preview=True)
    if not check.get("ok"):
        logger.info("[entity_delete] delete_entity: refused at declaration check: %s", check)
        return check

    placements = entity_placements(kind, name)
    if placements:
        where = ", ".join(
            p["pipeline_name"] + (" (hidden)" if p["hidden_pipeline"] else "")
            for p in placements
        )
        logger.info(
            "[entity_delete] delete_entity: refused, %s '%s' is on %d canvas(es): %s",
            kind,
            name,
            len(placements),
            where,
        )
        return {
            "ok": False,
            "reason": "placed",
            "placements": placements,
            "removed": check.get("removed"),
            "error": (
                f"'{name}' is still on the canvas of: {where}. Remove it from "
                f"there first, then delete."
            ),
        }

    if preview:
        return check

    result = remove_declaration(kind, name)
    logger.info(
        "[entity_delete] delete_entity: %s '%s' -> ok=%s reason=%s still_declared_in=%s",
        kind,
        name,
        result.get("ok"),
        result.get("reason"),
        result.get("still_declared_in"),
    )
    if result.get("ok"):
        from scistack_gui.services.layout_service import _notify_dag_updated

        _notify_dag_updated("delete_entity")
    return result
