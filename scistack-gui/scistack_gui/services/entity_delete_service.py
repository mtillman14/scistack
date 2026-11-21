"""
The sidebar's Delete (GitHub #26, ``.claude/plan-delete-entries-with-data.md``;
concepts in ``docs/claude/deleting-entries-with-data.md``).

Right-click any palette item → **Delete…** → a plan (dry run) → confirm. One
Delete removes, for every kind:

* the entity's **data and everything computed from it** (scidb
  ``variant_delete``: one transaction, saved to scidb's trash first);
* its **nodes on every canvas** (``layout_service.delete_layout``, the canvas's
  own node delete), whichever pipeline they are on;
* its **declaration**: the entities-file entry (``remove_declaration``), the
  chosen values of a Parameter (``update_parameter``), or a function's file
  when the GUI created it (functions_dir / glue_dir).

Being placed on a canvas never blocks a Delete (user, 2026-11-19; this
reverses the 2026-11-03 "refused while placed" rule). What the code does not
own is never touched: a function in the user's own ``.py``/``.m`` or a library
keeps its code, and an entity declared outside the entities file keeps its
declaration; the plan says so.

A **Submodule** Delete removes the pipeline's placements and hides the
pipeline (the store's user-facing removal); it deletes no data, because
records belong to functions, not pipelines.

**Undo** is one Ctrl Z step: the handler is ``undoable``, so the canvas rows
and the files come back through the generic history, and the data through
``history.note_trash`` (scidb's ``restore_deleted``).

**Ordering, and why nothing raises after the data delete.** A recording keeps
nothing when its handler raises, and the rows written before the raise stay
written. So every refusal is found by the plan first (stale entities file,
library-locked canvas, changed data). After the data delete, a failing step is
reported in ``warnings`` and the call still returns, so the record (data
included) is stored and Ctrl Z can undo all of it.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)

KINDS = ("variable", "parameter", "path_input", "function", "submodule")

#: Palette kind -> the React Flow node type(s) that kind is drawn as.
NODE_TYPES_BY_KIND = {
    "variable": ("variableNode",),
    "parameter": ("parameterNode",),
    "path_input": ("pathInputNode",),
    "function": ("functionNode", "glueNode"),
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

    node_types = NODE_TYPES_BY_KIND.get(kind)
    if node_types is None:
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
            if n.get("type") in node_types
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


# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------


def _jsonable(obj):
    from scistack_gui.services.variant_cards_service import _jsonable as owner

    return owner(obj)


def _data_targets(kind: str, name: str, values) -> list[dict]:
    """scidb ``variant_delete`` targets for this entity (scidb owns what they
    mean)."""
    if kind == "variable":
        return [{"variable": name}]
    if kind == "path_input":
        return [{"path_input": name}]
    if kind == "function":
        return [{"function": name}]
    if kind == "parameter":
        if values is None:
            return [{"parameter": name}]
        return [{"parameter": name, "value": v} for v in values]
    return []


def default_reason(kind: str, name: str, values=None) -> str:
    what = f"{name}={list(values)!r}" if values is not None else name
    return f"deleted from sidebar: {kind} {what}"


def _data_plan(db, targets: list[dict]) -> dict:
    from scidb.store.trash import trash_root
    from scidb.store.variant_delete import delete_plan

    if not targets:
        return {"total_records": 0, "by_variable": {}, "fingerprint": None, "undoable": True}
    plan = delete_plan(db, targets)
    payload = _jsonable(plan)
    payload.pop("record_ids", None)  # can be large; the counts are what is shown
    payload.pop("invocation_ids", None)
    payload.pop("run_ids", None)
    payload["total_records"] = plan.total_records
    payload["undoable"] = trash_root(db) is not None
    return payload


def _function_file_plan(name: str) -> dict:
    """What a function Delete does to its code: remove the file when the GUI
    created it and it defines only this function, otherwise nothing."""
    from scistack_gui.services import function_code_service, glue_service
    from scistack_gui.services.pipeline_service import get_function_source, get_registry

    src = get_function_source(name)
    if not src.get("ok"):
        return {
            "action": "none",
            "message": f"'{name}' has no source file of the project's (a library or built-in "
            "function); only its data and nodes are deleted.",
        }
    path = Path(src["file"])
    owned = glue_service._is_editable(path, function_code_service._functions_dir()) or (
        glue_service._is_editable(path, glue_service._glue_dir())
    )
    if not owned:
        return {
            "action": "none",
            "file": str(path),
            "line": src.get("line"),
            "message": f"'{name}' is your code in {path.name}; it is not touched, so the "
            "function stays in the palette. Only its data and nodes are deleted.",
        }
    sources = get_registry().get("function_sources") or {}
    sharing = sorted(
        other
        for other, info in sources.items()
        if other != name and info.get("file") and Path(info["file"]) == path
    )
    if sharing:
        return {
            "action": "none",
            "file": str(path),
            "message": f"{path.name} also defines {sharing}; the file is kept. Only "
            f"'{name}''s data and nodes are deleted.",
        }
    return {
        "action": "delete_file",
        "file": str(path),
        "message": f"{path.name} (created by SciStack) is deleted.",
    }


def _declaration_plan(kind: str, name: str, values) -> dict:
    """What happens to the declaration: ``{action, file?, removed?, message,
    blocked?}``. ``blocked`` refuses the whole Delete (a stale file)."""
    from scistack_gui.services.target_file_service import (
        entity_editability,
        remove_declaration,
    )

    if kind == "function":
        return _function_file_plan(name)
    if kind == "parameter" and values is not None:
        from scistack_gui.project import registry
        from scistack_gui.services.variant_cards_service import _same_value

        params = registry.get_parameters_registry()
        declared = list(params[name].values) if name in params else []
        remaining = [d for d in declared if not any(_same_value(d, v) for v in values)]
        if len(remaining) == len(declared):
            return {"action": "none", "message": "None of these values is declared in source."}
        edit = entity_editability("parameter", name)
        if not edit.get("editable"):
            return {
                "action": "none",
                "file": edit.get("file"),
                "line": edit.get("line"),
                "message": f"Declared in {edit.get('file')}; the values stay in source. Only "
                "their data is deleted.",
            }
        return {
            "action": "edit_values",
            "file": edit.get("file"),
            "remaining": remaining,
            "message": f"Removed from the declaration; {len(remaining)} value(s) remain.",
        }
    check = remove_declaration(kind, name, preview=True)
    if check.get("ok"):
        return {
            "action": "remove",
            "file": check.get("file"),
            "removed": check.get("removed"),
            "message": f"Removed from {Path(check['file']).name}.",
        }
    reason = check.get("reason")
    if reason == "unknown":
        return {"action": "none", "message": f"'{name}' is not declared anywhere (only run history)."}
    if reason in ("read_only", "unsupported_format"):
        return {
            "action": "none",
            "file": check.get("file"),
            "line": check.get("line"),
            "message": (check.get("error") or "")
            + " The declaration is kept; only its data and nodes are deleted.",
        }
    return {"action": "none", "blocked": check.get("error") or "The declaration cannot be edited.", "message": check.get("error")}


def _parameter_value_options(db, name: str) -> list[dict]:
    """``[{value, declared, has_data}]``: every value the dialog can tick.
    Declared values come from the registry (source), values with data from
    scidb (``variant_delete.parameter_values``, the delete target's own edge
    rule), so a ticked value means exactly what its delete target deletes."""
    from scidb.store.variant_delete import parameter_values

    from scistack_gui.project import registry
    from scistack_gui.services.variant_cards_service import _same_value

    params = registry.get_parameters_registry()
    declared = list(params[name].values) if name in params else []
    ran = parameter_values(db, name)
    options = [
        {"value": v, "declared": True, "has_data": any(_same_value(v, r) for r in ran)}
        for v in declared
    ]
    options += [
        {"value": r, "declared": False, "has_data": True}
        for r in ran
        if not any(_same_value(r, d) for d in declared)
    ]
    return _jsonable(options)


def _locked_nodes(db, placements: list[dict]) -> list[str]:
    from scistack_gui.stores import library_lock

    out = []
    for p in placements:
        for nid in p["node_ids"]:
            try:
                library_lock.check_node(db, nid, "deleting")
            except Exception as exc:  # the lock's refusal type is its own
                out.append(f"{p['pipeline_name']}: {exc}")
    return out


def _submodule_plan(db, pipeline_id: str) -> dict:
    from scistack_gui.stores import library_lock
    from scistack_gui.stores import pipeline_store as ps

    pipelines = {p["pipeline_id"]: p for p in ps.list_all_pipelines(db)}
    me = pipelines.get(pipeline_id)
    if me is None:
        return {"ok": False, "error": f"No pipeline {pipeline_id!r}.", "reason": "unknown"}
    uses = [u for u in ps.get_pipeline_uses(db) if u["child_pipeline_id"] == pipeline_id]
    placements: dict[str, dict] = {}
    blocked: list[str] = []
    for u in uses:
        parent = pipelines.get(u["parent_pipeline_id"], {})
        entry = placements.setdefault(
            u["parent_pipeline_id"],
            {
                "pipeline_id": u["parent_pipeline_id"],
                "pipeline_name": parent.get("name", u["parent_pipeline_id"]),
                "hidden_pipeline": bool(parent.get("hidden")),
                "node_ids": [],
            },
        )
        entry["node_ids"].append(u["use_id"])
        try:
            library_lock.check_scope(db, u["parent_pipeline_id"], "removing a submodule")
        except Exception as exc:
            blocked.append(f"{entry['pipeline_name']}: {exc}")
    visible = [p for p in pipelines.values() if not p.get("hidden")]
    if not me.get("hidden") and len(visible) <= 1:
        blocked.append("It is the last visible pipeline.")
    return {
        "ok": True,
        "kind": "submodule",
        "name": pipeline_id,
        "label": me.get("name", pipeline_id),
        "data": {"total_records": 0, "by_variable": {}, "fingerprint": None, "undoable": True},
        "declaration": {
            "action": "hide_pipeline",
            "message": "The pipeline is removed. No data is deleted: records belong to "
            "functions, not pipelines. To delete a step's data, delete its Function or "
            "Variable.",
        },
        "placements": list(placements.values()),
        "blocked": blocked,
        "reason": default_reason("submodule", me.get("name", pipeline_id)),
    }


def delete_entity_plan(db, kind: str, name: str, values=None) -> dict:
    """The dry run the confirmation shows. Writes nothing.

    ``{ok, kind, name, values, targets, data: {total_records, by_variable,
    seed_by_variable, downstream_by_variable, lost_locations, fingerprint,
    warnings, undoable}, declaration: {action, message, file?, removed?},
    placements: [...], blocked: [reason, ...], reason}``.
    """
    t0 = time.perf_counter()
    if kind not in KINDS:
        return {"ok": False, "error": f"Cannot delete a {kind!r}.", "reason": "unsupported_kind"}
    if kind == "submodule":
        plan = _submodule_plan(db, name)
    else:
        targets = _data_targets(kind, name, values)
        placements = entity_placements(kind, name) if values is None else []
        declaration = _declaration_plan(kind, name, values)
        blocked = _locked_nodes(db, placements)
        if declaration.get("blocked"):
            blocked.append(declaration["blocked"])
        plan = {
            "ok": True,
            "kind": kind,
            "name": name,
            "values": values,
            "targets": targets,
            "data": _data_plan(db, targets),
            "declaration": declaration,
            "placements": placements,
            "blocked": blocked,
            "reason": default_reason(kind, name, values),
        }
        if kind == "parameter":
            plan["value_options"] = _parameter_value_options(db, name)
    if plan.get("ok"):
        logger.info(
            "[entity_delete] plan: %s '%s' values=%s -> %d record(s) %s; declaration=%s; "
            "placements=%s; blocked=%s (%.0f ms)",
            kind,
            name,
            values,
            plan["data"]["total_records"],
            plan["data"].get("by_variable"),
            plan["declaration"]["action"],
            [(p["pipeline_name"], len(p["node_ids"])) for p in plan["placements"]],
            plan["blocked"],
            (time.perf_counter() - t0) * 1000.0,
        )
    return plan


# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------


def delete_entity(
    db,
    kind: str,
    name: str,
    values=None,
    *,
    fingerprint: "str | None" = None,
    reason: "str | None" = None,
) -> dict:
    """Delete what :func:`delete_entity_plan` shows. Call it from the
    ``undoable`` handler so it is one Ctrl Z step.

    Re-plans first and refuses (``{"ok": False, "reason": ...}``, nothing
    written) when the plan is blocked or its data fingerprint changed.
    """
    from scistack_gui.services.layout_service import _notify_dag_updated
    from scistack_gui.stores import history

    t0 = time.perf_counter()
    plan = delete_entity_plan(db, kind, name, values)
    if not plan.get("ok"):
        return plan
    if plan["blocked"]:
        logger.info("[entity_delete] delete: %s '%s' refused: %s", kind, name, plan["blocked"])
        return {"ok": False, "reason": "blocked", "error": " ".join(plan["blocked"]), "plan": plan}
    planned = plan["data"].get("fingerprint")
    if fingerprint is not None and planned != fingerprint:
        return {
            "ok": False,
            "reason": "changed",
            "error": "The data changed since the plan was shown; nothing was deleted. Review the new plan.",
            "plan": plan,
        }
    reason = (reason or "").strip() or plan["reason"]
    warnings: list[str] = []
    out: dict = {"ok": True, "kind": kind, "name": name, "values": values}

    # 1. Data: all or nothing, the only step that may raise.
    if plan["data"]["total_records"]:
        from scidb.store.variant_delete import delete_variant

        result = delete_variant(db, plan["targets"], reason, expect_fingerprint=planned)
        tid = result.tombstone.tombstone_id
        out["tombstone_id"] = tid
        out["deleted"] = result.plan.by_variable
        out["undoable"] = result.undoable and history.note_trash(tid)
        if not out["undoable"]:
            warnings.append("This data delete cannot be undone with Ctrl Z.")
    else:
        out["deleted"] = {}
        out["undoable"] = True

    # 2. Canvas nodes / placements. From here on nothing raises.
    out["nodes_removed"] = _remove_placements(kind, plan, warnings)

    # 3. Declaration / code.
    out["declaration"] = _apply_declaration(db, kind, name, plan, warnings)

    out["warnings"] = warnings
    _notify_dag_updated("delete_entity")
    logger.info(
        "[entity_delete] delete: %s '%s' values=%s -> deleted=%s tombstone=%s nodes=%d "
        "declaration=%s warnings=%s (%.0f ms)",
        kind,
        name,
        values,
        out["deleted"],
        out.get("tombstone_id"),
        out["nodes_removed"],
        out["declaration"],
        warnings,
        (time.perf_counter() - t0) * 1000.0,
    )
    return out


def _remove_placements(kind: str, plan: dict, warnings: list[str]) -> int:
    from scistack_gui.services import layout_service, scope_service

    removed = 0
    for p in plan["placements"]:
        for nid in p["node_ids"]:
            try:
                if kind == "submodule":
                    scope_service.remove_pipeline_use(nid)
                else:
                    layout_service.delete_layout(nid)
                removed += 1
            except Exception as exc:
                logger.exception("[entity_delete] removing node %s failed", nid)
                warnings.append(f"Could not remove the node on {p['pipeline_name']}: {exc}")
    return removed


def _apply_declaration(db, kind: str, name: str, plan: dict, warnings: list[str]) -> dict:
    decl = plan["declaration"]
    action = decl["action"]
    try:
        if action == "remove":
            from scistack_gui.services.target_file_service import remove_declaration

            res = remove_declaration(kind, name)
        elif action == "edit_values":
            from scistack_gui.services.layout_service import update_parameter

            res = update_parameter(name, decl["remaining"])
        elif action == "delete_file":
            res = _delete_function_file(Path(decl["file"]))
        elif action == "hide_pipeline":
            from scistack_gui.services import scope_service

            res = scope_service.hide_pipeline(name)
        else:
            return {"action": "none", "ok": True}
    except Exception as exc:
        logger.exception("[entity_delete] declaration step %s for %s '%s' failed", action, kind, name)
        res = {"ok": False, "error": str(exc)}
    if not res.get("ok"):
        warnings.append(f"The data was deleted, but the {action.replace('_', ' ')} step failed: {res.get('error')}")
    return {"action": action, **{k: v for k, v in res.items() if k in ("ok", "error", "file", "removed")}}


def _delete_function_file(path: Path) -> dict:
    """Delete a GUI-created function/glue file (snapshotted first, so Ctrl Z
    brings it back) and reload the project so the function leaves the
    registry. The plan decided it is SciStack's file."""
    from scistack_gui.services import glue_service
    from scistack_gui.stores import history

    history.note_write(path)
    path.unlink(missing_ok=True)
    logger.info("[entity_delete] deleted GUI-owned function file %s", path)
    reloaded = glue_service._reload_project()
    if reloaded.get("error"):
        return {"ok": False, "file": str(path), "error": reloaded["error"]}
    return {"ok": True, "file": str(path)}
