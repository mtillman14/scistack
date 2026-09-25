"""
Layout service — single source of truth for layout CRUD operations.

Thin orchestration keeping protocol adapters from importing data access directly.
"""

from __future__ import annotations

import logging

from scidb import BaseVariable

from scistack_gui.ids import (
    PATH_INPUT_ID_PREFIX,
    ROOT_SCOPE,
    param_node_id,
    path_input_node_id,
)

logger = logging.getLogger(__name__)


def _notify_dag_updated() -> None:
    """Broadcast dag_updated after a WIRING mutation (node create/delete,
    edge create/delete) so the canvas refetches and freshly placed nodes
    get their real DB-checked state — a re-dropped, re-wired function that
    is already computed must come back GREEN, not the frontend-local red
    (user-found 2026-07-19). Deliberately NOT called for position-only
    writes: drags would trigger a full graph rebuild per drop."""
    from scistack_gui.api.ws import push_message

    push_message({"type": "dag_updated"})


def get_layout(pipeline_id: str = ROOT_SCOPE) -> dict:
    from scistack_gui import layout as layout_store

    return layout_store.read_layout(pipeline_id)


def put_layout(
    node_id: str,
    x: float,
    y: float,
    node_type: str | None = None,
    label: str | None = None,
    pipeline_id: str = ROOT_SCOPE,
) -> dict:
    from scistack_gui import layout as layout_store

    logger.info(
        "[layout_service] put_layout called (node_id=%r, type=%r, label=%r, position=(%.1f, %.1f), scope=%r)",
        node_id,
        node_type,
        label,
        x,
        y,
        pipeline_id,
    )
    if node_type and label:
        logger.info("[layout_service] Creating/updating manual node")
        if node_type == "pathInputNode":
            existing = _placed_path_input(label, pipeline_id)
            if existing is not None and existing != node_id:
                logger.info(
                    "[layout_service] Refusing to place PathInput %r in scope %r: "
                    "already on this canvas as %r",
                    label,
                    pipeline_id,
                    existing,
                )
                return {
                    "ok": False,
                    "error": (
                        f"PathInput '{label}' is already on this canvas. A "
                        f"PathInput's name is its identity — wire the existing "
                        f"node, or declare a new PathInput with its own name."
                    ),
                    "reason": "duplicate_path_input",
                    "existing_node_id": existing,
                }
        if node_type == "functionNode":
            from scistack_gui import matlab_registry

            if matlab_registry.is_matlab_function(label):
                info = matlab_registry.get_matlab_function(label)
                logger.info(
                    "[layout_service] Function node placed: %r (MATLAB, n_outputs=%d, output_names=%s)",
                    label,
                    info.n_outputs,
                    info.output_names,
                )
            else:
                logger.info("[layout_service] Function node placed: %r (Python)", label)
        elif node_type == "variableNode" and label not in BaseVariable._all_subclasses:
            # NOT refused. A manual variable node whose label nothing declares
            # yet is a legitimate, designed state: it is a placeholder that
            # graduates to a canonical var__ id once a run gives it DB history
            # (graph_builder.merge_manual_nodes), and paste/extract copy such
            # nodes wholesale. Refusing here broke duplicate, paste,
            # extract-to-submodule and edge-driven binding.
            #
            # It is still worth a line in the log, because this is the state
            # that ends in "Unrecognized function or variable" if a MATLAB run
            # reaches it undeclared. The hard gate is at the run boundary,
            # where the decision is unambiguous — see api/run.py and
            # matlab_command._unresolvable_var_types.
            logger.info(
                "[layout_service] Variable node %r placed with label %r, which "
                "nothing declares yet — fine for a placeholder, but a MATLAB "
                "run will refuse until it is declared",
                node_id,
                label,
            )
        else:
            logger.debug(
                "[layout_service] Node added to DAG: node_id=%r, type=%r, label=%r",
                node_id,
                node_type,
                label,
            )
        layout_store.write_manual_node(
            node_id, x, y, node_type, label, pipeline_id=pipeline_id
        )
        logger.info("[layout_service] Manual node created/updated successfully")
        _notify_dag_updated()
    else:
        logger.info("[layout_service] Updating node position only (no type/label)")
        layout_store.write_node_position(node_id, x, y, pipeline_id=pipeline_id)
        logger.info("[layout_service] Node position updated successfully")
    return {"ok": True}


def _placed_path_input(label: str, pipeline_id: str) -> "str | None":
    """The id of the node already showing PathInput *label* on scope
    *pipeline_id*'s canvas, or ``None``.

    Answered from the scope's BUILT graph (``get_pipeline_graph``, without
    run states) rather than from positions/manual rows, because what a canvas
    shows is decided there — history-derived nodes default to root, declared-
    only ones do not, hidden ones are gone (``domain.scope_filter``). One
    canvas is one scope: the same PathInput on two hypothesis tabs is fine.
    """
    from scistack_gui.db import get_db
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    graph = get_pipeline_graph(get_db(), pipeline_id, run_states=False)
    for node in graph.get("nodes", []):
        if node.get("type") == "pathInputNode" and (
            (node.get("data") or {}).get("label") == label
        ):
            return node["id"]
    return None


def delete_layout(node_id: str) -> dict:
    from scistack_gui import layout as layout_store

    logger.info("[layout_service] delete_layout called (node_id=%r)", node_id)
    layout_store.delete_node(node_id)
    logger.info("[layout_service] Node deleted successfully")
    _notify_dag_updated()
    return {"ok": True}


def put_edge(
    db,
    edge_id: str,
    source: str,
    target: str,
    source_handle: str | None = None,
    target_handle: str | None = None,
) -> dict:
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store
    from scistack_gui.domain.graph_builder import candidate_edge_id, find_cycle
    from scistack_gui.domain.scope_filter import node_scope

    logger.info(
        "[layout_service] put_edge called (edge_id=%r, source=%r, target=%r, source_handle=%r, target_handle=%r)",
        edge_id,
        source,
        target,
        source_handle,
        target_handle,
    )

    # If this connection recreates a previously-hidden DB-derived edge
    # (same source/target — the candidate id is deterministic, see
    # graph_builder.candidate_edge_id), unhide the ORIGINAL edge instead of
    # creating a redundant manual one. This is what makes delete+reconnect
    # idempotent: state/execution recompute fresh from the real DB history
    # under the original edge id, not a new manual-edge id. Scoped to the
    # connection's own scope (derived from its endpoints, same as
    # delete_edge below) so reconnecting in one pipeline never unhides
    # another pipeline's independent placement of the same shared wiring.
    candidate = candidate_edge_id(source, target, target_handle)
    if candidate is not None:
        manual_nodes = pipeline_store.get_manual_nodes(db)
        positions_by_scope = layout_store.read_positions_by_scope()
        scope_id = node_scope(target, manual_nodes, positions_by_scope)
        if candidate in pipeline_store.get_hidden_edge_ids(db, scope_id):
            logger.info(
                "[layout_service] put_edge: reconnecting hidden DB-derived edge %r "
                "in scope=%r — unhiding instead of creating a manual edge",
                candidate,
                scope_id,
            )
            pipeline_store.unhide_edge(db, candidate, scope_id)
            _notify_dag_updated()
            return {"ok": True, "unhidden": candidate}

    # Checked against manual edges only (not the full DB-derived data-lineage
    # graph): computing that graph (domain.pipeline_service.get_pipeline_graph
    # -> api.pipeline._build_graph) has a side effect — it PERSISTS manual-node
    # graduation as part of building the response — so calling it here, before
    # this edge's wiring is fully in place, can graduate a node prematurely on
    # its still-incomplete wiring (regression found via
    # test_differently_wired_manual_node_does_not_graduate_or_show_green and
    # friends). A cycle closed purely through immutable, already-executed
    # DB-derived edges plus this one new manual edge won't be caught here —
    # it still surfaces at run time as scidb's PipelineCycleError.
    existing_edges = [
        e for e in pipeline_store.get_manual_edges(db) if e["id"] != edge_id
    ]
    cycle_path = find_cycle(existing_edges, source, target)
    if cycle_path is not None:
        logger.warning(
            "[layout_service] put_edge rejected — would create a cycle: %s",
            " -> ".join(cycle_path),
        )
        raise ValueError(
            f"connecting '{source}' to '{target}' would create a dependency "
            f"cycle: {' -> '.join(cycle_path)}"
        )
    pipeline_store.write_manual_edge(
        db,
        {
            "id": edge_id,
            "source": source,
            "target": target,
            "sourceHandle": source_handle,
            "targetHandle": target_handle,
        }
    )
    logger.info("[layout_service] Edge created successfully")
    _notify_dag_updated()
    return {"ok": True}


def delete_edge(
    db,
    edge_id: str,
    source: str = "",
    target: str = "",
    source_handle: str | None = None,
    target_handle: str | None = None,
) -> dict:
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store
    from scistack_gui.domain.scope_filter import node_scope
    from scistack_gui.ids import ROOT_SCOPE

    logger.info("[layout_service] delete_edge called (edge_id=%r)", edge_id)
    # Whether an edge is "manual" is decided by ACTUAL membership in the
    # manual-edges table, not the `manual__` id prefix — that prefix is
    # only the frontend's own naming convention for edges it creates via
    # onConnect; put_edge accepts any caller-supplied id, so a manual edge
    # can legitimately have a differently-shaped id (e.g. existing tests
    # PUT arbitrary ids like "e_del").
    is_manual = any(e["id"] == edge_id for e in pipeline_store.get_manual_edges(db))
    if is_manual:
        pipeline_store.delete_manual_edge(db, edge_id)
        logger.info("[layout_service] Manual edge hard-deleted")
    else:
        # DB-derived edge: never delete data, only hide it — build_edges
        # excludes it on every rebuild until unhide_edge (see put_edge's
        # reconnect-detection) or the restore panel brings it back. Scoped
        # to the edge's own scope (derived from its endpoints — same
        # canonical id shared by another pipeline's independent placement
        # of this wiring must stay visible there; see
        # plan-scope-hidden-nodes-edges.md).
        manual_nodes = pipeline_store.get_manual_nodes(db)
        positions_by_scope = layout_store.read_positions_by_scope()
        scope_id = (
            node_scope(target, manual_nodes, positions_by_scope)
            if target
            else node_scope(source, manual_nodes, positions_by_scope)
            if source
            else ROOT_SCOPE
        )
        logger.info(
            "[layout_service] delete_edge: hiding DB-derived edge in scope=%r",
            scope_id,
        )
        pipeline_store.hide_edge(
            db, edge_id, source, target, source_handle, target_handle, scope_id
        )
        logger.info("[layout_service] DB-derived edge hidden")
    _notify_dag_updated()
    return {"ok": True}


def unhide_edge(db, edge_id: str, pipeline_id: str = ROOT_SCOPE) -> dict:
    from scistack_gui import pipeline_store

    logger.info(
        "[layout_service] unhide_edge called (edge_id=%r, pipeline_id=%r)",
        edge_id,
        pipeline_id,
    )
    pipeline_store.unhide_edge(db, edge_id, pipeline_id)
    _notify_dag_updated()
    return {"ok": True}


def get_hidden_edges(db, pipeline_id: "str | None" = None) -> dict:
    from scistack_gui import pipeline_store

    return {"edges": pipeline_store.list_hidden_edges(db, pipeline_id)}


def get_notes() -> dict[str, str]:
    from scistack_gui import layout as layout_store

    return layout_store.read_notes()


def set_note(key: str, text: str) -> dict:
    from scistack_gui import layout as layout_store

    layout_store.write_note(key, text)
    return {"ok": True}


def get_parameters() -> list[dict]:
    """Source-scanned — see docs/claude/code-discovery-categories.md.

    ``values`` is always a list, whatever the count: a Parameter holding one
    value is not a different shape from one holding several (D6).

    ``source_file``/``source_line``/``declared_in_entities_file`` mirror what
    ``graph_builder.build_parameter_nodes`` puts on the canvas node's
    ``data`` — the sidebar list is the other surface that needs to offer
    "refresh from file" / "open source" (see ``is_declared_in_entities_file``,
    the single comparison shared by both)."""
    from scistack_gui import registry
    from scistack_gui.domain.graph_builder import is_declared_in_entities_file

    entities_file = (
        str(registry._config.entities_file)
        if registry._config is not None and registry._config.entities_file is not None
        else None
    )
    return [
        {
            "name": name,
            "values": list(p.values),
            "description": p.description,
            "source_file": getattr(p, "source_file", None),
            "source_line": getattr(p, "source_line", None),
            "declared_in_entities_file": is_declared_in_entities_file(
                getattr(p, "source_file", None), entities_file
            ),
        }
        for name, p in registry.get_parameters_registry().items()
    ]


def create_parameter(name: str, values: "list | None" = None) -> dict:
    """Append a new ``NAME = scidb.Parameter(...)`` to source. See
    :func:`update_parameter` for editing an existing one."""
    from scistack_gui.services.parameter_service import create_parameter as _create

    logger.debug("Node created (added to palette): type=parameter, name=%r", name)
    return _create(name, values or [])


def update_parameter(
    name: str, values: list, description: str = "", group: "dict | None" = None
) -> dict:
    """Rewrite an existing Parameter's declaration in source.

    Writes are confined to the configured entities file; a Parameter declared
    anywhere else comes back ``{"ok": False, "reason": "read_only"}`` with
    the ``file``/``line`` to point the user at. See
    ``docs/claude/entity-editability-model.md``.

    Range generation (start/end/step) stays a frontend concern — this always
    receives the final, already-computed flat list. *group* is how the panel
    says that list came from the Generate section rather than one "Add value"
    at a time, so the set can render as a single checkable row; it carries no
    values of its own and never affects what is written to source.
    """
    from scistack_gui.services.parameter_service import update_parameter as _update

    result = _update(name, values, description, group)
    if result.get("ok"):
        _notify_dag_updated()
    return result


def delete_parameter(name: str, pipeline_id: str = ROOT_SCOPE) -> dict:
    """"Delete" hides the node only — the source declaration is never
    touched (never delete, mark hidden). Reuses the same generic hide-node
    mechanism functions/variables/PathInputs already use."""
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    ps.hide_node(get_db(), param_node_id(name), pipeline_id=pipeline_id)
    return {"ok": True}


def get_path_inputs() -> list[dict]:
    """Source-scanned — see docs/claude/code-discovery-categories.md."""
    from scistack_gui import registry
    from scistack_gui.domain.graph_builder import path_input_display

    return [
        {"name": name, **path_input_display(obj)}
        for name, obj in registry.get_path_inputs_registry().items()
    ]


def create_path_input(
    name: str, template: str = "", root_folder: "str | None" = None
) -> dict:
    """Append a new ``NAME = PathInput(...)`` to source. See
    :func:`update_path_input` for editing an existing one."""
    from scistack_gui.services.path_input_service import create_path_input as _create

    return _create(name, template, root_folder)


def update_path_input(
    name: str,
    template: str,
    root_folder: "str | None" = None,
    alternate_templates: "list[dict] | None" = None,
) -> dict:
    """Rewrite an existing PathInput's declaration in source.

    Passing ``alternate_templates`` is how a PathInput becomes
    multi-template: it re-renders as
    ``EachOf(PathInput(...), PathInput(...))`` under the same name, which
    keeps the node's identity and every run recorded against the original
    template (see ``docs/claude/entity-editability-model.md`` Rule 1).
    """
    from scistack_gui.services.path_input_service import update_path_input as _update

    result = _update(name, template, root_folder, alternate_templates)
    if result.get("ok"):
        _notify_dag_updated()
    return result


#: The note key a PathInput's sidebar note lives under — mirrors the
#: frontend's ``noteKey`` (``${kind}:${name}``, kind ``pathInput``).
_PATH_INPUT_NOTE_KIND = "pathInput"


def rename_path_input(name: str, new_name: str) -> dict:
    """Rename a PathInput: its declaration, and everything the GUI keys by
    its name (``.claude/plan-pathinput-rename.md``).

    Order matters. The entities file is written first and is the only step
    that can refuse; if it does, nothing else has changed. After it
    succeeds the name IS new, so every remaining step brings GUI state into
    line with it:

    1. record ``old -> new`` so runs that recorded the old declared name
       attribute to the renamed node, not a ghost
       (``graph_builder.resolve_renamed_path_input``);
    2. move node-keyed DB state for every placement (``rebase_node``) and
       relabel ungraduated manual rows;
    3. move layout positions and the sidebar note.

    The template is untouched, so runs recorded WITHOUT a name still
    content-match. Scripts that refer to the old name are not rewritten —
    the GUI does not own them.
    """
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db
    from scistack_gui.services.target_file_service import rename_declaration

    new_name = (new_name or "").strip()
    logger.info("[layout_service] rename_path_input %r -> %r", name, new_name)
    result = rename_declaration("path_input", name, new_name)
    if not result.get("ok") or result.get("unchanged"):
        logger.info("[layout_service] rename_path_input: declaration step -> %s", result)
        return result

    old_bare = path_input_node_id(name)
    new_bare = path_input_node_id(new_name)
    try:
        db = get_db()
        ps.record_path_input_rename(db, name, new_name)
        counts = ps.rebase_node(db, old_bare, new_bare, new_label=new_name)
        counts["relabelled"] = ps.relabel_manual_nodes(
            db, "pathInputNode", name, new_name
        )
        counts["positions"] = layout_store.rebase_node_positions(
            old_bare,
            new_bare,
            note_keys=(
                f"{_PATH_INPUT_NOTE_KIND}:{name}",
                f"{_PATH_INPUT_NOTE_KIND}:{new_name}",
            ),
        )
    except Exception as e:
        # The file is already renamed and cannot sensibly be un-renamed from
        # here; say exactly what is left behind rather than pretend.
        logger.exception(
            "[layout_service] rename_path_input %r -> %r: declaration renamed, "
            "but moving GUI state failed",
            name,
            new_name,
        )
        _notify_dag_updated()
        return {
            "ok": False,
            "error": (
                f"'{name}' was renamed to '{new_name}' in {result.get('file')}, "
                f"but its canvas placements could not be moved: {e}. Re-place "
                f"'{new_name}' on the canvas; see scidb.log."
            ),
            "reason": "partial",
        }

    logger.info(
        "[layout_service] rename_path_input %r -> %r done: %s", name, new_name, counts
    )
    _notify_dag_updated()
    return {**result, "moved": counts}


def delete_path_input(name: str, pipeline_id: str = ROOT_SCOPE) -> dict:
    """"Delete" hides the node only — the source declaration is never
    touched (never delete, mark hidden). Reuses the same generic hide-node
    mechanism functions/variables already use."""
    from scistack_gui import pipeline_store as ps
    from scistack_gui.db import get_db

    ps.hide_node(get_db(), path_input_node_id(name), pipeline_id=pipeline_id)
    return {"ok": True}




def deep_copy_path_input(node_id: str) -> dict:
    """Give one PathInput node its own independent named source
    declaration — opt-in fork; every other placement of the original name
    is untouched. See pipeline_store's module docstring for the "shared by
    default" PathInput design this is the escape hatch for.

    Unlike the old layout.json version, the copy is a NEW top-level source
    declaration (``create_path_input``), not a new layout.json row — so it
    survives with the same "must configure a variable_file" constraint as
    any other GUI-initiated creation.
    """
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store as ps
    from scistack_gui import registry
    from scistack_gui.db import get_db
    from scistack_gui.domain.graph_builder import path_input_display
    from scistack_gui.ids import strip_placement
    from scistack_gui.domain.scope_filter import node_scope
    from scistack_gui.services.path_input_service import create_path_input

    db = get_db()
    manual_nodes = ps.get_manual_nodes(db)
    meta = manual_nodes.get(node_id)
    bare_id = strip_placement(node_id)
    if meta is not None:
        if meta.get("type") != "pathInputNode":
            raise ValueError(f"'{node_id}' is not a PathInput node")
        old_name = meta["label"]
    elif bare_id.startswith(PATH_INPUT_ID_PREFIX):
        parts = bare_id.split("__")
        old_name = parts[1] if len(parts) >= 2 else None
    else:
        old_name = None
    if not old_name:
        raise ValueError(f"'{node_id}' is not a PathInput node")

    obj = registry.get_path_input(old_name)
    if obj is None:
        raise ValueError(f"PathInput '{old_name}' not found in the registry")
    display = path_input_display(obj)

    positions_by_scope = layout_store.read_positions_by_scope()
    pipeline_id = node_scope(node_id, manual_nodes, positions_by_scope)
    pos = positions_by_scope.get(pipeline_id, {}).get(node_id, {"x": 0.0, "y": 0.0})

    new_name = _unique_path_input_name(old_name, registry.get_path_inputs_registry())
    result = create_path_input(new_name, display["template"], display.get("root_folder"))
    if not result.get("ok"):
        raise ValueError(result.get("error", "Failed to create PathInput copy"))

    # Upsert guarantees a row exists whether or not this node was manual
    # before (a DB-derived PathInput node has no _pipeline_nodes row until
    # its first override) — position is passed through unchanged.
    layout_store.write_manual_node(
        node_id, pos["x"], pos["y"], "pathInputNode", new_name, pipeline_id=pipeline_id
    )
    return {"ok": True, "name": new_name}


def _unique_path_input_name(base: str, existing: dict) -> str:
    """``base_copy``, ``base_copy2``, ... — first name not already in the
    registry."""
    if f"{base}_copy" not in existing:
        return f"{base}_copy"
    i = 2
    while f"{base}_copy{i}" in existing:
        i += 1
    return f"{base}_copy{i}"


def put_pending_constant(db, name: str, value: str) -> dict:
    from scistack_gui import pipeline_store

    logger.info(
        "[layout_service] put_pending_constant called (name=%r, value=%r)", name, value
    )
    pipeline_store.add_pending_constant(db, name, value)
    logger.info("[layout_service] Pending constant value added successfully")
    return {"ok": True}


def delete_pending_constant(db, name: str, value: str) -> dict:
    from scistack_gui import pipeline_store

    logger.info(
        "[layout_service] delete_pending_constant called (name=%r, value=%r)",
        name,
        value,
    )
    pipeline_store.remove_pending_constant(db, name, value)
    logger.info("[layout_service] Pending constant value removed successfully")
    return {"ok": True}


def put_node_config(db, node_id: str, config: dict) -> dict:
    from scistack_gui import pipeline_store

    pipeline_store.update_node_config(db, node_id, config)
    return {"ok": True}


def hide_variant_combo(
    db, function_name: str, node_id: str | None, variant_key: dict
) -> dict:
    """Hide one row of a function's constant Cartesian product — never
    deletes data, only excludes it from display and future runs."""
    from scistack_gui import pipeline_store
    from scistack_gui.ids import fn_node_id
    from scistack_gui.services.execution_service import resolve_combo_call_ids

    logger.info(
        "[layout_service] hide_variant_combo called (function_name=%r, "
        "node_id=%r, variant_key=%r)",
        function_name,
        node_id,
        variant_key,
    )
    call_ids = resolve_combo_call_ids(db, function_name, node_id, variant_key)
    for cid in call_ids:
        pipeline_store.hide_combo(
            db, fn_node_id(function_name, cid), function_name, variant_key
        )
    logger.info(
        "[layout_service] hide_variant_combo: hid %d call site(s)", len(call_ids)
    )
    _notify_dag_updated()
    return {"ok": True, "hidden_count": len(call_ids)}


def unhide_variant_combo(db, node_id: str) -> dict:
    from scistack_gui import pipeline_store

    logger.info("[layout_service] unhide_variant_combo called (node_id=%r)", node_id)
    pipeline_store.unhide_combo(db, node_id)
    _notify_dag_updated()
    return {"ok": True}


def get_hidden_combos(db, function_name: str) -> dict:
    from scistack_gui import pipeline_store

    return {"combos": pipeline_store.list_hidden_combos(db, function_name)}


def hide_parameter_value(
    db, const_name: str, value: str, pipeline_id: str = ROOT_SCOPE
) -> dict:
    """Hide one constant value — excludes it (and, once execution_service
    consults this, every call site using it) from future runs without
    deleting any DB history for it."""
    from scistack_gui import pipeline_store

    logger.info(
        "[layout_service] hide_parameter_value called (const_name=%r, "
        "value=%r, pipeline_id=%r)",
        const_name,
        value,
        pipeline_id,
    )
    pipeline_store.hide_parameter_value(db, const_name, value, pipeline_id)
    _notify_dag_updated()
    return {"ok": True}


def refresh_parameter_source(name: "str | None" = None) -> dict:
    """Re-read the TOML entities file and re-register everything it
    declares, without paying for a full ``refresh_module()``/``refresh_all()``
    (re-imports every module, re-parses every MATLAB source -- ~16.5s on a
    real project). ``registry.reload_entities_file()`` is a single TOML
    parse -- see its docstring for the measured cost comparison.

    *name* is the Parameter whose context menu triggered this; it is only
    logged, never used to scope the reload. The reload is whole-file, so
    every entities-file-declared Parameter picks up its current value as a
    side effect, not just the one clicked -- there is no per-key TOML read
    to scope down to, and the whole-file parse is already sub-second."""
    from scistack_gui import registry

    logger.info(
        "[layout_service] refresh_parameter_source called (name=%r)", name
    )
    error = registry.reload_entities_file()
    if error:
        logger.warning(
            "[layout_service] refresh_parameter_source failed: %s", error
        )
        return {"ok": False, "error": error}
    _notify_dag_updated()
    return {"ok": True}


def unhide_parameter_value(
    db, const_name: str, value: str, pipeline_id: str = ROOT_SCOPE
) -> dict:
    from scistack_gui import pipeline_store

    logger.info(
        "[layout_service] unhide_parameter_value called (const_name=%r, "
        "value=%r, pipeline_id=%r)",
        const_name,
        value,
        pipeline_id,
    )
    pipeline_store.unhide_parameter_value(db, const_name, value, pipeline_id)
    _notify_dag_updated()
    return {"ok": True}


def set_parameter_group_checked(
    db, const_name: str, values: list, checked: bool, pipeline_id: str = ROOT_SCOPE
) -> dict:
    """Check or uncheck a whole generated value set at once.

    The set is the unit the user toggles on the node, so this hides or
    unhides every member in ONE statement rather than one call per value —
    a 50-value range is the same N+1 shape as
    ``project_batched_provenance_hot_paths``.
    """
    from scistack_gui import pipeline_store

    members = [str(v) for v in values]
    logger.info(
        "[layout_service] set_parameter_group_checked called (const_name=%r, "
        "%d value(s), checked=%r, pipeline_id=%r)",
        const_name,
        len(members),
        checked,
        pipeline_id,
    )
    if checked:
        pipeline_store.unhide_parameter_values(db, const_name, members, pipeline_id)
    else:
        pipeline_store.hide_parameter_values(db, const_name, members, pipeline_id)
    _notify_dag_updated()
    return {"ok": True}


def get_hidden_constant_values(db, pipeline_id: "str | None" = ROOT_SCOPE) -> dict:
    from scistack_gui import pipeline_store

    return {
        "hidden_values": pipeline_store.list_hidden_parameter_values(db, pipeline_id)
    }
