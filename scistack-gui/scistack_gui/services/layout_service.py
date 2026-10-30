"""
Layout service — single source of truth for layout CRUD operations.

Thin orchestration keeping protocol adapters from importing data access directly.
"""

from __future__ import annotations

import logging
import re

from scidb import BaseVariable
from scifor.failures import ActionableError

from scistack_gui.domain.display_order import display_sorted
from scistack_gui.ids import (
    PATH_INPUT_ID_PREFIX,
    ROOT_SCOPE,
    param_node_id,
    path_input_node_id,
)

logger = logging.getLogger(__name__)


class EdgeCycleError(ActionableError, ValueError):
    """Connecting two nodes would close a dependency cycle. A refusal whose
    message says which edge to remove, so the journal files it as a setup
    problem, not a SciStack bug (``scifor.failures.classify_blame``)."""


def _notify_dag_updated(source: str | None = None) -> None:
    """Broadcast dag_updated after a WIRING mutation (node create/delete,
    edge create/delete) so the canvas refetches and freshly placed nodes
    get their real DB-checked state — a re-dropped, re-wired function that
    is already computed must come back GREEN, not the frontend-local red
    (user-found 2026-07-19). Deliberately NOT called for position-only
    writes: drags would trigger a full graph rebuild per drop.

    *source* labels the refresh it triggers in the canvas's
    ``[client] canvas refresh`` lines (frontend DAG/canvasRefreshLog.ts), so
    a placement's own refresh is told apart from the .duckdb watcher's."""
    from scistack_gui.api.ws import push_message

    push_message({"type": "dag_updated", **({"source": source} if source else {})})


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
        _notify_dag_updated("put_layout")
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
    from scistack_gui.domain.graph_builder import connection_id, find_cycle
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
    # (same connection — graph_builder.connection_id, which history edges
    # take as their id), unhide the ORIGINAL edge instead of
    # creating a redundant manual one. This is what makes delete+reconnect
    # idempotent: state/execution recompute fresh from the real DB history
    # under the original edge id, not a new manual-edge id. Scoped to the
    # connection's own scope (derived from its endpoints, same as
    # delete_edge below) so reconnecting in one pipeline never unhides
    # another pipeline's independent placement of the same shared wiring.
    manual_nodes = pipeline_store.get_manual_nodes(db)
    candidate = connection_id(source, target, target_handle, manual_nodes)
    positions_by_scope = layout_store.read_positions_by_scope()
    scope_id = node_scope(target, manual_nodes, positions_by_scope)
    hidden_here = pipeline_store.get_hidden_edge_ids(db, scope_id)
    if edge_id in hidden_here and candidate not in hidden_here:
        # A hide made with no port matches by the edge's own id
        # (pipeline_store._hidden_connection_id). Removing a stored edge hides
        # it (D2, 2026-10-22), so redrawing that same edge must unhide it, or
        # its stored row stays hidden under the id it is redrawn with.
        candidate = edge_id
    if not target_handle and candidate not in hidden_here:
        # A redraw with no port (an API caller; the canvas always sends one)
        # names the two NODES only. Restore the one hidden connection between
        # them; with several, it is ambiguous, so a drawn edge is stored.
        between = sorted(h for h in hidden_here if h.startswith(candidate))
        if len(between) == 1:
            candidate = between[0]
        elif between:
            logger.info(
                "[layout_service] put_edge: %d hidden connections between %s and %s "
                "and no port given; storing a drawn edge instead of guessing: %s",
                len(between),
                source,
                target,
                between,
            )
    if candidate in hidden_here:
        logger.info(
            "[layout_service] put_edge: reconnecting hidden DB-derived edge %r "
            "in scope=%r — unhiding instead of creating a manual edge",
            candidate,
            scope_id,
        )
        pipeline_store.unhide_connection(db, candidate, scope_id)
        _notify_dag_updated()
        return {"ok": True, "unhidden": candidate}

    # Checked against the edges the canvas DRAWS (edge_view.effective_edges),
    # not every stored row: since D2 (2026-10-22) removing an edge hides it
    # and keeps the row, and a removed edge closing a "cycle" rejected the
    # user's rewiring (scistack.log 2026-10-22 17:51: LoadedData -> fn was
    # removed, then fn -> LoadedData was refused three times).
    # Not the full graph build (domain.pipeline_service.get_pipeline_graph ->
    # api.pipeline._build_graph): it PERSISTS manual-node graduation as part
    # of building the response, so calling it here, before this edge's wiring
    # is in place, can graduate a node prematurely (regression found via
    # test_differently_wired_manual_node_does_not_graduate_or_show_green and
    # friends). A cycle closed through history edges not yet adopted as
    # stored rows still surfaces at run time as scidb's PipelineCycleError.
    from scistack_gui.domain.edge_view import effective_edges

    view = effective_edges(db, scope_id, caller="put_edge cycle check")
    existing_edges = [e for e in view.drawn if e["id"] != edge_id]
    cycle_path = find_cycle(existing_edges, source, target)
    if cycle_path is not None:
        by_ends = {(e["source"], e["target"]): e["id"] for e in existing_edges}
        logger.warning(
            "[layout_service] put_edge rejected — would create a cycle: %s "
            "(closed by drawn edge(s) %s)",
            " -> ".join(cycle_path),
            [
                by_ends[pair]
                for pair in zip(cycle_path[1:], cycle_path[2:])
                if pair in by_ends
            ],
        )
        raise EdgeCycleError(
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
    _notify_dag_updated("put_edge")
    return {"ok": True}


def delete_edge(
    db,
    edge_id: str,
    source: str = "",
    target: str = "",
    source_handle: str | None = None,
    target_handle: str | None = None,
) -> dict:
    from scistack_gui import pipeline_store

    logger.info("[layout_service] delete_edge called (edge_id=%r)", edge_id)
    # Every removal is a HIDE, drawn edge or not (user decision D2,
    # 2026-10-22: auditable in `_intent` + `_canvas_change`, undoable). Since
    # canvas adoption a node's edges are stored rows, and a hard delete would
    # leave nothing to audit or restore. The stored row stays; its connection
    # is hidden, which `edge_view` already applies to drawn edges
    # (`graph_builder.manual_edge_is_hidden`), and redrawing the same
    # connection unhides it (`put_edge`). A stored edge's own endpoints fill in
    # whatever the caller did not send, so the hide matches by connection.
    stored = next(
        (e for e in pipeline_store.get_manual_edges(db) if e["id"] == edge_id), None
    )
    if stored is not None:
        source = source or stored.get("source", "")
        target = target or stored.get("target", "")
        source_handle = source_handle or stored.get("sourceHandle")
        target_handle = target_handle or stored.get("targetHandle")
    _hide_edge_in_its_scope(
        db,
        edge_id,
        source,
        target,
        source_handle,
        target_handle,
        "stored row kept" if stored is not None else "no stored row",
    )
    _notify_dag_updated()
    return {"ok": True}


def _hide_edge_in_its_scope(
    db,
    edge_id: str,
    source: str,
    target: str,
    source_handle: "str | None",
    target_handle: "str | None",
    why: str,
) -> None:
    """Remove an edge from the canvas: THE edge removal (delete_edge and
    disconnect_removed_arguments). A hide, never a row delete (user decision
    D2, 2026-10-22).

    Scoped to the edge's own scope (derived from its endpoints): the same
    connection on another pipeline's independent placement stays visible
    there (plan-scope-hidden-nodes-edges.md)."""
    from scistack_gui import layout as layout_store
    from scistack_gui import pipeline_store
    from scistack_gui.domain.scope_filter import node_scope

    manual_nodes = pipeline_store.get_manual_nodes(db)
    positions_by_scope = layout_store.read_positions_by_scope()
    scope_id = (
        node_scope(target, manual_nodes, positions_by_scope)
        if target
        else node_scope(source, manual_nodes, positions_by_scope)
        if source
        else ROOT_SCOPE
    )
    pipeline_store.hide_edge(
        db, edge_id, source, target, source_handle, target_handle, scope_id
    )
    logger.info(
        "[layout_service] edge %s (%s -> %s, %s) hidden in scope=%r (%s)",
        edge_id,
        source,
        target,
        target_handle,
        scope_id,
        why,
    )


def disconnect_removed_arguments(db) -> list[dict]:
    """Refresh Code: disconnect every edge into a port whose argument the
    function no longer takes (user decision 2026-10-27: renaming or removing
    an argument disconnects its edge; the user redraws it onto the new port).

    The edges are the ones :func:`edge_view.effective_edges` already sets
    aside (``stale_port``, :func:`edge_view.missing_port`), so the rule has
    one owner. Each is hidden like a user delete, so renaming the argument
    back does NOT bring it back, and redrawing the connection unhides it
    (put_edge). An audit event names what was disconnected. Returns
    ``[{edge_id, source, target, port}]``.
    """
    from scistack_gui import history
    from scistack_gui.domain.edge_view import effective_edges
    from scistack_gui.ids import IN_HANDLE_PREFIX

    view = effective_edges(db, None, caller="disconnect_removed_arguments")
    disconnected: list[dict] = []
    for e in view.stale_port:
        handle = e.get("targetHandle") or ""
        _hide_edge_in_its_scope(
            db,
            e.get("id", ""),
            e.get("source", ""),
            e.get("target", ""),
            e.get("sourceHandle"),
            handle,
            "argument no longer in the function's signature",
        )
        disconnected.append(
            {
                "edge_id": e.get("id"),
                "source": e.get("source"),
                "target": e.get("target"),
                "port": handle[len(IN_HANDLE_PREFIX):],
            }
        )
    if disconnected:
        summary = ", ".join(
            f"{d['port']} on {d['target']} (from {d['source']})" for d in disconnected
        )
        logger.warning(
            "[layout_service] Refresh Code disconnected %d edge(s) whose "
            "argument the function no longer takes: %s — redraw them onto the "
            "current ports",
            len(disconnected),
            summary,
        )
        history.log_event(
            db,
            "disconnect removed arguments",
            "disconnect_removed_arguments",
            summary,
        )
    else:
        logger.info(
            "[layout_service] disconnect_removed_arguments: no edge on a removed "
            "argument"
        )
    return disconnected


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
    users = _parameter_users()
    return [
        {
            "name": name,
            "values": list(p.values),
            # E6: the libraries whose placed pipelines use it.
            "used_by_libraries": users.get(name, []),
            "description": p.description,
            "source_file": getattr(p, "source_file", None),
            "source_line": getattr(p, "source_line", None),
            "declared_in_entities_file": is_declared_in_entities_file(
                getattr(p, "source_file", None), entities_file
            ),
        }
        for name, p in display_sorted(
            registry.get_parameters_registry().items(), name=lambda kv: kv[0]
        )
    ]


def _parameter_users() -> dict:
    """``library_service.parameter_users`` for the open project (``{}``
    with none open)."""
    from scistack_gui import db as gui_db

    if not gui_db.is_loaded():
        return {}
    from scistack_gui.services.library_service import parameter_users

    return parameter_users(gui_db.get_db())


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
        for name, obj in display_sorted(
            registry.get_path_inputs_registry().items(), name=lambda kv: kv[0]
        )
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
    aliases: "dict | None" = None,
    key_regex: "dict | None" = None,
) -> dict:
    """Rewrite an existing PathInput's declaration in source.

    Passing ``alternate_templates`` is how a PathInput becomes
    multi-template: it re-renders as
    ``EachOf(PathInput(...), PathInput(...))`` under the same name, which
    keeps the node's identity and every run recorded against the original
    template (see ``docs/claude/entity-editability-model.md`` Rule 1).
    """
    from scistack_gui.services.path_input_service import update_path_input as _update

    result = _update(name, template, root_folder, alternate_templates, aliases, key_regex)
    if result.get("ok"):
        result["schema_key_types"] = _auto_declare_schema_key_types(
            key_regex, alternate_templates
        )
        _notify_dag_updated()
    return result


def _auto_declare_schema_key_types(key_regex, alternate_templates) -> dict:
    """A saved digit-only ``key_regex`` declares an undeclared schema key
    numeric (``schema_key_type_service.auto_declare_from_key_regex``). Never
    fails the save it follows: the declaration is a convenience, the
    entities-file write already succeeded."""
    from scistack_gui.services.schema_key_type_service import (
        auto_declare_from_key_regex,
        digit_only_keys,
    )

    empty = {"declared": [], "hints": []}
    if not digit_only_keys(key_regex, alternate_templates):
        return empty
    try:
        from scistack_gui.db import db_connection, get_db

        with db_connection("auto_declare_schema_key_types"):
            return auto_declare_from_key_regex(get_db(), key_regex, alternate_templates)
    except Exception:  # noqa: BLE001 - see docstring
        logger.warning(
            "[layout_service] auto-declaring schema key types from key_regex failed",
            exc_info=True,
        )
        return empty


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
                f"'{new_name}' on the canvas; see scistack.log."
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
    # The whole declaration — alternates and aliases included — or the copy
    # silently matches fewer files than the original.
    result = create_path_input(
        new_name,
        display["template"],
        display.get("root_folder"),
        display.get("alternate_templates") or None,
        display.get("aliases") or None,
    )
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


def set_shown_argument(db, node_id: str, argument: str, shown: bool) -> dict:
    """Reveal (``shown=True``) or re-hide an optional argument's port on one
    function node: ``shownArguments`` in its saved config, merged into the
    rest of the config (``update_node_config`` replaces the whole blob).
    ``graph_builder.port_visibility`` reads it; re-hiding a port something
    feeds changes nothing visible (a fed port is always drawn)."""
    from scistack_gui import pipeline_store

    config = pipeline_store.get_node_config(db, node_id)
    names = [n for n in config.get("shownArguments") or [] if n != argument]
    if shown:
        names.append(argument)
    logger.info(
        "[layout_service] set_shown_argument (node_id=%r, argument=%r, shown=%s) -> %s",
        node_id, argument, shown, names,
    )
    pipeline_store.update_node_config(db, node_id, {**config, "shownArguments": names})
    _notify_dag_updated("set_shown_argument")  # the node's ports change
    return {"ok": True, "shownArguments": names}


def set_on_duplicate(db, node_id: str, value: str) -> dict:
    """Set one function node's ``on_duplicate`` run option (``"error"`` |
    ``"combine"``, scidb.combine): ``runOptions.on_duplicate`` in its saved
    config, merged into the rest (``update_node_config`` replaces the whole
    blob). The duplicate-location popup's "Combine the rows" button calls it
    before re-running the node; ``run_failures.on_duplicate_from_config``
    reads it."""
    from scidb.combine import validate_on_duplicate

    from scistack_gui import pipeline_store

    validate_on_duplicate(value)
    config = pipeline_store.get_node_config(db, node_id)
    run_options = {**(config.get("runOptions") or {}), "on_duplicate": value}
    logger.info(
        "[layout_service] set_on_duplicate (node_id=%r) -> %r", node_id, value
    )
    pipeline_store.update_node_config(db, node_id, {**config, "runOptions": run_options})
    _notify_dag_updated("set_on_duplicate")  # the settings panel shows it
    return {"ok": True, "runOptions": run_options}


_PORT_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class OutputPortsError(ActionableError, ValueError):
    """An output port list that cannot be saved (a bad or repeated name)."""


def set_output_ports(db, node_id: str, ports: list, renames: "dict | None" = None) -> dict:
    """Declare a Python function node's output ports: ``outputPorts`` (names
    in return order) in its saved config, merged like ``shownArguments``.

    A Python function cannot be read for its outputs, so the user names them
    on the node (.claude/plan-python-output-ports.md); port *i* is return
    value *i*. Which variable a port saves to stays its EDGE's: *renames*
    (``{old: new}``) re-points the node's manual output edges from
    ``out__{old}`` to ``out__{new}`` in the same call, so a rename keeps its
    connection. An empty list clears the declaration (automatic again).
    """
    from scistack_gui import intent_store, pipeline_store
    from scistack_gui.ids import (
        OUT_HANDLE_PREFIX,
        VAR_ID_PREFIX,
        new_manual_edge_id,
        out_handle,
        strip_placement,
        var_node_id,
    )

    names = [str(p).strip() for p in ports or []]
    bad = [p for p in names if not _PORT_NAME.match(p)]
    if bad:
        raise OutputPortsError(
            f"Output port names must start with a letter or _ and use only "
            f"letters, digits and _: {bad}"
        )
    repeated = sorted({p for p in names if names.count(p) > 1})
    if repeated:
        raise OutputPortsError(f"Two output ports cannot share a name: {repeated}")

    # Which variable each port saves to BEFORE this edit (by the canvas's own
    # rule), so the edit cannot change it: a port with only script history is
    # otherwise mapped by position, and a reorder would swap its variables.
    from scistack_gui.ids import parse_fn_node_id
    from scistack_gui.services.execution_service import port_variables

    config_before = pipeline_store.get_node_config(db, node_id)
    parsed = parse_fn_node_id(strip_placement(node_id))
    fn_name = parsed[0] if parsed else (
        pipeline_store.get_manual_nodes(db).get(strip_placement(node_id)) or {}
    ).get("label", "")
    before = list(config_before.get("outputPorts") or []) or [
        *(renames or {}).keys(),
        *names,
    ]
    variables_before = port_variables(db, node_id, fn_name, before) if fn_name else {}
    old_of = {new: old for old, new in (renames or {}).items()}

    repointed = 0
    for old, new in (renames or {}).items():
        if not old or not new or old == new:
            continue
        for edge in pipeline_store.get_manual_edges(db):
            if strip_placement(edge.get("source", "")) != strip_placement(node_id):
                continue
            handle = edge.get("sourceHandle")
            # An edge drawn from the unnamed default handle may carry no
            # sourceHandle; its port was named after its variable (the
            # automatic ports), so match it by target.
            if handle != out_handle(old) and not (
                not handle and strip_placement(edge.get("target", "")) == var_node_id(old)
            ):
                continue
            intent_store.write_manual_edge(db, {**edge, "sourceHandle": out_handle(new)})
            repointed += 1

    # The edge owns a port's variable from the first edit on: write one for
    # each port that has a variable but no edge naming it yet; drop the edges
    # of ports that were removed.
    materialized = 0
    removed = 0
    node = strip_placement(node_id)
    from_node = [
        e for e in pipeline_store.get_manual_edges(db)
        if strip_placement(e.get("source", "")) == node
    ]
    named = {e.get("sourceHandle") for e in from_node}
    for p in names:
        variable = variables_before.get(old_of.get(p, p))
        if variable and out_handle(p) not in named:
            intent_store.write_manual_edge(
                db,
                {
                    "id": new_manual_edge_id(),
                    "source": node_id,
                    "target": var_node_id(variable),
                    "sourceHandle": out_handle(p),
                    "targetHandle": None,
                },
            )
            materialized += 1
    if not names:
        # Back to automatic: the ports are named after their variables again,
        # so each edge goes back to its variable's handle.
        for e in from_node:
            target = strip_placement(e.get("target", ""))
            if not target.startswith(VAR_ID_PREFIX):
                continue
            variable = target[len(VAR_ID_PREFIX):]
            if e.get("sourceHandle") != out_handle(variable):
                intent_store.write_manual_edge(db, {**e, "sourceHandle": out_handle(variable)})
                repointed += 1
    kept = {out_handle(p) for p in names}
    for e in from_node:
        handle = e.get("sourceHandle")
        if names and handle and handle.startswith(OUT_HANDLE_PREFIX) and handle not in kept:
            if not any(handle == out_handle(new) for new in old_of):
                pipeline_store.delete_manual_edge(db, e["id"])
                removed += 1

    config = pipeline_store.get_node_config(db, node_id)
    if names:
        config = {**config, "outputPorts": names}
    else:
        config = {k: v for k, v in config.items() if k != "outputPorts"}
    pipeline_store.update_node_config(db, node_id, config)
    logger.info(
        "[layout_service] set_output_ports (node_id=%r) -> %s; renames %s re-pointed "
        "%d edge(s); variables before %s; %d edge(s) written, %d removed",
        node_id,
        names,
        renames or {},
        repointed,
        variables_before,
        materialized,
        removed,
    )
    _notify_dag_updated("set_output_ports")  # the node's ports change
    return {"ok": True, "outputPorts": names, "repointed": repointed}


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
    declares, without paying for a full ``refresh_module()``/``registry_reload_service.refresh_registries()``
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
