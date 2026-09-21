"""
Layout, notes, Parameters, PathInputs, edges and node config — the handler
table for both transports (``api/handlers.py``).

    GET    /api/layout                          get_layout
    PUT    /api/layout/{node_id}                put_layout
    DELETE /api/layout/{node_id}                delete_layout
    PUT    /api/layout/{node_id}/config         put_node_config
    GET    /api/notes                           get_notes
    PUT    /api/notes/{key}                     set_note
    GET    /api/parameters                      get_parameters
    POST   /api/parameters                      create_parameter
    PUT    /api/parameters/{name}               update_parameter
    DELETE /api/parameters/{name}               delete_parameter
    POST   /api/parameters/{name}/refresh-source refresh_parameter_source
    GET    /api/path-inputs                     get_path_inputs
    POST   /api/path-inputs                     create_path_input
    PUT    /api/path-inputs/{name}              update_path_input
    DELETE /api/path-inputs/{name}              delete_path_input
    POST   /api/path-inputs/{node_id}/deep-copy deep_copy_path_input
    PUT    /api/edges/{edge_id}                 put_edge
    DELETE /api/edges/{edge_id}                 delete_edge
    POST   /api/edges/{edge_id}/unhide          unhide_edge
    GET    /api/edges/hidden                    get_hidden_edges

Every mutation here notifies through the service itself
(``layout_service._notify_dag_updated`` — wiring changes only, never a
position-only write), so no row needs ``notify_dag_updated``.
"""

import logging

from fastapi import APIRouter
from pydantic import BaseModel

from scistack_gui.api.handlers import Handler, install_routes
from scistack_gui.ids import ROOT_SCOPE
from scistack_gui.services import layout_service

logger = logging.getLogger(__name__)

router = APIRouter()


class ScopeQuery(BaseModel):
    pipeline_id: str | None = ROOT_SCOPE


class HiddenEdgesQuery(BaseModel):
    #: None = every scope's hidden edges.
    pipeline_id: str | None = None


class NodeRef(BaseModel):
    node_id: str


class PositionUpdate(BaseModel):
    node_id: str
    x: float
    y: float
    # Present only when the node was just dragged from the sidebar palette.
    node_type: str | None = None
    label: str | None = None
    # Scope the node lives on (nested pipelines); default = root canvas.
    pipeline_id: str | None = ROOT_SCOPE


class NodeConfigUpdate(BaseModel):
    node_id: str
    config: dict = {}


class NoteUpdate(BaseModel):
    key: str
    text: str


class ParameterCreate(BaseModel):
    name: str
    # One list whatever the count. Numbers stay int-or-float rather than
    # being coerced to float: the hidden-value store keys on the RENDERED
    # string, so silently turning 20 into 20.0 makes an unchecked '20' stop
    # matching (see domain.variant_resolver.is_hidden_value, which tolerates
    # both spellings precisely because this field preserves the difference).
    values: list[float | int | str | bool] | None = None


class ParameterUpdate(BaseModel):
    name: str
    values: list[float | int | str | bool] = []
    description: str | None = ""
    # {"kind": "range"|"list", "spec": {...}} when these values were written
    # in one go by the panel's Generate section; absent for a value added one
    # at a time. Display grouping only — never written to source.
    group: dict | None = None


class NamedInScope(BaseModel):
    name: str
    pipeline_id: str | None = ROOT_SCOPE


class ParameterName(BaseModel):
    name: str | None = None


class PathInputCreate(BaseModel):
    name: str
    template: str | None = ""
    root_folder: str | None = None


class PathInputUpdate(BaseModel):
    name: str
    template: str
    root_folder: str | None = None
    alternate_templates: list[dict] | None = None


class EdgeCreate(BaseModel):
    edge_id: str
    source: str
    target: str
    source_handle: str | None = None
    target_handle: str | None = None


class EdgeDelete(BaseModel):
    edge_id: str
    # Optional: the frontend already has the removed edge's endpoints in
    # local React Flow state — passed through so a hidden DB-derived edge
    # can be labeled in the restore panel. Absent for edges deleted some
    # other way (defaults keep the DELETE body optional).
    source: str | None = ""
    target: str | None = ""
    source_handle: str | None = None
    target_handle: str | None = None


class EdgeInScope(BaseModel):
    edge_id: str
    pipeline_id: str | None = ROOT_SCOPE


# --- the calls ---------------------------------------------------------------


def _get_layout(req: ScopeQuery) -> dict:
    return layout_service.get_layout(req.pipeline_id or ROOT_SCOPE)


def _put_layout(req: PositionUpdate) -> dict:
    return layout_service.put_layout(
        req.node_id, req.x, req.y, req.node_type, req.label, req.pipeline_id or ROOT_SCOPE
    )


def _delete_layout(req: NodeRef) -> dict:
    return layout_service.delete_layout(req.node_id)


def _put_node_config(db, req: NodeConfigUpdate) -> dict:
    return layout_service.put_node_config(db, req.node_id, req.config or {})


def _get_notes() -> dict:
    return layout_service.get_notes()


def _set_note(req: NoteUpdate) -> dict:
    return layout_service.set_note(req.key, req.text)


def _get_parameters() -> list:
    return layout_service.get_parameters()


def _create_parameter(req: ParameterCreate) -> dict:
    """Create a Parameter. ``values`` is the final, already-computed flat
    list -- range generation (start/end/step) is a frontend concern. Empty
    creates a Parameter with NO values, matching the 'New parameter' form,
    which only collects a name; it used to scaffold a placeholder ``0``,
    which is indistinguishable from a declared value once written."""
    return layout_service.create_parameter(req.name, req.values)


def _update_parameter(req: ParameterUpdate) -> dict:
    """Rewrite an existing Parameter's declaration in source.

    One route whatever the value count -- adding a value is adding an
    argument, not a change of kind (D6).

    Returns ``{"ok": False, "reason": "read_only", "file", "line"}`` when the
    Parameter is declared outside the configured entities file, so the
    frontend can render "declared in foo.py:42" rather than a generic hint.
    An empty ``values`` is accepted -- a Parameter's value set may be empty at
    any time, not only at creation -- and anything wired to it then fails
    loudly at run rather than running with an invented value.

    ``group`` marks the list as one generated set, for display only.
    """
    return layout_service.update_parameter(
        req.name, req.values, req.description or "", req.group
    )


def _delete_parameter(req: NamedInScope) -> dict:
    """Hides the node only — the source declaration is untouched."""
    return layout_service.delete_parameter(req.name, req.pipeline_id or ROOT_SCOPE)


def _refresh_parameter_source(req: ParameterName) -> dict:
    """Re-read the entities file so an externally hand-edited value (e.g. a
    dict/struct too complex for the sidebar's add-value form) shows up
    without the full "Refresh Code" rescan. *name* scopes nothing server-side
    (the reload is whole-file) — it is here for REST-consistency with the
    other single-name parameter routes and for logging only."""
    return layout_service.refresh_parameter_source(req.name)


def _get_path_inputs() -> list:
    result = layout_service.get_path_inputs()
    logger.info("get_path_inputs → %s", result)
    return result


def _create_path_input(req: PathInputCreate) -> dict:
    return layout_service.create_path_input(req.name, req.template or "", req.root_folder)


def _update_path_input(req: PathInputUpdate) -> dict:
    """Rewrite an existing PathInput's declaration in source.

    ``alternate_templates`` re-renders it as ``EachOf(PathInput(...), ...)``
    under the same name — that is what "multiple templates" is, not a
    separate concept.
    """
    return layout_service.update_path_input(
        req.name, req.template, req.root_folder, req.alternate_templates
    )


def _delete_path_input(req: NamedInScope) -> dict:
    """Hides the node only — the source declaration is untouched (never
    delete, mark hidden). To CHANGE a template, use ``update_path_input``."""
    return layout_service.delete_path_input(req.name, req.pipeline_id or ROOT_SCOPE)


def _deep_copy_path_input(req: NodeRef) -> dict:
    """Opt-in fork: give this ONE PathInput node placement an independent
    named definition, leaving every other placement of the original name
    untouched (see layout_service.deep_copy_path_input)."""
    return layout_service.deep_copy_path_input(req.node_id)


def _put_edge(db, req: EdgeCreate) -> dict:
    return layout_service.put_edge(
        db, req.edge_id, req.source, req.target, req.source_handle, req.target_handle
    )


def _delete_edge(db, req: EdgeDelete) -> dict:
    return layout_service.delete_edge(
        db,
        req.edge_id,
        req.source or "",
        req.target or "",
        req.source_handle,
        req.target_handle,
    )


def _unhide_edge(db, req: EdgeInScope) -> dict:
    return layout_service.unhide_edge(db, req.edge_id, req.pipeline_id or ROOT_SCOPE)


def _get_hidden_edges(db, req: HiddenEdgesQuery) -> list:
    return layout_service.get_hidden_edges(db, req.pipeline_id)


_BAD_REQUEST = {ValueError: 400}

LAYOUT_HANDLERS: tuple[Handler, ...] = (
    Handler("get_layout", "/layout", ScopeQuery, _get_layout, needs_db=False, http_method="GET"),
    Handler("put_layout", "/layout/{node_id}", PositionUpdate, _put_layout, needs_db=False, http_method="PUT"),
    Handler("delete_layout", "/layout/{node_id}", NodeRef, _delete_layout, needs_db=False, http_method="DELETE"),
    Handler("put_node_config", "/layout/{node_id}/config", NodeConfigUpdate, _put_node_config, http_method="PUT"),
    Handler("get_notes", "/notes", None, _get_notes, needs_db=False, http_method="GET"),
    Handler("set_note", "/notes/{key:path}", NoteUpdate, _set_note, needs_db=False, http_method="PUT"),
    Handler("get_parameters", "/parameters", None, _get_parameters, needs_db=False, http_method="GET"),
    Handler("create_parameter", "/parameters", ParameterCreate, _create_parameter, needs_db=False),
    Handler("update_parameter", "/parameters/{name}", ParameterUpdate, _update_parameter, needs_db=False, http_method="PUT"),
    Handler("delete_parameter", "/parameters/{name}", NamedInScope, _delete_parameter, needs_db=False, http_method="DELETE", body=True),
    Handler("refresh_parameter_source", "/parameters/{name}/refresh-source", ParameterName, _refresh_parameter_source, needs_db=False, body=False),
    Handler("get_path_inputs", "/path-inputs", None, _get_path_inputs, needs_db=False, http_method="GET"),
    Handler("create_path_input", "/path-inputs", PathInputCreate, _create_path_input, needs_db=False),
    Handler("update_path_input", "/path-inputs/{name}", PathInputUpdate, _update_path_input, needs_db=False, http_method="PUT"),
    Handler("delete_path_input", "/path-inputs/{name}", NamedInScope, _delete_path_input, needs_db=False, http_method="DELETE", body=True),
    Handler("deep_copy_path_input", "/path-inputs/{node_id}/deep-copy", NodeRef, _deep_copy_path_input, needs_db=False, http_errors=_BAD_REQUEST, body=False),
    Handler("put_edge", "/edges/{edge_id}", EdgeCreate, _put_edge, http_method="PUT", http_errors=_BAD_REQUEST),
    Handler("delete_edge", "/edges/{edge_id}", EdgeDelete, _delete_edge, http_method="DELETE", body=True),
    Handler("unhide_edge", "/edges/{edge_id}/unhide", EdgeInScope, _unhide_edge, body=True),
    Handler("get_hidden_edges", "/edges/hidden", HiddenEdgesQuery, _get_hidden_edges, http_method="GET"),
)

install_routes(router, LAYOUT_HANDLERS)
