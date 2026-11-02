"""
Variables: the handler table for both transports (``VARIABLE_HANDLERS``;
``api/handlers.py``). The queries live in ``services/variable_service.py``.
"""

import logging

from fastapi import APIRouter
from pydantic import BaseModel

from scistack_gui.api.handlers import Handler, install_routes
from scistack_gui.services import variable_service

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# The handler table — both transports (api/handlers.py)
# ---------------------------------------------------------------------------
#
#     GET  /api/variables/{name}/records            get_variable_records
#     GET  /api/variables/{name}/plot-data          get_variable_plot_data
#     GET  /api/variables/{variable_type}/columns   get_variable_columns
#     POST /api/variables/column-groups/plan        plan_column_groups
#     POST /api/variables/create                    create_variable


class VariableName(BaseModel):
    name: str


class VariableColumnsQuery(BaseModel):
    variable_type: str | None = None
    #: The older spelling some callers still send.
    name: str | None = None


class ColumnGroupsPlanRequest(BaseModel):
    #: The selection as the picker would store it ({"columns", "iterate",
    #: "groups"?}).
    selection: dict | None = None
    #: The node's output types, in the order a run pairs them.
    output_names: list[str] = []


class CreateVariableRequest(BaseModel):
    name: str | None = ""
    docstring: str | None = None
    language: str | None = "python"


def _get_variable_records(db, req: VariableName) -> dict:
    return variable_service.get_variable_records(req.name, db)


def _get_variable_plot_data(db, req: VariableName) -> dict:
    return variable_service.get_variable_plot_data(req.name, db)


def _get_variable_columns(db, req: VariableColumnsQuery) -> dict:
    """Which columns a consumer of this variable receives — the Inputs
    column picker's live read (and the glue panel's, through the same
    service), on every open: a variable re-saved with a different shape
    changes this answer, and neither surface caches it.

    Response: ``{"ok", "variable_type", "data_columns", "schema_keys",
    "note"}``. A scalar/array-stored variable reports its single class-named
    column plus the note explaining why, rather than an empty list, so the
    picker can say something true instead of looking broken.
    """
    variable_type = req.variable_type or req.name or ""
    if not variable_type:
        return {
            "ok": False,
            "error": (
                "This parameter is not wired to a variable yet, so there are "
                "no columns to show."
            ),
        }
    return variable_service.input_columns(variable_type, db=db)


def _plan_column_groups(req: ColumnGroupsPlanRequest) -> dict:
    """Combine or split, the group pairing and any error for a for_columns
    selection on a node with these outputs (``column_selection.plan_groups``,
    which asks scifor's owners). Called by the Inputs picker on every edit."""
    from scistack_gui.domain import column_selection

    return column_selection.plan_groups(req.selection, req.output_names)


def _create_variable(req: CreateVariableRequest) -> dict:
    """Define a new BaseVariable subclass by appending it to the user's
    module file, then refresh the registry so it's immediately available.

    Deliberately does NOT notify ``dag_updated``: a freshly declared type
    has no DB records yet, so it cannot appear as a canvas node (variable
    nodes come from ``list_variables``, not from the type registry — see
    ``graph_builder.build_variable_nodes``), and nothing in the frontend
    renders a per-node "declared" state that a broadcast would need to
    refresh (the undeclared case is only ever surfaced at the run boundary,
    see ``layout_service.write_manual_node``). Broadcasting here used to
    force every connected client through a full pipeline refetch+relayout
    plus registry/path-input/parameter/hidden-pipeline refetches for a
    change the canvas can't show. The caller updates its own sidebar
    registry state directly instead, same as ``create_parameter`` /
    ``create_path_input`` already do.
    """
    name = (req.name or "").strip()
    logger.info("create_variable request: name=%r docstring=%r", name, req.docstring)
    return variable_service.create_variable(name, req.docstring, req.language or "python")


VARIABLE_HANDLERS: tuple[Handler, ...] = (
    Handler("get_variable_records", "/variables/{name}/records", VariableName, _get_variable_records, http_method="GET"),
    Handler("get_variable_plot_data", "/variables/{name}/plot-data", VariableName, _get_variable_plot_data, http_method="GET"),
    Handler("get_variable_columns", "/variables/{variable_type}/columns", VariableColumnsQuery, _get_variable_columns, http_method="GET"),
    Handler("plan_column_groups", "/variables/column-groups/plan", ColumnGroupsPlanRequest, _plan_column_groups, needs_db=False, undoable=False),
    Handler("create_variable", "/variables/create", CreateVariableRequest, _create_variable, needs_db=False, undoable=True, undo_label="new variable"),
)

install_routes(router, VARIABLE_HANDLERS)
