"""
GET /pipeline and the parameter / combo visibility routes: the handler table
for both transports (``api/handlers.py``). The graph itself is built in
``services/graph_service.py``.
"""

from fastapi import APIRouter
from pydantic import BaseModel

from scistack_gui.api.handlers import Handler, install_routes
from scistack_gui.ids import ROOT_SCOPE

router = APIRouter()


# ---------------------------------------------------------------------------
# The handler table — both transports (api/handlers.py)
# ---------------------------------------------------------------------------
#
#     GET    /api/pipeline                                   get_pipeline
#     GET    /api/function/{name}/params                     get_function_params
#     GET    /api/function/{name}/source                     get_function_source
#     GET    /api/function/{name}/doc                        get_function_doc
#     PUT    /api/parameters/{name}/pending/{value}          put_pending_constant
#     DELETE /api/parameters/{name}/pending/{value}          delete_pending_constant
#     POST   /api/functions/{function_name}/hidden_combos    hide_combo
#     DELETE /api/functions/hidden_combos/{node_id}          unhide_combo
#     GET    /api/functions/{function_name}/hidden_combos    list_hidden_combos
#     POST   /api/parameters/{name}/hidden_values/{value}    hide_parameter_value
#     DELETE /api/parameters/{name}/hidden_values/{value}    unhide_parameter_value
#     POST   /api/parameters/{name}/group_checked            set_parameter_group_checked
#     GET    /api/parameters/hidden_values                   list_hidden_parameter_values


class ScopeQuery(BaseModel):
    pipeline_id: str | None = ROOT_SCOPE


class PipelineQuery(ScopeQuery):
    #: False = the graph without per-node run states (the Plot Studio
    #: pickers draw none; the state check is most of a build's cost).
    run_states: bool = True


class FunctionName(BaseModel):
    name: str


class PendingValue(BaseModel):
    name: str
    value: str


class HideComboRequest(BaseModel):
    function_name: str
    node_id: str | None = None
    variant_key: dict


class NodeRef(BaseModel):
    node_id: str


class FunctionRef(BaseModel):
    function_name: str


class ParameterValueInScope(BaseModel):
    name: str
    value: str
    pipeline_id: str | None = ROOT_SCOPE


class ParameterGroupChecked(BaseModel):
    name: str
    values: list[str] = []
    checked: bool = True
    pipeline_id: str | None = ROOT_SCOPE


def _get_pipeline(db, req: PipelineQuery) -> dict:
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    return get_pipeline_graph(
        db, req.pipeline_id or ROOT_SCOPE, run_states=req.run_states
    )


def _get_function_params(req: FunctionName) -> dict:
    from scistack_gui.services.pipeline_service import get_function_full_info

    return get_function_full_info(req.name)


def _get_function_source(req: FunctionName) -> dict:
    from scistack_gui.services.pipeline_service import get_function_source

    return get_function_source(req.name)


def _get_function_doc(req: FunctionName) -> dict:
    from scistack_gui.services.pipeline_service import get_function_doc

    return get_function_doc(req.name)


def _put_pending_constant(db, req: PendingValue) -> dict:
    from scistack_gui.services.layout_service import put_pending_constant

    put_pending_constant(db, req.name, req.value)
    return {"ok": True}


def _delete_pending_constant(db, req: PendingValue) -> dict:
    from scistack_gui.services.layout_service import delete_pending_constant

    delete_pending_constant(db, req.name, req.value)
    return {"ok": True}


def _hide_combo(db, req: HideComboRequest) -> dict:
    from scistack_gui.services.layout_service import hide_variant_combo

    return hide_variant_combo(db, req.function_name, req.node_id, req.variant_key)


def _unhide_combo(db, req: NodeRef) -> dict:
    from scistack_gui.services.layout_service import unhide_variant_combo

    return unhide_variant_combo(db, req.node_id)


def _list_hidden_combos(db, req: FunctionRef) -> list:
    from scistack_gui.services.layout_service import get_hidden_combos

    return get_hidden_combos(db, req.function_name)


def _hide_parameter_value(db, req: ParameterValueInScope) -> dict:
    from scistack_gui.services.layout_service import hide_parameter_value

    return hide_parameter_value(db, req.name, req.value, req.pipeline_id or ROOT_SCOPE)


def _unhide_parameter_value(db, req: ParameterValueInScope) -> dict:
    from scistack_gui.services.layout_service import unhide_parameter_value

    return unhide_parameter_value(db, req.name, req.value, req.pipeline_id or ROOT_SCOPE)


def _set_parameter_group_checked(db, req: ParameterGroupChecked) -> dict:
    """Check/uncheck every member of a generated value set in one call —
    the per-value method would be one request per value."""
    from scistack_gui.services.layout_service import set_parameter_group_checked

    return set_parameter_group_checked(
        db, req.name, req.values, req.checked, req.pipeline_id or ROOT_SCOPE
    )


def _list_hidden_parameter_values(db, req: ScopeQuery) -> dict:
    from scistack_gui.services.layout_service import get_hidden_constant_values

    return get_hidden_constant_values(db, req.pipeline_id or ROOT_SCOPE)


PIPELINE_HANDLERS: tuple[Handler, ...] = (
    Handler("get_pipeline", "/pipeline", PipelineQuery, _get_pipeline, http_method="GET"),
    Handler("get_function_params", "/function/{name}/params", FunctionName, _get_function_params, needs_db=False, http_method="GET"),
    Handler("get_function_source", "/function/{name}/source", FunctionName, _get_function_source, needs_db=False, http_method="GET"),
    Handler("get_function_doc", "/function/{name}/doc", FunctionName, _get_function_doc, needs_db=False, http_method="GET"),
    Handler("put_pending_constant", "/parameters/{name}/pending/{value}", PendingValue, _put_pending_constant, http_method="PUT", notify_dag_updated=True, undoable=True, undo_label="add value"),
    Handler("delete_pending_constant", "/parameters/{name}/pending/{value}", PendingValue, _delete_pending_constant, http_method="DELETE", notify_dag_updated=True, undoable=True, undo_label="remove value"),
    Handler("hide_combo", "/functions/{function_name}/hidden_combos", HideComboRequest, _hide_combo, undoable=True, undo_label="hide combination"),
    Handler("unhide_combo", "/functions/hidden_combos/{node_id}", NodeRef, _unhide_combo, http_method="DELETE", undoable=True, undo_label="show combination"),
    Handler("list_hidden_combos", "/functions/{function_name}/hidden_combos", FunctionRef, _list_hidden_combos, http_method="GET"),
    Handler("hide_parameter_value", "/parameters/{name}/hidden_values/{value}", ParameterValueInScope, _hide_parameter_value, body=True, undoable=True, undo_label="hide value"),
    Handler("unhide_parameter_value", "/parameters/{name}/hidden_values/{value}", ParameterValueInScope, _unhide_parameter_value, http_method="DELETE", body=True, undoable=True, undo_label="show value"),
    Handler("set_parameter_group_checked", "/parameters/{name}/group_checked", ParameterGroupChecked, _set_parameter_group_checked, undoable=True, undo_label="check value group"),
    Handler("list_hidden_parameter_values", "/parameters/hidden_values", ScopeQuery, _list_hidden_parameter_values, http_method="GET"),
)

install_routes(router, PIPELINE_HANDLERS)
