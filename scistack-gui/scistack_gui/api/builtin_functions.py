"""
Manual built-in/library function references — the handler table for both
transports (``api/handlers.py``).

    POST /api/functions/builtin       create_builtin_function
    POST /api/functions/new           create_function       — write a new function file
    GET  /api/functions/{name}/code   get_function_code     — its source + editability
    PUT  /api/functions/{name}/code   save_function_code    — rewrite it (rolled back if it breaks)

The validation/registration logic lives in
``services/builtin_function_service.py``; new functions the user writes
are ``services/function_code_service.py``.
"""

import logging

from fastapi import APIRouter
from pydantic import BaseModel

from scistack_gui.api.handlers import Handler, install_routes

logger = logging.getLogger(__name__)

router = APIRouter()


class CreateBuiltinFunctionRequest(BaseModel):
    language: str  # "python" | "matlab"
    reference: str  # e.g. "numpy.mean", "len", "mean"


def _create_builtin_function(req: CreateBuiltinFunctionRequest) -> dict:
    from scistack_gui.services.builtin_function_service import create_builtin_function

    return create_builtin_function(req.language, req.reference)


class CreateFunctionRequest(BaseModel):
    name: str
    params: list[str] = []


class FunctionCodeRef(BaseModel):
    name: str


class SaveFunctionCodeRequest(BaseModel):
    name: str
    source: str


def _create_function(req: CreateFunctionRequest) -> dict:
    from scistack_gui.services.function_code_service import create_function

    return create_function(req.name, req.params)


def _get_function_code(req: FunctionCodeRef) -> dict:
    from scistack_gui.services.function_code_service import read_function_source

    return read_function_source(req.name)


def _save_function_code(req: SaveFunctionCodeRequest) -> dict:
    from scistack_gui.services.function_code_service import update_function_source

    return update_function_source(req.name, req.source)


BUILTIN_FUNCTION_HANDLERS: tuple[Handler, ...] = (
    Handler("create_builtin_function", "/functions/builtin", CreateBuiltinFunctionRequest, _create_builtin_function, needs_db=False, undoable=True, undo_label="add built-in function"),
    Handler("create_function", "/functions/new", CreateFunctionRequest, _create_function, needs_db=False, notify_dag_updated=True, undoable=True, undo_label="new function"),
    Handler("get_function_code", "/functions/{name}/code", FunctionCodeRef, _get_function_code, needs_db=False, http_method="GET"),
    Handler("save_function_code", "/functions/{name}/code", SaveFunctionCodeRequest, _save_function_code, needs_db=False, http_method="PUT", notify_dag_updated=True, undoable=True, undo_label="edit function code"),
)

install_routes(router, BUILTIN_FUNCTION_HANDLERS)
