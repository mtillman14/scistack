"""
Manual built-in/library function references — the handler table for both
transports (``api/handlers.py``).

    POST /api/functions/builtin   create_builtin_function

The validation/registration logic lives in
``services/builtin_function_service.py``.
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


BUILTIN_FUNCTION_HANDLERS: tuple[Handler, ...] = (
    Handler("create_builtin_function", "/functions/builtin", CreateBuiltinFunctionRequest, _create_builtin_function, needs_db=False),
)

install_routes(router, BUILTIN_FUNCTION_HANDLERS)
