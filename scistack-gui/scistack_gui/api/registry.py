"""
The function/variable registry — the handler table for both transports
(``api/handlers.py``).

    GET  /api/registry   get_registry    — every function and variable type the server knows
    POST /api/refresh    refresh_module  — re-import the user module(s); notifies dag_updated
"""

import logging

from fastapi import APIRouter

from scistack_gui.api.handlers import Handler, install_routes

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_registry() -> dict:
    from scistack_gui.services.pipeline_service import get_registry

    return get_registry()


def _refresh_module() -> dict:
    from scistack_gui.services.pipeline_service import refresh_module

    return refresh_module()


REGISTRY_HANDLERS: tuple[Handler, ...] = (
    Handler("get_registry", "/registry", None, _get_registry, needs_db=False, http_method="GET"),
    Handler("refresh_module", "/refresh", None, _refresh_module, needs_db=False, notify_dag_updated=True),
)

install_routes(router, REGISTRY_HANDLERS)
