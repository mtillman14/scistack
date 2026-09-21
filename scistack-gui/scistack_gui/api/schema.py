"""
Database metadata — the handler table for both transports (``api/handlers.py``).

    GET /api/info        get_info        — metadata about the open database (header)
    GET /api/schema      get_schema      — schema keys and every key's distinct values
    GET /api/variables   get_variables_list
"""

from fastapi import APIRouter

from scistack_gui.api.handlers import Handler, install_routes

router = APIRouter()


def _get_info() -> dict:
    """Metadata about the open database (used by the frontend header)."""
    from scistack_gui.services.pipeline_service import get_info

    return get_info()


def _get_schema(db) -> dict:
    """Schema keys and all distinct values for each key."""
    from scistack_gui.services.pipeline_service import get_schema

    return get_schema(db)


def _get_variables_list() -> list:
    """All registered variable type names."""
    from scistack_gui.services.pipeline_service import get_variables_list

    return get_variables_list()


SCHEMA_HANDLERS: tuple[Handler, ...] = (
    Handler("get_info", "/info", None, _get_info, needs_db=False, http_method="GET"),
    Handler("get_schema", "/schema", None, _get_schema, http_method="GET"),
    Handler("get_variables_list", "/variables", None, _get_variables_list, needs_db=False, http_method="GET"),
)

install_routes(router, SCHEMA_HANDLERS)
