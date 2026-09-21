"""
Glue nodes — list, create, read, save, remove, and the live column list the
code panel shows beside the editor. The handler table for both transports
(``api/handlers.py``).

    GET    /api/glue                 list_glue
    GET    /api/glue/{name}          get_glue
    GET    /api/glue/{name}/columns  get_glue_columns
    POST   /api/glue                 create_glue
    PUT    /api/glue                 save_glue
    DELETE /api/glue/{name}          delete_glue

A glue node has **no run method**, by design (D5). It is transient by
construction, so a standalone run would produce nothing and a state badge
would describe nothing; it executes only as part of a consuming function's
run. See ``docs/claude/free-code-glue-nodes.md`` §5, and ``api/run.py``'s
refusal of a glue node id.

Every mutation notifies ``dag_updated`` on success — on BOTH transports.
Until the table, only the HTTP routes did; the extension's canvas learned
of a new glue node only when something else refreshed it.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter
from pydantic import BaseModel

from scistack_gui.api.handlers import Handler, install_routes
from scistack_gui.services import glue_service

logger = logging.getLogger(__name__)

router = APIRouter()


class GlueName(BaseModel):
    name: str


class GlueColumnsQuery(BaseModel):
    name: str | None = None
    variable_type: str | None = ""


class CreateGlueRequest(BaseModel):
    name: str
    param: str | None = "value"
    language: str | None = "python"


class SaveGlueRequest(BaseModel):
    name: str
    source: str


def _list_glue() -> dict:
    return {"nodes": glue_service.list_glue_nodes()}


def _get_glue(req: GlueName) -> dict:
    return glue_service.read_glue_source(req.name)


def _get_glue_columns(req: GlueColumnsQuery) -> dict:
    """The columns a glue on ``variable_type`` actually receives.

    Read live on every panel open rather than scaffolded into the file as a
    comment: a comment goes stale the moment the node is rewired.
    """
    if not req.variable_type:
        return {
            "ok": False,
            "error": (
                "This glue node is not wired to a variable yet, so there are "
                "no columns to show."
            ),
        }
    return glue_service.input_columns(req.variable_type)


def _create_glue(req: CreateGlueRequest) -> dict:
    return glue_service.create_glue_node(
        req.name, param=req.param or "value", language=req.language or "python"
    )


def _save_glue(req: SaveGlueRequest) -> dict:
    """Write the edited body, then refresh the registry.

    The refresh is the whole point of the round-trip: the new body has a new
    hash, so the consuming function's glue chain hash changes, so its next
    run recomputes instead of skipping. Saving without refreshing would look
    identical in the panel and silently keep running the old body.
    """
    return glue_service.update_glue_source(req.name, req.source)


def _delete_glue(req: GlueName) -> dict:
    """Remove the node from the canvas. The source file is never unlinked."""
    return glue_service.delete_glue_node(req.name)


_NO_DB = {"needs_db": False}

GLUE_HANDLERS: tuple[Handler, ...] = (
    Handler("list_glue", "/glue", None, _list_glue, http_method="GET", **_NO_DB),
    Handler("get_glue", "/glue/{name}", GlueName, _get_glue, http_method="GET", **_NO_DB),
    Handler("get_glue_columns", "/glue/{name}/columns", GlueColumnsQuery, _get_glue_columns, http_method="GET", **_NO_DB),
    Handler("create_glue", "/glue", CreateGlueRequest, _create_glue, notify_dag_updated=True, **_NO_DB),
    Handler("save_glue", "/glue", SaveGlueRequest, _save_glue, http_method="PUT", notify_dag_updated=True, **_NO_DB),
    Handler("delete_glue", "/glue/{name}", GlueName, _delete_glue, http_method="DELETE", notify_dag_updated=True, **_NO_DB),
)

install_routes(router, GLUE_HANDLERS)
