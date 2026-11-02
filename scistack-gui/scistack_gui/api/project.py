"""
Project config panel: the handler table for both transports
(``api/handlers.py``; ``PROJECT_HANDLERS`` at the bottom).

    GET    /api/project/code           get_project_code   — scanned exports from src/{project}/
    GET    /api/project/paths          get_project_paths  — resolved scistack.toml paths (Paths popup)
    POST   /api/project/paths          add_project_path   — add a discovery path (loose-script projects)
    DELETE /api/project/paths          remove_project_path
    POST   /api/project/entities-file  set_entities_file
    DELETE /api/project/entities-file  clear_entities_file
    POST   /api/project/refresh        refresh_project    — re-run both scans
    GET    /api/project/clear-data/plan clear_data_plan — what a clear would delete
    POST   /api/project/clear-data     clear_data         — delete records, keep the canvas

Every mutation notifies ``dag_updated`` on success (the row says so), on
both transports alike. The scan logic is ``services/project_service.py``.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter
from pydantic import BaseModel

from scistack_gui.api.handlers import Handler, install_routes
from scistack_gui.services import project_service

logger = logging.getLogger(__name__)

router = APIRouter(tags=["project"])


# ---------------------------------------------------------------------------
# The handler table
# ---------------------------------------------------------------------------


class PathBody(BaseModel):
    path: str | None = ""


def _get_project_code() -> dict:
    return project_service.get_project_code()


def _get_project_paths() -> dict:
    return project_service.get_project_paths()


def _add_project_path(req: PathBody) -> dict:
    return project_service.add_project_path(req.path or "")


def _remove_project_path(req: PathBody) -> dict:
    return project_service.remove_project_path(req.path or "")


def _set_entities_file(req: PathBody) -> dict:
    return project_service.set_project_entities_file(req.path or None)


def _clear_entities_file() -> dict:
    return project_service.clear_project_entities_file()


def _refresh_project() -> dict:
    return project_service.refresh_project_sync()


class ClearDataRequest(BaseModel):
    #: The plan's fingerprint the user confirmed (clear_data_plan).
    fingerprint: str
    reason: str = "cleared from the GUI"
    backup: bool = False


def _clear_data_plan(db) -> dict:
    from scistack_gui.services.clear_service import clear_data_plan

    return clear_data_plan(db)


def _clear_data(db, req: ClearDataRequest) -> dict:
    from scistack_gui.services.clear_service import clear_data

    return clear_data(db, req.reason, req.fingerprint, backup=req.backup)


_NO_DB = {"needs_db": False}

PROJECT_HANDLERS: tuple[Handler, ...] = (
    Handler("get_project_code", "/project/code", None, _get_project_code, http_method="GET", **_NO_DB),
    Handler("get_project_paths", "/project/paths", None, _get_project_paths, http_method="GET", **_NO_DB),
    Handler("add_project_path", "/project/paths", PathBody, _add_project_path, notify_dag_updated=True, **_NO_DB, undoable=True, undo_label="add path"),
    # The browser sends the path as a query parameter (DELETE without a body).
    Handler("remove_project_path", "/project/paths", PathBody, _remove_project_path, http_method="DELETE", body=False, notify_dag_updated=True, **_NO_DB, undoable=True, undo_label="remove path"),
    Handler("set_entities_file", "/project/entities-file", PathBody, _set_entities_file, notify_dag_updated=True, **_NO_DB, undoable=True, undo_label="set entities file"),
    Handler("clear_entities_file", "/project/entities-file", None, _clear_entities_file, http_method="DELETE", notify_dag_updated=True, **_NO_DB, undoable=True, undo_label="clear entities file"),
    Handler("refresh_project", "/project/refresh", None, _refresh_project, notify_dag_updated=True, **_NO_DB, undoable=False),
    # Clear all data, keep the pipeline (plan-clear-data.md, GUI half).
    # Not undoable: it deletes records (a tombstone and the optional backup
    # are the way back).
    Handler("clear_data_plan", "/project/clear-data/plan", None, _clear_data_plan, http_method="GET", undoable=False),
    Handler("clear_data", "/project/clear-data", ClearDataRequest, _clear_data, notify_dag_updated=True, undoable=False),
)

install_routes(router, PROJECT_HANDLERS)
