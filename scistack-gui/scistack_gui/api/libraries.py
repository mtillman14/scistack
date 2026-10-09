"""
Libraries: list, add, remove, sync, and placing their pipelines
(portability Stage 10a) -- the handler table for both transports.

    GET    /api/libraries                 list_libraries
    GET    /api/libraries/key-map         suggest_library_key_map
    POST   /api/libraries                 add_library     -- lists it in scistack.toml packages
    DELETE /api/libraries                 remove_library  -- unlists it (seeded pipelines stay)
    POST   /api/libraries/sync            sync_libraries  -- seed / re-sync now

A library's pipeline is placed with the ordinary ``add_pipeline_use``
(``api/scopes.py``), its binding's ``key_map`` taken from
``suggest_library_key_map``. Nothing here installs anything: a listed name
that is not importable is reported with what to do.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter
from pydantic import BaseModel

from scistack_gui.api.handlers import Handler, install_routes

logger = logging.getLogger(__name__)

router = APIRouter(tags=["libraries"])


class LibraryName(BaseModel):
    name: str


class LibraryPipelineRef(BaseModel):
    pipeline_id: str


def _list_libraries(db) -> dict:
    from scistack_gui.services.library_service import list_libraries

    return {"libraries": list_libraries(db)}


def _suggest_key_map(db, req: LibraryPipelineRef) -> dict:
    from scistack_gui.services.library_service import suggest_key_map

    return suggest_key_map(db, req.pipeline_id)


def _reload_and_sync(db) -> dict:
    from scistack_gui.db import get_db_path
    from scistack_gui.services.library_service import sync_libraries
    from scistack_gui.services.registry_reload_service import reload_registries_from_disk

    reload_registries_from_disk(get_db_path())
    return sync_libraries(db)


def _add_library(db, req: LibraryName) -> dict:
    import importlib.util

    from scistack_gui.config import add_package
    from scistack_gui.db import get_db_path
    from scistack_gui.services.library_service import list_libraries

    add_package(get_db_path(), req.name)
    top = req.name.strip().split(".")[0]
    installed = importlib.util.find_spec(top) is not None
    sync = _reload_and_sync(db) if installed else None
    import sys

    out = {
        "ok": True,
        "name": req.name.strip(),
        "installed": installed,
        "sync": sync,
        "libraries": list_libraries(db),
    }
    if not installed:
        out["hint"] = (
            f"'{top}' is not importable from this Python ({sys.executable}). Install it "
            f"(e.g. `{sys.executable} -m pip install <its distribution or folder>`), "
            "then Refresh."
        )
    logger.info("[libraries] add_library %s: installed=%s sync=%s", req.name, installed, sync)
    return out


def _remove_library(db, req: LibraryName) -> dict:
    from scistack_gui.config import remove_package
    from scistack_gui.db import get_db_path
    from scistack_gui.services.library_service import list_libraries

    remove_package(get_db_path(), req.name)
    sync = _reload_and_sync(db)
    logger.info("[libraries] remove_library %s: sync=%s", req.name, sync)
    return {"ok": True, "sync": sync, "libraries": list_libraries(db)}


def _sync_libraries(db) -> dict:
    from scistack_gui.services.library_service import sync_libraries

    return sync_libraries(db)


_BAD_REQUEST = {ValueError: 400, FileNotFoundError: 400}

LIBRARY_HANDLERS: tuple[Handler, ...] = (
    Handler("list_libraries", "/libraries", None, _list_libraries, http_method="GET"),
    Handler("suggest_library_key_map", "/libraries/key-map", LibraryPipelineRef, _suggest_key_map, http_method="GET", http_errors=_BAD_REQUEST),
    Handler("add_library", "/libraries", LibraryName, _add_library, http_errors=_BAD_REQUEST, notify_dag_updated=True, undoable=True, undo_label="add library"),
    Handler("remove_library", "/libraries", LibraryName, _remove_library, http_method="DELETE", body=False, http_errors=_BAD_REQUEST, notify_dag_updated=True, undoable=True, undo_label="remove library"),
    Handler("sync_libraries", "/libraries/sync", None, _sync_libraries, notify_dag_updated=True, undoable=False),
)

install_routes(router, LIBRARY_HANDLERS)
