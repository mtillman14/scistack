"""
Browser-frontend project-creation wizard endpoints.

POST /api/bootstrap/create — create a new .duckdb (folder + filename +
                              schema keys) and load pipeline code into it.
                              Also gives loose-script projects a scistack.toml
                              + an entities file (default
                              src/scistack_entities.toml) if they have none,
                              so a freshly-created project never sits in the
                              pure-folder-scan config state that
                              pre-existing projects opened without ever
                              running this endpoint still can (see
                              docs/claude/code-discovery-categories.md).
                              Create-only: a project that already declares
                              an entities file keeps it, whatever the
                              wizard's field says -- see
                              services.project_init_service.
POST /api/bootstrap/open   — open an existing .duckdb and load pipeline code

These exist so the standalone browser frontend can bootstrap a project the
same way the VS Code extension's "SciStack: Open Pipeline" wizard does
(extension/src/extension.ts), without requiring a --db path to already
exist when the server process starts. Both endpoints run the same sequence
__main__.py runs at CLI startup — see scistack_gui.bootstrap.

VS Code never hits these: server.py's JSON-RPC entry point always opens or
creates the database before the webview is shown, so its React bundle never
observes ``db_loaded: false`` from GET /api/info.
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from scistack_gui.bootstrap import open_or_create_project
from scistack_gui.services.pipeline_service import get_info

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/bootstrap", tags=["bootstrap"])


class CreateProjectRequest(BaseModel):
    folder: str
    filename: str
    schema_keys: list[str]
    module: str | None = None
    project: str | None = None
    entities_file: str | None = "src/scistack_entities.toml"
    """Where to create an entities file *if this project has none*.

    Relative to the *project root*, not to ``folder`` -- ``folder`` is where
    the database goes, which is typically a datasets directory. See
    ``config.resolve_project_root``. An explicit ``null`` opts out of project
    initialization entirely."""


class OpenProjectRequest(BaseModel):
    db_path: str
    module: str | None = None
    project: str | None = None


def _resolve_db_path(folder: str, filename: str) -> Path:
    filename = filename.strip()
    if not filename:
        raise HTTPException(status_code=400, detail="Filename must not be empty.")
    if "/" in filename or "\\" in filename:
        raise HTTPException(
            status_code=400, detail="Filename must not contain path separators."
        )
    if not filename.endswith(".duckdb"):
        filename += ".duckdb"
    return Path(folder).expanduser() / filename


@router.post("/create")
def create_project(req: CreateProjectRequest) -> dict:
    """Create a new database (folder must already exist) and load pipeline code."""
    logger.info(
        "[api.bootstrap] create request: folder=%s filename=%s schema_keys=%s "
        "module=%s project=%s",
        req.folder,
        req.filename,
        req.schema_keys,
        req.module,
        req.project,
    )
    folder = Path(req.folder).expanduser()
    if not folder.is_dir():
        raise HTTPException(
            status_code=404, detail=f"Folder does not exist: {folder}"
        )

    db_path = _resolve_db_path(req.folder, req.filename)
    if db_path.exists():
        # Checked here as well as inside open_or_create_project so the
        # frontend gets a 409 with this message rather than a bare
        # FileExistsError string.
        raise HTTPException(
            status_code=409, detail=f"Database already exists: {db_path}"
        )
    module = Path(req.module).expanduser() if req.module else None
    project = Path(req.project).expanduser() if req.project else None

    try:
        open_or_create_project(
            db_path,
            schema_keys=req.schema_keys,
            module=module,
            project=project,
            # An explicit null entities_file is an opt-out, so bootstrap's
            # own initialization must not put the files back.
            init_project_files=bool(req.entities_file),
            # Where to put one IF the project has none. This used to be a
            # separate eager config_mod.set_entities_file call right here,
            # which unconditionally re-pointed the key -- so creating a
            # database in a project that already declared its entities
            # somewhere else silently swapped in a new empty file and the
            # existing declarations vanished from the GUI. Routing it
            # through project_init_service makes it create-only, like every
            # other step there.
            entities_file=req.entities_file,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except FileExistsError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    logger.info("[api.bootstrap] created database at %s", db_path)
    return get_info()


@router.post("/open")
def open_project(req: OpenProjectRequest) -> dict:
    """Open an existing database and load pipeline code."""
    logger.info(
        "[api.bootstrap] open request: db_path=%s module=%s project=%s",
        req.db_path,
        req.module,
        req.project,
    )
    db_path = Path(req.db_path).expanduser()
    module = Path(req.module).expanduser() if req.module else None
    project = Path(req.project).expanduser() if req.project else None

    try:
        open_or_create_project(db_path, module=module, project=project)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    logger.info("[api.bootstrap] opened database at %s", db_path)
    return get_info()
