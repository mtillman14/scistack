"""
Whole-project bundles from the GUI (``.claude/plan-portability.md`` Stage 8)
-- the handler table for both transports (``api/handlers.py``).

    GET    /api/bundles/export-options   get_export_options
    POST   /api/bundles/export           export_project_bundle

Export writes the session's OWN, already-open project through
``headless.export_open_project`` -- the same composition ``scistack export``
reaches after opening -- so the GUI and the terminal write the same bundle.
The offered options and their defaults come from
``bundle_cli.export_choices`` (defaults from ``scidb.bundle.ExportOptions``).

Import is not here: it makes a NEW project, whose database this session's
process must not open beside its own. The VS Code extension's "Import
Project Bundle" command runs ``python -m scistack_gui.bundle_cli import``
in a separate process and then opens the new project as its own session.
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter
from pydantic import BaseModel

from scistack_gui.api.handlers import Handler, install_routes

logger = logging.getLogger(__name__)

router = APIRouter(tags=["bundles"])


class ExportBundle(BaseModel):
    out_path: str
    #: ``{option name: bool}``; an option left out takes its default.
    options: dict[str, bool] | None = None


def _get_export_options() -> dict:
    """The export dialog's checkboxes and a suggested file name."""
    from scidb.bundle import EXTENSION

    from scistack_gui.bundle_cli import export_choices
    from scistack_gui.db import get_db_path

    return {
        "choices": export_choices(),
        "extension": EXTENSION,
        "default_name": f"{Path(get_db_path()).stem}{EXTENSION}",
    }


def _export_project_bundle(db, req: ExportBundle) -> dict:
    from scidb.bundle import ExportOptions

    from scistack_gui.bundle_cli import EXPORT_FLAGS
    from scistack_gui.headless import export_open_project

    unknown = sorted(set(req.options or {}) - set(EXPORT_FLAGS))
    if unknown:
        raise ValueError(f"unknown export option(s) {unknown}; offered: {sorted(EXPORT_FLAGS)}")
    options = ExportOptions(**(req.options or {}))
    out = Path(req.out_path).expanduser()
    if not out.is_absolute():
        raise ValueError(f"export path must be absolute, got {req.out_path!r}")
    logger.info("[bundles] export_project_bundle -> %s (options %s)", out, options)
    path = export_open_project(out, options=options, db=db)
    return {"path": str(path), "bytes": path.stat().st_size, "options": options.to_dict()}


_BAD_REQUEST = {ValueError: 400}

BUNDLE_HANDLERS: tuple[Handler, ...] = (
    Handler("get_export_options", "/bundles/export-options", None, _get_export_options, http_method="GET", needs_db=False),
    Handler("export_project_bundle", "/bundles/export", ExportBundle, _export_project_bundle, http_errors=_BAD_REQUEST, undoable=False),
)

install_routes(router, BUNDLE_HANDLERS)
