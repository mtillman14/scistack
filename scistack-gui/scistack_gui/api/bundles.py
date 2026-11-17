"""
Whole-project bundles from the GUI (``.claude/plan-portability.md`` Stage 8)
-- the handler table for both transports (``api/handlers.py``).

    GET    /api/bundles/export-options   get_export_options
    POST   /api/bundles/export           export_project_bundle
    POST   /api/bundles/verify           verify_reproduction -- your re-run vs the exporter (scidb.project.verify)

Export writes the session's OWN, already-open project through
``headless.export_open_project`` -- the same composition ``scistack export``
reaches after opening -- so the GUI and the terminal write the same bundle.
The offered options and their defaults come from
``bundle_cli.export_choices`` (defaults from ``scidb.project.bundle.ExportOptions``).

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
    #: Write ``<name>.scistack.zip`` (the bundle wrapped in a plain zip,
    #: ``scidb.project.bundle.zip_for_attachment``) instead of the bare
    #: ``.scistack``: the bug reporter's file, which GitHub must accept as an
    #: attachment (issue #20).
    attachable: bool = False


def _get_export_options() -> dict:
    """The export dialog's checkboxes and a suggested file name."""
    from scidb.project.bundle import EXTENSION

    from scistack_gui.bundle_cli import export_choices
    from scistack_gui.runtime.db import get_db_path

    return {
        "choices": export_choices(),
        "extension": EXTENSION,
        "default_name": f"{Path(get_db_path()).stem}{EXTENSION}",
    }


def _export_project_bundle(db, req: ExportBundle) -> dict:
    from scidb.project.bundle import ExportOptions

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
    from scidb.project.bundle import ExportReport

    report = ExportReport()
    if req.attachable:
        path = _export_attachable(out, options, db, report)
    else:
        path = export_open_project(out, options=options, db=db, report=report)
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "options": options.to_dict(),
        "redaction": report.redaction,
        "warnings": report.warnings,
    }


def _export_attachable(out: Path, options, db, report) -> Path:
    """The bundle goes to a temporary folder and only the ``.zip`` lands at
    *out*, so the user's folder holds the one file to attach. The inner
    bundle is named after *out* without its ``.zip``/``.scistack``."""
    import tempfile

    from scidb.project.bundle import EXTENSION, zip_for_attachment

    from scistack_gui.headless import export_open_project

    stem = out.name
    for suffix in (".zip", EXTENSION):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
    with tempfile.TemporaryDirectory(prefix="scistack-attach-") as tmp:
        inner = export_open_project(Path(tmp) / f"{stem}{EXTENSION}", options=options, db=db, report=report)
        path = zip_for_attachment(inner, out.with_name(f"{stem}{EXTENSION}.zip"))
    logger.info("[bundles] attachable export -> %s (%d bytes)", path, path.stat().st_size)
    return path


class VerifyRequest(BaseModel):
    #: A run-history folder or a .scistack file; None = the imported run history (scidb.project.imported).
    against: str | None = None
    rtol: float | None = None
    atol: float | None = None


def _verify(db, req: VerifyRequest) -> dict:
    """Read-only, in the session's own process: a separate process cannot
    open the database this session holds."""
    from scidb.project.verify import DEFAULT_ATOL, DEFAULT_RTOL, verify
    from scifor.project.root import project_root

    report = verify(
        db,
        root=project_root(),
        against=req.against or None,
        rtol=DEFAULT_RTOL if req.rtol is None else req.rtol,
        atol=DEFAULT_ATOL if req.atol is None else req.atol,
    )
    return report.to_dict()


_BAD_REQUEST = {ValueError: 400}

BUNDLE_HANDLERS: tuple[Handler, ...] = (
    Handler("get_export_options", "/bundles/export-options", None, _get_export_options, http_method="GET", needs_db=False),
    Handler("verify_reproduction", "/bundles/verify", VerifyRequest, _verify, http_errors=_BAD_REQUEST, undoable=False),
    Handler("export_project_bundle", "/bundles/export", ExportBundle, _export_project_bundle, http_errors=_BAD_REQUEST, undoable=False),
)

install_routes(router, BUNDLE_HANDLERS)
