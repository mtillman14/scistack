"""
Opening a project without the GUI (no server, no frontend, no stdin loop).

Export and import run from the terminal as well as from the GUI
(``.claude/plan-portability.md`` Stage 3). Both open a project through the
same ``bootstrap.open_or_create_project`` the GUI's two entry points use --
there is no second "open a project" sequence -- and differ only in whether
code is discovered:

* :func:`open_for_export` discovers. A canvas is built with the code
  registry (function signatures give nodes their ports, PathInput history is
  matched to declarations by template, Parameters come from source), so a
  capture without it would not be the canvas the user sees. The code is the
  exporter's own, so importing it is no different from opening the GUI.
* :func:`open_for_import` does not. Applying a canvas needs only the database
  and the layout file, and an imported bundle's code must not run before the
  user has trusted it. ``portability_service.import_pipeline_document`` is
  then called with ``discovered=False`` and reports what it deferred.

Every service already reads the opened project through the process globals
``bootstrap`` sets (``scistack_gui.db``, the pinned project root), exactly as
in a GUI session.
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def open_for_export(db_path: "Path | str", *, project: "Path | None" = None):
    """Open an existing project WITH discovery; return the open database."""
    return _open(db_path, project=project, discover=True, schema_keys=None)


def open_for_import(
    db_path: "Path | str",
    *,
    project: "Path | None" = None,
    schema_keys: "list[str] | None" = None,
):
    """Open (or, with *schema_keys*, create) a project WITHOUT discovering or
    importing any code; return the open database."""
    return _open(db_path, project=project, discover=False, schema_keys=schema_keys)


def _open(db_path, *, project, discover: bool, schema_keys):
    from scistack_gui import db as gui_db
    from scistack_gui.bootstrap import open_or_create_project

    db_path = Path(db_path)
    # One open project per process, as in the GUI: close any earlier one first
    # (DuckDB refuses a second connection to a file this process holds).
    if gui_db.is_loaded():
        previous = gui_db.close_db()
        logger.info("[headless] closed %s before opening %s", previous, db_path)
    if project is not None:
        from scistack_gui.config import set_project_root_hint

        set_project_root_hint(project)
    result = open_or_create_project(
        db_path, schema_keys=schema_keys, discover=discover
    )
    logger.info(
        "[headless] opened %s (discover=%s): %d function(s), %d variable(s) "
        "registered; %d warning(s)",
        db_path,
        discover,
        result.functions_loaded,
        result.variables_loaded,
        len(result.warnings),
    )
    for warning in result.warnings:
        logger.warning("[headless] %s", warning)
    return gui_db.get_db()
