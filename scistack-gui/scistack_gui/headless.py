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
    return _open(db_path, project=project, discover=True, schema_keys=None)[0]


def open_for_import(
    db_path: "Path | str",
    *,
    project: "Path | None" = None,
    schema_keys: "list[str] | None" = None,
):
    """Open (or, with *schema_keys*, create) a project WITHOUT discovering or
    importing any code; return the open database."""
    return _open(db_path, project=project, discover=False, schema_keys=schema_keys)[0]


def _open(db_path, *, project, discover: bool, schema_keys):
    """Open the project; return (the open database, the bootstrap result)."""
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
    return gui_db.get_db(), result


# ---------------------------------------------------------------------------
# Whole-project bundles (scidb.bundle), composed for the terminal and the GUI
# ---------------------------------------------------------------------------


def bundle_providers() -> list:
    """The bundle sections this installation can write and read, beyond
    scidb's own ``config``: the GUI's canvas and the saved plots/presets.
    The one list both directions use."""
    from scistackplotdb.bundle_section import PlotsSection

    from scistack_gui.bundle_section import CodeSection, GuiSection

    return [CodeSection(), GuiSection(), PlotsSection()]


def export_open_project(out_path: "Path | str", *, options=None, db=None) -> Path:
    """Write the project THIS process has open as a ``.scistack``. The one
    export composition: the terminal reaches it through
    :func:`export_project_bundle` (which opens first), the GUI's
    ``export_project_bundle`` handler calls it on the session's own,
    already-discovered project (and passes the handler's *db*)."""
    from scidb.bundle import export_project
    from scifor.pathinput import project_root

    from scistack_gui import db as gui_db

    db = db if db is not None else gui_db.get_db()
    out = export_project(
        project_root(), db, out_path, options=options, providers=bundle_providers()
    )
    logger.info("[headless] exported %s to %s (options %s)", gui_db.get_db_path(), out, options)
    return out


def export_project_bundle(db_path: "Path | str", out_path: "Path | str", *, options=None) -> Path:
    """Open the project WITH discovery and write it as a ``.scistack``."""
    open_for_export(db_path)
    return export_open_project(out_path, options=options)


def preview_bundle(bundle_path: "Path | str") -> dict:
    """``scidb.bundle.preview`` with this installation's providers: what the
    import dialogs show (exporter schema, PathInputs, sections, environment).
    Runs no code."""
    from scidb.bundle import preview

    return preview(bundle_path, providers=bundle_providers())


def import_project_bundle(
    bundle_path: "Path | str",
    target_root: "Path | str",
    *,
    schema_keys: "list[str] | None" = None,
    key_map: "dict[str, str | None] | None" = None,
    path_roots: "dict[str, str] | None" = None,
    import_history: bool = True,
):
    """Make a NEW project at *target_root* from a ``.scistack``, opening it
    WITHOUT discovery (the bundle's code never runs here). *schema_keys*,
    *key_map* (exporter key -> yours, or None), *path_roots* (PathInput
    name -> your folder) and *import_history* as in
    ``scidb.bundle.import_project``."""
    from scidb.bundle import import_project

    root = Path(target_root)

    def _open(db_path, keys):
        return open_for_import(db_path, project=root, schema_keys=keys)

    return import_project(
        bundle_path,
        root,
        providers=bundle_providers(),
        schema_keys=schema_keys,
        key_map=key_map,
        path_roots=path_roots,
        import_history=import_history,
        open_db=_open,
    )


def check_code(db_path: "Path | str", *, project: "Path | None" = None) -> dict:
    """Open the project WITH discovery -- this imports its code, so only
    after the user trusted it -- and list every canvas node whose function
    or variable was not found (import step I15).

    Returns ``{"unresolved_labels": [...], "functions_loaded",
    "variables_loaded", "warnings": [...]}``."""
    from scistack_gui import pipeline_store as ps
    from scistack_gui.services import canvas_snapshot, portability_service

    db, result = _open(db_path, project=project, discover=True, schema_keys=None)
    pipeline_ids = [p["pipeline_id"] for p in ps.list_pipelines(db)]
    snap = canvas_snapshot.capture(db, pipeline_ids)
    unresolved = portability_service.unresolved_labels(snap)
    out = {
        "unresolved_labels": unresolved,
        "functions_loaded": result.functions_loaded + result.matlab_functions_loaded,
        "variables_loaded": result.variables_loaded + result.matlab_variables_loaded,
        "warnings": list(result.warnings),
    }
    logger.info(
        "[headless] check_code %s: %d pipeline(s), %d node(s), %d unresolved %s",
        db_path, len(pipeline_ids), len(snap.nodes), len(unresolved), unresolved,
    )
    return out
