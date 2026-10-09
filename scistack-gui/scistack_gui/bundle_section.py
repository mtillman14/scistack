"""
The ``gui`` section of a project bundle (``scidb.bundle``): every pipeline's
canvas, plus the project-wide GUI state they depend on.

Shares its parts with the single-pipeline export (one owner each):
``services.canvas_snapshot`` for the canvas, ``portability_service`` for the
global state (``export_globals`` / ``apply_globals``) and hypotheses. What is
different is the target: a whole-project import always goes into a NEW
project, so there is no reuse-or-fork decision -- every pipeline keeps its id,
and the root canvas ``main`` (which every new database already has) is filled
rather than forked into "main (imported)".

Hidden pipelines are not exported (hidden means removed from the project's
working set; feedback_never_delete_mark_hidden). Hidden edges are captured
but applied only when the bundle carries run history (portability Stage 7),
since they hide history-derived connections.
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)

GUI_FILE = "gui.json"
#: Version of the gui.json payload, inside a bundle whose format scidb owns.
GUI_FORMAT = 1


class GuiSection:
    name = "gui"

    def export(self, ctx) -> "dict[str, bytes]":
        """Capture every visible pipeline. The project must be open WITH
        discovery (``headless.open_for_export``): the canvas is built with the
        code registry."""
        from scistack_gui import pipeline_store as ps
        from scistack_gui.services import canvas_snapshot, portability_service

        db = ctx.db
        # A LIBRARY's pipelines are not the project's to carry: their content
        # comes from the installed library (re-seeded on the recipient's first
        # open), so only which ones exist travels, for the placements on the
        # project's canvases to point at (portability Stage 10a).
        library_rows = [r for r in ps.list_library_pipelines(db) if not r["hidden"]]
        library_ids = {r["pipeline_id"] for r in ps.list_library_pipelines(db)}
        pipelines = [p for p in ps.list_pipelines(db) if p["pipeline_id"] not in library_ids]
        pipeline_ids = [p["pipeline_id"] for p in pipelines]
        snap = canvas_snapshot.capture(db, pipeline_ids)
        payload = {
            "gui_format": GUI_FORMAT,
            "library_pipelines": [
                {k: r[k] for k in ("pipeline_id", "name", "library", "pipeline_name", "doc_pipeline_id")}
                for r in library_rows
            ],
            "pipelines": [
                {
                    "pipeline_id": p["pipeline_id"],
                    "name": p["name"],
                    "hypothesis": portability_service.hypothesis_of(db, p["pipeline_id"]),
                }
                for p in pipelines
            ],
            "canvas": snap.to_dict(),
            **portability_service.export_globals(db, snap, pipeline_ids),
        }
        logger.info(
            "[bundle_section] gui export: %d pipeline(s); %d library pipeline(s) by "
            "reference; canvas %s",
            len(pipelines),
            len(library_rows),
            snap.describe(),
        )
        files = {GUI_FILE: json.dumps(payload, indent=2, default=str).encode("utf-8")}
        if getattr(ctx.options, "include_data", False):
            files.update(_verbatim_export(db))
        return files

    def import_(self, ctx, files: "dict[str, bytes]") -> dict:
        """Write the canvas into the NEW project. ``ctx.db`` was opened by the
        front end WITHOUT discovery (``headless.open_for_import``), so nothing
        here runs the bundle's code; registry steps are deferred and
        reported."""
        from scistack_gui import pipeline_store as ps
        from scistack_gui.ids import ROOT_SCOPE
        from scistack_gui.services import canvas_snapshot, portability_service

        payload = json.loads(files[GUI_FILE].decode("utf-8"))
        if payload.get("gui_format") != GUI_FORMAT:
            raise ValueError(
                f"gui section format {payload.get('gui_format')!r}; this SciStack "
                f"reads {GUI_FORMAT}"
            )
        if getattr(ctx, "history_live", False) and VERBATIM_PREFIX + "tables.json" in files:
            return _verbatim_import(ctx.db, files)
        db = ctx.db
        local = portability_service.local_global_names(db, discovered=False)
        existing = {p["pipeline_id"]: p["name"] for p in ps.list_all_pipelines(db)}

        resolution: dict[str, str] = {}
        for p in payload["pipelines"]:
            pid = p["pipeline_id"]
            if pid == ROOT_SCOPE:
                if p["name"] != existing.get(ROOT_SCOPE):
                    ps.rename_pipeline(db, ROOT_SCOPE, p["name"])
            elif pid not in existing:
                ps.create_pipeline(db, p["name"], pipeline_id=pid)
            else:
                raise ValueError(f"the new project already has pipeline {pid!r}")
            resolution[pid] = pid
            portability_service.apply_hypothesis(db, pid, p.get("hypothesis"))

        # Library pipelines arrive EMPTY, owned, with no definition hash: the
        # first open with discovery finds the hash differs and seeds their
        # content from the installed library (services/library_service). The
        # import itself runs no code, so it cannot read the library here.
        from scistack_gui import library_lock

        library_placeholders = []
        with library_lock.library_writes():
            for row in payload.get("library_pipelines") or []:
                pid = row["pipeline_id"]
                if pid not in existing:
                    ps.create_pipeline(db, row["name"], pipeline_id=pid)
                ps.set_library_owner(
                    db, pid, row["library"], row["pipeline_name"], row["doc_pipeline_id"], ""
                )
                resolution[pid] = pid
                library_placeholders.append(f"{row['library']}/{row['pipeline_name']}")

        snap = canvas_snapshot.CanvasSnapshot.from_dict(payload.get("canvas") or {})
        # Into the recipient's schema when it differs (portability Stage 6).
        snap = canvas_snapshot.remap_schema_keys(snap, ctx.key_map, ctx.map_report)
        has_history = "history" in (ctx.manifest.get("sections") or {})
        old_to_new = canvas_snapshot.apply(db, snap, resolution, include_hides=has_history)
        globals_report = portability_service.apply_globals(
            db, payload, resolution, local, discovered=False
        )
        report = {
            "pipelines": len(resolution),
            "ids_written": len(old_to_new),
            "hides_applied": has_history,
            "library_pipelines": sorted(set(library_placeholders)),
            **globals_report,
        }
        logger.info("[bundle_section] gui import: %s", report)
        return report


# ---------------------------------------------------------------------------
# The project's code, as files (a bundle import is a COPY)
# ---------------------------------------------------------------------------

#: Inside the code section, not a project file: the list of code paths
#: discovery loads from OUTSIDE the project root (not copied; Stage 6 roots).
EXTERNAL_FILE = ".scistack-external.json"

#: Never part of the code: the config travels in scidb's own section, and
#: the database and its sidecars are data.
_NOT_CODE_SUFFIXES = (".duckdb", ".wal", ".pyc", ".layout.json")


class CodeSection:
    """The ``code`` section: the project's source tree.

    Exactly what discovery loads (``config.load_config``: modules, MATLAB
    sources, glue, the entities files) plus ``pyproject.toml`` and every
    file of the project's own package (``src/<pkg>/``, package data
    included). One list, the loader's, so the bundle carries the code the
    canvas was built from. Imported in the "files" phase: written before the
    new project is initialised, so its package IS the exporter's.
    """

    name = "code"
    phase = "files"  # scidb.bundle.PHASE_FILES

    def export(self, ctx) -> "dict[str, bytes]":
        from pathlib import Path

        from scifor.discovery import CONFIG_FILENAME, own_package_dir

        from scistack_gui.config import load_config

        root = Path(ctx.root).resolve()
        config = load_config(root, Path(str(ctx.db.dataset_db_path)))
        candidates: set[Path] = set()
        if (root / "pyproject.toml").is_file():
            candidates.add(root / "pyproject.toml")
        own = own_package_dir(root)
        if own is not None:
            candidates |= {
                p for p in own[1].rglob("*") if p.is_file() and "__pycache__" not in p.parts
            }
        for group in (
            config.modules,
            config.matlab_sources,
            config.matlab_functions,
            config.matlab_variables,
        ):
            candidates |= {Path(p) for p in group}
        for single in (
            config.entities_file,
            config.variable_file,
            config.matlab_entities_file,
        ):
            if single is not None and Path(single).is_file():
                candidates.add(Path(single))

        files: dict[str, bytes] = {}
        external: list[str] = []
        for path in sorted(candidates):
            resolved = path.resolve()
            try:
                rel = resolved.relative_to(root).as_posix()
            except ValueError:
                external.append(str(path))
                continue
            if rel == CONFIG_FILENAME or rel.endswith(_NOT_CODE_SUFFIXES):
                continue
            files[rel] = resolved.read_bytes()
        if external:
            files[EXTERNAL_FILE] = json.dumps(sorted(external), indent=2).encode("utf-8")
        logger.info(
            "[bundle_section] code export: %d file(s) from %s; %d outside the root "
            "(listed, not copied)",
            len(files) - (1 if external else 0),
            root,
            len(external),
        )
        return files

    def import_(self, ctx, files: "dict[str, bytes]") -> dict:
        from pathlib import Path

        root = Path(ctx.root)
        external = json.loads(files[EXTERNAL_FILE]) if EXTERNAL_FILE in files else []
        written = 0
        for rel, data in files.items():
            if rel == EXTERNAL_FILE:
                continue
            target = root / rel
            if target.exists():
                raise ValueError(f"{target} already exists; a bundle import makes a NEW project")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            written += 1
        if external:
            logger.warning(
                "[bundle_section] %d code path(s) were outside the exporter's project "
                "and are not in the bundle: %s",
                len(external),
                external,
            )
        report = {"files": written, "external": external}
        logger.info("[bundle_section] code import: %s", report)
        return report


# ---------------------------------------------------------------------------
# Verbatim mode: with the history and data live (portability Stage 7)
# ---------------------------------------------------------------------------

#: Inside the gui section: the GUI tables and layout file, copied exactly.
VERBATIM_PREFIX = "verbatim/"
LAYOUT_FILE = "layout.json"


def _verbatim_export(db) -> "dict[str, bytes]":
    """Every GUI table (``canvas_snapshot.table_portability``: canvas, global
    and history-tied alike) and the layout file, exactly as they are. Only
    meaningful where the run history goes live too: history-derived nodes
    keep their ids, ``_node_wiring`` keeps them attached to their history,
    and hidden edges hide connections that exist."""
    from scidb import table_copy

    from scistack_gui import layout as layout_store
    from scistack_gui.services.canvas_snapshot import table_portability

    files = {
        VERBATIM_PREFIX + rel: blob
        for rel, blob in table_copy.dump(db._duck, sorted(table_portability())).items()
    }
    layout = layout_store._layout_path()
    if layout.is_file():
        files[VERBATIM_PREFIX + LAYOUT_FILE] = layout.read_bytes()
    logger.info("[bundle_section] gui verbatim export: %d file(s)", len(files))
    return files


def _verbatim_import(db, files: "dict[str, bytes]") -> dict:
    from scidb import table_copy

    from scistack_gui import layout as layout_store

    tables = {
        rel[len(VERBATIM_PREFIX):]: blob
        for rel, blob in files.items()
        if rel.startswith(VERBATIM_PREFIX) and rel != VERBATIM_PREFIX + LAYOUT_FILE
    }
    loaded = table_copy.load(db._duck, tables)
    layout_bytes = files.get(VERBATIM_PREFIX + LAYOUT_FILE)
    if layout_bytes is not None:
        layout_store._layout_path().write_bytes(layout_bytes)
    report = {"mode": "verbatim", "tables": loaded["tables"], "layout": layout_bytes is not None}
    logger.info("[bundle_section] gui verbatim import: %s", report)
    return report
