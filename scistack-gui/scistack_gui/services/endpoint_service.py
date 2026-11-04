"""
Endpoint service — artifact/stat presentation data for endpoint nodes
(plan-endpoint-presentation.md, Part B item 1).

Read side rides on scidb's inspect facade: ``inspector.report(fn=...)``
already collects endpoint records with stamp verification
(figures) and parsed result JSON (stats) — this service only converts the
manifest dataclasses to JSON-safe dicts. Draft outputs never have records;
they reach the frontend through the show-run's ``show_rendered`` push, not
through here.
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from pathlib import Path

logger = logging.getLogger(__name__)

REPORT_DIRNAME = "scidb_report"


def endpoint_artifacts(db, fn_name: str) -> dict:
    """Recorded artifacts/stats for one endpoint function, JSON-safe.

    Shape: {"figures": [FigureEntry...], "stats": [StatEntry...],
    "warnings": [...]} — see scidb.inspect.report for the entry fields
    (artifact_path, artifact_exists, stamp_ok, schema, branch_params...).
    """
    data = db.inspect.report(fn=fn_name)
    result = {
        "figures": [asdict(f) for f in data.figures],
        "stats": [asdict(s) for s in data.stats],
        "warnings": list(data.warnings),
    }
    logger.info(
        "[endpoint_service] artifacts for %s: %d figure(s), %d stat(s), %d warning(s)",
        fn_name,
        len(result["figures"]),
        len(result["stats"]),
        len(result["warnings"]),
    )
    return result


def artifact_file_path(db, path: str) -> Path:
    """Resolve an artifact path for serving, guarded to the PROJECT DIR
    (the database file's parent, resolved) so the file route can't be
    used to read arbitrary files. Raises ValueError otherwise."""
    project_dir = Path(str(db.dataset_db_path)).resolve().parent
    resolved = Path(path).resolve()
    if project_dir not in resolved.parents and resolved != project_dir:
        logger.warning(
            "[endpoint_service] refused artifact path outside "
            "project dir: %s (project=%s)",
            resolved,
            project_dir,
        )
        raise ValueError(f"path is outside the project directory: {path}")
    if not resolved.is_file():
        raise FileNotFoundError(f"no such artifact file: {path}")
    return resolved


def write_report(db) -> dict:
    """Write the self-contained endpoint report next to the database and
    return the index.html path (embed=True → figures inlined, so the
    single file renders anywhere)."""
    out_dir = Path(str(db.dataset_db_path)).resolve().parent / REPORT_DIRNAME
    index = db.inspect.write_report(out_dir)
    logger.info("[endpoint_service] report written: %s", index)
    return {"ok": True, "index_path": str(index)}
