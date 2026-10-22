"""Clear every record, keep the pipeline: the GUI half.

``.claude/plan-clear-data.md`` (its GUI half, as re-planned by
``.claude/plan-canvas-as-document.md`` Stage 2 and decision Q3): with the
canvas a stored document, a clear needs no canvas snapshot and no restore.

1. Every canvas is built once, so adoption stores whatever history would
   draw that the document does not hold yet (nodes, edges, Parameter values).
2. ``scidb.clear_data.clear_data(..., allow_canvas=True)`` deletes the
   records and their run history. The document is untouched by construction.

The run log ``_run`` is kept (user decision Q4), and so are the GUI's own
history-class tables (``_node_wiring``, ``_adopted``): a re-run after the
clear is claimed by the node that states its wiring, exactly as before.

Undo history is kept: a clear touches no table undo tracks (the canvas, its
statements and ``_node_wiring`` are all untouched), so every recorded edit
still describes the canvas as it is.
"""

from __future__ import annotations

import logging
import time

from scistack_gui.ids import ROOT_SCOPE

logger = logging.getLogger(__name__)


def _adopt_every_canvas(db) -> list[str]:
    """Build every pipeline's canvas (no run states), adopting as it goes.
    Returns the scopes built."""
    from scistack_gui import pipeline_store as ps
    from scistack_gui.api.pipeline import _build_graph

    scopes = [ROOT_SCOPE] + sorted(
        p["pipeline_id"] for p in ps.list_all_pipelines(db) if p["pipeline_id"] != ROOT_SCOPE
    )
    for scope in scopes:
        _build_graph(db, scope, run_states=False)
    return scopes


def _plan_dict(plan) -> dict:
    return {
        "by_variable": plan.by_variable,
        "table_rows": plan.table_rows,
        "total_records": plan.total_records,
        "runs_kept": plan.runs_kept,
        "pins_to_release": plan.pins_to_release,
        "fingerprint": plan.fingerprint,
        "warnings": [w for w in plan.warnings if "GUI canvas" not in w],
    }


def clear_data_plan(db) -> dict:
    """What a clear would delete, after making the stored canvas complete.

    Adoption runs FIRST, so the plan the user confirms describes the
    database the clear will actually see (the fingerprint covers it)."""
    from scidb.clear_data import clear_plan

    t0 = time.perf_counter()
    scopes = _adopt_every_canvas(db)
    plan = clear_plan(db)
    logger.info(
        "[clear] plan: %d record(s) over %d variable(s), %d run(s) kept, canvases "
        "stored first: %s (%.1f ms)",
        plan.total_records,
        len(plan.by_variable),
        plan.runs_kept,
        scopes,
        (time.perf_counter() - t0) * 1000.0,
    )
    return _plan_dict(plan)


def clear_data(db, reason: str, fingerprint: str, backup: bool = False) -> dict:
    """Delete every record and its run history; keep every canvas.

    Refuses while a run is in progress (``api.run`` owns that state) and when
    the database changed since the plan was shown (``fingerprint``)."""
    from scidb.clear_data import clear_data as _clear

    if _run_in_progress():
        raise ValueError("a run is in progress; wait for it to finish before clearing")
    t0 = time.perf_counter()
    scopes = _adopt_every_canvas(db)
    result = _clear(
        db,
        reason or "cleared from the GUI",
        expect_fingerprint=fingerprint,
        backup=backup,
        allow_canvas=True,
    )
    logger.info(
        "[clear] cleared %d record(s) (tombstone %s); canvases kept: %s; backup=%s "
        "(%.1f ms)",
        result.plan.total_records,
        result.tombstone.tombstone_id,
        scopes,
        result.backup_path,
        (time.perf_counter() - t0) * 1000.0,
    )
    return {
        "ok": True,
        "deleted_records": result.plan.total_records,
        "deleted_rows": result.deleted_rows,
        "released_pins": result.released_pins,
        "tombstone_id": result.tombstone.tombstone_id,
        "backup_path": str(result.backup_path) if result.backup_path else None,
    }


def _run_in_progress() -> bool:
    from scistack_gui.api import run as run_api

    with run_api._active_runs_lock:
        return bool(run_api._active_runs)
