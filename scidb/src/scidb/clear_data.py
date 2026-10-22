"""Clear every record from a project database, keeping the pipeline.

The second sanctioned real delete in scidb, beside ``variant_delete`` (user
request 2026-10-21, ``.claude/plan-clear-data.md``): while debugging, wipe the
data and its run history, then re-run from a clean slate.

Two steps, like ``variant_delete``:

1. :func:`clear_plan` is a dry run. It counts what would go, per variable
   and per table, and writes nothing.
2. :func:`clear_data` re-plans inside one transaction, refuses if the counts
   differ from the plan the user saw (``expect_fingerprint``), deletes, writes
   a tombstone, releases every active pin and commits. Any failure rolls the
   whole thing back.

**Which tables** is decided by ONE classification: ``scidb.bundle``'s
``HISTORY_TABLES`` / ``INTENT_TABLES`` (what travels as history vs. intent)
plus :data:`KEPT_BY_CLEAR` here (history a clear deliberately keeps). The
guard test ``test_every_scidb_table_is_classified`` fails when a new scidb
table is in neither :data:`CLEARED_TABLES` nor :data:`KEPT_TABLES`.

Emptied: every variable's data rows (the tables and their views stay, and an
empty table takes a new column set on the next save:
``DatabaseManager._check_column_set``), ``_record``, ``_record_save``,
``_constant``, ``_invocation*``, ``_function_source`` and
``_run_invocation``.

Kept: the run log ``_run`` (that a run happened, when, from where; user
decision 2026-10-21), ``_schema`` (location interning: a re-save reuses the
same ids), pins (released, rows kept), tombstones, schema exclusions, schema
key types, variable registrations. Saved plots/stats and every GUI table are
not scidb's and are untouched.

**A GUI project is refused** unless ``allow_canvas=True``. The GUI's clear
(``scistack_gui.services.clear_service``) passes it after canvas adoption has
stored every node and edge that came from history
(plan-canvas-as-document Stage 2); this layer cannot do that itself.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from .bundle import HISTORY_TABLES, INTENT_TABLES
from .log import Log

if TYPE_CHECKING:
    from .database import DatabaseManager
    from .variant_delete import Tombstone

#: History a clear keeps, and why (the rest of ``HISTORY_TABLES`` minus the
#: intent tables is emptied).
KEPT_BY_CLEAR: dict[str, str] = {
    "_run": "the run log: that a run happened, when, and its origin (user decision 2026-10-21)",
    "_run_step": "part of the run log: the step name a run was given (for_each(step=...))",
    "_schema": "location interning; a re-save after the clear reuses the same schema ids",
    "_variant_pin": "audit trail; active pins are RELEASED, rows kept",
    "_variant_tombstone": "the audit trail of every real delete, this clear included",
}

#: Emptied by a clear, besides every variable's own data table.
CLEARED_TABLES: tuple[str, ...] = tuple(
    t for t in HISTORY_TABLES if t not in INTENT_TABLES and t not in KEPT_BY_CLEAR
) + ("_record_save",)

#: Every other scidb/sciduckdb table, and why a clear leaves it alone.
KEPT_TABLES: dict[str, str] = {
    **KEPT_BY_CLEAR,
    **{t: "dataset intent: governs a re-run" for t in INTENT_TABLES},
    "_registered_types": "variable registrations (which classes exist)",
    "_variables": "variable registrations and dtype metadata",
    "_variable_groups": "variable groups (declarations)",
    "_schema_key_type": "schema key types (declarations)",
}

#: Tables whose presence means the GUI has a canvas on this database.
CANVAS_TABLES: tuple[str, ...] = ("_pipelines", "_pipeline_nodes")


class ClearRefused(ValueError):
    """A clear that must not run, with the reason (shown as one line)."""


@dataclass
class ClearPlan:
    #: ``{variable: records}`` that would be deleted.
    by_variable: dict[str, int]
    #: ``{table: rows}`` that would be deleted (data tables included).
    table_rows: dict[str, int]
    total_records: int
    #: Runs in the kept run log.
    runs_kept: int
    pins_to_release: list[str]
    has_canvas: bool
    fingerprint: str
    warnings: list[str] = field(default_factory=list)


@dataclass
class ClearResult:
    tombstone: "Tombstone"
    deleted_rows: dict[str, int]
    released_pins: list[str]
    plan: ClearPlan
    backup_path: "Path | None" = None


def _data_tables(duck) -> list[str]:
    if not duck._table_exists("_registered_types"):
        return []
    return sorted(
        t
        for (t,) in duck._fetchall("SELECT DISTINCT table_name FROM _registered_types")
        if duck._table_exists(t)
    )


def _count(duck, table: str) -> int:
    return int(duck._fetchone(f'SELECT COUNT(*) FROM "{table}"')[0])


def clear_plan(db: "DatabaseManager") -> ClearPlan:
    """What :func:`clear_data` would delete. Writes nothing."""
    from .provenance import INTERNAL_RECORD_TYPES
    from .variant_pins import active_pins

    duck = db._duck
    t0 = time.perf_counter()
    table_rows: dict[str, int] = {}
    for table in list(CLEARED_TABLES) + _data_tables(duck):
        if duck._table_exists(table):
            table_rows[table] = _count(duck, table)

    by_variable: dict[str, int] = {}
    if duck._table_exists("_record"):
        for rtype, n in duck._fetchall(
            "SELECT type, COUNT(*) FROM _record GROUP BY type ORDER BY type"
        ):
            if rtype not in INTERNAL_RECORD_TYPES:
                by_variable[rtype] = int(n)
    runs_kept = _count(duck, "_run") if duck._table_exists("_run") else 0
    pins = sorted(active_pins(db))
    has_canvas = any(duck._table_exists(t) for t in CANVAS_TABLES)

    fingerprint = hashlib.sha1(
        json.dumps([sorted(table_rows.items()), runs_kept, pins]).encode()
    ).hexdigest()[:16]
    plan = ClearPlan(
        by_variable=by_variable,
        table_rows=dict(sorted(table_rows.items())),
        total_records=sum(by_variable.values()),
        runs_kept=runs_kept,
        pins_to_release=pins,
        has_canvas=has_canvas,
        fingerprint=fingerprint,
    )
    if has_canvas:
        plan.warnings.append(
            "this database has a GUI canvas: clear it from the GUI, which first "
            "stores every canvas node and edge that came from run history "
            "(canvas adoption), so the clear keeps them"
        )
    Log.info(
        f"clear_plan: {plan.total_records} record(s) by variable={by_variable}; "
        f"rows by table={plan.table_rows}; {runs_kept} run(s) kept; "
        f"pins to release={pins}; canvas={has_canvas}; fingerprint={fingerprint} "
        f"({(time.perf_counter() - t0) * 1000:.1f} ms)"
    )
    return plan


def backup_path_for(db_path: "Path | str", now: "datetime | None" = None) -> Path:
    """``<name>.duckdb.bak-<YYYYmmdd-HHMMSS>`` beside the database. Not a
    ``.duckdb`` suffix, so project discovery never mistakes it for one."""
    p = Path(db_path)
    stamp = (now or datetime.now()).strftime("%Y%m%d-%H%M%S")
    return p.with_name(f"{p.name}.bak-{stamp}")


def clear_data(
    db: "DatabaseManager",
    reason: str,
    *,
    expect_fingerprint: "str | None" = None,
    backup: bool = False,
    allow_canvas: bool = False,
) -> ClearResult:
    """Delete every record and its run history; keep the pipeline, the run
    log and every declaration. Cannot be undone (except from ``backup``);
    leaves a tombstone."""
    from .variant_delete import write_tombstone
    from .variant_pins import forget_type_graph, release_pin

    if not reason or not str(reason).strip():
        raise ValueError("clear_data needs a reason; it goes on the tombstone")
    duck = db._duck
    t0 = time.perf_counter()

    preview = clear_plan(db)
    if preview.has_canvas and not allow_canvas:
        raise ClearRefused(
            "this database has a GUI canvas, and part of it may still come only "
            "from the run history a clear deletes. Clear it from the GUI "
            "(SciStack: Clear All Data), which stores the canvas first. Nothing "
            "was deleted."
        )

    backup_path = None
    if backup:
        backup_path = backup_path_for(db.dataset_db_path)
        # Flush the write-ahead log into the file first, so the copy is complete.
        duck._execute("CHECKPOINT")
        shutil.copy2(db.dataset_db_path, backup_path)
        Log.info(f"clear_data: backed up {db.dataset_db_path} -> {backup_path}")

    duck._begin()
    try:
        plan = clear_plan(db)
        if expect_fingerprint is not None and plan.fingerprint != expect_fingerprint:
            raise ValueError(
                f"clear_data: the database changed since the plan was shown "
                f"(fingerprint {expect_fingerprint} -> {plan.fingerprint}); "
                f"nothing was deleted. Re-plan and confirm again."
            )
        deleted: dict[str, int] = {}
        for table, n in plan.table_rows.items():
            t_table = time.perf_counter()
            duck._execute(f'DELETE FROM "{table}"')
            deleted[table] = n
            Log.info(
                f"clear_data: emptied {table} ({n} row(s), "
                f"{(time.perf_counter() - t_table) * 1000:.1f} ms)"
            )
        tomb = write_tombstone(
            duck,
            reason,
            [{"all": True}],
            plan.by_variable,
            [],  # every record: listing them would only copy the database
        )
        released = []
        for variable in plan.pins_to_release:
            if release_pin(db, variable, f"database cleared (tombstone {tomb.tombstone_id})"):
                released.append(variable)
        duck._commit()
    except Exception:
        Log.warn("clear_data: failed; rolling back, nothing was deleted")
        try:
            duck._rollback()
        except Exception:  # pragma: no cover - rollback of a dead txn
            pass
        raise
    forget_type_graph(duck)
    forget = getattr(db, "_forget_schema_key_kinds", None)
    if forget is not None:
        forget()
    Log.info(
        f"clear_data: tombstone {tomb.tombstone_id}: deleted {plan.total_records} "
        f"record(s) {plan.by_variable}; rows={deleted}; kept {plan.runs_kept} run(s); "
        f"released pins={released}; backup={backup_path}; reason={reason!r} "
        f"({(time.perf_counter() - t0) * 1000:.1f} ms)"
    )
    return ClearResult(
        tombstone=tomb,
        deleted_rows=deleted,
        released_pins=released,
        plan=plan,
        backup_path=backup_path,
    )
