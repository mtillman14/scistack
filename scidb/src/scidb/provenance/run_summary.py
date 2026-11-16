"""What only the run itself knows (``_run_summary``).

The provenance graph answers what a run **computed**: its invocations, their
input and output records, the identity-bearing options. Four things it
cannot answer, because nothing wrote them down:

* a run that saved nothing (every combination up to date, failed, or without
  data) writes no ``_run`` row at all;
* the counts and the timing: total, computed, already up to date, failed, no
  data; duration; cancelled / stopped;
* the options that are not identity: ``locations=``, ``on_error``,
  ``skip_computed``, ``save``;
* which run a ``_run_outcome`` row belongs to.

This module is the one owner of that record. One row per ``for_each``
execution. ``run_id`` is the ``_run`` row's when the run saved (so the two
join), else a fresh id. ``group_id`` is the journal run: one user-visible
call, shared by the alternatives of an EachOf, and the ``run=<id>`` token of
the debug log.

**Writer:** :class:`RunRecorder`. ``scidb.run.foreach._run_concrete`` makes
one per run, chains it into the progress callback (it reads scifor's
``summary`` and ``combo_skip`` events), the save tells it the ``_run`` id,
and :meth:`RunRecorder.finish` writes the row and the run's
``_run_outcome`` rows in a ``finally``, so a stopped, cancelled or failed
run is recorded too. A failure to write is a WARN, never a failed run.
After writing it sends one ``run_recorded`` progress event carrying the
``run_id``, which is how a caller (the GUI, a pipeline's step report)
learns which report to open.

**Reader:** :func:`run_summaries` / :func:`run_summary`. The run report
(``scidb.provenance.run_report``) is the consumer; nothing else reads the
table.

**MATLAB runs** (their loop runs in MATLAB): ``scimatlab.bridge`` makes the
recorder in ``for_each_prepare``, and ``for_each_save`` feeds it the loop's
events, which ``scifor.matlab_loop.assemble`` kept in the Python loop's
shapes, then finishes it. ``on_error`` is not known there, and no
``run_recorded`` event is sent (nothing in MATLAB listens); the GUI finds
the rows with ``run_summaries(since=, language="matlab")``.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable

from scidb.model.log import Log
from scidb.provenance.outcomes import OutcomeCollector, record_outcomes
from scidb.store.tables import RUN_SUMMARY

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager

TABLE = RUN_SUMMARY.name

#: The progress event sent once the row is written.
RUN_RECORDED_EVENT = "run_recorded"

_COLUMNS = (
    "run_id", "group_id", "function_name", "output_types", "started_at",
    "duration_ms", "total", "computed", "up_to_date", "failed", "no_data",
    "saved_records", "cancelled", "stopped", "language", "settings", "notes",
)

#: Failure groups kept in ``notes`` (the per-location rows are ``_run_outcome``).
_MAX_NOTE_GROUPS = 50


@dataclass(frozen=True)
class RunSummary:
    """One stored run."""

    run_id: str
    group_id: str | None
    function_name: str
    output_types: tuple[str, ...]
    started_at: Any
    duration_ms: int | None
    total: int
    computed: int
    up_to_date: int
    failed: int
    no_data: int
    saved_records: int
    cancelled: bool
    stopped: bool
    language: str | None
    #: ``locations`` (the LocationFilter mapping or None), ``where``,
    #: ``on_error``, ``skip_computed``, ``save``.
    settings: dict = field(default_factory=dict)
    #: ``failures`` ([{reason, count, no_data, blame_label}]) and
    #: ``output_skips`` (scifor's wire shape).
    notes: dict = field(default_factory=dict)

    @property
    def verdict(self) -> str:
        """``cancelled`` / ``stopped`` / ``failed`` (nothing computed, something
        failed) / ``partial`` (some failed) / ``nothing_ran`` / ``ok``."""
        if self.cancelled:
            return "cancelled"
        if self.stopped:
            return "stopped"
        done = self.computed + self.up_to_date
        if self.failed and not done:
            return "failed"
        if self.failed:
            return "partial"
        if not done:
            return "nothing_ran"
        return "ok"

    def status_line(self) -> str:
        """The one sentence of counts. THE wording; every surface shows it."""
        n = self.total
        parts = []
        if self.computed:
            parts.append(f"{self.computed} computed")
        if self.up_to_date:
            parts.append(f"{self.up_to_date} already up to date")
        if self.failed:
            parts.append(f"{self.failed} failed")
        if self.no_data:
            parts.append(f"{self.no_data} no data")
        not_reached = n - self.computed - self.up_to_date - self.failed - self.no_data
        if not_reached > 0:
            parts.append(f"{not_reached} not reached")
        line = f"{n} combination{'' if n == 1 else 's'}"
        if parts:
            line += ": " + ", ".join(parts)
        line += "."
        if self.cancelled:
            line += " The run was cancelled."
        elif self.stopped:
            line += " The run stopped at the first error (on_error=stop)."
        return line

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "group_id": self.group_id,
            "function_name": self.function_name,
            "output_types": list(self.output_types),
            "started_at": _iso(self.started_at),
            "duration_ms": self.duration_ms,
            "total": self.total,
            "computed": self.computed,
            "up_to_date": self.up_to_date,
            "failed": self.failed,
            "no_data": self.no_data,
            "saved_records": self.saved_records,
            "cancelled": self.cancelled,
            "stopped": self.stopped,
            "language": self.language,
            "settings": dict(self.settings),
            "notes": dict(self.notes),
            "verdict": self.verdict,
            "status_line": self.status_line(),
        }


def _iso(value: Any) -> Any:
    return value.isoformat() if hasattr(value, "isoformat") else value


def _json_default(value: Any) -> Any:
    """Settings hold whatever the caller passed; never fail a run over one."""
    if isinstance(value, (set, frozenset, tuple)):
        return list(value)
    return str(value)


def _locations_setting(locations: Any) -> Any:
    """``locations=`` as its JSON mapping, or None when it selects everything."""
    if locations is None:
        return None
    try:
        from scifor.locations import LocationFilter

        flt = LocationFilter.of(locations)
        return None if flt.is_empty() else flt.to_dict()
    except Exception as e:  # recorded as text rather than lost
        Log.debug(f"[run_summary] locations= not serialisable as a filter: {e}")
        return {"text": str(locations)}


class RunRecorder:
    """Collects one run and writes it (see the module docstring)."""

    def __init__(
        self,
        function_name: str,
        *,
        locations: Any = None,
        where: Any = None,
        on_error: str | None = None,
        skip_computed: bool = False,
        save: bool = True,
        language: str = "python",
    ) -> None:
        self.function_name = function_name
        self.language = language
        self.started_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self._t0 = time.perf_counter()
        self.settings = {
            "locations": _locations_setting(locations),
            "where": None if where is None else str(where),
            "on_error": on_error,
            "skip_computed": bool(skip_computed),
            "save": bool(save),
        }
        self.counts = {"total": 0, "completed": 0, "skipped": 0, "no_data": 0}
        self.cancelled = False
        self.stopped = False
        self.failures: list[dict] = []
        self.output_skips: dict = {}
        self.outcomes = OutcomeCollector()
        #: The ``_run`` row's id, once the save has written one.
        self.saved_run_id: str | None = None
        self.saved_records = 0
        self.run_id: str | None = None

    # ---- collecting ------------------------------------------------------

    def observe(self, info: dict) -> None:
        """One scifor progress event."""
        self.outcomes.observe(info)
        for k in ("total", "completed", "skipped"):
            if info.get(k) is not None:
                self.counts[k] = int(info[k])
        if info.get("event") == "summary":
            self.counts["no_data"] = int(info.get("no_data") or 0)
            self.cancelled = bool(info.get("cancelled"))
            self.stopped = bool(info.get("stopped"))
            self.failures = [
                {
                    "reason": g.get("reason"),
                    "count": g.get("count"),
                    "no_data": bool(g.get("no_data")),
                    "blame_label": g.get("blame_label"),
                }
                for g in (info.get("failures") or [])[:_MAX_NOTE_GROUPS]
            ]
            self.output_skips = dict(info.get("output_skips") or {})

    def wrap(self, progress_fn: "Callable[[dict], None] | None") -> "Callable[[dict], None]":
        """*progress_fn* with this recorder listening first."""

        def _recording_progress_fn(info: dict) -> None:
            self.observe(info)
            if progress_fn is not None:
                progress_fn(info)

        return _recording_progress_fn

    def saved(self, run_id: str | None, n_records: int) -> None:
        """The save's verdict: the ``_run`` id it wrote (None when nothing
        reached the graph) and how many records it committed."""
        if run_id:
            self.saved_run_id = run_id
        self.saved_records += int(n_records or 0)

    # ---- writing ---------------------------------------------------------

    def finish(
        self,
        db: "DatabaseManager | None",
        *,
        output_types: list[str],
        up_to_date: int = 0,
        progress_fn: "Callable[[dict], None] | None" = None,
    ) -> str | None:
        """Write the row and the run's outcome rows; returns the run id, or
        None when nothing was stored. Never raises."""
        try:
            return self._finish(db, list(output_types or []), int(up_to_date or 0), progress_fn)
        except Exception as e:  # never fail a run over its record
            Log.warn(
                f"[run_summary] {self.function_name}: could not store the run "
                f"summary: {type(e).__name__}: {e}"
            )
            return None

    def _finish(self, db, output_types, up_to_date, progress_fn) -> str | None:
        fn = self.function_name
        if db is None:
            Log.debug(f"[run_summary] {fn}: no database; run not recorded")
            return None
        if getattr(db, "read_only", False):
            Log.debug(f"[run_summary] {fn}: read-only database; run not recorded")
            return None
        from scidb.provenance.graph import generate_run_id

        t0 = time.perf_counter()
        run_id = self.saved_run_id or generate_run_id()
        self.run_id = run_id
        journal = Log.current_run()
        group_id = getattr(journal, "run_id", None)
        c = self.counts
        failed = max(c["skipped"] - c["no_data"], 0)
        duration_ms = int((time.perf_counter() - self._t0) * 1000)
        row = [
            run_id,
            group_id,
            fn,
            [str(t) for t in output_types],
            self.started_at,
            duration_ms,
            c["total"] + up_to_date,
            c["completed"],
            up_to_date,
            failed,
            c["no_data"],
            self.saved_records,
            self.cancelled,
            self.stopped,
            self.language,
            json.dumps(self.settings, default=_json_default),
            json.dumps(
                {"failures": self.failures, "output_skips": self.output_skips},
                default=_json_default,
            ),
        ]
        db._duck.execute(
            f"INSERT INTO {TABLE} ({', '.join(_COLUMNS)}) VALUES "
            f"({', '.join('?' for _ in _COLUMNS)}) ON CONFLICT (run_id) DO NOTHING",
            row,
        )
        n_outcomes = 0
        if self.outcomes.events:
            try:
                n_outcomes = record_outcomes(
                    db, fn, output_types, self.outcomes.events, run_id=run_id
                )
            except Exception as e:  # never fail a run over its explanation
                Log.warn(
                    f"[outcomes] {fn}: could not store {len(self.outcomes.events)} "
                    f"location outcome(s): {type(e).__name__}: {e}"
                )
        Log.info(
            f"[run_summary] run={run_id} group={group_id or '-'} fn={fn} "
            f"total={row[6]} computed={c['completed']} up_to_date={up_to_date} "
            f"failed={failed} no_data={c['no_data']} saved_records={self.saved_records} "
            f"outcomes={n_outcomes} had_run_row={self.saved_run_id is not None} "
            f"duration_ms={duration_ms} wrote in {(time.perf_counter() - t0) * 1000:.1f} ms"
        )
        if progress_fn is not None:
            try:
                progress_fn({
                    "event": RUN_RECORDED_EVENT,
                    "run_id": run_id,
                    "group_id": group_id,
                    "function_name": fn,
                    # The positional fields every progress consumer reads.
                    "current": c["total"],
                    "total": c["total"],
                    "completed": c["completed"],
                    "skipped": c["skipped"],
                    "metadata": {},
                })
            except Exception as e:
                Log.debug(f"[run_summary] {fn}: run_recorded event not delivered: {e}")
        return run_id


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def _loads(text: Any) -> dict:
    if not text:
        return {}
    try:
        value = json.loads(text)
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _from_row(row) -> RunSummary:
    d = dict(zip(_COLUMNS, row))
    return RunSummary(
        run_id=d["run_id"],
        group_id=d["group_id"],
        function_name=d["function_name"],
        output_types=tuple(d["output_types"] or ()),
        started_at=d["started_at"],
        duration_ms=None if d["duration_ms"] is None else int(d["duration_ms"]),
        total=int(d["total"] or 0),
        computed=int(d["computed"] or 0),
        up_to_date=int(d["up_to_date"] or 0),
        failed=int(d["failed"] or 0),
        no_data=int(d["no_data"] or 0),
        saved_records=int(d["saved_records"] or 0),
        cancelled=bool(d["cancelled"]),
        stopped=bool(d["stopped"]),
        language=d["language"],
        settings=_loads(d["settings"]),
        notes=_loads(d["notes"]),
    )


def run_summaries(
    duck,
    function_name: str | None = None,
    *,
    outputs: "list[str] | None" = None,
    since: Any = None,
    language: str | None = None,
    limit: int = 50,
) -> list[RunSummary]:
    """Stored runs, newest first. *outputs* keeps the runs that wrote to any
    of those variable types (two canvas nodes may share one function).
    *since* (a naive UTC datetime) keeps the runs started at or after it,
    *language* those of one language: how a caller that only watched a run
    from outside (the GUI and a MATLAB run) finds what it stored.
    ``[]`` before the table exists."""
    if not duck.table_exists(TABLE):
        return []
    where, params = [], []
    if function_name is not None:
        where.append("function_name = ?")
        params.append(function_name)
    if outputs:
        where.append("list_has_any(output_types, ?)")
        params.append([str(o) for o in outputs])
    if since is not None:
        where.append("started_at >= ?")
        params.append(since)
    if language is not None:
        where.append("language = ?")
        params.append(language)
    sql = f"SELECT {', '.join(_COLUMNS)} FROM {TABLE}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY started_at DESC, run_id DESC LIMIT ?"
    params.append(int(limit))
    return [_from_row(r) for r in duck.fetchall(sql, params)]


def run_summary(duck, run_id: str) -> RunSummary | None:
    """One stored run, or None."""
    if not duck.table_exists(TABLE):
        return None
    rows = duck.fetchall(
        f"SELECT {', '.join(_COLUMNS)} FROM {TABLE} WHERE run_id = ?", [run_id]
    )
    return _from_row(rows[0]) if rows else None
