"""Try a step without saving anything ("▶ Try on…", GitHub #28).

Runs one function at some locations with ``save=False`` and run recording
suppressed (``run_summary.recording_suppressed``), so NOTHING is written,
not even ``_run_summary``. What comes back is what the scientist needs to
judge an in-progress function: per location each output's value preview
(``inspect.cells.describe_value``), the anomaly findings the run would have
stored (``provenance.findings.check_saved``, the same owner, not written),
the printed output, the failures, and the timing.

It is for one step only: nothing downstream can consume its outputs.
MATLAB functions run through the MATLAB bridge and are not covered here.
"""

from __future__ import annotations

import io
import time
from contextlib import redirect_stdout
from dataclasses import dataclass, field
from typing import Any

from scidb.model.log import Log

#: Locations described per Try (the rest are counted).
ROW_LIMIT = 200
#: Printed output kept, in characters.
PRINTED_MAX = 20_000


@dataclass
class TryResult:
    function_name: str
    rows: list[dict] = field(default_factory=list)
    total_rows: int = 0
    findings: list[dict] = field(default_factory=list)
    failures: list[dict] = field(default_factory=list)
    printed: str = ""
    duration_ms: int = 0
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "function_name": self.function_name,
            "rows": list(self.rows),
            "total_rows": self.total_rows,
            "truncated": len(self.rows) < self.total_rows,
            "findings": list(self.findings),
            "failures": list(self.failures),
            "printed": self.printed,
            "duration_ms": self.duration_ms,
            "notes": list(self.notes),
        }


def describe_try(
    db,
    result_tbl: Any,
    outputs: list,
    function_name: str,
    *,
    failures: list[dict] | None = None,
    printed: str = "",
    duration_ms: int = 0,
) -> TryResult:
    """The Try report of one ``for_each(save=False)`` result table."""
    from scifor import output_column_name

    from scidb.inspect.cells import describe_value
    from scidb.provenance.findings import SavedOutput, check_saved

    res = TryResult(function_name=function_name, failures=list(failures or []),
                    printed=(printed or "")[:PRINTED_MAX], duration_ms=int(duration_ms))
    if result_tbl is None or getattr(result_tbl, "empty", True):
        res.notes.append("Nothing was computed (no location ran, or every one failed).")
        return res
    schema_keys = [k for k in (db.dataset_schema_keys if db is not None else []) if k in result_tbl.columns]
    columns = {}
    for o in outputs or []:
        col = output_column_name(o)
        if col in result_tbl.columns:
            columns[col] = getattr(o, "__name__", None) or type(o).__name__
    if not columns:
        res.notes.append(f"No output column in the result ({list(result_tbl.columns)[:10]}).")
    saved: list = []
    records = result_tbl.to_dict("records")
    res.total_rows = len(records)
    for i, row in enumerate(records):
        location = {k: str(row[k]) for k in schema_keys if row.get(k) is not None}
        out = {}
        for col, variable in columns.items():
            value = row.get(col)
            out[variable] = describe_value(value).to_dict()
            saved.append(SavedOutput(variable, f"try:{i}:{variable}", location, value))
        if i < ROW_LIMIT:
            res.rows.append({"location": location, "outputs": out})
    try:
        res.findings = [f.to_dict() for f in check_saved(db, saved)] if db is not None else []
    except Exception as e:
        res.notes.append(f"Findings not checked: {type(e).__name__}: {e}")
    Log.info(
        f"[try] {function_name}: {res.total_rows} location(s), {len(res.findings)} finding(s), "
        f"{len(res.failures)} failure group(s), {len(res.printed)} printed char(s) in {res.duration_ms} ms"
    )
    return res


def try_step(fn, inputs: dict, outputs: list, *, locations: Any = None, db=None, **for_each_kwargs) -> TryResult:
    """``for_each(fn, inputs, outputs, save=False, locations=...)`` that
    writes nothing, described as a :class:`TryResult`."""
    from scifor import LoopHooks
    from scifor.project.names import function_name

    from scidb.provenance.run_summary import recording_suppressed
    from scidb.run.foreach import for_each

    failures: list[dict] = []

    def _progress(info: dict) -> None:
        if info.get("event") == "summary":
            failures.extend(info.get("failures") or [])

    buf = io.StringIO()
    t0 = time.perf_counter()
    with recording_suppressed(), redirect_stdout(buf):
        tbl = for_each(
            fn, inputs, list(outputs), save=False, locations=locations, db=db,
            hooks=LoopHooks(progress_fn=_progress), **for_each_kwargs,
        )
    if db is None:
        from scidb.store.database import database_or_none

        db = database_or_none()
    return describe_try(
        db, tbl, outputs, function_name(fn), failures=failures, printed=buf.getvalue(),
        duration_ms=int((time.perf_counter() - t0) * 1000),
    )
