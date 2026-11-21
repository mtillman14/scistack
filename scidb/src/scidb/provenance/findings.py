"""Anomaly findings: does anything a run saved look wrong? (GitHub #28)

Computed **once, in the run**, where every output is already in memory, and
stored **only when something is wrong**: one ``_run_outcome`` row per
finding (``outcome = 'finding'``, the record it is about, its kind and its
text, the ``run_id``), next to the failure rows. A healthy record writes
nothing, so no shape or size is stored.

The kinds (thresholds are the constants below, in this one module):

* **empty / all NaN** — the value has no elements, or every one is NaN (a
  scalar NaN too);
* **NaN introduced** — the output has NaN where no input record of that
  call had any (the input cells are read in one batched load per input
  variable, only for outputs that have NaN);
* **shape mismatch** — an array output whose shape differs from the shape
  most sibling locations have;
* **outlier** — a scalar output with ``|x − median| / MAD > OUTLIER_MAD``
  across sibling locations, only when there are at least ``OUTLIER_MIN_N``.

Siblings are this run's outputs of the variable plus the variable's current
records at the other locations (one batched load per variable, timed).

**Writer:** ``scidb.run.foreach_save`` hands the saved values to
:func:`check_saved`, the run's ``RunRecorder`` writes the findings with its
run id (:func:`write_findings`). The Python and the MATLAB bridge path both
save through ``apply_save_plan``, so both are covered. Records saved outside
``for_each`` (direct saves, raw loads) get no findings: their locations say
"not checked" (``scidb.provenance.status``).

**Readers:** :func:`findings_for_records`. A later run that rewrites a
location saves a new record, so the old record's findings no longer apply;
the same content saved again keeps its record id and so its findings.

**Try** (test mode) computes the same checks with :func:`check_saved` and
does not write them.

Cell kinds are ``scicanonicalhash._cells``'; shapes are
``scidb.inspect.cells.describe_value``'s.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from scidb.model.log import Log

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager

EMPTY = "empty"
ALL_NAN = "all_nan"
NAN_INTRODUCED = "nan_introduced"
SHAPE_MISMATCH = "shape_mismatch"
OUTLIER = "outlier"
KINDS = (EMPTY, ALL_NAN, NAN_INTRODUCED, SHAPE_MISMATCH, OUTLIER)

LABELS = {
    EMPTY: "empty",
    ALL_NAN: "all NaN",
    NAN_INTRODUCED: "NaN introduced",
    SHAPE_MISMATCH: "shape mismatch",
    OUTLIER: "outlier",
}

#: A scalar is an outlier above this many MADs from the siblings' median.
OUTLIER_MAD = 5.0
#: Outliers are looked for only among at least this many sibling values.
OUTLIER_MIN_N = 8
#: A shape mismatch needs at least this many siblings with a shape...
SHAPE_MIN_N = 3
#: ...and the most common shape held by at least this share of them.
SHAPE_MAJORITY = 0.5
#: Siblings are not loaded for a variable with more current records than this.
SIBLING_LOAD_MAX = 50_000

#: The ``_run_outcome.outcome`` of a finding row.
OUTCOME = "finding"


@dataclass
class SavedOutput:
    """One record a run saved, while its value is still in memory."""

    variable: str
    record_id: str
    location: dict[str, str]
    value: Any
    input_rids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Finding:
    variable: str
    record_id: str
    location: dict
    kind: str
    text: str

    def to_dict(self) -> dict:
        return {
            "variable": self.variable,
            "record_id": self.record_id,
            "location": dict(self.location),
            "kind": self.kind,
            "label": LABELS[self.kind],
            "text": self.text,
        }


# ---------------------------------------------------------------------------
# One value
# ---------------------------------------------------------------------------


def _value_summary(value: Any) -> tuple[str | None, int, int, str | None, float | None]:
    """``(problem kind, elements, NaN elements, shape text, scalar)``.

    A DataFrame is read column by column (every numeric cell's elements);
    anything else by ``scicanonicalhash._cells.classify``.
    """
    from scicanonicalhash._cells import ALL_NAN as C_ALL_NAN
    from scicanonicalhash._cells import BLANK, EMPTY as C_EMPTY, classify

    from scidb.inspect.cells import describe_value

    try:
        import pandas as pd

        is_frame = isinstance(value, pd.DataFrame)
    except ImportError:  # pragma: no cover
        is_frame = False
    if is_frame:
        if len(value) == 0:
            return EMPTY, 0, 0, "0 rows", None
        n = nan = 0
        for col in value.columns:
            for cell in value[col].tolist():
                kind, ce, cn = classify(cell)
                if kind == BLANK:
                    n += 1
                    nan += 1
                else:
                    n += ce
                    nan += cn
        kind = ALL_NAN if n and nan == n else None
        return kind, n, nan, f"{len(value)} rows × {len(value.columns)} columns", None
    kind, n, nan = classify(value)
    shape = describe_value(value)
    scalar = None
    if shape.kind == "scalar":
        try:
            scalar = float(value)
        except (TypeError, ValueError):
            scalar = None
    if kind == BLANK:
        return ALL_NAN, 1, 1, shape.shape_text, None
    if kind == C_EMPTY:
        return EMPTY, 0, 0, shape.shape_text, None
    if kind == C_ALL_NAN:
        return ALL_NAN, n, nan, shape.shape_text, None
    return None, n, nan, shape.shape_text if shape.kind == "array" else None, scalar


# ---------------------------------------------------------------------------
# A run's outputs
# ---------------------------------------------------------------------------


def check_saved(db: "DatabaseManager", saved: list[SavedOutput], *, siblings: bool = True) -> list[Finding]:
    """Every finding over one run's saved outputs. Never raises: a check
    that cannot run is logged and skipped (findings must not fail a run)."""
    if not saved:
        return []
    t0 = time.perf_counter()
    timings: dict[str, float] = {}
    out: list[Finding] = []
    summaries = {}
    for s in saved:
        try:
            summaries[s.record_id] = _value_summary(s.value)
        except Exception as e:
            Log.debug(f"[findings] {s.variable} {s.record_id[:12]}: unreadable value: {e}")
    timings["values"] = time.perf_counter() - t0

    has_nan: list[SavedOutput] = []
    for s in saved:
        summ = summaries.get(s.record_id)
        if summ is None:
            continue
        kind, n, nan, _shape, _scalar = summ
        if kind == EMPTY:
            out.append(Finding(s.variable, s.record_id, s.location, EMPTY, f"{s.variable} is empty here"))
        elif kind == ALL_NAN:
            out.append(Finding(
                s.variable, s.record_id, s.location, ALL_NAN,
                f"{s.variable} is all NaN here" + (f" ({nan} values)" if n > 1 else ""),
            ))
        elif nan:
            has_nan.append(s)

    t1 = time.perf_counter()
    try:
        out += _nan_introduced(db, has_nan, summaries)
    except Exception as e:
        Log.warn(f"[findings] NaN-introduced check skipped: {type(e).__name__}: {e}")
    timings["nan_introduced"] = time.perf_counter() - t1

    t1 = time.perf_counter()
    if siblings:
        by_var: dict[str, list[SavedOutput]] = {}
        for s in saved:
            by_var.setdefault(s.variable, []).append(s)
        for variable, items in by_var.items():
            try:
                out += _sibling_findings(db, variable, items, summaries)
            except Exception as e:
                Log.warn(f"[findings] {variable}: sibling checks skipped: {type(e).__name__}: {e}")
    timings["siblings"] = time.perf_counter() - t1

    counts = {k: sum(1 for f in out if f.kind == k) for k in KINDS}
    Log.info(
        f"[findings] {len(saved)} saved record(s): {len(out)} finding(s) "
        f"{ {k: n for k, n in counts.items() if n} }"
    )
    Log.timings("findings.check_saved", timings, extra=f"{len(saved)} record(s)",
                total=time.perf_counter() - t0)
    return out


def _nan_introduced(db, has_nan: list[SavedOutput], summaries: dict) -> list[Finding]:
    """Outputs with NaN where none of the call's input records had any."""
    if not has_nan:
        return []
    from scidb.inspect.cells import record_cells

    rids = sorted({r for s in has_nan for r in s.input_rids})
    types = _record_types(db, rids)
    by_type: dict[str, set] = {}
    for rid in rids:
        t = types.get(rid)
        if t and not t.startswith("__"):
            by_type.setdefault(t, set()).add(rid)
    cells = {}
    for t, wanted in by_type.items():
        cells.update(record_cells(db, t, wanted))
    out = []
    for s in has_nan:
        ins = [cells[r] for r in s.input_rids if r in cells]
        if not ins:
            continue  # constants / PathInputs only: nothing to compare with
        if any(c.nan_elements or c.problems for c in ins):
            continue  # carried from an input
        _k, n, nan, _shape, _scalar = summaries[s.record_id]
        out.append(Finding(
            s.variable, s.record_id, s.location, NAN_INTRODUCED,
            f"{s.variable} has {nan} NaN of {n} values here although no input had any",
        ))
    return out


def _record_types(db, rids: list[str]) -> dict[str, str]:
    from scidb.provenance.query import _chunked_in

    if not rids:
        return {}
    rows = _chunked_in(db._duck, "SELECT record_id, type FROM _record WHERE record_id IN ({ph})", rids)
    return {r: t for r, t in rows}


def _sibling_values(db, variable: str, skip: set) -> list[tuple[str | None, float | None]]:
    """``(shape text, scalar)`` of *variable*'s current records not in *skip*:
    one load of the variable."""
    from scidb.inspect.preview import _variable_class
    from scidb.provenance.query import current_records_by_schema_batch
    from scidb.run.bindings import RECORD_ID_COLUMN

    current = {r for rids in current_records_by_schema_batch(db._duck, variable).values() for r in rids}
    others = current - skip
    if not others:
        return []
    if len(current) > SIBLING_LOAD_MAX:
        Log.info(f"[findings] {variable}: {len(current)} records, siblings not loaded")
        return []
    frame = db.load_all_as_df(
        _variable_class(variable), layout="spread", include_rid=True,
        stringify_schema=True, version_id="all",
    )
    if frame.empty or RECORD_ID_COLUMN not in frame.columns:
        return []
    keys = set(db.dataset_schema_keys)
    data_cols = [c for c in frame.columns if c not in keys and not str(c).startswith("__")]
    frame = frame[frame[RECORD_ID_COLUMN].astype(str).isin(others)]
    out = []
    for _rid, group in frame.groupby(frame[RECORD_ID_COLUMN].astype(str), sort=False):
        if len(group) != 1 or len(data_cols) != 1:
            out.append((f"{len(group)} rows × {len(data_cols)} columns", None))
            continue
        _k, _n, _nan, shape, scalar = _value_summary(group.iloc[0][data_cols[0]])
        out.append((shape, scalar))
    return out


def _sibling_findings(db, variable: str, items: list[SavedOutput], summaries: dict) -> list[Finding]:
    import numpy as np

    mine = [(s, summaries[s.record_id]) for s in items if s.record_id in summaries]
    if not mine:
        return []
    wants_shapes = any(summ[3] is not None for _s, summ in mine)
    wants_scalars = any(summ[4] is not None for _s, summ in mine)
    if not (wants_shapes or wants_scalars):
        return []
    t0 = time.perf_counter()
    others = _sibling_values(db, variable, {s.record_id for s, _ in mine})
    Log.debug(
        f"[findings] {variable}: {len(others)} sibling record(s) loaded in "
        f"{(time.perf_counter() - t0) * 1000:.1f} ms"
    )
    out: list[Finding] = []
    if wants_shapes:
        shapes = [summ[3] for _s, summ in mine if summ[3] is not None] + [
            sh for sh, _v in others if sh is not None
        ]
        if len(shapes) >= SHAPE_MIN_N:
            from collections import Counter

            common, n_common = Counter(shapes).most_common(1)[0]
            if n_common / len(shapes) >= SHAPE_MAJORITY:
                for s, summ in mine:
                    if summ[3] is not None and summ[3] != common:
                        out.append(Finding(
                            variable, s.record_id, s.location, SHAPE_MISMATCH,
                            f"{variable} is {summ[3]} here; {n_common} of {len(shapes)} "
                            f"locations are {common}",
                        ))
    if wants_scalars:
        values = [summ[4] for _s, summ in mine if summ[4] is not None] + [
            v for _sh, v in others if v is not None
        ]
        arr = np.asarray([v for v in values if v == v], dtype=float)
        if arr.size >= OUTLIER_MIN_N:
            med = float(np.median(arr))
            mad = float(np.median(np.abs(arr - med)))
            if mad > 0:
                for s, summ in mine:
                    x = summ[4]
                    if x is None or x != x:
                        continue
                    score = abs(x - med) / mad
                    if score > OUTLIER_MAD:
                        out.append(Finding(
                            variable, s.record_id, s.location, OUTLIER,
                            f"{variable} = {x:g} here is {score:.1f} MADs from the median "
                            f"{med:g} of {arr.size} locations",
                        ))
    return out


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------


def write_findings(
    db: "DatabaseManager",
    function_name: str,
    output_types: list[str],
    findings: list[Finding],
    run_id: str | None,
) -> int:
    """One ``_run_outcome`` row per finding. Returns the rows written."""
    from scidb.provenance.outcomes import TABLE

    if not findings or getattr(db, "read_only", False):
        return 0
    t0 = time.perf_counter()
    schema_keys = list(db.dataset_schema_keys)
    at = datetime.now(timezone.utc).replace(tzinfo=None)
    cols = ["function_name", "output_types", *schema_keys, "outcome", "reason",
            "recorded_at", "run_id", "record_id", "finding"]
    col_sql = ", ".join('"' + c.replace('"', '""') + '"' for c in cols)
    one = "(" + ", ".join("?" for _ in cols) + ")"
    rows = [
        [function_name, [f.variable], *[f.location.get(k) for k in schema_keys],
         OUTCOME, f.text[:2000], at, run_id, f.record_id, f.kind]
        for f in findings
    ]
    for i in range(0, len(rows), 500):
        chunk = rows[i:i + 500]
        db._duck.execute(
            f"INSERT INTO {TABLE} ({col_sql}) VALUES " + ", ".join(one for _ in chunk),
            [v for row in chunk for v in row],
        )
    Log.info(
        f"[findings] {function_name}: stored {len(rows)} finding(s) for run {run_id} in "
        f"{(time.perf_counter() - t0) * 1000:.1f} ms"
    )
    return len(rows)


def findings_for_records(db: "DatabaseManager", record_ids) -> dict[str, list[dict]]:
    """``{record_id: [finding dict]}``: the findings of the newest run that
    wrote any for each record. ``{}`` before the table has the column."""
    from scidb.provenance.outcomes import TABLE
    from scidb.provenance.query import _chunked_in

    ids = list(dict.fromkeys(record_ids))
    duck = db._duck
    if not ids or not duck.table_exists(TABLE):
        return {}
    rows = _chunked_in(
        duck,
        f"SELECT record_id, finding, reason, run_id, recorded_at FROM {TABLE} "
        f"WHERE outcome = '{OUTCOME}' AND record_id IN ({{ph}})",
        ids,
    )
    newest_run: dict[str, Any] = {}
    for rid, _kind, _text, run_id, at in rows:
        prev = newest_run.get(rid)
        if prev is None or at > prev[1]:
            newest_run[rid] = (run_id, at)
    out: dict[str, list[dict]] = {}
    for rid, kind, text, run_id, _at in rows:
        if newest_run[rid][0] != run_id:
            continue
        out.setdefault(rid, []).append({
            "kind": kind, "label": LABELS.get(kind, kind), "text": text, "run_id": run_id,
        })
    return out


def findings_of_run(db: "DatabaseManager", run_id: str) -> list[dict]:
    """Every finding a run stored (the run report's "What looks odd?")."""
    from scidb.provenance.outcomes import TABLE

    duck = db._duck
    if not duck.table_exists(TABLE):
        return []
    schema_keys = list(db.dataset_schema_keys)
    key_cols = "".join(f'"{k}", ' for k in schema_keys)
    rows = duck.fetchall(
        f"SELECT {key_cols}output_types, record_id, finding, reason FROM {TABLE} "
        f"WHERE run_id = ? AND outcome = '{OUTCOME}'",
        [run_id],
    )
    n = len(schema_keys)
    return [
        {
            "location": {k: str(v) for k, v in zip(schema_keys, row[:n]) if v is not None},
            "variable": (list(row[n] or []) or [None])[0],
            "record_id": row[n + 1],
            "kind": row[n + 2],
            "label": LABELS.get(row[n + 2], row[n + 2]),
            "text": row[n + 3],
        }
        for row in rows
    ]
