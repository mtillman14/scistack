"""What is inside a variable's cells: NaNs and other suspect values that a
row count cannot see.

A record that holds a vector of 120 per-minute values, 40 of them NaN, is
one present, non-blank cell to every other check (the preview's non-blank
count, the expected-locations view). This module looks inside it. It is the
one owner of the per-location view (which location, which column) and of
loading. What counts as blank, empty, all NaN, partly NaN or constant, and
the counts' wording, belong to ``scicanonicalhash._cells``, the one reader
shared with scistackplot's plots; import those names from there. ``scidb
nans``, the data preview (``scidb.inspect.preview``) and the GUI only display
them.

Per cell (one row's value in one column):

* **blank** — a scalar NaN / None: the value itself is missing;
* **array** — a numeric vector or matrix (ndarray, or a list of numbers);
  its elements are counted, and the cell is
  * **empty** — no elements,
  * **all NaN** — every element NaN,
  * **partly NaN** — some elements NaN,
  * **constant** — two or more non-NaN elements, all equal (a sensor that
    stopped, a fill value).

Text, dicts and other objects are counted as cells and otherwise left alone.
Loads go through ``load_all_as_df(layout="spread")``, the loader ``for_each``
uses, so this is what a function receives.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from scicanonicalhash._cells import (
    ALL_NAN,
    CONSTANT,
    PARTIAL_NAN,
    PROBLEM_KINDS,
    PROBLEM_LABELS,
    CellStats,
    classify,
)

from scidb.model.log import Log


@dataclass
class CellProblem:
    """One suspect cell."""

    location: dict[str, str]
    column: str
    kind: str
    elements: int
    nan_elements: int

    @property
    def label(self) -> str:
        return PROBLEM_LABELS[self.kind]

    @property
    def detail(self) -> str:
        if self.kind in (ALL_NAN, PARTIAL_NAN):
            return f"{self.nan_elements} of {self.elements} values NaN"
        if self.kind == CONSTANT:
            return f"{self.elements} equal values"
        return ""

    def to_dict(self) -> dict:
        return {
            "location": dict(self.location),
            "column": self.column,
            "kind": self.kind,
            "label": self.label,
            "detail": self.detail,
            "elements": self.elements,
            "nan_elements": self.nan_elements,
        }


@dataclass
class CellProfile:
    variable: str
    schema_keys: list[str]
    location: dict[str, str]
    n_rows: int = 0
    columns: dict[str, CellStats] = field(default_factory=dict)
    problems: list[CellProblem] = field(default_factory=list)
    missing_columns: list[str] = field(default_factory=list)

    def by(self, key: str) -> list[tuple[str, dict[str, int]]]:
        """Problem counts per value of *key*, by kind, in first-seen order."""
        if key not in self.schema_keys:
            raise ValueError(f"{key!r} is not a schema key: {self.schema_keys}")
        out: dict[str, dict[str, int]] = {}
        for p in self.problems:
            value = p.location.get(key, "")
            out.setdefault(value, {k: 0 for k in PROBLEM_KINDS})[p.kind] += 1
        return list(out.items())

    def to_dict(self, limit: int | None = None) -> dict:
        problems = self.problems if limit is None else self.problems[:limit]
        return {
            "variable": self.variable,
            "schema_keys": list(self.schema_keys),
            "location": dict(self.location),
            "n_rows": self.n_rows,
            "columns": {c: s.to_dict() for c, s in self.columns.items()},
            "problems": [p.to_dict() for p in problems],
            "total_problems": len(self.problems),
            "truncated": len(problems) < len(self.problems),
            "missing_columns": list(self.missing_columns),
            "labels": dict(PROBLEM_LABELS),
        }


def cell_profile(
    db, variable, *, columns: list[str] | None = None, location: dict | None = None
) -> CellProfile:
    """Every cell of *variable* (optionally some columns, at a location)."""
    from sciduckdb import schema_str

    from scidb.inspect.preview import _variable_class

    t0 = time.perf_counter()
    name = getattr(variable, "__name__", variable)
    keys = list(db.dataset_schema_keys)
    location = {k: schema_str(v) for k, v in (location or {}).items() if v not in (None, "")}
    unknown = sorted(set(location) - set(keys))
    if unknown:
        raise ValueError(f"{unknown} are not schema keys. Schema keys: {keys}")
    frame = db.load_all_as_df(
        _variable_class(name),
        metadata=location or None,
        layout="spread",
        include_rid=True,
        stringify_schema=True,
    )
    data_columns = [c for c in frame.columns if c not in keys and not str(c).startswith("__")]
    profile = CellProfile(variable=name, schema_keys=keys, location=location, n_rows=len(frame))
    if columns:
        profile.missing_columns = [c for c in columns if c not in data_columns]
        data_columns = [c for c in columns if c in data_columns]
    key_columns = [k for k in keys if k in frame.columns]
    locs = frame[key_columns].to_dict("records") if key_columns else [{}] * len(frame)
    for col in data_columns:
        stats = CellStats()
        for loc, value in zip(locs, frame[col].tolist()):
            kind = stats.add(value)
            if kind is not None:
                _, n, nan = classify(value)
                profile.problems.append(CellProblem(
                    location={k: str(v) for k, v in loc.items() if v is not None and v == v},
                    column=col, kind=kind, elements=n, nan_elements=nan,
                ))
        profile.columns[col] = stats
    order = {k: i for i, k in enumerate(PROBLEM_KINDS)}
    profile.problems.sort(key=lambda p: order[p.kind])
    Log.info(
        f"[cells] {name}: {len(frame)} row(s), {len(data_columns)} column(s), "
        f"{len(profile.problems)} suspect cell(s) "
        + str({k: sum(s.kinds[k] for s in profile.columns.values()) for k in PROBLEM_KINDS})
        + f" in {(time.perf_counter() - t0) * 1000:.1f} ms"
    )
    return profile


# ---------------------------------------------------------------------------
# One value's shape, and the records of a run (scidb.provenance.run_report)
# ---------------------------------------------------------------------------

#: Characters of a scalar or text value shown inline.
_VALUE_TEXT_MAX = 60


@dataclass(frozen=True)
class ValueShape:
    """What one cell holds, without its values: the size and shape a
    researcher checks first. ``shape`` is ``()`` for a scalar. ``text`` is the
    value itself for a scalar or short text, else ``""``."""

    kind: str  # blank / scalar / text / array / dict / other
    shape: tuple[int, ...]
    dtype: str
    nbytes: int | None
    text: str = ""

    @property
    def shape_text(self) -> str:
        """``scalar``, ``1200``, ``3×1200``; THE spelling of a shape."""
        if self.kind == "blank":
            return "blank"
        if self.kind == "text":
            return f"text ({self.shape[0]} chars)"
        if self.kind == "dict":
            return f"dict ({self.shape[0]} keys)"
        if not self.shape:
            return "scalar" if self.kind == "scalar" else self.kind
        return "×".join(str(n) for n in self.shape)

    def to_dict(self) -> dict:
        return {
            "kind": self.kind,
            "shape": list(self.shape),
            "shape_text": self.shape_text,
            "dtype": self.dtype,
            "nbytes": self.nbytes,
            "text": self.text,
        }


def describe_value(value) -> ValueShape:
    """The :class:`ValueShape` of one cell. Blank is ``is_blank``'s answer
    (``scicanonicalhash._cells``), so this and the problem kinds agree."""
    import numpy as np

    from scicanonicalhash._cells import is_blank

    if is_blank(value):
        return ValueShape("blank", (), type(value).__name__, None)
    if isinstance(value, np.ndarray):
        if value.ndim == 0:
            return ValueShape(
                "scalar", (), str(value.dtype), int(value.nbytes), str(value.item())[:_VALUE_TEXT_MAX]
            )
        return ValueShape("array", tuple(int(n) for n in value.shape), str(value.dtype), int(value.nbytes))
    if isinstance(value, (bool, int, float, np.generic)):
        nbytes = int(value.nbytes) if isinstance(value, np.generic) else 8
        return ValueShape("scalar", (), type(value).__name__, nbytes, str(value)[:_VALUE_TEXT_MAX])
    if isinstance(value, str):
        return ValueShape(
            "text", (len(value),), "str", len(value.encode("utf-8", "replace")), value[:_VALUE_TEXT_MAX]
        )
    if isinstance(value, (list, tuple)):
        try:
            arr = np.asarray(value)
        except (TypeError, ValueError):  # ragged nesting
            return ValueShape("array", (len(value),), "ragged list", None)
        if arr.dtype.kind == "O":
            return ValueShape("array", (len(value),), "list", None)
        return ValueShape("array", tuple(int(n) for n in arr.shape), str(arr.dtype), int(arr.nbytes))
    if isinstance(value, dict):
        return ValueShape("dict", (len(value),), "dict", None)
    return ValueShape("other", (), type(value).__name__, None, str(value)[:_VALUE_TEXT_MAX])


@dataclass
class RecordCells:
    """One record's cells: per data column its shape (one row) or its
    row count (a table stored one row per inner row), and the cell counts."""

    record_id: str
    location: dict[str, str]
    n_rows: int
    #: column -> ValueShape (only when the record is one row).
    shapes: dict[str, ValueShape] = field(default_factory=dict)
    #: column -> CellStats over the record's rows.
    cells: dict[str, CellStats] = field(default_factory=dict)

    @property
    def nan_elements(self) -> int:
        return sum(s.nan_elements for s in self.cells.values())

    @property
    def problems(self) -> int:
        return sum(s.problems for s in self.cells.values())

    @property
    def nbytes(self) -> int:
        return sum(s.nbytes or 0 for s in self.shapes.values())

    def shape_text(self) -> str:
        """One spelling for the whole record: the column's shape when there
        is one data column, else ``R rows × C columns``."""
        if self.n_rows == 1 and len(self.shapes) == 1:
            return next(iter(self.shapes.values())).shape_text
        n_cols = len(self.cells)
        return (
            f"{self.n_rows} row{'' if self.n_rows == 1 else 's'} × "
            f"{n_cols} column{'' if n_cols == 1 else 's'}"
        )

    def findings(self) -> str:
        """The suspect cells, in ``PROBLEM_LABELS``' words; ``""`` for none."""
        parts = []
        for kind in PROBLEM_KINDS:
            n = sum(s.kinds[kind] for s in self.cells.values())
            if n:
                parts.append(f"{n} {PROBLEM_LABELS[kind]}" if n > 1 or self.n_rows > 1 else PROBLEM_LABELS[kind])
        if self.nan_elements and self.n_rows == 1:
            total = sum(s.elements for s in self.cells.values())
            parts.append(f"{self.nan_elements} of {total} values NaN")
        return ", ".join(parts)

    def to_dict(self) -> dict:
        one = next(iter(self.shapes.values())) if len(self.shapes) == 1 else None
        return {
            "record_id": self.record_id,
            "n_rows": self.n_rows,
            "shape_text": self.shape_text(),
            "nbytes": self.nbytes or None,
            "value": one.text if one is not None else "",
            "dtype": one.dtype if one is not None else "",
            "nan_elements": self.nan_elements,
            "problems": self.problems,
            "findings": self.findings(),
            "columns": {
                c: {**(self.shapes[c].to_dict() if c in self.shapes else {}), "cells": s.to_dict()}
                for c, s in self.cells.items()
            } if len(self.cells) > 1 else {},
        }


def record_cells(db, variable, record_ids) -> dict[str, RecordCells]:
    """``{record_id: RecordCells}`` for some records of *variable*, whatever
    their version: the records a run read or wrote, not the variable's latest.

    One load of the variable (every version, ``layout="spread"``, the loader
    ``for_each`` uses), filtered to *record_ids*; never one load per record.
    A record id with no data row (a lineage-only record) is absent.
    """
    from scidb.inspect.preview import _variable_class
    from scidb.run.bindings import RECORD_ID_COLUMN

    t0 = time.perf_counter()
    name = getattr(variable, "__name__", variable)
    wanted = {str(r) for r in record_ids}
    if not wanted:
        return {}
    keys = list(db.dataset_schema_keys)
    frame = db.load_all_as_df(
        _variable_class(name),
        layout="spread",
        include_rid=True,
        stringify_schema=True,
        version_id="all",
    )
    if frame.empty or RECORD_ID_COLUMN not in frame.columns:
        Log.info(f"[cells] record_cells({name}): no stored rows for {len(wanted)} record(s)")
        return {}
    n_loaded = len(frame)
    frame = frame[frame[RECORD_ID_COLUMN].astype(str).isin(wanted)]
    data_columns = [c for c in frame.columns if c not in keys and not str(c).startswith("__")]
    key_columns = [k for k in keys if k in frame.columns]
    out: dict[str, RecordCells] = {}
    for rid, group in frame.groupby(frame[RECORD_ID_COLUMN].astype(str), sort=False):
        first = group.iloc[0]
        location = {
            k: str(first[k]) for k in key_columns if first[k] is not None and first[k] == first[k]
        }
        rec = RecordCells(record_id=str(rid), location=location, n_rows=len(group))
        for col in data_columns:
            values = group[col].tolist()
            stats = CellStats()
            for v in values:
                stats.add(v)
            rec.cells[col] = stats
            if len(values) == 1:
                rec.shapes[col] = describe_value(values[0])
        out[str(rid)] = rec
    Log.info(
        f"[cells] record_cells({name}): {len(out)} of {len(wanted)} record(s) "
        f"from {n_loaded} loaded row(s), {len(data_columns)} column(s) in "
        f"{(time.perf_counter() - t0) * 1000:.1f} ms"
    )
    return out
