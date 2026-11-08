"""What is inside a variable's cells: NaNs and other suspect values that a
row count cannot see.

A record that holds a vector of 120 per-minute values, 40 of them NaN, is
one present, non-blank cell to every other check (the preview's non-blank
count, the expected-locations view). This module looks inside it. It is the
one owner of these counts and their wording; ``scidb nans``, the data
preview (``scidb.inspect.preview``) and the GUI only display them.

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
from typing import Any

import numpy as np

from scidb.model.log import Log

BLANK = "blank"
EMPTY = "empty"
ALL_NAN = "all_nan"
PARTIAL_NAN = "partial_nan"
CONSTANT = "constant"
#: Every problem kind, worst first.
PROBLEM_KINDS = (ALL_NAN, PARTIAL_NAN, BLANK, EMPTY, CONSTANT)

#: The one wording of each problem kind.
PROBLEM_LABELS = {
    ALL_NAN: "all NaN",
    PARTIAL_NAN: "partly NaN",
    BLANK: "blank",
    EMPTY: "empty array",
    CONSTANT: "constant",
}


def numeric_elements(value: Any) -> np.ndarray | None:
    """The value's elements as a float array when it is a numeric array
    (ndarray of numbers or bools, or a non-empty list/tuple of numbers);
    else None. Scalars are not arrays."""
    if isinstance(value, np.ndarray):
        if value.dtype.kind in "biuf":
            return value.astype(float, copy=False).ravel()
        if value.dtype.kind == "O" and value.size:
            try:
                return np.asarray(value.ravel(), dtype=float)
            except (TypeError, ValueError):
                return None
        return np.asarray([], dtype=float) if value.size == 0 else None
    if isinstance(value, (list, tuple)):
        if not value:
            return np.asarray([], dtype=float)
        if all(isinstance(v, (int, float, np.integer, np.floating, bool, np.bool_)) or v is None
               for v in value):
            return np.asarray([np.nan if v is None else v for v in value], dtype=float)
    return None


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, (float, np.floating)):
        return bool(np.isnan(value))
    try:
        import pandas as pd

        return value is pd.NA or value is pd.NaT
    except ImportError:  # pragma: no cover
        return False


def classify(value: Any) -> tuple[str | None, int, int]:
    """``(problem kind or None, elements, NaN elements)`` for one cell."""
    if _is_blank(value):
        return BLANK, 0, 0
    arr = numeric_elements(value)
    if arr is None:
        return None, 0, 0
    n = int(arr.size)
    if n == 0:
        return EMPTY, 0, 0
    nan = int(np.isnan(arr).sum())
    if nan == n:
        return ALL_NAN, n, nan
    if nan:
        return PARTIAL_NAN, n, nan
    if n >= 2 and float(np.nanmin(arr)) == float(np.nanmax(arr)):
        return CONSTANT, n, nan
    return None, n, nan


@dataclass
class CellStats:
    """Counts over the cells of one column (or one column at one value)."""

    cells: int = 0
    array_cells: int = 0
    elements: int = 0
    nan_elements: int = 0
    min_length: int | None = None
    max_length: int | None = None
    kinds: dict[str, int] = field(default_factory=lambda: {k: 0 for k in PROBLEM_KINDS})

    def add(self, value: Any) -> str | None:
        """Count one cell; returns its problem kind (or None)."""
        kind, n, nan = classify(value)
        self.cells += 1
        if kind not in (BLANK,) and (n or kind == EMPTY):
            self.array_cells += 1
            self.elements += n
            self.nan_elements += nan
            self.min_length = n if self.min_length is None else min(self.min_length, n)
            self.max_length = n if self.max_length is None else max(self.max_length, n)
        if kind is not None:
            self.kinds[kind] += 1
        return kind

    @property
    def problems(self) -> int:
        return sum(self.kinds.values())

    def describe(self) -> str:
        """One line: what is inside, and what looks wrong."""
        parts = []
        if self.array_cells:
            lengths = (
                f"length {self.min_length}" if self.min_length == self.max_length
                else f"lengths {self.min_length}-{self.max_length}"
            )
            parts.append(f"{self.array_cells} of {self.cells} cells are arrays ({lengths})")
            if self.nan_elements:
                share = 100.0 * self.nan_elements / self.elements if self.elements else 0.0
                parts.append(
                    f"{self.nan_elements} of {self.elements} values are NaN ({share:.1f}%)"
                )
        problems = [
            f"{self.kinds[k]} {PROBLEM_LABELS[k]}" for k in PROBLEM_KINDS if self.kinds[k]
        ]
        if problems:
            parts.append("cells: " + ", ".join(problems))
        return "; ".join(parts) if parts else "nothing suspect"

    def to_dict(self) -> dict:
        return {
            "cells": self.cells,
            "array_cells": self.array_cells,
            "elements": self.elements,
            "nan_elements": self.nan_elements,
            "min_length": self.min_length,
            "max_length": self.max_length,
            "kinds": dict(self.kinds),
            "problems": self.problems,
            "description": self.describe(),
        }


def cell_stats(values) -> CellStats:
    """:class:`CellStats` over an iterable of cells."""
    stats = CellStats()
    for v in values:
        stats.add(v)
    return stats


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
