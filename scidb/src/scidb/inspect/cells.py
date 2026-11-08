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
