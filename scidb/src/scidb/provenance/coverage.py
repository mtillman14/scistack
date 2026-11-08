"""Coverage across the pipeline, and one location followed through it.

Both are summaries of :func:`scidb.provenance.expectation.expectation`, run
once per variable, so a cell here can never disagree with the variable's
own expected-locations view. Nothing in this module decides a status.

* :func:`coverage` — variables (pipeline order) x values of the ``by`` keys:
  how many expected locations each cell holds, how many are present, how
  many are problems. The per-variable count of values with data is the
  attrition funnel ("40 subjects, then 38, then 31").
* :func:`follow` — one location (``subject=S07 trial=2``) through every
  variable in pipeline order: its status and why at each one, and where it
  is first missing although an upstream variable has it.

Variables come from ``provenance.query.variable_types_in_pipeline_order``.
Design: ``docs/claude/expected-locations.md``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from scidb.model.log import Log
from scidb.provenance import expectation as ex

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager

#: A cell with no expected or present location.
EMPTY = "empty"


@dataclass
class CoverageCell:
    """One variable at one value of the ``by`` keys."""

    counts: dict[str, int] = field(default_factory=ex._zero_counts)

    @property
    def total(self) -> int:
        return sum(self.counts.values())

    @property
    def present(self) -> int:
        return self.counts[ex.PRESENT] + self.counts[ex.PRESENT_UNEXPECTED]

    @property
    def problems(self) -> int:
        return self.counts[ex.MISSING_UNEXPECTED] + self.counts[ex.PRESENT_UNEXPECTED]

    @property
    def tone(self) -> str:
        """``problem`` if any location is; ``excluded`` if all are; ``empty``
        when nothing is expected or present; else ``ok``."""
        if self.total == 0:
            return EMPTY
        if self.problems:
            return ex.PROBLEM
        if self.counts[ex.EXCLUDED] == self.total:
            return ex.EXCLUDED
        return ex.OK

    def to_dict(self) -> dict:
        return {
            "counts": dict(self.counts),
            "total": self.total,
            "present": self.present,
            "problems": self.problems,
            "tone": self.tone,
        }


@dataclass
class Coverage:
    by: list[str]
    #: Values of the ``by`` keys (``(all)`` for a wildcard), in level order.
    values: list[tuple[str, ...]] = field(default_factory=list)
    #: Variables that have every ``by`` key, in pipeline order.
    variables: list[str] = field(default_factory=list)
    cells: dict[tuple[str, tuple], CoverageCell] = field(default_factory=dict)
    #: Variables left out, and why.
    skipped: dict[str, str] = field(default_factory=dict)

    def cell(self, variable: str, value: tuple) -> CoverageCell:
        return self.cells.get((variable, value)) or CoverageCell()

    def funnel(self) -> list[tuple[str, int, int]]:
        """``(variable, values with any data, values with anything expected)``."""
        out = []
        for v in self.variables:
            cells = [self.cell(v, value) for value in self.values]
            out.append(
                (v, sum(1 for c in cells if c.present), sum(1 for c in cells if c.total))
            )
        return out

    def to_dict(self) -> dict:
        return {
            "by": list(self.by),
            "values": [list(v) for v in self.values],
            "variables": list(self.variables),
            "cells": {
                v: {"|".join(value): self.cell(v, value).to_dict() for value in self.values}
                for v in self.variables
            },
            "funnel": [
                {"variable": v, "with_data": n, "expected": total}
                for v, n, total in self.funnel()
            ],
            "skipped": dict(self.skipped),
            "labels": dict(ex.STATUS_LABELS),
        }


@dataclass
class FollowStep:
    """One variable's rows at the followed location."""

    variable: str
    #: The variable's keys (its expected-locations columns).
    keys: list[str]
    rows: list[ex.ExpectationRow] = field(default_factory=list)
    fed_by: list[str] = field(default_factory=list)
    #: The followed keys this variable does not have (it is coarser).
    ignored_keys: list[str] = field(default_factory=list)

    @property
    def counts(self) -> dict[str, int]:
        c = ex._zero_counts()
        for r in self.rows:
            c[r.status] += 1
        return c

    @property
    def has_data(self) -> bool:
        return any(r.present for r in self.rows)

    @property
    def summary(self) -> str:
        """The row's own why when there is one row; else counts, then the
        first problem's why."""
        if not self.rows:
            return "no expected or present location here"
        if len(self.rows) == 1:
            return self.rows[0].why
        c = self.counts
        text = ", ".join(
            f"{n} {ex.STATUS_LABELS[s].lower()}" for s, n in c.items() if n
        )
        problem = next((r for r in self.rows if r.tone == ex.PROBLEM), None)
        return text + (f". First problem: {problem.why}" if problem else ".")

    @property
    def tone(self) -> str:
        if not self.rows:
            return EMPTY
        if any(r.tone == ex.PROBLEM for r in self.rows):
            return ex.PROBLEM
        if all(r.status == ex.EXCLUDED for r in self.rows):
            return ex.EXCLUDED
        return ex.OK

    def to_dict(self) -> dict:
        return {
            "variable": self.variable,
            "keys": list(self.keys),
            "ignored_keys": list(self.ignored_keys),
            "fed_by": list(self.fed_by),
            "counts": self.counts,
            "has_data": self.has_data,
            "tone": self.tone,
            "summary": self.summary,
            "rows": [r.to_dict() for r in self.rows],
        }


@dataclass
class Follow:
    location: dict[str, str]
    steps: list[FollowStep] = field(default_factory=list)
    #: Variables without data here although a variable feeding them has it.
    lost_at: list[str] = field(default_factory=list)
    #: Variables left out, and why.
    skipped: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "location": dict(self.location),
            "steps": [s.to_dict() for s in self.steps],
            "lost_at": list(self.lost_at),
            "skipped": dict(self.skipped),
        }


def _expectations(db, variables, t0):
    """``expectation()`` for each variable, in pipeline order; failures are
    reported per variable, never dropped silently."""
    from scidb.provenance.query import variable_type_edges, variable_types_in_pipeline_order

    edges = variable_type_edges(db._duck)
    order = variable_types_in_pipeline_order(db._duck, edges)
    if variables is not None:
        wanted = [getattr(v, "__name__", v) for v in variables]
        unknown = [v for v in wanted if v not in order]
        order = [v for v in order if v in wanted] + unknown
    results, skipped = {}, {}
    for name in order:
        try:
            results[name] = ex.expectation(db, name)
        except ValueError as e:
            skipped[name] = str(e)
            Log.warn(f"coverage: {name} skipped: {e}")
    Log.debug(
        f"coverage: {len(results)} expectation(s) in {(time.perf_counter() - t0) * 1000:.1f} ms"
    )
    return order, results, skipped, edges


def coverage(db: DatabaseManager, by: list[str], variables=None) -> Coverage:
    """Every variable that has all of the *by* keys, at every value of them."""
    t0 = time.perf_counter()
    schema_keys = list(db.dataset_schema_keys)
    requested = list(by or [])
    unknown = sorted(set(requested) - set(schema_keys))
    by = [k for k in schema_keys if k in requested]
    if not by or unknown:
        raise ValueError(
            f"Choose schema key(s) to cover by; got {requested}"
            + (f" (unknown: {unknown})" if unknown else "")
            + f". Schema keys: {schema_keys}"
        )
    order, results, skipped, _edges = _expectations(db, variables, t0)
    result = Coverage(by=by, skipped=skipped)
    seen: set[tuple] = set()
    for name in order:
        e = results.get(name)
        if e is None:
            continue
        if not set(by) <= set(e.keys):
            result.skipped[name] = f"saved at {'/'.join(e.keys) or 'no keys'}, without {by}"
            continue
        result.variables.append(name)
        unplaced = 0
        for r in e.rows:
            if not set(by) <= set(r.values):
                unplaced += 1
                continue
            value = tuple(ex.ALL_LEVELS if r.values[k] is None else r.values[k] for k in by)
            seen.add(value)
            result.cells.setdefault((name, value), CoverageCell()).counts[r.status] += 1
        if unplaced:
            Log.debug(f"coverage {name}: {unplaced} row(s) at other keys, not placed by {by}")
    rank = {k: {v: i for i, v in enumerate(str(x) for x in db.distinct_schema_values(k))} for k in by}
    result.values = sorted(
        seen, key=lambda v: tuple(rank[k].get(x, len(rank[k])) for k, x in zip(by, v))
    )
    Log.info(
        f"coverage by {by}: {len(result.variables)} variable(s) x {len(result.values)} value(s), "
        f"{len(result.skipped)} skipped, in {(time.perf_counter() - t0) * 1000:.1f} ms"
    )
    return result


def follow(db: DatabaseManager, variables=None, **location) -> Follow:
    """*location* through every variable, in pipeline order."""
    from scidb.provenance.query import related_variable_types
    from scidb.store.schema_values import schema_str

    t0 = time.perf_counter()
    schema_keys = list(db.dataset_schema_keys)
    if not location:
        raise ValueError("Name a location to follow, e.g. subject=S07 trial=2.")
    unknown = sorted(set(location) - set(schema_keys))
    if unknown:
        raise ValueError(f"Unknown schema key(s): {unknown}. Schema keys: {schema_keys}")
    location = {k: schema_str(location[k]) for k in schema_keys if k in location}
    order, results, skipped, edges = _expectations(db, variables, t0)
    result = Follow(location=location, skipped=skipped)
    had_data: dict[str, bool] = {}
    for name in order:
        e = results.get(name)
        if e is None:
            continue
        shared = [k for k in location if k in e.columns]
        if not shared:
            result.skipped[name] = f"has none of {list(location)}"
            continue
        rows = [
            r for r in e.rows
            if all(r.values.get(k) in (location[k], None) for k in shared if k in r.values)
            and any(k in r.values for k in shared)
        ]
        up = related_variable_types(db._duck, name, "upstream", edges=edges)
        step = FollowStep(
            variable=name,
            keys=list(e.keys),
            rows=rows,
            fed_by=sorted(t for t, d in up.items() if d == 1),
            ignored_keys=[k for k in location if k not in e.columns],
        )
        result.steps.append(step)
        had_data[name] = step.has_data
        if not step.has_data and any(had_data.get(u) for u in step.fed_by):
            result.lost_at.append(name)
    Log.info(
        f"follow {location}: {len(result.steps)} variable(s), lost at {result.lost_at or 'nowhere'}, "
        f"in {(time.perf_counter() - t0) * 1000:.1f} ms"
    )
    return result
