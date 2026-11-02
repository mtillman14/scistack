"""The reduction a figure went through, as typed steps (design review 06, P2).

``reduce`` APPLIES each step; ``export.reduction_code`` PRINTS it as plain
pandas, one emitter per step type. A step type that lives here is therefore
defined once: the rule is the step's own method, and codegen prints the same
rule rather than re-deriving it. :attr:`ResolvedPlot.reduction` records the
plan a figure was built with; the renderers ignore it.

Migrated so far (one step type per change, each held to "export equals
preview" by the export tests):

* :class:`LocationMask` — ``spec.location_filter``;
* :class:`ValueFilter` — one entry of ``spec.filters``.

Still re-derived by codegen (not yet steps here): the 1-D cell collapse, the
categorical rewrite, level groups, the collapse chain, compare-to-reference,
histogram bins.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Union

import pandas as pd

from ..model.spec import Filter, LocationFilter, PlotSpec


@dataclass(frozen=True)
class LocationMask:
    """Keep the rows ``location_filter`` selects (``resolve._location_mask``;
    semantics in docs/claude/location-filter-semantics.md)."""

    location_filter: LocationFilter


@dataclass(frozen=True)
class ValueFilter:
    """Keep the rows one :class:`~scistackplot.model.spec.Filter` keeps.

    Level membership is compared AS TEXT (a picker's selection crosses JSON as
    strings while the column may hold ``1`` or ``"01"``); the bounds compare
    the column coerced to numbers, so a non-number never passes a bound.
    A column the frame lacks constrains nothing (a spec outlives its table).
    """

    column: str
    include: tuple[str, ...] | None = None
    exclude: tuple[str, ...] | None = None
    minimum: float | None = None
    maximum: float | None = None

    @classmethod
    def of(cls, flt: Filter) -> "ValueFilter":
        """The step for one spec filter; level lists become sorted text, so
        one spec always prints byte-identical code."""
        return cls(
            column=flt.column,
            include=_texts(flt.include),
            exclude=_texts(flt.exclude),
            minimum=flt.minimum,
            maximum=flt.maximum,
        )

    def mask(self, frame: pd.DataFrame) -> pd.Series:
        """The rows of ``frame`` this filter keeps (all of them when the
        column is absent)."""
        keep = pd.Series(True, index=frame.index)
        if self.column not in frame.columns:
            return keep
        column = frame[self.column]
        if self.include is not None:
            keep &= column.astype(str).isin(self.include)
        if self.exclude is not None:
            keep &= ~column.astype(str).isin(self.exclude)
        if self.minimum is not None:
            keep &= pd.to_numeric(column, errors="coerce") >= self.minimum
        if self.maximum is not None:
            keep &= pd.to_numeric(column, errors="coerce") <= self.maximum
        return keep


ReductionStep = Union[LocationMask, ValueFilter]


@dataclass(frozen=True)
class ReductionPlan:
    """The ordered steps a figure's rows went through."""

    steps: tuple[ReductionStep, ...] = ()

    def of_type(self, kind: type) -> list[Any]:
        """The steps of one type, in order."""
        return [step for step in self.steps if isinstance(step, kind)]


def filter_steps(spec: PlotSpec, skip_columns: "frozenset[str]" = frozenset()) -> ReductionPlan:
    """The filtering half of the reduction: the location mask (when it is not
    inert), then one :class:`ValueFilter` per ``spec.filters`` entry whose
    column is not in ``skip_columns`` (codegen skips a filter on a text
    measure, which its categorical block already applied)."""
    steps: list[ReductionStep] = []
    if spec.location_filter.prefixes() or spec.location_filter.excluded():
        steps.append(LocationMask(spec.location_filter))
    steps.extend(
        ValueFilter.of(flt) for flt in spec.filters if flt.column not in skip_columns
    )
    return ReductionPlan(tuple(steps))


def _texts(values: "list[Any] | None") -> "tuple[str, ...] | None":
    return None if values is None else tuple(sorted({str(v) for v in values}))
