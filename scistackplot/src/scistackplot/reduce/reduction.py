"""The reduction a figure went through, as typed steps (design review 06, P2).

``reduce`` APPLIES each step; ``export.reduction_code`` PRINTS it as plain
pandas, one emitter per step type. A step type that lives here is therefore
defined once: the rule is the step's own method, and codegen prints the same
rule rather than re-deriving it. :attr:`ResolvedPlot.reduction` records the
plan a figure was built with; the renderers ignore it.

Migrated so far (one step type per change, each held to "export equals
preview" by the export tests):

* :class:`LocationMask` — ``spec.location_filter``;
* :class:`ValueFilter` — one entry of ``spec.filters``;
* :class:`CombineLevels` — one active ``spec.level_groups`` entry
  (``groups.combine_steps``);
* :class:`CollapseCells` — a 1-D measure drawn by a scalar kind
  (``cell.collapse_step``);
* :class:`CategoricalRewrite` — a text measure as numbers
  (``categorical.categorical_step``);
* ``compare.ComparisonPlan`` — compare to a reference level (its own module,
  planned per figure);
* :class:`CollapseLevels` — one pass of the collapse chain
  (``chain.collapse_passes``).

Histogram bins are not a step: ``bins.py`` keeps their rules as source text
(``EDGES_SOURCE``, ``BARS_SOURCE``) that the preview executes and the export
embeds verbatim, which is the same one-owner guarantee by another route.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Union

import numpy as np
import pandas as pd

from ..model.spec import CategoricalStat, Filter, LevelGroup, LocationFilter, PlotSpec, Statistic
from ..model.table import category_text
from .compare import ComparisonPlan
from .series_stats import cell_arrays, collapse_cells


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


@dataclass(frozen=True)
class CombineLevels:
    """One active combine (:class:`~scistackplot.model.spec.LevelGroup`): a
    new factor ``name`` mapped from ``source``'s levels, read as text.

    ``mapping`` is the EFFECTIVE mapping, in the spec's order: an empty label
    means "in no bucket" (the editor's (drop)), never a bucket named ``""``.
    ``unmatched`` None drops the rows the mapping does not name; any other
    value is the bucket they land in. Which combines are in effect is
    ``groups.active_level_groups``'s rule.
    """

    name: str
    source: str
    mapping: tuple[tuple[str, str], ...]
    unmatched: str | None = None

    @classmethod
    def of(cls, group: LevelGroup) -> "CombineLevels":
        """The step for one combine; the one owner of "effective mapping"."""
        mapping = tuple(
            (str(level), str(label))
            for level, label in group.mapping.items()
            if label is not None and str(label) != ""
        )
        return cls(name=group.name, source=group.source, mapping=mapping, unmatched=group.unmatched)

    def skip_reason(self, columns: "pd.Index | list[str]") -> str | None:
        """Why this combine cannot apply to a frame with ``columns``, or None.
        A spec outlives its table (the source may be gone), and writing over
        an existing column would silently replace real data."""
        if self.source not in columns:
            return f"reads {self.source!r}, which this table has no column for"
        if self.name in columns:
            return f"{self.name!r} is already a column of this table"
        return None

    def apply(self, frame: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """``frame`` with the combined column, and how many rows it dropped
        (unmatched rows, when ``unmatched`` is None)."""
        mapped = frame[self.source].astype(str).map(dict(self.mapping).get)
        if self.unmatched is None:
            keep = mapped.notna()
            dropped = len(keep) - int(keep.to_numpy().sum())
            return frame[keep].assign(**{self.name: mapped[keep]}), dropped
        return frame.assign(**{self.name: mapped.fillna(self.unmatched)}), 0

    def declared_levels(self) -> list[str]:
        """The buckets in the order the user wrote them, the catch-all last."""
        order = list(dict.fromkeys(label for _, label in self.mapping))
        if self.unmatched is not None and self.unmatched not in order:
            order.append(self.unmatched)
        return order


@dataclass(frozen=True)
class CollapseCells:
    """A 1-D measure reduced to one value per record by its ``statistic``,
    because a scalar kind was picked for it (``cell.cell_collapses`` decides).

    A cell is read by ``series_stats.cell_array``: an array or list of
    numbers is its samples; anything else (a lone number, None, a text array)
    has none and collapses to NaN, kept as a gap. NaN samples are skipped, as
    in pandas' ``Series.mean()``. ``exploded``: the source already holds one
    row per SAMPLE, with ``index_column`` as the within-record axis; the rows
    of one record (every other column equal) collapse into one row.
    """

    measure: str
    statistic: Statistic
    exploded: bool = False
    index_column: str | None = None

    def apply(self, frame: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
        """The collapsed frame, its row count and the samples it read."""
        if self.exploded:
            return self._apply_exploded(frame)
        arrays = cell_arrays(frame[self.measure])
        out = frame.copy()
        out[self.measure] = collapse_cells(arrays, self.statistic)
        return out, len(out), int(sum(a.size for a in arrays))

    def _apply_exploded(self, frame: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
        keys = [c for c in frame.columns if c not in (self.measure, self.index_column)]
        median = self.statistic is Statistic.MEDIAN
        if not keys:
            value = frame[self.measure].median() if median else frame[self.measure].mean()
            return pd.DataFrame({self.measure: [value]}), 1, len(frame)
        grouped = frame.groupby(keys, dropna=False, sort=False)[self.measure]
        out = (grouped.median() if median else grouped.mean()).reset_index()
        return out, len(out), len(frame)


#: The working column holding each record's category text during the one-hot.
CATEGORY_TEXT_COLUMN = "__category_text"


@dataclass(frozen=True)
class CategoricalRewrite:
    """A text measure rewritten into numbers (``categorical.categorical_step``
    decides every field; ``docs/claude/categorical-measures.md``).

    The text is ``table.category_text`` (a missing value is the level
    ``MISSING_LEVEL``). ``record_filters`` are the spec's filters ON the
    measure, ``(include, exclude)`` by category text, applied to the records
    first. Then, by ``mode``:

    * ``VALUE`` — each value becomes its index in ``levels`` (NaN when absent);
    * ``COUNT`` / ``PROPORTION`` — one row per (record, category of its
      field): ``pairs`` are the (field..., category) rows the plot holds, in
      order, over ``(*fields, level_name)``; the measure is 1 (x ``scale``)
      where the record IS that category, else 0. With one chosen ``level``
      the category column is dropped (a constant factor is no factor).
    """

    measure: str
    mode: CategoricalStat
    levels: tuple[str, ...]
    #: ``"count"`` / ``"%"`` / ``"categories"``: what the figure's numbers are.
    words: str
    record_filters: tuple[tuple[tuple[str, ...] | None, tuple[str, ...] | None], ...] = ()
    level_name: str = "Level"
    fields: tuple[str, ...] = ()
    pairs: tuple[tuple[Any, ...], ...] = ()
    level: str | None = None
    scale: float = 1.0

    def record_mask(self, text: pd.Series) -> pd.Series:
        """Which records the filters on the measure keep."""
        keep = pd.Series(True, index=text.index)
        for include, exclude in self.record_filters:
            if include is not None:
                keep &= text.isin(list(include))
            if exclude is not None:
                keep &= ~text.isin(list(exclude))
        return keep

    def observed(self) -> pd.DataFrame:
        """``pairs`` as the frame the one-hot merges on."""
        return pd.DataFrame(list(self.pairs), columns=[*self.fields, self.level_name])

    def apply(self, frame: pd.DataFrame) -> pd.DataFrame:
        text = category_text(frame[self.measure])
        keep = self.record_mask(text)
        if not bool(keep.all()):
            frame, text = frame[keep], text[keep]
        if self.mode is CategoricalStat.VALUE:
            codes = pd.Categorical(text, categories=list(self.levels)).codes.astype("float64")
            codes[codes < 0] = np.nan
            out = frame.copy()
            out[self.measure] = codes
            return out
        work = frame.assign(**{CATEGORY_TEXT_COLUMN: text.to_numpy()})
        observed = self.observed()
        if self.fields:
            out = work.merge(observed, on=list(self.fields), how="inner", sort=False)
        else:
            out = work.merge(observed, how="cross")
        out[self.measure] = (
            out[CATEGORY_TEXT_COLUMN] == out[self.level_name]
        ).astype("float64") * self.scale
        dropped = [CATEGORY_TEXT_COLUMN, *([self.level_name] if self.level is not None else [])]
        return out.drop(columns=dropped).reset_index(drop=True)


@dataclass(frozen=True)
class CollapseLevels:
    """One pass of the collapse chain (``chain.collapse_passes`` plans them):
    the measures reduced by ``how`` over ``dropped``, grouping on every other
    of ``factors`` still in the frame, plus a 1-D measure's ``index_column``
    and any ``carry`` column (one the caller composed from kept factors, so
    it is constant within each group). Missing levels are groups like any
    other (``dropna=False``) and groups keep their order of appearance.

    ``how`` is ``"mean"`` (the nested, unweighted mean), or for a text
    measure ``"sum"`` (a count) / ``"mean"`` of 0/100 indicators (a %).
    A pass none of whose keys is in the frame does nothing.
    """

    dropped: tuple[str, ...]
    factors: tuple[str, ...]
    measures: tuple[str, ...]
    how: str = "mean"
    index_column: str | None = None
    carry: tuple[str, ...] = ()

    def keep_among(self, columns: "list[str] | pd.Index") -> list[str]:
        """The columns a pass groups on, among ``columns``."""
        present = list(columns)
        keep = [name for name in self.factors if name in present and name not in self.dropped]
        if self.index_column and self.index_column in present and self.index_column not in keep:
            keep.append(self.index_column)
        keep.extend(c for c in self.carry if c in present and c not in keep)
        return keep

    def applies_to(self, columns: "list[str] | pd.Index") -> bool:
        """Whether any key of this pass is in ``columns`` (else a no-op)."""
        present = list(columns)
        return any(key in present for key in self.dropped)

    def apply(self, frame: pd.DataFrame) -> pd.DataFrame:
        if not self.applies_to(frame.columns):
            return frame
        keep = self.keep_among(frame.columns)
        measures = [m for m in self.measures if m in frame.columns]
        if keep:
            return (
                frame.groupby(keep, dropna=False, sort=False)[measures]
                .agg(self.how)
                .reset_index()
            )
        # Everything collapses to a single value.
        return frame[measures].agg(self.how).to_frame().T


#: A step type. ``compare.ComparisonPlan`` is the comparison step: planned per
#: figure (pairing depends on the figure's sample), applied by
#: ``compare.compare_sample`` and printed by ``reduction_code.emit_compare``.
ReductionStep = Union[
    LocationMask,
    ValueFilter,
    CombineLevels,
    CollapseCells,
    CategoricalRewrite,
    ComparisonPlan,
    CollapseLevels,
]


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
