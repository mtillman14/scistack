"""
``LongTable`` — a long-format DataFrame plus the column roles a plot needs.

Every source (CSV, DataFrame, scidb) hands the rest of the package one of
these. It answers three questions the raw DataFrame cannot: which columns are
*factors* (categorical things you can slice by), which are *measures* (the
numbers being plotted), and — critically — **what order each factor's levels
go in**.

That last one is not cosmetic. Schema keys like ``"01"`` are strings by project
rule, and pandas' default lexicographic ordering renders them as
``"1", "10", "2"`` on a categorical axis: visibly wrong, and wrong in a way
that looks like a data problem rather than a plotting problem. Sources that
know better (scidb knows its declared ``schema_key_types``) supply
``level_order`` explicitly; everything else falls back to the natural-sort
heuristic below.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable

import pandas as pd

from .shape import Shape, classify_column

_NUM_CHUNK = re.compile(r"(\d+)")

#: Prefix marking a factor as a **code-version** axis (``Code:bandpass_filter``)
#: rather than an experimental condition.
#:
#: Defined here, in the rendering layer, even though only the scidb-backed source
#: currently produces such factors: the distinction is about how a factor should
#: be *presented and defaulted* — a code axis is usually pinned to current, a
#: condition is usually faceted — and that is this layer's concern. Sources
#: conform to the convention rather than each inventing their own
#: (``scistackplotdb.VERSION_FACTOR_PREFIX`` is this constant).
CODE_FACTOR_PREFIX = "Code:"

#: Prefix marking a factor as a **run-options** axis (``Run:loadGaitRiteOneFile``)
#: — which ``for_each`` flags (``distribute``/``as_table``) the named function
#: ran under to produce these rows. The third kind of variant axis after branch
#: params and code versions, and presented like a code axis: it belongs to the
#: function, it is usually pinned to current, and it is answered by the
#: source's latest flag. Levels are scidb's ``run_options_label`` strings
#: (``distribute=true``); no ordinal, because the options have no version order.
RUN_FACTOR_PREFIX = "Run:"

#: Level given to rows a joined factor variable has no value for — a subject
#: who is not in the demographics sheet, say.
#:
#: Dropping those rows instead would remove data from the figure to answer a
#: question about grouping, and silently: the figure would simply hold fewer
#: subjects than the database does, with nothing on screen saying so. An
#: explicit level is visible, takes a role like any other level, and can be
#: filtered out deliberately in one click. Named here because naming is
#: presentation, even though only the scidb-backed source produces it.
MISSING_LEVEL = "(missing)"


def natural_sort_key(value: Any) -> tuple:
    """
    Sort key that orders embedded digit runs numerically.

    ``"1", "2", "10"`` and ``"s01", "s02", "s10"`` both come out right, while
    non-numeric values still sort stably. Digit runs compare as (0, int) and
    text as (1, str) so the two never compare against each other.
    """
    text = "" if value is None else str(value)
    parts: list[tuple[int, Any]] = []
    for chunk in _NUM_CHUNK.split(text):
        if not chunk:
            continue
        if chunk.isdigit():
            parts.append((0, int(chunk)))
        else:
            parts.append((1, chunk))
    return tuple(parts)


@dataclass
class FactorInfo:
    name: str
    levels: list[Any]
    #: True when this factor came from pipeline branch params rather than the
    #: dataset schema. The GUI marks these; pooling them means assigning
    #: 'aggregate' or 'free' deliberately, which ``roles.validate`` allows only
    #: when the spec says so rather than by defaulting.
    is_variant: bool = False
    #: True when this factor's levels are the measure's own FIELDS (the keys of
    #: a dict/struct variable, melted into long format) rather than an
    #: experimental condition. Defaults to one subplot per level, because the
    #: fields of a struct are parallel quantities — 13 muscles overplotted on
    #: one axis is not a figure anyone wanted.
    is_field: bool = False
    label: str | None = None
    #: Where a variant factor came from, when the source can say:
    #: ``{"kind": "code"|"param", "function": ..., "param": ...}``.
    #:
    #: Carried so a consumer never has to parse the column name. ``Code:bandpass``
    #: and ``bandpass.low_hz`` are scidb's namespacing conventions, and a GUI (or
    #: any other caller) reconstructing the producing function by splitting on
    #: ``":"`` and ``"."`` would be re-implementing them one layer away from
    #: where they are defined — the first place to break when they change.
    origin: dict[str, Any] | None = None

    @property
    def display(self) -> str:
        return self.label or self.name


@dataclass
class MeasureInfo:
    name: str
    shape: Shape
    label: str | None = None
    #: For 1-D measures whose arrays have already been exploded into rows.
    exploded: bool = False

    @property
    def display(self) -> str:
        return self.label or self.name


@dataclass
class LongTable:
    """A long-format table with declared column roles."""

    frame: pd.DataFrame
    factors: list[FactorInfo] = field(default_factory=list)
    measures: list[MeasureInfo] = field(default_factory=list)
    #: Within-observation axis for 1-D data (time, frame, percent). Present as
    #: a column only after the measure has been exploded.
    index_column: str | None = None
    name: str | None = None
    #: A variant selection the SOURCE recommends, or None if it has no opinion.
    #: Sources that can tell which rows are current say so here, and
    #: ``default_spec`` opens on it as the first named variant; the user is free
    #: to rename it, narrow it, or delete the row.
    #:
    #: The point is to keep "which rows are current" with the layer that knows
    #: — a scidb variable whose function was edited holds records from both the
    #: old and the new code, and only scidb can say which is which. A CSV has
    #: no such notion and leaves this None.
    default_pin: dict[str, Any] | None = None
    #: The dataset's schema keys that this table carries, **outermost first**
    #: (``["subject", "session", "trial"]``). Empty for sources with no
    #: hierarchy, which is the honest answer for a CSV.
    #:
    #: Two things need it and neither can derive it from the frame. Nesting:
    #: ``trial`` is meaningless without the ``subject`` it belongs to, so
    #: iterating it iterates that subject too (``roles.iterate_ancestors``).
    #: And ordering: a fan-out has to run subject-major so that stepping past
    #: the last trial of subject 1 rolls over to subject 2's first trial.
    schema_levels: list[str] = field(default_factory=list)
    #: Name of the per-row "my whole code chain is the newest at my own schema
    #: location" flag, when the source attaches one (scidb's ``CodeIsLatest``).
    #:
    #: Needed by name because ``"latest"`` in a variant selection resolves
    #: through it, and it is emphatically **not** the same thing as "the highest
    #: version ordinal": it is per schema location, so a subject never re-run
    #: under the newest code still contributes its own newest record instead of
    #: silently leaving the figure.
    latest_column: str | None = None
    #: How the per-sample reductions a plot needs should be performed — y
    #: extents, exploding a 1-D measure, striding for transport, averaging
    #: matrices. None means the pandas reference (``reducer.PandasReducer``).
    #:
    #: Set by the source that built the table, like ``default_pin`` and
    #: ``latest_column`` above: it is a fact about where the data lives, not
    #: about the data. A scidb-backed table can hand these operations to DuckDB,
    #: which performs them in one columnar pass; an in-memory table cannot, and
    #: says so by leaving this None. Typed loosely to keep ``table`` below
    #: ``reducer`` in the import graph; see ``reducer.Reducer`` for the contract.
    reducer: Any = None

    # ---- lookups ---------------------------------------------------------

    @property
    def factor_names(self) -> list[str]:
        return [f.name for f in self.factors]

    @property
    def measure_names(self) -> list[str]:
        return [m.name for m in self.measures]

    def factor(self, name: str) -> FactorInfo:
        for f in self.factors:
            if f.name == name:
                return f
        raise KeyError(f"No factor named {name!r}. Factors: {self.factor_names}")

    def measure(self, name: str) -> MeasureInfo:
        for m in self.measures:
            if m.name == name:
                return m
        raise KeyError(f"No measure named {name!r}. Measures: {self.measure_names}")

    def has_factor(self, name: str) -> bool:
        return any(f.name == name for f in self.factors)

    def is_schema_key(self, name: str) -> bool:
        """Whether this factor is a dataset schema key rather than a variant,
        a struct field, or anything else a source synthesized."""
        return name in self.schema_levels

    def shape_of(self, measure: str) -> Shape:
        return self.measure(measure).shape

    @property
    def variant_factors(self) -> list[FactorInfo]:
        return [f for f in self.factors if f.is_variant]

    @property
    def field_factors(self) -> list[FactorInfo]:
        return [f for f in self.factors if f.is_field]

    # ---- construction ----------------------------------------------------

    @classmethod
    def from_frame(
        cls,
        frame: pd.DataFrame,
        *,
        factors: Iterable[str] | None = None,
        measures: Iterable[str] | None = None,
        level_order: dict[str, list[Any]] | None = None,
        variant_factors: Iterable[str] = (),
        field_factors: Iterable[str] = (),
        index_column: str | None = None,
        name: str | None = None,
        default_pin: dict[str, Any] | None = None,
        latest_column: str | None = None,
        factor_origins: dict[str, dict] | None = None,
        schema_levels: Iterable[str] = (),
        measure_labels: dict[str, str] | None = None,
    ) -> "LongTable":
        """
        Build a LongTable, inferring column roles when they aren't given.

        Inference mirrors the R proof of concept's ``getFactorColNames``, but
        on shape rather than on R's ``is.factor``: a column that classifies as
        SCALAR or SERIES_1D is a measure, anything else is a factor. Callers
        that know better (every scidb-backed caller does) should pass explicit
        lists — inference is for the standalone CSV path.
        """
        level_order = dict(level_order or {})
        variant_set = set(variant_factors)
        field_set = set(field_factors)

        if factors is None or measures is None:
            inferred_measures: list[str] = []
            inferred_factors: list[str] = []
            for column in frame.columns:
                if column == index_column:
                    continue
                shape = classify_column(frame[column])
                if shape in (Shape.SCALAR, Shape.SERIES_1D, Shape.MATRIX_2D):
                    inferred_measures.append(column)
                else:
                    inferred_factors.append(column)
            factors = list(factors) if factors is not None else inferred_factors
            measures = list(measures) if measures is not None else inferred_measures

        factor_infos = []
        for column in factors:
            if column in level_order:
                levels = list(level_order[column])
            else:
                levels = sorted(
                    frame[column].dropna().unique().tolist(), key=natural_sort_key
                )
            factor_infos.append(
                FactorInfo(
                    name=column,
                    levels=levels,
                    is_variant=column in variant_set,
                    is_field=column in field_set,
                    origin=(factor_origins or {}).get(column),
                )
            )

        measure_infos = [
            MeasureInfo(
                name=column,
                shape=classify_column(frame[column]),
                # A stacked table's value column is named after the primary
                # variable but holds several; the label is how the axis says so
                # without the column name lying about what is in it.
                label=(measure_labels or {}).get(column),
            )
            for column in measures
        ]

        return cls(
            frame=frame,
            factors=factor_infos,
            measures=measure_infos,
            index_column=index_column,
            name=name,
            # A pin naming a column that isn't here would filter every row away
            # on the first render — drop it rather than produce an empty figure.
            default_pin={
                key: value
                for key, value in (default_pin or {}).items()
                if key in frame.columns
            }
            or None,
            latest_column=latest_column if latest_column in frame.columns else None,
            # Only keys this table actually carries: a variable saved at subject
            # level has no `trial` column, and an ancestor list naming one would
            # promote a factor that cannot be grouped by.
            schema_levels=[key for key in schema_levels if key in frame.columns],
        )

    def describe(self) -> dict:
        """JSON-serializable summary — what the GUI needs to build its controls."""
        return {
            "name": self.name,
            "row_count": int(len(self.frame)),
            "index_column": self.index_column,
            "factors": [
                {
                    "name": f.name,
                    "display": f.display,
                    "levels": [_jsonable(v) for v in f.levels],
                    "level_count": len(f.levels),
                    "is_variant": f.is_variant,
                    "is_field": f.is_field,
                    # A dataset schema key rather than a variant, a struct
                    # field, or anything else a source synthesized. Reported
                    # rather than left to the consumer to work out by
                    # intersecting two lists — which is policy, and policy
                    # lives here (CLAUDE.md NOTE 3).
                    "is_schema_key": self.is_schema_key(f.name),
                    "origin": f.origin,
                }
                for f in self.factors
            ],
            "measures": [
                {
                    "name": m.name,
                    "display": m.display,
                    "shape": str(m.shape),
                    "exploded": m.exploded,
                    "plottable": m.shape
                    in (Shape.SCALAR, Shape.SERIES_1D, Shape.MATRIX_2D),
                }
                for m in self.measures
            ],
        }


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)
