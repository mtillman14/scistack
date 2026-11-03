"""The wire shape of a plot spec and a stats spec, as pydantic models.

``scistackplot.PlotSpec.to_dict()`` and ``scistackstats.StatsSpec.to_dict()``
are what travels between the backend and the webview, and what is stored. That
dict is not the dataclass: enums are text, nulls are dropped, nested parts are
their own ``to_dict``s, and ``FactorVariable.variant`` is an object rather
than the in-memory tuple of pairs. These models declare that dict so
:mod:`scistack_gui.api.wire_schema` can put it in the generated TypeScript
(P5 stage 5, design review G8; user decision 2026-11-02: pydantic wire
models).

Ownership (.claude/plan-p5-stage5-spec-wire-models.md):

* ``to_dict`` / ``from_dict`` stay the one producer and reader of the dict.
  These models describe it. ``tests/test_spec_wire.py`` fails when a spec
  dataclass gains, loses or renames a field the model does not match, and
  when ``to_dict``'s output does not validate here with ``extra="forbid"``.
* Defaults are the dataclasses'. An optional field here defaults to
  "absent", which ``from_dict`` reads as the dataclass default, so no default
  is written twice.
* Every vocabulary comes from its owner: the enum classes for the plot's
  choices, the scistackstats tuples (``FAMILIES``, ``CELLS`` ...) for
  the table's. Nothing here spells a choice.
* A field is required only where ``from_dict`` requires it, and is typed for
  what ``from_dict`` ACCEPTS where that is wider than what ``to_dict`` emits
  (a histogram's ``exact_values`` arrives as the typed text ``"0, -1"``).

Each model's ``title`` is the dataclass name, which names the TypeScript
interface (``PlotSpec``, ``StatsSpec``, ``CustomPlot`` ...). The Python
names carry a ``Wire`` suffix so they never shadow the dataclasses.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from scistackplot import (
    TEXT_TARGETS,
    BinMode,
    CategoricalStat,
    CompareMode,
    ErrorBand,
    FitBy,
    HistMultiple,
    HistStat,
    MatchOp,
    PlotKind,
    Role,
    Statistic,
    StatsCorner,
)
from scistackplot.reduce.panels import Y_TITLES
from scistackstats.spec import (
    BAR_LABELS,
    BAR_PAIRS,
    CELLS,
    DIFFERENCE_SPREADS,
    FACTOR_KINDS,
    FAMILIES,
    FAMILY_SCOPES,
    GUIDE_MODES,
    MISSING_POLICIES,
    TAILS,
    VARIANT_MODES,
)


def _optional(**extra: Any) -> Any:
    """An optional field: absent on the wire means the dataclass default.

    The schema says what ``from_dict`` ACCEPTS, which is also what the
    webview holds: a part the webview adds (a new analysis) carries only the
    keys it set until the backend fills the rest from the dataclass. So no
    optional field is ``required``, even where ``to_dict`` always writes it.

    The ``None`` default is pydantic's "not given", never a value, so it is
    kept out of the schema: next to a ``$ref`` it made
    json-schema-to-typescript emit a numbered copy of the referenced type
    (``Aggregation1``, ``Statistic2``) instead of using it. ``extra`` is
    merged into the field's schema (a choice list)."""

    def schema(field_schema: dict[str, Any]) -> None:
        field_schema.pop("default", None)
        field_schema.update(extra)

    return Field(default=None, json_schema_extra=schema)


def _absent() -> Any:
    """An optional field with no further schema."""
    return _optional()


def _one_of(values: tuple[str, ...]) -> Any:
    """An optional text field limited to ``values`` (the owner's tuple). The
    choice reaches the schema (a TypeScript union); ``from_dict`` enforces it."""
    return _optional(enum=list(values))


def _values_of(values: tuple[str, ...]) -> Any:
    """An optional ``{name: choice}`` object whose values are limited to ``values``."""
    return _optional(additionalProperties={"type": "string", "enum": list(values)})


class _Wire(BaseModel):
    """Strict like ``from_dict``: an unknown key is an error, not ignored."""

    model_config = ConfigDict(extra="forbid")


# --- PlotSpec parts (scistackplot.model.spec and scistackplot.reduce) ---------


class MatcherWire(_Wire):
    """``scistackplot.Matcher``: one facet-grid row or column rule."""

    model_config = ConfigDict(title="Matcher")
    op: MatchOp = _absent()
    value: str = _absent()
    label: str | None = _absent()


class FilterWire(_Wire):
    """``scistackplot.Filter``: one row filter."""

    model_config = ConfigDict(title="Filter")
    column: str
    include: list[Any] | None = _absent()
    exclude: list[Any] | None = _absent()
    minimum: float | None = _absent()
    maximum: float | None = _absent()


class LocationFilterWire(_Wire):
    """``scistackplot.LocationFilter``: the schema location picker's selection."""

    model_config = ConfigDict(title="LocationFilter")
    keys: list[str] = _absent()
    #: Prefixes, each a list of ``[key, value]`` pairs, outermost first.
    include: list[list[list[str]]] = _absent()
    exclude_levels: dict[str, list[str]] = _absent()


class LevelGroupWire(_Wire):
    """``scistackplot.LevelGroup``: a combine."""

    model_config = ConfigDict(title="LevelGroup")
    name: str
    source: str
    mapping: dict[str, str] = _absent()
    unmatched: str | None = _absent()
    active: bool = _absent()


class FactorVariableWire(_Wire):
    """``scistackplot.FactorVariable``: a variable (or one column) joined in
    as a factor. ``variant`` is the selection as an object."""

    model_config = ConfigDict(title="FactorVariable")
    variable: str
    column: str | None = _absent()
    variant: dict[str, Any] | None = _absent()


class AggregationWire(_Wire):
    """``scistackplot.Aggregation``: centre and spread over the sample."""

    model_config = ConfigDict(title="Aggregation")
    statistic: Statistic = _absent()
    error: ErrorBand = _absent()
    pooled: bool = _absent()


class HistogramOptionsWire(_Wire):
    """``scistackplot.HistogramOptions``. ``bin_width``: a number, or duration
    text on a datetime axis; the range ends: a number or ISO date text."""

    model_config = ConfigDict(title="HistogramOptions")
    bins: BinMode = _absent()
    bin_count: int | None = _absent()
    bin_width: float | str | None = _absent()
    range_min: float | str | None = _absent()
    range_max: float | str | None = _absent()
    stat: HistStat = _absent()
    multiple: HistMultiple = _absent()
    #: A list back from the backend; the GUI may send the typed text.
    exact_values: list[float] | str | None = _absent()
    show_nan: bool = _absent()


class CategoricalOptionsWire(_Wire):
    """``scistackplot.CategoricalOptions``: how a text measure is drawn."""

    model_config = ConfigDict(title="CategoricalOptions")
    stat: CategoricalStat = _absent()
    level: str | None = _absent()


class BarOptionsWire(_Wire):
    """``scistackplot.BarOptions``."""

    model_config = ConfigDict(title="BarOptions")
    stack: bool = _absent()


class RegressionOptionsWire(_Wire):
    """``scistackplot.RegressionOptions``."""

    model_config = ConfigDict(title="RegressionOptions")
    fit_line: bool = _absent()
    show_stats: bool = _absent()
    ci_band: bool = _absent()
    fit_by: FitBy = _absent()
    stats_corner: StatsCorner = _absent()


class KdeOptionsWire(_Wire):
    """``scistackplot.KdeOptions``."""

    model_config = ConfigDict(title="KdeOptions")
    bandwidth_adjust: float | None = _absent()
    cut: float | None = _absent()
    fill: bool = _absent()


class FacetOptionsWire(_Wire):
    """``scistackplot.FacetOptions``: the facet grid."""

    model_config = ConfigDict(title="FacetOptions")
    n_rows: int | None = _absent()
    n_cols: int | None = _absent()
    rows: list[MatcherWire] = _absent()
    cols: list[MatcherWire] = _absent()
    share_x: bool = _absent()


class YAxisWire(_Wire):
    """``scistackplot.YAxis``. A limit is a number, or ISO date text on a
    datetime axis."""

    model_config = ConfigDict(title="YAxis")
    scope: list[str] = _absent()
    minimum: float | str | None = _absent()
    maximum: float | str | None = _absent()


class VariantSetWire(_Wire):
    """``scistackplot.VariantSet``: one row of the Variants section."""

    model_config = ConfigDict(title="VariantSet")
    name: str | None = _absent()
    selection: dict[str, Any] = _absent()
    variable: str | None = _absent()
    frozen: dict[str, list[str]] = _absent()
    follow_latest: bool = _absent()


class CustomPlotWire(_Wire):
    """``scistackplot.CustomPlot``: the ``plot_`` function drawing a custom figure."""

    model_config = ConfigDict(title="CustomPlot")
    function: str = _absent()
    node: str = _absent()
    as_table: bool | list[str] = _absent()
    inputs: dict[str, str] = _absent()


class AliasWire(_Wire):
    """``scistackplot.Alias``: what a thing and its levels read as."""

    model_config = ConfigDict(title="Alias")
    name: str | None = _absent()
    levels: dict[str, str] = _absent()


class TextSizesWire(_Wire):
    """``scistackplot.TextSizes``: point sizes per text element."""

    model_config = ConfigDict(title="TextSizes")
    base: float | None = _absent()
    target: str = _one_of(TEXT_TARGETS)
    title: float | None = _absent()
    x_label: float | None = _absent()
    y_label: float | None = _absent()
    x_ticks: float | None = _absent()
    y_ticks: float | None = _absent()
    groups: float | None = _absent()
    legend: float | None = _absent()
    legend_title: float | None = _absent()
    differences: float | None = _absent()
    ratios: dict[str, float] = _absent()
    floors: dict[str, float] = _absent()
    ceilings: dict[str, float] = _absent()


class StyleOptionsWire(_Wire):
    """``scistackplot.StyleOptions``. Width and height are inches."""

    model_config = ConfigDict(title="StyleOptions")
    palette: str | None = _absent()
    width: float = _absent()
    height: float = _absent()
    text: TextSizesWire = _absent()
    log_x: bool = _absent()
    log_y: bool = _absent()
    title: str | None = _absent()
    x_label: str | None = _absent()
    y_label: str | None = _absent()
    marker_size: float = _absent()
    sample_weight: float = _absent()
    line_weight: float = _absent()
    alpha: float = _absent()
    mark_color: str | None = _absent()
    hide_legend_ticks: bool = _absent()
    tick_rotation: int | None = _absent()
    tick_every: int | None = _absent()
    y_titles: str = _one_of(Y_TITLES)
    not_plotted_note: bool = _absent()


class PanelOverrideWire(_Wire):
    """``scistackplot.PanelOverride``: one faceted panel's own y limits and title."""

    model_config = ConfigDict(title="PanelOverride")
    match: dict[str, str] = _absent()
    y_minimum: float | None = _absent()
    y_maximum: float | None = _absent()
    y_label: str | None = _absent()
    y_label_hidden: bool | None = _absent()


class DifferenceBarWire(_Wire):
    """``scistackplot.DifferenceBar``: a bar joining two ticks of one panel.
    ``origin`` is written only when not ``"manual"``."""

    model_config = ConfigDict(title="DifferenceBar")
    match: dict[str, str] = _absent()
    a: dict[str, str] = _absent()
    b: dict[str, str] = _absent()
    label: str | None = _absent()
    origin: str | None = _absent()


class ComparisonWire(_Wire):
    """``scistackplot.Comparison``: compare to a reference level."""

    model_config = ConfigDict(title="Comparison")
    layer: str
    level: str
    mode: CompareMode = _absent()
    active: bool = _absent()


class PlotSpecWire(_Wire):
    """``scistackplot.PlotSpec.to_dict()``: a complete plot description."""

    model_config = ConfigDict(title="PlotSpec")
    measures: list[str]
    x_measure: str | None = _absent()
    roles: dict[str, Role] = _absent()
    groups: list[str] = _absent()
    color: str | None = _absent()
    kind: PlotKind = _absent()
    custom: CustomPlotWire | None = _absent()
    aggregate: AggregationWire = _absent()
    cell_statistic: Statistic = _absent()
    index_column: str | None = _absent()
    show_sample: list[str] = _absent()
    join_sample: bool | None = _absent()
    sample_color: str | None = _absent()
    sample_in_legend: bool = _absent()
    show_missing_levels: bool = _absent()
    time_zone: str = _absent()
    histogram: HistogramOptionsWire | None = _absent()
    regression: RegressionOptionsWire | None = _absent()
    kde: KdeOptionsWire | None = _absent()
    categorical: CategoricalOptionsWire | None = _absent()
    bar: BarOptionsWire | None = _absent()
    facet: FacetOptionsWire = _absent()
    y_axis: YAxisWire = _absent()
    style: StyleOptionsWire | None = _absent()
    aliases: dict[str, AliasWire] = _absent()
    colors: dict[str, dict[str, str]] = _absent()
    panel_overrides: list[PanelOverrideWire] = _absent()
    difference_bars: list[DifferenceBarWire] = _absent()
    comparison: ComparisonWire | None = _absent()
    filters: list[FilterWire] = _absent()
    location_filter: LocationFilterWire = _absent()
    factor_variables: list[FactorVariableWire] = _absent()
    level_groups: list[LevelGroupWire] = _absent()
    variant_sets: list[VariantSetWire] = _absent()


# --- StatsSpec parts (scistackstats.spec) ------------------------------------


class DVWire(_Wire):
    """``scistackstats.DV``: one dependent variable, or chosen columns of one."""

    model_config = ConfigDict(title="DV")
    variable: str
    columns: list[str] | None = _absent()


class DescriptiveSpecWire(_Wire):
    """``scistackstats.DescriptiveSpec``: what the Descriptives tab shows."""

    model_config = ConfigDict(title="DescriptiveSpec")
    cell: str = _one_of(CELLS)
    statistics: list[str] = _absent()


class FormatSpecWire(_Wire):
    """``scistackstats.FormatSpec``: how numbers read."""

    model_config = ConfigDict(title="FormatSpec")
    decimals: int | None = _absent()
    p_decimals: int = _absent()
    effect_decimals: int = _absent()
    stars: bool = _absent()


class AnalysisWire(_Wire):
    """``scistackstats.Analysis``: one analysis on the Comparisons tab."""

    model_config = ConfigDict(title="Analysis")
    id: str = _absent()
    family: str = _one_of(FAMILIES)
    omnibus: bool = _absent()
    pairwise: bool = _absent()
    omnibus_test: str | None = _absent()
    pairwise_test: str | None = _absent()
    equal_var: bool = _absent()
    alpha: float = _absent()
    ci_level: float = _absent()
    tails: str = _one_of(TAILS)
    correction: str = _absent()
    family_scope: str = _one_of(FAMILY_SCOPES)
    effect_size: str | None = _absent()
    difference_spread: str = _one_of(DIFFERENCE_SPREADS)
    overrides: dict[str, str] = _values_of(FACTOR_KINDS)
    missing: str = _one_of(MISSING_POLICIES)
    #: The mixed model's right side, lme4 spelling. Absent = the design's default.
    formula: str | None = _absent()
    covariates: list[FactorVariableWire] | None = _absent()


class GuideStateWire(_Wire):
    """``scistackstats.GuideState``: the view and the wizard's progress."""

    model_config = ConfigDict(title="GuideState")
    mode: str = _one_of(GUIDE_MODES)
    #: Absent: the first step. Step ids are text, not narrowed to the
    #: table's GUIDE_STEPS: Plot Studio's step-by-step view keeps this same
    #: state with its own steps (PLOT_GUIDE_STEPS, "figure"). A table's
    #: from_dict checks its own.
    step: str = _absent()
    done: list[str] = _absent()


class OverlaySpecWire(_Wire):
    """``scistackstats.spec.OverlaySpec``: brackets drawn on the linked figure."""

    model_config = ConfigDict(title="OverlaySpec")
    show: bool = _absent()
    analysis: str | None = _absent()
    pairs: str = _one_of(BAR_PAIRS)
    label: str = _one_of(BAR_LABELS)
    hidden: list[str] = _absent()


class StatsSpecWire(_Wire):
    """``scistackstats.StatsSpec.to_dict()``: a statistics table. ``design``
    is the plot spec it analyses."""

    model_config = ConfigDict(title="StatsSpec")
    format: int
    design: PlotSpecWire
    dvs: list[DVWire] = _absent()
    linked_plot: str | None = _absent()
    descriptives: DescriptiveSpecWire = _absent()
    comparisons: list[AnalysisWire] = _absent()
    variant_mode: str = _one_of(VARIANT_MODES)
    formatting: FormatSpecWire = _absent()
    overlay: OverlaySpecWire = _absent()
    guide: GuideStateWire = _absent()


#: The specs the frontend gets types for. Their parts come along as ``$defs``.
SPEC_MODELS: tuple[type[BaseModel], ...] = (PlotSpecWire, StatsSpecWire)
