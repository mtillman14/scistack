"""The spec wire models match ``PlotSpec.to_dict`` / ``StatsSpec.to_dict``.

``scistack_gui.api.spec_wire`` declares the dict the specs travel as, and the
frontend's spec types are generated from it (P5 stage 5, G8). ``to_dict`` /
``from_dict`` stay the producer and reader, so these tests are what keep the
two from drifting:

* every spec dataclass and its wire model name the same fields;
* a spec with EVERY field off its default, and the default spec, serialise
  to a dict that validates with ``extra="forbid"`` and dumps back unchanged;
* the maximal specs really exercise every wire field (nested included);
* the dump reads back to the same spec through ``from_dict``.

A failure here means a spec field changed without its wire model (or the
other way round). Fix the model in ``spec_wire.py``, then regenerate the
frontend types: ``python scistack-gui/tools/gen_ts_types.py``.
"""

from __future__ import annotations

import dataclasses
import json
import types
import typing
from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from scistack_gui.api import spec_wire as w
from scistack_gui.api.wire_schema import api_json_schema
from scistackplot import (
    Aggregation,
    Alias,
    BarOptions,
    BinMode,
    CategoricalOptions,
    CategoricalStat,
    CompareMode,
    Comparison,
    CustomPlot,
    DifferenceBar,
    ErrorBand,
    FacetOptions,
    FactorVariable,
    Filter,
    FitBy,
    HistMultiple,
    HistogramOptions,
    HistStat,
    KdeOptions,
    LevelGroup,
    LocationFilter,
    Matcher,
    MatchOp,
    PanelOverride,
    PlotKind,
    PlotSpec,
    RegressionOptions,
    Role,
    Statistic,
    StatsCorner,
    StyleOptions,
    TextSizes,
    VariantSet,
    YAxis,
)
from scistackstats import DV, Analysis, DescriptiveSpec, FormatSpec, GuideState, StatsSpec
from scistackstats.spec import CELLS, FAMILIES, OverlaySpec

PAIRS: list[tuple[type, type[BaseModel]]] = [
    (Matcher, w.MatcherWire),
    (Filter, w.FilterWire),
    (LocationFilter, w.LocationFilterWire),
    (LevelGroup, w.LevelGroupWire),
    (FactorVariable, w.FactorVariableWire),
    (Aggregation, w.AggregationWire),
    (HistogramOptions, w.HistogramOptionsWire),
    (CategoricalOptions, w.CategoricalOptionsWire),
    (BarOptions, w.BarOptionsWire),
    (RegressionOptions, w.RegressionOptionsWire),
    (KdeOptions, w.KdeOptionsWire),
    (FacetOptions, w.FacetOptionsWire),
    (YAxis, w.YAxisWire),
    (VariantSet, w.VariantSetWire),
    (CustomPlot, w.CustomPlotWire),
    (Alias, w.AliasWire),
    (TextSizes, w.TextSizesWire),
    (StyleOptions, w.StyleOptionsWire),
    (PanelOverride, w.PanelOverrideWire),
    (DifferenceBar, w.DifferenceBarWire),
    (Comparison, w.ComparisonWire),
    (PlotSpec, w.PlotSpecWire),
    (DV, w.DVWire),
    (DescriptiveSpec, w.DescriptiveSpecWire),
    (FormatSpec, w.FormatSpecWire),
    (Analysis, w.AnalysisWire),
    (GuideState, w.GuideStateWire),
    (OverlaySpec, w.OverlaySpecWire),
    (StatsSpec, w.StatsSpecWire),
]

#: Keys a ``to_dict`` writes that are not dataclass fields.
WIRE_ONLY = {StatsSpec: {"format"}}


def _maximal_plot_spec() -> PlotSpec:
    """Every field off its default, so every wire key is written."""
    return PlotSpec(
        measures=["Force"],
        x_measure="Time",
        roles={
            "session": Role.GROUP,
            "subject": Role.COLLAPSE,
            "side": Role.FACET,
            "cond": Role.ITERATE,
        },
        groups=["session"],
        color="session",
        kind=PlotKind.BAR,
        custom=CustomPlot(function="plot_x", as_table=["a"], inputs={"a": "Force"}, node="n1"),
        aggregate=Aggregation(statistic=Statistic.MEDIAN, error=ErrorBand.SEM, pooled=True),
        cell_statistic=Statistic.MEDIAN,
        index_column="t",
        show_sample=["subject"],
        join_sample=False,
        sample_color="subject",
        sample_in_legend=False,
        show_missing_levels=False,
        time_zone="UTC",
        histogram=HistogramOptions(
            bins=BinMode.WIDTH,
            bin_count=5,
            bin_width="1h",
            range_min=0.0,
            range_max="2026-01-01",
            stat=HistStat.PERCENT,
            multiple=HistMultiple.STACK,
            exact_values=(0.0,),
            show_nan=False,
        ),
        regression=RegressionOptions(
            fit_line=False,
            show_stats=False,
            ci_band=True,
            fit_by=FitBy.POOLED,
            stats_corner=StatsCorner.LOWER_RIGHT,
        ),
        kde=KdeOptions(bandwidth_adjust=2.0, cut=1.0, fill=True),
        categorical=CategoricalOptions(stat=CategoricalStat.PROPORTION, level="fall"),
        bar=BarOptions(stack=True),
        facet=FacetOptions(
            n_rows=2,
            n_cols=3,
            rows=[Matcher(op=MatchOp.STARTS_WITH, value="L", label="Left")],
            cols=[Matcher(op=MatchOp.REGEX, value="^R", label="Right")],
            share_x=False,
        ),
        y_axis=YAxis(scope=["side"], minimum=0.0, maximum=10.0),
        style=StyleOptions(
            palette="deep",
            width=5.0,
            height=4.0,
            text=TextSizes(
                base=10.0,
                target="slide",
                title=12.0,
                x_label=11.0,
                y_label=11.0,
                x_ticks=9.0,
                y_ticks=9.0,
                groups=9.0,
                legend=9.0,
                legend_title=10.0,
                differences=9.0,
                ratios={"title": 1.2},
                floors={"title": 8.0},
                ceilings={"title": 20.0},
            ),
            log_x=True,
            log_y=True,
            title="T",
            x_label="X",
            y_label="Y",
            marker_size=20.0,
            sample_weight=2.0,
            line_weight=0.5,
            alpha=0.5,
            mark_color="#ff0000",
            hide_legend_ticks=True,
            tick_rotation=45,
            tick_every=2,
            y_titles="first_column",
            not_plotted_note=True,
        ),
        aliases={"session": Alias(name="Session", levels={"1": "Pre"})},
        colors={"session": {"1": "#000000"}},
        panel_overrides=[
            PanelOverride(
                match={"side": "L"},
                y_minimum=0.0,
                y_maximum=5.0,
                y_label="L",
                y_label_hidden=True,
            )
        ],
        difference_bars=[
            DifferenceBar(
                match={}, a={"session": "1"}, b={"session": "2"}, label="**", origin="test"
            )
        ],
        comparison=Comparison(layer="session", level="1", mode=CompareMode.PERCENT, active=False),
        filters=[Filter(column="session", include=["1"], exclude=["3"], minimum=0.0, maximum=1.0)],
        location_filter=LocationFilter(
            keys=["subject", "trial"],
            include=[[["subject", "01"]]],
            exclude_levels={"trial": ["3"]},
        ),
        factor_variables=[
            FactorVariable.from_dict(
                {"variable": "Demo", "column": "Sex", "variant": {"CodeIsLatest": True}}
            )
        ],
        level_groups=[
            LevelGroup(
                name="Phase", source="session", mapping={"1": "A"}, unmatched="B", active=False
            )
        ],
        variant_sets=[
            VariantSet(
                name="v1",
                selection={"Code:f": "latest"},
                variable="Force",
                frozen={"Code:f": ["v1"]},
                follow_latest=True,
            )
        ],
    )


def _maximal_stats_spec() -> StatsSpec:
    return StatsSpec(
        design=_maximal_plot_spec(),
        dvs=(DV("Force", ("a",)),),
        linked_plot="p1",
        descriptives=DescriptiveSpec(cell="median_iqr", statistics=("n", "mean")),
        comparisons=(
            Analysis(
                id="a1",
                family="parametric",
                omnibus=False,
                pairwise=True,
                omnibus_test="anova",
                pairwise_test="ttest",
                equal_var=True,
                alpha=0.01,
                ci_level=0.9,
                tails="greater",
                correction="bonferroni",
                family_scope="dv",
                effect_size="cohen_d",
                difference_spread="se",
                overrides={"session": "within"},
                missing="complete_cases",
                formula="session + (1 | subject)",
                covariates=(
                    FactorVariable.from_dict(
                        {"variable": "Demo", "column": "Age", "variant": {"CodeIsLatest": True}}
                    ),
                ),
            ),
        ),
        variant_mode="sweep",
        formatting=FormatSpec(decimals=2, p_decimals=4, effect_decimals=3, stars=True),
        overlay=OverlaySpec(show=True, analysis="a1", pairs="all", label="p", hidden=("x",)),
        guide=GuideState(mode="guided", step="unit", done=("measure",)),
    )


def _as_json(raw: dict) -> dict:
    """The dict as it crosses the wire (tuples become lists)."""
    return json.loads(json.dumps(raw))


def _wire_dump(model: type[BaseModel], raw: dict) -> dict:
    return model.model_validate(raw).model_dump(mode="json", exclude_unset=True)


@pytest.mark.parametrize("dataclass_type, model", PAIRS, ids=lambda p: getattr(p, "__name__", ""))
def test_wire_model_names_the_dataclass_fields(dataclass_type, model):
    fields = {f.name for f in dataclasses.fields(dataclass_type)}
    expected = fields | WIRE_ONLY.get(dataclass_type, set())
    assert set(model.model_fields) == expected
    assert model.model_config.get("title") == dataclass_type.__name__


@pytest.mark.parametrize(
    "spec, model",
    [
        (_maximal_plot_spec(), w.PlotSpecWire),
        (PlotSpec(measures=["Force"]), w.PlotSpecWire),
        (_maximal_stats_spec(), w.StatsSpecWire),
        (StatsSpec(design=PlotSpec(measures=["Force"])), w.StatsSpecWire),
    ],
    ids=["plot-maximal", "plot-default", "stats-maximal", "stats-default"],
)
def test_to_dict_validates_and_dumps_back_unchanged(spec, model):
    raw = _as_json(spec.to_dict())
    assert _wire_dump(model, raw) == raw
    assert type(spec).from_dict(_wire_dump(model, raw)) == spec


def _models_in(annotation: Any) -> list[type[BaseModel]]:
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return [annotation]
    return [m for arg in typing.get_args(annotation) for m in _models_in(arg)]


Check = typing.Callable[[type[BaseModel], dict, str], None]


def _walk(model: type[BaseModel], raw: dict, path: str, check: Check) -> None:
    """``check`` every model-shaped part of ``raw``, nested ones included."""
    check(model, raw, path)
    for name, info in model.model_fields.items():
        if name in raw:
            _descend(info.annotation, raw[name], f"{path}.{name}", check)


def _descend(annotation: Any, value: Any, path: str, check: Check) -> None:
    origin = typing.get_origin(annotation)
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        _walk(annotation, value, path, check)
    elif origin in (typing.Union, types.UnionType):
        for arg in typing.get_args(annotation):
            arg_origin = typing.get_origin(arg)
            if isinstance(value, dict) and _models_in(arg) and (
                arg_origin is dict or (isinstance(arg, type) and issubclass(arg, BaseModel))
            ):
                _descend(arg, value, path, check)
                return
            if isinstance(value, list) and arg_origin is list:
                _descend(arg, value, path, check)
                return
    elif origin is list and _models_in(annotation):
        assert value, f"{path} is empty in the spec under test"
        for item in value:
            _descend(typing.get_args(annotation)[0], item, f"{path}[]", check)
    elif origin is dict and _models_in(annotation):
        assert value, f"{path} is empty in the spec under test"
        for item in value.values():
            _descend(typing.get_args(annotation)[1], item, f"{path}{{}}", check)


def _every_field_present(model: type[BaseModel], raw: dict, path: str) -> None:
    missing = sorted(set(model.model_fields) - set(raw))
    assert not missing, f"{path}: {missing} never written by the maximal spec"


@pytest.mark.parametrize(
    "spec, model",
    [(_maximal_plot_spec(), w.PlotSpecWire), (_maximal_stats_spec(), w.StatsSpecWire)],
    ids=["plot", "stats"],
)
def test_maximal_spec_writes_every_wire_field(spec, model):
    _walk(model, _as_json(spec.to_dict()), model.__name__, _every_field_present)


def test_unknown_key_is_refused_like_from_dict():
    raw = PlotSpec(measures=["Force"]).to_dict()
    with pytest.raises(ValidationError):
        w.PlotSpecWire.model_validate({**raw, "x_layers": ["session"]})


def test_histogram_accepts_the_typed_exact_values_text():
    """The GUI sends the box's text; from_dict parses it."""
    raw = {"measures": ["Force"], "histogram": {"exact_values": "0, -1"}}
    w.PlotSpecWire.model_validate(raw)
    assert PlotSpec.from_dict(raw).histogram.exact_values == (-1.0, 0.0)


def test_spec_types_are_in_the_generated_schema():
    defs = api_json_schema()["$defs"]
    titles = {d.get("title") for d in defs.values()}
    assert {"PlotSpec", "StatsSpec", "CustomPlot", "Analysis", "GuideState"} <= titles


def test_choices_come_from_their_owners():
    """A vocabulary in the schema is the owner's tuple or enum, in order."""
    defs = api_json_schema()["$defs"]
    by_title = {d.get("title"): d for d in defs.values()}
    analysis = by_title["Analysis"]["properties"]
    assert analysis["family"]["enum"] == list(FAMILIES)
    assert by_title["DescriptiveSpec"]["properties"]["cell"]["enum"] == list(CELLS)
    assert by_title["PlotKind"]["enum"] == [k.value for k in PlotKind]


def test_spec_fields_carry_no_null_default():
    """A ``"default": null`` beside a ``$ref`` made json-schema-to-typescript
    emit numbered copies of the referenced type (``Aggregation1``)."""
    defs = api_json_schema()["$defs"]
    wire_titles = {model.model_config.get("title") for _, model in PAIRS}
    with_default = [
        f"{d['title']}.{name}"
        for d in defs.values()
        if d.get("title") in wire_titles
        for name, prop in (d.get("properties") or {}).items()
        if "default" in prop
    ]
    assert with_default == []
