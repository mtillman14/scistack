"""
Difference bars, Stage 1 (2026-09-27): the spec field, the text size, the
capability and the one resolver. ``scistackplot.diffbars`` owns what a bar
names (plan D1: the panel's ITERATE + FACET values and each end's x-layer
values, all as text) and which ticks of which panel it lands on. Placement,
rendering, export and GUI are later stages (``.claude/plan-difference-bars.md``).
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest

import scistackplot.reduce as reduce_mod
from scistackplot import (
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    resolve,
)
from scistackplot.capability import capabilities
from scistackplot.diffbars import (
    DEFAULT_LABEL,
    DifferenceBar,
    bars_for_panel,
    not_in_figure,
    slot_endpoints,
    unavailable,
)
from scistackplot.restore import restore_spec
from scistackplot.shape import Shape
from scistackplot.spec import TextSizes
from scistackplot.textsize import resolve_sizes
from scistackplot.xaxis import plan_x_axis

LAYER = "scistackplot"


@pytest.fixture(autouse=True)
def _clear_plan_cache():
    reduce_mod.clear_plan_cache()
    yield
    reduce_mod.clear_plan_cache()


@pytest.fixture
def gait_table() -> LongTable:
    """Two groups x two subjects each x sessions pre/post/follow x sides L/R,
    except that side L has no "follow" session (a ragged panel)."""
    rows = []
    for group, subjects in (("sham", ("01", "02")), ("stim", ("03", "04"))):
        for subject in subjects:
            for side in ("L", "R"):
                for number, session in enumerate(("pre", "post", "follow")):
                    if side == "L" and session == "follow":
                        continue
                    rows.append(
                        {
                            "group": group,
                            "subject": subject,
                            "side": side,
                            "session": session,
                            "Step": 1.0 + number + int(subject) / 10,
                        }
                    )
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["group", "subject", "side", "session"],
        measures=["Step"],
        name="Step",
        schema_levels=["subject", "side", "session"],
        level_order={"session": ["pre", "post", "follow"]},
    )


def _spec(*bars: DifferenceBar, **extra) -> PlotSpec:
    """A figure per group, a panel per side, a bar per session over subjects."""
    return PlotSpec(
        measures=["Step"],
        kind=PlotKind.BAR,
        roles={
            "group": Role.ITERATE,
            "side": Role.FACET,
            "session": Role.GROUP,
            "subject": Role.COLLAPSE,
        },
        groups=["session"],
        difference_bars=list(bars),
        **extra,
    )


def _bar(group: str, side: str, a: str, b: str, label: str = "*") -> DifferenceBar:
    return DifferenceBar(
        match={"group": group, "side": side},
        a={"session": a},
        b={"session": b},
        label=label,
    )


def _figure(figures, group: str):
    return next(f for f in figures if f.figure_key["group"] == group)


def _panel(figure, side: str):
    return next(p for p in figure.panels if p.key["side"] == side)


# --- the bar itself -----------------------------------------------------------


def test_for_panel_stores_panel_values_as_text():
    bar = DifferenceBar.for_panel({"subject": "01", "session": 2}, {"x": "a"}, {"x": "b"})
    assert bar.match == {"subject": "01", "session": "2"}
    assert bar.label == DEFAULT_LABEL
    assert bar.matches_panel({"subject": "01", "session": 2})


def test_zero_padding_is_kept_and_is_not_the_integer():
    bar = DifferenceBar(match={"subject": "01"}, a={"x": "a"}, b={"x": "b"})
    assert bar.matches_panel({"subject": "01"})
    assert not bar.matches_panel({"subject": 1})


def test_a_partial_panel_key_does_not_match():
    bar = DifferenceBar(match={"side": "L"}, a={"x": "a"}, b={"x": "b"})
    assert not bar.matches_panel({"side": "L", "group": "sham"})


def test_an_empty_match_is_the_one_panel_of_an_unkeyed_figure():
    bar = DifferenceBar(match={}, a={"x": "a"}, b={"x": "b"})
    assert bar.matches_panel({})
    assert not bar.matches_panel({"side": "L"})


@pytest.mark.parametrize(
    ("a", "b", "fragment"),
    [
        ({"x": "a"}, {"x": "a"}, "same tick"),
        ({}, {"x": "a"}, "names no tick"),
        ({"x": "a"}, {"y": "b"}, "different x layers"),
    ],
)
def test_problem_names_a_bar_that_can_never_draw(a, b, fragment):
    assert fragment in DifferenceBar(a=a, b=b).problem()


def test_same_pair_is_unordered():
    one = _bar("sham", "L", "pre", "post")
    assert one.same_pair(_bar("sham", "L", "post", "pre", label="**"))
    assert not one.same_pair(_bar("sham", "R", "pre", "post"))


# --- serialisation ------------------------------------------------------------


def test_bars_round_trip_through_json():
    spec = _spec(_bar("sham", "L", "pre", "post", "**"))
    again = PlotSpec.from_json(spec.to_json())
    assert again.difference_bars == spec.difference_bars


def test_bars_round_trip_through_toml():
    pytest.importorskip("tomli_w")
    spec = _spec(_bar("stim", "R", "pre", "follow"))
    assert PlotSpec.from_toml(spec.to_toml()).difference_bars == spec.difference_bars


def test_a_spec_without_bars_reads_as_none():
    raw = _spec().to_dict()
    raw.pop("difference_bars", None)
    assert PlotSpec.from_dict(raw).difference_bars == []


def test_a_missing_label_reads_as_the_default():
    assert DifferenceBar.from_dict({"a": {"x": "1"}, "b": {"x": "2"}}).label == "*"


def test_restore_keeps_the_bars():
    spec = _spec(_bar("sham", "L", "pre", "post"))
    restored = restore_spec(spec.to_dict())
    assert restored.spec.difference_bars == spec.difference_bars
    assert not [n for n in restored.notes if n.path.startswith("difference_bars")]


# --- text size (user choice: its own element) --------------------------------


def test_the_label_size_follows_base_until_set():
    assert resolve_sizes(StyleOptions(text=TextSizes(base=12.0))).differences == 12.0


def test_a_set_label_size_is_pinned():
    sizes = resolve_sizes(StyleOptions(text=TextSizes(base=12.0, differences=9.0)))
    assert sizes.differences == 9.0
    assert sizes.is_pinned("differences")


# --- where bars are offered ---------------------------------------------------


@pytest.mark.parametrize("kind", ["bar", "box", "violin", "scatter", "strip", "spaghetti"])
def test_categorical_scalar_kinds_offer_bars(kind):
    assert unavailable(PlotSpec(measures=["M"], kind=PlotKind(kind)), Shape.SCALAR) is None


@pytest.mark.parametrize("kind", ["line", "band", "heatmap"])
def test_other_kinds_say_why_not(kind):
    assert "no tick-by-tick marks" in unavailable(
        PlotSpec(measures=["M"], kind=PlotKind(kind)), Shape.SCALAR
    )


def test_an_x_measure_says_why_not():
    spec = PlotSpec(measures=["M", "N"], x_measure="N", kind=PlotKind.SCATTER)
    assert "x-y plot" in unavailable(spec, Shape.SCALAR)


def test_a_series_shape_says_why_not():
    assert "one value per row" in unavailable(
        PlotSpec(measures=["M"], kind=PlotKind.BAR), Shape.SERIES_1D
    )


def test_capabilities_ship_the_answer(gait_table):
    report = capabilities(_spec(), gait_table)
    assert report["difference_bars"] == {"available": True, "reason": None}


# --- ticks --------------------------------------------------------------------


def test_leaf_values_are_recorded_with_spacers_as_none():
    plan = plan_x_axis(
        [("sham", "pre"), ("sham", "post"), ("stim", "pre")],
        [["sham", "stim"], ["pre", "post"]],
    )
    assert len(plan.leaf_values) == len(plan.order)
    assert plan.leaf_values == [("sham", "pre"), ("sham", "post"), None, ("stim", "pre")]


def test_single_layer_endpoints_are_the_ticks_in_order(gait_table):
    figure = _figure(resolve(_spec(), gait_table), "sham")
    ends = slot_endpoints(figure)
    assert [end.values for end in ends] == [
        {"session": "pre"},
        {"session": "post"},
        {"session": "follow"},
    ]
    assert [end.position for end in ends] == [0.0, 1.0, 2.0]


def test_nested_endpoints_name_every_layer_and_skip_spacers(gait_table):
    spec = PlotSpec(
        measures=["Step"],
        kind=PlotKind.BAR,
        roles={
            "group": Role.GROUP,
            "session": Role.GROUP,
            "side": Role.ITERATE,
            "subject": Role.COLLAPSE,
        },
        groups=["session", "group"],
    )
    figure = resolve(spec, gait_table)[0]
    ends = slot_endpoints(figure)
    assert all(set(end.values) == {"group", "session"} for end in ends)
    # Spacers carry no endpoint; every endpoint sits on its own slot.
    assert len({end.slot for end in ends}) == len(ends) < len(figure.x_order)
    assert all(not str(figure.x_order[end.slot]).startswith(" gap") for end in ends)


# --- resolving ----------------------------------------------------------------


def test_a_bar_lands_on_its_own_panel_only(gait_table):
    figures = resolve(_spec(_bar("sham", "R", "follow", "pre")), gait_table)
    sham = _figure(figures, "sham")
    drawn, unresolved = bars_for_panel(sham, _panel(sham, "R"))
    assert not unresolved
    assert [(d.left.values, d.right.values) for d in drawn] == [
        ({"session": "pre"}, {"session": "follow"})  # left end first
    ]
    assert bars_for_panel(sham, _panel(sham, "L")) == ([], [])
    stim = _figure(figures, "stim")
    assert all(bars_for_panel(stim, p) == ([], []) for p in stim.panels)
    assert not_in_figure(sham) == []
    assert not_in_figure(stim) == [_bar("sham", "R", "follow", "pre")]


def test_an_end_with_no_mark_in_the_panel_is_unresolved(gait_table):
    """Side L has no "follow" session: the axis has the tick, the panel is empty there."""
    sham = _figure(resolve(_spec(_bar("sham", "L", "pre", "follow")), gait_table), "sham")
    drawn, unresolved = bars_for_panel(sham, _panel(sham, "L"))
    assert drawn == []
    assert "'follow' has no mark in this panel" in unresolved[0].reason


def test_an_unknown_level_is_unresolved(gait_table):
    sham = _figure(resolve(_spec(_bar("sham", "R", "pre", "later")), gait_table), "sham")
    _, unresolved = bars_for_panel(sham, _panel(sham, "R"))
    assert "'later' is not a tick of this figure" in unresolved[0].reason


def test_ends_naming_other_layers_are_unresolved(gait_table):
    bar = DifferenceBar(
        match={"group": "sham", "side": "R"}, a={"visit": "1"}, b={"visit": "2"}
    )
    sham = _figure(resolve(_spec(bar), gait_table), "sham")
    _, unresolved = bars_for_panel(sham, _panel(sham, "R"))
    assert "the axis has ['session']" in unresolved[0].reason


def test_the_same_pair_twice_draws_once_with_the_last_label(gait_table, caplog):
    first = _bar("sham", "R", "pre", "post", "*")
    second = _bar("sham", "R", "post", "pre", "**")
    sham = _figure(resolve(_spec(first, second), gait_table), "sham")
    with caplog.at_level(logging.WARNING, logger=LAYER):
        drawn, _ = bars_for_panel(sham, _panel(sham, "R"))
    assert [d.bar.label for d in drawn] == ["**"]
    assert "is given more than once" in caplog.text


def test_resolving_is_logged_once_per_figure(gait_table, caplog):
    bars = (_bar("sham", "R", "pre", "post"), _bar("sham", "L", "pre", "follow"))
    with caplog.at_level(logging.INFO, logger=LAYER):
        resolve(_spec(*bars), gait_table)
    assert (
        "difference bars [group=sham]: 1 resolved (group=sham, side=R: pre–post '*'), "
        "1 unresolved" in caplog.text
    )
    assert "difference bars [group=stim]: 0 resolved (none), 0 unresolved, 2 not in this figure" in (
        caplog.text
    )


def test_a_bar_that_can_never_draw_warns(gait_table, caplog):
    with caplog.at_level(logging.WARNING, logger=LAYER):
        resolve(_spec(_bar("sham", "R", "pre", "pre")), gait_table)
    assert "can never be drawn (both ends are the same tick)" in caplog.text


def test_adding_a_bar_does_not_rebuild_the_plan(gait_table):
    """`difference_bars` is plan-irrelevant: adding one only redraws."""
    resolve(_spec(), gait_table)
    figures = resolve(_spec(_bar("sham", "R", "pre", "post")), gait_table)
    # The cached plan's spec carries the new bars (reduce._with_presentation).
    assert _figure(figures, "sham").spec.difference_bars == [_bar("sham", "R", "pre", "post")]
    assert reduce_mod._plan_cache_key(_spec(), gait_table) == reduce_mod._plan_cache_key(
        _spec(_bar("sham", "R", "pre", "post")), gait_table
    )
