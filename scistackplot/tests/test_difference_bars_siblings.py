"""
Difference bars, Stage 3b (2026-09-27): figures of a fan-out that share a y
range stay in sync when one figure's bars raise its top (plan D7, user
choice B). ``reduce`` attaches the sibling figures that carry bars
(``ResolvedPlot.difference_siblings``); ``diffbars.sibling_floors`` places
their bars on THIS figure's measured panels (the cheap way) and the renderer
raises every range they share.
"""

from __future__ import annotations

import logging

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
import pytest  # noqa: E402

import scistackplot.reduce as reduce_mod  # noqa: E402
from scistackplot import (  # noqa: E402
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    YAxis,
    resolve,
    resolve_one,
)
from scistackplot.diffbars import DifferenceBar, figure_has_bars, range_key  # noqa: E402
from scistackplot.render.mpl import DIFF_BARS_ATTR  # noqa: E402
from scistackplot.render.mpl import render as render_mpl  # noqa: E402

LAYER = "scistackplot"


@pytest.fixture(autouse=True)
def _clear_plan_cache():
    reduce_mod.clear_plan_cache()
    yield
    reduce_mod.clear_plan_cache()
    plt.close("all")


def _table(*, stim_sides=("L", "R")) -> LongTable:
    rows = []
    for group, subjects in (("sham", ("01", "02", "03")), ("stim", ("04", "05", "06"))):
        for subject in subjects:
            for side in ("L", "R") if group == "sham" else stim_sides:
                for number, session in enumerate(("pre", "post", "follow")):
                    rows.append(
                        {
                            "group": group,
                            "subject": subject,
                            "side": side,
                            "session": session,
                            "Step": 2.0 + number + int(subject) * 0.1,
                        }
                    )
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["group", "subject", "side", "session"],
        measures=["Step"],
        name="Step",
        schema_levels=["subject", "side", "session"],
        level_order={"session": ["pre", "post", "follow"], "group": ["sham", "stim"]},
    )


#: Three stacked bars on sham's R panel: enough to raise the shared top.
SHAM_BARS = (
    DifferenceBar(match={"group": "sham", "side": "R"}, a={"session": "pre"}, b={"session": "post"}),
    DifferenceBar(match={"group": "sham", "side": "R"}, a={"session": "post"}, b={"session": "follow"}),
    DifferenceBar(match={"group": "sham", "side": "R"}, a={"session": "pre"}, b={"session": "follow"}),
)


def _spec(*bars: DifferenceBar, **extra) -> PlotSpec:
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
        style=StyleOptions(width=6.0, height=4.0),
        **extra,
    )


def _top(figure) -> float:
    fig = render_mpl(figure)
    fig.canvas.draw()
    tops = {round(ax.get_ylim()[1], 9) for ax in fig.axes if ax.get_visible()}
    assert len(tops) == 1, tops
    return tops.pop()


# --- which figures are siblings -----------------------------------------------


def test_figure_has_bars_reads_the_iterate_part_of_the_match():
    spec = _spec(*SHAM_BARS)
    assert figure_has_bars(spec, {"group": "sham"})
    assert not figure_has_bars(spec, {"group": "stim"})


def test_resolve_one_builds_only_the_siblings_with_bars():
    table = _table()
    stim, _, _ = resolve_one(_spec(*SHAM_BARS), table, 1)
    assert [s.figure_key for s in stim.difference_siblings] == [{"group": "sham"}]
    sham, _, _ = resolve_one(_spec(*SHAM_BARS), table, 0)
    assert sham.difference_siblings == []  # stim carries no bars


def test_resolve_attaches_the_same_siblings():
    figures = resolve(_spec(*SHAM_BARS), _table())
    by_group = {f.figure_key["group"]: f for f in figures}
    assert [id(s) for s in by_group["stim"].difference_siblings] == [id(by_group["sham"])]
    assert by_group["sham"].difference_siblings == []


def test_a_scope_that_separates_the_figures_has_no_siblings():
    spec = _spec(*SHAM_BARS, y_axis=YAxis(scope=["group"]))
    stim, _, _ = resolve_one(spec, _table(), 1)
    assert stim.difference_siblings == []


def test_siblings_are_not_serialised_or_compared():
    stim, _, _ = resolve_one(_spec(*SHAM_BARS), _table(), 1)
    assert "difference_siblings" not in stim.to_dict()


def test_range_key_is_the_scope_values_plus_the_typed_ends():
    figures = resolve(_spec(*SHAM_BARS, y_axis=YAxis(scope=["side"], maximum=None)), _table())
    keys = {range_key(f, p) for f in figures for p in f.panels}
    assert len(keys) == 2  # L and R, across both figures


# --- the shared top -----------------------------------------------------------


def test_a_figure_without_bars_is_raised_to_its_siblings_top(caplog):
    table = _table()
    sham, _, _ = resolve_one(_spec(*SHAM_BARS), table, 0)
    stim, _, _ = resolve_one(_spec(*SHAM_BARS), table, 1)
    plain_top = stim.y_limits[1]
    sham_top = _top(sham)
    with caplog.at_level(logging.INFO, logger=LAYER):
        stim_top = _top(stim)
    assert sham_top > plain_top
    # Estimated on stim's own panels (same grid, same size): the same top
    # to well within a point.
    assert stim_top == pytest.approx(sham_top, rel=1e-3)
    assert "estimated on this figure's panels" in caplog.text
    assert "raised to at least" in caplog.text


def test_without_bars_anywhere_nothing_is_raised():
    stim, _, _ = resolve_one(_spec(), _table(), 1)
    assert _top(stim) == pytest.approx(stim.y_limits[1])


def test_a_separate_scope_leaves_the_other_figure_alone():
    spec = _spec(*SHAM_BARS, y_axis=YAxis(scope=["group"]))
    stim, _, _ = resolve_one(spec, _table(), 1)
    assert _top(stim) == pytest.approx(stim.y_limits[1])


def test_a_typed_max_is_not_raised_by_a_sibling():
    spec = _spec(*SHAM_BARS, y_axis=YAxis(maximum=9.0))
    stim, _, _ = resolve_one(spec, _table(), 1)
    assert _top(stim) == pytest.approx(9.0)


def test_a_sibling_with_a_different_grid_is_laid_out(caplog):
    """Stim has only side R: one panel, where sham has two. Its panel heights
    are not sham's, so sham is laid out for real."""
    table = _table(stim_sides=("R",))
    stim, _, _ = resolve_one(_spec(*SHAM_BARS), table, 1)
    assert (stim.grid_rows, stim.grid_cols) != (
        stim.difference_siblings[0].grid_rows,
        stim.difference_siblings[0].grid_cols,
    )
    with caplog.at_level(logging.INFO, logger=LAYER):
        top = _top(stim)
    assert "laid out (its grid differs)" in caplog.text
    assert top > stim.y_limits[1]


def test_the_figure_with_bars_still_places_its_own(caplog):
    sham, _, _ = resolve_one(_spec(*SHAM_BARS), _table(), 0)
    fig = render_mpl(sham)
    placement = getattr(fig, DIFF_BARS_ATTR)
    assert placement.count == len(SHAM_BARS)
