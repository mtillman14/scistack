"""The automatic text size (scistackplot.autosize; user, 2026-10-06).

Part 1 tests the search with a fake layout, so its rules are pinned without
matplotlib: per-element independence, the band, "rotation and legend-below
only at the floor", the shared plot-area rule. Part 2 checks the real
matplotlib layout and that every consumer draws the one chosen set.
"""

from __future__ import annotations

import logging
from dataclasses import replace

import pytest

from scistackplot import TEXT_TARGETS, TextSizes
from scistackplot.autosize import (
    ALL,
    BANDS,
    CEILING_RATIOS,
    ELEMENTS,
    MIN_DATA_FRACTION,
    LayoutReport,
    auto_elements,
    candidates,
    choose_sizes,
    element_band,
)

# ---------------------------------------------------------------------------
# Part 1: the search, with a fake layout


def fake_layout(limits: dict[str, float], *, data=None, couple=None):
    """A layout where each element is clean up to its own limit.

    ``limits[e]``: the largest clean size of e (anything above it reports
    the problem that element really has). ``data(sizes)``: the plot-area
    fraction. ``couple(sizes)``: extra x-tick limit that depends on others.
    """

    def measure(sizes: dict[str, float]) -> LayoutReport:
        over = {e for e, limit in limits.items() if e in sizes and sizes[e] > limit + 1e-9}
        if couple is not None and sizes.get("x_ticks", 0) > couple(sizes) + 1e-9:
            over.add("x_ticks")
        return LayoutReport(
            tick_steps=("rotate_45",) if "x_ticks" in over else (),
            bracket_steps=("shrink",) if "groups" in over else (),
            legend_steps=("below",) if "legend" in over else (),
            left_overlap_pt=3.0 if "y_ticks" in over else 0.0,
            overflow_by_element=tuple(
                (e, 0.2) for e in ("title", "x_label", "y_label", "differences") if e in over
            ),
            data_fraction=data(sizes) if data else 1.0,
        )

    return measure


def _choose(limits=None, *, target="print", elements=ELEMENTS, **kwargs):
    return choose_sizes(
        target=target, elements=elements, measure=fake_layout(limits or {}, **kwargs)
    )


def test_every_target_has_a_band():
    assert set(BANDS) == set(TEXT_TARGETS)
    assert set(CEILING_RATIOS) == set(ELEMENTS)


def test_the_bands_the_user_confirmed():
    assert (BANDS["print"].floor_pt, BANDS["print"].ceiling_pt) == (8.0, 12.0)
    assert (BANDS["slide"].floor_pt, BANDS["slide"].ceiling_pt) == (14.0, 28.0)


def test_candidates_run_from_the_ceiling_to_exactly_the_floor():
    assert candidates(8.0, 10.0) == [10.0, 9.5, 9.0, 8.5, 8.0]
    assert candidates(8.0, 9.996) == [9.996, 9.496, 8.996, 8.496, 8.0]
    assert candidates(12.0, 10.0) == [10.0]


def test_a_figure_that_fits_at_the_ceiling_costs_one_layout():
    result = _choose()
    assert result.layouts == 1
    for element in ELEMENTS:
        assert result.sizes[element] == pytest.approx(element_band(element, "print")[1])
    assert result.binding == {}


def test_a_long_x_title_does_not_shrink_the_y_ticks():
    """The user's example (2026-10-06): each element sizes on its own."""
    result = _choose({"x_label": 9.0})
    assert result.sizes["x_label"] == 9.0
    assert result.sizes["y_ticks"] == 12.0
    assert result.sizes["x_ticks"] == 12.0
    assert "x_label" in result.binding and set(result.binding) == {"x_label"}


def test_several_elements_search_on_the_same_layouts():
    """Parallel binary search: about log2(candidates) + 1 layouts, not that
    once per element."""
    result = _choose({"x_ticks": 9.5, "legend": 10.0, "y_ticks": 8.5, "title": 11.0})
    assert result.sizes["x_ticks"] == 9.5
    assert result.sizes["legend"] == 10.0
    assert result.sizes["y_ticks"] == 8.5
    assert result.sizes["title"] == pytest.approx(11.0, abs=0.5)
    assert result.layouts <= 7


def test_rotation_is_allowed_only_at_the_floor():
    """x ticks that rotate at every size: the floor is chosen and the
    fitting handles the rest; the reason is said."""
    result = _choose({"x_ticks": 1.0})
    assert result.sizes["x_ticks"] == 8.0
    assert "x_ticks" in result.at_floor
    assert "rotate" in result.binding["x_ticks"]


def test_moving_the_legend_below_is_a_problem():
    result = _choose({"legend": 10.5})
    assert result.sizes["legend"] == 10.5
    assert "below" in result.binding["legend"]


def test_the_plot_area_rule_shrinks_every_element():
    """The one shared rule: too little plot is all the text at once."""

    def data(sizes):
        return 1.0 - 0.006 * sum(v for e, v in sizes.items() if e != "base")

    result = _choose(data=data)
    assert all(result.sizes[e] < element_band(e, "print")[1] for e in ELEMENTS)
    assert data({e: result.sizes[e] for e in ELEMENTS}) >= MIN_DATA_FRACTION
    assert result.layouts <= 16


def test_the_combination_is_verified():
    """x ticks fit only if x + y <= 21: the per-element search probes them
    with y at other sizes, so the chosen pair must be checked as a pair."""
    result = _choose({"y_ticks": 11.0}, couple=lambda s: 21.0 - s.get("y_ticks", 0))
    sizes = result.sizes
    assert sizes["x_ticks"] + sizes["y_ticks"] <= 21.0 + 1e-9
    last_sizes, last_blame = result.tried[-1]
    assert (last_sizes["x_ticks"], last_sizes["y_ticks"]) == (sizes["x_ticks"], sizes["y_ticks"])
    assert last_blame == {}


def test_brackets_are_never_larger_than_the_ticks():
    result = _choose({"x_ticks": 8.5})
    assert result.sizes["groups"] <= result.sizes["x_ticks"]


def test_base_is_the_smaller_tick_size():
    result = _choose({"x_ticks": 9.0})
    assert result.sizes["base"] == 9.0


def test_the_slide_band_is_larger():
    result = _choose(target="slide")
    assert result.sizes["x_ticks"] == 28.0
    assert _choose({"x_ticks": 1.0}, target="slide").sizes["x_ticks"] == 14.0


def test_fixed_elements_are_not_searched():
    assert "y_ticks" not in auto_elements(TextSizes(y_ticks=10.0))
    result = _choose(elements=auto_elements(TextSizes(y_ticks=10.0)))
    assert "y_ticks" not in result.sizes


def test_everything_fixed_needs_no_layout():
    result = _choose(elements=())
    assert result.layouts == 0 and result.sizes == {"base": 12.0}


def test_blame_names_the_element_that_has_the_problem():
    blame = LayoutReport(
        tick_steps=("strip_prefix", "wrap"),
        legend_steps=("wrap_title", "short_handles"),
    ).blame()
    assert blame == {}, "harmless steps are not problems"
    blame = LayoutReport(left_overlap_pt=2.0).blame()
    assert set(blame) == {"y_ticks", "y_label"}
    blame = LayoutReport(below_overlap_pt=2.0).blame()
    assert set(blame) == {"x_ticks", "x_label", "groups"}
    blame = LayoutReport(overflow_by_element=(("x_label", 0.3),)).blame()
    assert set(blame) == {"x_label"}
    blame = LayoutReport(unattributed_overflow_in=0.3).blame()
    assert set(blame) == {ALL}
    blame = LayoutReport(tick_steps=("shrink",), ticks_fit=False).blame()
    assert blame["x_ticks"] == ("x ticks shrink", "x ticks overlap")


# ---------------------------------------------------------------------------
# Part 2: the real layout

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from scistackplot import (  # noqa: E402
    generate_plot_function,
    layout_decisions,
    render_matplotlib,
    render_plotly,
    resolve,
)
from scistackplot.autosize import settle  # noqa: E402
from test_mpl_label_fit import _graph1, gait_table  # noqa: E402,F401


def _auto(spec, **text):
    return replace(spec, style=replace(spec.style, text=TextSizes(**text)))


def _one(spec, table):
    (resolved,) = resolve(spec, table)
    return resolved


def test_auto_is_settled_and_drawn(gait_table):
    resolved = settle(_one(_auto(_graph1(width=8.0, height=6.0)), gait_table))
    auto = resolved.auto_text
    assert auto is not None and auto.layouts >= 1
    for element in ELEMENTS:
        floor, ceiling = element_band(element, "print")
        assert floor - 1e-9 <= auto.sizes[element] <= ceiling + 1e-9, element
    figure = render_matplotlib(resolved)
    try:
        figure.canvas.draw()
        ax = figure.axes[0]
        assert ax.get_yticklabels()[0].get_fontsize() == pytest.approx(auto.sizes["y_ticks"])
    finally:
        plt.close(figure)


def test_a_narrow_figure_shrinks_the_x_ticks_first(gait_table):
    """15 ticks at 3.5 in cannot sit upright at 12 pt; the y ticks have no
    such problem, so they are never made smaller than the x ticks."""
    auto = settle(_one(_auto(_graph1(width=3.5, height=3.0)), gait_table)).auto_text
    assert "x_ticks" in auto.binding
    assert auto.sizes["y_ticks"] >= auto.sizes["x_ticks"]


def test_a_fixed_base_is_not_searched(gait_table):
    resolved = _one(_graph1(width=8.0, height=6.0), gait_table)  # FIXED_14
    assert settle(resolved) is resolved
    assert layout_decisions(resolved)["auto_text"] is None


def test_preview_save_and_decisions_choose_the_same_sizes(gait_table):
    resolved = _one(_auto(_graph1(width=8.0, height=6.0)), gait_table)
    decisions = layout_decisions(resolved)
    chosen = decisions["auto_text"]
    assert chosen is not None
    assert settle(resolved).auto_text.sizes == chosen.sizes  # the memo, not a 2nd search
    meta = render_plotly(resolved, decisions=decisions)["layout"]["meta"]["text_sizes"]
    assert meta["x_ticks"] == pytest.approx(chosen.sizes["x_ticks"])
    assert meta["auto"]["sizes"]["y_ticks"] == pytest.approx(chosen.sizes["y_ticks"])
    assert meta["targets"] == list(TEXT_TARGETS)


def test_generated_code_bakes_in_the_chosen_sizes(gait_table):
    spec = _auto(_graph1(width=8.0, height=6.0))
    chosen = settle(_one(spec, gait_table)).auto_text
    source = generate_plot_function(spec, gait_table)
    assert "# text sizes: auto, chosen by scistackplot" in source
    assert f"'ytick.labelsize': {chosen.sizes['y_ticks']!r}" in source


def test_slide_text_is_larger_than_print(gait_table):
    spec = _graph1(width=13.33, height=7.5)
    print_ = settle(_one(_auto(spec), gait_table)).auto_text
    slide = settle(_one(_auto(spec, target="slide"), gait_table)).auto_text
    assert slide.sizes["y_ticks"] > print_.sizes["y_ticks"]


def test_trial_layouts_log_at_debug_only(gait_table, caplog):
    """A failing candidate's WARN (overlap, legend too tall) describes a
    layout that is thrown away; it must not read as the figure's own."""
    caplog.set_level(logging.DEBUG)
    settle(_one(_auto(_graph1(width=3.5, height=3.0)), gait_table))
    trial = [r for r in caplog.records if "[auto-size trial" in r.getMessage()]
    assert trial, "the trials should still be visible at DEBUG"
    assert all(r.levelno == logging.DEBUG for r in trial)
    assert any("auto text size" in r.getMessage() and r.levelno == logging.INFO for r in caplog.records)
