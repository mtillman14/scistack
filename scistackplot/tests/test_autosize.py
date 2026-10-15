"""The automatic text size: ONE reference (scistackplot.autosize; user, 2026-10-14).

Part 1 tests the search with a fake layout, so its rules are pinned without
matplotlib: one dial, the range, binary-search cost, "at the bottom the
fitting handles it". Part 2 checks the real matplotlib layout and that every
consumer draws the one chosen reference.
"""

from __future__ import annotations

import logging
import math
from dataclasses import replace

import pandas as pd
import pytest

from scistackplot import DEFAULT_TEXT_RATIOS, TEXT_TARGETS, TextSizes
from scistackplot.autosize import (
    MIN_TICK_LABELS,
    LayoutReport,
    candidates,
    choose_reference,
)

# ---------------------------------------------------------------------------
# Part 1: the search, with a fake layout


def fake_layout(limit: float, report: LayoutReport | None = None, calls: list | None = None):
    """Clean up to ``limit``; above it, ``report`` (x ticks rotate by default)."""
    failing = report or LayoutReport(tick_steps=("rotate_45",))

    def measure(reference: float) -> LayoutReport:
        if calls is not None:
            calls.append(reference)
        return failing if reference > limit + 1e-9 else LayoutReport()

    return measure


def _choose(limit=99.0, text=None, **kwargs):
    return choose_reference(text=text or TextSizes(), measure=fake_layout(limit, **kwargs))


def test_candidates_run_from_the_top_to_exactly_the_bottom():
    assert candidates(8.0, 10.0) == [10.0, 9.5, 9.0, 8.5, 8.0]
    assert candidates(8.0, 9.996) == [9.996, 9.496, 8.996, 8.496, 8.0]
    assert candidates(12.0, 10.0) == [10.0]


def test_a_figure_clean_at_the_top_costs_one_layout():
    result = _choose()
    assert result.layouts == 1
    assert result.reference == 12.0
    assert result.limit == () and result.failing == ()
    assert result.sizes["title"] == pytest.approx(12.0 * DEFAULT_TEXT_RATIOS["title"])


def test_one_failing_rule_steps_every_element_down_together():
    """The user's rule (2026-10-14): proportions hold. A long x title does not
    leave the y ticks at the top while it shrinks alone."""
    result = _choose(10.0, report=LayoutReport(overflow_by_element=(("x_label", 0.2),)))
    assert result.reference == 10.0
    for element, ratio in DEFAULT_TEXT_RATIOS.items():
        assert result.sizes[element] == pytest.approx(10.0 * ratio), element
    assert result.limit == ("x_label runs 0.20in off the canvas",)


def test_binary_search_costs_about_log2_of_the_candidates():
    calls: list[float] = []
    result = _choose(9.5, calls=calls)
    assert result.reference == 9.5
    n = len(candidates(8.0, 12.0))
    assert result.layouts <= 1 + math.ceil(math.log2(n)) + 1
    assert calls[0] == 12.0, "the first layout is the top"


def test_at_the_bottom_the_rules_still_failing_are_named():
    result = _choose(1.0)
    assert result.reference == 8.0 and result.at_bottom
    assert result.failing == ("x ticks rotate 45°",)
    assert "still failing" in result.describe()


def test_the_slide_range_is_larger():
    assert _choose(text=TextSizes(target="slide")).reference == 28.0
    assert _choose(1.0, text=TextSizes(target="slide")).reference == 14.0


def test_user_bounds_widen_the_range():
    result = _choose(text=TextSizes(ceilings={"y_ticks": 16.0}))
    assert result.span == (8.0, 16.0)
    assert result.reference == 16.0
    # The other elements are held at their own ceilings, and say so.
    assert result.sizes["x_ticks"] == 12.0 and result.clamped["x_ticks"] == "ceiling"
    assert result.sizes["y_ticks"] == 16.0 and "y_ticks" not in result.clamped


def test_fixed_elements_are_not_scaled():
    result = _choose(10.0, text=TextSizes(y_ticks=7.0))
    assert result.reference == 10.0
    assert result.sizes["y_ticks"] == 7.0


def test_everything_fixed_needs_no_layout():
    text = TextSizes(**{e: 10.0 for e in DEFAULT_TEXT_RATIOS})
    result = _choose(text=text)
    assert result.layouts == 0 and result.span is None


def test_failures_name_every_rule():
    assert LayoutReport(
        tick_steps=("strip_prefix", "wrap"), legend_steps=("wrap_title", "short_handles")
    ).failures() == (), "harmless steps are not failures"
    assert LayoutReport(left_overlap_pt=2.0).failures()[0].startswith("y-side text overlaps")
    assert LayoutReport(below_overlap_pt=2.0).failures()[0].startswith("x-side text overlaps")
    assert LayoutReport(legend_steps=("below",)).failures() == ("legend moves below",)
    assert LayoutReport(tick_steps=("shrink",), ticks_fit=False).failures() == (
        "x ticks shrink", "x ticks overlap",
    )
    assert "collide" in LayoutReport(x_tick_collisions=("(0,0)",)).failures()[0]
    assert "collide" in LayoutReport(y_tick_collisions=("(1,0)",)).failures()[0]
    few = LayoutReport(fewest_y_ticks=("(4,0)", MIN_TICK_LABELS - 1)).failures()
    assert few == (f"panel (4,0) shows 2 y tick label(s) (< {MIN_TICK_LABELS})",)
    assert LayoutReport(fewest_y_ticks=("(0,0)", MIN_TICK_LABELS)).failures() == ()
    assert "x tick label" in LayoutReport(fewest_x_ticks=("(0,0)", 1)).failures()[0]
    assert "taller than the panel" in LayoutReport(
        y_title_overruns=(("(0,0)", 12.0),)
    ).failures()[0]
    assert LayoutReport(unattributed_overflow_in=0.3).failures() == (
        "content runs 0.30in off the canvas",
    )


def test_to_dict_for_the_gui():
    payload = _choose(10.0).to_dict()
    assert payload["reference"] == 10.0
    assert payload["span"] == [8.0, 12.0]
    assert payload["limit"] == ["x ticks rotate 45°"]
    assert payload["failing"] == [] and payload["at_bottom"] is False
    assert set(payload["sizes"]) == set(DEFAULT_TEXT_RATIOS)


# ---------------------------------------------------------------------------
# Part 2: the real layout

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from scistackplot import (  # noqa: E402
    FacetOptions,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    generate_plot_function,
    layout_decisions,
    render_matplotlib,
    render_plotly,
    resolve,
)
from scistackplot.autosize import settle  # noqa: E402
from test_mpl_label_fit import _graph1, gait_table  # noqa: E402,F401

#: The most layouts a 9-candidate search may take (the top, then log2).
MAX_LAYOUTS = 1 + math.ceil(math.log2(len(candidates(8.0, 12.0)))) + 1


def _auto(spec, **text):
    return replace(spec, style=replace(spec.style, text=TextSizes(**text)))


def _one(spec, table):
    (resolved,) = resolve(spec, table)
    return resolved


MUSCLES = ["TA", "SOL", "GM", "VL", "RF"]


@pytest.fixture
def five_muscles() -> LongTable:
    rows = [
        {
            "subject": f"S{s}",
            "muscle": muscle,
            "session": session,
            "Peak": 100.0 * (m + 1) + 7.0 * s + 3.0 * k,
        }
        for s in range(4)
        for m, muscle in enumerate(MUSCLES)
        for k, session in enumerate(["pre", "post", "follow-up"])
    ]
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "muscle", "session"],
        measures=["Peak"],
        name="Peak",
        level_order={"muscle": MUSCLES, "session": ["pre", "post", "follow-up"]},
        schema_levels=["subject", "muscle", "session"],
    )


def _grid_spec(width: float, height: float) -> PlotSpec:
    """Five panels in a 2x3 grid (one slot empty)."""
    return PlotSpec(
        measures=["Peak"],
        roles={"muscle": Role.FACET, "session": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.BAR,
        facet=FacetOptions(n_cols=3),
        style=StyleOptions(width=width, height=height),
    )


@pytest.mark.parametrize("size", [(8.0, 6.0), (13.06, 8.89)], ids=["8x6", "13.06x8.89"])
def test_a_2x3_grid_is_limited_only_by_a_named_rule(five_muscles, size):
    auto = settle(_one(_grid_spec(*size), five_muscles)).auto_text
    assert auto is not None, "auto spec must settle"
    assert auto.failing == (), auto.describe()
    if auto.reference < auto.span[1]:
        assert auto.limit, f"below the top with no rule named: {auto.describe()}"
    assert auto.layouts <= MAX_LAYOUTS, auto.describe()
    # One dial: every scaled element keeps its ratio unless clamped.
    for element, ratio in DEFAULT_TEXT_RATIOS.items():
        if element not in auto.clamped:
            assert auto.sizes[element] == pytest.approx(auto.reference * ratio, abs=1e-3)


def test_the_bigger_canvas_never_draws_smaller_text(five_muscles):
    small = settle(_one(_grid_spec(8.0, 6.0), five_muscles)).auto_text
    large = settle(_one(_grid_spec(13.06, 8.89), five_muscles)).auto_text
    assert large.reference >= small.reference, (small.describe(), large.describe())


def test_auto_is_settled_and_drawn(gait_table):
    resolved = settle(_one(_auto(_graph1(width=8.0, height=6.0)), gait_table))
    auto = resolved.auto_text
    assert auto is not None and auto.layouts >= 1
    assert 8.0 - 1e-9 <= auto.reference <= 12.0 + 1e-9
    figure = render_matplotlib(resolved)
    try:
        figure.canvas.draw()
        ax = figure.axes[0]
        assert ax.get_yticklabels()[0].get_fontsize() == pytest.approx(auto.sizes["y_ticks"])
        assert ax.yaxis.label.get_fontsize() == pytest.approx(auto.sizes["y_label"])
    finally:
        plt.close(figure)


def test_a_narrow_figure_steps_the_reference_down_and_says_why(gait_table):
    """15 ticks at 3.5 in cannot sit upright at 12 pt: the whole reference
    steps down, and the rule that failed one step up is named."""
    auto = settle(_one(_auto(_graph1(width=3.5, height=3.0)), gait_table)).auto_text
    assert auto.reference < 12.0
    assert auto.limit or auto.failing, auto.describe()
    assert auto.sizes["y_ticks"] == auto.sizes["x_ticks"]


def test_a_fixed_base_is_not_searched(gait_table):
    resolved = _one(_graph1(width=8.0, height=6.0), gait_table)  # FIXED_14
    assert settle(resolved) is resolved
    assert layout_decisions(resolved)["auto_text"] is None


def test_preview_save_and_decisions_choose_the_same_reference(gait_table):
    resolved = _one(_auto(_graph1(width=8.0, height=6.0)), gait_table)
    decisions = layout_decisions(resolved)
    chosen = decisions["auto_text"]
    assert chosen is not None
    assert settle(resolved).auto_text.reference == chosen.reference  # the memo
    meta = render_plotly(resolved, decisions=decisions)["layout"]["meta"]["text_sizes"]
    assert meta["x_ticks"] == pytest.approx(chosen.sizes["x_ticks"])
    assert meta["title"] == pytest.approx(chosen.sizes["title"])
    assert meta["auto"]["reference"] == pytest.approx(chosen.reference)
    assert meta["targets"] == list(TEXT_TARGETS)


def test_generated_code_bakes_in_the_drawn_sizes(gait_table):
    spec = _auto(_graph1(width=8.0, height=6.0))
    chosen = settle(_one(spec, gait_table)).auto_text
    source = generate_plot_function(spec, gait_table)
    assert "# text sizes: auto, chosen by scistackplot" in source
    assert f"'ytick.labelsize': {chosen.sizes['y_ticks']!r}" in source
    assert f"'axes.labelsize': {chosen.sizes['x_label']!r}" in source
    assert f"'font.size': {chosen.reference!r}" in source


def test_slide_text_is_larger_than_print(gait_table):
    spec = _graph1(width=13.33, height=7.5)
    print_ = settle(_one(_auto(spec), gait_table)).auto_text
    slide = settle(_one(_auto(spec, target="slide"), gait_table)).auto_text
    assert slide.reference > print_.reference


def test_trial_layouts_log_at_debug_only(gait_table, caplog):
    """A failing candidate's WARN (overlap, legend too tall) describes a
    layout that is thrown away; it must not read as the figure's own."""
    caplog.set_level(logging.DEBUG)
    settle(_one(_auto(_graph1(width=3.5, height=3.0)), gait_table))
    trial = [r for r in caplog.records if "[auto-size trial" in r.getMessage()]
    assert trial, "the trials should still be visible at DEBUG"
    assert all(r.levelno == logging.DEBUG for r in trial)
    info = [
        r.getMessage() for r in caplog.records
        if "auto text size" in r.getMessage() and r.levelno == logging.INFO
    ]
    assert info and "reference" in info[-1]
