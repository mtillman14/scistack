"""
Stage 2b of the label-legibility plan: the exported legend fits the figure.

The steps, least destructive first: wrap the title at " / ", shorten the line
samples, shrink the text (never below the tick labels' floor). If it's still
wider than ``LEGEND_BUDGET``, or if at the right it leaves the x labels no room,
it moves below the panels in as many columns as fit. Checked as properties of
the canvas: the budget is kept, the floor is kept, and a legend below never
overlaps the panels.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from scistackplot import render_matplotlib, resolve  # noqa: E402
from scistackplot.render.mpl import LEGEND_ATTR, LEGEND_BUDGET  # noqa: E402
from scistackplot.ticklabels import TICK_POLICY  # noqa: E402
from test_mpl_label_fit import _graph1 as _plain_graph1  # noqa: E402
from test_mpl_label_fit import _graph2 as _plain_graph2  # noqa: E402
from test_mpl_label_fit import gait_table  # noqa: E402,F401


def _graph1(**kwargs):
    """graph1 as reported: the Show-sample points coloured by subject, so the
    legend lists SS01… under "session / subject" (spec/images/graph1.png)."""
    return replace(_plain_graph1(**kwargs), sample_color="subject")


def _graph2(**kwargs):
    """graph2 as reported: "InterventionGroup / subject" (graph2.png)."""
    return replace(_plain_graph2(**kwargs), sample_color="subject")


def _draw(spec, table):
    (resolved,) = resolve(spec, table)
    figure = render_matplotlib(resolved)
    figure.canvas.draw()
    return figure


def _roomy_x_table():
    """Five session ticks and six subjects: x labels that fit with room to
    spare, so the legend's own rule is the only thing moving it."""
    import pandas as pd

    from scistackplot import LongTable

    rows = [
        {"subject": f"SS{s:02d}", "session": session, "M": float(k + s)}
        for s in range(1, 7)
        for k, session in enumerate(["BL", "MID24", "POST24", "MO1FU", "MO3FU"])
    ]
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "session"],
        measures=["M"],
        name="M",
        level_order={"session": ["BL", "MID24", "POST24", "MO1FU", "MO3FU"]},
        schema_levels=["subject", "session"],
    )


def test_a_long_title_wraps_before_anything_shrinks():
    """At 5in "session / subject" is the legend's widest line, ~35% of the
    width unwrapped. The x labels fit easily, so the legend stays at the
    right and the first narrowing step is the title wrap. (graph1 itself, at
    5in, sends the legend below: its 17 ticks need the width.)"""
    from scistackplot import PlotKind, PlotSpec, Role, StyleOptions

    spec = PlotSpec(
        measures=["M"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["session"],
        color="session",
        kind=PlotKind.BAR,
        show_sample=["subject"],
        sample_color="subject",
        style=StyleOptions(width=5.0, height=4.0),
    )
    figure = _draw(spec, _roomy_x_table())
    try:
        legend = getattr(figure, LEGEND_ATTR)
        assert not legend["below"], legend
        assert legend["steps"][0] == "wrap_title", legend
        (drawn,) = figure.legends
        assert drawn.get_title().get_text() == "session /\nsubject"
    finally:
        plt.close(figure)


def test_a_legend_that_fits_is_left_alone(gait_table):
    """Wide enough that "session / subject" sits within the budget as is."""
    figure = _draw(_graph1(width=12.0), gait_table)
    try:
        legend = getattr(figure, LEGEND_ATTR)
        assert legend["steps"] == [] and not legend["below"], legend
        (drawn,) = figure.legends
        assert drawn.get_title().get_text() == "session / subject"
    finally:
        plt.close(figure)


@pytest.mark.parametrize("width", [3.0, 6.0, 8.0, 14.0])
def test_a_legend_at_the_right_keeps_to_its_budget(gait_table, width):
    figure = _draw(_graph2(width=width), gait_table)
    try:
        legend = getattr(figure, LEGEND_ATTR)
        if not legend["below"]:
            assert legend["width_frac"] <= LEGEND_BUDGET + 1e-9, legend
    finally:
        plt.close(figure)


@pytest.mark.parametrize("width", [3.0, 6.0, 8.0, 14.0])
def test_legend_text_never_drops_below_the_floor(gait_table, width):
    figure = _draw(_graph2(width=width), gait_table)
    try:
        floor = TICK_POLICY.font_floor(14.0)
        (drawn,) = figure.legends
        sizes = [t.get_fontsize() for t in drawn.get_texts()] + [
            drawn.get_title().get_fontsize()
        ]
        assert min(sizes) >= floor - 1e-6, sizes
    finally:
        plt.close(figure)


def test_a_legend_too_wide_even_narrowed_goes_below(gait_table):
    """At 3in the budget is ~65pt; "InterventionGroup /" alone is wider at
    the floor font."""
    figure = _draw(_graph2(width=3.0), gait_table)
    try:
        legend = getattr(figure, LEGEND_ATTR)
        assert legend["below"], legend
        assert "even narrowed" in legend["reason"], legend
        (drawn,) = figure.legends
        # Below, the title is one line again: width is no longer the constraint.
        assert "\n" not in drawn.get_title().get_text()
        assert legend["columns"] >= 1
    finally:
        plt.close(figure)


@pytest.mark.parametrize(
    "spec",
    [
        # Moved below because the x labels needed the width.
        _graph2(),
        # Moved below because even narrowed it was too wide; three entries
        # (the subjects left out of the legend) fit in a row or two.
        replace(_graph2(width=4.0), sample_in_legend=False),
    ],
    ids=["graph2", "graph2-narrow-unlisted"],
)
def test_a_legend_below_never_overlaps_the_panels(gait_table, spec):
    figure = _draw(spec, gait_table)
    try:
        assert getattr(figure, LEGEND_ATTR)["below"]
        renderer = figure.canvas.get_renderer()
        (drawn,) = figure.legends
        top = drawn.get_window_extent(renderer).y1
        lowest = min(
            ax.get_tightbbox(renderer).y0 for ax in figure.axes if ax.get_visible()
        )
        assert top <= lowest + 1, (top, lowest)
        assert drawn.get_window_extent(renderer).x1 <= figure.bbox.x1 + 1
    finally:
        plt.close(figure)
