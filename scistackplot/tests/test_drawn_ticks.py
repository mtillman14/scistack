"""Only DRAWN tick labels are measured, and the preview copies the export's y text.

C-Brace, 2026-10-14, two bugs with one root — matplotlib's tick list is not
the drawn tick list:

1. ``ax.get_yticklabels()`` also returns labels for the locator's positions
   just outside the view (an AutoLocator steps past both ends). They are never
   drawn but they measured, so the auto text size read a 1x1 figure's y ticks
   as "0.43in off the canvas" and held them at the 8 pt floor. Owner:
   ``mpl.drawn_ticks``.
2. The preview let plotly choose its own y tick values and title standoff, so
   its y text was wider than the column gap the export measured for it and
   ran into the panel on its left. The export now hands over what it drew
   (``base.AxisText``) and the preview draws exactly that.
"""

from __future__ import annotations

import pandas as pd
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from scistackplot import (  # noqa: E402
    LongTable,
    Role,
    StyleOptions,
    TextSizes,
    YAxis,
    layout_decisions,
    render_matplotlib,
    render_plotly,
    resolve,
)
from scistackplot.autosize import settle  # noqa: E402
from scistackplot.render.mpl import AXIS_TEXT_ATTR, drawn_ticks, layout_report  # noqa: E402
from test_ylimits import _spec, spread_table  # noqa: E402,F401

#: Limits no locator lands on, so it proposes a tick beyond each end.
ODD_LIMITS = YAxis(minimum=0.03, maximum=0.97)


def _single_panel(table, *, width=8.0, height=6.0, text=None, y_axis=ODD_LIMITS):
    spec = _spec(
        roles={"subject": Role.ITERATE, "muscle": Role.ITERATE, "trial": Role.GROUP},
        y_axis=y_axis,
        style=StyleOptions(text=text or TextSizes(), width=width, height=height),
    )
    return resolve(spec, table)[0]


# --- 1. drawn_ticks ----------------------------------------------------------


def test_drawn_ticks_leaves_out_the_locators_positions_beyond_the_view():
    fig, ax = plt.subplots()
    try:
        ax.set_ylim(0.3, 9.7)
        fig.canvas.draw()
        proposed = ax.get_yticklabels()
        drawn = drawn_ticks(ax.yaxis)
        # Precondition: matplotlib really does propose off-view labels here.
        assert len(proposed) > len(drawn)
        assert drawn and all(0.3 <= value <= 9.7 for value, _ in drawn)
    finally:
        plt.close(fig)


def test_drawn_ticks_keeps_ticks_exactly_at_the_limits():
    fig, ax = plt.subplots()
    try:
        ax.set_ylim(0.0, 10.0)
        fig.canvas.draw()
        values = [value for value, _ in drawn_ticks(ax.yaxis)]
        assert values[0] == pytest.approx(0.0) and values[-1] == pytest.approx(10.0)
    finally:
        plt.close(fig)


def test_a_single_panel_reports_no_phantom_y_tick_overflow(spread_table):
    resolved = settle(_single_panel(spread_table))
    figure = render_matplotlib(resolved)
    try:
        figure.canvas.draw()
        ax = figure.axes[0]
        assert len(ax.get_yticklabels()) > len(drawn_ticks(ax.yaxis))  # precondition
    finally:
        plt.close(figure)
    report = layout_report(resolved)
    assert "y_ticks" not in dict(report.overflow_by_element)


def test_the_auto_size_puts_a_single_panel_at_the_top(spread_table):
    """1x1 at 8 x 6 in: nothing collides, so the reference is the top and
    the search costs one layout (plan stage 3)."""
    auto = settle(_single_panel(spread_table)).auto_text
    assert auto.reference == pytest.approx(auto.span[1]), auto.describe()
    assert auto.layouts == 1, auto.describe()
    assert auto.sizes["y_ticks"] == pytest.approx(12.0)


# --- 2. the preview copies the export's y text --------------------------------


def _side_by_side(table, width=4.0, height=3.0):
    """Two panels in one row on DIFFERENT y scales: the right panel keeps its
    own tick numbers and its own y title, drawn into the column gap."""
    spec = _spec(
        ["subject", "muscle"],
        style=StyleOptions(text=TextSizes(base=12.0), width=width, height=height),
    )
    return resolve(spec, table)[0]


def test_the_preview_draws_the_exports_y_ticks_and_title_distance(spread_table):
    resolved = _side_by_side(spread_table)
    decisions = layout_decisions(resolved, width_in=4.0, height_in=3.0)
    y_axes = {(a.row, a.col): a for a in decisions["y_axes"]}
    assert set(y_axes) == {(0, 0), (0, 1)}
    layout = render_plotly(resolved, decisions=decisions, fixed_size_px=(288, 216))["layout"]
    for (row, col), measured in y_axes.items():
        key = "yaxis" if col == 0 else "yaxis2"
        axis = layout[key]
        assert measured.texts is not None
        assert axis["tickmode"] == "array"
        assert axis["tickvals"] == list(measured.values)
        assert [t.replace("<br>", "\n") for t in axis["ticktext"]] == list(measured.texts)
        low, high = sorted(axis["range"])
        assert all(low - 1e-9 <= v <= high + 1e-9 for v in axis["tickvals"])
        assert measured.title_gap_pt is not None and measured.title_gap_pt > 0
        assert axis["title"]["standoff"] == measured.title_gap_pt


def test_an_offset_label_is_not_copied():
    """``5`` with a ``+1.5e3`` corner label cannot be drawn as plain ticks:
    the preview keeps plotly's own values (the export draws what it draws)."""
    rows = [
        {"subject": "01", "trial": trial, "EMG": [1500.0, 1500.0 + 0.001 * k, 1500.002]}
        for k, trial in ((1, "1"), (2, "2"))
    ]
    table = LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "trial"],
        measures=["EMG"],
        name="EMG",
        schema_levels=["subject", "trial"],
    )
    spec = _spec(
        roles={"subject": Role.ITERATE, "trial": Role.GROUP},
        style=StyleOptions(text=TextSizes(base=12.0), width=8.0, height=6.0),
    )
    resolved = resolve(spec, table)[0]
    figure = render_matplotlib(resolved)
    try:
        figure.canvas.draw()
        (measured,) = [a for a in getattr(figure, AXIS_TEXT_ATTR) if a.axis == "y"]
        if not figure.axes[0].yaxis.get_offset_text().get_text():
            pytest.skip("matplotlib drew no offset label for this range")
        assert measured.texts is None
    finally:
        plt.close(figure)
