"""The auto size's measured rules on DRAWN text (render.mpl; plan stage 2, 2026-10-14),
and the preview's copy of the export's numeric x ticks (stage 4).

Rules measured by ``mpl._tick_and_title_rules``:

1. numeric x and y drawn tick labels must not collide;
3. every panel shows at least ``MIN_TICK_LABELS`` y tick labels (too tall);
4. every numeric x axis shows at least that many x tick labels (too wide);
5. a y title is no taller than its panel.

The raw-matplotlib tests pin each measurement on a figure built to break it;
the spec tests check the rules reach ``layout_report`` and the preview.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import MaxNLocator  # noqa: E402

from scistackplot import (  # noqa: E402
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    TextSizes,
    layout_decisions,
    render_plotly,
    resolve,
)
from scistackplot.autosize import MIN_TICK_LABELS, settle  # noqa: E402
from scistackplot.render.mpl import (  # noqa: E402
    _tick_and_title_rules,
    is_numeric_axis,
    layout_report,
)

# --- the measurements, on raw figures ----------------------------------------


def test_numeric_x_labels_that_overlap_are_a_collision():
    fig, ax = plt.subplots(figsize=(2.0, 2.0))
    try:
        ax.set_xlim(100000, 900000)
        ax.xaxis.set_major_locator(MaxNLocator(nbins=20))
        ax.ticklabel_format(axis="x", style="plain", useOffset=False)
        ax.tick_params(labelsize=12)
        fig.canvas.draw()
        rules = _tick_and_title_rules(fig)
        assert rules["x_tick_collisions"] == ("(0,0)",)
        assert rules["y_tick_collisions"] == ()
    finally:
        plt.close(fig)


def test_well_spaced_labels_do_not_collide():
    fig, ax = plt.subplots(figsize=(8.0, 6.0))
    try:
        ax.set_xlim(0, 10)
        ax.set_ylim(0, 10)
        fig.canvas.draw()
        rules = _tick_and_title_rules(fig)
        assert rules["x_tick_collisions"] == () and rules["y_tick_collisions"] == ()
        assert rules["fewest_y_ticks"][1] >= MIN_TICK_LABELS
        assert rules["fewest_x_ticks"][1] >= MIN_TICK_LABELS
        assert rules["y_title_overruns"] == ()
    finally:
        plt.close(fig)


def test_short_panels_at_a_big_size_show_too_few_y_ticks():
    """5x1 short panels: matplotlib's locator keeps fewer ticks as the font
    grows (``get_tick_space``), so at 20 pt each panel shows < 3."""
    fig, axes = plt.subplots(5, 1, figsize=(4.0, 2.5))
    try:
        for ax in axes:
            ax.set_ylim(0, 1)
            ax.tick_params(labelsize=20)
        fig.canvas.draw()
        panel, count = _tick_and_title_rules(fig)["fewest_y_ticks"]
        assert count < MIN_TICK_LABELS, (panel, count)
    finally:
        plt.close(fig)


def test_hidden_shared_labels_are_not_counted():
    """A shared y axis right of the first column draws no numbers by design."""
    fig, axes = plt.subplots(1, 2, figsize=(6.0, 4.0), sharey=True)
    try:
        axes[1].tick_params(labelleft=False)
        fig.canvas.draw()
        panel, count = _tick_and_title_rules(fig)["fewest_y_ticks"]
        assert panel == "(0,0)" and count >= MIN_TICK_LABELS
    finally:
        plt.close(fig)


def test_a_y_title_longer_than_its_panel_is_an_overrun():
    fig, ax = plt.subplots(figsize=(4.0, 1.2))
    try:
        ax.set_ylabel("tibialis anterior · left · follow-up session")
        fig.canvas.draw()
        ((panel, over),) = _tick_and_title_rules(fig)["y_title_overruns"]
        assert panel == "(0,0)" and over > 0
    finally:
        plt.close(fig)


def test_categorical_axes_are_left_to_the_tick_fit():
    fig, ax = plt.subplots()
    try:
        ax.set_xticks(range(3))
        ax.set_xticklabels(["a", "b", "c"])
        assert not is_numeric_axis(ax.xaxis)
        assert is_numeric_axis(ax.yaxis)
        fig.canvas.draw()
        assert _tick_and_title_rules(fig)["fewest_x_ticks"] is None
    finally:
        plt.close(fig)


# --- through a spec ----------------------------------------------------------


@pytest.fixture
def wide_x_table() -> LongTable:
    """A scatter whose x values print wide (``250000``)."""
    rng = np.random.default_rng(3)
    rows = [
        {
            "subject": f"S{s}",
            "trial": str(t),
            "Work": float(rng.uniform(120000, 980000)),
            "Peak": float(rng.normal(1.0, 0.2)),
        }
        for s in range(3)
        for t in range(6)
    ]
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "trial"],
        measures=["Peak", "Work"],
        name="Peak",
        schema_levels=["subject", "trial"],
    )


def _scatter(width: float, height: float, text: TextSizes | None = None) -> PlotSpec:
    return PlotSpec(
        measures=["Peak"],
        x_measure="Work",
        roles={"subject": Role.GROUP, "trial": Role.GROUP},
        kind=PlotKind.SCATTER,
        style=StyleOptions(width=width, height=height, text=text or TextSizes()),
    )


def test_layout_report_carries_the_new_rules(wide_x_table):
    (resolved,) = resolve(_scatter(8.0, 6.0, TextSizes(base=12.0)), wide_x_table)
    report = layout_report(resolved)
    assert report.fewest_y_ticks is not None and report.fewest_x_ticks is not None
    assert report.fewest_y_ticks[1] >= MIN_TICK_LABELS


def test_a_narrow_numeric_x_scatter_settles_without_colliding_labels(wide_x_table):
    """Wide x numbers on a narrow figure: whatever reference is chosen, its
    layout has no colliding x labels unless the reference is the bottom."""
    (resolved,) = resolve(_scatter(3.0, 3.0), wide_x_table)
    auto = settle(resolved).auto_text
    assert auto.layouts >= 1
    if auto.at_bottom and auto.failing:
        assert "still failing" in auto.describe()
    else:
        assert auto.failing == (), auto.describe()


def test_the_preview_draws_the_exports_numeric_x_ticks(wide_x_table):
    (resolved,) = resolve(_scatter(5.0, 4.0), wide_x_table)
    decisions = layout_decisions(resolved, width_in=5.0, height_in=4.0)
    (measured,) = decisions["x_axes"]
    assert measured.axis == "x" and measured.texts is not None
    layout = render_plotly(resolved, decisions=decisions, fixed_size_px=(360, 288))["layout"]
    axis = layout["xaxis"]
    assert axis["tickmode"] == "array"
    assert axis["tickvals"] == list(measured.values)
    assert [t.replace("<br>", "\n") for t in axis["ticktext"]] == list(measured.texts)
    # And the sizes drawn are the export's.
    meta = layout["meta"]["text_sizes"]
    assert meta["x_ticks"] == pytest.approx(decisions["auto_text"].sizes["x_ticks"])
