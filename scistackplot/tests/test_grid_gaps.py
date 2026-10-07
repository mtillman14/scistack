"""
Text between two panels has a size in POINTS, so the room for it must too.

The preview used to size its cell gaps as fixed fractions of the figure
(``plotly_.X_GAP`` / ``Y_GAP``). On a narrow figure, or once the right panel
kept its own y numbers (per-panel scales), the right panel's y title ran into
the panel on its left; 90 degree tick labels on an inner row reached the row
below. The export measures how far each panel's text reaches outside its axes
(``base.GridReach``, ``mpl._grid_reach``) and the preview sizes its margins and
gaps from that (``plotly_._frame``), 1 pt = 1 px.
"""

from __future__ import annotations

import pandas as pd
import pytest

matplotlib = pytest.importorskip("matplotlib")
import matplotlib.pyplot as plt  # noqa: E402

from scistackplot import (  # noqa: E402
    FacetOptions,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    TextSizes,
    layout_decisions,
    render_matplotlib,
    render_plotly,
    resolve,
)

#: The panel-spacing measurements here were written at the old fixed 14 pt
#: default; the automatic size is tested in test_autosize.py.
FIXED_14 = TextSizes(base=14.0)
from scistackplot.render.mpl import GRID_REACH_ATTR  # noqa: E402
from scistackplot.render.plotly_ import X_GAP, X_GROUP_TAG  # noqa: E402
from test_ylimits import _spec, spread_table  # noqa: E402,F401

#: Narrow enough that a fraction-of-the-width gap was too small.
NARROW = (4.0, 3.0)


def _side_by_side(table, width, height):
    """Two panels in one row on DIFFERENT y scales, so the right panel keeps
    its own tick numbers as well as its y title."""
    spec = _spec(["subject", "muscle"], style=StyleOptions(text=FIXED_14, width=width, height=height))
    return resolve(spec, table)


def _preview(resolved, width, height):
    decisions = layout_decisions(resolved, width_in=width, height_in=height)
    size = (width * 72.0, height * 72.0)
    return decisions, render_plotly(resolved, decisions=decisions, fixed_size_px=size)["layout"]


def _plot_px(layout, size):
    margin = layout["margin"]
    return (
        size[0] - margin["l"] - margin["r"],
        size[1] - margin["t"] - margin["b"],
    )


def test_the_export_keeps_the_right_panels_y_labels_off_the_left_panel(spread_table):
    for resolved in _side_by_side(spread_table, *NARROW):
        assert (resolved.grid_rows, resolved.grid_cols) == (1, 2)
        figure = render_matplotlib(resolved)
        try:
            figure.canvas.draw()
            renderer = figure.canvas.get_renderer()
            left, right = figure.axes[0], figure.axes[1]
            assert right.get_ylabel()
            reach = right.get_tightbbox(renderer).x0
            assert reach >= left.get_window_extent(renderer).x1 - 1
            measured = getattr(figure, GRID_REACH_ATTR)
            assert measured.left_inner_pt > 0
        finally:
            plt.close(figure)


def test_the_preview_sizes_the_column_gap_from_the_measured_reach(spread_table):
    for resolved in _side_by_side(spread_table, *NARROW):
        decisions, layout = _preview(resolved, *NARROW)
        reach = decisions["grid_reach"]
        plot_w, _ = _plot_px(layout, (NARROW[0] * 72.0, NARROW[1] * 72.0))
        gap_px = (layout["xaxis2"]["domain"][0] - layout["xaxis"]["domain"][1]) * plot_w
        assert gap_px >= reach.left_inner_pt
        # The case that overlapped: the old fraction was too little here.
        assert X_GAP * plot_w < reach.left_inner_pt


def test_the_preview_left_margin_holds_the_first_columns_labels(spread_table):
    for resolved in _side_by_side(spread_table, *NARROW):
        decisions, layout = _preview(resolved, *NARROW)
        assert layout["margin"]["l"] >= decisions["grid_reach"].left_outer_pt


def _wrapped_nested_grid(rotation):
    """3 panels in 2 columns: (0,1) has an empty cell below it, so its tick
    labels and brackets hang into the gap between the two rows."""
    rows = [
        {"group": g, "session": s, "site": site, "subject": p, "StepLength": 1.0}
        for g in ["stim", "sham"]
        for s in ["pre-intervention", "post-intervention"]
        for site in ["A", "B", "C"]
        for p in ["01", "02"]
    ]
    table = LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["group", "session", "site", "subject"],
        measures=["StepLength"],
        schema_levels=["subject"],
    )
    spec = PlotSpec(
        measures=["StepLength"],
        roles={
            "group": Role.GROUP,
            "session": Role.GROUP,
            "site": Role.FACET,
            "subject": Role.COLLAPSE,
        },
        groups=["group", "session"],
        kind=PlotKind.BOX,
        facet=FacetOptions(n_cols=2),
        style=StyleOptions(text=FIXED_14, width=8.0, height=8.0, tick_rotation=rotation),
    )
    return resolve(spec, table)[0]


def test_rotated_ticks_on_an_inner_row_stay_off_the_row_below():
    resolved = _wrapped_nested_grid(90)
    decisions, layout = _preview(resolved, 8.0, 8.0)
    reach = decisions["grid_reach"]
    upright = layout_decisions(_wrapped_nested_grid(0), width_in=8.0, height_in=8.0)
    assert reach.below_inner_pt > upright["grid_reach"].below_inner_pt

    _, plot_h = _plot_px(layout, (8.0 * 72.0, 8.0 * 72.0))
    # slots: 1=(0,0) 2=(0,1) 3=(1,0).
    gap_px = (layout["yaxis2"]["domain"][0] - layout["yaxis3"]["domain"][1]) * plot_h
    assert gap_px >= reach.below_inner_pt
    # And the brackets drawn there: bottom of the lowest label above row 1.
    geometry = decisions["bracket_geometry"]
    lowest = min(
        a["y"] * plot_h + a["yshift"] - geometry.row_height_pt
        for a in layout["annotations"]
        if a["xref"] == "x2" and str(a.get("name", "")).startswith(X_GROUP_TAG)
    )
    assert lowest >= layout["yaxis3"]["domain"][1] * plot_h


# --- hidden y titles give their room back (per-panel overrides, D5/D6) ------------

from scistackplot.panels import Y_TITLES_FIRST_COLUMN, PanelOverride  # noqa: E402

WIDE = (9.0, 3.0)


def _shared_row(*overrides, **style):
    """1 x 3 on ONE scale: the inner panels carry a y title and no tick numbers,
    so their title is what the column gap is sized for."""
    rows = [
        {"subject": s, "muscle": m, "trial": t, "EMG": [0.0, 1.0, 2.0]}
        for s in ("01", "02")
        for m in ("TA", "Tibialis posterior long", "S")
        for t in ("1", "2")
    ]
    table = LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "muscle", "trial"],
        measures=["EMG"],
        name="EMG",
        schema_levels=["subject", "muscle", "trial"],
        level_order={"muscle": ["TA", "Tibialis posterior long", "S"]},
    )
    spec = PlotSpec(
        measures=["EMG"],
        roles={"subject": Role.GROUP, "muscle": Role.FACET, "trial": Role.GROUP},
        kind=PlotKind.LINE,
        style=StyleOptions(text=FIXED_14, width=WIDE[0], height=WIDE[1], **style),
        panel_overrides=list(overrides),
    )
    (resolved,) = resolve(spec, table)
    assert (resolved.grid_rows, resolved.grid_cols) == (1, 3)
    return resolved


def _reach(resolved):
    figure = render_matplotlib(resolved)
    try:
        return getattr(figure, GRID_REACH_ATTR)
    finally:
        plt.close(figure)


def _inner_gap_px(resolved):
    _, layout = _preview(resolved, *WIDE)
    plot_w, _ = _plot_px(layout, (WIDE[0] * 72.0, WIDE[1] * 72.0))
    return (layout["xaxis2"]["domain"][0] - layout["xaxis"]["domain"][1]) * plot_w


def test_hiding_the_inner_titles_shrinks_the_column_gap_and_showing_restores_it():
    shown = _shared_row()
    hidden = _shared_row(y_titles=Y_TITLES_FIRST_COLUMN)
    again = _shared_row()

    assert _reach(hidden).left_inner_pt < _reach(shown).left_inner_pt
    assert _inner_gap_px(hidden) < _inner_gap_px(shown)
    # Exactly back: the gap is measured, never remembered.
    assert _reach(again).left_inner_pt == _reach(shown).left_inner_pt
    assert _inner_gap_px(again) == _inner_gap_px(shown)


def test_hiding_the_first_columns_title_shrinks_the_left_margin():
    shown = _shared_row()
    hidden = _shared_row(PanelOverride(match={"muscle": "TA"}, y_label_hidden=True))

    assert _reach(hidden).left_outer_pt < _reach(shown).left_outer_pt
    _, shown_layout = _preview(shown, *WIDE)
    _, hidden_layout = _preview(hidden, *WIDE)
    assert hidden_layout["margin"]["l"] <= shown_layout["margin"]["l"]


def test_hiding_one_inner_title_frees_nothing_while_another_shows():
    """One gap width for the whole grid, and a y title is rotated: its
    horizontal room is one line of text whatever its length. So while ANY
    inner panel still shows its title, the gap stays."""
    shown = _shared_row()
    narrow_hidden = _shared_row(PanelOverride(match={"muscle": "S"}, y_label_hidden=True))

    assert _reach(narrow_hidden).left_inner_pt == _reach(shown).left_inner_pt

