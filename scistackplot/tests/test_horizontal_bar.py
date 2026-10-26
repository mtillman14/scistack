"""
The horizontal bar kind (``PlotKind.HBAR``; user, 2026-10-26).

HBAR is BAR transposed: ONE data path (``spec.BAR_KINDS``), and orientation
a drawing concern owned by ``spec.is_horizontal``. So these tests hold two
things:

1. the data is the vertical bar's, exactly — same frames, same limits, same
   availability (a horizontal bar can never summarise differently);
2. every renderer — matplotlib, the plotly preview, the exported seaborn
   code — draws it turned: categories down the left (first level at the
   top), values along x, brackets at the left, the overlay swapped.

Difference bars are deliberately absent (``diffbars.HORIZONTAL_REASON``).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.collections import PathCollection  # noqa: E402
from matplotlib.patches import Rectangle  # noqa: E402

from scistackplot import (  # noqa: E402
    Aggregation,
    ErrorBand,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    Shape,
    available_plots,
    generate_plot_function,
    is_horizontal,
    render_matplotlib,
    render_plotly,
    resolve,
)
from scistackplot.capability import why_unavailable  # noqa: E402
from scistackplot.diffbars import HORIZONTAL_REASON, unavailable  # noqa: E402
from scistackplot.render import layout_decisions  # noqa: E402
from scistackplot.render.base import axis_sides, shows_category_labels  # noqa: E402
from scistackplot.render.mpl import LABEL_FIT_ATTR, X_GROUP_LABEL_GID  # noqa: E402
from scistackplot.render.plotly_ import X_GROUP_TAG  # noqa: E402
from scistackplot.resolved import X, Y, Y_HIGH, Y_LOW  # noqa: E402
from scistackplot.spec import BAR_KINDS, HORIZONTAL_KINDS, StyleOptions  # noqa: E402
from scistackplot.temporal import BAR_REASON  # noqa: E402

# subject x session x trial, plus `group` (a subject property). Every value
# distinct, so a bar in the wrong place carries a number that gives it away.
ROWS = [
    ("01", "A", "pre", "1", 1.0),
    ("01", "A", "pre", "2", 3.0),
    ("01", "A", "post", "1", 10.0),
    ("01", "A", "post", "2", 12.0),
    ("02", "A", "pre", "1", 5.0),
    ("02", "A", "post", "1", 20.0),
    ("03", "B", "pre", "1", 7.0),
    ("03", "B", "pre", "2", 8.0),
    ("03", "B", "post", "1", 30.0),
    ("04", "B", "pre", "1", 9.0),
    ("04", "B", "post", "1", 40.0),
    ("04", "B", "post", "2", 44.0),
]


@pytest.fixture
def table() -> LongTable:
    frame = pd.DataFrame(ROWS, columns=["subject", "group", "session", "trial", "M"])
    return LongTable.from_frame(
        frame,
        factors=["subject", "group", "session", "trial"],
        measures=["M"],
        name="M",
        schema_levels=["subject", "session", "trial"],
        level_order={"session": ["pre", "post"]},
    )


@pytest.fixture
def flat() -> LongTable:
    """The same rows without `group`: one figure, for the exported code."""
    frame = pd.DataFrame(ROWS, columns=["subject", "group", "session", "trial", "M"])
    return LongTable.from_frame(
        frame.drop(columns=["group"]),
        factors=["subject", "session", "trial"],
        measures=["M"],
        name="M",
        schema_levels=["subject", "session", "trial"],
        level_order={"session": ["pre", "post"]},
    )


FLAT_ROLES = {"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE}


def _spec(kind=PlotKind.HBAR, **kwargs) -> PlotSpec:
    base = dict(
        measures=["M"],
        roles={
            "subject": Role.COLLAPSE,
            "group": Role.ITERATE,
            "session": Role.GROUP,
            "trial": Role.COLLAPSE,
        },
        groups=["session"],
        kind=kind,
        aggregate=Aggregation(error=ErrorBand.SD),
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _nested(kind=PlotKind.HBAR, **kwargs) -> PlotSpec:
    """Sessions as the ticks, `group` as the brackets around them."""
    return _spec(
        kind,
        roles={
            "subject": Role.COLLAPSE,
            "group": Role.GROUP,
            "session": Role.GROUP,
            "trial": Role.COLLAPSE,
        },
        groups=["session", "group"],
        **kwargs,
    )


def _faceted(kind=PlotKind.HBAR) -> PlotSpec:
    return _spec(
        kind,
        roles={
            "subject": Role.COLLAPSE,
            "group": Role.FACET,
            "session": Role.GROUP,
            "trial": Role.COLLAPSE,
        },
    )


def _first(spec, table):
    return resolve(spec, table)[0]


def _bars(ax) -> list[Rectangle]:
    return [p for p in ax.patches if isinstance(p, Rectangle) and p.get_width() != 0]


def _drawn(figure):
    figure.canvas.draw()
    return figure


# --- the kind and its one owner -----------------------------------------------


def test_hbar_is_a_bar_drawn_horizontally():
    assert PlotKind("hbar") is PlotKind.HBAR
    assert PlotKind.HBAR in BAR_KINDS
    assert HORIZONTAL_KINDS == (PlotKind.HBAR,)
    assert is_horizontal(PlotKind.HBAR)
    assert not is_horizontal(PlotKind.BAR)
    assert not is_horizontal(None)


@pytest.mark.parametrize(
    "roles",
    [
        {"session": Role.GROUP},
        {"session": Role.GROUP, "trial": Role.COLLAPSE},
        {"session": Role.GROUP, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
    ],
)
def test_hbar_is_offered_wherever_bar_is(roles):
    kinds = available_plots(Shape.SCALAR, roles, n_groups=1)
    assert (PlotKind.BAR in kinds) == (PlotKind.HBAR in kinds)
    assert PlotKind.HBAR in kinds


def test_a_datetime_measure_offers_no_horizontal_bar_either():
    roles = {"session": Role.GROUP}
    assert PlotKind.HBAR not in available_plots(Shape.SCALAR, roles, temporal=True)
    assert why_unavailable(PlotKind.HBAR, Shape.SCALAR, roles, temporal=True) == BAR_REASON


def test_difference_bars_are_not_offered_on_a_horizontal_bar():
    assert unavailable(_spec(), Shape.SCALAR) == HORIZONTAL_REASON
    assert unavailable(_spec(PlotKind.BAR), Shape.SCALAR) is None


# --- one data path --------------------------------------------------------------


@pytest.mark.parametrize("make", [_spec, _nested, _faceted])
def test_hbar_resolves_exactly_the_vertical_bars_data(table, make):
    vertical = resolve(make(PlotKind.BAR), table)
    horizontal = resolve(make(PlotKind.HBAR), table)
    assert len(vertical) == len(horizontal)
    for v, h in zip(vertical, horizontal):
        assert h.x_order == v.x_order
        assert h.y_limits == v.y_limits
        assert h.encoding.y_low == Y_LOW and h.encoding.y_high == Y_HIGH
        assert [p.title for p in h.panels] == [p.title for p in v.panels]
        for vp, hp in zip(v.panels, h.panels):
            pd.testing.assert_frame_equal(
                hp.frame.reset_index(drop=True), vp.frame.reset_index(drop=True)
            )
            assert hp.y_limits == vp.y_limits


# --- matplotlib -----------------------------------------------------------------


def test_mpl_draws_one_horizontal_bar_per_level_first_at_the_top(table):
    resolved = _first(_spec(), table)
    figure = _drawn(render_matplotlib(resolved))
    try:
        ax = figure.axes[0]
        frame = resolved.panels[0].frame
        order = [str(v) for v in resolved.x_order]
        expected = {order.index(str(x)): y for x, y in zip(frame[X], frame[Y])}
        bars = _bars(ax)
        assert len(bars) == len(expected)
        for bar in bars:
            position = bar.get_y() + bar.get_height() / 2.0
            assert bar.get_x() == pytest.approx(0.0)
            assert bar.get_width() == pytest.approx(expected[round(position)])
        # Inverted: the first level (position 0) is at the top.
        low, high = ax.get_ylim()
        assert low > high
        assert [t.get_text() for t in ax.get_yticklabels()] == ["pre", "post"]
        # The value range is the logical y range, drawn along x.
        assert ax.get_xlim() == pytest.approx(resolved.panels[0].y_limits or resolved.y_limits)
    finally:
        plt.close(figure)


def test_mpl_error_bars_run_along_x(table):
    figure = _drawn(render_matplotlib(_first(_spec(), table)))
    try:
        # barh(xerr=...) registers a BarContainer AND its ErrorbarContainer.
        (container,) = [
            c for c in figure.axes[0].containers if getattr(c, "errorbar", None)
        ]
        segments = [
            segment
            for collection in container.errorbar.lines[2]
            for segment in collection.get_segments()
        ]
        assert segments
        for (x0, y0), (x1, y1) in segments:
            assert y0 == pytest.approx(y1)
            assert x0 != pytest.approx(x1)
    finally:
        plt.close(figure)


def test_mpl_titles_categories_left_values_below(table):
    resolved = _first(_spec(style=StyleOptions(x_label="Session", y_label="Step")), table)
    figure = _drawn(render_matplotlib(resolved))
    try:
        ax = figure.axes[0]
        assert ax.get_ylabel() == "Session"
        assert ax.get_xlabel() == "Step"
    finally:
        plt.close(figure)


def test_mpl_log_value_axis_is_x(table):
    figure = _drawn(render_matplotlib(_first(_spec(style=StyleOptions(log_y=True)), table)))
    try:
        ax = figure.axes[0]
        assert ax.get_xscale() == "log"
        assert ax.get_yscale() == "linear"
    finally:
        plt.close(figure)


def test_mpl_facet_names_are_the_value_titles_under_each_panel(table):
    resolved = _first(_faceted(), table)
    figure = _drawn(render_matplotlib(resolved))
    try:
        visible = [ax for ax in figure.axes if ax.get_visible()]
        assert len(visible) == 2
        assert sorted(ax.get_xlabel() for ax in visible) == ["A", "B"]
        # The category labels only where nothing sits to the left.
        for panel, ax in zip(resolved.panels, visible):
            labelled = shows_category_labels(resolved, panel.grid_row, panel.grid_col)
            shown = [t.get_text() for t in ax.get_yticklabels() if t.get_visible() and t.get_text()]
            assert bool(shown) == labelled
    finally:
        plt.close(figure)


def test_mpl_nested_layers_are_brackets_at_the_left(table):
    resolved = _first(_nested(), table)
    assert resolved.x_plan and resolved.x_plan.groups
    figure = _drawn(render_matplotlib(resolved))
    try:
        ax = figure.axes[0]
        renderer = figure.canvas.get_renderer()
        brackets = [t for t in ax.texts if t.get_gid() == X_GROUP_LABEL_GID]
        assert sorted(t.get_text() for t in brackets) == ["A", "B"]
        axes_left = ax.get_window_extent(renderer).x0
        tick_lefts = [t.get_window_extent(renderer).x0 for t in ax.get_yticklabels() if t.get_text()]
        for text in brackets:
            box = text.get_window_extent(renderer)
            assert text.get_ha() == "right"
            # Left of the axes AND clear of the tick labels.
            assert box.x1 < axes_left
            assert box.x1 <= min(tick_lefts) + 0.5
        fits = getattr(figure, LABEL_FIT_ATTR)
        assert fits["ticks"] is not None and fits["brackets"] is not None
        assert fits["bracket_geometry"].tick_depth_pt > 0
    finally:
        plt.close(figure)


def test_mpl_overlay_points_sit_in_their_bars(table):
    resolved = _first(_spec(show_sample=["subject"]), table)
    figure = _drawn(render_matplotlib(resolved))
    try:
        ax = figure.axes[0]
        points = np.vstack(
            [c.get_offsets() for c in ax.collections if isinstance(c, PathCollection)]
            + [np.column_stack(line.get_data()) for line in ax.get_lines() if line.get_marker() == "o"]
        )
        sample = resolved.panels[0].sample
        assert len(points) >= len(sample)
        values = sorted(round(float(v), 9) for v in sample[Y])
        # x = the value, y = within half a slot of the bar it belongs to.
        assert set(round(float(x), 9) for x in points[:, 0]) <= set(values)
        assert all(-0.5 <= y <= len(resolved.x_order) - 0.5 for y in points[:, 1])
    finally:
        plt.close(figure)


# --- the plotly preview ----------------------------------------------------------


def test_plotly_bars_are_horizontal_on_a_reversed_positional_axis(table):
    resolved = _first(_spec(), table)
    payload = render_plotly(resolved)
    bars = [t for t in payload["data"] if t["type"] == "bar"]
    assert bars
    frame = resolved.panels[0].frame
    for trace in bars:
        assert trace["orientation"] == "h"
        assert "error_x" in trace and "error_y" not in trace
        assert sorted(trace["x"]) == sorted(float(v) for v in frame[Y])
        assert all(isinstance(v, float) for v in trace["y"])
    y_axis = payload["layout"]["yaxis"]
    n = len(resolved.x_order)
    assert y_axis["tickvals"] == list(range(n))
    assert y_axis["range"] == [n - 0.5, -0.5]
    x_axis = payload["layout"]["xaxis"]
    assert x_axis["range"] == pytest.approx(list(resolved.panels[0].y_limits or resolved.y_limits))
    assert payload["layout"]["meta"]["axis_sides"] == {"x": "left", "y": "bottom"}
    assert payload["layout"]["meta"]["difference_bars"] is None


def test_plotly_value_hover_reads_x(table):
    payload = render_plotly(_first(_spec(show_sample=["subject"]), table))
    templates = [t.get("hovertemplate", "") for t in payload["data"] if t.get("hovertemplate")]
    assert templates
    assert all("%{x}" in t and "%{y}" not in t for t in templates)


def test_plotly_overlay_is_swapped(table):
    resolved = _first(_spec(show_sample=["subject"]), table)
    payload = render_plotly(resolved)
    overlay = [t for t in payload["data"] if t["type"] == "scatter"]
    assert overlay
    values = {round(float(v), 9) for v in resolved.panels[0].sample[Y]}
    for trace in overlay:
        assert {round(float(v), 9) for v in trace["x"]} <= values
        assert all(-0.5 <= v <= len(resolved.x_order) - 0.5 for v in trace["y"])


def test_plotly_brackets_hang_left_of_the_cell(table):
    payload = render_plotly(_first(_nested(), table))
    notes = [a for a in payload["layout"]["annotations"] if a.get("name", "").startswith(X_GROUP_TAG)]
    assert sorted(a["text"] for a in notes) == ["A", "B"]
    for note in notes:
        assert note["xref"] == "paper" and note["yref"] == "y"
        assert note["xanchor"] == "right"
        assert note["xshift"] < 0
    rules = [s for s in payload["layout"].get("shapes", []) if s.get("name", "").startswith(X_GROUP_TAG)]
    assert rules
    for rule in rules:
        assert rule["xsizemode"] == "pixel"
        assert rule["x0"] == rule["x1"] < 0
        assert rule["y0"] < rule["y1"]


def test_plotly_takes_the_exports_category_fit_on_y(table):
    resolved = _first(_spec(), table)
    decisions = layout_decisions(resolved)
    fit = decisions["ticks"]
    assert fit is not None
    payload = render_plotly(resolved, decisions=decisions)
    y_axis = payload["layout"]["yaxis"]
    assert y_axis["ticktext"] == list(fit.rows[0])
    assert y_axis["tickfont"]["size"] == fit.font_pt
    # The bottom axis is numeric; the decision never lands there.
    assert "tickangle" not in y_axis or y_axis["tickangle"] == 0


def test_axis_sides_owner(table):
    assert axis_sides(_first(_spec(PlotKind.BAR), table)) == {"x": "bottom", "y": "left"}
    assert axis_sides(_first(_spec(), table)) == {"x": "left", "y": "bottom"}


# --- the exported code -------------------------------------------------------------


def _run(source: str, frame, function_name: str):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace[function_name](frame.copy(), "figure.png")


def test_generated_code_draws_the_previewed_horizontal_bars(flat):
    pytest.importorskip("seaborn")
    spec = _spec(roles=FLAT_ROLES)
    source = generate_plot_function(spec, flat, function_name="plot_h")
    assert 'orient="h"' in source
    assert "x='M'" in source
    figure = _run(source, flat.frame, "plot_h")
    try:
        (preview,) = resolve(spec, flat)
        ax = figure.axes[0]
        figure.canvas.draw()
        labels = [t.get_text() for t in ax.get_yticklabels()]
        assert labels == [preview.text.x_tick(v) for v in preview.x_order]
        by_level = dict(zip((str(v) for v in preview.panels[0].frame[X]), preview.panels[0].frame[Y]))
        drawn = sorted(
            (bar.get_y() + bar.get_height() / 2.0, bar.get_width()) for bar in _bars(ax)
        )
        assert [w for _, w in drawn] == pytest.approx([by_level[str(v)] for v in preview.x_order])
        # The value axis is x; the categories read top-down.
        low, high = ax.get_ylim()
        assert low > high
    finally:
        plt.close(figure)


@pytest.mark.parametrize(
    "extra",
    [
        dict(show_sample=["subject"]),
        dict(style=StyleOptions(log_y=True)),
    ],
)
def test_generated_horizontal_code_runs(flat, extra):
    pytest.importorskip("seaborn")
    spec = _spec(roles=FLAT_ROLES, **extra)
    source = generate_plot_function(spec, flat, function_name="plot_h")
    figure = _run(source, flat.frame, "plot_h")
    try:
        ax = figure.axes[0]
        if spec.style.log_y:
            assert ax.get_xscale() == "log"
        if spec.show_sample:
            assert any(line.get_marker() == "o" for line in ax.get_lines()) or any(
                isinstance(c, PathCollection) for c in ax.collections
            )
    finally:
        plt.close(figure)


def test_generated_faceted_code_names_panels_on_x(table):
    pytest.importorskip("seaborn")
    spec = _faceted()
    source = generate_plot_function(spec, table, function_name="plot_h")
    assert "sharey=False" not in source
    assert "_ax.set_xlabel(" in source
    figure = _run(source, table.frame, "plot_h")
    try:
        visible = [ax for ax in figure.axes if ax.get_visible()]
        assert sorted(ax.get_xlabel() for ax in visible) == ["A", "B"]
    finally:
        plt.close(figure)
