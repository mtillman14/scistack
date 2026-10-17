"""
Difference bars, Stage 3 (2026-09-27): the matplotlib export draws them.
``mpl._draw_difference_bars`` measures each panel and label, places with
``diffbars.place_figure`` and draws. These tests read the DRAWN artists back
in display coordinates, so they check what the file shows rather than what
placement intended.
"""

from __future__ import annotations

import logging

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pytest  # noqa: E402
from matplotlib.collections import PathCollection  # noqa: E402
from matplotlib.markers import MarkerStyle  # noqa: E402

import scistackplot.reduce as reduce_mod  # noqa: E402
from scistackplot import (  # noqa: E402
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    YAxis,
    resolve,
)
from scistackplot.diffbars import FOOT_GAP_PT, GAP_PT, LINE_PT, DifferenceBar  # noqa: E402
from scistackplot.render.mpl import (  # noqa: E402
    DIFF_BAR_GID,
    DIFF_BARS_ATTR,
    DIFF_LABEL_GID,
    GRID_REACH_ATTR,
)
from scistackplot.render.mpl import render as render_mpl  # noqa: E402

LAYER = "scistackplot"
#: Display-space slack: antialiasing and a cap's half-thickness.
SLACK_PX = 1.0


@pytest.fixture(autouse=True)
def _clear_plan_cache():
    reduce_mod.clear_plan_cache()
    yield
    reduce_mod.clear_plan_cache()
    plt.close("all")


@pytest.fixture
def gait_table() -> LongTable:
    rows = []
    for subject in ("01", "02", "03", "04"):
        for side in ("L", "R"):
            for number, session in enumerate(("pre", "post", "follow", "late")):
                rows.append(
                    {
                        "subject": subject,
                        "side": side,
                        "session": session,
                        # post is the tall one, so a pre–follow bar must clear it.
                        "Step": 2.0
                        + (4.0 if session == "post" else number * 0.5)
                        + int(subject) * 0.3,
                    }
                )
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "side", "session"],
        measures=["Step"],
        name="Step",
        schema_levels=["subject", "side", "session"],
        level_order={"session": ["pre", "post", "follow", "late"]},
    )


def _bar(side: str, a: str, b: str, label: str = "*") -> DifferenceBar:
    return DifferenceBar(match={"side": side}, a={"session": a}, b={"session": b}, label=label)


BARS = (
    _bar("R", "pre", "post"),
    _bar("R", "post", "follow", "**"),
    _bar("R", "pre", "follow"),
    _bar("R", "follow", "late", "n.s."),
)


def _spec(*bars: DifferenceBar, kind: PlotKind = PlotKind.BAR, **extra) -> PlotSpec:
    return PlotSpec(
        measures=["Step"],
        kind=kind,
        roles={"side": Role.FACET, "session": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["session"],
        difference_bars=list(bars),
        style=extra.pop("style", StyleOptions(width=6.0, height=4.0)),
        **extra,
    )


def _render(spec: PlotSpec, table: LongTable):
    figure = resolve(spec, table)[0]
    fig = render_mpl(figure)
    fig.canvas.draw()
    return figure, fig


def _axes_for(fig, figure, side: str):
    index = next(i for i, p in enumerate(figure.panels) if p.key["side"] == side)
    # Axes in grid order; panels are placed by grid_row/grid_col.
    panel = figure.panels[index]
    visible = [ax for ax in fig.axes if ax.get_visible()]
    for ax in visible:
        spec = ax.get_subplotspec()
        if spec.rowspan.start == panel.grid_row and spec.colspan.start == panel.grid_col:
            return index, ax
    raise AssertionError(f"no axes for panel {side}")


def _mark_points(ax) -> np.ndarray:
    """Every drawn mark vertex/centre in display pixels, bars excluded."""
    points: list[np.ndarray] = []
    for line in ax.lines:
        if line.get_gid() == DIFF_BAR_GID:
            continue
        xy = np.column_stack(line.get_data()).astype(float)
        if len(xy):
            points.append(ax.transData.transform(xy))
    for patch in ax.patches:
        points.append(patch.get_transform().transform(patch.get_path().vertices))
    for collection in ax.collections:
        if isinstance(collection, PathCollection):
            offsets = collection.get_offsets()
            if len(offsets):
                points.append(collection.get_offset_transform().transform(offsets))
            continue
        transform = collection.get_transform()
        for path in collection.get_paths():
            if len(path.vertices):
                points.append(transform.transform(path.vertices))
    found = np.vstack(points) if points else np.zeros((0, 2))
    return found[~np.isnan(found).any(axis=1)]


def _check_clearance(fig, ax, placed) -> list[str]:
    """Every mark point under a bar's span is at least the gap below its line."""
    px_per_pt = fig.dpi / 72.0
    marks = _mark_points(ax)
    problems = []
    for bar in placed:
        (left_px, line_px), (right_px, _) = ax.transData.transform(
            [(bar.left, bar.y), (bar.right, bar.y)]
        )
        under = marks[(marks[:, 0] >= left_px - SLACK_PX) & (marks[:, 0] <= right_px + SLACK_PX)]
        if not len(under):
            continue
        highest = under[:, 1].max()
        room = line_px - highest
        if room < GAP_PT * px_per_pt - SLACK_PX:
            problems.append(
                f"{bar.bar.describe()}: line only {room / px_per_pt:.2f} pt above a mark"
            )
    return problems


def _marker_half_heights_px(marker, size_pt: np.ndarray, edge_pt: float, px_per_pt: float):
    """How far a marker reaches above its centre (and sideways), in px: from
    its real path, so a flat cap (``"_"``) adds only its edge width, not a
    disc's radius. ``size_pt`` is the marker's size (a Line2D markersize; for
    a scatter, the side of its area)."""
    style = MarkerStyle(marker)
    vertices = style.get_path().transformed(style.get_transform()).vertices
    up = float(np.abs(vertices[:, 1]).max()) if len(vertices) else 0.0
    side = float(np.abs(vertices[:, 0]).max()) if len(vertices) else 0.0
    edge = edge_pt / 2 * px_per_pt
    return up * size_pt * px_per_pt + edge, side * size_pt * px_per_pt + edge


def _column_tops(ax, x_px: float, px_per_pt: float) -> list[tuple[float, str]]:
    """The top (display px) of every drawn thing in the column a leg at
    *x_px* occupies, bars excluded, each with what drew it: where each
    segment or edge crosses that x (a line adds half its width), and each
    marker whose real extent reaches it. Sideways neighbours — sample points
    at their offsets, a jittered strip — are not under the leg."""
    tops: list[tuple[float, str]] = []
    # The leg is a line: its column is its width, not its centre.
    half_leg_px = LINE_PT / 2 * px_per_pt

    def crossings(xy: np.ndarray, what: str, half_width_px: float = 0.0) -> None:
        xy = np.asarray(xy, dtype=float)
        for (x0, y0), (x1, y1) in zip(xy[:-1], xy[1:]):
            if not np.isfinite([x0, y0, x1, y1]).all():
                continue
            reach = SLACK_PX + half_leg_px
            if not (min(x0, x1) - reach <= x_px <= max(x0, x1) + reach):
                continue
            if abs(x1 - x0) < 1e-9:
                y = max(y0, y1)
            else:
                t = min(max((x_px - x0) / (x1 - x0), 0.0), 1.0)
                y = y0 + t * (y1 - y0)
            tops.append((y + half_width_px, what))

    def markers(centres, up_px, side_px, what: str) -> None:
        for (mx, my), up, side in zip(centres, up_px, side_px):
            if np.isfinite([mx, my]).all() and abs(mx - x_px) <= side + half_leg_px + SLACK_PX:
                tops.append((my + up, what))

    for line in ax.lines:
        if line.get_gid() == DIFF_BAR_GID:
            continue
        name = f"line {line.get_label()!r}"
        xy = ax.transData.transform(np.column_stack(line.get_data()).astype(float))
        if line.get_linestyle() not in ("None", "none", "", " "):
            crossings(xy, name, line.get_linewidth() * px_per_pt / 2)
        marker = line.get_marker()
        if marker not in (None, "None", "none", "", " ") and line.get_markersize():
            up, side = _marker_half_heights_px(
                marker,
                np.asarray(line.get_markersize(), float),
                line.get_markeredgewidth(),
                px_per_pt,
            )
            n = len(xy)
            markers(xy, np.full(n, up), np.full(n, side), f"{name} marker {marker!r}")
    # Path.to_polygons(transform) applies only a transform's AFFINE part, so
    # on a log axis it misplaces every vertex: transform the vertices here.
    for patch in ax.patches:
        for polygon in patch.get_path().to_polygons():
            crossings(patch.get_transform().transform(polygon), type(patch).__name__)
    for collection in ax.collections:
        name = type(collection).__name__
        if isinstance(collection, PathCollection):
            centres = collection.get_offset_transform().transform(collection.get_offsets())
            sides = np.sqrt(
                np.broadcast_to(np.asarray(collection.get_sizes(), float), (len(centres),))
            )
            widths = collection.get_linewidths()
            edge = float(widths[0]) if len(widths) else 0.0
            path = collection.get_paths()[0] if collection.get_paths() else None
            vertices = path.vertices if path is not None else np.zeros((0, 2))
            up_unit = float(np.abs(vertices[:, 1]).max()) if len(vertices) else 0.5
            side_unit = float(np.abs(vertices[:, 0]).max()) if len(vertices) else 0.5
            ups = up_unit * sides * px_per_pt + edge / 2 * px_per_pt
            reach = side_unit * sides * px_per_pt + edge / 2 * px_per_pt
            markers(centres, ups, reach, f"{name} marker")
            continue
        transform = collection.get_transform()
        for path in collection.get_paths():
            for polyline in path.to_polygons(closed_only=False):
                crossings(transform.transform(polyline), name)
    return tops


def _check_feet(fig, ax, placed) -> list[str]:
    """Every leg's foot is at least FOOT_GAP_PT above whatever is drawn in
    the leg's own column, so a leg never reads as touching the data. Each
    failure names the artist, so a miss in ``diffbars.panel_obstacles`` shows
    which mark it forgot."""
    px_per_pt = fig.dpi / 72.0
    problems = []
    for bar in placed:
        for x, foot in ((bar.left, bar.left_foot), (bar.right, bar.right_foot)):
            x_px, foot_px = ax.transData.transform([(x, foot)])[0]
            tops = _column_tops(ax, x_px, px_per_pt)
            if not tops:
                continue
            top_px, what = max(tops)
            room = foot_px - top_px
            if room < FOOT_GAP_PT * px_per_pt - SLACK_PX:
                problems.append(
                    f"{bar.bar.describe()}: foot at {x:g} only "
                    f"{room / px_per_pt:.2f} pt above {what}"
                )
    return problems


@pytest.mark.parametrize("kind", [PlotKind.BAR, PlotKind.BOX, PlotKind.VIOLIN, PlotKind.STRIP])
def test_every_bar_clears_every_drawn_mark(gait_table, kind):
    figure, fig = _render(_spec(*BARS, kind=kind), gait_table)
    placement = getattr(fig, DIFF_BARS_ATTR)
    index, ax = _axes_for(fig, figure, "R")
    assert len(placement.bars[index]) == len(BARS)
    assert _check_clearance(fig, ax, placement.bars[index]) == []
    assert _check_feet(fig, ax, placement.bars[index]) == []


def test_bars_clear_the_sample_overlay(gait_table):
    figure, fig = _render(_spec(*BARS, show_sample=["subject"]), gait_table)
    placement = getattr(fig, DIFF_BARS_ATTR)
    index, ax = _axes_for(fig, figure, "R")
    assert _check_clearance(fig, ax, placement.bars[index]) == []
    assert _check_feet(fig, ax, placement.bars[index]) == []


def test_bars_clear_a_spaghetti(gait_table):
    spec = PlotSpec(
        measures=["Step"],
        kind=PlotKind.SPAGHETTI,
        roles={"side": Role.FACET, "session": Role.GROUP, "subject": Role.GROUP},
        groups=["subject", "session"],
        difference_bars=list(BARS),
        style=StyleOptions(width=6.0, height=4.0),
    )
    figure, fig = _render(spec, gait_table)
    index, ax = _axes_for(fig, figure, "R")
    placement = getattr(fig, DIFF_BARS_ATTR)
    assert _check_clearance(fig, ax, placement.bars[index]) == []
    assert _check_feet(fig, ax, placement.bars[index]) == []


def test_one_line_and_one_label_per_bar_as_placed(gait_table):
    figure, fig = _render(_spec(*BARS), gait_table)
    placement = getattr(fig, DIFF_BARS_ATTR)
    index, ax = _axes_for(fig, figure, "R")
    lines = [line for line in ax.lines if line.get_gid() == DIFF_BAR_GID]
    labels = [text for text in ax.texts if text.get_gid() == DIFF_LABEL_GID]
    assert len(lines) == len(labels) == len(BARS)
    assert sorted(t.get_text() for t in labels) == sorted(b.label for b in BARS)
    drawn = {tuple(np.round(line.get_ydata(), 9)) for line in lines}
    expected = {
        tuple(np.round([b.left_foot, b.y, b.y, b.right_foot], 9)) for b in placement.bars[index]
    }
    assert drawn == expected


def test_each_label_is_drawn_above_its_line_and_inside_the_axes(gait_table):
    figure, fig = _render(_spec(*BARS), gait_table)
    placement = getattr(fig, DIFF_BARS_ATTR)
    index, ax = _axes_for(fig, figure, "R")
    renderer = fig.canvas.get_renderer()
    axes_box = ax.get_window_extent(renderer)
    # Matched by position, not text: two bars may carry the same label ("*").
    by_place = {(b.bar.label, round(b.middle, 9), round(b.label_y, 9)): b for b in placement.bars[index]}
    for text in (t for t in ax.texts if t.get_gid() == DIFF_LABEL_GID):
        box = text.get_window_extent(renderer)
        x, y = text.get_position()
        bar = by_place[(text.get_text(), round(x, 9), round(y, 9))]
        line_px = ax.transData.transform((bar.middle, bar.y))[1]
        assert box.y0 >= line_px - SLACK_PX
        assert box.y1 <= axes_box.y1 + SLACK_PX


def test_labels_do_not_overlap_each_other(gait_table):
    figure, fig = _render(_spec(*BARS), gait_table)
    _, ax = _axes_for(fig, figure, "R")
    renderer = fig.canvas.get_renderer()
    boxes = [t.get_window_extent(renderer) for t in ax.texts if t.get_gid() == DIFF_LABEL_GID]
    for i, first in enumerate(boxes):
        for second in boxes[i + 1 :]:
            assert not first.overlaps(second)


def test_a_shared_axis_is_raised_for_every_panel(gait_table):
    figure, fig = _render(_spec(*BARS), gait_table)
    placement = getattr(fig, DIFF_BARS_ATTR)
    tops = {round(ax.get_ylim()[1], 9) for ax in fig.axes if ax.get_visible()}
    assert len(tops) == 1
    top = tops.pop()
    assert top > figure.y_limits[1]
    assert all(round(high, 9) == top for _, high in placement.limits.values())


def test_a_typed_max_draws_no_bar_above_it(gait_table, caplog):
    spec = _spec(*BARS, y_axis=YAxis(maximum=7.0))
    with caplog.at_level(logging.WARNING, logger=LAYER):
        figure, fig = _render(spec, gait_table)
    index, ax = _axes_for(fig, figure, "R")
    assert ax.get_ylim()[1] == pytest.approx(7.0)
    placement = getattr(fig, DIFF_BARS_ATTR)
    for bar in placement.bars.get(index, []):
        assert bar.label_top <= 7.0
    assert "do not fit under the Max you set" in caplog.text


def test_a_log_axis_draws_the_bars(gait_table):
    figure, fig = _render(_spec(*BARS, style=StyleOptions(width=6.0, height=4.0, log_y=True)), gait_table)
    index, ax = _axes_for(fig, figure, "R")
    placement = getattr(fig, DIFF_BARS_ATTR)
    assert len(placement.bars[index]) == len(BARS)
    assert _check_clearance(fig, ax, placement.bars[index]) == []
    assert _check_feet(fig, ax, placement.bars[index]) == []


def test_the_label_size_is_the_differences_text_size(gait_table):
    from scistackplot.spec import TextSizes

    style = StyleOptions(width=6.0, height=4.0, text=TextSizes(base=12.0, differences=20.0))
    figure, fig = _render(_spec(*BARS, style=style), gait_table)
    _, ax = _axes_for(fig, figure, "R")
    sizes = {t.get_fontsize() for t in ax.texts if t.get_gid() == DIFF_LABEL_GID}
    assert sizes == {20.0}


def test_no_bars_leaves_the_figure_as_it_was(gait_table):
    figure, fig = _render(_spec(), gait_table)
    assert getattr(fig, DIFF_BARS_ATTR) is None
    for ax in (a for a in fig.axes if a.get_visible()):
        assert ax.get_ylim() == pytest.approx(figure.y_limits)
        assert not [line for line in ax.lines if line.get_gid() == DIFF_BAR_GID]


def test_the_grid_reach_is_still_measured_after_the_bars(gait_table):
    _, fig = _render(_spec(*BARS), gait_table)
    assert getattr(fig, GRID_REACH_ATTR) is not None


def test_the_layout_passes_are_logged(gait_table, caplog):
    with caplog.at_level(logging.DEBUG, logger=LAYER):
        _render(_spec(*BARS), gait_table)
    assert "difference bars: layout pass 1" in caplog.text
    assert "difference bars placed:" in caplog.text
    assert "labels measured" in caplog.text
