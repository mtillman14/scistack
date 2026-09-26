"""
Stage 2 of the label-legibility plan: the matplotlib export fits its x labels.

The fixtures recreate the two figures that motivated it
(spec/images/graph1.png, graph2.png): a nested bar plot of session ticks under
InterventionGroup brackets with a Show-sample overlay, and the same thing in
two facet columns. The checks are properties of what lands on the canvas, not
pixel baselines, so they hold for any figure:

* no two visible tick labels overlap (upright / 90 deg: their boxes; 45 deg:
  their parallel strips, which is the geometry ``ticklabels`` fits);
* no bracket label overlaps another bracket label or a tick label, and the
  brackets sit BELOW the tick labels;
* a nested axis carries no x title (its rows already name every level).
"""

from __future__ import annotations

import math

import pandas as pd
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from scistackplot.render.mpl import LABEL_FIT_ATTR, LEGEND_ATTR  # noqa: E402
from scistackplot import (  # noqa: E402
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    generate_plot_function,
    render_matplotlib,
    resolve,
)

SESSIONS = ["BL", "MID24", "POST24", "MO1FU", "MO3FU"]
GROUPS = ["Digitimer", "Onward", "Sham"]
#: Overlap below this is anti-aliasing, not a collision a reader sees.
TOLERANCE_PT = 0.5


@pytest.fixture
def gait_table() -> LongTable:
    rows = []
    for g, group in enumerate(GROUPS):
        for s in range(4):
            subject = f"SS{g * 4 + s + 1:02d}"
            for k, session in enumerate(SESSIONS):
                for speed in ["SSV", "FV"]:
                    rows.append(
                        {
                            "subject": subject,
                            "InterventionGroup": group,
                            "session": session,
                            "speed": speed,
                            "Distance": 200.0 + 10 * k + 5 * s + 30 * g,
                        }
                    )
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "InterventionGroup", "session", "speed"],
        measures=["Distance"],
        name="Distance",
        level_order={"session": SESSIONS, "InterventionGroup": GROUPS},
        schema_levels=["subject", "session"],
    )


def _graph1(width=6.0, height=4.5, **style) -> PlotSpec:
    return PlotSpec(
        measures=["Distance"],
        roles={
            "session": Role.GROUP,
            "InterventionGroup": Role.GROUP,
            "subject": Role.COLLAPSE,
            "speed": Role.COLLAPSE,
        },
        groups=["session", "InterventionGroup"],
        color="session",
        kind=PlotKind.BAR,
        show_sample=["subject"],
        style=StyleOptions(width=width, height=height, **style),
    )


def _graph2(width=8.0, height=5.0, **style) -> PlotSpec:
    return PlotSpec(
        measures=["Distance"],
        roles={
            "session": Role.GROUP,
            "InterventionGroup": Role.GROUP,
            "subject": Role.COLLAPSE,
            "speed": Role.FACET,
        },
        groups=["session", "InterventionGroup"],
        color="InterventionGroup",
        kind=PlotKind.BAR,
        show_sample=["subject"],
        style=StyleOptions(width=width, height=height, **style),
    )


def _draw(spec, table):
    (resolved,) = resolve(spec, table)
    figure = render_matplotlib(resolved)
    figure.canvas.draw()
    return resolved, figure


def _pt(figure, pixels):
    return pixels * 72.0 / figure.dpi


def _tick_labels(ax):
    return [t for t in ax.get_xticklabels() if t.get_visible() and t.get_text()]


def _bracket_labels(ax):
    return [t for t in ax.texts if t.get_visible() and t.get_text()]


def _bracket_rules(ax):
    """The rules ``_draw_x_groups`` adds: unclipped grey lines. add_artist
    files a Line2D under ``ax.lines``, not ``ax.artists``."""
    return [
        line
        for line in ax.lines
        if not line.get_clip_on() and line.get_color() == "#888888"
    ]


def _assert_ticks_do_not_overlap(figure, ax):
    renderer = figure.canvas.get_renderer()
    labels = sorted(_tick_labels(ax), key=lambda t: t.get_position()[0])
    for a, b in zip(labels, labels[1:]):
        rotation = round(a.get_rotation()) % 180
        if rotation in (0, 90):
            # Upright or vertical: the bounding box IS the text.
            left = a.get_window_extent(renderer)
            right = b.get_window_extent(renderer)
            assert _pt(figure, right.x0 - left.x1) >= -TOLERANCE_PT, (
                a.get_text(),
                b.get_text(),
            )
        else:
            # Rotated: parallel strips through their ticks. Their bounding
            # boxes overlap even when the text does not, so measure the strips.
            xa, xb = (
                ax.transData.transform((t.get_position()[0], 0.0))[0] for t in (a, b)
            )
            # A drawn line is at least as tall as "lp" (matplotlib's Text
            # layout), whatever its own glyphs are.
            heights = [
                _pt(
                    figure,
                    max(
                        renderer.get_text_width_height_descent(
                            s, t.get_fontproperties(), ismath=False
                        )[1]
                        for s in (t.get_text(), "lp")
                    ),
                )
                for t in (a, b)
            ]
            apart = _pt(figure, abs(xb - xa)) * math.sin(math.radians(rotation))
            assert apart >= sum(heights) / 2 - TOLERANCE_PT, (a.get_text(), b.get_text())


def _assert_brackets_are_clear(figure, ax):
    renderer = figure.canvas.get_renderer()
    brackets = sorted(_bracket_labels(ax), key=lambda t: t.get_window_extent(renderer).x0)
    ticks = _tick_labels(ax)
    tick_bottom = min((t.get_window_extent(renderer).y0 for t in ticks), default=None)
    for a, b in zip(brackets, brackets[1:]):
        ea, eb = a.get_window_extent(renderer), b.get_window_extent(renderer)
        if ea.y1 > eb.y0 and eb.y1 > ea.y0:  # same row
            assert _pt(figure, eb.x0 - ea.x1) >= -TOLERANCE_PT, (a.get_text(), b.get_text())
    for text in brackets:
        if tick_bottom is not None:
            assert text.get_window_extent(renderer).y1 <= tick_bottom + 1, text.get_text()


def _labelled_axes(figure):
    return [ax for ax in figure.axes if ax.get_visible() and _tick_labels(ax)]


# --- the two figures from the report -----------------------------------------


@pytest.mark.parametrize(
    "spec",
    # graph2 at 14in: its two panels need the width (see the next test).
    [_graph1(), _graph2(width=14.0)],
    ids=["graph1", "graph2-wide"],
)
def test_no_label_overlaps_on_the_reported_figures(gait_table, spec):
    resolved, figure = _draw(spec, gait_table)
    try:
        fit = getattr(figure, LABEL_FIT_ATTR)["ticks"]
        assert fit.fits, fit.describe()
        axes = _labelled_axes(figure)
        assert axes
        for ax in axes:
            _assert_ticks_do_not_overlap(figure, ax)
            _assert_brackets_are_clear(figure, ax)
            assert {t.get_text() for t in _bracket_labels(ax)} == set(GROUPS)
    finally:
        plt.close(figure)


def test_graph2_at_its_own_width_the_legend_gives_way(gait_table):
    """At 8in, with the legend at the right, each panel had ~7pt per position:
    no label could fit. The legend now narrows, and when the x labels still
    cannot fit beside it, it moves below the panels and gives the width back.

    The panels are then within a point or two of fitting 17 names at 90
    degrees, so this asserts that the answer is HONEST either way: it fits
    and nothing overlaps, or it says it still overlaps.
    """
    _, figure = _draw(_graph2(), gait_table)
    try:
        legend = getattr(figure, LEGEND_ATTR)
        assert legend["below"], legend
        assert "x labels" in legend["reason"], legend
        fit = getattr(figure, LABEL_FIT_ATTR)["ticks"]
        if fit.fits:
            for ax in _labelled_axes(figure):
                _assert_ticks_do_not_overlap(figure, ax)
        else:
            assert "STILL OVERLAPS" in fit.describe()
    finally:
        plt.close(figure)


@pytest.mark.parametrize("make", [_graph1, _graph2], ids=["graph1", "graph2"])
def test_a_nested_axis_has_no_title(gait_table, make):
    """"session / InterventionGroup" repeated the rows above and below it."""
    resolved, figure = _draw(make(), gait_table)
    try:
        assert resolved.labels.x == ""
        assert all(ax.get_xlabel() == "" for ax in figure.axes)
        assert figure.get_supxlabel() == ""
    finally:
        plt.close(figure)


def test_a_stated_title_on_a_nested_axis_sits_below_the_brackets(gait_table):
    resolved, figure = _draw(_graph1(x_label="Session by group"), gait_table)
    try:
        renderer = figure.canvas.get_renderer()
        (ax,) = _labelled_axes(figure)
        title = ax.xaxis.label.get_window_extent(renderer)
        lowest = min(t.get_window_extent(renderer).y0 for t in _bracket_labels(ax))
        assert title.y1 <= lowest + 1
    finally:
        plt.close(figure)


def test_brackets_keep_their_distance_on_a_short_panel(gait_table):
    """The old placement was a fraction of the axes height: on a short panel
    that fraction was smaller than the tick labels, so the brackets sat on
    them. In points, the distance is the same at any height."""
    resolved, figure = _draw(_graph1(), gait_table)
    _, low = _draw(_graph1(height=2.2), gait_table)
    try:
        for fig in (figure, low):
            for ax in _labelled_axes(fig):
                _assert_brackets_are_clear(fig, ax)
    finally:
        plt.close(figure)
        plt.close(low)


# --- the ladder on real text ---------------------------------------------------


def _single_axis_table(levels) -> LongTable:
    rows = [
        {"level": level, "trial": trial, "M": float(i + trial)}
        for i, level in enumerate(levels)
        for trial in range(2)
    ]
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["level", "trial"],
        measures=["M"],
        name="M",
        level_order={"level": list(levels)},
        schema_levels=["level", "trial"],
    )


def _single_axis_spec(width) -> PlotSpec:
    return PlotSpec(
        measures=["M"],
        roles={"level": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["level"],
        kind=PlotKind.BAR,
        style=StyleOptions(width=width, height=3.0),
    )


def test_many_numbered_ids_are_stripped_and_thinned():
    ids = [f"SS{i:02d}" for i in range(1, 41)]
    resolved, figure = _draw(_single_axis_spec(4.0), _single_axis_table(ids))
    try:
        (ax,) = _labelled_axes(figure)
        shown = [t.get_text() for t in ax.get_xticklabels()]
        assert shown[0] == "01" and shown[-1] == "40"  # the ends are kept
        assert "" in shown  # thinned
        assert not any(label.startswith("SS") for label in shown)
        _assert_ticks_do_not_overlap(figure, ax)
    finally:
        plt.close(figure)


def test_long_names_rotate_and_are_never_thinned():
    names = ["BaselineVisit", "MidpointVisit", "EndOfTraining", "OneMonthLater", "ThreeMonthsOn", "SixMonthsOn"]
    resolved, figure = _draw(_single_axis_spec(4.0), _single_axis_table(names))
    try:
        (ax,) = _labelled_axes(figure)
        shown = [t.get_text() for t in ax.get_xticklabels()]
        assert shown == names  # names: never stripped, never thinned
        assert {round(t.get_rotation()) for t in ax.get_xticklabels()} <= {45, 90}
        _assert_ticks_do_not_overlap(figure, ax)
    finally:
        plt.close(figure)


def test_labels_that_fit_are_untouched():
    names = ["pre", "post"]
    resolved, figure = _draw(_single_axis_spec(6.0), _single_axis_table(names))
    try:
        (ax,) = _labelled_axes(figure)
        assert [t.get_text() for t in ax.get_xticklabels()] == names
        assert all(t.get_rotation() == 0 for t in ax.get_xticklabels())
        # A single layer keeps its title: "pre"/"post" do not say what they are.
        assert ax.get_xlabel() == "level"
    finally:
        plt.close(figure)


# --- hiding labels the legend already gives (opt-in) -----------------------------


def test_hiding_legend_ticks_is_off_by_default(gait_table):
    _, figure = _draw(_graph1(), gait_table)
    try:
        (ax,) = _labelled_axes(figure)
        assert {t.get_text() for t in _tick_labels(ax)} == set(SESSIONS)
    finally:
        plt.close(figure)


def test_hiding_legend_ticks_blanks_the_colour_layer_ticks(gait_table):
    """graph1: session is both the tick layer and the colour."""
    _, figure = _draw(_graph1(hide_legend_ticks=True), gait_table)
    try:
        axes = [ax for ax in figure.axes if ax.get_visible()]
        assert all(not _tick_labels(ax) for ax in axes)
        # The brackets are a different layer, so they keep their names.
        assert {t.get_text() for ax in axes for t in _bracket_labels(ax)} == set(GROUPS)
    finally:
        plt.close(figure)


def test_hiding_legend_ticks_blanks_a_colour_layer_bracket_row(gait_table):
    """graph2: InterventionGroup is the brackets AND the colour."""
    _, figure = _draw(_graph2(hide_legend_ticks=True), gait_table)
    try:
        axes = _labelled_axes(figure)
        assert axes
        for ax in axes:
            assert {t.get_text() for t in _tick_labels(ax)} == set(SESSIONS)
            assert not _bracket_labels(ax)
            # No label shown, so no rule: a bare line under the ticks names nothing.
            assert not _bracket_rules(ax)
    finally:
        plt.close(figure)


def test_labelled_brackets_keep_their_rules(gait_table):
    _, figure = _draw(_graph2(), gait_table)
    try:
        axes = _labelled_axes(figure)
        assert axes
        for ax in axes:
            assert len(_bracket_rules(ax)) == len(_bracket_labels(ax)) == len(GROUPS)
    finally:
        plt.close(figure)


# --- the title has one owner ---------------------------------------------------


def test_generated_code_leaves_a_nested_axis_untitled(gait_table):
    source = generate_plot_function(_graph1(), gait_table)
    assert "set_axis_labels(''" in source


# --- measurement ---------------------------------------------------------------


def test_a_label_is_measured_at_full_line_height():
    """ "BL" has no descender, but matplotlib draws it in a line as tall as
    "lp". Measured by its own glyphs, 90-degree labels were packed closer
    than they are drawn (graph1: BL / MID24 overlapped)."""
    from scistackplot.render.mpl import _text_measure

    figure = plt.figure()
    try:
        measure = _text_measure(figure)
        assert measure("BL", 14.0)[1] == pytest.approx(measure("lp", 14.0)[1])
        assert measure("BL", 14.0)[0] < measure("BLBL", 14.0)[0]
    finally:
        plt.close(figure)


def test_hidden_ticks_leave_the_innermost_brackets_unruled(gait_table):
    """graph1 + hide_legend_ticks: the rule over each bracket label spans the
    ticks above it — with those ticks hidden it bracketed nothing
    (spec/images/bars_wrong_horz_lines.png). The labels stay; the rules go."""
    _, figure = _draw(_graph1(hide_legend_ticks=True), gait_table)
    try:
        # Not _labelled_axes: that keys on tick labels, which are all hidden here.
        axes = [ax for ax in figure.axes if ax.get_visible() and _bracket_labels(ax)]
        assert axes
        for ax in axes:
            assert not _tick_labels(ax)
            assert {t.get_text() for t in _bracket_labels(ax)} == set(GROUPS)
            assert not _bracket_rules(ax)
    finally:
        plt.close(figure)
