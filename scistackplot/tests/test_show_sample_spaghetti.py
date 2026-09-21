"""
"Show sample" on a spaghetti (2026-09-21): the trials and cycles behind each
subject's point, drawn on that subject's line.

A spaghetti's mark is a point on a LINE, shifted sideways from its tick by
``ResolvedPlot.series_offsets``; an overlay point therefore sits at ITS line's
shift plus its own small offset (``render.base.sample_positions``,
``SAMPLE_LINE``), with the identity offsets scaled to the number of lines so
one subject's trials never stray into the next subject's band. Both the lines
layer and a collapsed key drawn one line each (``GroupingLayers.units``) are
lines in this sense.
"""

from __future__ import annotations

import pandas as pd
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.collections import PathCollection  # noqa: E402

from scistackplot import (  # noqa: E402
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    capabilities,
    render_matplotlib,
    render_plotly,
    resolve,
)
from scistackplot.resolved import SAMPLE_LINE, SERIES, X  # noqa: E402
from scistackplot.roles import complete_roles, overlay_join, overlay_unavailable  # noqa: E402
from scistackplot.spaghetti import overlay_offsets  # noqa: E402
from scistackplot.shape import Shape  # noqa: E402

# subject x session x trial, plus a subject-level `group`. Every value is
# distinct so a point on the wrong line carries a number that says so.
ROWS = [
    ("01", "A", "s1", "1", 1.0),
    ("01", "A", "s1", "2", 3.0),
    ("01", "A", "s2", "1", 10.0),
    ("01", "A", "s2", "2", 12.0),
    ("02", "A", "s1", "1", 5.0),
    ("02", "A", "s2", "1", 20.0),
    ("03", "B", "s1", "1", 7.0),
    ("03", "B", "s1", "2", 8.0),
    ("03", "B", "s2", "1", 30.0),
    ("04", "B", "s1", "1", 9.0),
    ("04", "B", "s2", "1", 40.0),
]


@pytest.fixture
def frame() -> pd.DataFrame:
    return pd.DataFrame(ROWS, columns=["subject", "group", "session", "trial", "M"])


@pytest.fixture
def table(frame) -> LongTable:
    return LongTable.from_frame(
        frame,
        factors=["subject", "group", "session", "trial"],
        measures=["M"],
        name="M",
        schema_levels=["subject", "session", "trial"],
        level_order={"session": ["s1", "s2"], "group": ["A", "B"]},
    )


def _lines_are_subjects(show=("trial",), **kwargs) -> PlotSpec:
    """The classic: one line per subject across the sessions, trials
    collapsed; `group` separates figures (unmentioned) unless said."""
    base = dict(
        measures=["M"],
        roles={
            "subject": Role.GROUP,
            "session": Role.GROUP,
            "trial": Role.COLLAPSE,
            "group": Role.FACET,
        },
        groups=["subject", "session"],
        kind=PlotKind.SPAGHETTI,
        show_sample=list(show),
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _lines_are_collapsed_subjects(show=("trial",), **kwargs) -> PlotSpec:
    """The user's shape: group lines across session ticks, subject and trial
    collapsed — a subject recurs at every session, so it is drawn one line
    each (`GroupingLayers.units`) inside its group."""
    base = dict(
        measures=["M"],
        roles={
            "subject": Role.COLLAPSE,
            "session": Role.GROUP,
            "trial": Role.COLLAPSE,
            "group": Role.GROUP,
        },
        groups=["group", "session"],
        kind=PlotKind.SPAGHETTI,
        show_sample=list(show),
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _one(spec, table):
    figures = resolve(spec, table)
    assert len(figures) == 1, [f.title for f in figures]
    return figures[0]


def _value_to_row(frame) -> dict[float, tuple[str, str, str]]:
    return {
        float(m): (str(s), str(g), str(sess))
        for s, g, sess, _, m in frame.itertuples(index=False)
    }


# --- availability ---------------------------------------------------------------------


def test_a_spaghetti_can_carry_the_overlay(table):
    spec = _lines_are_subjects()
    roles = complete_roles(spec, table)
    assert overlay_unavailable(spec, roles, Shape.SCALAR) is None
    report = capabilities(spec, table)["sample_overlay"]
    assert report["available"] is True
    assert [f["name"] for f in report["factors"]] == ["trial"]


def test_trials_under_sessions_are_points_not_lines(table):
    """A trial belongs to one session, so the automatic join declines — the
    same rule as on a bar, with the lines layer counted as a grouping layer."""
    spec = _lines_are_subjects()
    join = overlay_join(spec, complete_roles(spec, table), table)
    assert join.join is False and join.automatic


# --- placement: each point on its own line ------------------------------------------


def test_overlay_rows_carry_their_line(table):
    figure = _one(_lines_are_subjects(), table)
    for panel in figure.panels:
        sample = panel.sample
        assert SAMPLE_LINE in sample.columns
        # The line id is the marks' own series id, so the offsets find it —
        # and every line in this panel has its trials behind it. (The overlay
        # frame carries the SHOWN keys as columns, not the line layers.)
        assert set(sample[SAMPLE_LINE]) == set(panel.frame[SERIES])
        assert "subject" not in sample.columns and "trial" in sample.columns


def test_offsets_are_scaled_to_the_number_of_lines(table):
    figure = _one(_lines_are_subjects(), table)
    n_lines = len(figure.series_offsets)
    assert n_lines == 4
    ids = {str(v) for panel in figure.panels for v in panel.sample[SERIES].unique()}
    assert figure.sample_offsets == overlay_offsets(ids, n_lines)
    # Inside one line's band: the gap between neighbouring lines is 0.4 / 3.
    assert max(abs(v) for v in figure.sample_offsets.values()) < 0.4 / (n_lines - 1) / 2


def _mpl_sample_points(drawn) -> list[tuple[float, float]]:
    return [
        (float(x), float(y))
        for ax in drawn.axes
        for c in ax.collections
        if isinstance(c, PathCollection)
        for x, y in c.get_offsets()
    ]


def _check_points_on_their_line(points, figure, frame):
    """Every (x, y) sits at its subject's line: tick of its session +
    that line's series offset, within the identity band."""
    order = [str(v) for v in figure.x_order]
    by_value = _value_to_row(frame)
    band = max(abs(v) for v in figure.sample_offsets.values()) + 1e-9
    assert points, "no overlay points drawn"
    for x, y in points:
        subject, group, session = by_value[round(y, 6)]
        line_id = next(
            key for key in figure.series_offsets if key.split(" | ")[-1] == subject
        )
        expected = order.index(session) + figure.series_offsets[line_id]
        assert abs(x - expected) <= band, (subject, session, x, expected)


def test_mpl_points_sit_on_their_subjects_line(table, frame):
    figure = _one(_lines_are_subjects(), table)
    drawn = render_matplotlib(figure)
    try:
        points = _mpl_sample_points(drawn)
        assert len(points) == len(frame), "one point per trial"
        _check_points_on_their_line(points, figure, frame)
    finally:
        plt.close(drawn)


def test_plotly_points_sit_on_their_subjects_line(table, frame):
    figure = _one(_lines_are_subjects(), table)
    payload = render_plotly(figure)
    overlay = [t for t in payload["data"] if t.get("legendrank") == 2000]
    assert overlay and {t["mode"] for t in overlay} == {"markers"}
    points = [(float(x), float(y)) for t in overlay for x, y in zip(t["x"], t["y"], strict=True)]
    assert len(points) == len(frame)
    _check_points_on_their_line(points, figure, frame)


def test_both_backends_place_every_point_at_the_same_x(table):
    figure = _one(_lines_are_subjects(), table)
    payload = render_plotly(figure)
    plotly_xs = sorted(
        round(float(x), 6)
        for t in payload["data"]
        if t.get("legendrank") == 2000
        for x in t["x"]
    )
    drawn = render_matplotlib(figure)
    try:
        mpl_xs = sorted(round(x, 6) for x, _ in _mpl_sample_points(drawn))
    finally:
        plt.close(drawn)
    assert mpl_xs == plotly_xs


def test_collapsed_subjects_drawn_as_lines_carry_their_trials_too(table, frame):
    """The user's shape: subject collapsed but drawn one line each inside its
    group; the trials still land on the subject's own line, not the group's."""
    figure = _one(_lines_are_collapsed_subjects(), table)
    assert len(figure.series_offsets) == 4, "one line per (group, subject)"
    for panel in figure.panels:
        assert set(panel.sample[SAMPLE_LINE]) <= set(panel.frame[SERIES])
    drawn = render_matplotlib(figure)
    try:
        points = _mpl_sample_points(drawn)
        assert len(points) == len(frame)
        _check_points_on_their_line(points, figure, frame)
    finally:
        plt.close(drawn)


def test_the_marks_do_not_move_when_the_overlay_is_shown(table):
    """The lines and their markers are where they were; the overlay is added
    on top — the same promise the bar overlay makes."""
    plain = _one(_lines_are_subjects(show=()), table)
    shown = _one(_lines_are_subjects(), table)
    assert plain.series_offsets == shown.series_offsets
    for a, b in zip(plain.panels, shown.panels, strict=True):
        pd.testing.assert_frame_equal(
            a.frame.sort_values([SERIES, X]).reset_index(drop=True),
            b.frame.sort_values([SERIES, X]).reset_index(drop=True),
        )


# --- the export -----------------------------------------------------------------------


def _run(source: str, frame, function_name: str = "plot_m"):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace[function_name](frame.copy(), "figure.png")


def test_generated_code_lands_the_points_where_the_preview_draws_them(table, frame):
    pytest.importorskip("seaborn")
    from scistackplot import generate_plot_function

    spec = _lines_are_subjects(roles={
        "subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE, "group": Role.ITERATE,
    })
    figures = resolve(spec, table)
    preview = next(f for f in figures if f.figure_key.get("group") == "A")
    drawn = render_matplotlib(preview)
    try:
        preview_points = sorted(round(x, 6) for x, _ in _mpl_sample_points(drawn))
    finally:
        plt.close(drawn)

    source = generate_plot_function(spec, table)
    generated = _run(source, frame[frame["group"] == "A"])
    try:
        # The overlay is drawn as marker-only Line2D's; the spaghetti's own
        # lines are solid.
        generated_points = sorted(
            round(float(x), 6)
            for ax in generated.axes
            for line in ax.get_lines()
            if line.get_marker() == "o" and line.get_linestyle() == "None"
            for x in line.get_xdata()
        )
    finally:
        plt.close(generated)
    assert generated_points == preview_points
    assert len(generated_points) == int((frame["group"] == "A").sum())
