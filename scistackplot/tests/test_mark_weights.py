"""
Mark weights (2026-09-24): ``StyleOptions.sample_weight`` scales the "Show
sample" overlay's points AND lines, ``StyleOptions.line_weight`` a
spaghetti's own points and lines — multipliers, 1 = the look before the
knobs existed.

``scistackplot.weights`` is the one owner; these tests hold the three readers
(matplotlib, plotly, the generated seaborn code) to it, and pin that 1x
draws exactly what the hard-coded sizes drew.
"""

from __future__ import annotations

import math

import pandas as pd
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.collections import PathCollection  # noqa: E402

from scistackplot import (  # noqa: E402
    Aggregation,
    ErrorBand,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    render_matplotlib,
    render_plotly,
    resolve,
)
from scistackplot.roles import RoleError  # noqa: E402
from scistackplot.weights import (  # noqa: E402
    PLOTLY_PX_PER_PT,
    SAMPLE_LINE_WIDTH,
    SAMPLE_MARKER_FRACTION,
    SPAGHETTI_LINE_WIDTH,
    sample_weight,
    spaghetti_weight,
    weight_problem,
)

ROWS = [
    ("01", "A", "s1", "1", 1.0),
    ("01", "A", "s1", "2", 3.0),
    ("01", "A", "s2", "1", 10.0),
    ("01", "A", "s2", "2", 12.0),
    ("02", "A", "s1", "1", 5.0),
    ("02", "A", "s2", "1", 20.0),
    ("03", "B", "s1", "1", 7.0),
    ("03", "B", "s2", "1", 30.0),
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


def _spaghetti(show=("trial",), **style) -> PlotSpec:
    """One line per subject across the sessions, trials shown as points
    (a trial belongs to one session, so they are never joined)."""
    return PlotSpec(
        measures=["M"],
        roles={
            "subject": Role.GROUP,
            "session": Role.GROUP,
            "trial": Role.COLLAPSE,
            "group": Role.ITERATE,
        },
        groups=["subject", "session"],
        kind=PlotKind.SPAGHETTI,
        show_sample=list(show),
        style=StyleOptions(**style),
    )


def _joined_bar(**style) -> PlotSpec:
    """Bars per session with each subject's mean joined across them."""
    return PlotSpec(
        measures=["M"],
        roles={
            "subject": Role.COLLAPSE,
            "session": Role.GROUP,
            "trial": Role.COLLAPSE,
            "group": Role.ITERATE,
        },
        groups=["session"],
        kind=PlotKind.BAR,
        aggregate=Aggregation(error=ErrorBand.SD),
        show_sample=["subject"],
        style=StyleOptions(**style),
    )


def _figure_a(spec, table):
    return next(f for f in resolve(spec, table) if f.figure_key.get("group") == "A")


def _solid_marked_lines(figure) -> list:
    return [
        line
        for ax in figure.axes
        for line in ax.get_lines()
        if line.get_marker() == "o" and line.get_linestyle() == "-"
    ]


def _marker_only_lines(figure) -> list:
    return [
        line
        for ax in figure.axes
        for line in ax.get_lines()
        if line.get_marker() == "o" and line.get_linestyle() == "None"
    ]


def _run(source: str, frame, function_name: str = "plot_m"):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace[function_name](frame.copy(), "figure.png")


# --- the owner --------------------------------------------------------------------


def test_one_x_is_the_look_before_the_knobs():
    style = StyleOptions()
    sample = sample_weight(style)
    assert sample.marker_pt == pytest.approx(math.sqrt(36.0 * SAMPLE_MARKER_FRACTION))
    assert sample.line_pt == SAMPLE_LINE_WIDTH == 1.0
    # plotly drew the overlay at 8 * sqrt(fraction) px, the spaghetti at 8 px.
    assert sample.marker_px == pytest.approx(8.0 * math.sqrt(SAMPLE_MARKER_FRACTION))
    lines = spaghetti_weight(style)
    assert lines.marker_pt == pytest.approx(6.0)
    assert lines.marker_px == pytest.approx(8.0)
    assert lines.line_pt == SPAGHETTI_LINE_WIDTH == 1.2
    assert PLOTLY_PX_PER_PT == pytest.approx(4.0 / 3.0)


def test_a_weight_scales_diameter_and_width_together():
    style = StyleOptions(sample_weight=2.0, line_weight=3.0)
    base = StyleOptions()
    assert sample_weight(style).marker_pt == pytest.approx(2 * sample_weight(base).marker_pt)
    assert sample_weight(style).line_pt == pytest.approx(2 * sample_weight(base).line_pt)
    assert sample_weight(style).marker_area == pytest.approx(4 * sample_weight(base).marker_area)
    assert spaghetti_weight(style).marker_pt == pytest.approx(3 * spaghetti_weight(base).marker_pt)
    assert spaghetti_weight(style).line_pt == pytest.approx(3 * spaghetti_weight(base).line_pt)


def test_the_knobs_are_independent():
    assert sample_weight(StyleOptions(line_weight=4.0)) == sample_weight(StyleOptions())
    assert spaghetti_weight(StyleOptions(sample_weight=4.0)) == spaghetti_weight(StyleOptions())


@pytest.mark.parametrize("bad", [0, 0.0, -1.0, float("nan"), float("inf"), "2", True, None])
def test_weight_problem_names_bad_values(bad):
    assert weight_problem(bad) is not None


@pytest.mark.parametrize("good", [1, 0.25, 4.0])
def test_weight_problem_accepts_positive_numbers(good):
    assert weight_problem(good) is None


@pytest.mark.parametrize("field", ["sample_weight", "line_weight"])
def test_validate_refuses_a_non_positive_weight(table, field):
    with pytest.raises(RoleError, match=field):
        resolve(_spaghetti(**{field: 0.0}), table)


# --- matplotlib -------------------------------------------------------------------


def test_mpl_spaghetti_lines_follow_line_weight(table):
    drawn = render_matplotlib(_figure_a(_spaghetti(show=(), line_weight=2.0), table))
    try:
        lines = _solid_marked_lines(drawn)
        assert lines
        assert {round(line.get_linewidth(), 6) for line in lines} == {2.4}
        assert {round(line.get_markersize(), 6) for line in lines} == {12.0}
    finally:
        plt.close(drawn)


def test_mpl_overlay_points_follow_sample_weight_not_line_weight(table):
    spec = _spaghetti(sample_weight=3.0, line_weight=0.5)
    drawn = render_matplotlib(_figure_a(spec, table))
    try:
        sizes = {
            round(float(s), 6)
            for c in drawn.axes[0].collections
            if isinstance(c, PathCollection)
            for s in c.get_sizes()
        }
        assert sizes == {round(sample_weight(spec.style).marker_area, 6)}
        assert {round(line.get_linewidth(), 6) for line in _solid_marked_lines(drawn)} == {0.6}
    finally:
        plt.close(drawn)


def test_mpl_joined_overlay_lines_follow_sample_weight(table):
    spec = _joined_bar(sample_weight=2.0)
    figure = _figure_a(spec, table)
    assert figure.sample_join
    drawn = render_matplotlib(figure)
    try:
        overlay = _solid_marked_lines(drawn)
        assert overlay
        assert {round(line.get_linewidth(), 6) for line in overlay} == {2.0}
        expected = round(sample_weight(spec.style).marker_pt, 6)
        assert {round(line.get_markersize(), 6) for line in overlay} == {expected}
    finally:
        plt.close(drawn)


# --- plotly -----------------------------------------------------------------------


def test_plotly_spaghetti_and_overlay_follow_their_own_knob(table):
    spec = _spaghetti(sample_weight=2.0, line_weight=1.5)
    payload = render_plotly(_figure_a(spec, table))
    overlay = [t for t in payload["data"] if t.get("legendrank") == 2000]
    lines = [
        t
        for t in payload["data"]
        if t.get("mode") == "lines+markers" and t.get("legendrank") != 2000
    ]
    assert overlay and lines
    assert {round(t["marker"]["size"], 6) for t in overlay} == {round(sample_weight(spec.style).marker_px, 6)}
    assert {round(t["line"]["width"], 6) for t in overlay} == {2.0}
    assert {round(t["marker"]["size"], 6) for t in lines} == {12.0}
    assert {round(t["line"]["width"], 6) for t in lines} == {1.8}


def test_plotly_meta_says_which_knob_applies(table):
    spaghetti = render_plotly(_figure_a(_spaghetti(), table))["layout"]["meta"]["mark_weights"]
    assert spaghetti["lines"]["applies"] is True
    assert spaghetti["sample"]["applies"] is True
    assert spaghetti["lines"]["weight"] == 1.0

    no_sample = render_plotly(_figure_a(_spaghetti(show=()), table))["layout"]["meta"]["mark_weights"]
    assert no_sample["sample"]["applies"] is False

    bar = render_plotly(_figure_a(_joined_bar(), table))["layout"]["meta"]["mark_weights"]
    assert bar["lines"]["applies"] is False
    assert bar["sample"]["applies"] is True


# --- the export draws what the preview draws --------------------------------------


def test_generated_spaghetti_matches_the_preview_weights(table, frame):
    pytest.importorskip("seaborn")
    from scistackplot import generate_plot_function

    spec = _spaghetti(sample_weight=2.5, line_weight=2.0)
    drawn = render_matplotlib(_figure_a(spec, table))
    try:
        preview_lines = {
            (round(l.get_linewidth(), 3), round(l.get_markersize(), 3), l.get_alpha())
            for l in _solid_marked_lines(drawn)
        }
        preview_points = {
            round(math.sqrt(float(s)), 3)
            for c in drawn.axes[0].collections
            if isinstance(c, PathCollection)
            for s in c.get_sizes()
        }
    finally:
        plt.close(drawn)

    generated = _run(generate_plot_function(spec, table), frame[frame["group"] == "A"])
    try:
        generated_lines = {
            (round(l.get_linewidth(), 3), round(l.get_markersize(), 3), l.get_alpha())
            for l in _solid_marked_lines(generated)
        }
        generated_points = {round(l.get_markersize(), 3) for l in _marker_only_lines(generated)}
    finally:
        plt.close(generated)
    assert generated_lines == preview_lines == {(2.4, 12.0, spec.style.alpha)}
    assert generated_points == preview_points


def test_generated_joined_overlay_matches_the_preview_weights(table, frame):
    pytest.importorskip("seaborn")
    from scistackplot import generate_plot_function

    spec = _joined_bar(sample_weight=3.0)
    drawn = render_matplotlib(_figure_a(spec, table))
    try:
        preview = {
            (round(l.get_linewidth(), 3), round(l.get_markersize(), 3))
            for l in _solid_marked_lines(drawn)
        }
    finally:
        plt.close(drawn)

    generated = _run(generate_plot_function(spec, table), frame[frame["group"] == "A"])
    try:
        exported = {
            (round(l.get_linewidth(), 3), round(l.get_markersize(), 3))
            for l in _solid_marked_lines(generated)
        }
    finally:
        plt.close(generated)
    assert exported == preview
    assert preview == {(3.0, round(sample_weight(spec.style).marker_pt, 3))}
