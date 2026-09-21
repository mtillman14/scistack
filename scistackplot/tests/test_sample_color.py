"""
"Show sample" coloured by its own key (``PlotSpec.sample_color``).

Bars coloured by intervention group, one colour per subject on top — and a
joined line runs across the marks' colours (pre → post inside one group),
which is what the subject's own colour makes unambiguous. Plan:
.claude/plan-sample-color.md; doc: docs/claude/show-sample-overlay.md.
"""

from __future__ import annotations

import pandas as pd
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.collections import PathCollection  # noqa: E402

from scistackplot import (
    Aggregation,
    ErrorBand,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    RoleError,
    capabilities,
    generate_plot_function,
    overlay_color,
    overlay_join,
    overlay_steps,
    render_matplotlib,
    render_plotly,
    resolve,
    validate,
)
from scistackplot.render.base import (
    SAMPLE_PALETTE,
    palette_for,
    sample_palette_for,
)
from scistackplot.resolved import COLOR, SAMPLE_COLOR, SERIES
from scistackplot.roles import complete_roles
from plot_geometry import despaced

# subject x session x trial, plus `group` — a subject property with no
# schema depth (the user's Demographics.InterventionGroup).
ROWS = [
    ("01", "A", "s1", "1", 1.0),
    ("01", "A", "s1", "2", 3.0),
    ("01", "A", "s2", "1", 10.0),
    ("02", "A", "s1", "1", 5.0),
    ("02", "A", "s2", "1", 20.0),
    ("03", "B", "s1", "1", 7.0),
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
    )


def _spec(color: str, sample_color=None, show=("subject",), **kwargs) -> PlotSpec:
    """Group by session and group, one of them coloured; subject and trial
    collapsed; show the subjects."""
    base = dict(
        measures=["M"],
        roles={
            "subject": Role.COLLAPSE,
            "group": Role.GROUP,
            "session": Role.GROUP,
            "trial": Role.COLLAPSE,
        },
        groups=["session", "group"] if color == "session" else ["group", "session"],
        color=color or None,
        kind=PlotKind.BAR,
        aggregate=Aggregation(error=ErrorBand.SD),
        show_sample=list(show),
        sample_color=sample_color,
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _figure(spec, table):
    (figure,) = resolve(spec, table)
    return figure


def _overlay_lines(drawn):
    return [
        line
        for ax in drawn.axes
        for line in ax.get_lines()
        if line.get_marker() == "o" and len(line.get_xdata())
    ]


def _overlay_traces(payload):
    return [t for t in payload["data"] if t["type"] == "scatter" and "legendrank" in t]


# --- the spec and the rule --------------------------------------------------


def test_sample_color_round_trips_and_defaults_to_none():
    spec = _spec("session", "subject")
    assert PlotSpec.from_dict(spec.to_dict()).sample_color == "subject"
    plain = _spec("session")
    assert "sample_color" not in plain.to_dict(), "TOML has no null"
    assert PlotSpec.from_dict(plain.to_dict()).sample_color is None


def test_the_setting_is_active_only_for_a_shown_key(table):
    spec = _spec("session", "subject")
    steps = overlay_steps(spec, complete_roles(spec, table), table)
    assert overlay_color(spec, steps) == "subject"
    # trial is collapsed but not shown (subject is the cut): inert.
    inert = _spec("session", "trial")
    steps = overlay_steps(inert, complete_roles(inert, table), table)
    assert overlay_color(inert, steps) is None
    # Showing trial implies subject, so either colours.
    both = _spec("session", "trial", show=("trial",))
    steps = overlay_steps(both, complete_roles(both, table), table)
    assert overlay_color(both, steps) == "trial"
    assert overlay_color(_spec("session", "subject", show=()), None) is None


def test_validate_refuses_a_name_that_is_no_factor_and_accepts_an_inert_one(table):
    with pytest.raises(RoleError, match="sample_color names unknown factor"):
        validate(_spec("session", "subjcet"), table)
    validate(_spec("session", "trial"), table)  # inert, not an error


def test_auto_join_ignores_a_depth_less_grouping_layer(table):
    """The user's assignment: colour = session, ticks = group (no depth).
    subject is above session, so the points are repeated measures — the
    depth-less layer neither blocks the rule nor changes its answer."""
    for color in ("session", "group"):
        spec = _spec(color, "subject")
        join = overlay_join(spec, complete_roles(spec, table), table)
        assert join.join is True and join.automatic is True, (color, join.reason)


# --- reduce ---------------------------------------------------------------------


def test_the_overlay_carries_its_own_colour_beside_the_marks(table):
    figure = _figure(_spec("session", "subject"), table)
    assert figure.sample_color == "subject"
    assert figure.sample_color_order == ["01", "02", "03", "04"]
    sample = figure.panels[0].sample
    assert set(sample[COLOR]) == {"s1", "s2"}, "the mark's colour still places the point"
    assert set(sample[SAMPLE_COLOR]) == {"01", "02", "03", "04"}
    assert set(sample[SERIES]) == {"01", "02", "03", "04"}


def test_without_the_setting_nothing_changes(table):
    figure = _figure(_spec("session"), table)
    assert figure.sample_color is None and figure.sample_color_order == []
    assert SAMPLE_COLOR not in figure.panels[0].sample.columns
    assert figure.to_dict()["sample"]["color"] is None


def test_to_dict_carries_the_overlay_colour(table):
    payload = _figure(_spec("group", "subject"), table).to_dict()["sample"]
    assert payload["color"] == "subject"
    assert payload["color_order"] == ["01", "02", "03", "04"]


def test_the_palette_is_indexed_by_the_figure_wide_order(table):
    figure = _figure(_spec("session", "subject"), table)
    assert sample_palette_for(figure, "03", 0) == SAMPLE_PALETTE[2]
    assert sample_palette_for(figure, "01", 5) == SAMPLE_PALETTE[0]
    # A level the order never names falls back to the position given.
    assert sample_palette_for(figure, "99", 1) == SAMPLE_PALETTE[1]


# --- matplotlib ------------------------------------------------------------------


@pytest.mark.parametrize("color", ["session", "group", ""])
def test_mpl_one_line_per_subject_in_its_own_colour_across_the_marks(table, color):
    """Whatever layer is coloured (or none), a subject is ONE polyline in
    its own palette colour, crossing the marks' colours where it must."""
    spec = _spec(color, "subject") if color else _spec("", "subject", groups=["session", "group"])
    figure = _figure(spec, table)
    drawn = render_matplotlib(figure)
    lines = _overlay_lines(drawn)
    assert len(lines) == 4, "one polyline per subject, never split per mark colour"
    colours = {line.get_color() for line in lines}
    assert colours == {SAMPLE_PALETTE[i] for i in range(4)}
    for line in lines:
        xs = list(line.get_xdata())
        assert len(xs) == 2 and xs == sorted(xs)
    plt.close(drawn)


def test_mpl_line_runs_between_the_two_session_ticks_when_the_colour_is_session(table):
    """Colour = session on a [session, group] grouping: session stays a tick
    inside its group bracket (colour is paint), so subject 01's line runs
    from the A·s1 tick to the A·s2 tick, each point on its own bar."""
    from scistackplot.xaxis import leaf_key

    figure = _figure(_spec("session", "subject"), table)
    drawn = render_matplotlib(figure)
    by_colour = {line.get_color(): line for line in _overlay_lines(drawn)}
    line = by_colour[SAMPLE_PALETTE[0]]  # subject 01
    xs = [float(x) for x in line.get_xdata()]
    order = [str(key) for key in figure.x_order]
    offset = figure.sample_offsets["01"]
    assert xs == pytest.approx(
        [order.index(leaf_key(("A", "s1"))) + offset, order.index(leaf_key(("A", "s2"))) + offset]
    )
    plt.close(drawn)


def test_mpl_marks_keep_their_own_colour(table):
    figure = _figure(_spec("session", "subject"), table)
    drawn = render_matplotlib(figure)
    bar_colours = {
        patch.get_facecolor()[:3] for patch in drawn.axes[0].patches if patch.get_height()
    }
    expected = {
        matplotlib.colors.to_rgb(palette_for(figure, level, i))
        for i, level in enumerate(["s1", "s2"])
    }
    assert bar_colours == expected
    plt.close(drawn)


def test_mpl_legend_lists_the_marks_then_the_subjects(table):
    figure = _figure(_spec("session", "subject"), table)
    drawn = render_matplotlib(figure)
    assert len(drawn.legends) == 1
    legend = drawn.legends[0]
    labels = [t.get_text() for t in legend.get_texts()]
    assert labels == ["s1", "s2", "01", "02", "03", "04"]
    assert legend.get_title().get_text() == "session / subject"
    plt.close(drawn)


def test_mpl_without_the_setting_points_take_the_marks_colour(table):
    figure = _figure(_spec("session"), table)
    drawn = render_matplotlib(figure)
    labels = [t.get_text() for legend in drawn.legends for t in legend.get_texts()]
    assert labels == ["s1", "s2"], "no legend entry per subject"
    # Split per mark colour, as before: each subject has a one-row run per
    # session, so no line is drawn — points only.
    assert _overlay_lines(drawn) == []
    points = sum(len(c.get_offsets()) for c in drawn.axes[0].collections if isinstance(c, PathCollection))
    assert points == 8
    plt.close(drawn)


# --- plotly ---------------------------------------------------------------------------


def test_plotly_one_trace_per_subject_with_a_legend_entry_and_group(table):
    payload = render_plotly(_figure(_spec("session", "subject"), table))
    traces = _overlay_traces(payload)
    assert len(traces) == 4
    assert {t["mode"] for t in traces} == {"lines+markers"}
    assert [t["name"] for t in traces] == ["01", "02", "03", "04"]
    assert all(t["showlegend"] for t in traces), "one entry per subject, first trace"
    assert {t["legendgroup"] for t in traces} == {"sample:01", "sample:02", "sample:03", "sample:04"}
    assert [t["marker"]["color"] for t in traces] == list(SAMPLE_PALETTE[:4])
    assert payload["layout"]["legend"]["title"]["text"] == "session / subject"


def test_plotly_marks_keep_their_colour_and_legend(table):
    payload = render_plotly(_figure(_spec("session", "subject"), table))
    bars = [t for t in payload["data"] if t["type"] == "bar"]
    assert [t["name"] for t in bars] == ["s1", "s2"]
    assert all(t["showlegend"] for t in bars)


def test_plotly_without_the_setting_is_unchanged(table):
    payload = render_plotly(_figure(_spec("session"), table))
    overlay = [t for t in payload["data"] if t["type"] == "scatter"]
    assert overlay and not any(t["showlegend"] for t in overlay)
    assert {t["legendgroup"] for t in overlay} == {"s1", "s2"}
    assert payload["layout"]["legend"]["title"]["text"] == "session"


def test_both_backends_place_every_point_at_the_same_x(table):
    figure = _figure(_spec("session", "subject"), table)
    plotly_xs = sorted(round(x, 6) for t in _overlay_traces(render_plotly(figure)) for x in t["x"])
    drawn = render_matplotlib(figure)
    mpl_xs = sorted(round(float(x), 6) for line in _overlay_lines(drawn) for x in line.get_xdata())
    plt.close(drawn)
    assert plotly_xs == mpl_xs


# --- codegen --------------------------------------------------------------------------


def _run(source: str, frame, function_name: str = "plot_m"):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace[function_name](frame.copy(), "figure.png")


@pytest.mark.parametrize("color", ["session", "group"])
def test_generated_lines_land_where_the_preview_draws_them(table, frame, color):
    spec = _spec(color, "subject")
    figure = _figure(spec, table)
    source = generate_plot_function(spec, table)
    assert "_sample_palette" in source and "from matplotlib.lines import Line2D" in source
    generated = _run(source, frame)
    lines = _overlay_lines(generated)
    assert len(lines) == 4, "one run per subject, across the hues"
    generated_xs = sorted(round(float(x), 6) for line in lines for x in line.get_xdata())
    drawn = render_matplotlib(figure)
    # The preview keeps a spacer slot between the group brackets; the export
    # cannot (`plot_geometry.despaced`).
    preview_xs = sorted(
        despaced([float(x) for line in _overlay_lines(drawn) for x in line.get_xdata()], figure)
    )
    plt.close(drawn)
    plt.close(generated)
    assert generated_xs == preview_xs


def test_generated_legend_lists_the_hue_then_the_subjects(table, frame):
    generated = _run(generate_plot_function(_spec("session", "subject"), table), frame)
    assert len(generated.legends) == 1
    legend = generated.legends[0]
    assert [t.get_text() for t in legend.get_texts()] == ["s1", "s2", "01", "02", "03", "04"]
    assert legend.get_title().get_text() == "session / subject"
    plt.close(generated)


def test_generated_code_without_the_setting_is_unchanged(table, frame):
    source = generate_plot_function(_spec("session"), table)
    assert "_sample_palette" not in source and "Line2D" not in source
    generated = _run(source, frame)
    # Split per hue: every run is one row long, so no line spans two points
    # (the export draws a one-point `plot`, the preview a `scatter`).
    runs = _overlay_lines(generated)
    assert runs and all(len(line.get_xdata()) == 1 for line in runs), "split per hue"
    plt.close(generated)


# --- capability -------------------------------------------------------------------------


def test_capability_report_says_what_may_colour_the_overlay(table):
    report = capabilities(_spec("session", "subject"), table)["sample_overlay"]
    assert report["color"] == {"setting": "subject", "active": "subject", "options": ["subject"]}
    report = capabilities(_spec("session", "trial", show=("trial",)), table)["sample_overlay"]
    assert report["color"] == {"setting": "trial", "active": "trial", "options": ["subject", "trial"]}
    report = capabilities(_spec("session", "trial"), table)["sample_overlay"]
    assert report["color"] == {"setting": "trial", "active": None, "options": ["subject"]}
    report = capabilities(_spec("session"), table)["sample_overlay"]
    assert report["color"] == {"setting": None, "active": None, "options": ["subject"]}
