"""
"Show sample" — Stage 3: both renderers draw the overlay in the same place.

Placement is one arithmetic (``render.base.sample_positions``): the mark's own
position (its tick; on a spaghetti its line's shift) + the identity's offset. Joined identities are lines
with markers, others are markers; nothing joins the legend; a plotly figure
with an overlay is drawn on the positional axis (``_positional_x``).
"""

from __future__ import annotations

import numpy as np
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
    render_matplotlib,
    render_plotly,
    resolve,
)
from scistackplot.resolved import SERIES, X
from scistackplot.xaxis import LEAF_SEPARATOR


@pytest.fixture
def unbalanced() -> LongTable:
    rows = [
        ("01", "pre", "1", 1.0),
        ("01", "pre", "2", 2.0),
        ("01", "pre", "3", 3.0),
        ("02", "pre", "1", 9.0),
        ("01", "post", "1", 10.0),
        ("01", "post", "2", 20.0),
        ("02", "post", "1", 30.0),
    ]
    frame = pd.DataFrame(rows, columns=["subject", "session", "trial", "M"])
    return LongTable.from_frame(
        frame,
        factors=["subject", "session", "trial"],
        measures=["M"],
        name="M",
        schema_levels=["subject", "session", "trial"],
        level_order={"session": ["pre", "post"]},
    )


def _spec(show, kind=PlotKind.BAR, **kwargs) -> PlotSpec:
    base = dict(
        measures=["M"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=kind,
        aggregate=Aggregation(error=ErrorBand.SD),
        show_sample=list(show),
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _figure(spec, table):
    (figure,) = resolve(spec, table)
    return figure


def _overlay_lines(drawn):
    return [line for line in drawn.axes[0].get_lines() if line.get_marker() == "o"]


def _overlay_points(drawn):
    """The scatter collections — never an error bar's LineCollection."""
    return [c for c in drawn.axes[0].collections if isinstance(c, PathCollection)]


def _overlay_traces(payload):
    return [t for t in payload["data"] if t["type"] == "scatter" and t["showlegend"] is False]


# --- matplotlib ----------------------------------------------------------------


def test_mpl_joins_subjects_across_sessions(unbalanced):
    drawn = render_matplotlib(_figure(_spec(["subject"]), unbalanced))
    lines = _overlay_lines(drawn)
    assert len(lines) == 2, "one polyline per subject"
    for line in lines:
        xs = list(line.get_xdata())
        assert xs == sorted(xs) and len(xs) == 2
    # No colour level: the whole spaghetti band, tick ± 0.2.
    offsets = sorted(round(float(line.get_xdata()[0]), 6) for line in lines)
    assert offsets == [-0.2, 0.2]
    plt.close(drawn)


def test_mpl_draws_trials_as_points_only(unbalanced):
    drawn = render_matplotlib(_figure(_spec(["trial"]), unbalanced))
    assert _overlay_lines(drawn) == []
    points = sum(len(c.get_offsets()) for c in _overlay_points(drawn))
    assert points == 7
    plt.close(drawn)


def test_mpl_overlay_never_joins_the_legend(unbalanced):
    spec = _spec(["trial"], roles={
        "subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE,
    }, groups=["subject", "session"], color="subject")
    drawn = render_matplotlib(_figure(spec, unbalanced))
    labels = {text.get_text() for legend in drawn.legends for text in legend.get_texts()}
    assert labels == {"01", "02"}
    plt.close(drawn)


def test_mpl_points_sit_on_their_own_subjects_tick(unbalanced):
    """Subject is coloured AND a tick (colour is paint): subject 02's trials
    land inside subject 02's own bar — at the composed tick whose leaf is
    02 — never in subject 01's, and never outside a bar's span."""
    spec = _spec(["trial"], roles={
        "subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE,
    }, groups=["subject", "session"], color="subject")
    figure = _figure(spec, unbalanced)
    drawn = render_matplotlib(figure)
    collections = _overlay_points(drawn)
    assert len(collections) >= 2
    subject_at_tick = {
        index: str(key).split(LEAF_SEPARATOR)[-1] for index, key in enumerate(figure.x_order)
    }
    # Every trial value in the fixture is unique, so it names its subject.
    subject_of_value = {
        float(value): str(subject)
        for subject, value in unbalanced.frame[["subject", "M"]].itertuples(index=False)
    }
    placed = 0
    for c in collections:
        for x, y in c.get_offsets():
            tick = round(float(x))
            assert abs(x - tick) < 0.4, "a point strayed out of its tick's span"
            assert subject_at_tick[tick] == subject_of_value[float(y)]
            placed += 1
    assert placed == 7, "one point per trial"
    plt.close(drawn)


# --- plotly ------------------------------------------------------------------------


def test_plotly_overlay_is_lines_and_markers_when_joined(unbalanced):
    payload = render_plotly(_figure(_spec(["subject"]), unbalanced))
    traces = _overlay_traces(payload)
    assert len(traces) == 2
    assert {t["mode"] for t in traces} == {"lines+markers"}
    assert {t["name"] for t in traces} == {"01", "02"}
    assert all("subject=" in text for t in traces for text in t["customdata"])


def test_plotly_overlay_is_markers_when_not_joined(unbalanced):
    payload = render_plotly(_figure(_spec(["trial"]), unbalanced))
    traces = _overlay_traces(payload)
    assert {t["mode"] for t in traces} == {"markers"}
    assert sum(len(t["x"]) for t in traces) == 7


def test_plotly_axis_turns_positional_with_an_overlay(unbalanced):
    plain = render_plotly(_figure(_spec([]), unbalanced))["layout"]["xaxis"]
    assert plain["type"] == "category"
    overlaid = render_plotly(_figure(_spec(["subject"]), unbalanced))["layout"]["xaxis"]
    assert overlaid["type"] == "-"
    assert overlaid["tickvals"] == [0, 1] and overlaid["ticktext"] == ["pre", "post"]
    assert overlaid["range"] == [-0.5, 1.5]


def test_plotly_marks_are_placed_by_index_on_a_positional_axis(unbalanced):
    payload = render_plotly(_figure(_spec(["subject"]), unbalanced))
    bars = [t for t in payload["data"] if t["type"] == "bar"]
    assert bars and bars[0]["x"] == [0.0, 1.0]
    assert bars[0]["customdata"] == ["pre", "post"], "hover names the level, not the index"


def test_plotly_overlay_keeps_the_marks_legend_only(unbalanced):
    spec = _spec(["trial"], roles={
        "subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE,
    }, groups=["subject", "session"], color="subject")
    payload = render_plotly(_figure(spec, unbalanced))
    shown = [t["name"] for t in payload["data"] if t["showlegend"]]
    assert shown == ["01", "02"]
    assert {t["legendgroup"] for t in _overlay_traces(payload)} == {"01", "02"}


# --- parity ---------------------------------------------------------------------------


@pytest.mark.parametrize("show", [["subject"], ["trial"]])
def test_both_backends_place_every_point_at_the_same_x(unbalanced, show):
    spec = _spec(show, roles={
        "subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE,
    }, groups=["subject", "session"], color="subject") if show == ["trial"] else _spec(show)
    figure = _figure(spec, unbalanced)
    plotly_xs = sorted(round(x, 6) for t in _overlay_traces(render_plotly(figure)) for x in t["x"])
    drawn = render_matplotlib(figure)
    if figure.sample_join:
        mpl_xs = sorted(round(float(x), 6) for line in _overlay_lines(drawn) for x in line.get_xdata())
    else:
        mpl_xs = sorted(round(float(x), 6) for c in _overlay_points(drawn) for x, _ in c.get_offsets())
    plt.close(drawn)
    assert mpl_xs == plotly_xs


def test_a_box_with_an_overlay_still_draws_its_boxes(unbalanced):
    figure = _figure(_spec(["trial"], kind=PlotKind.BOX), unbalanced)
    payload = render_plotly(figure)
    assert [t["type"] for t in payload["data"]].count("box") == 1
    assert payload["data"][0]["x"] == [0.0, 0.0, 1.0, 1.0]
    drawn = render_matplotlib(figure)
    assert len(_overlay_points(drawn)) >= 1  # the points, on top of the boxes
    plt.close(drawn)


@pytest.mark.parametrize("kind", [PlotKind.BAR, PlotKind.BOX, PlotKind.VIOLIN, PlotKind.STRIP])
def test_plotly_overlay_never_paints_labels_on_the_marks(unbalanced, kind):
    """Regression: with the overlay on, hover strings once rode in every
    trace's ``text``, and plotly paints a bar trace's ``text`` onto the bars.
    Hover-only content belongs in ``customdata``; no trace of any kind may
    carry ``text`` (or a mode that would draw it)."""
    payload = render_plotly(_figure(_spec(["subject"], kind=kind), unbalanced))
    assert payload["data"], "nothing rendered"
    for trace in payload["data"]:
        assert "text" not in trace, f"{trace['type']} trace carries paintable text"
        assert "text" not in str(trace.get("mode", "")), f"{trace['type']} mode draws text"
        if "hovertemplate" in trace:
            assert "%{customdata}" in trace["hovertemplate"]
            assert "%{text}" not in trace["hovertemplate"]


# --- the colour is the ONLY grouping layer ------------------------------------------
#
# Regression (scidb.log 2026-09-21): grouping by ColName alone and colouring by
# ColName used to leave no tick layer (the coloured layer was pulled out of
# the ticks), so the marks sat at one unlabelled position, ``x_order`` was
# ``None``, the axis read as numeric, and both renderers dropped the overlay
# without a word. Since colour became paint the coloured layer IS the tick
# axis; these tests hold the overlay to it.


def _colour_only(show, **kwargs):
    return _spec(show, color="session", **kwargs)


def test_the_coloured_only_grouping_is_the_tick_axis(unbalanced):
    """Colour is paint: a grouping of one coloured layer is that layer's
    ticks, painted — not one unlabelled position with a dodge."""
    figure = _figure(_colour_only(["subject"]), unbalanced)
    assert figure.x_order == ["pre", "post"]
    assert figure.encoding.color and figure.color_order == ["pre", "post"]


def test_plotly_draws_the_overlay_when_the_colour_is_the_only_grouping(unbalanced):
    payload = render_plotly(_figure(_colour_only(["subject"]), unbalanced))
    traces = _overlay_traces(payload)
    # In the marks' colour a run never crosses colour levels (base.sample_groups):
    # one single-point run per subject per session, never joined.
    assert len(traces) == 4
    assert {t["mode"] for t in traces} == {"markers"}
    assert sum(len(t["x"]) for t in traces) == 4
    # Every point sits inside its own session's tick (0 = pre, 1 = post).
    for trace in traces:
        for x in trace["x"]:
            assert abs(x - round(x)) < 0.5 and round(x) in (0, 1)
    bars = [t for t in payload["data"] if t["type"] == "bar"]
    assert [b["x"] for b in bars] == [[0.0], [1.0]], "one bar per tick, painted"
    axis = payload["layout"]["xaxis"]
    assert axis["tickvals"] == [0, 1] and axis["range"] == [-0.5, 1.5]


def test_plotly_draws_the_overlay_with_its_own_colour_on_a_coloured_only_grouping(unbalanced):
    """The reported shape: colour by the grouping, points coloured by subject."""
    spec = _colour_only(["subject"], sample_color="subject", join_sample=True)
    payload = render_plotly(_figure(spec, unbalanced))
    traces = [t for t in payload["data"] if t["type"] == "scatter"]
    assert len(traces) == 2 and {t["name"] for t in traces} == {"01", "02"}
    assert {t["mode"] for t in traces} == {"lines+markers"}


def test_mpl_draws_the_overlay_when_the_colour_is_the_only_grouping(unbalanced):
    drawn = render_matplotlib(_figure(_colour_only(["subject"]), unbalanced))
    # Points only, split per mark colour (see the plotly twin above).
    assert _overlay_lines(drawn) == []
    xs = [float(x) for c in _overlay_points(drawn) for x, _ in c.get_offsets()]
    assert len(xs) == 4
    for x in xs:
        assert not np.isnan(x) and abs(x - round(x)) < 0.5
    # The bars themselves are placed too (they used to land at NaN on export).
    bars = [p for p in drawn.axes[0].patches if p.get_height() > 0]
    assert len(bars) == 2 and all(not np.isnan(p.get_x()) for p in bars)
    plt.close(drawn)


def test_a_dropped_overlay_is_said_out_loud(unbalanced, monkeypatch):
    """The guard both renderers share names the panel and the axis when it
    discards points, so a future silent drop shows in scidb.log."""
    from scistackplot.render import base

    figure = _figure(_colour_only(["subject"]), unbalanced)
    monkeypatch.setattr(figure, "x_order", None)
    warned: list[str] = []
    monkeypatch.setattr(base.Log, "warn", lambda msg, *a, **k: warned.append(msg % a))
    assert base.sample_dropped_reason(figure.panels[0], figure)
    assert render_plotly(figure)["data"] and _overlay_traces(render_plotly(figure)) == []
    assert warned and "NOT drawn" in warned[0] and "x_order=None" in warned[0]


# --- the span: a line never leaves its bracket (2026-09-21) ----------------------


@pytest.fixture
def nested() -> LongTable:
    """subject x session x trial, each record two ColName fields — the user's
    case: bars grouped [ColName, session], subjects shown and joined."""
    import itertools

    rows = []
    for subject, session, trial, field in itertools.product(
        ["01", "02"], ["pre", "post"], ["1", "2"], ["A", "B"]
    ):
        rows.append((subject, session, trial, field, float(len(rows))))
    frame = pd.DataFrame(rows, columns=["subject", "session", "trial", "ColName", "M"])
    return LongTable.from_frame(
        frame,
        factors=["subject", "session", "trial", "ColName"],
        measures=["M"],
        field_factors=["ColName"],
        name="M",
        schema_levels=["subject", "session", "trial"],
        level_order={"session": ["pre", "post"]},
    )


def _nested_spec(groups, show=("subject",), **kwargs) -> PlotSpec:
    roles = {
        "subject": Role.COLLAPSE, "session": Role.COLLAPSE,
        "trial": Role.COLLAPSE, "ColName": Role.COLLAPSE,
    }
    for name in groups:
        roles[name] = Role.GROUP
    base = dict(
        measures=["M"], roles=roles, groups=list(groups), kind=PlotKind.BAR,
        aggregate=Aggregation(error=ErrorBand.SD), show_sample=list(show),
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _runs(spec, table):
    from plot_geometry import mpl_sample_runs, plotly_sample_runs

    figure = _figure(spec, table)
    drawn = render_matplotlib(figure)
    mpl = mpl_sample_runs(drawn, figure)
    plt.close(drawn)
    return figure, mpl, plotly_sample_runs(render_plotly(figure), figure)


def test_the_overlay_frame_carries_the_run(nested):
    """`__run` is the bracket's value: subject 01 has one run per session."""
    from scistackplot.resolved import RUN

    figure = _figure(_nested_spec(["ColName", "session"]), nested)
    sample = figure.panels[0].sample
    assert set(sample[RUN]) == {"pre", "post"}
    assert sample.groupby([SERIES, RUN]).ngroups == 4, "2 subjects x 2 sessions"
    flat = _figure(_nested_spec(["session"]), nested).panels[0].sample
    assert set(flat[RUN]) == {""}, "no bracket: one run per identity"


@pytest.mark.parametrize("groups", [["ColName", "session"], ["session", "ColName"]])
def test_a_joined_line_spans_the_innermost_tick_inside_one_bracket(nested, groups):
    """Bars nested [inner, outer]: each subject's line joins its two points
    inside one bracket, and never runs on into the next bracket — the
    user's ask (2026-09-21). Both backends draw the same runs."""
    from plot_geometry import run_brackets

    figure, mpl, plotly = _runs(_nested_spec(groups), nested)
    assert figure.sample_join is True, figure.sample_join_reason
    assert mpl == plotly
    assert len(mpl) == 4, "2 subjects x 2 brackets"
    for run in mpl:
        assert run.joined and len(run.points) == 2
        assert len(run_brackets(run, n_layers=2)) == 1, run
    # A subject keeps its offset in every bracket: the offset is per identity.
    offsets = {run.points[0][1] for run in mpl}
    assert offsets == {-0.2, 0.2}


def test_a_trial_joins_across_the_columns_of_its_record(nested):
    """subject the bracket, ColName the span, trial shown: trial 1 of subject
    01 is one short line A -> B (the old whole-axis rule drew points)."""
    figure, mpl, plotly = _runs(_nested_spec(["ColName", "subject"], show=("trial",)), nested)
    assert figure.sample_join is True, figure.sample_join_reason
    assert mpl == plotly
    # 2 subjects x 2 sessions x 2 trials, each a 2-point line inside its subject.
    assert len(mpl) == 8 and all(run.joined and len(run.points) == 2 for run in mpl)


def test_points_only_runs_are_unchanged_by_the_bracket(nested):
    figure, mpl, plotly = _runs(_nested_spec(["session", "ColName"], show=("trial",)), nested)
    assert figure.sample_join is False
    assert not any(run.joined for run in mpl)
    assert sum(len(run.points) for run in mpl) == sum(len(run.points) for run in plotly) == 16


def test_a_flat_axis_still_joins_the_whole_axis(nested):
    """No bracket: the innermost tick is the only tick, so the line spans it all."""
    figure, mpl, plotly = _runs(_nested_spec(["session"]), nested)
    assert mpl == plotly and len(mpl) == 2
    assert all(run.joined and len(run.points) == 2 for run in mpl)
