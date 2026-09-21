"""
"Show sample" — Stage 3: both renderers draw the overlay in the same place.

Placement is one arithmetic (``render.base.sample_positions``): tick index +
the mark's dodge slot + the identity's offset. Joined identities are lines
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
from scistackplot.render.base import dodge_offset, dodge_width
from scistackplot.resolved import SERIES, X


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


def test_mpl_points_sit_inside_their_colour_slot(unbalanced):
    """Subject is the colour: subject 02's trials must land inside subject
    02's bar — the second of two dodge slots — never in subject 01's."""
    spec = _spec(["trial"], roles={
        "subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE,
    }, groups=["subject", "session"], color="subject")
    figure = _figure(spec, unbalanced)
    drawn = render_matplotlib(figure)
    collections = _overlay_points(drawn)
    assert len(collections) >= 2
    slot_1 = dodge_offset(1, 2)
    half = dodge_width(2) / 2
    # The last collection drawn is subject 02's (colour order), at x=pre (0).
    xs = [x for c in collections for x, _ in c.get_offsets() if abs(x - slot_1) < half]
    assert xs, "no point landed in the second slot"
    for c in collections:
        for x, _ in c.get_offsets():
            tick = round(float(x))
            assert abs(x - tick) < 0.4, "a point strayed out of its tick's span"
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
# ColName leaves no tick layer, so the marks sit at the one unlabelled position
# (``resolved.UNLABELLED_X``). ``x_order`` used to be ``None`` there, the axis
# read as numeric, and both renderers dropped the overlay without a word —
# plotly still drew the bars off the ``""`` strings, so the bars "worked".


def _colour_only(show, **kwargs):
    return _spec(show, color="session", **kwargs)


def test_colour_only_figure_has_the_unlabelled_axis_level(unbalanced):
    from scistackplot.resolved import UNLABELLED_X

    figure = _figure(_colour_only(["subject"]), unbalanced)
    assert figure.x_order == [UNLABELLED_X]
    assert figure.encoding.color and figure.color_order == ["pre", "post"]


def test_plotly_draws_the_overlay_when_the_colour_is_the_only_grouping(unbalanced):
    payload = render_plotly(_figure(_colour_only(["subject"]), unbalanced))
    traces = _overlay_traces(payload)
    # In the marks' colour a run never crosses colour slots (base.sample_groups):
    # one single-point run per subject per session, never joined.
    assert len(traces) == 4
    assert {t["mode"] for t in traces} == {"markers"}
    assert sum(len(t["x"]) for t in traces) == 4
    # Every point sits inside a dodge slot of the single tick at 0.
    for trace in traces:
        for x in trace["x"]:
            assert abs(x) < 0.5
    bars = [t for t in payload["data"] if t["type"] == "bar"]
    assert [b["x"] for b in bars] == [[0.0], [0.0]]
    axis = payload["layout"]["xaxis"]
    assert axis["tickvals"] == [0] and axis["range"] == [-0.5, 0.5]


def test_plotly_draws_the_overlay_with_its_own_colour_and_no_tick_layer(unbalanced):
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
        assert not np.isnan(x) and abs(x) < 0.5
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
