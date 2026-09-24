"""
Stage 4 of the label-legibility plan: the plotly preview applies the EXPORT's
label and legend decisions instead of deciding (or estimating) its own.

``layout_decisions`` lays the figure out with matplotlib at a size and reads
back what was decided; ``render_plotly(decisions=...)`` draws exactly that.
One owner, so the preview cannot disagree with the file.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

pytest.importorskip("matplotlib")

from scistackplot import layout_decisions, render_plotly, resolve  # noqa: E402
from scistackplot.render.plotly_ import PREVIEW_FONT_FAMILY, X_GROUP_TAG  # noqa: E402
from test_mpl_label_fit import (  # noqa: E402,F401
    _graph1,
    _graph2,
    _single_axis_spec,
    _single_axis_table,
    gait_table,
)


def _one(spec, table):
    (resolved,) = resolve(spec, table)
    return resolved


def _x_axes(payload):
    return {k: v for k, v in payload["layout"].items() if k.startswith("xaxis")}


def test_decisions_are_the_exports_at_the_size_asked(gait_table):
    resolved = _one(replace(_graph2(), sample_color="subject"), gait_table)
    at_own = layout_decisions(resolved)
    assert (at_own["width_in"], at_own["height_in"]) == (8.0, 5.0)
    assert at_own["legend"]["below"]  # the x labels need the width at 8in
    wide = layout_decisions(resolved, width_in=16.0, height_in=5.0)
    assert wide["width_in"] == 16.0
    assert wide["ticks"] is not None and at_own["ticks"] is not None


def test_the_preview_draws_the_tick_decision(gait_table):
    ids = [f"SS{i:02d}" for i in range(1, 41)]
    resolved = _one(_single_axis_spec(4.0), _single_axis_table(ids))
    decisions = layout_decisions(resolved)
    ticks = decisions["ticks"]
    assert ticks.prefix == "SS" and ticks.every > 1
    payload = render_plotly(resolved, decisions=decisions)
    (axis,) = _x_axes(payload).values()
    assert axis["ticktext"] == list(ticks.rows[0])
    assert axis["tickangle"] == -ticks.rotation
    assert axis["tickfont"] == {"size": ticks.font_pt}
    assert payload["layout"]["meta"]["label_fit"]["fits"] is ticks.fits


def test_the_preview_draws_the_legend_decision(gait_table):
    resolved = _one(replace(_graph2(), sample_color="subject"), gait_table)
    decisions = layout_decisions(resolved)
    legend = render_plotly(resolved, decisions=decisions)["layout"]["legend"]
    assert decisions["legend"]["below"]
    assert legend["orientation"] == "h" and legend["yref"] == "container"
    assert legend["font"] == {"size": decisions["legend"]["font_pt"]}


def test_a_wrapped_legend_title_is_wrapped_in_the_preview_too():
    from test_mpl_legend_fit import _roomy_x_table
    from scistackplot import PlotKind, PlotSpec, Role, StyleOptions

    spec = PlotSpec(
        measures=["M"],
        roles={"session": Role.GROUP, "subject": Role.COLLAPSE},
        groups=["session"],
        color="session",
        kind=PlotKind.BAR,
        show_sample=["subject"],
        sample_color="subject",
        style=StyleOptions(width=5.0, height=4.0),
    )
    resolved = _one(spec, _roomy_x_table())
    decisions = layout_decisions(resolved)
    assert "wrap_title" in decisions["legend"]["steps"]
    title = render_plotly(resolved, decisions=decisions)["layout"]["legend"]["title"]["text"]
    assert title == "session /<br>subject"


def test_bracket_labels_take_the_bracket_decision(gait_table):
    resolved = _one(_graph1(), gait_table)
    decisions = layout_decisions(resolved)
    brackets = decisions["brackets"]
    payload = render_plotly(resolved, decisions=decisions)
    tagged = [
        a for a in payload["layout"]["annotations"] if str(a.get("name", "")).startswith(X_GROUP_TAG)
    ]
    assert tagged
    assert {a["font"]["size"] for a in tagged} == {brackets.font_pt}


def _bracket_rules(payload):
    return [
        s for s in payload["layout"].get("shapes", [])
        if str(s.get("name", "")).startswith(X_GROUP_TAG)
    ]


def test_labelled_brackets_keep_their_rules_in_the_preview(gait_table):
    resolved = _one(_graph2(), gait_table)
    payload = render_plotly(resolved, decisions=layout_decisions(resolved))
    assert _bracket_rules(payload)


def test_a_blanked_bracket_row_draws_no_rules_in_the_preview(gait_table):
    """graph2 + hide_legend_ticks: InterventionGroup is the brackets AND the
    colour, so the export blanks those labels — and a bare rule names nothing."""
    resolved = _one(_graph2(hide_legend_ticks=True), gait_table)
    payload = render_plotly(resolved, decisions=layout_decisions(resolved))
    assert not _bracket_rules(payload)


def test_export_size_is_drawn_at_its_size(gait_table):
    resolved = _one(_graph1(), gait_table)
    payload = render_plotly(resolved, fixed_size_px=(432, 324))
    layout = payload["layout"]
    assert (layout["width"], layout["height"], layout["autosize"]) == (432, 324, False)
    assert layout["meta"]["fixed_size"] == [432, 324]


def test_the_preview_uses_the_font_the_decisions_were_measured_in(gait_table):
    payload = render_plotly(_one(_graph1(), gait_table))
    assert payload["layout"]["font"]["family"] == PREVIEW_FONT_FAMILY


def test_without_decisions_the_preview_is_unchanged(gait_table):
    """Library callers and older panels: no decisions, upright, pane-filling."""
    payload = render_plotly(_one(_graph1(), gait_table))
    assert all(axis["tickangle"] == 0 for axis in _x_axes(payload).values())
    assert "width" not in payload["layout"]
    assert "label_fit" not in payload["layout"]["meta"]
