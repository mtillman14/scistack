"""
An uncoloured grouping layer on a line or a band is told apart by DASH STYLE.

Decision D4 (2026-09-17): the coloured layer is the colour; every other series
layer composes into a dash id that gets a style from ``DASH_CYCLE`` — the same
style in every panel and every colour, decided once per figure
(``reduce._dash_styles``) so the two renderers cannot disagree, and listed in
the legend so a printed figure reads without hovering.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import pytest

from scistackplot import LongTable, PlotKind, PlotSpec, Role, render_plotly, resolve
from scistackplot.render.base import dash_levels, shows_legend
from scistackplot.resolved import DASH, DASH_CYCLE, MPL_DASHES, SERIES


@pytest.fixture
def two_layer_series() -> LongTable:
    """3 subjects x 2 sessions x 3 trials of a 1-D signal."""
    rows = []
    rng = np.random.default_rng(4)
    for subject in ["01", "02", "03"]:
        for session in ["pre", "post"]:
            for trial in ["1", "2", "3"]:
                rows.append(
                    {"subject": subject, "session": session, "trial": trial,
                     "S": list(rng.normal(size=6))}
                )
    return LongTable.from_frame(
        pd.DataFrame(rows), factors=["subject", "session", "trial"], measures=["S"],
        schema_levels=["subject", "session", "trial"], name="S",
        level_order={"session": ["pre", "post"]},
    )


def _spec(kind=PlotKind.BAND, **kwargs) -> PlotSpec:
    base = dict(
        measures=["S"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["subject", "session"],
        color="session",
        kind=kind,
    )
    base.update(kwargs)
    return PlotSpec(**base)


def test_the_uncoloured_layer_gets_a_dash_id(two_layer_series):
    (figure,) = resolve(_spec(), two_layer_series)
    frame = figure.panels[0].frame
    assert figure.encoding.dash == DASH
    assert set(frame[DASH]) == {"01", "02", "03"}, "the subject, not the session"
    # Composed outermost first: the session wraps the subject in this spec.
    assert set(frame[SERIES]) == {f"{t} | {s}" for s in ["01", "02", "03"] for t in ["pre", "post"]}


def test_styles_are_decided_once_per_figure(two_layer_series):
    (figure,) = resolve(_spec(), two_layer_series)
    assert figure.dash_styles == {"01": "solid", "02": "dash", "03": "dot"}
    assert list(figure.dash_styles.values()) == list(DASH_CYCLE[:3])
    assert figure.labels.dash == "subject"


def test_the_same_subject_keeps_its_dash_in_every_panel(two_layer_series):
    spec = _spec(roles={"subject": Role.GROUP, "session": Role.FACET, "trial": Role.COLLAPSE},
                 groups=["subject"], color=None)
    (figure,) = resolve(spec, two_layer_series)
    assert len(figure.panels) == 2
    assert figure.dash_styles == {"01": "solid", "02": "dash", "03": "dot"}
    for panel in figure.panels:
        assert set(panel.frame[DASH]) == {"01", "02", "03"}


def test_no_uncoloured_layer_means_no_dash(two_layer_series):
    spec = _spec(roles={"subject": Role.GROUP, "session": Role.COLLAPSE, "trial": Role.COLLAPSE},
                 groups=["subject"], color="subject")
    (figure,) = resolve(spec, two_layer_series)
    assert figure.encoding.dash is None
    assert figure.dash_styles == {}
    assert DASH not in figure.panels[0].frame.columns


def test_lines_dash_too(two_layer_series):
    (figure,) = resolve(_spec(kind=PlotKind.LINE), two_layer_series)
    assert figure.encoding.dash == DASH
    assert figure.dash_styles


def test_spaghetti_does_not_dash(scalar_table):
    spec = PlotSpec(
        measures=["StepLength"],
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["subject", "session"],
        kind=PlotKind.SPAGHETTI,
    )
    (figure,) = resolve(spec, scalar_table)
    assert figure.encoding.dash is None


def test_past_the_cycle_the_figure_warns(caplog):
    rows = [
        {"subject": f"{n:02d}", "trial": "1", "S": [1.0, 2.0, 3.0]}
        for n in range(1, len(DASH_CYCLE) + 3)
    ]
    table = LongTable.from_frame(
        pd.DataFrame(rows), factors=["subject", "trial"], measures=["S"],
        schema_levels=["subject", "trial"], name="S",
    )
    spec = PlotSpec(measures=["S"], roles={"subject": Role.GROUP, "trial": Role.COLLAPSE},
                    groups=["subject"], kind=PlotKind.BAND)
    with caplog.at_level(logging.WARNING, logger="scistackplot"):
        (figure,) = resolve(spec, table)
    assert len(figure.dash_styles) == len(DASH_CYCLE) + 2
    assert figure.dash_styles["01"] == figure.dash_styles["07"], "the cycle repeats"
    assert any("dash styles" in r.getMessage() for r in caplog.records)
    # Still drawn dashed, but not listed: repeating styles name nothing.
    assert dash_levels(figure) == []
    assert not [t for t in render_plotly(figure)["data"] if t.get("legendgroup", "").startswith("dash:")]


# --- renderers ---------------------------------------------------------------


def test_plotly_draws_each_series_in_its_dash(two_layer_series):
    (figure,) = resolve(_spec(), two_layer_series)
    payload = render_plotly(figure)
    lines = [t for t in payload["data"] if t.get("mode") == "lines" and t.get("hovertext")]
    by_series = {t["hovertext"]: t["line"]["dash"] for t in lines}
    assert by_series["pre | 01"] == by_series["post | 01"] == "solid"
    assert by_series["pre | 02"] == "dash"
    assert by_series["post | 03"] == "dot"


def test_plotly_lists_the_dash_styles_in_the_legend(two_layer_series):
    (figure,) = resolve(_spec(), two_layer_series)
    payload = render_plotly(figure)
    shown = {t["name"]: t for t in payload["data"] if t.get("showlegend")}
    assert {"pre", "post", "01", "02", "03"} <= set(shown)
    assert shown["02"]["line"]["dash"] == "dash"
    assert shown["02"]["legendgroup"] == "dash:02"


def test_a_band_per_series_in_plotly(two_layer_series):
    (figure,) = resolve(_spec(), two_layer_series)
    fills = [t for t in render_plotly(figure)["data"] if t.get("fill") == "toself"]
    assert len(fills) == 6, "one shaded region per (session, subject)"


def test_the_legend_shows_for_dashes_alone(two_layer_series):
    """No colour at all, three dash styles: that is a legend worth drawing."""
    spec = _spec(roles={"subject": Role.GROUP, "session": Role.COLLAPSE, "trial": Role.COLLAPSE},
                 groups=["subject"], color=None)
    (figure,) = resolve(spec, two_layer_series)
    assert figure.encoding.color is None
    assert dash_levels(figure) == ["01", "02", "03"]
    assert shows_legend(figure)


def test_matplotlib_draws_the_same_dashes(two_layer_series):
    matplotlib = pytest.importorskip("matplotlib")
    from scistackplot import render_matplotlib

    (figure,) = resolve(_spec(), two_layer_series)
    drawn = render_matplotlib(figure)
    ax = next(a for a in drawn.axes if a.get_visible())
    styles = {line.get_linestyle() for line in ax.get_lines() if len(line.get_xdata())}
    assert styles == {"-", "--", ":"}, styles
    labels = {t.get_text() for t in drawn.legends[0].get_texts()}
    assert {"pre", "post", "01", "02", "03"} <= labels
    assert MPL_DASHES["solid"] == "-"
    matplotlib.pyplot.close(drawn)
