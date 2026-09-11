"""Y limits split by a chosen set of factors.

The user's requirement, 2026-09-11: "I need the option to set them at any schema
level — per lowest schema level across all facets, or per lowest schema level
per facet (true auto-scaling), or no schema levels, in which case all plots
across the whole dataset would have the same y limits."

One control expresses all three, and the two ends are the interesting ones:

* **nothing selected** is the hard case, not the easy one — one range across
  every figure of a fan-out, including the figures ``resolve_one`` deliberately
  never builds. It has to come off the table rather than off the figures;
* **everything selected** is per-panel autoscale, which also has to switch OFF
  the renderers' axis sharing, or one panel's range silently drags the rest.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import pytest

from scistackplot import (
    Aggregation,
    ErrorBand,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    Statistic,
    YAxis,
    render_matplotlib,
    render_plotly,
    resolve,
    resolve_one,
)
from scistackplot.render.base import shares_y_axis
from scistackplot.resolved import Y, Y_HIGH, Y_LOW

matplotlib = pytest.importorskip("matplotlib")

LAYER = "scistackplot"


@pytest.fixture
def spread_table() -> LongTable:
    """Two subjects x two muscles, on deliberately different scales.

    Subject 01 spans 0-1, subject 02 spans 0-100, and within each subject the
    two muscles differ tenfold again. Every scope therefore produces visibly
    different numbers — an assertion that passes by coincidence is not possible
    on this fixture.
    """
    rows = []
    for subject, scale in (("01", 1.0), ("02", 100.0)):
        for muscle, factor in (("TA", 1.0), ("SOL", 0.1)):
            # The two trials differ, so a mean has a real spread around it and
            # the BAND test below measures something. Identical trials give
            # SD=0 and a band that coincides with its centre line, which would
            # pass every assertion without testing any of them.
            for trial, wobble in (("1", 0.8), ("2", 1.0)):
                peak = scale * factor * wobble
                rows.append(
                    {
                        "subject": subject,
                        "muscle": muscle,
                        "trial": trial,
                        "EMG": [0.0, 0.5 * peak, peak],
                    }
                )
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "muscle", "trial"],
        measures=["EMG"],
        name="EMG",
        schema_levels=["subject", "muscle", "trial"],
    )


def _spec(scope=None, **overrides) -> PlotSpec:
    base = dict(
        measures=["EMG"],
        roles={
            "subject": Role.ITERATE,
            "muscle": Role.FACET,
            "trial": Role.FREE,
        },
        kind=PlotKind.LINE,
        y_axis=YAxis(scope=list(scope or [])),
    )
    base.update(overrides)
    return PlotSpec(**base)


def _all_panels(figures):
    return [panel for figure in figures for panel in figure.panels]


# --- the three scopes -------------------------------------------------------


def test_no_scope_is_one_range_for_the_whole_dataset(spread_table):
    """The headline case. Every panel of every figure, one range — including
    the figures this call had to build to check."""
    figures = resolve(_spec(), spread_table)

    limits = {panel.y_limits for panel in _all_panels(figures)}

    assert len(limits) == 1
    low, high = limits.pop()
    # Subject 02's TA reaches 100; that is what every panel is scaled to.
    assert high > 100.0
    assert low < 0.0  # padding below the zero floor


def test_scoping_by_the_iterate_factor_separates_the_figures(spread_table):
    """One range per subject: identical within a figure, different between."""
    figures = resolve(_spec(["subject"]), spread_table)

    per_figure = [{panel.y_limits for panel in f.panels} for f in figures]

    assert all(len(limits) == 1 for limits in per_figure), "facets must agree"
    first, second = (limits.pop() for limits in per_figure)
    assert first != second
    # Subject 01 tops out at 1.0, subject 02 at 100.0.
    assert first[1] < 10.0 < second[1]


def test_scoping_by_every_panel_factor_autoscales_each_panel(spread_table):
    """True per-panel autoscale: each panel brackets exactly its own data."""
    figures = resolve(_spec(["subject", "muscle"]), spread_table)

    for figure in figures:
        for panel in figure.panels:
            drawn = pd.to_numeric(panel.frame[Y], errors="coerce").dropna()
            low, high = panel.y_limits
            assert low <= drawn.min()
            assert high >= drawn.max()
        # And the panels of one figure now genuinely differ (TA vs SOL, 10x).
        assert len({panel.y_limits for panel in figure.panels}) == 2


def test_a_scope_across_figures_holds_for_a_figure_built_alone(spread_table):
    """The reason the limits are computed off the TABLE.

    ``resolve_one`` builds one figure and never looks at the others — that is
    the optimisation the interactive panel depends on. A global scope still has
    to produce the global range, so paging through the fan-out must not rescale
    the axis under the user.
    """
    spec = _spec()

    alone, _labels, _index = resolve_one(spec, spread_table, 0)
    together = resolve(spec, spread_table)[0]

    assert alone.panels[0].y_limits == together.panels[0].y_limits
    # And it is the GLOBAL range, not this figure's own.
    assert alone.panels[0].y_limits[1] > 100.0


# --- overrides --------------------------------------------------------------


def test_manual_limits_beat_every_scope(spread_table):
    """Both ends pinned: the scope still names a per-panel split, and every
    panel ignores it."""
    spec = _spec(y_axis=YAxis(scope=["subject", "muscle"], minimum=-5.0, maximum=5.0))

    figures = resolve(spec, spread_table)

    assert {panel.y_limits for panel in _all_panels(figures)} == {(-5.0, 5.0)}


def test_one_end_can_be_pinned_while_the_other_is_computed(spread_table):
    """A floor of zero with a computed ceiling — the common case, and the
    reason the two ends are independent rather than one range."""
    spec = _spec(y_axis=YAxis(scope=[], minimum=0.0))

    panel = resolve(spec, spread_table)[0].panels[0]

    assert panel.y_limits[0] == 0.0
    assert panel.y_limits[1] > 100.0


def test_an_inverted_manual_range_is_ordered_not_obeyed(spread_table):
    """A minimum above the maximum is a typo. matplotlib would flip the axis
    and draw the figure upside down with nothing to say why."""
    spec = _spec(y_axis=YAxis(scope=[], minimum=10.0, maximum=-10.0))

    panel = resolve(spec, spread_table)[0].panels[0]

    assert panel.y_limits == (-10.0, 10.0)


# --- what may separate limits ----------------------------------------------


def test_a_within_panel_factor_is_dropped_from_the_scope(spread_table, caplog):
    """A COLOR or FREE factor lives inside one panel, so separating limits by
    it asks one axis for two ranges — there is no figure that satisfies it."""
    spec = _spec(
        ["trial"],
        roles={
            "subject": Role.ITERATE,
            "muscle": Role.FACET,
            "trial": Role.COLOR,
        },
    )

    with caplog.at_level(logging.WARNING, logger=LAYER):
        figures = resolve(spec, spread_table)

    assert "trial" in "\n".join(r.getMessage() for r in caplog.records)
    assert figures[0].y_scope == []
    # Dropped, not obeyed and not fatal: the figure falls back to one range.
    assert len({panel.y_limits for panel in _all_panels(figures)}) == 1


# --- aggregating kinds ------------------------------------------------------


def test_a_band_reaches_past_its_centre_line(spread_table):
    """A BAND draws mean ± spread, so the limits have to cover the BAND.

    Computing them from the raw data would be wrong in both directions: the
    mean of several trials sits inside the data, and the band around it can sit
    outside. A limit that clips the band it was computed for is the visible
    symptom.
    """
    spec = _spec(
        kind=PlotKind.BAND,
        aggregate=Aggregation(statistic=Statistic.MEAN, error=ErrorBand.SD),
    )

    figure = resolve(spec, spread_table)[0]

    low, high = figure.panels[0].y_limits
    for panel in figure.panels:
        drawn_high = pd.to_numeric(panel.frame[Y_HIGH], errors="coerce").dropna()
        drawn_low = pd.to_numeric(panel.frame[Y_LOW], errors="coerce").dropna()
        if len(drawn_high):
            assert high >= drawn_high.max(), "the band is clipped by its own axis"
            assert low <= drawn_low.min()


# --- both renderers apply the same numbers ---------------------------------


def test_shared_limits_let_the_renderers_link_the_axes(spread_table):
    figures = resolve(_spec(["subject"]), spread_table)
    figure = figures[0]

    assert shares_y_axis(figure) is True
    assert figure.y_limits is not None

    payload = render_plotly(figure)
    assert payload["layout"]["yaxis"]["range"] == list(figure.y_limits)
    # A second panel MATCHES the first rather than repeating the range.
    assert payload["layout"]["yaxis2"].get("matches") == "y"


def test_per_panel_limits_stop_both_renderers_sharing_an_axis(spread_table):
    """The half of this that is easy to get wrong.

    matplotlib's `sharey` and plotly's `matches` tie the axes together, so a
    per-panel range would be overwritten by whichever panel was drawn last.
    Both have to be switched off, and the figure-level `y_limits` is how they
    learn that: it is None exactly when the panels disagree.
    """
    figure = resolve(_spec(["subject", "muscle"]), spread_table)[0]

    assert shares_y_axis(figure) is False
    assert figure.y_limits is None

    payload = render_plotly(figure)
    assert "matches" not in payload["layout"]["yaxis2"]
    ranges = [
        payload["layout"]["yaxis"]["range"],
        payload["layout"]["yaxis2"]["range"],
    ]
    assert ranges[0] != ranges[1]

    mpl = render_matplotlib(figure)
    try:
        drawn = [tuple(round(v, 6) for v in ax.get_ylim()) for ax in mpl.axes]
        expected = [
            tuple(round(v, 6) for v in panel.y_limits) for panel in figure.panels
        ]
        assert sorted(drawn) == sorted(expected)
    finally:
        matplotlib.pyplot.close(mpl)


def test_both_renderers_apply_the_same_numbers(spread_table):
    """Renderer parity, same shape as the legend test: the interactive view and
    the exported PNG cannot disagree about the axis."""
    figure = resolve(_spec(["subject"]), spread_table)[0]

    payload = render_plotly(figure)
    mpl = render_matplotlib(figure)
    try:
        from_plotly = [round(v, 6) for v in payload["layout"]["yaxis"]["range"]]
        from_mpl = [round(v, 6) for v in mpl.axes[0].get_ylim()]
        assert from_plotly == from_mpl
    finally:
        matplotlib.pyplot.close(mpl)


def test_independently_scaled_panels_keep_their_tick_labels(spread_table):
    """Hiding inner tick labels is only honest when they would be identical.

    A grid of panels at different scales, numbered down the left column only,
    reads as one shared scale — the exact misread per-panel limits exist to
    avoid. Asserted on the shared rule rather than on drawn Text objects, so it
    pins the decision both renderers make instead of one backend's internals.
    """
    from scistackplot.render.base import shows_y_labels

    shared = resolve(_spec(["subject"]), spread_table)[0]
    separate = resolve(_spec(["subject", "muscle"]), spread_table)[0]

    # The second column: suppressed when it repeats the first, kept when it
    # does not.
    assert shows_y_labels(shared, 0, 1) is False
    assert shows_y_labels(separate, 0, 1) is True
    # The leftmost column is always labelled, either way.
    assert shows_y_labels(shared, 0, 0) is True
    assert shows_y_labels(separate, 0, 0) is True


# --- serialization ----------------------------------------------------------


def test_the_scope_survives_a_round_trip():
    spec = PlotSpec(
        measures=["EMG"],
        y_axis=YAxis(scope=["subject"], minimum=0.0, maximum=2.5),
    )

    restored = PlotSpec.from_json(spec.to_json())

    assert restored.y_axis.scope == ["subject"]
    assert restored.y_axis.minimum == 0.0
    assert restored.y_axis.maximum == 2.5


def test_an_empty_limit_box_reads_as_compute_it():
    """The GUI sends "" for a cleared field; `float("")` would raise, and
    treating it as 0 would pin the axis at zero instead of releasing it."""
    restored = PlotSpec.from_dict(
        {"measures": ["EMG"], "y_axis": {"scope": [], "minimum": "", "maximum": None}}
    )

    assert restored.y_axis.minimum is None
    assert restored.y_axis.maximum is None
    assert restored.y_axis.is_manual is False


def test_a_spec_written_before_this_stage_still_loads():
    """`share_y` is gone. An older saved spec must not raise on the way in —
    it simply stops carrying an opinion, and the default (one range for
    everything) is what `share_y=True` meant for a single figure anyway."""
    restored = PlotSpec.from_dict(
        {"measures": ["EMG"], "facet": {"n_cols": 2, "share_x": True, "share_y": False}}
    )

    assert restored.facet.n_cols == 2
    assert restored.y_axis.scope == []
