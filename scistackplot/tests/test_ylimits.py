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
import math

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
from scistackplot.resolved import SERIES, Y, Y_HIGH, Y_LOW

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


#: Kinds that draw the sample (a distribution or a summary of it) rather
#: than one mark per row — for these the default spec collapses `trial`.
SAMPLE_KINDS = (PlotKind.BAND, PlotKind.BAR, PlotKind.BOX, PlotKind.VIOLIN)


def _spec(scope=None, **overrides) -> PlotSpec:
    """A figure per subject, a panel per muscle. `trial` is one line per
    trial for a line plot and the SAMPLE (collapsed) for the kinds that need
    one — the two readings the old FREE role used to fold together."""
    kind = overrides.get("kind", PlotKind.LINE)
    base = dict(
        measures=["EMG"],
        roles={
            "subject": Role.ITERATE,
            "muscle": Role.FACET,
            "trial": Role.COLLAPSE if kind in SAMPLE_KINDS else Role.GROUP,
        },
        kind=kind,
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
    """A GROUP or COLLAPSE factor lives inside one panel, so separating limits
    by it asks one axis for two ranges — there is no figure that satisfies it."""
    spec = _spec(
        ["trial"],
        roles={
            "subject": Role.ITERATE,
            "muscle": Role.FACET,
            "trial": Role.GROUP,
        },
        color="trial",
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


# ---------------------------------------------------------------------------
# The invariant: nothing a panel draws lies outside its own limits
# ---------------------------------------------------------------------------
#
# Added 2026-09-14 after "data is often cut off vertically". The limits used to
# compute the statistic at SCOPE granularity — unticking `subject` pooled every
# subject into one mean ± SEM whose range collapsed around the grand mean, and
# every subject's own band fell outside it. The fixture below is built so that
# pooled-SEM is narrower than any one panel's band; the test failed on it
# before the fix for every scope that omitted a panel factor.


@pytest.fixture
def three_level_table() -> LongTable:
    """Three subjects at three well-separated levels (~10, ~5, ~0), two
    muscles a little apart, five nearly identical trials each.

    Nearly identical trials give every panel a NARROW band; well-separated
    subjects give the pooled statistic a WIDE spread and — once divided by the
    pooled n — a SEM/CI95 range that covers none of the panels.
    """
    rows = []
    for subject, level in (("01", 10.0), ("02", 5.0), ("03", 0.0)):
        for muscle, offset in (("TA", 0.0), ("SOL", 0.5)):
            for trial, wobble in enumerate((0.98, 0.99, 1.0, 1.01, 1.02), start=1):
                base = level + offset
                rows.append(
                    {
                        "subject": subject,
                        "muscle": muscle,
                        "trial": str(trial),
                        "EMG": [base * wobble, base * wobble + 0.1, base * wobble + 0.2],
                    }
                )
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "muscle", "trial"],
        measures=["EMG"],
        name="EMG",
        schema_levels=["subject", "muscle", "trial"],
    )


SCOPES = [[], ["subject"], ["muscle"], ["subject", "muscle"]]
KINDS = [PlotKind.LINE, PlotKind.SCATTER, PlotKind.BOX, PlotKind.BAND, PlotKind.BAR]
BANDS = [
    (Statistic.MEAN, ErrorBand.SD),
    (Statistic.MEAN, ErrorBand.SEM),
    (Statistic.MEAN, ErrorBand.CI95),
    (Statistic.MEAN, ErrorBand.IQR),
    (Statistic.MEDIAN, ErrorBand.IQR),
]


def _drawn_extent(panel) -> tuple[float, float] | None:
    """The lowest and highest value a panel actually draws."""
    columns = [c for c in (Y, Y_LOW, Y_HIGH) if c in panel.frame.columns]
    values = pd.concat(
        [pd.to_numeric(panel.frame[c], errors="coerce") for c in columns]
    ).dropna()
    if values.empty:
        return None
    return float(values.min()), float(values.max())


def _assert_within_limits(figures, *, label: str) -> None:
    checked = 0
    for figure in figures:
        for panel in figure.panels:
            drawn = _drawn_extent(panel)
            if drawn is None:
                continue
            assert panel.y_limits is not None, f"{label}: panel {panel.key} has no limits"
            low, high = panel.y_limits
            assert low <= drawn[0], f"{label}: {panel.key} floor {low} above drawn {drawn[0]}"
            assert high >= drawn[1], f"{label}: {panel.key} ceiling {high} below drawn {drawn[1]}"
            checked += 1
    assert checked, f"{label}: nothing was drawn"


@pytest.mark.parametrize("statistic,error", BANDS, ids=[f"{s.value}-{e.value}" for s, e in BANDS])
@pytest.mark.parametrize("kind", KINDS, ids=[k.value for k in KINDS])
@pytest.mark.parametrize("scope", SCOPES, ids=["global", "subject", "muscle", "panel"])
def test_every_panel_draws_inside_its_own_limits(three_level_table, scope, kind, statistic, error):
    """For every scope, every kind and every error band: the statistic is
    computed at the granularity it is DRAWN at, and the scope only decides
    which panels share a range."""
    spec = _spec(
        scope,
        kind=kind,
        aggregate=Aggregation(statistic=statistic, error=error),
    )
    figures = resolve(spec, three_level_table)
    _assert_within_limits(figures, label=f"{kind.value}/{scope}/{statistic.value}±{error.value}")


def test_a_shared_scope_is_the_union_of_the_panels_it_shares(three_level_table):
    """Scope [] on a BAND ± SEM: the range must reach subject 01's band near
    10 AND subject 03's near 0 — not the pooled mean ± pooled SEM, which sat
    between them and covered neither."""
    spec = _spec([], kind=PlotKind.BAND, aggregate=Aggregation(error=ErrorBand.SEM))
    figures = resolve(spec, three_level_table)

    limits = {panel.y_limits for figure in figures for panel in figure.panels}
    assert len(limits) == 1, "one range everywhere"
    low, high = limits.pop()
    # Subject 03 TA's centre at position 0 is 0.0; subject 01 SOL's at
    # position 2 is 10.7. The pooled statistic reached neither.
    assert low <= 0.0
    assert high >= 10.7


def test_unticking_a_factor_never_narrows_a_panel_below_its_own_range(three_level_table):
    """Sharing can only WIDEN a panel's range: its per-panel limits are the
    tightest, and every coarser scope contains them."""
    band = dict(kind=PlotKind.BAND, aggregate=Aggregation(error=ErrorBand.SEM))
    tight = {
        (figure.figure_label, tuple(panel.key.items())): panel.y_limits
        for figure in resolve(_spec(["subject", "muscle"], **band), three_level_table)
        for panel in figure.panels
    }
    for scope in ([], ["subject"], ["muscle"]):
        for figure in resolve(_spec(scope, **band), three_level_table):
            for panel in figure.panels:
                own = tight[(figure.figure_label, tuple(panel.key.items()))]
                low, high = panel.y_limits
                assert low <= own[0] and high >= own[1], (scope, panel.key)


def test_the_centre_line_is_inside_the_limits_when_it_leaves_its_iqr():
    """MEAN with an IQR band on skewed data: the mean sits above the third
    quartile, and the line drawn through it was clipped while the band was
    covered."""
    rows = [
        {"subject": "01", "trial": str(i), "EMG": [value] * 3}
        for i, value in enumerate((0.0, 0.0, 0.0, 0.0, 10.0), start=1)
    ]
    table = LongTable.from_frame(
        pd.DataFrame(rows), factors=["subject", "trial"], measures=["EMG"], name="EMG"
    )
    spec = PlotSpec(
        measures=["EMG"],
        roles={"subject": Role.ITERATE, "trial": Role.COLLAPSE},
        kind=PlotKind.BAND,
        aggregate=Aggregation(statistic=Statistic.MEAN, error=ErrorBand.IQR),
    )
    figure = resolve(spec, table)[0]
    panel = figure.panels[0]
    centre = pd.to_numeric(panel.frame[Y]).max()
    assert centre == pytest.approx(2.0)
    assert panel.frame[Y_HIGH].max() == pytest.approx(0.0)
    assert panel.y_limits[1] >= centre


def test_a_bar_chart_keeps_zero_in_view(three_level_table):
    """Bars rise from zero; a range that brackets only the bar tops cuts
    every bar off at its base."""
    spec = _spec(["subject", "muscle"], kind=PlotKind.BAR, aggregate=Aggregation(error=ErrorBand.SD))
    for figure in resolve(spec, three_level_table):
        for panel in figure.panels:
            low, high = panel.y_limits
            assert low <= 0.0 <= high, panel.key


def test_a_collapsed_line_is_scaled_to_the_sample_lines_it_draws(three_level_table):
    """trial is the only collapsed key, so it is the SAMPLE: the line draws one
    line per trial (schema-level parity, 2026-09-19), and the limits bracket
    those lines — not the per-position means the deleted `final` step drew."""
    spec = _spec(
        ["subject", "muscle"],
        kind=PlotKind.LINE,
        roles={"subject": Role.ITERATE, "muscle": Role.FACET, "trial": Role.COLLAPSE},
    )
    figures = resolve(spec, three_level_table)
    _assert_within_limits(figures, label="collapsed line")
    # Subject 01, TA: five trial lines reaching 10.2 x 1.02 ... + 0.2 = 10.4.
    top = next(p for p in figures[0].panels if p.key["muscle"] == "TA")
    assert top.frame[SERIES].nunique() == 5, "one line per trial"
    drawn = _drawn_extent(top)
    assert drawn[1] == pytest.approx(10.4)
    assert top.y_limits[1] == pytest.approx(drawn[1] + 0.05 * (drawn[1] - drawn[0]))


# --- a kind switch on a cached plan -----------------------------------------


def test_switching_kind_on_a_cached_plan_recomputes_the_limits(three_level_table):
    """LINE -> BAND reuses the plan (deliberately: kind is not in its key) but
    must NOT reuse the line's limits — the band draws mean ± SD, which sits
    outside the observations for small n. Until 2026-09-14 it did."""
    from scistackplot import reduce as reduce_mod

    reduce_mod._plan_cache.clear()
    # The same roles for both — the plan is keyed by them — so the line is
    # the mean over trials and the band is that mean ± SD.
    collapsed = {"subject": Role.ITERATE, "muscle": Role.FACET, "trial": Role.COLLAPSE}
    line = _spec([], kind=PlotKind.LINE, roles=collapsed)
    band = _spec([], kind=PlotKind.BAND, aggregate=Aggregation(error=ErrorBand.SD), roles=collapsed)

    resolve_one(line, three_level_table, 0)
    figure, _, _ = resolve_one(band, three_level_table, 0)

    assert len(reduce_mod._plan_cache) == 1, "the plan was shared"
    _assert_within_limits([figure], label="band after line")
    # And back again: the line's own limits, not the band's.
    figure, _, _ = resolve_one(line, three_level_table, 0)
    _assert_within_limits([figure], label="line after band")


def test_each_extent_mode_is_computed_once(three_level_table, caplog):
    from scistackplot import reduce as reduce_mod

    reduce_mod._plan_cache.clear()
    collapsed = {"subject": Role.ITERATE, "muscle": Role.FACET, "trial": Role.COLLAPSE}
    line = _spec([], kind=PlotKind.LINE, roles=collapsed)
    band = _spec([], kind=PlotKind.BAND, aggregate=Aggregation(error=ErrorBand.SD), roles=collapsed)
    with caplog.at_level(logging.INFO, logger=LAYER):
        for spec in (line, band, line, band):
            resolve_one(spec, three_level_table, 0)
    misses = [r for r in caplog.records if "y limits: MISS" in r.getMessage()]
    # The build computed the line's; the band's was the one miss.
    assert len(misses) == 1
    assert "summary" in misses[0].getMessage()


# --- log axes ---------------------------------------------------------------


def test_a_log_axis_never_gets_a_non_positive_floor(spread_table):
    """Every trace in `spread_table` starts at 0.0. On a log axis the floor
    is the smallest POSITIVE drawn value, padded geometrically — a linear 5 %
    pad below zero is an axis plotly cannot draw."""
    from scistackplot import StyleOptions

    spec = _spec(["subject", "muscle"], style=StyleOptions(log_y=True))
    figure = resolve(spec, spread_table)[0]
    for panel in figure.panels:
        low, high = panel.y_limits
        positive = pd.to_numeric(panel.frame[Y], errors="coerce")
        positive = positive[positive > 0]
        assert 0 < low <= positive.min(), panel.key
        assert high >= positive.max()


def test_plotly_gets_a_log_range_in_log10_units(spread_table):
    """plotly's `range` on a log axis is log10: handing over data units asked
    for 10^0.95 .. 10^105 and the figure came back empty."""
    import math

    from scistackplot import StyleOptions

    spec = _spec(["subject"], style=StyleOptions(log_y=True))
    figure = resolve(spec, spread_table)[0]
    payload = render_plotly(figure)

    axis = payload["layout"]["yaxis"]
    assert axis["type"] == "log"
    low, high = figure.y_limits
    assert axis["range"] == pytest.approx([math.log10(low), math.log10(high)])

    mpl = render_matplotlib(figure)
    try:
        assert mpl.axes[0].get_ylim() == pytest.approx((low, high))
    finally:
        matplotlib.pyplot.close(mpl)


def test_a_hand_typed_zero_floor_on_a_log_axis_is_lifted_not_passed_through(spread_table):
    from scistackplot import StyleOptions

    spec = _spec(["subject"], style=StyleOptions(log_y=True), y_axis=YAxis(scope=["subject"], minimum=0.0))
    figure = resolve(spec, spread_table)[0]
    payload = render_plotly(figure)
    low, high = payload["layout"]["yaxis"]["range"]
    assert all(math.isfinite(v) for v in (low, high))
    assert low < high


# --- keys and eligibility -----------------------------------------------------


def test_a_missing_level_finds_its_own_group():
    """pandas hands a NaN level back as nan, and nan != nan: the entry the
    extents wrote could never be found, so the panel fell back to the global
    range. Both sides now key NaN as None."""
    from scistackplot.ylimits import GLOBAL_KEY, limits_for, scope_key

    limits = {(None,): (1.0, 2.0), ("TA",): (5.0, 6.0), GLOBAL_KEY: (0.0, 9.0)}
    assert scope_key({"muscle": float("nan")}, ["muscle"]) == (None,)
    assert limits_for(limits, {"muscle": float("nan")}, ["muscle"], YAxis()) == (1.0, 2.0)


def test_the_figure_reports_the_panel_factors_the_scope_may_name(spread_table):
    """A schema key the spec never mentions defaults to ITERATE and is a
    panel factor. The GUI builds its checkboxes from this list, not from
    `spec.roles`, or the defaulted key never gets one."""
    spec = _spec(
        ["trial"],
        roles={"muscle": Role.FACET, "trial": Role.ITERATE},
    )
    figure = resolve(spec, spread_table)[0]
    assert figure.panel_factors == ["subject", "trial", "muscle"]

    meta = render_plotly(figure)["layout"]["meta"]
    assert meta["panel_factors"] == ["subject", "trial", "muscle"]
    assert meta["y_scope"] == ["trial"]


def test_a_bar_chart_on_a_log_axis_does_not_reach_for_zero(three_level_table):
    """Bars rise from zero on a LINEAR axis. A log axis has no zero; folding it
    in put log10(0) on the axis."""
    from scistackplot import StyleOptions

    spec = _spec(
        ["subject", "muscle"],
        kind=PlotKind.BAR,
        aggregate=Aggregation(error=ErrorBand.SD),
        style=StyleOptions(log_y=True),
    )
    for figure in resolve(spec, three_level_table):
        for panel in figure.panels:
            drawn = _drawn_extent(panel)
            if drawn is None or drawn[1] <= 0:
                continue
            low, high = panel.y_limits
            assert 0 < low and math.isfinite(low), panel.key
            assert math.isfinite(high)


def test_a_plotly_violin_spans_its_data_not_two_bandwidths_past_it(three_level_table):
    """plotly's default violin `spanmode` is "soft": the KDE runs past the
    extremes, into the part of the axis the limits cut off. matplotlib's spans
    exactly [min, max]; both must."""
    spec = _spec(["subject", "muscle"], kind=PlotKind.VIOLIN)
    figure = resolve(spec, three_level_table)[0]
    violins = [t for t in render_plotly(figure)["data"] if t["type"] == "violin"]
    assert violins
    assert all(t["spanmode"] == "hard" for t in violins)


def test_a_column_mixing_scalar_and_array_cells_scales_every_panel(caplog):
    """A melted struct variable is ONE column holding every field, and fields
    differ: `GAITRiteLoaded` keeps 9 per-trial scalars next to 42 per-step
    arrays. Deciding scalar-vs-array once per column cast every array cell to
    NaN, 42 of 51 panels fell back to the global range, agreed, and were drawn
    linked on the 9 scalar fields' scale (2026-09-14)."""
    rows = []
    for subject in ("01", "02"):
        # Arrays first: `shape.classify_column` reads the shape off the first
        # cell, as it does for the real table (its first cells are arrays).
        rows.append({"subject": subject, "ColName": "StepLength", "GR": [0.5, 0.6, 0.7]})
        rows.append({"subject": subject, "ColName": "StanceTime", "GR": [1.0, 1.1, 1.2]})
        rows.append({"subject": subject, "ColName": "Velocity", "GR": 30.0 + float(subject)})
    table = LongTable.from_frame(
        pd.DataFrame(rows), factors=["subject", "ColName"], measures=["GR"], name="GR"
    )
    spec = PlotSpec(
        measures=["GR"],
        roles={"subject": Role.ITERATE, "ColName": Role.FACET},
        kind=PlotKind.LINE,
        y_axis=YAxis(scope=["subject", "ColName"]),
    )
    with caplog.at_level(logging.INFO, logger=LAYER):
        figure = resolve(spec, table)[0]

    by_name = {panel.key["ColName"]: panel for panel in figure.panels}
    step, stance = by_name["StepLength"], by_name["StanceTime"]
    assert step.y_limits != stance.y_limits, "array panels fell back to one shared range"
    assert step.y_limits[0] <= 0.5 and step.y_limits[1] >= 0.7
    assert step.y_limits[1] < 1.0, "the scalar field's 31 leaked into an array panel's range"
    assert figure.y_limits is None, "panels differ, so the axes must not be linked"
    # No cell was dropped: the scalar cells were extents of their own.
    assert not [r for r in caplog.records if "hold nothing numeric" in r.getMessage()]
