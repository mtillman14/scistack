"""PlotKind.HISTOGRAM (docs/claude/histogram-and-regression.md).

User rules (2026-10-22): one bin mode at a time (auto / count / width); a
datetime measure bins along a date x axis with a duration width; percent and
density are per colour level; colour levels overlay by default; a histogram
draws the SAMPLE, so it needs a collapsed factor; log axes are ignored.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scistackplot import (
    BinMode,
    HistMultiple,
    HistogramOptions,
    HistStat,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    capabilities,
    generate_plot_function,
    plot_data,
    render_plotly,
    resolve,
)
from scistackplot.bins import (
    EDGES_SOURCE,
    MAX_BINS,
    BinError,
    bin_edges,
    edge_arguments,
    histogram_limits,
    parse_width,
)
from scistackplot.capability import available_plots, why_unavailable
from scistackplot.reduce import clear_plan_cache
from scistackplot.resolved import COUNT, X_HIGH, X_LOW, Y, Y_BASE
from scistackplot.roles import CONTINUOUS_LOG_REASON, RoleError, roles_for_kind
from scistackplot.shape import Shape
from scistackplot.spec import YAxis


@pytest.fixture(autouse=True)
def _fresh_plans():
    clear_plan_cache()
    yield
    clear_plan_cache()


def hist(**kwargs) -> PlotSpec:
    base = dict(
        measures=["StepLength"],
        roles={"subject": Role.ITERATE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.HISTOGRAM,
    )
    base.update(kwargs)
    return PlotSpec(**base)


def bars(figure, panel=0) -> pd.DataFrame:
    return figure.panels[panel].frame


# --- the edges: one owner ----------------------------------------------------


def test_width_bins_start_at_a_multiple_of_the_width():
    edges = bin_edges([1.3, 2.2, 3.9], HistogramOptions(bins=BinMode.WIDTH, bin_width=0.5))
    assert edges.edges[0] == pytest.approx(1.0)
    assert edges.edges[-1] >= 3.9
    assert np.allclose(np.diff(edges.edges), 0.5)
    assert edges.width == pytest.approx(0.5)


def test_count_bins_span_the_range_evenly():
    edges = bin_edges([0.0, 10.0, 4.0], HistogramOptions(bins=BinMode.COUNT, bin_count=5))
    assert edges.edges == pytest.approx((0.0, 2.0, 4.0, 6.0, 8.0, 10.0))


def test_auto_bins_are_numpys():
    values = np.random.default_rng(0).normal(size=200)
    edges = bin_edges(values, HistogramOptions())
    assert edges.edges == pytest.approx(tuple(np.histogram_bin_edges(values, bins="auto")))


def test_range_pins_an_end():
    edges = bin_edges([1.0, 2.0, 3.0], HistogramOptions(bins=BinMode.COUNT, bin_count=2, range_min=0.0))
    assert edges.edges == pytest.approx((0.0, 1.5, 3.0))


def test_a_width_mode_without_a_width_falls_back_to_auto_with_a_note():
    edges = bin_edges([1.0, 2.0, 3.0], HistogramOptions(bins=BinMode.WIDTH))
    assert edges.mode is BinMode.AUTO
    assert any("automatically" in note for note in edges.notes)


def test_too_many_bins_is_refused_with_the_fix():
    with pytest.raises(BinError, match=str(MAX_BINS)):
        bin_edges([0.0, 1.0], HistogramOptions(bins=BinMode.WIDTH, bin_width=1e-6))


def test_datetime_width_is_a_duration():
    assert parse_width("1h", temporal=True) == 3_600_000.0
    assert parse_width("7D", temporal=True) == 7 * 86_400_000.0
    with pytest.raises(BinError, match="duration"):
        parse_width(5, temporal=True)
    with pytest.raises(BinError):
        parse_width("1 fortnightly", temporal=True)


def test_generated_edges_match_the_library_rule():
    """The export restates nothing: it runs EDGES_SOURCE, the same text."""
    values = np.random.default_rng(3).normal(5, 2, size=80)
    for options in (
        HistogramOptions(),
        HistogramOptions(bins=BinMode.COUNT, bin_count=7),
        HistogramOptions(bins=BinMode.WIDTH, bin_width=0.75, range_max=12.0),
    ):
        namespace: dict = {}
        exec(EDGES_SOURCE, namespace)  # noqa: S102
        generated = namespace["_scistack_bin_edges"](values, *edge_arguments(options, temporal=False))
        assert tuple(generated) == pytest.approx(bin_edges(values, options).edges)


def test_limits_are_matplotlibs_bar_autoscale():
    assert histogram_limits([4.0, 10.0]) == (0.0, pytest.approx(10.5))
    assert histogram_limits([]) == (0.0, 1.0)


# --- availability ----------------------------------------------------------


def test_offered_for_a_scalar_with_a_sample_and_not_with_an_x_measure():
    sample = {"session": Role.GROUP, "trial": Role.COLLAPSE}
    assert PlotKind.HISTOGRAM in available_plots(Shape.SCALAR, sample, n_groups=1)
    assert PlotKind.HISTOGRAM not in available_plots(Shape.SCALAR, sample, has_x_measure=True)
    assert "sample" in why_unavailable(PlotKind.HISTOGRAM, Shape.SCALAR, {"session": Role.GROUP}, n_groups=1)


def test_one_grouping_layer_at_most(scalar_table):
    spec = hist(
        roles={"subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session", "subject"],
    )
    with pytest.raises(RoleError, match="ONE grouping layer"):
        resolve(spec, scalar_table)


def test_picking_it_on_the_opening_state_collapses_the_grouped_level(scalar_table):
    from scistackplot.roles import default_assignment

    opening = default_assignment(scalar_table, "StepLength")
    spec = PlotSpec(
        measures=["StepLength"], roles=opening.roles, groups=opening.groups, color=opening.color
    )
    suggested = roles_for_kind(spec, scalar_table, PlotKind.HISTOGRAM)
    assert suggested is not None
    assert suggested.roles["trial"] is Role.COLLAPSE
    assert "trial" not in suggested.groups


def test_capabilities_report_the_section(scalar_table):
    caps = capabilities(hist(), scalar_table)
    entry = next(k for k in caps["kinds"] if k["kind"] == "histogram")
    assert entry["available"]
    assert caps["histogram"]["applies"] and caps["histogram"]["problem"] is None
    bad = capabilities(hist(histogram=HistogramOptions(bins=BinMode.COUNT, bin_count=0)), scalar_table)
    assert "bin count" in bad["histogram"]["problem"]


# --- the figure ------------------------------------------------------------


def test_counts_add_up_to_the_sample(scalar_table):
    figure = resolve(hist(), scalar_table)[0]
    frame = bars(figure)
    # Subject 01: 2 sessions x 4 trials, each trial a sample row.
    assert int(frame[COUNT].sum()) == 8
    assert figure.bins is not None and figure.bins.n_values == 8
    assert figure.color_factor == "session"  # the one layer is its colour
    assert figure.labels.x == "StepLength" and figure.labels.y == "Count"


def test_every_panel_shares_the_figures_edges(scalar_table):
    figure = resolve(hist(roles={"subject": Role.FACET, "session": Role.GROUP, "trial": Role.COLLAPSE}), scalar_table)[0]
    assert len(figure.panels) == 3
    lefts = {tuple(sorted(set(p.frame[X_LOW]))) for p in figure.panels}
    assert len(lefts) == 1
    # One shared count axis, from zero.
    assert figure.y_limits is not None and figure.y_limits[0] == 0.0


def test_percent_is_per_colour_level(scalar_table):
    figure = resolve(hist(histogram=HistogramOptions(stat=HistStat.PERCENT)), scalar_table)[0]
    frame = bars(figure)
    for _level, rows in frame.groupby("__color"):
        assert (rows[Y] - rows[Y_BASE]).sum() == pytest.approx(100.0)
    assert figure.labels.y == "Percent"


def test_stacked_levels_sit_on_each_other(scalar_table):
    figure = resolve(hist(histogram=HistogramOptions(multiple=HistMultiple.STACK)), scalar_table)[0]
    frame = bars(figure)
    first, last = figure.color_order[0], figure.color_order[-1]
    bottom = frame[frame["__color"] == last]
    top = frame[frame["__color"] == first]
    # Reversed colour order (seaborn's): the last level at the bottom.
    assert (bottom[Y_BASE] == 0).all()
    assert np.allclose(top[Y_BASE].to_numpy(), bottom[Y].to_numpy())


def test_dodged_levels_split_each_bin(scalar_table):
    figure = resolve(hist(histogram=HistogramOptions(multiple=HistMultiple.DODGE)), scalar_table)[0]
    frame = bars(figure)
    widths = (frame[X_HIGH] - frame[X_LOW]).to_numpy()
    assert np.allclose(widths, figure.bins.width / 2)


def test_pinned_y_end_wins(scalar_table):
    figure = resolve(hist(y_axis=YAxis(maximum=20.0)), scalar_table)[0]
    assert figure.panels[0].y_limits == (0.0, 20.0)


def test_log_axes_are_ignored(scalar_table):
    figure = resolve(hist(style=StyleOptions(log_y=True)), scalar_table)[0]
    assert not figure.spec.style.log_y
    assert CONTINUOUS_LOG_REASON in figure.layout_notes


def test_preview_draws_the_bars(scalar_table):
    figure = resolve(hist(), scalar_table)[0]
    payload = render_plotly(figure)
    traces = [t for t in payload["data"] if t.get("type") == "bar"]
    assert traces and all("width" in t and "base" in t for t in traces)
    assert payload["layout"]["xaxis"]["range"] == pytest.approx([figure.bins.edges[0], figure.bins.edges[-1]])
    assert payload["layout"]["meta"]["bins"]["n_bins"] == figure.bins.n_bins


def test_save_data_offers_the_bars(scalar_table):
    caps = capabilities(hist(), scalar_table)
    keys = [d["key"] for d in caps["data_export"]["depths"]]
    assert "__bars" in keys
    data = plot_data(hist(), scalar_table, depth="__bars")
    assert {"bin_start", "bin_end", "count", "session", "subject"} <= set(data.columns)
    assert int(data["count"].sum()) == len(scalar_table.frame)  # every trial of every subject


# --- datetimes ---------------------------------------------------------------


@pytest.fixture
def times_table() -> LongTable:
    start = pd.Timestamp("2026-07-01 08:00")
    rows = [
        {"subject": f"{s:02d}", "trial": str(t), "When": start + pd.Timedelta(minutes=37 * (s * 4 + t))}
        for s in range(1, 4)
        for t in range(1, 5)
    ]
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "trial"],
        measures=["When"],
        schema_levels=["subject", "trial"],
    )


def test_datetime_bins_along_a_date_x_axis(times_table):
    spec = PlotSpec(
        measures=["When"],
        roles={"subject": Role.COLLAPSE, "trial": Role.COLLAPSE},
        kind=PlotKind.HISTOGRAM,
        histogram=HistogramOptions(bins=BinMode.WIDTH, bin_width="1h"),
    )
    (figure,) = resolve(spec, times_table)
    assert figure.temporal is None
    assert figure.temporal_x is not None
    assert figure.bins.width == pytest.approx(3_600_000.0)
    # Hour bins start on the hour (wall clock).
    assert figure.bins.edges[0] % 3_600_000.0 == 0
    assert figure.labels.x.endswith("(local time)")
    payload = render_plotly(figure)
    assert payload["layout"]["xaxis"]["type"] == "date"


# --- the export --------------------------------------------------------------


def _run(source: str, frame, name: str):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace[name](frame.copy(), "figure.png")


def test_generated_code_draws_the_previewed_bars(scalar_table):
    pytest.importorskip("seaborn")
    spec = hist(roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE})
    source = generate_plot_function(spec, scalar_table, function_name="plot_h")
    assert 'kind="hist"' in source and "_scistack_bin_edges" in source
    figure = _run(source, scalar_table.frame, "plot_h")
    (preview,) = resolve(spec, scalar_table)
    ax = figure.axes[0]
    drawn = sorted(round(p.get_height(), 9) for p in ax.patches if p.get_height() > 0)
    expected = sorted(round(v, 9) for v in (bars(preview)[Y] - bars(preview)[Y_BASE]) if v > 0)
    assert drawn == expected
    assert ax.get_xlim() == pytest.approx((preview.bins.edges[0], preview.bins.edges[-1]))
    assert ax.get_ylim() == pytest.approx(preview.panels[0].y_limits)
