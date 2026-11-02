"""PlotKind.KDE — a smoothed histogram (user, 2026-11-01).

`.claude/plan-histogram-special-bins-and-kde.md`: a Gaussian KDE (Scott's
bandwidth x adjust), one curve per colour level within a panel, area 1; a
univariate kind (`spec.UNIVARIATE_KINDS`) like the histogram: it draws the
sample, refuses an x measure and takes one grouping layer as its colour.
One owner of the curve: `kde.KDE_SOURCE`, run by the preview and emitted
verbatim by the export.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scistackplot import (
    KdeOptions,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    UNIVARIATE_KINDS,
    capabilities,
    generate_plot_function,
    render_plotly,
    resolve,
)
from scistackplot.capability import available_plots
from scistackplot.kde import GRID_POINTS, KDE_SOURCE, kde_problem
from scistackplot.reduce import clear_plan_cache
from scistackplot.resolved import COUNT, X, Y
from scistackplot.roles import KDE_NO_X, RoleError
from scistackplot.shape import Shape


@pytest.fixture(autouse=True)
def _fresh_plans():
    clear_plan_cache()
    yield
    clear_plan_cache()


def kde(**kwargs) -> PlotSpec:
    base = dict(
        measures=["StepLength"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.KDE,
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _source():
    namespace: dict = {}
    exec(KDE_SOURCE, namespace)  # noqa: S102
    return namespace["_scistack_kde"]


def test_the_curve_is_scipys_gaussian_kde_with_scotts_bandwidth():
    scipy_stats = pytest.importorskip("scipy.stats")
    values = np.random.default_rng(3).normal(10.0, 2.0, size=57)
    xs, density = _source()(values, 1.0, 0.0, GRID_POINTS)
    reference = scipy_stats.gaussian_kde(values, bw_method="scott")
    assert np.allclose(density, reference(xs), rtol=1e-10)
    assert xs[0] == values.min() and xs[-1] == values.max()  # cut 0


def test_bandwidth_adjust_scales_the_kernel_and_cut_extends_the_grid():
    scipy_stats = pytest.importorskip("scipy.stats")
    values = np.random.default_rng(4).normal(0.0, 1.0, size=40)
    xs, density = _source()(values, 0.5, 3.0, GRID_POINTS)
    reference = scipy_stats.gaussian_kde(values, bw_method="scott")
    reference.set_bandwidth(reference.factor * 0.5)
    assert np.allclose(density, reference(xs), rtol=1e-10)
    assert xs[0] < values.min() and xs[-1] > values.max()


def test_no_curve_without_two_different_values():
    assert _source()([1.0], 1.0, 0.0, GRID_POINTS) is None
    assert _source()([2.0, 2.0, np.nan], 1.0, 0.0, GRID_POINTS) is None


def test_settings_problems():
    assert kde_problem(0.0, 0.0) is not None
    assert kde_problem(1.0, -1.0) is not None
    assert kde_problem(1.0, 0.0) is None


def test_it_is_univariate_and_offered_beside_the_histogram(scalar_table):
    assert PlotKind.KDE in UNIVARIATE_KINDS
    roles = {"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE}
    assert PlotKind.KDE in available_plots(Shape.SCALAR, roles, n_groups=1)
    caps = capabilities(kde(), scalar_table)
    assert caps["kde"]["applies"] and caps["kde"]["problem"] is None


def test_an_x_measure_is_refused(scalar_table):
    with pytest.raises(RoleError, match="density curve"):
        resolve(kde(x_measure="StepLength"), scalar_table)
    assert "X variable" in KDE_NO_X


def test_one_curve_per_colour_level_with_area_one(scalar_table):
    figure = resolve(kde(), scalar_table)[0]
    frame = figure.panels[0].frame
    assert figure.color_factor == "session"
    assert figure.labels.y == "Density" and figure.labels.x == "StepLength"
    for _level, rows in frame.groupby("__color"):
        assert len(rows) == GRID_POINTS
        area = np.trapezoid(rows[Y], rows[X]) if hasattr(np, "trapezoid") else np.trapz(rows[Y], rows[X])
        # Truncated at the data's ends (cut 0): some, never more, of the area.
        assert 0.0 < area <= 1.0
        assert rows[COUNT].iloc[0] == 3  # one value per subject (trials averaged)
    assert figure.kde_range == pytest.approx((frame[X].min(), frame[X].max()))
    assert figure.panels[0].y_limits == pytest.approx((0.0, frame[Y].max() * 1.05))


def test_preview_draws_lines_and_fills_when_asked(scalar_table):
    figure = resolve(kde(kde=KdeOptions(fill=True)), scalar_table)[0]
    payload = render_plotly(figure)
    traces = [t for t in payload["data"] if t.get("mode") == "lines"]
    assert len(traces) == 2 and all(t["fill"] == "tozeroy" for t in traces)
    assert payload["layout"]["xaxis"]["range"] == pytest.approx(list(figure.kde_range))


def test_a_level_with_one_value_draws_nothing_and_says_why():
    frame = pd.DataFrame(
        {"group": ["A", "A", "A", "B"], "trial": ["1", "2", "3", "1"], "V": [1.0, 2.0, 4.0, 3.0]}
    )
    table = LongTable.from_frame(frame, factors=["group", "trial"], measures=["V"], schema_levels=["group", "trial"])
    spec = PlotSpec(
        measures=["V"],
        roles={"group": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["group"],
        kind=PlotKind.KDE,
    )
    (figure,) = resolve(spec, table)
    assert set(figure.panels[0].frame["__color"]) == {"A"}
    assert any("B: no density curve" in note for note in figure.layout_notes)


def _run(source: str, frame, name: str):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace[name](frame.copy(), "figure.png")


@pytest.mark.parametrize("fill", [False, True])
def test_generated_code_draws_the_previewed_curves(scalar_table, fill):
    pytest.importorskip("seaborn")
    spec = kde(kde=KdeOptions(bandwidth_adjust=0.7, fill=fill))
    source = generate_plot_function(spec, scalar_table, function_name="plot_k")
    assert "_scistack_kde" in source and 'kind="line"' in source
    figure = _run(source, scalar_table.frame, "plot_k")
    (preview,) = resolve(spec, scalar_table)
    ax = figure.axes[0]
    drawn = sorted(
        (round(float(line.get_xdata()[0]), 9), round(float(np.max(line.get_ydata())), 9))
        for line in ax.lines
        if len(line.get_xdata()) == GRID_POINTS
    )
    frame = preview.panels[0].frame
    expected = sorted(
        (round(float(rows[X].iloc[0]), 9), round(float(rows[Y].max()), 9))
        for _level, rows in frame.groupby("__color")
    )
    assert drawn == expected
    assert ax.get_xlim() == pytest.approx(preview.kde_range)
    assert ax.get_ylim() == pytest.approx(preview.panels[0].y_limits)
    # A shade per curve when filled, none otherwise.
    assert (len(ax.collections) >= 2) if fill else not ax.collections
