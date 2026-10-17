"""CI95 is t-based everywhere: the preview, the numpy twin and the export.

Regression for the divergence found 2026-09-27 (statistics plan, Stage 1):
the preview drew ``1.96 · SEM`` and the exported code drew seaborn's bootstrap
``("ci", 95)``. The formula's one owner is now ``csvstats.descriptive``;
``scistackplot.spread`` only draws "no interval" as zero width. The reference
values are hand-checked t quantiles, so a return to 1.96 fails at n = 2 and 5.
"""

from __future__ import annotations

import matplotlib
import numpy as np
import pandas as pd
import pytest

from scistackplot import LongTable, resolve
from scistackplot.codegen import generate_plot_function
from scistackplot.resolved import X, Y, Y_HIGH, Y_LOW
from scistackplot.series_stats import position_stats
from scistackplot.spec import Aggregation, ErrorBand, PlotKind, PlotSpec, Role, Statistic
from scistackplot.spread import QUANTILE_METHOD, SD_DDOF, ci95_spread, sem_spread

matplotlib.use("Agg")

#: t(0.975, n - 1) from published tables.
T_975 = {2: 12.7062, 5: 2.7764, 30: 2.0452}


@pytest.mark.parametrize("n", sorted(T_975))
def test_spread_is_the_t_based_half_width(n):
    assert ci95_spread(1.0, n) == pytest.approx(T_975[n] / np.sqrt(n), abs=1e-4)
    assert ci95_spread(1.0, n) != pytest.approx(1.96 / np.sqrt(n), abs=1e-3)


def test_fewer_than_two_values_draw_a_zero_width_bar():
    """csv-stats says NaN (no interval); the plot draws it as zero width."""
    assert ci95_spread(0.0, 1) == 0.0
    assert ci95_spread(0.0, 0) == 0.0
    assert sem_spread(0.0, 1) == 0.0


def test_spread_keeps_the_series_index():
    sd = pd.Series([1.0, 1.0, 0.0], index=["a", "b", "c"])
    counts = pd.Series([2, 5, 1], index=["a", "b", "c"])
    out = ci95_spread(sd, counts)
    assert list(out.index) == ["a", "b", "c"]
    assert out["a"] == pytest.approx(T_975[2] / np.sqrt(2), abs=1e-4)
    assert out["c"] == 0.0
    assert sem_spread(sd, counts)["b"] == pytest.approx(1 / np.sqrt(5))


def test_the_plot_uses_csv_stats_conventions():
    """numpy's nanpercentile default (series_stats) must be csv-stats' method."""
    assert SD_DDOF == 1
    assert QUANTILE_METHOD == "linear"


def test_no_formula_is_restated_in_scistackplot():
    """One owner: csvstats.descriptive. No t quantile or z constant here."""
    import pathlib

    import scistackplot

    root = pathlib.Path(scistackplot.__file__).parent
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "t.ppf" not in text, path
        assert "1.96 *" not in text, path


def _values(n: int) -> np.ndarray:
    return np.arange(1.0, n + 1.0) ** 1.5


def _expected_half(values: np.ndarray) -> float:
    n = len(values)
    return T_975[n] * values.std(ddof=1) / np.sqrt(n)


@pytest.mark.parametrize("n", sorted(T_975))
def test_numpy_twin_is_t_based(n):
    values = _values(n)
    cells = [np.array([v]) for v in values]
    centre, low, high, _ = position_stats(cells, Statistic.MEAN, ErrorBand.CI95)
    assert high[0] - centre[0] == pytest.approx(_expected_half(values), rel=1e-4)
    assert centre[0] - low[0] == pytest.approx(_expected_half(values), rel=1e-4)


def _bar_table(n_per_level: int):
    rows = []
    for level, offset in (("A", 0.0), ("B", 10.0)):
        for i, value in enumerate(_values(n_per_level)):
            rows.append((f"S{i:02d}", level, value + offset))
    frame = pd.DataFrame(rows, columns=["subject", "brand", "M"])
    table = LongTable.from_frame(
        frame, factors=["subject", "brand"], measures=["M"], name="M",
        schema_levels=["subject", "brand"],
    )
    return frame, table


def _bar_spec(statistic=Statistic.MEAN) -> PlotSpec:
    return PlotSpec(
        measures=["M"],
        roles={"subject": Role.COLLAPSE, "brand": Role.GROUP},
        groups=["brand"],
        kind=PlotKind.BAR,
        aggregate=Aggregation(statistic=statistic, error=ErrorBand.CI95),
    )


@pytest.mark.parametrize("n", sorted(T_975))
def test_preview_bars_are_t_based(n):
    _, table = _bar_table(n)
    (preview,) = resolve(_bar_spec(), table)
    drawn = preview.panels[0].frame.set_index(X)
    half = _expected_half(_values(n))
    for level in ("A", "B"):
        assert drawn.loc[level, Y_HIGH] - drawn.loc[level, Y] == pytest.approx(half, rel=1e-4)
        assert drawn.loc[level, Y] - drawn.loc[level, Y_LOW] == pytest.approx(half, rel=1e-4)


def test_export_calls_csv_stats_not_seaborns_bootstrap():
    _, table = _bar_table(5)
    source = generate_plot_function(_bar_spec(), table)
    assert '("ci", 95)' not in source
    assert "errorbar=_ci95" in source
    assert "from csvstats.descriptive import ci_interval" in source
    assert "_ci95 = ci_interval" in source
    assert "t.ppf" not in source, "the formula is called, not restated"
    assert "(t-based)" in source, "the docstring says which CI"


def test_median_centre_is_emitted_for_a_median_bar():
    _, table = _bar_table(5)
    source = generate_plot_function(_bar_spec(Statistic.MEDIAN), table)
    assert 'ci_interval(values, centre="median")' in source


def _exported_bar_extents(figure) -> list[tuple[float, float]]:
    """(low, high) of every error bar seaborn drew (one Line2D per bar)."""
    extents = []
    for ax in figure.axes:
        for line in ax.lines:
            ys = np.asarray(line.get_ydata(), dtype=float)
            ys = ys[~np.isnan(ys)]
            if ys.size and np.ptp(ys) > 0:
                extents.append((round(float(ys.min()), 6), round(float(ys.max()), 6)))
    return sorted(extents)


@pytest.mark.parametrize("n", [2, 5, 30])
def test_export_draws_the_preview_error_bars(n):
    """Preview == export == reference (feedback: export matches preview)."""
    frame, table = _bar_table(n)
    spec = _bar_spec()
    namespace: dict = {}
    exec(compile(generate_plot_function(spec, table), "<generated>", "exec"), namespace)  # noqa: S102
    function = next(v for k, v in namespace.items() if k.startswith("plot_"))
    figure = function(frame.copy(), "figure.png")
    exported = _exported_bar_extents(figure)
    matplotlib.pyplot.close(figure)

    (preview,) = resolve(spec, table)
    drawn = preview.panels[0].frame
    previewed = sorted(
        (round(float(lo), 6), round(float(hi), 6))
        for lo, hi in zip(drawn[Y_LOW], drawn[Y_HIGH])
    )
    assert len(exported) == len(previewed) == 2
    flat = [end for pair in exported for end in pair]
    assert flat == pytest.approx([end for pair in previewed for end in pair], rel=1e-6)
