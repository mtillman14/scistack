"""
Compare to reference, exported: the generated seaborn code draws what the
preview draws (feedback: export matches preview; fix codegen, never test
around a difference). Bar heights and "Show sample" point heights are read
back from the executed figure and compared with the resolved panels.
"""

from __future__ import annotations

import pandas as pd
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.collections import PathCollection  # noqa: E402
from matplotlib.patches import Rectangle  # noqa: E402

pytest.importorskip("seaborn")

from scistackplot import (  # noqa: E402
    Aggregation,
    CompareMode,
    Comparison,
    ErrorBand,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    generate_plot_function,
    resolve,
)
from scistackplot.reduce import clear_plan_cache  # noqa: E402
from scistackplot.resolved import Y  # noqa: E402

ROWS = [
    ("01", "s1", "1", 1.0),
    ("01", "s1", "2", 3.0),
    ("01", "s2", "1", 3.0),
    ("01", "s2", "2", 5.0),
    ("01", "s3", "1", 5.0),
    ("01", "s3", "2", 7.0),
    ("02", "s1", "1", 10.0),
    ("02", "s2", "1", 15.0),
    ("02", "s3", "1", 30.0),
    ("03", "s1", "1", 4.0),
    ("03", "s2", "1", 2.0),
]


@pytest.fixture
def frame() -> pd.DataFrame:
    clear_plan_cache()
    return pd.DataFrame(ROWS, columns=["subject", "session", "trial", "M"])


@pytest.fixture
def table(frame) -> LongTable:
    return LongTable.from_frame(
        frame,
        factors=["subject", "session", "trial"],
        measures=["M"],
        name="M",
        schema_levels=["subject", "session", "trial"],
    )


def _spec(mode=CompareMode.DIFFERENCE, **kwargs) -> PlotSpec:
    base = dict(
        measures=["M"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=PlotKind.BAR,
        aggregate=Aggregation(error=ErrorBand.SD),
        comparison=Comparison(layer="session", level="s1", mode=mode),
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _run(source: str, frame):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace["plot_m"](frame.copy(), "figure.png")


def _bar_heights(figure) -> list[float]:
    ax = figure.axes[0]
    return sorted(
        round(float(p.get_height()), 6)
        for p in ax.patches
        if isinstance(p, Rectangle) and p.get_width() > 0 and p.get_zorder() < 3
    )


def _point_ys(figure) -> list[float]:
    ax = figure.axes[0]
    ys = [float(y) for line in ax.get_lines() if line.get_marker() == "o" for y in line.get_ydata()]
    ys += [
        float(y)
        for c in ax.collections
        if isinstance(c, PathCollection) and c.get_zorder() >= 3
        for _, y in c.get_offsets()
    ]
    return sorted(round(y, 6) for y in ys)


def _preview_heights(spec, table) -> list[float]:
    (resolved,) = resolve(spec, table)
    return sorted(round(float(v), 6) for v in resolved.panels[0].frame[Y])


@pytest.mark.parametrize(
    "spec_kwargs",
    [
        {},
        {"mode": CompareMode.PERCENT},
        # Per summary: pooled trials against the s1 mean over all of them.
        {"aggregate": Aggregation(error=ErrorBand.SD, pooled=True)},
    ],
    ids=["paired-difference", "paired-percent", "pooled-per-summary"],
)
def test_exported_bars_match_the_preview(table, frame, spec_kwargs):
    spec = _spec(**spec_kwargs)
    source = generate_plot_function(spec, table)
    assert "# compare to reference" in source
    figure = _run(source, frame)
    heights = _bar_heights(figure)
    plt.close(figure)
    assert heights == pytest.approx(_preview_heights(spec, table))


def test_exported_bars_drop_the_unit_the_preview_drops(table, frame):
    rows = frame[~((frame["subject"] == "03") & (frame["session"] == "s1"))]
    table = LongTable.from_frame(
        rows.reset_index(drop=True),
        factors=["subject", "session", "trial"],
        measures=["M"],
        name="M",
        schema_levels=["subject", "session", "trial"],
    )
    spec = _spec()
    figure = _run(generate_plot_function(spec, table), rows)
    heights = _bar_heights(figure)
    plt.close(figure)
    assert heights == pytest.approx(_preview_heights(spec, table))


def test_exported_sample_points_match_the_preview(table, frame):
    spec = _spec(show_sample=["trial"])
    (resolved,) = resolve(spec, table)
    preview = sorted(round(float(v), 6) for v in resolved.panels[0].sample[Y])
    figure = _run(generate_plot_function(spec, table), frame)
    generated = _point_ys(figure)
    plt.close(figure)
    assert generated == pytest.approx(preview)


def test_exported_y_title_matches_the_preview(table, frame):
    spec = _spec(CompareMode.PERCENT)
    (resolved,) = resolve(spec, table)
    figure = _run(generate_plot_function(spec, table), frame)
    title = figure.axes[0].get_ylabel()
    plt.close(figure)
    assert title == resolved.labels.y == "M, % change from session s1"
