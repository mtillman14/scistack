""""Show sample" on a histogram / KDE: a crowding-shaded rug.

User rules (2026-11-01, `.claude/plan-histogram-rug.md`): the sample values go
along x, one tick each; one row per colour level; shaded light to dark in the
level's own colour by how many of its values share 1/RUG_SLOTS of the axis;
a single-value bin's values sit under its slot, a NaN draws no tick; no note.
One owner: `bins.RUG_SOURCE`, read by reduce, both renderers and the export.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scistackplot import (
    HistogramOptions,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    capabilities,
    generate_plot_function,
    render_plotly,
    resolve,
)
from scistackplot.reduce.bins import RUG_ROW_HEIGHT, RUG_SHADES, RUG_SLOTS, RUG_SOURCE
from scistackplot.reduce.resolve import clear_plan_cache
from scistackplot.reduce.resolved import COUNT, RUG_ROW, RUG_SHADE, X, Y


@pytest.fixture(autouse=True)
def _fresh_plans():
    clear_plan_cache()
    yield
    clear_plan_cache()


#: Per group: three zeros, one NaN, a pair at exactly 5, and a spread.
VALUES = [0.0, 0.0, 0.0, np.nan, 5.0, 5.0, 1.0, 2.0, 3.0, 8.0]


@pytest.fixture
def table() -> LongTable:
    rows = [
        {"group": group, "trial": str(i), "Speed": value}
        for group in ("A", "B")
        for i, value in enumerate(VALUES)
    ]
    return LongTable.from_frame(
        pd.DataFrame(rows), factors=["group", "trial"], measures=["Speed"], schema_levels=["group", "trial"]
    )


def spec(kind=PlotKind.HISTOGRAM, **options) -> PlotSpec:
    return PlotSpec(
        measures=["Speed"],
        roles={"group": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["group"],
        kind=kind,
        show_sample=["trial"],
        histogram=HistogramOptions(**options),
    )


def _source() -> dict:
    namespace: dict = {}
    exec(RUG_SOURCE, namespace)  # noqa: S102
    return namespace


def test_source_places_counts_and_shades():
    ns = _source()
    xs = ns["_scistack_rug_positions"]([0.0, 2.0, np.nan], (0.0,), [-3.0, 0.0], [-2.0, 1.0], ["exact", "range"])
    assert xs[0] == -2.5 and xs[1] == 2.0 and np.isnan(xs[2])
    counts = ns["_scistack_rug_counts"](np.array([1.0, 1.0, 9.0, np.nan, 20.0]), 0.0, 10.0, 400)
    assert counts.tolist() == [2, 2, 1, 0, 0]  # NaN / outside: no tick
    assert ns["_scistack_rug_shade"]([1, 5, 9], 9, 8).tolist() == [0, 4, 7]
    assert ns["_scistack_rug_shade"]([1, 1], 1, 8).tolist() == [0, 0]  # nothing crowded: all light
    assert ns["_scistack_rug_color"]("#000000", 7, 8) == "#000000"
    assert ns["_scistack_rug_color"]("rgb(0, 0, 0)", 0, 8) == "#bfbfbf"  # 25% of the colour


def test_the_rug_replaces_the_points(table):
    figure = resolve(spec(), table)[0]
    panel = figure.panels[0]
    assert panel.sample is None and panel.rug is not None
    # Every finite value, NaN dropped: 9 per group.
    assert len(panel.rug) == 18
    assert sorted(panel.rug[RUG_ROW].unique()) == [0, 1]
    assert set(figure.rug_scale) == {"A", "B"}


def test_single_value_bin_values_sit_under_their_slot(table):
    figure = resolve(spec(exact_values=(0.0,)), table)[0]
    lefts, rights, kinds, _labels = figure.bins.slots()
    centre = (lefts[0] + rights[0]) / 2.0
    rug = figure.panels[0].rug
    zeros = rug[rug[Y] == 0.0]
    assert np.allclose(zeros[X], centre)
    assert (zeros[COUNT] == 3).all()  # the three zeros share one slot
    assert figure.rug_scale["A"] == 3
    assert (zeros[RUG_SHADE] == RUG_SHADES - 1).all()  # the darkest spot


def test_identical_values_are_darker_than_lone_ones(table):
    rug = resolve(spec(), table)[0].panels[0].rug
    a = rug[rug["__color"] == "A"]
    pair = a[a[Y] == 5.0]
    lone = a[a[Y] == 8.0]
    assert (pair[COUNT] == 2).all() and (lone[COUNT] == 1).all()
    assert pair[RUG_SHADE].iloc[0] > lone[RUG_SHADE].iloc[0]


def test_capability_reports_a_rug_and_join_is_inert(table):
    caps = capabilities(spec(), table)["sample_overlay"]
    assert caps["available"] and caps["form"] == "rug"
    assert caps["join"]["join"] is False


def test_matplotlib_draws_one_row_per_level(table):
    pytest.importorskip("matplotlib")
    from matplotlib.collections import LineCollection

    from scistackplot import render_matplotlib

    figure = resolve(spec(), table)[0]
    ax = render_matplotlib(figure).axes[0]
    rugs = [c for c in ax.collections if isinstance(c, LineCollection)]
    assert len(rugs) == 2
    bands = sorted({(round(s[0][1], 6), round(s[1][1], 6)) for c in rugs for s in c.get_segments()})
    assert bands == [(0.0, RUG_ROW_HEIGHT), (RUG_ROW_HEIGHT, 2 * RUG_ROW_HEIGHT)]
    assert ax.get_ylim() == pytest.approx(figure.panels[0].y_limits)  # the rug moves no axis


def test_plotly_draws_the_ticks_and_a_hover(table):
    figure = resolve(spec(), table)[0]
    payload = render_plotly(figure)
    rug = [t for t in payload["data"] if t.get("name") == "rug"]
    lines = [t for t in rug if t["mode"] == "lines"]
    hovers = [t for t in rug if t["mode"] == "markers"]
    assert len(hovers) == 2
    drawn = sorted(x for t in lines for x in t["x"] if x is not None)
    assert len(drawn) == 2 * 18  # two ends per tick
    assert payload["layout"]["meta"]["rug"]["slots"] == RUG_SLOTS
    # One trace per (row, shade): never more than RUG_SHADES a row.
    assert len(lines) <= 2 * RUG_SHADES


def test_kde_gets_the_same_rug(table):
    figure = resolve(spec(kind=PlotKind.KDE), table)[0]
    rug = figure.panels[0].rug
    lo, hi = figure.kde_range
    assert len(rug) == 18
    assert rug[X].between(lo, hi).all()


def _run(source: str, frame, name: str):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace[name](frame.copy(), "figure.png")


def _ticks(ax):
    from matplotlib.collections import LineCollection
    from matplotlib.colors import to_hex

    out = []
    for collection in ax.collections:
        if not isinstance(collection, LineCollection):
            continue
        segments = collection.get_segments()
        colours = [to_hex(c) for c in collection.get_colors()]
        if len(colours) == 1:
            colours = colours * len(segments)
        for segment, colour in zip(segments, colours, strict=True):
            out.append((round(float(segment[0][0]), 9), round(float(segment[0][1]), 6), colour))
    return sorted(out)


@pytest.mark.parametrize("kind", [PlotKind.HISTOGRAM, PlotKind.KDE])
def test_generated_code_draws_the_previewed_rug(table, kind):
    pytest.importorskip("seaborn")
    from scistackplot import render_matplotlib

    plot = spec(kind=kind, exact_values=(0.0,))
    source = generate_plot_function(plot, table, function_name="plot_r")
    assert "_scistack_rug_counts" in source
    exported = _run(source, table.frame, "plot_r").axes[0]
    (preview,) = resolve(plot, table)
    drawn = render_matplotlib(preview).axes[0]
    assert _ticks(exported) == _ticks(drawn) and _ticks(drawn)
