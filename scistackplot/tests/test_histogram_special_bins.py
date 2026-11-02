"""Histogram special slots: single-value bins and the NaN bar.

User rules (2026-11-01, `.claude/plan-histogram-special-bins-and-kde.md`):
a value in ``exact_values`` (e.g. 0) leaves the regular bins for its own
hatched bar LEFT of the range; NaN values get a bar RIGHT of it, on by
default; percent is of every value drawn; under density neither special bar
is drawn (inert, with a note); numbers only (a date axis ignores both).
One owner: ``bins.BARS_SOURCE`` — the preview, both renderers and the export
read its slots.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scistackplot import (
    HistMultiple,
    HistogramOptions,
    HistStat,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    generate_plot_function,
    plot_data,
    render_plotly,
    resolve,
)
from scistackplot.reduce.bins import BARS_SOURCE, DENSITY_SPECIAL_NOTE
from scistackplot.reduce.resolve import clear_plan_cache
from scistackplot.reduce.resolved import BIN_KIND, BIN_LABEL, COUNT, X_HIGH, X_LOW, Y, Y_BASE
from scistackplot.model.spec import HistogramOptions as Options


@pytest.fixture(autouse=True)
def _fresh_plans():
    clear_plan_cache()
    yield
    clear_plan_cache()


#: 12 values per group: four zeros, two NaN, six spread over 1..6.
VALUES = [0.0, 0.0, 0.0, 0.0, np.nan, np.nan, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0]


@pytest.fixture
def zeros_table() -> LongTable:
    rows = [
        {"group": group, "trial": str(i), "Speed": value}
        for group in ("A", "B")
        for i, value in enumerate(VALUES)
    ]
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["group", "trial"],
        measures=["Speed"],
        schema_levels=["group", "trial"],
    )


def hist(**options) -> PlotSpec:
    return PlotSpec(
        measures=["Speed"],
        roles={"group": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["group"],
        kind=PlotKind.HISTOGRAM,
        histogram=HistogramOptions(**options),
    )


def bars(figure) -> pd.DataFrame:
    return figure.panels[0].frame


def test_options_round_trip_and_parse_typed_text():
    options = Options.from_dict({"exact_values": "0, -1, x, 0", "show_nan": False})
    assert options.exact_values == (-1.0, 0.0)
    assert options.show_nan is False
    assert Options.from_dict(options.to_dict()) == options
    assert Options().show_nan is True  # on by default


def test_an_exact_value_is_counted_once_in_its_own_slot(zeros_table):
    figure = resolve(hist(exact_values=(0.0,)), zeros_table)[0]
    frame = bars(figure)
    for _level, rows in frame.groupby("__color"):
        exact = rows[rows[BIN_KIND] == "exact"]
        regular = rows[rows[BIN_KIND] == "range"]
        assert exact[COUNT].tolist() == [4]
        assert exact[BIN_LABEL].tolist() == ["= 0"]
        assert int(regular[COUNT].sum()) == 6  # the zeros are in no regular bin
    # The regular edges span the remaining values only.
    assert figure.bins.edges[0] == pytest.approx(1.0)
    assert figure.bins.n_values == 12


def test_the_exact_slot_sits_left_of_the_range_after_a_gap(zeros_table):
    figure = resolve(hist(exact_values=(0.0,), show_nan=False), zeros_table)[0]
    lefts, rights, kinds, _labels = figure.bins.slots()
    width = float(np.median(np.diff(figure.bins.edges)))
    assert kinds[0] == "exact"
    assert rights[0] == pytest.approx(figure.bins.edges[0] - width)  # one-bin gap
    assert rights[0] - lefts[0] == pytest.approx(width)


def test_nan_bar_is_on_by_default_right_of_the_range(zeros_table):
    figure = resolve(hist(), zeros_table)[0]
    frame = bars(figure)
    nan = frame[frame[BIN_KIND] == "nan"]
    assert nan[COUNT].tolist() == [2, 2]
    assert (nan[X_LOW] > figure.bins.edges[-1]).all()
    assert figure.bins.n_nan == 4 and figure.bins.nan


def test_nan_bar_toggled_off_still_reports_the_count(zeros_table):
    figure = resolve(hist(show_nan=False), zeros_table)[0]
    assert not (bars(figure)[BIN_KIND] == "nan").any()
    assert figure.bins.n_nan == 4 and not figure.bins.nan
    assert figure.bins.to_dict()["n_nan"] == 4


def test_percent_is_of_every_value_drawn(zeros_table):
    figure = resolve(hist(exact_values=(0.0,), stat=HistStat.PERCENT), zeros_table)[0]
    for _level, rows in bars(figure).groupby("__color"):
        assert (rows[Y] - rows[Y_BASE]).sum() == pytest.approx(100.0)
        nan = rows[rows[BIN_KIND] == "nan"]
        assert float((nan[Y] - nan[Y_BASE]).iloc[0]) == pytest.approx(100.0 * 2 / 12)


def test_percent_without_the_nan_bar_is_of_the_non_nan_values(zeros_table):
    figure = resolve(hist(exact_values=(0.0,), stat=HistStat.PERCENT, show_nan=False), zeros_table)[0]
    for _level, rows in bars(figure).groupby("__color"):
        exact = rows[rows[BIN_KIND] == "exact"]
        assert float((exact[Y] - exact[Y_BASE]).iloc[0]) == pytest.approx(100.0 * 4 / 10)


def test_density_draws_no_special_slot_and_says_why(zeros_table):
    figure = resolve(hist(exact_values=(0.0,), stat=HistStat.DENSITY), zeros_table)[0]
    frame = bars(figure)
    assert set(frame[BIN_KIND]) == {"range"}
    assert DENSITY_SPECIAL_NOTE in figure.bins.notes
    for _level, rows in frame.groupby("__color"):
        widths = rows[X_HIGH] - rows[X_LOW]
        # The regular bins' area is 1: the zeros stay out of them.
        assert float(((rows[Y] - rows[Y_BASE]) * widths).sum()) == pytest.approx(1.0)


def test_stacked_special_slots_sit_on_each_other(zeros_table):
    figure = resolve(hist(exact_values=(0.0,), multiple=HistMultiple.STACK), zeros_table)[0]
    exact = bars(figure)[bars(figure)[BIN_KIND] == "exact"].sort_values(Y_BASE)
    assert exact[Y_BASE].tolist() == [0.0, 4.0]
    assert exact[Y].tolist() == [4.0, 8.0]


def test_ticks_name_the_special_slots(zeros_table):
    figure = resolve(hist(exact_values=(0.0,)), zeros_table)[0]
    values, labels = figure.bins.ticks()
    assert "= 0" in labels and "NaN" in labels
    regular = [v for v, label in zip(values, labels) if label not in ("= 0", "NaN")]
    assert all(figure.bins.edges[0] <= v <= figure.bins.edges[-1] for v in regular)
    # No special slot, no special ticks: the axis keeps its own.
    plain = resolve(hist(show_nan=False), zeros_table)[0]
    assert plain.bins.ticks() is None


def test_preview_hatches_and_names_the_special_bars(zeros_table):
    figure = resolve(hist(exact_values=(0.0,)), zeros_table)[0]
    payload = render_plotly(figure)
    trace = next(t for t in payload["data"] if t.get("type") == "bar")
    assert "/" in trace["marker"]["pattern"]["shape"]
    assert any(row[1] == "= 0" for row in trace["customdata"])
    axis = payload["layout"]["xaxis"]
    assert "= 0" in axis["ticktext"] and "NaN" in axis["ticktext"]
    lefts, rights, _k, _l = figure.bins.slots()
    assert axis["range"] == pytest.approx([lefts.min(), rights.max()])


def test_matplotlib_draws_the_same_bars_hatched(zeros_table):
    pytest.importorskip("matplotlib")
    from scistackplot import render_matplotlib

    figure = resolve(hist(exact_values=(0.0,)), zeros_table)[0]
    fig = render_matplotlib(figure)
    ax = fig.axes[0]
    hatched = [p for p in ax.patches if p.get_hatch()]
    assert len(hatched) == 4  # exact + NaN, two colour levels
    assert "= 0" in [t.get_text() for t in ax.get_xticklabels()]


def test_save_data_writes_what_each_special_slot_holds(zeros_table):
    data = plot_data(hist(exact_values=(0.0,)), zeros_table, depth="__bars")
    exact = data[data["bin_kind"] == "exact"]
    nan = data[data["bin_kind"] == "nan"]
    assert (exact["bin_start"] == 0.0).all() and (exact["bin_end"] == 0.0).all()
    assert nan["bin_start"].isna().all()
    assert int(data["count"].sum()) == 24  # every value, NaN included


def test_a_date_axis_ignores_the_special_slots():
    start = pd.Timestamp("2026-07-01 08:00")
    rows = [
        {"trial": str(t), "When": start + pd.Timedelta(minutes=10 * t) if t % 3 else pd.NaT}
        for t in range(12)
    ]
    table = LongTable.from_frame(
        pd.DataFrame(rows), factors=["trial"], measures=["When"], schema_levels=["trial"]
    )
    spec = PlotSpec(
        measures=["When"],
        roles={"trial": Role.COLLAPSE},
        kind=PlotKind.HISTOGRAM,
        histogram=HistogramOptions(exact_values=(0.0,)),
    )
    (figure,) = resolve(spec, table)
    assert set(bars(figure)[BIN_KIND]) == {"range"}
    assert figure.bins.ticks() is None


def test_one_source_the_export_runs_verbatim():
    namespace: dict = {}
    exec(BARS_SOURCE, namespace)  # noqa: S102
    counts, heights = namespace["_scistack_bar_heights"](
        VALUES, [1.0, 3.5, 6.0], (0.0,), True, "count"
    )
    assert counts.tolist() == [4, 3, 3, 2]  # = 0 | [1, 3.5) | [3.5, 6] | NaN


def _run(source: str, frame, name: str):
    namespace: dict = {}
    exec(compile(source, "<generated>", "exec"), namespace)  # noqa: S102
    return namespace[name](frame.copy(), "figure.png")


@pytest.mark.parametrize("stat", [HistStat.COUNT, HistStat.PERCENT])
@pytest.mark.parametrize("multiple", [HistMultiple.LAYER, HistMultiple.STACK])
def test_generated_code_draws_the_previewed_bars(zeros_table, stat, multiple):
    pytest.importorskip("seaborn")
    spec = hist(exact_values=(0.0,), stat=stat, multiple=multiple)
    source = generate_plot_function(spec, zeros_table, function_name="plot_h")
    assert "_scistack_bar_heights" in source
    figure = _run(source, zeros_table.frame, "plot_h")
    (preview,) = resolve(spec, zeros_table)
    ax = figure.axes[0]
    drawn = sorted(round(p.get_height(), 9) for p in ax.patches if p.get_height() > 0)
    frame = bars(preview)
    expected = sorted(round(v, 9) for v in (frame[Y] - frame[Y_BASE]) if v > 0)
    assert drawn == expected
    hatched = sorted(round(p.get_height(), 9) for p in ax.patches if p.get_hatch() and p.get_height() > 0)
    special = frame[frame[BIN_KIND] != "range"]
    assert hatched == sorted(round(v, 9) for v in (special[Y] - special[Y_BASE]) if v > 0)
    lefts, rights, _k, _l = preview.bins.slots()
    assert ax.get_xlim() == pytest.approx((lefts.min(), rights.max()))
    assert ax.get_ylim() == pytest.approx(preview.panels[0].y_limits)
    assert "= 0" in [t.get_text() for t in ax.get_xticklabels()]


def test_generated_edges_are_a_list_for_seaborn():
    """With `weights=` (percent), seaborn 0.13 tests `bins == "auto"`; an
    ndarray answers element-wise and raises "truth value ... ambiguous"
    (2026-11-01). The export passes the edges as a list."""
    spec = hist(exact_values=(0.0,), stat=HistStat.PERCENT)
    table = LongTable.from_frame(
        pd.DataFrame({"group": ["A"] * 4, "trial": list("1234"), "Speed": [0.0, 1.0, 2.0, 3.0]}),
        factors=["group", "trial"], measures=["Speed"], schema_levels=["group", "trial"],
    )
    source = generate_plot_function(spec, table, function_name="plot_h")
    assert "bins=list(_edges)" in source and 'weights="_hist_w"' in source
