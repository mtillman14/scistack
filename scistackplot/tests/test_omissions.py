""""Not plotted" — what a figure leaves out (user, 2026-11-01).

`.claude/plan-plot-omissions.md`. One owner, `scistackplot.omissions`; every
drop site reports there. Reasons: no value, no record, outside the bin range
(lost); NaN elements, partly averaged (thinned). Filtered rows are never
listed; a histogram with its NaN bar shown draws its NaNs; a text measure
draws a missing value as a category.
"""

from __future__ import annotations

from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from scistackplot import (
    HistogramOptions,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    plot_data,
    render_plotly,
    resolve,
)
from scistackplot.export import NOT_PLOTTED_DEPTH
from scistackplot.omissions import (
    NAN_ELEMENTS,
    NO_RECORD,
    NO_VALUE,
    OUTSIDE_RANGE,
    PARTLY_AVERAGED,
    OmissionReport,
    combine,
)
from scistackplot.reduce import clear_plan_cache
from scistackplot.spec import LocationFilter


@pytest.fixture(autouse=True)
def _fresh_plans():
    clear_plan_cache()
    yield
    clear_plan_cache()


def _table(values: dict, absent: list[tuple] | None = None) -> LongTable:
    rows = [
        {"subject": s, "trial": t, "Speed": v} for (s, t), v in values.items()
    ]
    table = LongTable.from_frame(
        pd.DataFrame(rows), factors=["subject", "trial"], measures=["Speed"], schema_levels=["subject", "trial"]
    )
    if absent is not None:
        table.absent = pd.DataFrame(absent, columns=["subject", "trial"])
    return table


#: Subject 01: four trials, one NaN (partly averaged when trials collapse).
#: Subject 02: both trials NaN (no value once collapsed). Subject 03: fine.
VALUES = {
    ("01", "1"): 1.0, ("01", "2"): 2.0, ("01", "3"): np.nan, ("01", "4"): 4.0,
    ("02", "1"): np.nan, ("02", "2"): np.nan,
    ("03", "1"): 3.0, ("03", "2"): 5.0,
}


def bar(**kwargs) -> PlotSpec:
    base = dict(measures=["Speed"], roles={"subject": Role.GROUP, "trial": Role.COLLAPSE}, groups=["subject"], kind=PlotKind.BAR)
    base.update(kwargs)
    return PlotSpec(**base)


def means(**kwargs) -> PlotSpec:
    """One bar of subject means: trials are averaged INTO each subject before
    the sample (subjects) is summarised — where "partly averaged" lives."""
    base = dict(measures=["Speed"], roles={"subject": Role.COLLAPSE, "trial": Role.COLLAPSE}, kind=PlotKind.BAR)
    base.update(kwargs)
    return PlotSpec(**base)


def reasons(report: OmissionReport) -> dict:
    return report.counts()


def test_partly_averaged_and_no_value_after_a_collapse():
    figure = resolve(means(), _table(VALUES))[0]
    report = figure.omissions
    assert reasons(report) == {NO_VALUE: 1, PARTLY_AVERAGED: 1}
    empty = next(i for i in report.items if i.reason == NO_VALUE)
    partial = next(i for i in report.items if i.reason == PARTLY_AVERAGED)
    assert empty.keys == {"subject": "02"}
    assert partial.keys == {"subject": "01"} and partial.detail == "3 of 4 records had a value"
    assert report.drawn == 2 and report.offered == 3
    assert report.button_text() == "Not plotted: 1 of 3"
    assert report.sentence() == "1 of 3 values not plotted (1 no value); 1 partly averaged"


def test_a_bar_of_trials_leaves_its_empty_trials_out_as_no_value():
    """Trials ARE the sample here (nothing averaged before it): each empty
    trial is left out of its subject's bar mean — "No value", never "partly
    averaged" (that needs a deeper key averaged first; 2026-11-01)."""
    report = resolve(bar(), _table(VALUES))[0].omissions
    assert reasons(report) == {NO_VALUE: 3}
    assert report.drawn == 5 and report.offered == 8


def test_no_value_per_row_when_nothing_is_collapsed():
    spec = PlotSpec(measures=["Speed"], roles={"subject": Role.GROUP, "trial": Role.GROUP}, groups=["trial", "subject"], kind=PlotKind.SCATTER)
    report = resolve(spec, _table(VALUES))[0].omissions
    assert reasons(report) == {NO_VALUE: 3}
    assert {tuple(sorted(i.keys.items())) for i in report.items} == {
        (("subject", "01"), ("trial", "3")), (("subject", "02"), ("trial", "1")), (("subject", "02"), ("trial", "2")),
    }


def test_no_record_inside_the_figure_and_through_the_filters():
    table = _table(VALUES, absent=[("01", "5"), ("04", "1")])
    report = resolve(bar(), table)[0].omissions
    assert [i.keys for i in report.items if i.reason == NO_RECORD] == [
        {"subject": "01", "trial": "5"}, {"subject": "04", "trial": "1"},
    ]
    # A location the user filtered away is never "not plotted".
    filtered = bar(location_filter=LocationFilter(keys=["subject"], exclude_levels={"subject": ["04"]}))
    clear_plan_cache()
    kept = resolve(filtered, table)[0].omissions
    assert [i.keys["subject"] for i in kept.items if i.reason == NO_RECORD] == ["01"]


def test_no_record_goes_to_its_own_figure_under_a_fan_out():
    table = _table(VALUES, absent=[("01", "5"), ("03", "9")])
    spec = PlotSpec(measures=["Speed"], roles={"subject": Role.ITERATE, "trial": Role.GROUP}, groups=["trial"], kind=PlotKind.BAR)
    figures = resolve(spec, table)
    by_subject = {f.figure_key["subject"]: f.omissions for f in figures}
    assert [i.keys["trial"] for i in by_subject["01"].items if i.reason == NO_RECORD] == ["5"]
    assert [i.keys["trial"] for i in by_subject["03"].items if i.reason == NO_RECORD] == ["9"]
    total = combine([f.omissions for f in figures])
    assert total.counts()[NO_RECORD] == 2


def test_histogram_nan_bar_draws_its_nans_and_a_typed_range_reports_the_rest():
    hist = PlotSpec(measures=["Speed"], roles={"subject": Role.COLLAPSE, "trial": Role.COLLAPSE}, kind=PlotKind.HISTOGRAM)
    # Both keys collapse: the sample is subject means. 02's is NaN, drawn
    # in the NaN bar (on by default).
    report = resolve(hist, _table(VALUES))[0].omissions
    assert NO_VALUE not in reasons(report)
    clear_plan_cache()
    off = resolve(replace(hist, histogram=HistogramOptions(show_nan=False)), _table(VALUES))[0].omissions
    assert reasons(off).get(NO_VALUE) == 1
    clear_plan_cache()
    pinned = replace(hist, histogram=HistogramOptions(range_max=3.0))
    outside = [i for i in resolve(pinned, _table(VALUES))[0].omissions.items if i.reason == OUTSIDE_RANGE]
    assert [i.keys for i in outside] == [{"subject": "03"}]  # its mean 4 > 3


def test_nan_elements_of_a_1d_measure():
    frame = pd.DataFrame(
        {"subject": ["01", "02"], "Signal": [[1.0, np.nan, 3.0], [1.0, 2.0, 3.0]]}
    )
    table = LongTable.from_frame(frame, factors=["subject"], measures=["Signal"], schema_levels=["subject"])
    spec = PlotSpec(measures=["Signal"], roles={"subject": Role.GROUP}, groups=["subject"], kind=PlotKind.LINE)
    report = resolve(spec, table)[0].omissions
    assert reasons(report) == {NAN_ELEMENTS: 1}
    assert report.items[0].keys == {"subject": "01"} and report.items[0].detail == "1 of 3 elements NaN"
    assert report.button_text() == "Partly plotted: 1"


def test_a_text_measure_draws_its_missing_values():
    frame = pd.DataFrame({"subject": ["01", "02", "03"], "Side": ["L", None, "R"]})
    table = LongTable.from_frame(frame, factors=["subject"], measures=["Side"], schema_levels=["subject"])
    spec = PlotSpec(measures=["Side"], roles={"subject": Role.COLLAPSE}, kind=PlotKind.BAR)
    assert resolve(spec, table)[0].omissions.items == []


def test_footnote_off_by_default_and_drawn_when_asked():
    table = _table(VALUES)
    payload = render_plotly(resolve(means(), table)[0])
    assert not any(a.get("name") == "not_plotted_note" for a in payload["layout"]["annotations"])
    clear_plan_cache()
    on = resolve(means(style=StyleOptions(not_plotted_note=True)), table)[0]
    notes = [a for a in render_plotly(on)["layout"]["annotations"] if a.get("name") == "not_plotted_note"]
    assert len(notes) == 1 and "1 of 3 values not plotted" in notes[0]["text"]


def test_matplotlib_draws_the_footnote():
    pytest.importorskip("matplotlib")
    from scistackplot import render_matplotlib

    figure = resolve(means(style=StyleOptions(not_plotted_note=True)), _table(VALUES))[0]
    fig = render_matplotlib(figure)
    assert any(t.get_gid() == "not_plotted_note" for t in fig.texts)


def test_csv_lists_every_item():
    table = _table(VALUES, absent=[("04", "1")])
    data = plot_data(means(), table, depth=NOT_PLOTTED_DEPTH)
    assert list(data.columns)[-3:] == ["variable", "reason", "detail"]
    assert sorted(data["reason"]) == ["No record", "No value", "Partly averaged"]


def test_report_to_dict_caps_items_but_keeps_counts():
    from scistackplot.omissions import MAX_ITEMS, Omission

    report = OmissionReport(drawn=1, items=[Omission({"s": str(i)}, "V", NO_VALUE) for i in range(MAX_ITEMS + 3)])
    data = report.to_dict()
    assert len(data["items"]) == MAX_ITEMS and data["truncated"] == 3
    assert data["counts"] == {NO_VALUE: MAX_ITEMS + 3}
