"""
"Show sample" — Stage 2: the overlay rows a figure carries, numerically.

The unbalanced fixture from ``test_nested_collapse`` (subject 01 has trials
{1, 2, 3} in ``pre``, subject 02 has {9}) so the overlay levels differ:

    show subject  -> per-subject means      pre: {2, 9}
    show trial    -> every trial            pre: {1, 2, 3, 9}

and the marks are unchanged by either (bar at 5.5, nested).
"""

from __future__ import annotations

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
    resolve,
)
from scistackplot.resolved import COLOR, SERIES, X, Y
from scistackplot.spaghetti import overlay_offsets, series_offsets
from scistackplot.ylimits import ExtentMode


@pytest.fixture
def unbalanced() -> LongTable:
    rows = [
        ("01", "pre", "1", 1.0),
        ("01", "pre", "2", 2.0),
        ("01", "pre", "3", 3.0),
        ("02", "pre", "1", 9.0),
        ("01", "post", "1", 10.0),
        ("01", "post", "2", 20.0),
        ("02", "post", "1", 30.0),
    ]
    frame = pd.DataFrame(rows, columns=["subject", "session", "trial", "M"])
    return LongTable.from_frame(
        frame,
        factors=["subject", "session", "trial"],
        measures=["M"],
        name="M",
        schema_levels=["subject", "session", "trial"],
    )


def _spec(show, kind=PlotKind.BAR, **kwargs) -> PlotSpec:
    base = dict(
        measures=["M"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=kind,
        aggregate=Aggregation(error=ErrorBand.SD),
        show_sample=list(show),
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _figure(spec, table):
    (figure,) = resolve(spec, table)
    return figure


def _at(sample: pd.DataFrame, x) -> list[float]:
    return sorted(sample[sample[X] == x][Y].tolist())


# --- the rows ----------------------------------------------------------------


def test_no_selection_carries_no_sample(unbalanced):
    figure = _figure(_spec([]), unbalanced)
    assert figure.panels[0].sample is None
    assert figure.sample_shown == [] and figure.sample_offsets == {}
    assert figure.to_dict()["panels"][0]["sample"] is None


def test_show_subject_is_the_per_subject_means(unbalanced):
    figure = _figure(_spec(["subject"]), unbalanced)
    sample = figure.panels[0].sample
    assert _at(sample, "pre") == pytest.approx([2.0, 9.0])
    assert _at(sample, "post") == pytest.approx([15.0, 30.0])
    assert figure.sample_shown == ["subject"]
    assert sorted(sample["subject"].unique()) == ["01", "02"]


def test_show_trial_is_every_trial(unbalanced):
    figure = _figure(_spec(["trial"]), unbalanced)
    sample = figure.panels[0].sample
    assert _at(sample, "pre") == pytest.approx([1.0, 2.0, 3.0, 9.0])
    assert figure.sample_shown == ["subject", "trial"]
    # The identity is composed outermost first, like every other id.
    assert set(sample[SERIES]) == {"01 | 1", "01 | 2", "01 | 3", "02 | 1"}


def test_the_marks_do_not_change(unbalanced):
    plain = _figure(_spec([]), unbalanced).panels[0].frame
    with_overlay = _figure(_spec(["trial"]), unbalanced).panels[0].frame
    pd.testing.assert_frame_equal(plain, with_overlay)
    assert with_overlay[with_overlay[X] == "pre"][Y].iloc[0] == pytest.approx(5.5)


def test_pooled_overlay_pools_the_cut_keys(unbalanced):
    """Under "weight by N" the marks pool every collapsed key; an overlay of
    subjects must then be pooled trial means — the same numbers, or the
    points would sit around a centre they were never averaged into."""
    spec = _spec(["subject"], aggregate=Aggregation(error=ErrorBand.SD, pooled=True))
    figure = _figure(spec, unbalanced)
    assert _at(figure.panels[0].sample, "pre") == pytest.approx([2.0, 9.0])
    # (Balanced-within-subject here, so pooled == nested per subject; what the
    # flag changes is the STEP count, which the log line names as pooled.)


def test_every_overlay_kind_carries_the_sample(unbalanced):
    for kind in (PlotKind.BAR, PlotKind.BOX, PlotKind.VIOLIN, PlotKind.SCATTER, PlotKind.STRIP):
        figure = _figure(_spec(["trial"], kind=kind), unbalanced)
        assert figure.panels[0].sample is not None, kind
        assert len(figure.panels[0].sample) == 7, kind


def test_an_inert_selection_carries_nothing(unbalanced):
    spec = _spec(["trial"], kind=PlotKind.SPAGHETTI, roles={
        "subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE,
    }, groups=["subject", "session"])
    figure = _figure(spec, unbalanced)
    assert figure.panels[0].sample is None


def test_overlay_is_split_by_facet_like_the_marks(unbalanced):
    spec = _spec(["trial"], roles={
        "subject": Role.COLLAPSE, "session": Role.FACET, "trial": Role.COLLAPSE,
    }, groups=[])
    figure = _figure(spec, unbalanced)
    by_title = {panel.title: panel.sample for panel in figure.panels}
    assert set(by_title) == {"pre", "post"}
    assert sorted(by_title["pre"][Y]) == pytest.approx([1.0, 2.0, 3.0, 9.0])
    assert sorted(by_title["post"][Y]) == pytest.approx([10.0, 20.0, 30.0])


def test_overlay_carries_the_marks_own_colour_and_x(unbalanced):
    """``__x`` and ``__color`` are the MARK's own columns, so a renderer
    places a point by exactly the mark's position and dodge."""
    spec = _spec(["trial"], roles={
        "subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE,
    }, groups=["subject", "session"], color="subject")
    figure = _figure(spec, unbalanced)
    sample = figure.panels[0].sample
    assert set(sample[COLOR]) == {"01", "02"}
    assert set(sample[X]) == set(figure.panels[0].frame[X])
    assert figure.sample_shown == ["trial"]


# --- join and offsets ----------------------------------------------------------


def test_join_is_decided_once_per_figure(unbalanced):
    joined = _figure(_spec(["subject"]), unbalanced)
    assert joined.sample_join is True and "Repeated measures" in joined.sample_join_reason
    points = _figure(_spec(["trial"]), unbalanced)
    assert points.sample_join is False
    forced = _figure(_spec(["trial"], join_sample=True), unbalanced)
    assert forced.sample_join is True


def test_offsets_are_the_spaghetti_rule_inside_the_slot(unbalanced):
    """One colour level: the whole spaghetti band. Two: half of it, so the
    points stay inside their own (half-width) mark."""
    figure = _figure(_spec(["subject"]), unbalanced)
    assert figure.sample_offsets == series_offsets(["01", "02"])
    assert figure.sample_offsets == {"01": -0.2, "02": 0.2}

    spec = _spec(["trial"], roles={
        "subject": Role.GROUP, "session": Role.GROUP, "trial": Role.COLLAPSE,
    }, groups=["subject", "session"], color="subject")
    figure = _figure(spec, unbalanced)
    ids = sorted(figure.panels[0].sample[SERIES].unique())
    assert figure.sample_offsets == overlay_offsets(ids, 2)
    assert max(abs(v) for v in figure.sample_offsets.values()) == pytest.approx(0.1)


def test_overlay_offsets_shrink_with_the_colour_count():
    assert overlay_offsets(["a", "b"], 1) == {"a": -0.2, "b": 0.2}
    assert overlay_offsets(["a", "b"], 4) == {"a": -0.05, "b": 0.05}
    assert overlay_offsets(["a"], 3) == {"a": 0.0}


# --- transport -------------------------------------------------------------------


def test_to_dict_carries_the_overlay(unbalanced):
    payload = _figure(_spec(["subject"]), unbalanced).to_dict()
    assert payload["sample"] == {
        "shown": ["subject"],
        "join": True,
        "join_reason": payload["sample"]["join_reason"],
        "offsets": {"01": -0.2, "02": 0.2},
    }
    rows = payload["panels"][0]["sample"]
    assert len(rows) == 4
    assert {row["subject"] for row in rows} == {"01", "02"}


# --- y limits ------------------------------------------------------------------


def test_limits_include_the_overlay_points():
    """Subject 01 has trials {1, 2, 3}, subject 02 has {4, 20}: a box draws
    the per-subject means {2, 12}, so its axis spans 2..12; showing every
    trial puts 1 and 20 on it, and both ends must move to hold them."""
    rows = [("01", "1", 1.0), ("01", "2", 2.0), ("01", "3", 3.0), ("02", "1", 4.0), ("02", "2", 20.0)]
    frame = pd.DataFrame(rows, columns=["subject", "trial", "M"])
    table = LongTable.from_frame(
        frame, factors=["subject", "trial"], measures=["M"], name="M",
        schema_levels=["subject", "trial"],
    )
    roles = {"subject": Role.COLLAPSE, "trial": Role.COLLAPSE}
    box = PlotSpec(measures=["M"], roles=roles, groups=[], kind=PlotKind.BOX)
    low, high = _figure(box, table).panels[0].y_limits
    assert low <= 2.0 < 4.0 < high <= 20.0, "the sample-level axis: means only"

    with_points = PlotSpec(measures=["M"], roles=roles, groups=[], kind=PlotKind.BOX, show_sample=["trial"])
    low2, high2 = _figure(with_points, table).panels[0].y_limits
    assert high2 >= 20.0 > high
    assert low2 <= 1.0 and low2 < low


def test_limits_on_the_unbalanced_fixture_only_widen(unbalanced):
    """Every subject-02 mean is a single trial there, so the top end is already
    reached; the overlay can only ever widen an axis, never narrow it."""
    box = _figure(_spec([], kind=PlotKind.BOX), unbalanced).panels[0].y_limits
    with_points = _figure(_spec(["trial"], kind=PlotKind.BOX), unbalanced).panels[0].y_limits
    assert with_points[0] <= box[0] and with_points[1] >= box[1]


def test_extent_mode_keys_the_memo_by_the_selection(unbalanced):
    roles = _spec([]).roles
    assert ExtentMode.for_spec(_spec([]), roles).overlay == ()
    assert ExtentMode.for_spec(_spec(["trial"]), roles).overlay == ("trial",)
    # An overlay the kind cannot carry does not miss the memo.
    assert ExtentMode.for_spec(_spec(["trial"], kind=PlotKind.SPAGHETTI), roles).overlay == ()
