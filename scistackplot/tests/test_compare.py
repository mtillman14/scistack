"""
Compare to reference (``PlotSpec.comparison``, ``scistackplot.compare``):
difference / % change from one level of a grouping layer, numerically.

Plan ``.claude/plan-compare-to-reference.md``; doc
``docs/claude/compare-to-reference.md``. The fixture is chosen so every rule
has a number that only it produces:

    subject 01: s1 trials {1, 3} -> 2    s2 {3, 5} -> 4    s3 {5, 7} -> 6
    subject 02: s1 {10}          -> 10   s2 {15}   -> 15   s3 {30}   -> 30
    subject 03: s1 {4}           -> 4    s2 {2}    -> 2    (no s3)

Paired difference (each subject minus its own s1): s2 = mean{2, 5, -2} =
5/3; s3 = mean{4, 20} = 12. Per summary it would be s3 = 18 - 16/3 = 12.67,
so s3 tells the two apart (subject 03 is missing there).

Paired % change: s2 = mean{100, 50, -50} = 33.33; the ratio of the means
(7 / (16/3) - 1 = 31.25 %) is NOT what is drawn.
"""

from __future__ import annotations

import json
import math

import numpy as np
import pandas as pd
import pytest

from scistackplot import (
    Aggregation,
    Alias,
    CompareMode,
    Comparison,
    ErrorBand,
    LongTable,
    PlotKind,
    PlotSpec,
    Role,
    StyleOptions,
    capabilities,
    plot_data,
    resolve,
)
from scistackplot.compare import NO_REFERENCE, NOT_POSITIVE, RAGGED, plan_comparison
from scistackplot.reduce import _plan, clear_plan_cache, planned_y_limits
from scistackplot.resolved import COLOR, X, Y, Y_HIGH, Y_LOW
from scistackplot.roles import complete_assignment

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


def _table(rows=ROWS) -> LongTable:
    frame = pd.DataFrame(rows, columns=["subject", "session", "trial", "M"])
    return LongTable.from_frame(
        frame,
        factors=["subject", "session", "trial"],
        measures=["M"],
        name="M",
        schema_levels=["subject", "session", "trial"],
    )


@pytest.fixture
def table() -> LongTable:
    clear_plan_cache()
    return _table()


def _spec(mode=CompareMode.DIFFERENCE, kind=PlotKind.BAR, level="s1", **kwargs) -> PlotSpec:
    base = dict(
        measures=["M"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE},
        groups=["session"],
        kind=kind,
        aggregate=Aggregation(error=ErrorBand.SD),
        comparison=Comparison(layer="session", level=level, mode=mode),
    )
    base.update(kwargs)
    return PlotSpec(**base)


def _bars(spec, table) -> dict[str, tuple[float, float, float]]:
    (figure,) = resolve(spec, table)
    (panel,) = figure.panels
    return {
        str(row[X]): (row[Y], row[Y_LOW], row[Y_HIGH])
        for _, row in panel.frame.iterrows()
    }


# --- the spec ---------------------------------------------------------------------


def test_comparison_round_trips_through_json():
    spec = _spec(CompareMode.PERCENT)
    back = PlotSpec.from_json(spec.to_json())
    assert back.comparison == spec.comparison
    assert json.loads(spec.to_json())["comparison"] == {
        "layer": "session",
        "level": "s1",
        "mode": "percent",
        "active": True,
    }


def test_no_comparison_round_trips_as_absent():
    spec = _spec(comparison=None)
    assert "comparison" not in spec.to_dict()
    assert PlotSpec.from_json(spec.to_json()).comparison is None


# --- paired -----------------------------------------------------------------------


def test_paired_difference_is_each_subject_against_its_own_reference(table):
    bars = _bars(_spec(), table)
    # The reference tick keeps its mark at exactly 0, with no spread (D4).
    assert bars["s1"] == pytest.approx((0.0, 0.0, 0.0))
    assert bars["s2"][0] == pytest.approx(5 / 3)
    # Per summary would give 18 - 16/3; paired drops nothing and averages
    # subject 01's and 02's own changes.
    assert bars["s3"][0] == pytest.approx(12.0)


def test_paired_percent_is_the_mean_of_the_changes_not_the_change_of_the_means(table):
    bars = _bars(_spec(CompareMode.PERCENT), table)
    assert bars["s1"][0] == pytest.approx(0.0)
    assert bars["s2"][0] == pytest.approx(100 / 3)
    assert not math.isclose(bars["s2"][0], 31.25)


def test_the_plan_says_paired_and_why(table):
    spec = _spec()
    plan = _plan(spec, table)
    comparison = plan_comparison(plan.spec, plan.roles, plan.table)
    assert comparison is not None
    assert comparison.paired is True
    assert comparison.sample == ("subject",)
    assert "every session" in comparison.reason


def test_a_different_reference_level(table):
    bars = _bars(_spec(level="s2"), table)
    assert bars["s2"][0] == pytest.approx(0.0)
    # s1 - s2 per subject: {-2, -5, 2}
    assert bars["s1"][0] == pytest.approx(-5 / 3)


# --- per summary ------------------------------------------------------------------


def test_an_unpairable_sample_is_compared_with_the_reference_centre(table):
    """Subject in panels, trial the sample: a trial belongs to one session,
    so nothing pairs, and each trial is measured from the s1 mean."""
    spec = _spec(roles={"subject": Role.FACET, "session": Role.GROUP, "trial": Role.COLLAPSE})
    plan = _plan(spec, table)
    comparison = plan_comparison(plan.spec, plan.roles, plan.table)
    assert comparison is not None and comparison.paired is False
    (figure,) = resolve(spec, table)
    panel = next(p for p in figure.panels if p.key == {"subject": "01"})
    rows = {str(r[X]): r for _, r in panel.frame.iterrows()}
    # Subject 01's s1 trials {1, 3} from their mean 2: {-1, 1}. The reference
    # keeps its own spread around 0 (D4).
    assert rows["s1"][Y] == pytest.approx(0.0)
    assert rows["s1"][Y_HIGH] - rows["s1"][Y_LOW] > 0
    assert rows["s2"][Y] == pytest.approx(2.0)


def test_pooled_samples_compare_per_summary(table):
    spec = _spec(aggregate=Aggregation(error=ErrorBand.SD, pooled=True))
    bars = _bars(spec, table)
    # Every trial against the s1 mean over every trial {1, 3, 10, 4} = 4.5.
    assert bars["s2"][0] == pytest.approx(np.mean([3, 5, 15, 2]) - 4.5)


# --- drops ------------------------------------------------------------------------


def test_a_unit_with_no_reference_is_dropped_whole_and_reported(table):
    rows = [row for row in ROWS if not (row[0] == "03" and row[1] == "s1")]
    table = _table(rows)
    (figure,) = resolve(_spec(), table)
    outcome = figure.comparison["outcome"]
    assert outcome["dropped"] == {NO_REFERENCE: ["subject=03"]}
    bars = {str(r[X]): r[Y] for _, r in figure.panels[0].frame.iterrows()}
    # s2 without subject 03: mean{2, 5}
    assert bars["s2"] == pytest.approx(3.5)


def test_a_percent_change_from_a_non_positive_reference_is_dropped_once():
    rows = [(s, sess, t, (-1.0 if (s == "02" and sess == "s1") else v)) for s, sess, t, v in ROWS]
    table = _table(rows)
    (figure,) = resolve(_spec(CompareMode.PERCENT), table)
    dropped = figure.comparison["outcome"]["dropped"]
    assert dropped == {NOT_POSITIVE: ["subject=02"]}


# --- "Show sample" ----------------------------------------------------------------


def test_sample_trials_are_measured_from_their_own_subjects_reference(table):
    spec = _spec(show_sample=["trial"])
    (figure,) = resolve(spec, table)
    sample = figure.panels[0].sample
    s2_subject_01 = sample[(sample[X] == "s2") & (sample["subject"] == "01")]
    # Trials {3, 5} minus subject 01's s1 mean 2: {1, 3}; they average back to
    # the subject's own change, 2.
    assert sorted(s2_subject_01[Y]) == pytest.approx([1.0, 3.0])


# --- inert ------------------------------------------------------------------------


def test_a_layer_that_no_longer_groups_is_inert_not_refused(table):
    spec = _spec(comparison=Comparison(layer="trial", level="1"))
    bars = _bars(spec, table)
    # Raw values: subject means {2, 10, 4} at s1.
    assert bars["s1"][0] == pytest.approx(16 / 3)
    state = capabilities(spec, table)["comparison"]["state"]
    assert "not a grouping layer" in state["inert"]


def test_a_log_axis_leaves_the_comparison_inert(table):
    spec = _spec(style=StyleOptions(log_y=True))
    bars = _bars(spec, table)
    assert bars["s1"][0] == pytest.approx(16 / 3)


def test_switched_off_keeps_the_settings_and_draws_raw(table):
    spec = _spec(comparison=Comparison(layer="session", level="s1", active=False))
    assert _bars(spec, table)["s1"][0] == pytest.approx(16 / 3)
    assert PlotSpec.from_json(spec.to_json()).comparison.active is False


# --- words ------------------------------------------------------------------------


def test_the_y_title_says_what_was_computed(table):
    (figure,) = resolve(_spec(), table)
    assert figure.labels.y == "Δ M (from session s1)"
    (figure,) = resolve(_spec(CompareMode.PERCENT), table)
    assert figure.labels.y == "M, % change from session s1"


def test_the_y_title_reads_aliases_and_a_typed_title_wins(table):
    spec = _spec(aliases={"M": Alias(name="Mass"), "session": Alias(name="Visit", levels={"s1": "baseline"})})
    (figure,) = resolve(spec, table)
    assert figure.labels.y == "Δ Mass (from Visit baseline)"
    spec = _spec(style=StyleOptions(y_label="Change"))
    (figure,) = resolve(spec, table)
    assert figure.labels.y == "Change"


# --- y limits and the plan cache (D5) ---------------------------------------------


def test_toggling_reuses_the_plan_but_recomputes_the_limits(table):
    raw = _spec(comparison=None)
    compared = _spec(level="s3")
    first = _plan(raw, table)
    second = _plan(compared, table)
    # The same planned frames: the comparison is not part of the data question.
    assert first.frame is second.frame
    _scope, raw_limits = planned_y_limits(raw, table)
    _scope, compared_limits = planned_y_limits(compared, table)
    assert raw_limits != compared_limits
    # s1 - s3 per subject is {-4, -20}: the compared axis reaches below zero.
    assert min(low for low, _ in compared_limits.values()) < -10


# --- "Save data" ------------------------------------------------------------------


def test_saved_data_holds_the_compared_sample(table):
    spec = _spec(kind=PlotKind.BOX)
    data = plot_data(spec, table)
    s3 = data[data["session"] == "s3"].set_index("subject")["M"]
    assert s3.to_dict() == pytest.approx({"01": 4.0, "02": 20.0})
    (figure,) = resolve(spec, table)
    drawn = sorted(figure.panels[0].frame[Y])
    assert sorted(data["M"]) == pytest.approx(drawn)


def test_saved_data_down_to_trial_uses_the_subject_baseline(table):
    data = plot_data(_spec(kind=PlotKind.BOX), table, depth="trial")
    rows = data[(data["subject"] == "01") & (data["session"] == "s2")]
    assert sorted(rows["M"]) == pytest.approx([1.0, 3.0])


# --- the capability report --------------------------------------------------------


def test_capabilities_offer_the_grouping_layers_and_a_default(table):
    report = capabilities(_spec(comparison=None), table)["comparison"]
    assert report["available"] is True
    assert report["default_layer"] == "session"
    assert report["default_level"] == "s1"
    assert report["layers"] == [{"name": "session", "levels": ["s1", "s2", "s3"]}]
    assert report["state"] == {"set": False}


def test_capabilities_state_the_pairing(table):
    state = capabilities(_spec(), table)["comparison"]["state"]
    assert state["paired"] is True
    assert state["inert"] is None


# --- 1-D --------------------------------------------------------------------------


def _series_table(lengths: dict[tuple[str, str], int] | None = None) -> LongTable:
    rows = []
    for subject, offset in (("01", 0.0), ("02", 10.0)):
        for session, add in (("s1", 0.0), ("s2", 1.0)):
            n = (lengths or {}).get((subject, session), 4)
            rows.append(
                {
                    "subject": subject,
                    "session": session,
                    "Signal": [offset + add + i for i in range(n)],
                }
            )
    return LongTable.from_frame(
        pd.DataFrame(rows),
        factors=["subject", "session"],
        measures=["Signal"],
        name="Signal",
        schema_levels=["subject", "session"],
    )


def _series_spec(kind=PlotKind.LINE) -> PlotSpec:
    return PlotSpec(
        measures=["Signal"],
        roles={"subject": Role.COLLAPSE, "session": Role.GROUP},
        groups=["session"],
        color="session",
        kind=kind,
        comparison=Comparison(layer="session", level="s2"),
    )


@pytest.mark.parametrize("kind", [PlotKind.LINE, PlotKind.BAND])
def test_one_d_is_compared_position_by_position(kind):
    clear_plan_cache()
    (figure,) = resolve(_series_spec(kind), _series_table())
    frame = figure.panels[0].frame
    # s1 is s2 minus 1 at every position, for both subjects.
    s1 = frame[frame[COLOR] == "s1"]
    assert set(np.round(s1[Y], 9)) == {-1.0}


def test_a_one_d_series_with_a_different_length_is_dropped():
    clear_plan_cache()
    table = _series_table({("02", "s1"): 3})
    (figure,) = resolve(_series_spec(), table)
    assert figure.comparison["outcome"]["dropped"] == {RAGGED: ["subject=02"]}


# --- the plan's own groups --------------------------------------------------------


def test_the_reference_layer_must_be_a_completed_grouping_layer(table):
    spec = _spec()
    assert "session" in complete_assignment(spec, table).groups
