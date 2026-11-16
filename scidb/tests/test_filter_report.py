"""``filter_report``: which locations of a variable pass which filter (issue #13).

The one owner of pass / fail / no value; the where= check figure only draws
it. A location the filter variable says nothing about is "no value" (a run
would raise a coverage error there; the check shows the gap).
"""

from scidb import BaseVariable
from scidb.model.filters import (
    VERDICT_FAIL,
    VERDICT_NO_VALUE,
    VERDICT_PASS,
    VERDICT_PREFIX,
    filter_label,
    filter_report,
)


class FrStep(BaseVariable):
    schema_version = 1


class FrSpeed(BaseVariable):
    schema_version = 1


class FrAge(BaseVariable):
    schema_version = 1


_CLASSES = {c.__name__: c for c in (FrStep, FrSpeed, FrAge)}
_F0 = f"{VERDICT_PREFIX}0"
_F1 = f"{VERDICT_PREFIX}1"


def _verdicts(frame, column=_F0):
    return {(row["subject"], row["trial"]): row[column] for _, row in frame.iterrows()}


def _seed():
    for subject, trial, speed in [(1, 1, 1.2), (1, 2, 1.5), (2, 1, 1.1), (2, 2, 1.6)]:
        FrStep.save(0.5, subject=subject, trial=trial)
        FrSpeed.save(speed, subject=subject, trial=trial)
    FrAge.save(82, subject=1)
    FrAge.save(60, subject=2)


def test_same_level_filter_passes_and_fails_per_location(db):
    _seed()
    frame, keys = filter_report(
        db, FrStep, [{"variable": "FrSpeed", "op": ">", "value": "1.3"}], _CLASSES.get
    )
    assert keys == ["FrSpeed > 1.3"]
    assert _verdicts(frame) == {
        ("1", "1"): VERDICT_FAIL,
        ("1", "2"): VERDICT_PASS,
        ("2", "1"): VERDICT_FAIL,
        ("2", "2"): VERDICT_PASS,
    }


def test_a_coarser_filter_applies_to_every_location_beneath_it(db):
    _seed()
    frame, _ = filter_report(
        db, FrStep, [{"variable": "FrAge", "op": ">", "value": "80"}], _CLASSES.get
    )
    assert set(_verdicts(frame).values()) == {VERDICT_PASS, VERDICT_FAIL}
    assert _verdicts(frame)[("1", "1")] == VERDICT_PASS
    assert _verdicts(frame)[("2", "2")] == VERDICT_FAIL


def test_a_location_the_filter_variable_has_no_value_for_is_no_value(db):
    _seed()
    FrStep.save(0.5, subject=3, trial=1)  # no Age for subject 3
    frame, _ = filter_report(
        db, FrStep, [{"variable": "FrAge", "op": ">", "value": "80"}], _CLASSES.get
    )
    assert _verdicts(frame)[("3", "1")] == VERDICT_NO_VALUE


def test_two_filters_get_a_column_each_in_the_order_given(db):
    _seed()
    frame, keys = filter_report(
        db,
        FrStep,
        [
            {"variable": "FrSpeed", "op": ">", "value": "1.3"},
            {"variable": "FrAge", "op": ">", "value": "80"},
        ],
        _CLASSES.get,
    )
    assert keys == ["FrSpeed > 1.3", "FrAge > 80"]
    assert _verdicts(frame, _F0)[("1", "2")] == VERDICT_PASS
    assert _verdicts(frame, _F1)[("1", "2")] == VERDICT_PASS
    assert _verdicts(frame, _F1)[("2", "2")] == VERDICT_FAIL


def test_the_label_is_the_filters_own_text_without_a_variable_class():
    assert filter_label({"variable": "Age", "op": ">", "value": "80"}) == "Age > 80"
    assert (
        filter_label({"variable": "Gait", "column": "Side", "op": "==", "value": "L"})
        == "Gait['Side'] == 'L'"
    )
