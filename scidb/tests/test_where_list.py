"""``where=[a, b]`` means AND, and a filter can be held as a dict.

Issue #13. One owner for both: ``scidb.model.filters`` (``all_of``,
``as_filter``, ``filter_from_dict``, ``where_source``). ``load`` and
``for_each`` only call it, so a list cannot mean one thing in one and
another in the other.
"""

import pytest

from scidb import BaseVariable, EachOf, all_of, for_each
from scidb.model.filters import (
    ColumnFilter,
    CompoundFilter,
    VariableFilter,
    and_terms,
    as_filter,
    filter_from_dict,
    filter_to_dict,
    filter_value,
    filters_from_dicts,
    where_source,
)


class WlStep(BaseVariable):
    schema_version = 1


class WlSide(BaseVariable):
    schema_version = 1


class WlSpeed(BaseVariable):
    schema_version = 1


class WlDoubled(BaseVariable):
    schema_version = 1


_CLASSES = {c.__name__: c for c in (WlStep, WlSide, WlSpeed)}


def _double(x):
    return x * 2


@pytest.fixture
def gait_db(db):
    """subject x trial; Side L on trial 1, R on trial 2; speeds 1.2, 1.5, 1.1, 1.6."""
    rows = [(1, 1, "L", 1.2, 0.65), (1, 2, "R", 1.5, 0.72), (2, 1, "L", 1.1, 0.68), (2, 2, "R", 1.6, 0.75)]
    for subject, trial, side, speed, step in rows:
        WlSide.save(side, subject=subject, trial=trial)
        WlSpeed.save(speed, subject=subject, trial=trial)
        WlStep.save(step, subject=subject, trial=trial)
    return db


class TestListMeansAnd:
    def test_all_of_is_the_and_of_its_entries(self):
        combined = all_of([WlSide == "L", WlSpeed > 1.15])
        assert isinstance(combined, CompoundFilter) and combined.op == "AND"
        assert [f.to_key() for f in and_terms(combined)] == ["WlSide == 'L'", "WlSpeed > 1.15"]

    def test_one_entry_is_that_filter_and_none_is_no_filter(self):
        only = WlSide == "L"
        assert all_of([only]) is only
        assert all_of([]) is None
        assert as_filter([]) is None

    def test_as_filter_leaves_everything_but_a_list_alone(self):
        only = WlSide == "L"
        each = EachOf(only, WlSpeed > 1)
        assert as_filter(only) is only
        assert as_filter(each) is each
        assert as_filter("x > 1") == "x > 1"
        assert as_filter(None) is None

    def test_a_non_filter_entry_is_refused_by_name(self):
        with pytest.raises(TypeError, match="must be filters"):
            all_of([WlSide == "L", "trial > 1"])

    def test_load_with_a_list_matches_load_with_and(self, gait_db):
        listed = WlStep.load(where=[WlSide == "L", WlSpeed > 1.15])
        anded = WlStep.load(where=(WlSide == "L") & (WlSpeed > 1.15))
        as_values = lambda loaded: sorted(v.data for v in (loaded if isinstance(loaded, list) else [loaded]))
        assert as_values(listed) == as_values(anded) == [0.65]

    def test_for_each_with_a_list_is_one_run_over_the_rows_passing_both(self, gait_db):
        result = for_each(
            _double,
            inputs={"x": WlStep},
            outputs=[WlDoubled],
            where=[WlSide == "L", WlSpeed > 1.15],
            subject=[1, 2],
            trial=[1, 2],
        )
        assert result is not None
        assert sorted(round(v, 2) for v in result["WlDoubled"]) == [1.3]

    def test_each_of_is_still_one_run_per_filter(self, gait_db):
        result = for_each(
            _double,
            inputs={"x": WlStep},
            outputs=[WlDoubled],
            where=EachOf(WlSide == "L", WlSide == "R"),
            subject=[1, 2],
            trial=[1, 2],
        )
        assert result is not None and len(result) == 4


class TestFilterAsData:
    def test_text_values_are_read_as_literals_when_they_are_one(self):
        assert filter_value("80") == 80
        assert filter_value(" 1.5 ") == 1.5
        assert filter_value("True") is True
        assert filter_value("'M'") == "M"
        assert filter_value("M") == "M"
        assert filter_value(3) == 3

    def test_a_dict_becomes_a_variable_or_column_filter(self):
        whole = filter_from_dict({"variable": "WlSpeed", "op": ">", "value": "1.15"}, _CLASSES.get)
        assert isinstance(whole, VariableFilter)
        assert whole.to_key() == "WlSpeed > 1.15"
        column = filter_from_dict(
            {"variable": "WlStep", "column": "Side", "op": "==", "value": "L"}, _CLASSES.get
        )
        assert isinstance(column, ColumnFilter)
        assert column.to_key() == "WlStep['Side'] == 'L'"

    def test_dict_round_trip_keeps_the_filter(self):
        for original in (WlSpeed > 1.15, WlSide == "L"):
            again = filter_from_dict(filter_to_dict(original), _CLASSES.get)
            assert again.to_key() == original.to_key()

    @pytest.mark.parametrize(
        "spec, message",
        [
            ({"variable": "", "op": "==", "value": "1"}, "names no variable"),
            ({"variable": "WlSpeed", "op": "IN", "value": "1"}, "must be one of"),
            ({"variable": "WlSpeed", "op": ">", "value": " "}, "no value"),
            ({"variable": "Nope", "op": ">", "value": "1"}, "not a known variable"),
        ],
    )
    def test_an_unusable_dict_says_why(self, spec, message):
        with pytest.raises(ValueError, match=message):
            filter_from_dict(spec, _CLASSES.get)

    def test_blank_rows_are_skipped_and_half_filled_ones_are_not(self):
        rows = [
            {"variable": "", "op": "==", "value": ""},
            {"variable": "WlSide", "op": "==", "value": "L"},
        ]
        assert [f.to_key() for f in filters_from_dicts(rows, _CLASSES.get)] == ["WlSide == 'L'"]
        with pytest.raises(ValueError, match="no value"):
            filters_from_dicts([{"variable": "WlSide", "op": "==", "value": ""}], _CLASSES.get)

    def test_where_source_is_the_python_a_user_would_write(self):
        assert where_source(None) == ""
        assert where_source(WlSpeed > 1.15) == "WlSpeed > 1.15"
        assert where_source([WlSpeed > 1.15, WlSide == "L"]) == "[WlSpeed > 1.15, WlSide == 'L']"
        assert where_source((WlSpeed > 1.15) & (WlSide == "L")) == "[WlSpeed > 1.15, WlSide == 'L']"
        assert (
            where_source((WlSide == "L") | (WlSide == "R")) == "(WlSide == 'L') | (WlSide == 'R')"
        )
        assert where_source(~(WlSide == "L")) == "~(WlSide == 'L')"
