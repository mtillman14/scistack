"""Tests for EachOf's new standalone expansion in scifor.for_each.

The container (EachOf.alternatives) moved from scidb to scifor; this is the
first time pure Python scifor.for_each can expand it at all -- previously
only scidb.for_each's DB-aware recursion supported EachOf.
"""

import pandas as pd
import pytest

from scifor import Col, EachOf, LoopHooks, for_each, set_schema


def setup_function():
    set_schema([])


def make_df(subjects=(1, 2)):
    return pd.DataFrame({"subject": list(subjects), "value": [float(s) for s in subjects]})


def test_empty_each_of_constructs():
    """Construction is legal -- it is what a scidb.Parameter with no value
    yet is, and MATLAB cannot conditionally skip the superclass call that
    would otherwise have to refuse it. Expansion is where it is refused."""
    assert EachOf().alternatives == []


def test_expanding_an_empty_axis_raises_and_names_the_input():
    """The failure this replaces was SILENT: the cartesian product over a
    zero-length axis is empty, so for_each iterated zero times, wrote no
    records and returned as though it had worked."""
    set_schema(["subject"])
    with pytest.raises(ValueError, match="no alternatives") as excinfo:
        for_each(
            lambda value: value,
            inputs={"value": EachOf()},
            subject=[1, 2],
        )
    assert "'value'" in str(excinfo.value)


def test_expanding_an_empty_where_axis_raises():
    set_schema(["subject"])
    with pytest.raises(ValueError, match="where="):
        for_each(
            lambda value: value,
            inputs={"value": make_df((1, 2))},
            where=EachOf(),
            subject=[1, 2],
        )


def test_single_alternative_behaves_like_passing_it_directly():
    set_schema(["subject"])
    df_a = make_df((1, 2))
    result = for_each(
        lambda value: value,
        inputs={"value": EachOf(df_a)},
        subject=[1, 2],
    )
    assert sorted(result["output"]) == [1.0, 2.0]


def test_each_of_input_expands_and_concatenates():
    # Both alternatives carry every iterated subject -- a combo scifor can't
    # match now skips as NoDataError, so a partial-subject df here would
    # silently drop rows instead of cleanly testing "N alternatives -> N
    # recursive calls, concatenated".
    set_schema(["subject"])
    df_a = make_df((1, 2))
    df_b = pd.DataFrame({"subject": [1, 2], "value": [10.0, 20.0]})
    result = for_each(
        lambda value: value,
        inputs={"value": EachOf(df_a, df_b)},
        subject=[1, 2],
    )
    # 2 alternatives x 2 subjects = 4 rows total.
    assert len(result) == 4
    assert sorted(result["output"]) == [1.0, 2.0, 10.0, 20.0]


def test_each_of_constant_axis():
    set_schema(["subject"])
    df = make_df((1, 2))
    result = for_each(
        lambda value, bandwidth: value * bandwidth,
        inputs={"value": df, "bandwidth": EachOf(1.0, 2.0)},
        subject=[1, 2],
    )
    # 2 subjects x 2 bandwidth alternatives = 4 rows total.
    assert len(result) == 4


def test_each_of_where_axis():
    # Each where= alternative recurses over both subjects independently;
    # the combo it excludes now filters to 0 rows and is skipped as
    # NoDataError (expected: this alternative has no data for that
    # subject), rather than completing with empty data. So the total is
    # just the 2 combos where subject actually matches the alternative's
    # where= clause, carrying the real per-subject value.
    set_schema(["subject"])
    df = make_df((1, 2))
    result = for_each(
        lambda value: value,
        inputs={"value": df},
        where=EachOf(Col("subject") == 1, Col("subject") == 2),
        subject=[1, 2],
    )
    assert len(result) == 2
    assert sorted(result["output"]) == [1.0, 2.0]


def test_each_of_cancel_check_stops_immediately():
    """A _cancel_check that's already True stops before any alternative
    produces rows (checked per-combo, including inside each recursive
    EachOf call)."""
    set_schema(["subject"])
    df = make_df((1,))

    result = for_each(
        lambda value: value,
        inputs={"value": EachOf(df, df, df)},
        subject=[1],
        hooks=LoopHooks(cancel_check=lambda: True),
    )
    assert result is None or len(result) == 0
