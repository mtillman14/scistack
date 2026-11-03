"""With no schema, the iterated keys filter (RunPlan.filter_keys).

One rule for both languages since 2026-11-02 (plan stage 3). Mirrors
MATLAB's TestSciforForEachNoSchema.m; before, Python handed every combo the
whole table when no schema was set.
"""

import logging

import pandas as pd
import pytest

from scifor import ColumnSelection, for_each, set_schema


@pytest.fixture(autouse=True)
def _no_schema():
    set_schema([])
    yield
    set_schema([])


def test_single_key_filters():
    df = pd.DataFrame({"pass": [1, 2], "value": [10, 30]})
    result = for_each(lambda x: x, {"x": df}, outputs=["out"], **{"pass": [1, 2]})
    assert sorted(result["out"].tolist()) == [10, 30]


def test_two_keys_filter():
    df = pd.DataFrame(
        {"pass": [1, 1, 2, 2], "Cycle": ["A", "B", "A", "B"], "value": [10, 20, 30, 40]}
    )
    result = for_each(
        lambda x: x, {"x": df}, outputs=["out"], **{"pass": [1, 2], "Cycle": ["A", "B"]}
    )
    assert len(result) == 4
    row = result[(result["pass"] == 2) & (result["Cycle"] == "B")]
    assert row["out"].tolist() == [40]


def test_empty_list_resolves_and_filters():
    df = pd.DataFrame(
        {"pass": [1, 1, 2, 2], "Cycle": ["A", "B", "A", "B"], "value": [10, 20, 30, 40]}
    )
    result = for_each(lambda x: x, {"x": df}, outputs=["out"], **{"pass": [], "Cycle": []})
    row = result[(result["pass"] == 1) & (result["Cycle"] == "A")]
    assert row["out"].tolist() == [10]


def test_column_selection_gets_one_value():
    df = pd.DataFrame({"pass": [1, 2], "data": [5.0, 6.0], "other": [0, 0]})
    result = for_each(
        lambda d: d, {"d": ColumnSelection(df, ["data"])}, outputs=["out"],
        **{"pass": [1, 2]},
    )
    assert sorted(result["out"].tolist()) == [5.0, 6.0]


def test_the_choice_is_logged(caplog):
    df = pd.DataFrame({"pass": [1], "value": [10]})
    with caplog.at_level(logging.INFO, logger="scifor"):
        for_each(lambda x: x, {"x": df}, outputs=["out"], **{"pass": [1]})
    assert any("[plan] no schema: filtering inputs by the iterated keys ['pass']"
               in r.getMessage() for r in caplog.records)
