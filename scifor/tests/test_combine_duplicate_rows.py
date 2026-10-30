"""scifor.combine_duplicate_rows — the one owner of "combine rows that share
key columns" (``on_duplicate="combine"``).

Plan: ``.claude/plan-combine-duplicate-locations.md``.
"""

import math

import numpy as np
import pandas as pd
import pytest

from scifor import CombineMixedTypesError, combine_duplicate_rows


def _hospital():
    return pd.DataFrame(
        {
            "subject": ["S01", "S01", "S02"],
            "stay_days": [5, 12, 7],
            "age": [60, 60, 45],
            "ward": ["A", "B", "A"],
        }
    )


def test_unique_keys_table_unchanged():
    df = pd.DataFrame({"subject": ["S01", "S02"], "x": [1.0, 2.0]})
    out, report = combine_duplicate_rows(df, ["subject"])
    assert out is df
    assert report.combined == [] and report.list_columns == []


def test_identical_rows_collapse_to_one():
    df = pd.DataFrame({"subject": ["S01", "S01"], "x": [1.0, 1.0]})
    out, report = combine_duplicate_rows(df, ["subject"])
    assert len(out) == 1
    assert out["x"].iloc[0] == 1.0
    assert report.list_columns == [] and report.collapsed_columns == ["x"]


def test_differing_column_lists_equal_column_collapses():
    out, report = combine_duplicate_rows(_hospital(), ["subject"])
    assert list(out["subject"]) == ["S01", "S02"]
    assert out["stay_days"].iloc[0] == [5, 12]
    assert out["ward"].iloc[0] == ["A", "B"]
    assert out["age"].iloc[0] == 60
    # other locations keep their single value
    assert out["stay_days"].iloc[1] == 7
    assert out["ward"].iloc[1] == "A"
    assert report.list_columns == ["stay_days", "ward"]
    assert report.collapsed_columns == ["age"]
    assert report.combined[0]["key"] == {"subject": "S01"}
    assert report.combined[0]["rows"] == 2


def test_collapsed_column_keeps_dtype():
    out, _ = combine_duplicate_rows(_hospital(), ["subject"])
    assert out["age"].dtype == np.int64


def test_three_rows_keep_full_list_for_alignment():
    df = pd.DataFrame(
        {"subject": ["S01"] * 3, "age": [60, 60, 61], "stay": [1, 2, 3]}
    )
    out, _ = combine_duplicate_rows(df, ["subject"])
    assert out["age"].iloc[0] == [60, 60, 61]
    assert out["stay"].iloc[0] == [1, 2, 3]


def test_nan_equals_nan():
    df = pd.DataFrame({"subject": ["S01", "S01"], "x": [np.nan, np.nan]})
    out, _ = combine_duplicate_rows(df, ["subject"])
    assert len(out) == 1 and math.isnan(out["x"].iloc[0])


def test_value_and_nan_is_a_list():
    df = pd.DataFrame({"subject": ["S01", "S01"], "x": [5.0, np.nan]})
    out, _ = combine_duplicate_rows(df, ["subject"])
    cell = out["x"].iloc[0]
    assert cell[0] == 5.0 and math.isnan(cell[1])


def test_null_keys_group_together():
    df = pd.DataFrame({"subject": [None, None, "S02"], "x": [1.0, 2.0, 3.0]})
    out, _ = combine_duplicate_rows(df, ["subject"])
    assert len(out) == 2
    assert out["x"].iloc[0] == [1.0, 2.0]


def test_variant_key_columns_keep_rows_apart():
    df = pd.DataFrame(
        {"subject": ["S01", "S01"], "__rid_x": ["a", "b"], "x": [1.0, 2.0]}
    )
    out, report = combine_duplicate_rows(df, ["subject", "__rid_x"])
    assert out is df and report.combined == []


def test_unhashable_key_cells():
    df = pd.DataFrame(
        {"subject": ["S01", "S01"], "bp": [{"a": 1}, {"a": 1}], "x": [1.0, 2.0]}
    )
    out, _ = combine_duplicate_rows(df, ["subject", "bp"])
    assert len(out) == 1 and out["x"].iloc[0] == [1.0, 2.0]


def test_non_scalar_cell_leaves_group_uncombined():
    df = pd.DataFrame(
        {
            "subject": ["S01", "S01", "S02"],
            "sig": [np.array([1.0, 2.0]), np.array([3.0]), np.array([4.0])],
        }
    )
    out, report = combine_duplicate_rows(df, ["subject"])
    assert len(out) == 3
    assert report.uncombinable[0]["key"] == {"subject": "S01"}
    assert report.uncombinable[0]["column"] == "sig"


def test_mixed_kinds_refused():
    df = pd.DataFrame({"subject": ["S01", "S01"], "x": [5, "x"]})
    with pytest.raises(CombineMixedTypesError) as info:
        combine_duplicate_rows(df, ["subject"])
    assert info.value.problems[0]["column"] == "x"
    assert info.value.remedy["kind"] == "combine_mixed_types"


def test_mixed_kinds_across_locations_refused():
    """The list column's single values elsewhere share its type."""
    df = pd.DataFrame({"subject": ["S01", "S01", "S02"], "x": [5, 6, "x"]})
    with pytest.raises(CombineMixedTypesError):
        combine_duplicate_rows(df, ["subject"])


def test_int_and_float_are_one_kind():
    df = pd.DataFrame({"subject": ["S01", "S01"], "x": pd.Series([5, 5.5], dtype=object)})
    out, _ = combine_duplicate_rows(df, ["subject"])
    assert out["x"].iloc[0] == [5, 5.5]


def test_one_and_true_do_not_collapse():
    df = pd.DataFrame({"subject": ["S01", "S01"], "x": pd.Series([1, True], dtype=object)})
    with pytest.raises(CombineMixedTypesError):
        combine_duplicate_rows(df, ["subject"])


def test_datetimes_needing_a_list_refused():
    df = pd.DataFrame(
        {
            "subject": ["S01", "S01"],
            "admitted": pd.to_datetime(["2026-01-01", "2026-02-01"]),
        }
    )
    with pytest.raises(CombineMixedTypesError, match="dates/times"):
        combine_duplicate_rows(df, ["subject"])


def test_equal_datetimes_collapse():
    df = pd.DataFrame(
        {
            "subject": ["S01", "S01"],
            "born": pd.to_datetime(["1960-01-01", "1960-01-01"]),
            "x": [1.0, 2.0],
        }
    )
    out, _ = combine_duplicate_rows(df, ["subject"])
    assert out["born"].iloc[0] == pd.Timestamp("1960-01-01")


def test_column_neither_key_nor_value_keeps_rows_apart():
    """Never drop data: a differing column outside value_columns is a key."""
    df = pd.DataFrame(
        {"subject": ["S01", "S01"], "note": ["a", "b"], "x": [1.0, 2.0]}
    )
    out, report = combine_duplicate_rows(df, ["subject"], value_columns=["x"])
    assert len(out) == 2 and report.combined == []


def test_explicit_value_columns_combine():
    df = pd.DataFrame(
        {"subject": ["S01", "S01"], "note": ["a", "a"], "x": [1.0, 2.0]}
    )
    out, report = combine_duplicate_rows(df, ["subject"], value_columns=["x"])
    assert out["note"].iloc[0] == "a"
    assert out["x"].iloc[0] == [1.0, 2.0]
    assert report.list_columns == ["x"]


def test_output_row_maps_every_input_row():
    df = pd.DataFrame(
        {"subject": ["S01", "S02", "S01", "S03"], "x": [1.0, 2.0, 3.0, 4.0]}
    )
    out, report = combine_duplicate_rows(df, ["subject"])
    assert list(out["subject"]) == ["S01", "S02", "S03"]
    assert report.output_row == [0, 1, 0, 2]


def test_output_row_identity_when_unchanged():
    df = pd.DataFrame({"subject": ["S01", "S02"], "x": [1.0, 2.0]})
    _, report = combine_duplicate_rows(df, ["subject"])
    assert report.output_row == [0, 1]
