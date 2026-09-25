"""Tests for scifor.for_each in standalone (no-DB) mode."""

import logging

import numpy as np
import pandas as pd
import pytest

import scifor
from scifor import (
    Col,
    ColName,
    ColumnSelection,
    Fixed,
    Merge,
    PathOutput,
    for_each,
    set_schema,
)


def setup_function():
    # Reset schema before each test
    set_schema([])


# ---------------------------------------------------------------------------
# Helper fixtures
# ---------------------------------------------------------------------------


def make_df(subjects=(1, 2), sessions=("pre", "post"), data_col="emg"):
    rows = []
    for s in subjects:
        for sess in sessions:
            rows.append({"subject": s, "session": sess, data_col: float(s) + 0.1})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# DataFrame detection
# ---------------------------------------------------------------------------


def test_per_combo_df_detected():
    """DataFrame with schema key columns is treated as per-combo."""
    set_schema(["subject", "session"])
    df = make_df()
    results = for_each(
        lambda emg: emg,
        inputs={"emg": df},
        subject=[1, 2],
        session=["pre", "post"],
    )
    assert len(results) == 4


def test_constant_df_passed_unchanged():
    """DataFrame without schema key columns is passed unchanged on every iteration."""
    set_schema(["subject", "session"])
    coeffs = pd.DataFrame({"freq_low": [10], "freq_high": [100]})
    received = []

    def fn(coeffs_input):
        received.append(coeffs_input)
        return 0

    for_each(
        fn,
        inputs={"coeffs_input": coeffs},
        subject=[1, 2],
        session=["pre"],
    )
    assert len(received) == 2
    for r in received:
        pd.testing.assert_frame_equal(r, coeffs)


def test_per_combo_df_single_value_extracted():
    """1 row, 1 data column -> scalar extracted."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "value": [10.0, 20.0]})
    received = []

    def fn(x):
        received.append(x)
        return x

    for_each(fn, inputs={"x": df}, subject=[1, 2])
    assert received == [10.0, 20.0]


def test_per_combo_df_multiple_rows_passed_as_df():
    """Multiple matching rows -> numpy column vector passed."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "emg": [1.0, 2.0, 3.0, 4.0],
        }
    )
    received_shapes = []
    received_types = []

    def fn(data):
        received_shapes.append(data.shape)
        received_types.append(type(data))
        return 0

    for_each(fn, inputs={"data": df}, subject=[1, 2])
    # After dropping schema col "subject", we get a vector of 2 emg values
    assert received_shapes == [(2, 1), (2, 1)]
    assert received_types == [np.ndarray, np.ndarray]


def test_as_table_forces_dataframe():
    """as_table=True keeps DataFrame even for 1-row/1-col result."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "value": [10.0, 20.0]})
    received = []

    def fn(x):
        received.append(x)
        return 0

    for_each(fn, inputs={"x": df}, subject=[1, 2], as_table=True)
    assert all(isinstance(r, pd.DataFrame) for r in received)


# ---------------------------------------------------------------------------
# Fixed(DataFrame, ...)
# ---------------------------------------------------------------------------


def test_fixed_dataframe():
    """Fixed(df, session='pre') filters with overridden metadata."""
    set_schema(["subject", "session"])
    df = make_df()
    received = []

    def fn(baseline, current):
        received.append((baseline, current))
        return 0

    for_each(
        fn,
        inputs={
            "baseline": Fixed(df, session="pre"),
            "current": df,
        },
        subject=[1],
        session=["pre", "post"],
    )
    # both iterations: baseline always has session=pre
    assert len(received) == 2
    for baseline, _current in received:
        # baseline is a scalar (1 row, 1 data col)
        assert baseline == pytest.approx(1.1)


# ---------------------------------------------------------------------------
# [] standalone resolution
# ---------------------------------------------------------------------------


def test_empty_list_resolved_from_df():
    """[] resolved by scanning DataFrame inputs for distinct values."""
    set_schema(["subject", "session"])
    df = make_df(subjects=(1, 2, 3), sessions=("pre",))
    results = for_each(
        lambda emg: emg,
        inputs={"emg": df},
        subject=[],
        session=["pre"],
    )
    assert len(results) == 3


def test_empty_list_no_df_raises():
    """[] without a DataFrame input raises a clear error."""
    set_schema(["subject"])
    with pytest.raises(ValueError, match="no input DataFrame"):
        for_each(
            lambda: None,
            inputs={},
            subject=[],
        )


# ---------------------------------------------------------------------------
# Return DataFrame
# ---------------------------------------------------------------------------


def test_return_df_metadata_columns():
    """Result DataFrame has metadata columns."""
    set_schema(["subject", "session"])
    df = make_df()
    result = for_each(
        lambda emg: emg * 2,
        inputs={"emg": df},
        subject=[1],
        session=["pre", "post"],
    )
    assert "subject" in result.columns
    assert "session" in result.columns


def test_return_df_uses_output_names():
    """Result DataFrame uses output_names for output columns."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "value": [10.0, 20.0]})
    result = for_each(
        lambda x: x * 2,
        inputs={"x": df},
        output_names=["doubled_value"],
        subject=[1, 2],
    )
    assert "doubled_value" in result.columns
    assert list(result["doubled_value"]) == [20.0, 40.0]


def test_return_df_multiple_outputs():
    """Multiple outputs with output_names."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "value": [10.0, 20.0]})
    result = for_each(
        lambda x: (x * 2, x * 3),
        inputs={"x": df},
        output_names=["doubled", "tripled"],
        subject=[1, 2],
    )
    assert "doubled" in result.columns
    assert "tripled" in result.columns


def test_return_df_auto_output_names():
    """output_names=3 auto-generates output_1, output_2, output_3."""
    set_schema(["subject"])
    result = for_each(
        lambda: (1, 2, 3),
        inputs={},
        output_names=3,
        subject=[1],
    )
    assert "output_1" in result.columns
    assert "output_2" in result.columns
    assert "output_3" in result.columns


# ---------------------------------------------------------------------------
# distribute=True
# ---------------------------------------------------------------------------


def test_distribute_requires_schema():
    """distribute=True with no schema raises ValueError."""
    with pytest.raises(ValueError, match="set_schema"):
        for_each(
            lambda: [1, 2, 3],
            inputs={},
            distribute=True,
            subject=[1],
        )


def test_distribute_with_no_iterated_key_defaults_to_top_of_schema():
    """distribute=True with no metadata_iterable matching a schema key
    distributes to the top of the schema instead of raising — e.g. a
    fully static PathInput with no {key} placeholders leaves nothing to
    iterate, but the schema itself still tells us where to expand."""
    set_schema(["pass", "cycle"])

    def fn():
        return np.array([1.0, 2.0])

    result = for_each(
        fn,
        inputs={},
        distribute=True,
    )
    assert len(result) == 2
    assert sorted(result["pass"]) == [1, 2]
    assert "cycle" not in result.columns


def test_distribute_splits_into_result_table():
    """distribute=True splits output and expands result table rows."""
    set_schema(["subject", "trial"])

    def fn():
        return np.array([10.0, 20.0, 30.0])

    result = for_each(
        fn,
        inputs={},
        distribute=True,
        subject=[1],
    )
    # 3 pieces with trial=1,2,3
    assert len(result) == 3
    trials = list(result["trial"])
    assert sorted(trials) == [1, 2, 3]


# ---------------------------------------------------------------------------
# dry_run
# ---------------------------------------------------------------------------


def test_dry_run_returns_none(capsys):
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "value": [1.0, 2.0]})
    result = for_each(
        lambda x: x,
        inputs={"x": df},
        subject=[1, 2],
        dry_run=True,
    )
    assert result is None
    captured = capsys.readouterr()
    assert "[dry-run]" in captured.out


# ---------------------------------------------------------------------------
# Constant inputs
# ---------------------------------------------------------------------------


def test_constant_scalar_input():
    set_schema(["subject"])
    received = []

    def fn(x, alpha):
        received.append(alpha)
        return x

    for_each(
        fn,
        inputs={"x": 1.0, "alpha": 0.5},
        subject=[1, 2],
    )
    assert received == [0.5, 0.5]


# ---------------------------------------------------------------------------
# where= with Col filters
# ---------------------------------------------------------------------------


def test_where_col_filter():
    """where= filters DataFrame rows after combo filtering."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 1],
            "speed": [0.5, 1.5, 2.5],
            "value": [10.0, 20.0, 30.0],
        }
    )
    received = []

    def fn(data):
        received.append(data)
        return 0

    for_each(
        fn,
        inputs={"data": df},
        where=Col("speed") > 1.0,
        as_table=True,
        subject=[1],
    )
    assert len(received) == 1
    assert len(received[0]) == 2  # Only speed > 1.0 rows


# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------


def test_merge_two_dataframes():
    """Merge combines two DataFrames column-wise per combo."""
    set_schema(["subject"])
    df1 = pd.DataFrame({"subject": [1, 2], "force": [10.0, 20.0]})
    df2 = pd.DataFrame({"subject": [1, 2], "emg": [0.1, 0.2]})
    received = []

    def fn(combined):
        received.append(combined)
        return 0

    for_each(
        fn,
        inputs={"combined": Merge(df1, df2)},
        subject=[1, 2],
    )
    assert len(received) == 2
    # Each merged result should have both data columns
    assert "force" in received[0].columns
    assert "emg" in received[0].columns


# ---------------------------------------------------------------------------
# ColumnSelection
# ---------------------------------------------------------------------------


def test_column_selection_single():
    """ColumnSelection extracts a single column as array."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "speed": [1.5, 2.5], "force": [10.0, 20.0]})
    received = []

    def fn(speed):
        received.append(speed)
        return 0

    for_each(
        fn,
        inputs={"speed": ColumnSelection(df, ["speed"])},
        subject=[1, 2],
    )
    assert len(received) == 2
    np.testing.assert_array_equal(received[0], np.array([1.5]))
    np.testing.assert_array_equal(received[1], np.array([2.5]))


def test_column_selection_multiple():
    """ColumnSelection with multiple columns returns sub-DataFrame."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {"subject": [1, 2], "a": [1.0, 2.0], "b": [3.0, 4.0], "c": [5.0, 6.0]}
    )
    received = []

    def fn(data):
        received.append(data)
        return 0

    for_each(
        fn,
        inputs={"data": ColumnSelection(df, ["a", "b"])},
        subject=[1, 2],
    )
    assert len(received) == 2
    assert isinstance(received[0], pd.DataFrame)
    assert list(received[0].columns) == ["a", "b"]


def test_as_table_with_single_column_selection():
    """as_table=True + single ColumnSelection returns DataFrame with schema cols."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "trial": [1, 2, 1, 2],
            "signal": [10.0, 20.0, 30.0, 40.0],
            "noise": [0.1, 0.2, 0.3, 0.4],
        }
    )
    received = []

    def fn(data):
        received.append(data)
        return 0

    for_each(
        fn,
        inputs={"data": ColumnSelection(df, ["signal"])},
        as_table=True,
        subject=[1, 2],
    )
    assert len(received) == 2
    # Must be DataFrames, not arrays
    for r in received:
        assert isinstance(r, pd.DataFrame), f"Expected DataFrame, got {type(r)}"
    # Must have schema column + selected data column
    assert "subject" in received[0].columns
    assert "signal" in received[0].columns
    # Must NOT have unselected columns
    assert "noise" not in received[0].columns
    assert "trial" not in received[0].columns
    # Verify data values
    np.testing.assert_array_equal(received[0]["signal"].values, [10.0, 20.0])
    np.testing.assert_array_equal(received[1]["signal"].values, [30.0, 40.0])


def test_as_table_with_multi_column_selection():
    """as_table=True + multi ColumnSelection returns DataFrame with schema cols + selected cols."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "a": [1.0, 2.0, 3.0, 4.0],
            "b": [10.0, 20.0, 30.0, 40.0],
            "c": [100.0, 200.0, 300.0, 400.0],
        }
    )
    received = []

    def fn(data):
        received.append(data)
        return 0

    for_each(
        fn,
        inputs={"data": ColumnSelection(df, ["a", "b"])},
        as_table=True,
        subject=[1, 2],
    )
    assert len(received) == 2
    for r in received:
        assert isinstance(r, pd.DataFrame)
    # Must have schema col + selected cols
    assert "subject" in received[0].columns
    assert "a" in received[0].columns
    assert "b" in received[0].columns
    # Must NOT have unselected col
    assert "c" not in received[0].columns
    # Verify values
    np.testing.assert_array_equal(received[0]["a"].values, [1.0, 2.0])
    np.testing.assert_array_equal(received[1]["b"].values, [30.0, 40.0])


def test_as_table_false_with_column_selection_returns_array():
    """as_table=False (default) + single ColumnSelection returns array, not DataFrame."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 2],
            "signal": [10.0, 20.0],
            "noise": [0.1, 0.2],
        }
    )
    received = []

    def fn(data):
        received.append(data)
        return 0

    for_each(
        fn,
        inputs={"data": ColumnSelection(df, ["signal"])},
        subject=[1, 2],
    )
    assert len(received) == 2
    # Without as_table, single column selection returns a numpy array
    for r in received:
        assert isinstance(r, np.ndarray), f"Expected ndarray, got {type(r)}"


# ---------------------------------------------------------------------------
# iterate=True (for_columns) + as_table
# ---------------------------------------------------------------------------


def test_iterate_default_passes_bare_arrays():
    """iterate=True without as_table feeds each column as a bare numpy array."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "a": [1.0, 2.0, 3.0, 4.0],
            "b": [10.0, 20.0, 30.0, 40.0],
        }
    )
    received = []

    def fn(v):
        received.append(v)
        return float(np.max(v))

    result = for_each(
        fn,
        inputs={"v": ColumnSelection(df, ["a", "b"], iterate=True)},
        subject=[1, 2],
    )
    # Each per-column call gets a bare numpy array
    for r in received:
        assert isinstance(r, np.ndarray), f"Expected ndarray, got {type(r)}"
    # Reassembled 1xN per combo: max of a/b per subject
    assert list(result["a"]) == [2.0, 4.0]
    assert list(result["b"]) == [20.0, 40.0]


def test_iterate_as_table_passes_dataframe_with_schema_cols():
    """iterate=True + as_table feeds a DataFrame with all schema cols + the one current column."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "a": [1.0, 2.0, 3.0, 4.0],
            "b": [10.0, 20.0, 30.0, 40.0],
            "noise": [0.1, 0.2, 0.3, 0.4],
        }
    )
    received = []

    def fn(v):
        received.append(v)
        # v is a DataFrame; the single non-schema column is the current one
        data_col = [c for c in v.columns if c != "subject"][0]
        return float(v[data_col].max())

    result = for_each(
        fn,
        inputs={"v": ColumnSelection(df, ["a", "b"], iterate=True)},
        as_table=True,
        subject=[1, 2],
    )
    # Each per-column call gets a DataFrame
    for r in received:
        assert isinstance(r, pd.DataFrame), f"Expected DataFrame, got {type(r)}"
        # schema column present
        assert "subject" in r.columns
        # exactly one non-schema column (the current iterated column)
        non_schema = [c for c in r.columns if c != "subject"]
        assert len(non_schema) == 1
        assert non_schema[0] in ("a", "b")
        # non-selected data columns dropped
        assert "noise" not in r.columns
    # Reassembled output matches the bare-array case
    assert list(result["a"]) == [2.0, 4.0]
    assert list(result["b"]) == [20.0, 40.0]


def test_iterate_as_table_enables_argmax_label_lookup():
    """With a label column declared as a schema key, as_table iterate can map argmax to that label."""
    # intervention is a schema key but is NOT iterated (only subject is)
    set_schema(["subject", "intervention"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 1, 2, 2, 2],
            "intervention": [1, 2, 3, 1, 2, 3],
            "StepLength": [0.5, 0.9, 0.2, 0.1, 0.4, 0.8],
            "Cadence": [10.0, 5.0, 8.0, 7.0, 9.0, 3.0],
        }
    )
    received_cols = []

    def best_intervention(v):
        data_col = [c for c in v.columns if c not in ("subject", "intervention")][0]
        received_cols.append(data_col)
        return v.loc[v[data_col].idxmax(), "intervention"]

    result = for_each(
        best_intervention,
        inputs={"v": ColumnSelection(df, ["StepLength", "Cadence"], iterate=True)},
        as_table=True,
        subject=[1, 2],
    )
    # subject 1: StepLength best at intervention 2; Cadence best at intervention 1
    # subject 2: StepLength best at intervention 3; Cadence best at intervention 2
    assert list(result["StepLength"]) == [2, 3]
    assert list(result["Cadence"]) == [1, 2]


# ---------------------------------------------------------------------------
# iterate=True (for_columns) multi-output-per-column reassembly
# ---------------------------------------------------------------------------


def test_iterate_dict_return_expands_to_suffixed_columns():
    """A dict return per column expands to <col>__<key> columns."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "a": [1.0, 2.0, 3.0, 4.0],
            "b": [10.0, 20.0, 30.0, 40.0],
        }
    )

    def stats(v):
        return {"min": float(np.min(v)), "max": float(np.max(v))}

    result = for_each(
        stats,
        inputs={"v": ColumnSelection(df, ["a", "b"], iterate=True)},
        subject=[1, 2],
    )
    # Column order: a's keys, then b's keys
    assert list(result.columns) == ["subject", "a__min", "a__max", "b__min", "b__max"]
    # subject 1: a=[1,2], b=[10,20]; subject 2: a=[3,4], b=[30,40]
    assert list(result["a__min"]) == [1.0, 3.0]
    assert list(result["a__max"]) == [2.0, 4.0]
    assert list(result["b__min"]) == [10.0, 30.0]
    assert list(result["b__max"]) == [20.0, 40.0]


def test_iterate_varying_output_counts_per_column():
    """Different source columns may return different numbers/names of outputs."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "a": [1.0, 2.0, 3.0, 4.0],
            "b": [10.0, 20.0, 30.0, 40.0],
        }
    )

    def stats(v):
        # 'a' gets just max; 'b' gets max + min — keyed off magnitude
        if np.max(v) < 5:
            return {"max": float(np.max(v))}
        return {"max": float(np.max(v)), "min": float(np.min(v))}

    result = for_each(
        stats,
        inputs={"v": ColumnSelection(df, ["a", "b"], iterate=True)},
        subject=[1, 2],
    )
    # a -> one column; b -> two columns
    assert list(result.columns) == ["subject", "a__max", "b__max", "b__min"]
    assert list(result["a__max"]) == [2.0, 4.0]
    assert list(result["b__max"]) == [20.0, 40.0]
    assert list(result["b__min"]) == [10.0, 30.0]


def test_iterate_series_return_expands_like_dict():
    """A pandas Series return expands by index label."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 1, 2, 2], "a": [1.0, 2.0, 3.0, 4.0]})

    def stats(v):
        return pd.Series({"sum": float(np.sum(v)), "n": float(len(v))})

    result = for_each(
        stats,
        inputs={"v": ColumnSelection(df, ["a"], iterate=True)},
        subject=[1, 2],
    )
    assert list(result.columns) == ["subject", "a__sum", "a__n"]
    assert list(result["a__sum"]) == [3.0, 7.0]
    assert list(result["a__n"]) == [2.0, 2.0]


def test_iterate_scalar_return_still_unsuffixed():
    """Back-compat: a scalar return keeps the source column name (no suffix)."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "a": [5.0, 6.0], "b": [7.0, 8.0]})

    result = for_each(
        lambda v: float(np.max(v)),
        inputs={"v": ColumnSelection(df, ["a", "b"], iterate=True)},
        subject=[1, 2],
    )
    assert list(result.columns) == ["subject", "a", "b"]
    assert list(result["a"]) == [5.0, 6.0]


def test_iterate_multi_output_with_as_table_value_and_label():
    """The motivating case: per column, return both the max value and the
    identity of the best label (a non-iterated schema key)."""
    set_schema(["subject", "intervention"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 1, 2, 2, 2],
            "intervention": [1, 2, 3, 1, 2, 3],
            "StepLength": [0.5, 0.9, 0.2, 0.1, 0.4, 0.8],
            "Cadence": [10.0, 5.0, 8.0, 7.0, 9.0, 3.0],
        }
    )

    def best(v):
        col = [c for c in v.columns if c not in ("subject", "intervention")][0]
        idx = v[col].idxmax()
        return {
            "value": float(v.loc[idx, col]),
            "best": int(v.loc[idx, "intervention"]),
        }

    result = for_each(
        best,
        inputs={"v": ColumnSelection(df, ["StepLength", "Cadence"], iterate=True)},
        as_table=True,
        subject=[1, 2],
    )
    assert list(result["StepLength__value"]) == [0.9, 0.8]
    assert list(result["StepLength__best"]) == [2, 3]
    assert list(result["Cadence__value"]) == [10.0, 9.0]
    assert list(result["Cadence__best"]) == [1, 2]


def test_iterate_duplicate_output_column_raises():
    """Colliding produced output names raise a clear error.

    Column 'a' returning key 'b' -> 'a__b'; column 'a__b' returning a scalar
    -> 'a__b'. The two collide.
    """
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 1], "a": [1.0, 2.0], "a__b": [3.0, 4.0]})

    def stats(v):
        col = [c for c in v.columns if c != "subject"][0]
        if col == "a":
            return {"b": float(np.max(v["a"]))}  # -> "a__b"
        return float(np.max(v["a__b"]))  # scalar -> "a__b" (collision)

    with pytest.raises(ValueError, match="duplicate output column"):
        for_each(
            stats,
            inputs={"v": ColumnSelection(df, ["a", "a__b"], iterate=True)},
            as_table=True,
            subject=[1],
        )


def test_iterate_multirow_dataframe_return_raises():
    """A multi-row DataFrame return is rejected (must collapse to one row)."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 1], "a": [1.0, 2.0]})

    def bad(v):
        return pd.DataFrame({"x": [1.0, 2.0]})

    with pytest.raises(ValueError, match="row DataFrame"):
        for_each(
            bad,
            inputs={"v": ColumnSelection(df, ["a"], iterate=True)},
            subject=[1],
        )


def test_iterate_function_failure_lists_all_bad_columns(capsys):
    """A function that fails on some columns raises ColumnFunctionError naming
    EVERY offending column (not just the first), and logs it to stderr."""
    set_schema(["subject"])
    # Two numeric columns the function can handle; two string columns it can't.
    df = pd.DataFrame(
        {
            "subject": [1, 1],
            "good1": [1.0, 2.0],
            "label1": ["x", "y"],
            "good2": [3.0, 4.0],
            "label2": ["p", "q"],
        }
    )

    with pytest.raises(scifor.ColumnFunctionError) as excinfo:
        for_each(
            lambda v: float(np.mean(np.asarray(v, dtype=float))),
            inputs={
                "v": ColumnSelection(
                    df, ["good1", "label1", "good2", "label2"], iterate=True
                )
            },
            subject=[1],
        )

    err = excinfo.value
    # Both bad columns are reported; neither good column is.
    failed_cols = {col for col, _ in err.failures}
    assert failed_cols == {"label1", "label2"}
    msg = str(err)
    assert "label1" in msg and "label2" in msg
    assert "good1" not in msg and "good2" not in msg
    # The full error is echoed to stderr for visibility.
    assert "label1" in capsys.readouterr().err


def test_iterate_function_failure_propagates_not_skipped():
    """Per-column failures are a hard error, not a swallowed [skip] that yields
    an empty result."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 1], "label": ["x", "y"]})

    with pytest.raises(scifor.ColumnFunctionError):
        for_each(
            lambda v: float(np.mean(np.asarray(v, dtype=float))),
            inputs={"v": ColumnSelection(df, ["label"], iterate=True)},
            subject=[1],
        )


# ---------------------------------------------------------------------------
# iterate=True (for_columns) all-columns resolution (empty [] = all)
# ---------------------------------------------------------------------------


def test_iterate_all_columns_resolves_from_dataframe():
    """for_columns with no/empty columns iterates over all non-schema columns."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "a": [1.0, 2.0, 3.0, 4.0],
            "b": [10.0, 20.0, 30.0, 40.0],
        }
    )
    result = for_each(
        lambda v: float(np.max(v)),
        inputs={"v": ColumnSelection(df, iterate=True)},  # no columns -> all
        subject=[1, 2],
    )
    # Schema key 'subject' excluded; iterates a, b
    assert list(result.columns) == ["subject", "a", "b"]
    assert list(result["a"]) == [2.0, 4.0]
    assert list(result["b"]) == [20.0, 40.0]


def test_iterate_empty_list_equivalent_to_all():
    """An explicit empty list [] is identical to the no-arg all-columns case."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "a": [5.0, 6.0], "b": [7.0, 8.0]})
    result = for_each(
        lambda v: float(np.max(v)),
        inputs={"v": ColumnSelection(df, [], iterate=True)},
        subject=[1, 2],
    )
    assert list(result.columns) == ["subject", "a", "b"]


def test_iterate_none_alias_for_all():
    """None is accepted as a backward-compatible alias for the [] sentinel."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "a": [5.0, 6.0]})
    result = for_each(
        lambda v: float(np.max(v)),
        inputs={"v": ColumnSelection(df, None, iterate=True)},
        subject=[1, 2],
    )
    assert list(result.columns) == ["subject", "a"]


def test_noniterate_all_columns_passes_all_data_cols():
    """Non-iterate ColumnSelection with empty columns passes all data columns
    (schema keys excluded) as a sub-DataFrame."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "a": [1.0, 2.0, 3.0, 4.0],
            "b": [10.0, 20.0, 30.0, 40.0],
        }
    )
    received = []

    def fn(data):
        received.append(data)
        return 0

    for_each(fn, inputs={"data": ColumnSelection(df)}, subject=[1, 2])
    for r in received:
        assert isinstance(r, pd.DataFrame)
        assert list(r.columns) == ["a", "b"]  # schema col excluded


# ---------------------------------------------------------------------------
# excl_columns — drop named columns from the resolved selection
# ---------------------------------------------------------------------------


def test_iterate_excl_columns_drops_from_all_columns():
    """excl_columns removes columns from the all-columns expansion: they are not
    iterated and are absent from the aggregated result."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "a": [1.0, 2.0, 3.0, 4.0],
            "label": ["x", "x", "y", "y"],  # non-numeric, to be excluded
            "b": [10.0, 20.0, 30.0, 40.0],
        }
    )
    result = for_each(
        lambda v: float(np.max(v)),
        inputs={"v": ColumnSelection(df, iterate=True, excl_columns=["label"])},
        subject=[1, 2],
    )
    # 'label' neither iterated nor present in the output.
    assert list(result.columns) == ["subject", "a", "b"]
    assert "label" not in result.columns


def test_iterate_excl_columns_drops_from_explicit_list():
    """excl_columns also subtracts from an explicit columns list."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 2],
            "a": [1.0, 2.0],
            "b": [3.0, 4.0],
            "c": [5.0, 6.0],
        }
    )
    result = for_each(
        lambda v: float(np.max(v)),
        inputs={
            "v": ColumnSelection(df, ["a", "b", "c"], iterate=True, excl_columns=["b"])
        },
        subject=[1, 2],
    )
    assert list(result.columns) == ["subject", "a", "c"]


def test_iterate_excl_columns_lets_run_succeed_after_failure():
    """The workflow from ColumnFunctionError: excluding the reported non-numeric
    columns makes the same run complete."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "good": [1.0, 2.0, 3.0, 4.0],
            "label1": ["x", "x", "y", "y"],
            "label2": ["p", "p", "q", "q"],
        }
    )

    def fn(v):
        return float(np.mean(np.asarray(v, dtype=float)))

    # Without exclusion: hard error naming both bad columns.
    with pytest.raises(scifor.ColumnFunctionError) as excinfo:
        for_each(fn, inputs={"v": ColumnSelection(df, iterate=True)}, subject=[1, 2])
    assert {c for c, _ in excinfo.value.failures} == {"label1", "label2"}

    # Excluding them: the run completes over the remaining numeric column.
    result = for_each(
        fn,
        inputs={
            "v": ColumnSelection(df, iterate=True, excl_columns=["label1", "label2"])
        },
        subject=[1, 2],
    )
    assert list(result.columns) == ["subject", "good"]


def test_noniterate_excl_columns_drops_data_column():
    """excl_columns works in non-iterate mode: dropped column is absent from the
    sub-DataFrame passed to the function. (Two data columns remain so the
    selection stays a DataFrame rather than collapsing to a 1-col array.)"""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "a": [1.0, 2.0, 3.0, 4.0],
            "b": [10.0, 20.0, 30.0, 40.0],
            "c": [100.0, 200.0, 300.0, 400.0],
        }
    )
    received = []
    for_each(
        lambda data: received.append(data) or 0,
        inputs={"data": ColumnSelection(df, excl_columns=["b"])},
        subject=[1, 2],
    )
    for r in received:
        assert list(r.columns) == ["a", "c"]  # 'b' excluded, schema key excluded


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_function_error_skips(caplog):
    """Function errors skip the iteration gracefully.

    Per-iteration [skip] lines are DEBUG records on the "scifor" logger
    (not stdout) since the logging redesign.
    """
    import logging

    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "value": [10.0, 20.0]})
    call_count = [0]

    def fn(x):
        call_count[0] += 1
        if call_count[0] == 1:
            raise ValueError("bad")
        return x

    with caplog.at_level(logging.DEBUG, logger="scifor"):
        result = for_each(fn, inputs={"x": df}, subject=[1, 2])
    assert any("[skip]" in r.getMessage() for r in caplog.records)
    assert len(result) == 1  # only subject=2 succeeded


# ---------------------------------------------------------------------------
# Result table structure
# ---------------------------------------------------------------------------


def test_result_table_default_output_name():
    """Default output column is 'output'."""
    set_schema(["subject"])
    result = for_each(
        lambda: 42,
        inputs={},
        subject=[1, 2],
    )
    assert "output" in result.columns
    assert list(result["output"]) == [42, 42]


def test_all_skipped_returns_empty_df():
    """When all iterations fail, result is an empty DataFrame."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "value": [1.0, 2.0]})

    def always_fails(x):
        raise ValueError("always")

    result = for_each(always_fails, inputs={"x": df}, subject=[1, 2])
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


def test_no_data_combo_is_skipped_not_passed_to_fn():
    """A combo with no matching rows never reaches fn -- it's skipped as
    NoDataError before the call, mirroring MATLAB's scifor:NoData."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "value": [1.0, 2.0]})
    received = []

    def fn(value):
        received.append(value)
        return value

    result = for_each(fn, inputs={"value": df}, subject=[1, 2, 3])
    assert len(received) == 2  # subject=3 never called
    assert len(result) == 2


def test_no_data_combo_as_table_passes_empty_table():
    """as_table=True exempts the NoData raise -- an empty table is valid
    output for that combo, so fn is still called (mirrors MATLAB's
    'unless as_table, where empty table is valid')."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "value": [1.0, 2.0]})
    received = []

    def fn(value):
        received.append(value)
        return len(value)

    for_each(fn, inputs={"value": df}, subject=[1, 2, 3], as_table=True)
    assert len(received) == 3  # subject=3 called with an empty DataFrame
    assert len(received[-1]) == 0


def test_no_data_combo_in_iterate_mode_skips_even_with_as_table():
    """for_columns (iterate=True) has no as_table exemption -- a combo with
    no matching rows always raises NoDataError, mirroring MATLAB's
    prepare_iterate_table (which takes no as_table parameter)."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {"subject": [1, 2], "a": [1.0, 2.0], "b": [10.0, 20.0]}
    )
    received = []

    def fn(v):
        received.append(v)
        return float(v[v.columns[-1]].max()) if len(v) else float("nan")

    for_each(
        fn,
        inputs={"v": ColumnSelection(df, ["a", "b"], iterate=True)},
        as_table=True,
        subject=[1, 2, 3],
    )
    # subject=3 never reaches fn for either column
    assert len(received) == 4  # 2 subjects x 2 columns


def test_dataframe_output_without_schema_columns_stays_whole():
    """A returned DataFrame whose columns say nothing about WHERE its rows
    belong is one value per combo, not one row per combo.

    Every row of ``{"val": [10, 20, 30]}`` addresses the same subject, so
    spreading them would file three indistinguishable records at one address
    (the vo2max explosion: one 322-row CSV became 322 records).
    """
    set_schema(["subject"])
    result = for_each(
        lambda: pd.DataFrame({"val": [10.0, 20.0, 30.0]}),
        inputs={},
        subject=[1, 2],
    )
    assert len(result) == 2  # one row per subject
    assert "subject" in result.columns
    assert list(result["subject"]) == [1, 2]
    # The whole table travels in the output column.
    for value in result["output"]:
        assert isinstance(value, pd.DataFrame)
        assert list(value["val"]) == [10.0, 20.0, 30.0]


def test_dataframe_output_with_unpinned_schema_column_spreads():
    """The case the spread is FOR: the returned rows carry a schema key the
    call did not pin, so each row addresses its own location."""
    set_schema(["subject", "session"])
    result = for_each(
        lambda: pd.DataFrame({"session": ["01", "02", "03"], "val": [1.0, 2.0, 3.0]}),
        inputs={},
        subject=[1, 2],
    )
    # 2 subjects x 3 sessions supplied by the data = 6 rows
    assert len(result) == 6
    assert sorted(set(result["session"])) == ["01", "02", "03"]
    assert "val" in result.columns


def test_distribute_composes_with_whole_table_rule():
    """distribute splits BEFORE result collection and stamps each piece with
    its own distribute_key, so the pieces arrive already addressed and the
    rule leaves them alone: one row per piece, as before."""
    set_schema(["subject", "session"])
    result = for_each(
        lambda: pd.DataFrame({"val": [10.0, 20.0, 30.0]}),
        inputs={},
        distribute=True,
        subject=[1],
    )
    # 3 pieces, filed at session=1..3 (one level below the deepest iterated key)
    assert len(result) == 3
    assert sorted(result["session"]) == [1, 2, 3]


def test_distribute_reads_target_key_column_when_present(caplog):
    """A returned DataFrame that carries the distribute target as a column
    already says where each row belongs: its values address the pieces and
    the column is stripped from the data. Row ordinals are NOT used. This is
    what the MATLAB loop has always done (+scifor/for_each.m), so a labelled
    table now files identically in both languages."""
    set_schema(["subject", "session"])
    with caplog.at_level(logging.INFO):
        result = for_each(
            lambda: pd.DataFrame({"session": ["07", "03"], "val": [1.0, 2.0]}),
            inputs={},
            distribute=True,
            subject=[1],
        )
    assert len(result) == 2
    # Values, in the table's own order — not 1, 2.
    assert list(result["session"]) == ["07", "03"]
    assert list(result["val"]) == [1.0, 2.0]
    # Exactly one 'session' column: the data column was removed, so it does
    # not collide with the metadata column of the same name.
    assert list(result.columns).count("session") == 1
    assert "column is removed from the data" in caplog.text
    assert not any("collide" in r.message for r in caplog.records)


def test_distribute_uses_ordinals_when_target_key_column_absent():
    """Without the column, position addresses the pieces (1-based) — the
    original contract for unlabelled outputs."""
    set_schema(["subject", "session"])
    result = for_each(
        lambda: pd.DataFrame({"val": [1.0, 2.0]}),
        inputs={},
        distribute=True,
        subject=[1],
    )
    assert list(result["session"]) == [1, 2]


def test_distribute_column_reading_top_of_schema_nothing_iterated():
    """The 2026-09-15 loadFunctionalOutcomes shape with distribute ON: no
    schema level iterated, so the target is the top of the schema. A
    'subject' column addresses the pieces; a 'session' column is an unpinned
    schema key, so the spread rule files each piece at (subject, session)."""
    set_schema(["subject", "session", "speed"])
    result = for_each(
        lambda: pd.DataFrame(
            {"subject": ["A", "A", "B"], "session": [1, 2, 1], "val": [1.0, 2.0, 3.0]}
        ),
        inputs={},
        distribute=True,
    )
    assert len(result) == 3
    assert list(result["subject"]) == ["A", "A", "B"]
    assert list(result["session"]) == [1, 2, 1]
    assert list(result.columns).count("subject") == 1
    assert "speed" not in result.columns


def test_distribute_at_deepest_key_still_raises():
    """There is no escape hatch below the deepest key, and that guard is what
    makes 'add a schema level' the honest answer rather than distribute."""
    set_schema(["subject", "session"])
    with pytest.raises(ValueError, match="no lower level to distribute to"):
        for_each(
            lambda: pd.DataFrame({"val": [1.0, 2.0]}),
            inputs={},
            distribute=True,
            subject=[1],
            session=["01"],
        )


def test_pinned_schema_column_in_output_warns_and_is_dropped(caplog):
    """A returned schema-key column that the combo ALREADY pins adds no
    address information: the combination's value is the address, and the
    data copy is dropped (with a WARN) rather than stored. Before 2026-09-16
    it survived into the result and, in the one-record-per-combo path, into
    the variable's DuckDB table as a data column named after a schema key."""
    set_schema(["subject"])
    with caplog.at_level(logging.WARNING, logger="scifor"):
        result = for_each(
            lambda: pd.DataFrame({"subject": [99], "val": [1.0]}),
            inputs={},
            subject=[1, 2],
        )
    assert any("pins" in r.message and "dropped" in r.message for r in caplog.records), [
        r.message for r in caplog.records
    ]
    # The combo address, not the data column's 99.
    assert sorted(result["subject"]) == [1, 2]
    assert list(result["val"]) == [1.0, 1.0]


def test_pinned_schema_column_dropped_from_whole_table_record():
    """The one-record-per-combo path (multi-row table, no unpinned key): the
    nested DataFrame is stored WITHOUT the pinned schema-key column. This is
    the shape that produced a `subject` data column in a variable table and
    broke the plot loader (two `subject` columns in one frame)."""
    set_schema(["subject", "session"])
    result = for_each(
        lambda: pd.DataFrame(
            {"subject": [1, 1, 1], "session": ["a", "a", "a"], "val": [1.0, 2.0, 3.0]}
        ),
        inputs={},
        subject=[1],
        session=["a"],
    )
    assert len(result) == 1
    assert result.iloc[0]["subject"] == 1 and result.iloc[0]["session"] == "a"
    nested = result.iloc[0]["output"]
    assert isinstance(nested, pd.DataFrame)
    assert list(nested.columns) == ["val"]
    assert len(nested) == 3


# ---------------------------------------------------------------------------
# ColName resolution
# ---------------------------------------------------------------------------


def test_colname_single_data_column():
    """ColName(df) resolves to the one non-schema data column name."""
    set_schema(["subject", "session"])
    df = make_df()  # has columns: subject, session, emg
    received = []

    def fn(table, col_name):
        received.append(col_name)
        return table[col_name].mean()

    for_each(
        fn,
        inputs={"table": df, "col_name": ColName(df)},
        as_table=True,
        subject=[1],
        session=["pre"],
    )
    assert received[0] == "emg"


def test_colname_multiple_data_columns_errors():
    """ColName raises ValueError when the DataFrame has 2+ data columns."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 2],
            "emg": [0.1, 0.2],
            "force": [1.0, 2.0],
        }
    )
    with pytest.raises(ValueError, match="2 data columns"):
        for_each(
            lambda table, col_name: 0,
            inputs={"table": df, "col_name": ColName(df)},
            subject=[1],
        )


def test_colname_no_data_columns_errors():
    """ColName raises ValueError when all columns are schema keys."""
    set_schema(["subject", "session"])
    df = pd.DataFrame({"subject": [1], "session": ["pre"]})
    with pytest.raises(ValueError, match="no data columns"):
        for_each(
            lambda table, col_name: 0,
            inputs={"table": df, "col_name": ColName(df)},
            subject=[1],
            session=["pre"],
        )


def test_colname_with_other_inputs():
    """ColName works alongside regular table and constant inputs."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 2],
            "velocity": [3.0, 4.0],
        }
    )
    received = []

    def fn(table, col_name, scale):
        received.append((col_name, scale))
        return table[col_name].iloc[0] * scale

    for_each(
        fn,
        inputs={"table": df, "col_name": ColName(df), "scale": 2.0},
        as_table=True,
        subject=[1, 2],
    )
    assert len(received) == 2
    assert received[0] == ("velocity", 2.0)
    assert received[1] == ("velocity", 2.0)


# ---------------------------------------------------------------------------
# Deferred ColName() — resolves to the current for_columns column
# ---------------------------------------------------------------------------


def test_deferred_colname_resolves_to_current_column():
    """No-arg ColName() resolves per-column to the current for_columns column."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "a": [1.0, 2.0, 3.0, 4.0],
            "b": [10.0, 20.0, 30.0, 40.0],
        }
    )
    received = []

    def fn(v, col_name):
        received.append(col_name)
        return float(np.max(v))

    result = for_each(
        fn,
        inputs={
            "v": ColumnSelection(df, ["a", "b"], iterate=True),
            "col_name": ColName(),
        },
        subject=[1, 2],
    )
    # Two combos x two columns; each call sees the name of its current column.
    assert received == ["a", "b", "a", "b"]
    assert list(result["a"]) == [2.0, 4.0]
    assert list(result["b"]) == [20.0, 40.0]


def test_deferred_colname_with_as_table_iterate_input():
    """Deferred ColName() works when the iterate input is fed as_table."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "a": [1.0, 2.0, 3.0, 4.0],
            "b": [10.0, 20.0, 30.0, 40.0],
        }
    )
    received = []

    def fn(v, col_name):
        received.append(col_name)
        # col_name tells the function which column to read without sniffing.
        return float(v[col_name].max())

    result = for_each(
        fn,
        inputs={
            "v": ColumnSelection(df, ["a", "b"], iterate=True),
            "col_name": ColName(),
        },
        as_table=True,
        subject=[1, 2],
    )
    assert received == ["a", "b", "a", "b"]
    assert list(result["a"]) == [2.0, 4.0]
    assert list(result["b"]) == [20.0, 40.0]


def test_deferred_colname_with_two_zipped_iterate_inputs():
    """Deferred ColName() resolves to the shared column axis of zipped inputs."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "a": [1.0, 2.0, 3.0, 4.0],
            "b": [10.0, 20.0, 30.0, 40.0],
        }
    )
    received = []

    def fn(x, y, col_name):
        received.append(col_name)
        return float(np.max(x)) + float(np.max(y))

    result = for_each(
        fn,
        inputs={
            "x": ColumnSelection(df, ["a", "b"], iterate=True),
            "y": ColumnSelection(df, ["a", "b"], iterate=True),
            "col_name": ColName(),
        },
        subject=[1, 2],
    )
    assert received == ["a", "b", "a", "b"]
    assert list(result["a"]) == [4.0, 8.0]
    assert list(result["b"]) == [40.0, 80.0]


def test_deferred_colname_without_iterate_input_raises():
    """No-arg ColName() with no for_columns input is a hard error."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "velocity": [3.0, 4.0]})
    with pytest.raises(ValueError, match="requires at least one iterate input"):
        for_each(
            lambda table, col_name: 0,
            inputs={"table": df, "col_name": ColName()},
            as_table=True,
            subject=[1, 2],
        )


# ---------------------------------------------------------------------------
# Bare ColName class (forgiving, no parentheses) is normalized to ColName()
# ---------------------------------------------------------------------------


def test_bare_colname_class_resolves_like_deferred():
    """Passing the bare ColName class behaves like the deferred ColName()."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "a": [1.0, 2.0, 3.0, 4.0],
            "b": [10.0, 20.0, 30.0, 40.0],
        }
    )
    received = []

    def fn(v, col_name):
        received.append(col_name)
        return float(np.max(v))

    result = for_each(
        fn,
        inputs={
            "v": ColumnSelection(df, ["a", "b"], iterate=True),
            "col_name": ColName,
        },  # bare class, no parentheses
        subject=[1, 2],
    )
    assert received == ["a", "b", "a", "b"]
    assert list(result["a"]) == [2.0, 4.0]
    assert list(result["b"]) == [20.0, 40.0]


def test_bare_colname_class_without_iterate_input_raises_clear_error():
    """Bare ColName class with no for_columns input names the bare-class mistake."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "velocity": [3.0, 4.0]})
    with pytest.raises(ValueError, match="bare ColName class"):
        for_each(
            lambda table, col_name: 0,
            inputs={"table": df, "col_name": ColName},  # bare class, no parentheses
            as_table=True,
            subject=[1, 2],
        )


# ---------------------------------------------------------------------------
# PathOutput — output-path template (combo metadata + {ColName})
# ---------------------------------------------------------------------------


def test_pathoutput_colname_token_resolves_per_column():
    """PathOutput(Path) substitutes {ColName} per column and preserves Path type."""
    from pathlib import Path

    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "a": [1.0, 2.0, 3.0, 4.0],
            "b": [10.0, 20.0, 30.0, 40.0],
        }
    )
    received = []

    def fn(v, filename):
        received.append(filename)
        return float(np.max(v))

    root = Path("/tmp/anova")
    result = for_each(
        fn,
        inputs={
            "v": ColumnSelection(df, ["a", "b"], iterate=True),
            "filename": PathOutput(root / "{ColName}_anova2way.pdf"),
        },
        subject=[1, 2],
    )
    # Two combos x two columns; each call sees its own per-column Path.
    assert received == [
        Path("/tmp/anova/a_anova2way.pdf"),
        Path("/tmp/anova/b_anova2way.pdf"),
        Path("/tmp/anova/a_anova2way.pdf"),
        Path("/tmp/anova/b_anova2way.pdf"),
    ]
    assert all(isinstance(p, Path) for p in received)
    assert list(result["a"]) == [2.0, 4.0]
    assert list(result["b"]) == [20.0, 40.0]


def test_pathoutput_str_template_preserves_str_type():
    """PathOutput(str) substitutes {ColName} and returns a str (not a Path)."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1],
            "a": [1.0, 2.0],
            "b": [10.0, 20.0],
        }
    )
    received = []

    def fn(v, filename):
        received.append(filename)
        return float(np.max(v))

    for_each(
        fn,
        inputs={
            "v": ColumnSelection(df, ["a", "b"], iterate=True),
            "filename": PathOutput("{ColName}_results.json"),
        },
        subject=[1],
    )
    assert received == ["a_results.json", "b_results.json"]
    assert all(isinstance(s, str) for s in received)


def test_pathoutput_substitutes_combo_metadata():
    """PathOutput fills {subject} from the combo metadata (no for_columns needed)."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "velocity": [3.0, 4.0]})
    received = []

    def fn(velocity, filename):
        received.append(filename)
        return float(np.max(velocity))

    for_each(
        fn,
        inputs={
            "velocity": ColumnSelection(df, ["velocity"]),
            "filename": PathOutput("subject_{subject}.pdf"),
        },
        subject=[1, 2],
    )
    assert received == ["subject_1.pdf", "subject_2.pdf"]


def test_pathoutput_combines_metadata_and_column():
    """PathOutput fills both {subject} (combo) and {ColName} (current column)."""
    set_schema(["subject"])
    df = pd.DataFrame(
        {
            "subject": [1, 1, 2, 2],
            "a": [1.0, 2.0, 3.0, 4.0],
            "b": [10.0, 20.0, 30.0, 40.0],
        }
    )
    received = []

    def fn(v, filename):
        received.append(filename)
        return float(np.max(v))

    for_each(
        fn,
        inputs={
            "v": ColumnSelection(df, ["a", "b"], iterate=True),
            "filename": PathOutput("{subject}_{ColName}.pdf"),
        },
        subject=[1, 2],
    )
    assert received == ["1_a.pdf", "1_b.pdf", "2_a.pdf", "2_b.pdf"]


def test_pathoutput_without_tokens_passes_through_unchanged():
    """A template with no recognized tokens is returned verbatim each combo."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "velocity": [3.0, 4.0]})
    received = []

    def fn(velocity, filename):
        received.append(filename)
        return float(np.max(velocity))

    for_each(
        fn,
        inputs={
            "velocity": ColumnSelection(df, ["velocity"]),
            "filename": PathOutput("static_name.pdf"),
        },
        subject=[1, 2],
    )
    assert received == ["static_name.pdf", "static_name.pdf"]


def test_pathoutput_colname_token_without_iterate_input_raises():
    """{ColName} resolves per-column, so it requires an iterate input."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "velocity": [3.0, 4.0]})
    with pytest.raises(ValueError, match="requires at least one iterate input"):
        for_each(
            lambda table, filename: 0,
            inputs={"table": df, "filename": PathOutput("{ColName}.pdf")},
            as_table=True,
            subject=[1, 2],
        )


# ---------------------------------------------------------------------------
# Dtype restore robustness
# ---------------------------------------------------------------------------


def test_restore_schema_column_dtypes_duplicate_labels():
    """Duplicate column labels must not crash the dtype restore.

    Arises when a function returns its input DataFrame with a metadata
    column still inside, so result assembly appends the combo's metadata
    column a second time under the same label (regression: AttributeError
    'DataFrame' object has no attribute 'dtype'). The duplicated label is
    warned about and skipped; other columns still restore.
    """
    from scifor.foreach import _restore_schema_column_dtypes

    df = pd.concat(
        [
            pd.DataFrame({"session": ["A"], "subject": [1]}),
            pd.DataFrame({"session": ["AA"], "value": [1.0]}),
        ],
        axis=1,
    )
    assert list(df.columns) == ["session", "subject", "session", "value"]

    out = _restore_schema_column_dtypes(
        df,
        {
            "session": pd.api.types.pandas_dtype("object"),
            "subject": pd.api.types.pandas_dtype("float64"),
        },
    )

    # No crash, duplicate labels untouched, non-duplicated column restored
    # (int 1 -> float64 is a lossless cast).
    assert list(out.columns) == ["session", "subject", "session", "value"]
    assert out["subject"].dtype == pd.api.types.pandas_dtype("float64")


# ---------------------------------------------------------------------------
# _mapping_inputs: dict-valued records
# ---------------------------------------------------------------------------


def test_mapping_input_single_row_is_delivered_as_a_dict():
    """A dict-valued variable is stored one column per key and loaded spread,
    so a single record is indistinguishable from a one-row table by the time
    it reaches extraction. Marked via _mapping_inputs, it is rebuilt.

    Regression: the 1-row unwrap only fires for a SINGLE data column, and a
    dict has as many columns as keys, so the function received a 1xN frame
    and every key access returned a Series instead of the saved value.
    """
    set_schema(["pass"])
    df = pd.DataFrame(
        {
            "pass": [1, 2, 3],
            "RHAM": [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
            "RVL": [[7.0], [8.0], [9.0]],
        }
    )
    received = []

    def fn(emg):
        received.append(emg)
        return 0.0

    for_each(
        fn,
        inputs={"emg": df},
        _mapping_inputs={"emg": ["RHAM", "RVL"]},
        **{"pass": [1, 2, 3]},
    )

    assert len(received) == 3
    assert all(isinstance(r, dict) for r in received)
    assert [sorted(r) for r in received] == [["RHAM", "RVL"]] * 3
    assert received[0]["RHAM"] == [1.0, 2.0]
    assert received[2]["RVL"] == [9.0]


def test_mapping_input_multi_row_slice_stays_a_frame():
    """A coarser iteration level has no single dict to give, so the frame is
    still the honest answer."""
    set_schema(["pass", "cycle"])
    df = pd.DataFrame(
        {
            "pass": [1, 1, 2],
            "cycle": [1, 2, 1],
            "RHAM": [1.0, 2.0, 3.0],
            "RVL": [4.0, 5.0, 6.0],
        }
    )
    received = []

    for_each(
        lambda emg: received.append(emg) or 0.0,
        inputs={"emg": df},
        _mapping_inputs={"emg": ["RHAM", "RVL"]},
        **{"pass": [1]},
    )

    assert len(received) == 1
    assert isinstance(received[0], pd.DataFrame)
    assert len(received[0]) == 2


def test_mapping_input_ignored_under_as_table():
    """as_table is an explicit request for the frame and still wins."""
    set_schema(["pass"])
    df = pd.DataFrame({"pass": [1], "RHAM": [1.0], "RVL": [2.0]})
    received = []

    for_each(
        lambda emg: received.append(emg) or 0.0,
        inputs={"emg": df},
        as_table=True,
        _mapping_inputs={"emg": ["RHAM", "RVL"]},
        **{"pass": [1]},
    )

    assert isinstance(received[0], pd.DataFrame)


def test_mapping_input_ignored_under_column_selection():
    """Asking for specific columns is more specific than the mapping mark."""
    set_schema(["pass"])
    df = pd.DataFrame({"pass": [1], "RHAM": [1.0], "RVL": [2.0]})
    received = []

    for_each(
        lambda emg: received.append(emg) or 0.0,
        inputs={"emg": ColumnSelection(df, ["RHAM"])},
        _mapping_inputs={"emg": ["RHAM", "RVL"]},
        **{"pass": [1]},
    )

    assert not isinstance(received[0], dict)


def test_unmarked_multi_column_input_is_unchanged():
    """Without the mark, a genuine table-valued variable keeps its frame —
    the reason this cannot be inferred from shape alone."""
    set_schema(["pass"])
    df = pd.DataFrame({"pass": [1], "a": [1.0], "b": [2.0]})
    received = []

    for_each(
        lambda tbl: received.append(tbl) or 0.0,
        inputs={"tbl": df},
        **{"pass": [1]},
    )

    assert isinstance(received[0], pd.DataFrame)


def test_mapping_input_falls_back_when_no_declared_column_survives():
    """A stale/renamed column list degrades to the previous behavior rather
    than handing the function an empty struct."""
    set_schema(["pass"])
    df = pd.DataFrame({"pass": [1], "a": [1.0], "b": [2.0]})
    received = []

    for_each(
        lambda tbl: received.append(tbl) or 0.0,
        inputs={"tbl": df},
        _mapping_inputs={"tbl": ["gone", "also_gone"]},
        **{"pass": [1]},
    )

    assert not isinstance(received[0], dict)


def test_distribute_target_is_reported_at_info(caplog):
    """The resolved distribute target must be observable at INFO.

    This one line is the only difference in the log between "distribute ran"
    and "distribute was silently dropped upstream" — and the drop is invisible
    otherwise, because the run still succeeds and still saves, just at the
    wrong granularity. At DEBUG the two were indistinguishable, which is how a
    MATLAB run requested with distribute=true went undiagnosed until the saved
    columns were read by hand (2026-09-14).
    """
    set_schema(["subject", "trial"])

    def fn():
        return np.array([10.0, 20.0])

    with caplog.at_level(logging.INFO):
        for_each(fn, inputs={}, distribute=True, subject=[1])

    assert "resolve_distribute_target" in caplog.text
    assert "trial" in caplog.text


def test_distribute_targets_the_unpopulated_key_below_the_deepest_iterated(caplog):
    """The exact shape the 2026-09-14 GAITRite run had: schema
    [subject, session, speed, trial, cycle], 'cycle' with no values so it is
    dropped from the iteration, leaving 'cycle' as the distribute target.

    The run that exposed the bug saved 560 rows with columns
    [subject, session, speed, trial, GAITRiteLoaded] — no 'cycle'. That absence
    is the ground truth this asserts against.
    """
    set_schema(["subject", "session", "speed", "trial", "cycle"])

    def fn():
        return np.array([1.0, 2.0, 3.0])

    with caplog.at_level(logging.INFO):
        result = for_each(
            fn,
            inputs={},
            distribute=True,
            subject=["SS01"],
            session=["BL"],
            speed=["FV"],
            trial=[1],
        )

    assert "cycle" in result.columns
    assert sorted(result["cycle"]) == [1, 2, 3]
    assert len(result) == 3
    assert "resolve_distribute_target" in caplog.text


def test_case_a_adopts_template_keys_in_placeholder_order(tmp_path):
    """Case A adopts discovered keys in TEMPLATE PLACEHOLDER order.

    With no schema set and no ``key=[]`` kwargs, ``PathInput.apply_discovery``
    takes Case A and adopts every discovered key. The order it adopts them in
    is ``combos[0].keys()`` — the order the walk bound them, i.e. the order the
    placeholders appear in the template.

    Pinned here because the MATLAB port reproduces this order deliberately
    (``+scifor/for_each.m``, Case A block, which orders by
    ``pi.placeholder_keys()``). If Python's order ever changes, a MATLAB run and
    a Python run of the same pipeline would put the same columns in different
    places, and only this test would say so.
    """
    set_schema([])
    for subject, session, speed in [
        ("1", "A", "fast"),
        ("1", "A", "slow"),
        ("1", "B", "fast"),
        ("2", "A", "fast"),
    ]:
        d = tmp_path / subject / "XSENS" / session
        d.mkdir(parents=True, exist_ok=True)
        (d / f"{subject}_XSENS_{session}_{speed}-001.xlsx").write_text("")

    path_input = scifor.PathInput(
        "{subject}/XSENS/{session}/{subject}_XSENS_{session}_{speed}-001.xlsx",
        root_folder=str(tmp_path), name="{subject}/XSENS/{session}/{subject}_XSENS_{session}_{speed}-001.xlsx",
    )

    # The parameter name must BE the input name: for_each calls fn(**kwargs).
    # A mismatch raises TypeError on every combo, which continue-and-report
    # swallows into an empty result — indistinguishable from "discovery found
    # nothing" at the assert, which is how this test read as a discovery bug.
    result = for_each(
        lambda xlsx_file_path: str(xlsx_file_path),
        inputs={"xlsx_file_path": path_input},
    )

    # One row per real file, not the 2x2x2 Cartesian product: Case A returns
    # the disk combos so they drive iteration directly.
    assert len(result) == 4

    # Repeated placeholders ({subject} and {session} each appear twice) are
    # adopted once, at first appearance.
    key_cols = [c for c in result.columns if c in {"subject", "session", "speed"}]
    assert key_cols == ["subject", "session", "speed"]


# ---------------------------------------------------------------------------
# spread_nested_results — the spread rule applied to a nested-mode table
# (what the MATLAB loop hands back across the bridge)
# ---------------------------------------------------------------------------


def _nested(rows):
    """Build a nested-mode result table: metadata columns + 'Out' cells."""
    return pd.DataFrame(rows)


def test_spread_nested_results_spreads_labelled_table(caplog):
    """The 2026-09-15 loadFunctionalOutcomes shape: nothing iterated, ONE
    combination row whose 'Out' cell is a table carrying subject/session.
    Every row must get its own address, exactly as the Python loop files it."""
    from scifor.foreach import spread_nested_results

    inner = pd.DataFrame(
        {"subject": ["A", "A", "B"], "session": [1, 2, 1], "val": [1.0, 2.0, 3.0]}
    )
    nested = _nested([{"Out": inner}])
    with caplog.at_level(logging.INFO):
        out = spread_nested_results(nested, ["Out"], ["subject", "session", "speed"])
    assert len(out) == 3
    assert "Out" not in out.columns  # flat: data columns spread directly
    assert list(out["subject"]) == ["A", "A", "B"]
    assert list(out["session"]) == [1, 2, 1]
    assert list(out["val"]) == [1.0, 2.0, 3.0]
    assert "discriminated by unpinned schema key(s)" in caplog.text


def test_spread_nested_results_keeps_metadata_per_row():
    """Pinned metadata (an iterated subject, a __rid_* column) is replicated
    onto every spread row; the unpinned key comes from the data."""
    from scifor.foreach import spread_nested_results

    nested = _nested(
        [
            {
                "subject": "A",
                "__rid_x": "r1",
                "Out": pd.DataFrame({"session": [1, 2], "val": [1.0, 2.0]}),
            },
            {
                "subject": "B",
                "__rid_x": "r2",
                "Out": pd.DataFrame({"session": [1], "val": [3.0]}),
            },
        ]
    )
    out = spread_nested_results(nested, ["Out"], ["subject", "session"])
    assert len(out) == 3
    assert list(out["subject"]) == ["A", "A", "B"]
    assert list(out["__rid_x"]) == ["r1", "r1", "r2"]
    assert list(out["session"]) == [1, 2, 1]


def test_spread_nested_results_unlabelled_table_stays_one_row(caplog):
    """A multi-row table with NO schema-key column has nothing to address
    its rows with — one record per combination, table kept whole in the
    output cell, and the log says so."""
    from scifor.foreach import spread_nested_results

    inner = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    nested = _nested([{"subject": "A", "Out": inner}])
    with caplog.at_level(logging.INFO):
        out = spread_nested_results(nested, ["Out"], ["subject", "session"])
    assert len(out) == 1
    assert list(out.columns) == ["subject", "Out"]
    assert out["Out"].iloc[0].equals(inner)
    assert "saving each whole table as ONE record" in caplog.text


def test_spread_nested_results_non_dataframe_cells_pass_through():
    """Scalars / arrays are not spread candidates; the table comes back with
    the same rows and values."""
    from scifor.foreach import spread_nested_results

    nested = _nested([{"subject": "A", "Out": 1.5}, {"subject": "B", "Out": 2.5}])
    out = spread_nested_results(nested, ["Out"], ["subject", "session"])
    assert len(out) == 2
    assert list(out["Out"]) == [1.5, 2.5]


def test_spread_nested_results_ignores_absent_output_columns():
    """An already-flat table (no output column present) is returned as is."""
    from scifor.foreach import spread_nested_results

    flat = pd.DataFrame({"subject": ["A"], "val": [1.0]})
    out = spread_nested_results(flat, ["Out"], ["subject"])
    assert out is flat


# ---------------------------------------------------------------------------
# _select_rows: the caller narrows a frame beyond the schema filter
# ---------------------------------------------------------------------------


class TestSelectRowsHook:
    """``_select_rows(name, frame, metadata)`` runs after scifor's own
    schema-key filter and before extraction. It is how a caller that knows
    which ROWS a combination reads (scidb: which record ids) says so, instead
    of extending scifor's schema with private keys and stripping them back
    out (the convention this replaced, 2026-09-20)."""

    def _two_variants(self):
        # Two rows per (subject, session): the same location, two "variants",
        # distinguished by a bookkeeping column the function must never see.
        rows = []
        for s in (1, 2):
            for v, rid in (("a", "r-a"), ("b", "r-b")):
                rows.append({"subject": s, "session": "pre", "__rec": rid, "emg": f"{s}{v}"})
        return pd.DataFrame(rows)

    def test_hook_sees_the_schema_filtered_frame_and_its_return_is_used(self):
        set_schema(["subject", "session"])
        seen: list = []

        def select(name, frame, metadata):
            seen.append((name, sorted(frame["subject"].unique()), dict(metadata)))
            keep = frame[frame["__rec"] == metadata["__pick"]]
            return keep.drop(columns=["__rec"])

        def fn(x):
            return x

        combos = [
            {"subject": 1, "session": "pre", "__pick": "r-a"},
            {"subject": 1, "session": "pre", "__pick": "r-b"},
            {"subject": 2, "session": "pre", "__pick": "r-a"},
        ]
        result = for_each(
            fn,
            {"x": self._two_variants()},
            output_names=["out"],
            _all_combos=combos,
            _select_rows=select,
            subject=[1, 2],
            session=["pre"],
        )
        # The hook saw only the rows the schema filter left (one subject each).
        assert [s for _n, s, _m in seen] == [[1], [1], [2]]
        assert all(n == "x" for n, _s, _m in seen)
        # Its return is what the function received: one row, one column, so
        # a scalar — and the bookkeeping column never reached the function.
        assert list(result["out"]) == ["1a", "1b", "2a"]
        # The combo's own keys pass through to the result row untouched.
        assert list(result["__pick"]) == ["r-a", "r-b", "r-a"]

    def test_hook_returning_nothing_is_no_data_for_that_combo(self):
        set_schema(["subject", "session"])

        def select(name, frame, metadata):
            return frame.iloc[0:0]

        result = for_each(
            lambda x: x,
            {"x": self._two_variants()},
            output_names=["out"],
            _select_rows=select,
            subject=[1, 2],
            session=["pre"],
        )
        assert result is None or result.empty

    def test_no_hook_is_the_old_behaviour(self):
        set_schema(["subject", "session"])
        result = for_each(
            lambda x: len(x),
            {"x": self._two_variants()},
            output_names=["n"],
            subject=[1, 2],
            session=["pre"],
        )
        # Two rows per location, no hook → the function gets both.
        assert list(result["n"]) == [2, 2]

    def test_hook_is_applied_on_the_for_columns_path_too(self):
        set_schema(["subject", "session"])
        df = self._two_variants().rename(columns={"emg": "a"})
        df["b"] = df["a"] + "!"

        def select(name, frame, metadata):
            return frame[frame["__rec"] == "r-a"].drop(columns=["__rec"])

        result = for_each(
            lambda x: str(x[0]) if hasattr(x, "__len__") else str(x),
            {"x": ColumnSelection(df, ["a", "b"], iterate=True)},
            output_names=["out"],
            _select_rows=select,
            subject=[1],
            session=["pre"],
        )
        assert set(result.columns) >= {"a", "b"}
        assert list(result["a"]) == ["1a"] and list(result["b"]) == ["1a!"]
