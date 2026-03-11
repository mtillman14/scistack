"""Tests for scifor.for_each in standalone (no-DB) mode."""

import pytest
import pandas as pd
import numpy as np

import scifor
from scifor import set_schema, for_each, Fixed, Merge, ColumnSelection, Col


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
    """Multiple matching rows -> sub-DataFrame passed."""
    set_schema(["subject"])
    df = pd.DataFrame({
        "subject": [1, 1, 2, 2],
        "trial": [1, 2, 1, 2],
        "emg": [1.0, 2.0, 3.0, 4.0],
    })
    received_shapes = []
    received_types = []

    def fn(data):
        received_shapes.append(data.shape)
        received_types.append(type(data))
        return 0

    for_each(fn, inputs={"data": df}, subject=[1, 2])
    # After dropping schema col "subject", we get a vector of 2 emg values
    assert received_shapes == [(2,1), (2,1)]
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
    df = pd.DataFrame({
        "subject": [1, 1, 1],
        "speed": [0.5, 1.5, 2.5],
        "value": [10.0, 20.0, 30.0],
    })
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
    df = pd.DataFrame({"subject": [1, 2], "a": [1.0, 2.0], "b": [3.0, 4.0], "c": [5.0, 6.0]})
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
    df = pd.DataFrame({
        "subject": [1, 1, 2, 2],
        "trial": [1, 2, 1, 2],
        "signal": [10.0, 20.0, 30.0, 40.0],
        "noise": [0.1, 0.2, 0.3, 0.4],
    })
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
    df = pd.DataFrame({
        "subject": [1, 1, 2, 2],
        "a": [1.0, 2.0, 3.0, 4.0],
        "b": [10.0, 20.0, 30.0, 40.0],
        "c": [100.0, 200.0, 300.0, 400.0],
    })
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
    df = pd.DataFrame({
        "subject": [1, 2],
        "signal": [10.0, 20.0],
        "noise": [0.1, 0.2],
    })
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
# Error handling
# ---------------------------------------------------------------------------

def test_function_error_skips(capsys):
    """Function errors skip the iteration gracefully."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "value": [10.0, 20.0]})
    call_count = [0]

    def fn(x):
        call_count[0] += 1
        if call_count[0] == 1:
            raise ValueError("bad")
        return x

    result = for_each(fn, inputs={"x": df}, subject=[1, 2])
    out = capsys.readouterr().out
    assert "[skip]" in out
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


def test_flatten_mode_dataframe_outputs():
    """When fn returns DataFrames, metadata is replicated per row."""
    set_schema(["subject"])
    result = for_each(
        lambda: pd.DataFrame({"val": [10.0, 20.0, 30.0]}),
        inputs={},
        subject=[1, 2],
    )
    # 2 subjects * 3 rows each = 6 total rows
    assert len(result) == 6
    assert "subject" in result.columns
    assert "val" in result.columns
