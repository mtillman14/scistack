"""Tests for scifor.for_each in standalone (no-DB) mode."""

import pytest
import pandas as pd
import numpy as np

import scifor
from scifor import set_schema, for_each, Fixed, Col


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


class MockOutput:
    saved = []

    @classmethod
    def save(cls, data, **metadata):
        cls.saved.append({"data": data, "metadata": metadata})

    @classmethod
    def reset(cls):
        cls.saved = []


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
        outputs=[],
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
        outputs=[],
        subject=[1, 2],
        session=["pre"],
    )
    assert len(received) == 2
    for r in received:
        pd.testing.assert_frame_equal(r, coeffs)


def test_per_combo_df_single_value_extracted():
    """1 row, 1 data column → scalar extracted."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "value": [10.0, 20.0]})
    received = []

    def fn(x):
        received.append(x)
        return x

    for_each(fn, inputs={"x": df}, outputs=[], subject=[1, 2])
    assert received == [10.0, 20.0]


def test_per_combo_df_multiple_rows_passed_as_df():
    """Multiple matching rows → sub-DataFrame passed."""
    set_schema(["subject"])
    df = pd.DataFrame({
        "subject": [1, 1, 2, 2],
        "trial": [1, 2, 1, 2],
        "emg": [1.0, 2.0, 3.0, 4.0],
    })
    received_shapes = []

    def fn(data):
        received_shapes.append(data.shape)
        return 0

    for_each(fn, inputs={"data": df}, outputs=[], subject=[1, 2])
    assert received_shapes == [(2, 3), (2, 3)]


def test_as_table_forces_dataframe():
    """as_table=True keeps DataFrame even for 1-row/1-col result."""
    set_schema(["subject"])
    df = pd.DataFrame({"subject": [1, 2], "value": [10.0, 20.0]})
    received = []

    def fn(x):
        received.append(x)
        return 0

    for_each(fn, inputs={"x": df}, outputs=[], subject=[1, 2], as_table=True)
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
        outputs=[],
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
        outputs=[],
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
            outputs=[],
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
        outputs=[],
        subject=[1],
        session=["pre", "post"],
    )
    assert "subject" in result.columns
    assert "session" in result.columns


# ---------------------------------------------------------------------------
# distribute=True
# ---------------------------------------------------------------------------

def test_distribute_requires_schema():
    """distribute=True with no schema raises ValueError."""
    with pytest.raises(ValueError, match="set_schema"):
        for_each(
            lambda: [1, 2, 3],
            inputs={},
            outputs=[],
            distribute=True,
            subject=[1],
        )


def test_distribute_saves_pieces():
    """distribute=True splits output and saves each piece separately."""
    set_schema(["subject", "trial"])
    MockOutput.reset()

    def fn():
        return np.array([10.0, 20.0, 30.0])

    for_each(
        fn,
        inputs={},
        outputs=[MockOutput],
        distribute=True,
        subject=[1],
    )
    # 3 pieces saved with trial=1,2,3
    assert len(MockOutput.saved) == 3
    trials = [s["metadata"]["trial"] for s in MockOutput.saved]
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
        outputs=[],
        subject=[1, 2],
        dry_run=True,
    )
    assert result is None
    captured = capsys.readouterr()
    assert "[dry-run]" in captured.out


# ---------------------------------------------------------------------------
# save=False
# ---------------------------------------------------------------------------

def test_save_false_does_not_call_save():
    set_schema(["subject"])
    MockOutput.reset()

    def fn():
        return 42

    for_each(
        fn,
        inputs={},
        outputs=[MockOutput],
        save=False,
        subject=[1],
    )
    assert MockOutput.saved == []


# ---------------------------------------------------------------------------
# pass_metadata
# ---------------------------------------------------------------------------

def test_pass_metadata():
    set_schema(["subject"])
    received_meta = []

    def fn(subject):
        received_meta.append(subject)
        return subject

    for_each(
        fn,
        inputs={},
        outputs=[],
        pass_metadata=True,
        subject=[1, 2],
    )
    assert received_meta == [1, 2]


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
        outputs=[],
        subject=[1, 2],
    )
    assert received == [0.5, 0.5]


# ---------------------------------------------------------------------------
# Load failure skips iteration gracefully
# ---------------------------------------------------------------------------

def test_load_failure_skips(capsys):
    set_schema(["subject"])

    class FailingInput:
        __name__ = "FailingInput"

        @staticmethod
        def load(**meta):
            if meta.get("subject") == 2:
                raise RuntimeError("no data")
            return 42

    def fn(x):
        return x

    result = for_each(
        fn,
        inputs={"x": FailingInput},
        outputs=[],
        subject=[1, 2],
    )
    out = capsys.readouterr().out
    assert "[skip]" in out
    assert len(result) == 1  # only subject=1 succeeded
