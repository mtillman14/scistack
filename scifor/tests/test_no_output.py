"""NoOutput: one output skipped at one combination while the call's other
outputs are kept (Stage 1 of .claude/plan-for-columns-groups.md).

The marker is internal: for_columns split mode will set it for a group with
no data or one the function raised on. These tests return it from a plain
multi-output function to pin the loop's handling before anything produces it.
"""

import logging

import pandas as pd
import pytest

from scifor import ColumnSelection, for_each, set_schema
from scifor.outputs import NoOutput, is_no_output


@pytest.fixture(autouse=True)
def _schema():
    set_schema(["subject"])
    yield
    set_schema([])


DATA = pd.DataFrame({"subject": [1, 2, 3], "x": [1.0, 2.0, 3.0]})


def skip_first_at_2(x):
    if x == 2.0:
        return NoOutput("no_data"), x * 10
    return x, x * 10


def _summary(events):
    (summary,) = [e for e in events if e.get("event") == "summary"]
    return summary


def test_marker_basics():
    assert is_no_output(NoOutput("no_data"))
    assert not is_no_output(None)
    assert not is_no_output(float("nan"))
    assert repr(NoOutput("failed", "ValueError: x")) == "NoOutput('failed', 'ValueError: x')"
    with pytest.raises(ValueError, match="reason must be one of"):
        NoOutput("missing")


def test_one_skipped_output_keeps_the_other(caplog):
    events = []
    with caplog.at_level(logging.INFO, logger="scifor"):
        result = for_each(
            skip_first_at_2,
            inputs={"x": ColumnSelection(DATA, ["x"])},
            outputs=["A", "B"],
            subject=[1, 2, 3],
            _progress_fn=events.append,
        )
    by_subject = result.set_index("subject")
    # Standalone: the skipped cell is missing, the other output is kept.
    assert pd.isna(by_subject.loc[2, "A"])  # None in a float column reads back as NaN
    assert by_subject.loc[2, "B"] == 20.0
    assert by_subject.loc[1, "A"] == 1.0
    # The combination still completed: one of its outputs was produced.
    summary = _summary(events)
    assert summary["completed"] == 3
    assert summary["no_data"] == 0
    assert summary["output_skips"] == {
        "A": [{"reason": "no_data", "detail": "", "count": 1, "combos": ["subject=2"]}]
    }
    assert "output A: no data × 1 — subject=2" in caplog.text


def test_keep_no_output_leaves_the_marker_for_a_saving_caller():
    result = for_each(
        skip_first_at_2,
        inputs={"x": ColumnSelection(DATA, ["x"])},
        outputs=["A", "B"],
        subject=[1, 2, 3],
        _keep_no_output=True,
    )
    cell = result.set_index("subject").loc[2, "A"]
    assert is_no_output(cell)
    assert cell.reason == "no_data"


def all_no_data(x):
    return NoOutput("no_data", "group empty"), NoOutput("no_data", "group empty")


def test_every_output_no_data_is_one_no_data_combination():
    events = []
    result = for_each(
        all_no_data,
        inputs={"x": ColumnSelection(DATA, ["x"])},
        outputs=["A", "B"],
        subject=[1],
        _progress_fn=events.append,
    )
    summary = _summary(events)
    assert summary["completed"] == 0
    assert summary["no_data"] == 1
    assert summary["failed"] == 0
    assert result is None or result.empty


def one_failed_one_no_data(x):
    return NoOutput("failed", "ValueError: all blank"), NoOutput("no_data")


def test_every_output_skipped_with_a_failure_is_one_failed_combination():
    events = []
    for_each(
        one_failed_one_no_data,
        inputs={"x": ColumnSelection(DATA, ["x"])},
        outputs=["A", "B"],
        subject=[1],
        _progress_fn=events.append,
    )
    summary = _summary(events)
    assert summary["completed"] == 0
    assert summary["failed"] == 1
    (failure,) = summary["failures"]
    assert "A: ValueError: all blank" in str(failure)


def one_row_frames(x):
    return pd.DataFrame({"a": [x]}), pd.DataFrame({"b": [x * 10]})


def test_multi_output_one_row_frames_stay_per_output(caplog):
    """Several outputs are never spread into one wide row: each output keeps
    its own frame in its own cell (scidb/tests/test_multi_output_frames.py)."""
    with caplog.at_level(logging.INFO, logger="scifor"):
        result = for_each(
            one_row_frames,
            inputs={"x": ColumnSelection(DATA, ["x"])},
            outputs=["A", "B"],
            subject=[1, 2],
        )
    assert list(result.columns) == ["subject", "A", "B"]
    row = result.set_index("subject").loc[1]
    assert list(row["A"].columns) == ["a"]
    assert list(row["B"].columns) == ["b"]
    assert "each output's DataFrame is saved as its own record" in caplog.text


def test_single_output_one_row_frame_still_spreads():
    result = for_each(
        lambda x: pd.DataFrame({"a": [x], "b": [x * 10]}),
        inputs={"x": ColumnSelection(DATA, ["x"])},
        subject=[1, 2],
    )
    assert list(result.columns) == ["subject", "a", "b"]
