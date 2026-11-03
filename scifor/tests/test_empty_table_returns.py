"""A function that returns an EMPTY table (``pd.DataFrame()``, or a frame with
columns but no rows) has nothing to save at that combination.

``scifor.outputs.is_empty_table`` owns the rule. The slot becomes
``NoOutput("no_data", EMPTY_TABLE_DETAIL)``; every slot empty -> the
combination is one no-data combination; and the run WARNs once per output,
naming the combinations. Before 2026-11-03 the outcome depended on what the
OTHER combinations returned: dropped silently (spread mode), or kept as a
record value that the save turned into a phantom record or a failed batch.
"""

import logging

import pandas as pd
import pytest

from scifor import ColumnSelection, LoopHooks, for_each, set_schema
from scifor.outputs import (
    EMPTY_TABLE_DETAIL,
    empty_tables_to_no_output,
    is_empty_table,
    is_no_output,
)


@pytest.fixture(autouse=True)
def _schema():
    set_schema(["subject"])
    yield
    set_schema([])


DATA = pd.DataFrame({"subject": [1, 2, 3], "x": [1.0, 2.0, 3.0]})
INPUTS = {"x": ColumnSelection(DATA, ["x"])}


def _summary(events):
    (summary,) = [e for e in events if e.get("event") == "summary"]
    return summary


def _warnings(caplog):
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


# --- the rule -------------------------------------------------------------


@pytest.mark.parametrize(
    "value, expected",
    [
        (pd.DataFrame(), True),
        (pd.DataFrame({"a": []}), True),  # columns, no rows
        (pd.DataFrame({"a": [1.0]}), False),
        (pd.DataFrame(index=[0]), False),  # one row, no columns: not "empty" here
        (None, False),
        ([], False),
        (0.0, False),
    ],
)
def test_is_empty_table(value, expected):
    assert is_empty_table(value) is expected


def test_empty_tables_to_no_output_keeps_the_shape():
    df = pd.DataFrame({"a": [1.0]})
    single, idx = empty_tables_to_no_output(pd.DataFrame())
    assert is_no_output(single) and single.detail == EMPTY_TABLE_DETAIL and idx == [0]
    same, idx = empty_tables_to_no_output(df)
    assert same is df and idx == []
    pair, idx = empty_tables_to_no_output((df, pd.DataFrame()))
    assert isinstance(pair, tuple) and pair[0] is df and is_no_output(pair[1])
    assert idx == [1]


# --- one output -----------------------------------------------------------


def one_row_or_empty(x):
    """Spread mode: the other combinations return 1-row frames."""
    if x == 2.0:
        return pd.DataFrame()
    return pd.DataFrame({"v": [x * 10]})


def multi_row_or_empty_first(x):
    """One-record-per-combination mode, the EMPTY frame first (it used to
    define the save's reference schema)."""
    if x == 1.0:
        return pd.DataFrame()
    return pd.DataFrame({"v": [x, x + 0.5]})


def zero_row_frame_with_columns(x):
    if x == 2.0:
        return pd.DataFrame({"v": pd.Series([], dtype=float)})
    return pd.DataFrame({"v": [x * 10]})


@pytest.mark.parametrize(
    "fn, empty_subject",
    [
        (one_row_or_empty, 2),
        (multi_row_or_empty_first, 1),
        (zero_row_frame_with_columns, 2),
    ],
)
def test_empty_return_is_one_no_data_combination(fn, empty_subject, caplog):
    events = []
    with caplog.at_level(logging.INFO, logger="scifor"):
        result = for_each(
            fn,
            inputs=INPUTS,
            outputs=["Out"],
            subject=[1, 2, 3],
            hooks=LoopHooks(progress_fn=events.append),
        )
    summary = _summary(events)
    assert summary["completed"] == 2
    assert summary["no_data"] == 1
    assert summary["failed"] == 0
    assert empty_subject not in set(result["subject"])
    # One WARN for the output, naming the combination.
    (warn,) = [w for w in _warnings(caplog) if EMPTY_TABLE_DETAIL in w]
    assert warn == (
        f"output Out: the function returned an empty table at 1 combination(s) "
        f"— nothing saved there: subject={empty_subject}"
    )


def test_every_combination_empty(caplog):
    events = []
    with caplog.at_level(logging.INFO, logger="scifor"):
        result = for_each(
            lambda x: pd.DataFrame(),
            inputs=INPUTS,
            outputs=["Out"],
            subject=[1, 2, 3],
            hooks=LoopHooks(progress_fn=events.append),
        )
    summary = _summary(events)
    assert summary["completed"] == 0
    assert summary["no_data"] == 3
    assert result is None or result.empty
    assert any("at 3 combination(s)" in w for w in _warnings(caplog))
    # No data, not a failure: the "every iteration failed" WARN stays quiet.
    assert not any("every one of the" in w for w in _warnings(caplog))


# --- several outputs ------------------------------------------------------


def a_empty_at_2(x):
    a = pd.DataFrame() if x == 2.0 else pd.DataFrame({"a": [x]})
    return a, pd.DataFrame({"b": [x * 10]})


def test_one_empty_output_keeps_the_other(caplog):
    events = []
    with caplog.at_level(logging.INFO, logger="scifor"):
        result = for_each(
            a_empty_at_2,
            inputs=INPUTS,
            outputs=["A", "B"],
            subject=[1, 2, 3],
            hooks=LoopHooks(progress_fn=events.append),
        )
    by_subject = result.set_index("subject")
    assert by_subject.loc[2, "A"] is None  # standalone: a missing cell
    assert by_subject.loc[2, "B"]["b"].iloc[0] == 20.0
    summary = _summary(events)
    assert summary["completed"] == 3
    assert summary["output_skips"] == {
        "A": [
            {
                "reason": "no_data",
                "detail": EMPTY_TABLE_DETAIL,
                "count": 1,
                "combos": ["subject=2"],
            }
        ]
    }
    assert any(w.startswith("output A: the function returned an empty table") for w in _warnings(caplog))


def test_one_empty_output_is_a_marker_for_a_saving_caller():
    result = for_each(
        a_empty_at_2,
        inputs=INPUTS,
        outputs=["A", "B"],
        subject=[1, 2, 3],
        hooks=LoopHooks(keep_no_output=True),
    )
    cell = result.set_index("subject").loc[2, "A"]
    assert is_no_output(cell)
    assert (cell.reason, cell.detail) == ("no_data", EMPTY_TABLE_DETAIL)


def test_every_output_empty_is_one_no_data_combination():
    events = []
    for_each(
        lambda x: (pd.DataFrame(), pd.DataFrame()),
        inputs=INPUTS,
        outputs=["A", "B"],
        subject=[1],
        hooks=LoopHooks(progress_fn=events.append),
    )
    summary = _summary(events)
    assert (summary["completed"], summary["no_data"], summary["failed"]) == (0, 1, 0)
    (failure,) = summary["failures"]
    assert f"A: {EMPTY_TABLE_DETAIL}" in str(failure)


def test_non_table_outputs_are_untouched():
    """Only tables: an empty list or array is a value, not "no output"."""
    result = for_each(
        lambda x: [],
        inputs=INPUTS,
        outputs=["Out"],
        subject=[1, 2],
    )
    assert list(result["Out"]) == [[], []]
