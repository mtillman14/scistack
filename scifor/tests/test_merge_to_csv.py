"""Tests for scifor.Merge.to_csv() flat-table inner-join export.

to_csv() joins the Merge constituents on their shared schema columns (THE
Merge rule, docs/claude/merge-cases.json),
keeps one copy of those columns, and writes one flat CSV. Non-schema columns
may not overlap, and a location on one side only is an error.
"""

import pandas as pd
import pytest

from scifor import Col, ColumnSelection, Merge, set_schema


def setup_function():
    # Reset schema before each test.
    set_schema([])


def _read(path):
    return pd.read_csv(path)


# ---------------------------------------------------------------------------
# Basic inner join
# ---------------------------------------------------------------------------


def test_two_dataframes_inner_join_one_copy_of_schema(tmp_path):
    """Two constituents sharing a schema col → inner join, single schema copy."""
    set_schema(["subject", "trial"])
    step = pd.DataFrame(
        {"subject": [1, 1, 2], "trial": [1, 2, 1], "StepLength": [0.5, 0.6, 0.7]}
    )
    speed = pd.DataFrame(
        {"subject": [1, 1, 2], "trial": [1, 2, 1], "Speed": [1.1, 1.2, 1.3]}
    )
    out = tmp_path / "gait.csv"
    Merge(step, speed).to_csv(str(out))

    df = _read(out)
    # One copy of each schema column, plus both data columns.
    assert list(df.columns) == ["subject", "trial", "StepLength", "Speed"]
    assert len(df) == 3
    row = df[(df.subject == 1) & (df.trial == 2)].iloc[0]
    assert row.StepLength == 0.6 and row.Speed == 1.2


def test_rows_on_one_side_only_raise(tmp_path):
    """A location present in only one constituent is an error naming it, never
    a silent drop (THE Merge rule, docs/claude/merge-cases.json)."""
    set_schema(["subject", "trial"])
    step = pd.DataFrame(
        {"subject": [1, 1, 2], "trial": [1, 2, 1], "StepLength": [0.5, 0.6, 0.7]}
    )
    # speed is missing subject=1/trial=2 and adds subject=3/trial=1.
    speed = pd.DataFrame(
        {"subject": [1, 2, 3], "trial": [1, 1, 1], "Speed": [1.1, 1.3, 9.9]}
    )
    out = tmp_path / "gait.csv"
    with pytest.raises(ValueError, match="Merge would drop rows"):
        Merge(step, speed).to_csv(str(out))
    assert not out.exists()


def test_partial_schema_overlap_joins_on_common_keys(tmp_path):
    """A constituent with fewer schema cols joins on the shared subset."""
    set_schema(["subject", "trial"])
    per_trial = pd.DataFrame(
        {"subject": [1, 1], "trial": [1, 2], "StepLength": [0.5, 0.6]}
    )
    per_subject = pd.DataFrame({"subject": [1], "Height": [180.0]})
    out = tmp_path / "j.csv"
    Merge(per_trial, per_subject).to_csv(str(out))

    df = _read(out)
    assert list(df.columns) == ["subject", "trial", "StepLength", "Height"]
    assert len(df) == 2  # Height broadcast across subject=1's two trials
    assert set(df.Height) == {180.0}


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


def test_where_filter(tmp_path):
    """where= filters constituent rows before the join."""
    set_schema(["subject", "trial"])
    step = pd.DataFrame(
        {"subject": [1, 1, 2], "trial": [1, 2, 1], "StepLength": [0.5, 0.6, 0.7]}
    )
    speed = pd.DataFrame(
        {"subject": [1, 1, 2], "trial": [1, 2, 1], "Speed": [1.1, 1.2, 1.3]}
    )
    out = tmp_path / "gait.csv"
    Merge(step, speed).to_csv(str(out), where=Col("StepLength") > 0.55)

    df = _read(out)
    assert set(df.StepLength) == {0.6, 0.7}


def test_metadata_scalar_filter(tmp_path):
    """A scalar metadata kwarg filters by equality on the schema column."""
    set_schema(["subject", "trial"])
    step = pd.DataFrame(
        {"subject": [1, 1, 2], "trial": [1, 2, 1], "StepLength": [0.5, 0.6, 0.7]}
    )
    speed = pd.DataFrame(
        {"subject": [1, 1, 2], "trial": [1, 2, 1], "Speed": [1.1, 1.2, 1.3]}
    )
    out = tmp_path / "gait.csv"
    Merge(step, speed).to_csv(str(out), subject=1)

    df = _read(out)
    assert set(df.subject) == {1}
    assert len(df) == 2


def test_metadata_list_filter(tmp_path):
    """A list metadata kwarg filters by membership."""
    set_schema(["subject", "trial"])
    step = pd.DataFrame(
        {"subject": [1, 2, 3], "trial": [1, 1, 1], "StepLength": [0.5, 0.6, 0.7]}
    )
    speed = pd.DataFrame(
        {"subject": [1, 2, 3], "trial": [1, 1, 1], "Speed": [1.1, 1.2, 1.3]}
    )
    out = tmp_path / "gait.csv"
    Merge(step, speed).to_csv(str(out), subject=[1, 3])

    df = _read(out)
    assert set(df.subject) == {1, 3}


# ---------------------------------------------------------------------------
# ColumnSelection constituents
# ---------------------------------------------------------------------------


def test_column_selection_keeps_join_keys(tmp_path):
    """A ColumnSelection picks a subset of data cols but still joins on schema."""
    set_schema(["subject", "trial"])
    gait = pd.DataFrame(
        {
            "subject": [1, 1],
            "trial": [1, 2],
            "StepLength": [0.5, 0.6],
            "Cadence": [100, 110],
        }
    )
    speed = pd.DataFrame({"subject": [1, 1], "trial": [1, 2], "Speed": [1.1, 1.2]})
    out = tmp_path / "sel.csv"
    Merge(ColumnSelection(gait, ["StepLength"]), speed).to_csv(str(out))

    df = _read(out)
    assert list(df.columns) == ["subject", "trial", "StepLength", "Speed"]
    assert "Cadence" not in df.columns
    assert len(df) == 2


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


def test_non_csv_filename_rejected(tmp_path):
    set_schema(["subject"])
    a = pd.DataFrame({"subject": [1], "A": [1.0]})
    b = pd.DataFrame({"subject": [1], "B": [2.0]})
    with pytest.raises(ValueError, match="must be a string ending with '.csv'"):
        Merge(a, b).to_csv(str(tmp_path / "out.txt"))


def test_no_shared_schema_column_errors(tmp_path):
    """Constituents with no shared schema column cannot be inner-joined."""
    set_schema(["subject", "session"])
    a = pd.DataFrame({"subject": [1], "A": [1.0]})
    b = pd.DataFrame({"session": ["pre"], "B": [2.0]})
    with pytest.raises(ValueError, match="no shared schema column"):
        Merge(a, b).to_csv(str(tmp_path / "out.csv"))
