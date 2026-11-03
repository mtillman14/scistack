"""Tests for scifor.Merge.as_df() — inner-join conversion to a pandas DataFrame.

as_df() shares its join/filter core with to_csv() (see csv_export.merge_to_dataframe),
but returns the DataFrame in-memory instead of writing a file.
"""

import pandas as pd
import pytest

from scifor import Col, ColumnSelection, Merge, set_schema


def setup_function():
    set_schema([])


def test_as_df_matches_to_csv_output(tmp_path):
    """as_df() returns the same frame to_csv() would write."""
    set_schema(["subject", "trial"])
    step = pd.DataFrame(
        {"subject": [1, 1, 2], "trial": [1, 2, 1], "StepLength": [0.5, 0.6, 0.7]}
    )
    speed = pd.DataFrame(
        {"subject": [1, 1, 2], "trial": [1, 2, 1], "Speed": [1.1, 1.2, 1.3]}
    )
    merged = Merge(step, speed)

    df = merged.as_df()
    assert list(df.columns) == ["subject", "trial", "StepLength", "Speed"]
    assert len(df) == 3

    out = tmp_path / "gait.csv"
    merged.to_csv(str(out))
    written = pd.read_csv(out)
    pd.testing.assert_frame_equal(
        df.reset_index(drop=True), written.reset_index(drop=True)
    )


def test_as_df_rows_on_one_side_only_raise():
    set_schema(["subject", "trial"])
    step = pd.DataFrame(
        {"subject": [1, 1, 2], "trial": [1, 2, 1], "StepLength": [0.5, 0.6, 0.7]}
    )
    speed = pd.DataFrame(
        {"subject": [1, 2, 3], "trial": [1, 1, 1], "Speed": [1.1, 1.3, 9.9]}
    )
    with pytest.raises(ValueError, match="Merge would drop rows"):
        Merge(step, speed).as_df()


def test_as_df_where_filter():
    set_schema(["subject", "trial"])
    step = pd.DataFrame(
        {"subject": [1, 1, 2], "trial": [1, 2, 1], "StepLength": [0.5, 0.6, 0.7]}
    )
    speed = pd.DataFrame(
        {"subject": [1, 1, 2], "trial": [1, 2, 1], "Speed": [1.1, 1.2, 1.3]}
    )
    df = Merge(step, speed).as_df(where=Col("StepLength") > 0.55)
    assert set(df.StepLength) == {0.6, 0.7}


def test_as_df_metadata_filter():
    set_schema(["subject", "trial"])
    step = pd.DataFrame(
        {"subject": [1, 2, 3], "trial": [1, 1, 1], "StepLength": [0.5, 0.6, 0.7]}
    )
    speed = pd.DataFrame(
        {"subject": [1, 2, 3], "trial": [1, 1, 1], "Speed": [1.1, 1.2, 1.3]}
    )
    df = Merge(step, speed).as_df(subject=[1, 3])
    assert set(df.subject) == {1, 3}


def test_as_df_column_selection():
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
    df = Merge(ColumnSelection(gait, ["StepLength"]), speed).as_df()
    assert list(df.columns) == ["subject", "trial", "StepLength", "Speed"]
    assert "Cadence" not in df.columns


def test_as_df_returns_dataframe_type():
    """No filename needed; returns a real DataFrame."""
    set_schema(["subject"])
    a = pd.DataFrame({"subject": [1, 2], "A": [1.0, 2.0]})
    b = pd.DataFrame({"subject": [1, 2], "B": [3.0, 4.0]})
    df = Merge(a, b).as_df()
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["subject", "A", "B"]
