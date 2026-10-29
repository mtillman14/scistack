"""A function with several outputs that each return a DataFrame saves each
output with ITS OWN frame.

Found reading scifor's result assembly (2026-10-28, Stage 1 of
.claude/plan-for-columns-groups.md): when every output is a DataFrame of at
most one row, ``_results_to_output_dataframe`` spreads them, concatenating
all outputs side by side into one wide row. The output names then are not
columns of the result table, so ``_save_results`` takes its flatten branch
for EACH output and builds that output's record from EVERY data column.

for_columns split mode (one group per output, groups usually returning
small frames) is exactly this shape, so these pin what it relies on.
"""

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each

SCHEMA = ["subject"]


class FramesIn(BaseVariable):
    pass


class FramesA(BaseVariable):
    pass


class FramesB(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "multi_output_frames.duckdb", SCHEMA)
    for s in (1, 2):
        FramesIn.save(np.arange(3.0) + s, db=db, subject=s)
    yield db
    _scifor.set_schema([])
    db.close()


def one_row_frames(x):
    return (
        pd.DataFrame({"a_mean": [float(np.mean(x))]}),
        pd.DataFrame({"b_max": [float(np.max(x))], "b_min": [float(np.min(x))]}),
    )


def one_row_frames_same_column(x):
    return (
        pd.DataFrame({"value": [float(np.mean(x))]}),
        pd.DataFrame({"value": [float(np.max(x))]}),
    )


def multi_row_frames(x):
    return (
        pd.DataFrame({"a": list(x)}),
        pd.DataFrame({"b": list(x * 10), "c": list(x * 100)}),
    )


def _only(var, db, subject):
    rec = var.load(db=db, subject=subject)
    assert not isinstance(rec, list), f"{var.__name__} at subject={subject}: {len(rec)} records"
    return rec.data


def test_one_row_frames_keep_their_own_columns(db):
    for_each(one_row_frames, inputs={"x": FramesIn}, outputs=[FramesA, FramesB], subject=[1, 2], db=db)

    a = _only(FramesA, db, 1)
    b = _only(FramesB, db, 1)
    assert list(a.columns) == ["a_mean"]
    assert list(b.columns) == ["b_max", "b_min"]
    assert a["a_mean"].iloc[0] == pytest.approx(2.0)
    assert b["b_max"].iloc[0] == pytest.approx(3.0)
    assert b["b_min"].iloc[0] == pytest.approx(1.0)


def test_one_row_frames_with_the_same_column_name(db):
    for_each(
        one_row_frames_same_column,
        inputs={"x": FramesIn},
        outputs=[FramesA, FramesB],
        subject=[1, 2],
        db=db,
    )

    assert _only(FramesA, db, 2)["value"].iloc[0] == pytest.approx(3.0)
    assert _only(FramesB, db, 2)["value"].iloc[0] == pytest.approx(4.0)


def test_multi_row_frames_keep_their_own_columns(db):
    for_each(multi_row_frames, inputs={"x": FramesIn}, outputs=[FramesA, FramesB], subject=[1, 2], db=db)

    a = _only(FramesA, db, 1)
    b = _only(FramesB, db, 1)
    assert list(a.columns) == ["a"]
    assert list(b.columns) == ["b", "c"]
    assert list(a["a"]) == pytest.approx([1.0, 2.0, 3.0])
    assert list(b["c"]) == pytest.approx([100.0, 200.0, 300.0])
