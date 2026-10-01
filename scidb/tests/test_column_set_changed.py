"""A variable keeps the columns of its first save, and says so clearly.

Regression for scidb.log 2026-10-01: GaitRiteSymmetry was first saved with
L_*/R_* columns. A later run produced A_*/U_* columns and failed with DuckDB's
``Binder Error: Table "GaitRiteSymmetry_data" does not have a column with
name "A_Idx_GR"``, which says nothing about why or what to do. The other
direction, a save with FEWER columns, succeeded silently and overwrote the
variable's column list. The owner is DatabaseManager._check_column_set.
"""

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import (
    BaseVariable,
    ColumnSetChangedError,
    OutputSaveError,
    configure_database,
    for_each,
)

SCHEMA = ["subject"]


class ColSetOut(BaseVariable):
    pass


class ColSetIn(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "colset.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


def _frame(*cols):
    return pd.DataFrame({c: [1.0, 2.0] for c in cols})


def _save(db, frame, subject=1):
    return db.save_batch(ColSetOut, [(frame, {"subject": subject})])


def _frames(db):
    """{subject: that record's DataFrame}. load_all_as_df returns one row per
    record, with the record's DataFrame in its "data" column."""
    loaded = db.load_all_as_df(ColSetOut)
    return {int(row["subject"]): row["data"] for _, row in loaded.iterrows()}


def test_same_columns_save_normally(db):
    _save(db, _frame("L_Idx", "R_Idx"), subject=1)
    _save(db, _frame("L_Idx", "R_Idx"), subject=2)
    frames = _frames(db)
    assert sorted(frames) == [1, 2]
    assert all(list(f.columns) == ["L_Idx", "R_Idx"] and len(f) == 2 for f in frames.values())


def test_new_columns_are_refused_with_a_clear_message(db):
    _save(db, _frame("L_Idx", "R_Idx"))
    with pytest.raises(ColumnSetChangedError) as err:
        _save(db, _frame("A_Idx", "U_Idx"), subject=2)
    e = err.value
    assert e.variable == "ColSetOut"
    assert e.added == ["A_Idx", "U_Idx"]
    assert e.removed == ["L_Idx", "R_Idx"]
    assert e.n_existing == 1
    msg = str(e)
    assert "keeps the columns of its first save" in msg
    assert "NEW variable" in msg and "delete ColSetOut's existing records" in msg


def test_fewer_columns_are_refused_too(db):
    """This used to succeed and overwrite the column list."""
    _save(db, _frame("L_Idx", "R_Idx"))
    with pytest.raises(ColumnSetChangedError) as err:
        _save(db, _frame("L_Idx"), subject=2)
    assert err.value.removed == ["R_Idx"] and err.value.added == []
    # The old record still loads with BOTH columns; nothing was written.
    frames = _frames(db)
    assert sorted(frames) == [1]
    assert list(frames[1].columns) == ["L_Idx", "R_Idx"]


def test_an_emptied_variable_takes_the_new_columns(db):
    _save(db, _frame("L_Idx", "R_Idx"))
    # What the Variants popup Delete leaves behind: the table, with no rows.
    db._duck._execute(f'DELETE FROM "{ColSetOut.table_name()}"')
    _save(db, _frame("A_Idx", "U_Idx"), subject=2)
    # Only the _data rows were deleted here (the real Delete also removes the
    # _record entries), so assert on the new record alone.
    frames = _frames(db)
    assert list(frames[2].columns) == ["A_Idx", "U_Idx"]


def test_a_for_each_run_carries_the_message(db):
    _save(db, _frame("L_Idx", "R_Idx"))
    ColSetIn.save(np.arange(2.0), db=db, subject=1)

    def produce_ua(x):
        return pd.DataFrame({"A_Idx": x, "U_Idx": x})

    with pytest.raises(OutputSaveError) as err:
        for_each(produce_ua, inputs={"x": ColSetIn}, outputs=[ColSetOut], subject=[1], db=db)
    assert "ColumnSetChangedError" in str(err.value)
    assert "keeps the columns of its first save" in str(err.value)
