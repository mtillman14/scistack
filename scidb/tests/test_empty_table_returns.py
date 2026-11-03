"""An EMPTY table returned by a for_each function saves nothing there
(``scifor.outputs.is_empty_table``), and ``save_batch`` never lets an empty
frame define the table or become a record with no data rows.

Before 2026-11-03, in one-record-per-combination mode (multi-row frames, or
several outputs), the empty frame reached ``save_batch`` as a record value:
not first -> a phantom record with no data rows; first, on a new variable ->
a zero-column reference schema (``CREATE TABLE ... (record_id ..., )``);
first, on a variable with records -> ``ColumnSetChangedError``.
"""

import logging

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb.model.exceptions import NotFoundError
from scifor.outputs import EMPTY_TABLE_DETAIL

SCHEMA = ["subject"]


class EmptyIn(BaseVariable):
    pass


class EmptyOut(BaseVariable):
    pass


class EmptyOutB(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "empty_returns.duckdb", SCHEMA)
    for s in (1, 2, 3):
        EmptyIn.save(np.arange(3.0) + s, db=db, subject=s)
    yield db
    _scifor.set_schema([])
    db.close()


def _first(x) -> float:
    return float(np.min(x))


def multi_row_empty_at_1(x):
    """Multi-row frames: one record per combination. Subject 1 (the FIRST
    item of the save batch) returns an empty frame."""
    if _first(x) == 1.0:
        return pd.DataFrame()
    return pd.DataFrame({"v": np.asarray(x, dtype=float)})


def multi_row_always(x):
    return pd.DataFrame({"v": np.asarray(x, dtype=float)})


def multi_row_empty_at_2(x):
    if _first(x) == 2.0:
        return pd.DataFrame({"v": pd.Series([], dtype=float)})
    return pd.DataFrame({"v": np.asarray(x, dtype=float)})


def one_row_empty_at_2(x):
    if _first(x) == 2.0:
        return pd.DataFrame()
    return pd.DataFrame({"v": [float(np.mean(x))]})


def _run(fn, db, caplog, outputs=(EmptyOut,)):
    with caplog.at_level(logging.INFO):
        for_each(fn, inputs={"x": EmptyIn}, outputs=list(outputs), subject=[1, 2, 3], db=db)


def test_empty_first_on_a_new_variable_saves_the_rest(db, caplog):
    _run(multi_row_empty_at_1, db, caplog)
    with pytest.raises(NotFoundError):
        EmptyOut.load(db=db, subject=1)
    for s in (2, 3):
        assert list(EmptyOut.load(db=db, subject=s).data["v"]) == [s, s + 1.0, s + 2.0]
    assert "returned an empty table at 1 combination(s)" in caplog.text


def test_empty_first_on_a_variable_with_records(db, caplog):
    _run(multi_row_always, db, caplog)
    caplog.clear()
    # Used to raise ColumnSetChangedError (the empty frame, first in the
    # batch, defined a zero-column schema against the stored "v" table).
    # A different function into the same output is a second variant, so
    # subjects 2 and 3 now hold two variants each (a bare load() there is
    # ambiguous by design); subject 1 keeps only the first run's record.
    _run(multi_row_empty_at_1, db, caplog)
    assert "ColumnSetChanged" not in caplog.text
    assert "returned an empty table at 1 combination(s)" in caplog.text
    assert list(EmptyOut.load(db=db, subject=1).data["v"]) == [1.0, 2.0, 3.0]


def test_empty_not_first_leaves_no_phantom_record(db, caplog):
    _run(multi_row_empty_at_2, db, caplog)
    with pytest.raises(NotFoundError):
        EmptyOut.load(db=db, subject=2)
    assert len(db.load_all_as_df(EmptyOut, layout="spread")) == 6  # 3 rows x subjects 1, 3


def test_empty_in_spread_mode_saves_nothing_there(db, caplog):
    _run(one_row_empty_at_2, db, caplog)
    with pytest.raises(NotFoundError):
        EmptyOut.load(db=db, subject=2)
    assert EmptyOut.load(db=db, subject=3).data["v"].iloc[0] == pytest.approx(4.0)
    assert "returned an empty table at 1 combination(s)" in caplog.text


def test_output_empty_everywhere_next_to_a_full_one(db, caplog):
    def a_never(x):
        return pd.DataFrame(), pd.DataFrame({"b": [float(np.max(x))]})

    _run(a_never, db, caplog, outputs=(EmptyOut, EmptyOutB))
    assert db.load_all_as_df(EmptyOut).empty
    assert len(db.load_all_as_df(EmptyOutB, layout="spread")) == 3
    assert "[save] EmptyOut: 0 records this run (3 no data)" in caplog.text
    assert "returned an empty table at 3 combination(s)" in caplog.text


# --- save_batch, for every other caller -----------------------------------


def test_save_batch_skips_empty_frames(db, caplog):
    full = pd.DataFrame({"v": [1.0, 2.0]})
    with caplog.at_level(logging.WARNING):
        rids = db.save_batch(
            EmptyOut,
            [
                (pd.DataFrame(), {"subject": 1}),
                (full, {"subject": 2}),
                (pd.DataFrame({"v": pd.Series([], dtype=float)}), {"subject": 3}),
            ],
        )
    assert rids[0] is None and rids[2] is None
    assert isinstance(rids[1], str)
    assert list(EmptyOut.load(db=db, subject=2).data["v"]) == [1.0, 2.0]
    skipped = [r.getMessage() for r in caplog.records if "SKIPPED record" in r.getMessage()]
    assert len(skipped) == 2
    assert all("the table is empty (no rows)" in m for m in skipped)


def test_save_batch_all_empty_saves_nothing(db):
    rids = db.save_batch(EmptyOut, [(pd.DataFrame(), {"subject": 1})])
    assert rids == [None]
    with pytest.raises(NotFoundError):
        EmptyOut.load(db=db, subject=1)


def test_detail_text_is_the_one_owner():
    assert EMPTY_TABLE_DETAIL == "returned an empty table"
