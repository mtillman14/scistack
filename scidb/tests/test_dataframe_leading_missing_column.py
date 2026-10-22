"""A DataFrame column whose first cells are blank is typed by its first value.

Regression for scidb.log 2026-10-22: ``pandas.read_csv`` on a CERNER pull
returned IP_SPINAL_STIMULATION as an object column holding NaN (blank) then
'Y'. Column typing read row 0 only, so NaN typed it DOUBLE and the save
failed with ``Could not convert string 'Y' to DOUBLE`` — reported everywhere
as ``TransactionException ... Current transaction is aborted`` because the
``finally: unregister`` raised over the real error. Owners:
sciduckdb._representative_cell (typing) and SciDuck._unregister_frame
(cleanup that never masks).
"""

import duckdb
import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database
from sciduckdb import SciDuck
from sciduckdb.sciduckdb import infer_data_columns

SCHEMA = ["subject"]


class LeadingBlankTable(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "leading_blank.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


def _csv_like_frame():
    # What read_csv yields for a mostly-blank Y/N column next to numbers.
    return pd.DataFrame({
        "AGE": [41.0, 55.0, np.nan],
        "IP_SPINAL_STIMULATION": pd.Series([np.nan, "Y", np.nan], dtype=object),
    })


def test_object_column_typed_from_first_non_missing_cell():
    types, meta = infer_data_columns(_csv_like_frame())
    assert types["IP_SPINAL_STIMULATION"] == "VARCHAR"
    assert meta["columns"]["IP_SPINAL_STIMULATION"]["python_type"] == "str"
    # A typed float column is still DOUBLE even when row 0 is NaN.
    assert types["AGE"] == "DOUBLE"


def test_all_missing_object_column_keeps_row_zero():
    frame = pd.DataFrame({"X": pd.Series([None, None], dtype=object)})
    types, _ = infer_data_columns(frame)
    assert "X" in types


def test_save_batch_round_trips_leading_blank_column(db):
    db.save_batch(LeadingBlankTable, [(_csv_like_frame(), {"subject": 1})])
    loaded = db.load_all_as_df(LeadingBlankTable)
    frame = loaded.iloc[0]["data"]
    col = list(frame["IP_SPINAL_STIMULATION"])
    assert pd.isna(col[0]) and col[1] == "Y" and pd.isna(col[2])
    assert list(frame["AGE"])[:2] == [41.0, 55.0]


def test_failed_bulk_insert_in_transaction_raises_the_real_error(tmp_path):
    duck = SciDuck(str(tmp_path / "mask.duckdb"), dataset_schema=["subject"])
    try:
        duck._execute('CREATE TABLE t ("record_id" VARCHAR, "x" DOUBLE)')
        duck._begin()
        with pytest.raises(duckdb.ConversionException):
            duck._bulk_insert("t", ["record_id", "x"], [("r1", "Y")])
        duck._rollback()
    finally:
        duck.close()
