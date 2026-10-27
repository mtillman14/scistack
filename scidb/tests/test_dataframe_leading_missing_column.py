"""DataFrame column types: judged over the whole save, decided by the first
values, refused only on a real contradiction.

Regression for scistack.log 2026-10-22: ``pandas.read_csv`` on a CERNER pull
returned IP_SPINAL_STIMULATION as an object column holding NaN (blank) then
'Y'. Column typing read row 0 only, so NaN typed it DOUBLE and the save
failed with ``Could not convert string 'Y' to DOUBLE`` — reported everywhere
as ``TransactionException`` because the ``finally: unregister`` raised over
the real error. The re-run spread the CSV one record per subject with the
first subject blank. Owners: sciduckdb ``_batch_column_type`` (typing a
save), ``_str_to_storage`` (text cells), ``column_type_change`` (the rule),
scidb ``DatabaseManager._reconcile_column_types`` (against stored data),
``SciDuck._unregister_frame`` (cleanup that never masks).

Agreed rules (user, 2026-10-22): an all-empty column is undecided; the first
save with values decides (numeric -> DOUBLE, any string -> VARCHAR); a
contradiction is refused (ColumnTypeChangedError), except integer ->
DOUBLE widens (D1) and numbers into a text column are stored as text (D2).
"""

import duckdb
import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, ColumnTypeChangedError, configure_database
from sciduckdb import SciDuck, column_type_change
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


def _obj(*values):
    return pd.Series(list(values), dtype=object)


def _csv_like_frame():
    # What read_csv yields for a mostly-blank Y/N column next to numbers.
    return pd.DataFrame({
        "AGE": [41.0, 55.0, np.nan],
        "IP_SPINAL_STIMULATION": _obj(np.nan, "Y", np.nan),
    })


def _save(db, frame, subject):
    return db.save_batch(LeadingBlankTable, [(frame, {"subject": subject})])


def _frames(db):
    """{subject: that record's DataFrame}."""
    loaded = db.load_all_as_df(LeadingBlankTable)
    return {int(row["subject"]): row["data"] for _, row in loaded.iterrows()}


def _stored_type(db, col):
    return db.stored_data_column_types(LeadingBlankTable.table_name())[col]


def _stored_meta(db, col):
    return db.get_dtype_meta("LeadingBlankTable")["columns"][col]


# ---------------------------------------------------------------------------
# Typing one save (sciduckdb)
# ---------------------------------------------------------------------------


def test_object_column_typed_from_its_values_not_row_zero():
    types, meta = infer_data_columns(_csv_like_frame())
    assert types["IP_SPINAL_STIMULATION"] == "VARCHAR"
    assert meta["columns"]["IP_SPINAL_STIMULATION"]["python_type"] == "str"
    # A typed float column is still DOUBLE even when row 0 is NaN.
    assert types["AGE"] == "DOUBLE"


def test_column_blank_in_first_record_typed_from_later_record():
    first = pd.DataFrame({"X": _obj(np.nan)})
    later = pd.DataFrame({"X": _obj(np.nan, "Y")})
    types, meta = infer_data_columns(first, more_frames=[later])
    assert types["X"] == "VARCHAR"
    assert meta["columns"]["X"] == {"python_type": "str"}


@pytest.mark.parametrize(
    "column",
    [_obj(None, None), pd.Series([np.nan, np.nan], dtype=float)],
    ids=["object", "float64"],
)
def test_all_empty_column_is_undecided(column):
    types, meta = infer_data_columns(pd.DataFrame({"X": column}))
    assert types["X"] == "VARCHAR"
    assert meta["columns"]["X"].get("empty") is True


def test_numbers_and_strings_across_records_make_text():
    first = pd.DataFrame({"X": [2.5]})
    later = pd.DataFrame({"X": _obj("Y")})
    types, _ = infer_data_columns(first, more_frames=[later])
    assert types["X"] == "VARCHAR"


def test_integers_with_blanks_make_double():
    types, _ = infer_data_columns(pd.DataFrame({"X": _obj(np.nan, 1, 2)}))
    assert types["X"] == "DOUBLE"


@pytest.mark.parametrize(
    "stored, incoming, expected",
    [
        ("DOUBLE", "DOUBLE", "same"),
        ("DOUBLE", "BIGINT", "same"),
        ("BIGINT", "DOUBLE", "widen"),
        ("VARCHAR", "DOUBLE", "as_text"),
        ("VARCHAR", "BIGINT", "as_text"),
        ("DOUBLE", "VARCHAR", "refuse"),
        ("DOUBLE", "DOUBLE[]", "refuse"),
        ("TIMESTAMP", "DOUBLE", "refuse"),
    ],
)
def test_column_type_change_rule(stored, incoming, expected):
    assert column_type_change(stored, incoming) == expected


# ---------------------------------------------------------------------------
# One save, round trip
# ---------------------------------------------------------------------------


def test_save_batch_round_trips_leading_blank_column(db):
    _save(db, _csv_like_frame(), 1)
    frame = _frames(db)[1]
    col = list(frame["IP_SPINAL_STIMULATION"])
    assert pd.isna(col[0]) and col[1] == "Y" and pd.isna(col[2])
    assert list(frame["AGE"])[:2] == [41.0, 55.0]


def test_save_batch_spread_records_first_subject_blank(db):
    # Re-run of 2026-10-22: rows spread one record per subject, and subject 1
    # had no IP_SPINAL_STIMULATION value.
    items = [
        (pd.DataFrame({"AGE": [41.0], "IP_SPINAL_STIMULATION": _obj(np.nan)}),
         {"subject": 1}),
        (pd.DataFrame({"AGE": [55.0], "IP_SPINAL_STIMULATION": _obj("Y")}),
         {"subject": 2}),
    ]
    db.save_batch(LeadingBlankTable, items)
    frames = _frames(db)
    assert pd.isna(list(frames[1]["IP_SPINAL_STIMULATION"])[0])
    assert list(frames[2]["IP_SPINAL_STIMULATION"]) == ["Y"]


def test_all_empty_column_round_trips_as_nan(db):
    _save(db, pd.DataFrame({"X": _obj(None, None)}), 1)
    col = _frames(db)[1]["X"]
    assert col.isna().all()


def test_text_column_never_stores_the_text_nan(db):
    _save(db, _csv_like_frame(), 1)
    n = db._duck._fetchall(
        f'SELECT COUNT(*) FROM "{LeadingBlankTable.table_name()}" '
        f"WHERE \"IP_SPINAL_STIMULATION\" = 'nan'"
    )[0][0]
    assert n == 0


# ---------------------------------------------------------------------------
# Across saves (scidb _reconcile_column_types)
# ---------------------------------------------------------------------------


def test_blank_then_text_decides_text(db):
    _save(db, pd.DataFrame({"X": _obj(np.nan, np.nan)}), 1)
    _save(db, pd.DataFrame({"X": _obj(np.nan, "Y")}), 2)
    assert _stored_type(db, "X") == "VARCHAR"
    assert _stored_meta(db, "X") == {"python_type": "str"}
    frames = _frames(db)
    assert frames[1]["X"].isna().all()
    assert pd.isna(list(frames[2]["X"])[0]) and list(frames[2]["X"])[1] == "Y"


def test_blank_then_numbers_decides_double(db):
    _save(db, pd.DataFrame({"X": _obj(np.nan, np.nan)}), 1)
    _save(db, pd.DataFrame({"X": [2.5, 3.0]}), 2)
    assert _stored_type(db, "X") == "DOUBLE"
    assert list(_frames(db)[2]["X"]) == [2.5, 3.0]


def test_numbers_then_text_is_refused_and_nothing_saved(db):
    _save(db, pd.DataFrame({"X": [2.5, 3.0]}), 1)
    with pytest.raises(ColumnTypeChangedError) as err:
        _save(db, pd.DataFrame({"X": _obj("N/A", 4.0)}), 2)
    msg = str(err.value)
    assert "X" in msg and "DOUBLE" in msg and "'N/A'" in msg
    assert sorted(_frames(db)) == [1]
    assert _stored_type(db, "X") == "DOUBLE"


def test_numbers_then_blank_keeps_double(db):
    _save(db, pd.DataFrame({"X": [2.5, 3.0]}), 1)
    _save(db, pd.DataFrame({"X": [np.nan, np.nan]}), 2)
    assert _stored_type(db, "X") == "DOUBLE"
    assert _stored_meta(db, "X") == {"python_type": "float"}
    assert _frames(db)[2]["X"].isna().all()


def test_integers_then_fractions_widen_to_double(db):
    # D1: no stored number changes value.
    _save(db, pd.DataFrame({"X": [1, 2]}), 1)
    assert _stored_type(db, "X") == "BIGINT"
    _save(db, pd.DataFrame({"X": [2.5]}), 2)
    assert _stored_type(db, "X") == "DOUBLE"
    frames = _frames(db)
    assert list(frames[1]["X"]) == [1.0, 2.0]
    assert list(frames[2]["X"]) == [2.5]


def test_text_then_numbers_stored_as_text(db):
    # D2: numbers may go into a text column.
    _save(db, pd.DataFrame({"X": _obj("Y", "N")}), 1)
    _save(db, pd.DataFrame({"X": [3.0, 4.5]}), 2)
    assert _stored_type(db, "X") == "VARCHAR"
    assert list(_frames(db)[2]["X"]) == ["3", "4.5"]


def test_single_save_path_reconciles_too(db):
    _save(db, pd.DataFrame({"X": _obj(np.nan)}), 1)
    db.save(LeadingBlankTable(pd.DataFrame({"X": _obj("Y")})), {"subject": 2})
    assert _stored_type(db, "X") == "VARCHAR"
    assert _stored_meta(db, "X") == {"python_type": "str"}


# ---------------------------------------------------------------------------
# The real error reaches the caller
# ---------------------------------------------------------------------------


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
