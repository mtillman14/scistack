"""``RecordStore`` (``scidb.store.record_store``): the one owner of the storage SQL.

Guards (P4 stage 5, design review 04a B2 / 06 PD1):

* the long frame plotting reads holds every non-excluded record, schema keys
  as TEXT, and — in its metadata-only form — no data column in the query;
* the payload only ever travels through ``fetchdf``, never ``fetchall``
  (``.claude/plot-at-scale-plan.md`` §7: 18x, a Python float per sample);
* the catalog reads (table, columns, dtype, occupied keys, a sampled cell,
  distinct levels) answer from the stored tables;
* ``save_batch``'s inserts run through ``write_batch_rows`` on the same store
  ``db.record_store`` hands out, rebuilt when the connection is replaced.
"""

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database
from scidb.store.record_store import RecordStore


class StoreSignal(BaseVariable):
    pass


class StoreScalar(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "store.duckdb", ["subject", "trial"])
    for s in (1, 2):
        for t in (1, 2):
            StoreSignal.save(np.arange(4.0) * s + t, db=database, subject=s, trial=t)
            StoreScalar.save(float(s * 10 + t), db=database, subject=s, trial=t)
    yield database
    _scifor.set_schema([])
    database.close()


def _spy(monkeypatch, duck, name):
    seen: list = []
    real = getattr(duck, name)

    def spy(sql, *args, **kwargs):
        seen.append(sql)
        return real(sql, *args, **kwargs)

    monkeypatch.setattr(duck, name, spy)
    return seen


def test_db_hands_out_one_store_per_connection(db):
    assert isinstance(db.record_store, RecordStore)
    assert db.record_store is db.record_store
    assert db.record_store.connection is db._duck


def test_catalog(db):
    store = db.record_store
    assert store.schema_keys() == ["subject", "trial"]
    assert {"StoreSignal", "StoreScalar"} <= set(store.variable_names())
    assert store.table_name("StoreSignal")
    assert list(store.stored_column_types("StoreScalar"))
    assert store.dtype_meta("StoreScalar") is not None
    assert store.dtype_meta("NoSuchVariable") is None
    assert store.occupied_keys("StoreScalar") == ["subject", "trial"]


def test_long_frame_reads_every_record_with_text_keys(db, monkeypatch):
    store = db.record_store
    (column,) = list(store.stored_column_types("StoreScalar"))
    fetchdf = _spy(monkeypatch, db._duck, "fetchdf")
    frame = store.long_frame("StoreScalar", [column])
    assert len(frame) == 4
    assert list(frame.columns) == ["record_id", column, "subject", "trial"]
    assert sorted(frame["subject"].astype(str)) == ["1", "1", "2", "2"]
    assert len(fetchdf) == 1 and "CAST(" in fetchdf[0]


def test_metadata_only_long_frame_selects_no_data_column(db, monkeypatch):
    store = db.record_store
    columns = list(store.stored_column_types("StoreSignal"))
    fetchdf = _spy(monkeypatch, db._duck, "fetchdf")
    frame = store.long_frame("StoreSignal")
    assert list(frame.columns) == ["record_id", "subject", "trial"]
    assert len(frame) == 4
    for column in columns:
        assert f'"{column}"' not in fetchdf[0]


def test_payload_never_goes_through_fetchall(db, monkeypatch):
    store = db.record_store
    columns = list(store.stored_column_types("StoreSignal"))
    fetchall = _spy(monkeypatch, db._duck, "fetchall")
    store.long_frame("StoreSignal", columns)
    for sql in fetchall:
        for column in columns:
            assert f't."{column}"' not in sql, sql


def test_sample_and_levels(db):
    store = db.record_store
    (column,) = list(store.stored_column_types("StoreScalar"))
    assert store.sample_cell("StoreScalar", column) is not None
    levels = store.column_levels("StoreScalar", column, limit=10)
    assert sorted(levels, key=float) == ["11.0", "12.0", "21.0", "22.0"]
    assert len(store.column_levels("StoreScalar", column, limit=2)) == 2


def test_save_batch_inserts_through_the_store(db, monkeypatch):
    calls = []
    real = db.record_store.write_batch_rows
    monkeypatch.setattr(
        db.record_store, "write_batch_rows", lambda rows: calls.append(rows) or real(rows)
    )
    rids = db.save_batch(StoreScalar, [(99.0, {"subject": 3, "trial": 1})])
    assert len(calls) == 1 and calls[0].record_ids == rids
    assert db.record_store.occupied_keys("StoreScalar") == ["subject", "trial"]
