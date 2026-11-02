"""A for_each save writes its data and its provenance graph together, or not at all.

Regression for design review 04b W1 (plan-tidy P0): record_run ran in a
second transaction inside ``except Exception: Log.error(...)``, so a failed
graph write left data rows with no invocation edges and the run reported
success. Now a provenance failure raises OutputSaveError, and (decision D9)
the call's one transaction rolls back the data with it.
"""

import numpy as np
import pytest

import scidb.provenance_save as provenance_save
import scifor as _scifor
from scidb import BaseVariable, OutputSaveError, configure_database, for_each

SCHEMA = ["subject"]


class ProvFailIn(BaseVariable):
    pass


class ProvFailOut(BaseVariable):
    pass


class ProvFailOther(BaseVariable):
    pass


def double(x):
    return x * 2


def double_twice(x):
    return x * 2, x * 3


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "prov_fail.duckdb", SCHEMA)
    for s in (1, 2):
        ProvFailIn.save(np.arange(3.0), db=db, subject=s)
    yield db
    _scifor.set_schema([])
    db.close()


def _count(db, sql: str, params=None) -> int:
    return db._duck._fetchall(sql, params or [])[0][0]


def _records_of(db, type_name: str) -> int:
    return _count(db, "SELECT COUNT(*) FROM _record WHERE type = ?", [type_name])


def _boom(*args, **kwargs):
    raise RuntimeError("boom")


def _run(db):
    for_each(double, inputs={"x": ProvFailIn}, outputs=[ProvFailOut], subject=[1, 2], db=db)


def test_record_run_failure_fails_the_run(db, monkeypatch):
    monkeypatch.setattr(provenance_save, "record_run", _boom)
    with pytest.raises(OutputSaveError) as err:
        _run(db)
    assert err.value.saved == 0
    assert err.value.failures == ["provenance graph not recorded: RuntimeError: boom"]
    assert "provenance graph not recorded" in str(err.value)
    # SciStack's own write broke: not the user's values, not their setup.
    assert err.value.blame == "internal"


def test_record_run_failure_rolls_back_data(db, monkeypatch):
    monkeypatch.setattr(provenance_save, "record_run", _boom)
    with pytest.raises(OutputSaveError):
        _run(db)
    assert _records_of(db, "ProvFailOut") == 0


def test_graph_write_failure_inside_the_transaction_rolls_back_data(db, monkeypatch):
    """A failure deep inside record_run's own (nested) transaction: its
    rollback only dooms the outer one, which then discards the data too."""
    real = db._duck._bulk_insert

    def bulk_insert(table, *args, **kwargs):
        if table == "_run_invocation":
            raise RuntimeError("graph insert failed")
        return real(table, *args, **kwargs)

    monkeypatch.setattr(db._duck, "_bulk_insert", bulk_insert)
    with pytest.raises(OutputSaveError) as err:
        _run(db)
    assert "graph insert failed" in str(err.value)
    assert _records_of(db, "ProvFailOut") == 0
    assert db._duck._tx_depth == 0


def test_a_rolled_back_first_save_registers_the_type_next_time(db, monkeypatch):
    """The rollback listener drops the registered-type cache, so a type first
    saved inside a rolled-back transaction is registered again by the next
    save instead of being trusted from memory."""
    with monkeypatch.context() as m:
        m.setattr(provenance_save, "record_run", _boom)
        with pytest.raises(OutputSaveError):
            _run(db)
    _run(db)
    assert _records_of(db, "ProvFailOut") == 2
    assert _count(
        db, "SELECT COUNT(*) FROM _registered_types WHERE type_name = ?", ["ProvFailOut"]
    ) == 1


def test_no_record_without_output_edge(db):
    """Invariant: every computed record has the edge that says which
    invocation produced it."""
    for_each(
        double_twice,
        inputs={"x": ProvFailIn},
        outputs=[ProvFailOut, ProvFailOther],
        subject=[1, 2],
        db=db,
    )
    orphans = db._duck._fetchall(
        "SELECT r.record_id, r.type FROM _record r "
        "WHERE r.type IN ('ProvFailOut', 'ProvFailOther') AND NOT EXISTS ("
        "  SELECT 1 FROM _invocation_output o WHERE o.output_record_id = r.record_id)"
    )
    assert orphans == []
    assert _records_of(db, "ProvFailOut") == 2
    assert _records_of(db, "ProvFailOther") == 2
