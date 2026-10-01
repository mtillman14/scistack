"""A run whose results cannot be saved fails; it does not report success.

Regression for scidb.log 2026-10-01: calculateSymmetryOneVector computed 420
results, the GaitRiteSymmetry batch insert failed (the table had no
``A_Idx_GR`` column), _save_results logged ERROR and returned normally, and
the MATLAB run was reported ``verdict=done``. It now raises OutputSaveError
after every output has been attempted.
"""

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, OutputSaveError, configure_database, for_each

SCHEMA = ["subject"]


class SaveErrIn(BaseVariable):
    pass


class SaveErrGood(BaseVariable):
    pass


class SaveErrBad(BaseVariable):
    pass


def double(x):
    return x * 2


def double_twice(x):
    return x * 2, x * 3


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "save_err.duckdb", SCHEMA)
    for s in (1, 2):
        SaveErrIn.save(np.arange(3.0), db=db, subject=s)
    yield db
    _scifor.set_schema([])
    db.close()


def _fail_for(db, monkeypatch, failing_cls):
    real = db.save_batch

    def save_batch(cls, items, **kwargs):
        if cls is failing_cls:
            raise RuntimeError('Table "x_data" does not have a column with name "A_Idx_GR"')
        return real(cls, items, **kwargs)

    monkeypatch.setattr(db, "save_batch", save_batch)


def test_a_failed_save_raises(db, monkeypatch):
    _fail_for(db, monkeypatch, SaveErrBad)
    with pytest.raises(OutputSaveError) as err:
        for_each(double, inputs={"x": SaveErrIn}, outputs=[SaveErrBad], subject=[1, 2], db=db)
    assert err.value.saved == 0
    assert len(err.value.failures) == 1
    assert "SaveErrBad: 2 record(s) not saved" in err.value.failures[0]
    assert "A_Idx_GR" in str(err.value)


def test_the_other_output_still_saves(db, monkeypatch):
    _fail_for(db, monkeypatch, SaveErrBad)
    with pytest.raises(OutputSaveError) as err:
        for_each(
            double_twice,
            inputs={"x": SaveErrIn},
            outputs=[SaveErrGood, SaveErrBad],
            subject=[1, 2],
            db=db,
        )
    assert err.value.saved == 2
    assert [f.split(":")[0] for f in err.value.failures] == ["SaveErrBad"]
    assert len(db.load_all_as_df(SaveErrGood)) == 2


def test_a_clean_save_does_not_raise(db):
    for_each(double, inputs={"x": SaveErrIn}, outputs=[SaveErrGood], subject=[1, 2], db=db)
    assert len(db.load_all_as_df(SaveErrGood)) == 2
