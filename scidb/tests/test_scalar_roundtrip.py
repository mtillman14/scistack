"""A saved Python scalar loads back as that Python scalar, and re-saving it
creates no new record (P4 stage 10).

Before: ``sciduckdb.restore_types`` assigned the restored list back into a
DataFrame column, pandas re-inferred a numpy dtype, and ``True`` / ``42``
loaded as ``np.True_`` / ``np.int64``. ``x is True`` was False,
``json.dumps`` raised, and — because ``canonical_hash`` takes the ndarray
branch for numpy ints and bools — a load-and-re-save forked a new record of
identical data. Round-tripping data must never create a variant.
"""

import json

import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database


class RoundTripScalar(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "scalars.duckdb", ["subject"])
    yield database
    _scifor.set_schema([])
    database.close()


def _record_ids(db):
    return sorted(
        r[0]
        for r in db._duck.fetchall(
            "SELECT record_id FROM _record WHERE type = 'RoundTripScalar'"
        )
    )


@pytest.mark.parametrize("value", [True, False, 42, 3.14, "text"], ids=repr)
def test_loads_back_as_the_python_scalar(db, value):
    RoundTripScalar.save(value, db=db, subject=1)
    loaded = RoundTripScalar.load(db=db, subject=1).data
    assert type(loaded) is type(value)
    assert loaded == value
    if isinstance(value, bool):
        assert loaded is value
    json.dumps(loaded)


@pytest.mark.parametrize("value", [True, 42, 3.14], ids=repr)
def test_resave_of_a_loaded_scalar_keeps_its_record_id(db, value):
    RoundTripScalar.save(value, db=db, subject=1)
    before = _record_ids(db)
    loaded = RoundTripScalar.load(db=db, subject=1).data
    RoundTripScalar.save(loaded, db=db, subject=1)
    assert _record_ids(db) == before
