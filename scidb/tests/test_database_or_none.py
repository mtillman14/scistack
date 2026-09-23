"""One spelling of "which database does this call use" (cleanup-audit F5/F14).

``foreach`` spelled it as ``try: get_database() except Exception: pass`` in a
dozen places, so a database that could not be opened (a lock, above all)
looked exactly like "no database" and the run carried on with different
schema keys. And scidb read the dataset's schema keys back from scifor's
``set_schema`` COPY rather than from the database that holds them.
"""

import pytest

import scifor as _scifor

from scidb import configure_database
from scidb import database as _database
from scidb.database import _local, database_or_none, dataset_schema_keys_of
from scidb.foreach import _get_schema_keys


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "don.duckdb", ["subject", "session"])
    yield db
    _scifor.set_schema([])
    db.close()
    if hasattr(_local, "database"):
        delattr(_local, "database")


def test_nothing_configured_is_none():
    if hasattr(_local, "database"):
        delattr(_local, "database")
    assert database_or_none() is None
    assert dataset_schema_keys_of() == []


def test_a_passed_database_wins(db):
    assert database_or_none(db) is db


def test_a_failure_to_open_is_not_no_database(db, monkeypatch):
    def locked():
        raise OSError("IO Error: Could not set lock on file")

    monkeypatch.setattr(_database, "get_database", locked)
    with pytest.raises(OSError):
        database_or_none()
    with pytest.raises(OSError):
        _get_schema_keys(None)


def test_schema_keys_come_from_the_database_not_scifors_copy(db):
    _scifor.set_schema(["something", "else"])
    assert dataset_schema_keys_of() == ["subject", "session"]
    assert _get_schema_keys(None) == {"subject", "session"}
