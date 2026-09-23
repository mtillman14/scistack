"""``list_versions`` derives branch params in ONE batched pass (cleanup-audit F3).

It used to walk each record's ancestry twice — once for the record's
direct-save kwargs, once for its branch params — reachable from MATLAB's
``list_versions`` as well as Python's.
"""

import numpy as np
import pytest

import scifor as _scifor

from scidb import BaseVariable, configure_database, provenance_query


class Versioned(BaseVariable):
    pass


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "list_versions.duckdb", ["subject"])
    yield db
    _scifor.set_schema([])
    db.close()


def _seed(db):
    Versioned.save(np.array([1.0]), db=db, subject="1", note="first")
    Versioned.save(np.array([2.0]), db=db, subject="1", note="second")


def test_versions_are_listed_with_their_save_kwargs(db):
    _seed(db)
    versions = db.list_versions(Versioned, subject="1")
    assert len(versions) == 2
    assert {v["schema"]["subject"] for v in versions} == {"1"}
    notes = {v["branch_params"].get("__save__.note") for v in versions}
    assert notes == {"first", "second"}


def test_no_record_is_walked_one_at_a_time(db, monkeypatch):
    _seed(db)
    expected = db.list_versions(Versioned, subject="1")

    def per_record_walk(*_a, **_k):
        raise AssertionError("list_versions walked a record's ancestry by itself")

    monkeypatch.setattr(provenance_query, "derived_branch_params", per_record_walk)
    assert db.list_versions(Versioned, subject="1") == expected
