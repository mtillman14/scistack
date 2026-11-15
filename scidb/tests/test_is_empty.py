"""``scidb.is_empty``: the one owner of "does this dataset hold data"
(``scidb.store.emptiness``; .claude/plan-change-schema-keys.md).

Empty = no variable holds a saved record. Run history (failure outcomes, the
run log, internal record types) is not data.
"""

from __future__ import annotations

import pandas as pd
import pytest
import scifor as _scifor
from scifor import PathInput

import scidb
from scidb import BaseVariable, OutputSaveError, configure_database, for_each
from scidb.store.clear_data import clear_data, clear_plan


class IeDemo(BaseVariable):
    pass


def ie_read(path):
    return pd.read_csv(path)


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "empty.duckdb", ["subject", "session"])
    yield db
    _scifor.set_schema([])
    db.close()


def test_a_fresh_database_is_empty(db):
    assert scidb.is_empty(db) is True
    assert scidb.is_empty() is True  # the configured database


def test_a_refused_duplicate_save_leaves_it_empty(db, tmp_path):
    """The trap: the failed first load writes run history, not data."""
    path = tmp_path / "t.csv"
    path.write_text("subject,group\nS1,a\nS1,b\n")
    with pytest.raises(OutputSaveError):
        for_each(ie_read, {"path": PathInput(str(path), name="P")}, [IeDemo])
    assert scidb.is_empty(db) is True


def test_one_good_save_makes_it_not_empty(db):
    IeDemo.save(1.0, db=db, subject="S1")
    assert scidb.is_empty(db) is False


def test_clear_data_makes_it_empty_again(db):
    IeDemo.save(1.0, db=db, subject="S1")
    clear_data(db, "test")
    assert scidb.is_empty(db) is True


@pytest.mark.parametrize("saved", [False, True])
def test_clear_plan_agrees_with_is_empty(db, saved):
    if saved:
        IeDemo.save(1.0, db=db, subject="S1")
    assert (clear_plan(db).total_records == 0) == scidb.is_empty(db)
