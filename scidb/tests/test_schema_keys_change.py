"""Changing the schema keys of an EMPTY dataset (GitHub issue #10;
``scidb.store.schema_keys_change``; .claude/plan-change-schema-keys.md)."""

from __future__ import annotations

import pandas as pd
import pytest
import scifor as _scifor
from scifor import PathInput

from scidb import (
    BaseVariable,
    OutputSaveError,
    SchemaKeysMismatchError,
    configure_database,
    for_each,
)
from scidb.store.exclusions import exclude_schema, list_exclusions
from scidb.store.schema_keys_change import (
    SchemaKeysChangeRefused,
    change_schema_keys,
    propose_schema_keys,
    validate_schema_keys,
)


class SkDemo(BaseVariable):
    pass


def sk_read(path):
    return pd.read_csv(path)


@pytest.fixture
def path(tmp_path):
    _scifor.set_schema([])
    yield tmp_path / "keys.duckdb"
    _scifor.set_schema([])


def _open(path, keys):
    _scifor.set_schema([])
    return configure_database(path, keys)


def _csv(tmp_path):
    p = tmp_path / "t.csv"
    p.write_text("subject,visit,score\nS1,1,10\nS1,2,11\nS2,1,12\n")
    return PathInput(str(p), name="SkPath")


# --- the pure rules ------------------------------------------------------------


def test_propose_is_path_input_keys_then_new_data_columns_in_order():
    assert propose_schema_keys([["subject", "visit"], ["subject", "task"]], ["score", "visit", "age"]) == [
        {"key": "subject", "source": "path_input"},
        {"key": "visit", "source": "path_input"},
        {"key": "task", "source": "path_input"},
        {"key": "score", "source": "data_column"},
        {"key": "age", "source": "data_column"},
    ]


@pytest.mark.parametrize(
    "bad", [[], ["a", "a"], ["schema_id"], ["schema_level"], ["__x"], [""], ['a"b']]
)
def test_validate_rejects_bad_keys(bad):
    with pytest.raises(ValueError):
        validate_schema_keys(bad)


def test_validate_accepts_good_keys():
    assert validate_schema_keys(["subject", "visit"]) == ["subject", "visit"]


# --- the change ----------------------------------------------------------------


def test_change_then_the_colliding_csv_saves(path, tmp_path):
    db = _open(path, ["subject"])
    pi = _csv(tmp_path)
    with pytest.raises(OutputSaveError):
        for_each(sk_read, {"path": pi}, [SkDemo])
    result = change_schema_keys(db, ["subject", "visit"], reason="test")
    assert result.old_keys == ["subject"]
    assert result.new_keys == ["subject", "visit"]

    db = _open(path, ["subject", "visit"])
    for_each(sk_read, {"path": pi}, [SkDemo])
    n = db._duck.fetchall("SELECT COUNT(*) FROM _record WHERE type = 'SkDemo'")[0][0]
    assert n > 0
    db.close()


def test_non_empty_is_refused_and_nothing_changes(path):
    db = _open(path, ["subject"])
    SkDemo.save(1.0, db=db, subject="S1")
    with pytest.raises(SchemaKeysChangeRefused, match="already holds data"):
        change_schema_keys(db, ["subject", "visit"])
    cols = [r[0] for r in db._duck.fetchall(
        "SELECT column_name FROM information_schema.columns WHERE table_name='_schema' ORDER BY ordinal_position")]
    assert cols == ["schema_id", "schema_level", "subject"]
    db.close()


def test_reopening_with_the_old_keys_raises_a_mismatch_naming_both(path):
    db = _open(path, ["subject"])
    change_schema_keys(db, ["subject", "visit"])
    with pytest.raises(SchemaKeysMismatchError) as info:
        _open(path, ["subject"])
    assert info.value.passed == ["subject"]
    assert info.value.existing == ["subject", "visit"]
    assert "['subject']" in str(info.value) and "['subject', 'visit']" in str(info.value)


def test_a_rename_carries_the_key_type_and_the_exclusions(path):
    db = _open(path, ["subject", "visit"])
    db.set_schema_key_type("visit", "numeric")
    exclude_schema("bad", db=db, subject="S9")
    change_schema_keys(db, ["subject", "session"], renames={"visit": "session"})
    db = _open(path, ["subject", "session"])
    assert db.dataset_schema_key_types.get("session") == "numeric"
    assert "visit" not in db.dataset_schema_key_types
    assert "S9" in list_exclusions(db=db)["subject"].tolist()
    db.close()


def test_deleting_a_key_an_exclusion_names_is_refused(path):
    db = _open(path, ["subject", "visit"])
    exclude_schema("bad", db=db, visit="2")
    with pytest.raises(SchemaKeysChangeRefused, match="visit"):
        change_schema_keys(db, ["subject"])
    db.close()


def test_reorder_only(path):
    db = _open(path, ["subject", "visit"])
    change_schema_keys(db, ["visit", "subject"])
    db = _open(path, ["visit", "subject"])
    assert db.dataset_schema_keys == ["visit", "subject"]
    db.close()


def test_invalid_keys_are_refused(path):
    db = _open(path, ["subject"])
    with pytest.raises(SchemaKeysChangeRefused, match="schema_id"):
        change_schema_keys(db, ["subject", "schema_id"])
    db.close()


# --- the mismatch message ------------------------------------------------------


@pytest.mark.parametrize(
    "passed",
    [["wrong"], ["subject"], ["subject", "visit", "extra"], ["visit", "subject"]],
)
def test_mismatch_message_names_both_lists(path, passed):
    db = _open(path, ["subject", "visit"])
    db.close()
    with pytest.raises(SchemaKeysMismatchError) as info:
        _open(path, passed)
    text = str(info.value)
    assert str(passed) in text and "['subject', 'visit']" in text
    assert isinstance(info.value, ValueError)
    if sorted(passed) == ["subject", "visit"]:
        assert "order differs" in text
