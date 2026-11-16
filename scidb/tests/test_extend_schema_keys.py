"""Add keys to a dataset that holds data; change keys no record names.

Plan: .claude/plan-branched-schema-keys.md, stage extend_tree. The argument:
a NULL in a key column already means "this record does not name this key", so
a NEW key changes the meaning of nothing that exists, and a key no record or run
names can change without changing any record. Used keys stay fixed.
"""

from __future__ import annotations

import pytest
import scifor as _scifor

from scidb import BaseVariable, configure_database
from scidb.provenance.expectation import marks, mark_expectation
from scidb.store.database import clear_current_database
from scidb.store.emptiness import key_usage, usage_by_branch
from scidb.store.schema_keys_change import (
    SchemaKeysChangeRefused,
    change_schema_keys,
    extend_schema_keys,
)
from scidb.store.schema_tree_store import read_parents
from scifor import SchemaTree

ITEM_BRANCH = [["brand", "category", "item"]]
BOTH = [["brand", "category", "item"], ["brand", "location", "person"]]


class ExtPrice(BaseVariable):
    pass


class ExtAge(BaseVariable):
    pass


@pytest.fixture(autouse=True)
def _clean():
    _scifor.set_schema([])
    yield
    clear_current_database()
    _scifor.set_schema([])


def _open(path, declaration):
    _scifor.set_schema([])
    return configure_database(path, declaration)


def _seed(db):
    for b in ("B1", "B2"):
        for c in ("c1", "c2"):
            for i in ("i1", "i2"):
                ExtPrice.save(10.0, brand=b, category=c, item=i)


def _snapshot(db):
    """Everything an extend must leave exactly as it was."""
    duck = db._duck
    schema = duck.fetchall(
        "SELECT schema_id, schema_level, brand, category, item FROM _schema ORDER BY schema_id"
    )
    records = duck.fetchall("SELECT record_id, type, schema_id FROM _record ORDER BY record_id")
    invocations = duck.fetchall("SELECT invocation_id FROM _invocation ORDER BY invocation_id")
    # The new keys add all-NULL columns to a loaded frame; the OLD columns and
    # the data must be identical.
    frame = db.load_all_as_df(ExtPrice)
    cols = [c for c in ("brand", "category", "item", "data") if c in frame.columns]
    frame = frame[cols].sort_values(cols[:3]).reset_index(drop=True)
    return schema, records, invocations, frame.astype(str).to_dict("list")


@pytest.fixture
def seeded(tmp_path):
    path = tmp_path / "t.duckdb"
    db = _open(path, ITEM_BRANCH)
    _seed(db)
    return path, db


# --- the invariant ---------------------------------------------------------


def test_adding_a_branch_changes_nothing_that_exists(seeded):
    path, db = seeded
    before = _snapshot(db)

    result = extend_schema_keys(db, BOTH, reason="add the person branch")
    assert result.added_keys == ["location", "person"]

    db = _open(path, BOTH)
    assert db.dataset_schema_keys == ["brand", "category", "item", "location", "person"]
    assert read_parents(db._duck)["person"] == "location"
    assert _snapshot(db) == before, "record ids, schema ids, invocations and loaded frames are unchanged"
    db.close()


def test_then_save_at_the_new_branch_and_load_both(seeded):
    path, db = seeded
    extend_schema_keys(db, BOTH)
    db = _open(path, BOTH)
    ExtAge.save(31.0, brand="B1", location="l1", person="p1")
    assert float(ExtAge.load(brand="B1", location="l1", person="p1").data) == 31.0
    assert float(ExtPrice.load(brand="B1", category="c1", item="i1").data) == 10.0
    db.close()


def test_a_deeper_level_can_be_added_under_a_leaf(seeded):
    path, db = seeded
    extend_schema_keys(db, [["brand", "category", "item", "size"]])
    db = _open(path, [["brand", "category", "item", "size"]])
    assert db.schema_tree.parent_of["size"] == "item"
    assert float(ExtPrice.load(brand="B1", category="c1", item="i1").data) == 10.0
    db.close()


def test_reopening_an_extended_database_with_the_old_tree_is_a_mismatch(seeded):
    from scidb import SchemaKeysMismatchError

    path, db = seeded
    extend_schema_keys(db, BOTH)
    with pytest.raises(SchemaKeysMismatchError):
        _open(path, ITEM_BRANCH)


# --- unused keys are free, used keys are fixed --------------------------------


def test_extend_then_delete_restores_the_database(seeded):
    path, db = seeded
    before = _snapshot(db)
    extend_schema_keys(db, BOTH)
    db = _open(path, BOTH)
    change_schema_keys(db, ITEM_BRANCH, reason="take the branch back")
    db = _open(path, ITEM_BRANCH)
    assert _snapshot(db) == before
    assert read_parents(db._duck) == {"brand": None, "category": "brand", "item": "category"}
    db.close()


def test_delete_is_refused_once_one_record_names_the_key(seeded):
    path, db = seeded
    extend_schema_keys(db, BOTH)
    db = _open(path, BOTH)
    ExtAge.save(31.0, brand="B1", location="l1", person="p1")
    with pytest.raises(SchemaKeysChangeRefused, match="'location' is in use"):
        change_schema_keys(db, ITEM_BRANCH)
    assert db.dataset_schema_keys == ["brand", "category", "item", "location", "person"]
    db.close()


def test_deleting_a_key_alone_is_refused_while_its_child_stays(seeded):
    path, db = seeded
    extend_schema_keys(db, BOTH)
    db = _open(path, BOTH)
    with pytest.raises(SchemaKeysChangeRefused, match="'person'"):
        change_schema_keys(db, [["brand", "category", "item"], ["brand", "person"]])
    db.close()


def test_a_used_key_cannot_be_renamed_or_moved(seeded):
    path, db = seeded
    with pytest.raises(SchemaKeysChangeRefused, match="'item' is in use"):
        change_schema_keys(db, [["brand", "category", "product"]], renames={"item": "product"})
    with pytest.raises(SchemaKeysChangeRefused, match="'item' is in use"):
        change_schema_keys(db, [["brand", "category"], ["brand", "item"]])  # item under brand
    db.close()


def test_a_new_key_cannot_be_inserted_between_existing_ones(seeded):
    path, db = seeded
    with pytest.raises(SchemaKeysChangeRefused, match="order"):
        change_schema_keys(db, [["brand", "category", "colour", "item"]])
    db.close()


def test_extend_refuses_anything_but_additions(seeded):
    path, db = seeded
    with pytest.raises(SchemaKeysChangeRefused, match="parent"):
        extend_schema_keys(db, [["brand", "category"], ["brand", "item"]])
    with pytest.raises(SchemaKeysChangeRefused, match="No new keys"):
        extend_schema_keys(db, ITEM_BRANCH)
    db.close()


def test_renaming_an_unused_key_carries_its_expectation_marks(seeded):
    path, db = seeded
    extend_schema_keys(db, BOTH)
    db = _open(path, BOTH)
    mark_expectation(db, ExtAge, True, "every site is surveyed", location="l1")
    change_schema_keys(
        db,
        [["brand", "category", "item"], ["brand", "site", "person"]],
        renames={"location": "site"},
    )
    db = _open(path, [["brand", "category", "item"], ["brand", "site", "person"]])
    assert [m.keys for m in marks(db)] == [{"site": "l1"}]
    db.close()


def test_an_unused_key_can_be_reparented(seeded):
    path, db = seeded
    extend_schema_keys(db, BOTH)
    db = _open(path, BOTH)
    moved = [["brand", "category", "item"], ["brand", "category", "location", "person"]]
    change_schema_keys(db, moved)
    db = _open(path, moved)
    assert db.schema_tree.parent_of["location"] == "category"
    db.close()


def test_a_key_with_an_intent_row_cannot_be_deleted_silently(seeded):
    path, db = seeded
    extend_schema_keys(db, BOTH)
    db = _open(path, BOTH)
    mark_expectation(db, ExtAge, True, "surveyed", location="l1")
    with pytest.raises(SchemaKeysChangeRefused, match="location"):
        change_schema_keys(db, ITEM_BRANCH)
    db.close()


# --- the usage owner -------------------------------------------------------


def test_key_usage_names_what_uses_each_key(seeded):
    path, db = seeded
    extend_schema_keys(db, BOTH)
    db = _open(path, BOTH)
    usage = key_usage(db)
    assert usage["item"].records == 8 and usage["item"].used
    assert not usage["location"].used and not usage["person"].used
    branches = {tuple(b): (used, n) for b, used, n in usage_by_branch(db)}
    assert branches[("brand", "category", "item")] == (True, 8)
    assert branches[("location", "person")][0] is False
    db.close()


def test_the_flat_tree_round_trips_as_a_tree(seeded):
    path, db = seeded
    extend_schema_keys(db, BOTH)
    db = _open(path, BOTH)
    assert db.schema_tree == SchemaTree.from_paths(BOTH)
    db.close()
