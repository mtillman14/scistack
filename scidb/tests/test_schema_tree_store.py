"""Declaring and storing a branched schema (scidb.store.schema_tree_store).

The tree is a default and the key set is the truth
(.claude/plan-branched-schema-keys.md, stage declare_and_store): the database
stores each key's parent beside the key order, reopening with another tree is
refused like another key list, and a plain list is the one-branch tree it
always meant.
"""

from __future__ import annotations

import pytest
import scifor as _scifor
from scifor import SchemaTree

from scidb import BaseVariable, SchemaKeysMismatchError, configure_database
from scidb.project.init import init_project
from scidb.project import bundle
from scidb.store.database import clear_current_database, get_database
from scidb.store.schema_keys_change import SchemaKeysChangeRefused, change_schema_keys
from scidb.store.schema_tree_store import read_parents, tree_from_db

SHOP = [["brand", "category", "item"], ["brand", "location", "person"]]
FLAT = ["brand", "category", "item", "location", "person"]


class StDemo(BaseVariable):
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


def test_branches_are_stored_and_the_flat_list_keeps_the_order(tmp_path):
    db = _open(tmp_path / "t.duckdb", SHOP)
    assert db.dataset_schema_keys == FLAT
    assert db.schema_tree.parent_of["person"] == "location"
    assert read_parents(db._duck) == {
        "brand": None, "category": "brand", "item": "category",
        "location": "brand", "person": "location",
    }
    db.close()


def test_scifor_is_told_the_tree_not_just_the_list(tmp_path):
    db = _open(tmp_path / "t.duckdb", SHOP)
    assert _scifor.get_schema() == FLAT
    assert not _scifor.get_schema_tree().is_linear
    db.close()


def test_a_plain_list_is_stored_as_a_linear_tree(tmp_path):
    db = _open(tmp_path / "t.duckdb", ["subject", "session"])
    assert read_parents(db._duck) == {"subject": None, "session": "subject"}
    assert db.schema_tree.is_linear
    db.close()


def test_reopening_with_the_same_branches_works(tmp_path):
    path = tmp_path / "t.duckdb"
    _open(path, SHOP).close()
    db = _open(path, SHOP)
    assert db.schema_tree == SchemaTree.from_paths(SHOP)
    db.close()


def test_reopening_a_branched_database_with_the_flat_list_is_refused_naming_parents(tmp_path):
    path = tmp_path / "t.duckdb"
    _open(path, SHOP).close()
    with pytest.raises(SchemaKeysMismatchError) as excinfo:
        _open(path, FLAT)
    message = str(excinfo.value)
    assert "parents differ" in message
    assert "'location'" in message
    assert excinfo.value.existing_parents["location"] == "brand"


def test_a_refused_tree_does_not_leave_the_file_locked(tmp_path):
    path = tmp_path / "t.duckdb"
    _open(path, SHOP).close()
    with pytest.raises(SchemaKeysMismatchError):
        _open(path, FLAT)
    _open(path, SHOP).close()  # would raise an IO lock error if the refusal leaked


def test_a_database_with_no_stored_parents_gets_the_declared_tree(tmp_path):
    """A database created before trees: its first writable open stores them."""
    path = tmp_path / "t.duckdb"
    db = _open(path, ["a", "b"])
    db._duck.execute("DELETE FROM _schema_key_parent")
    db.close()
    db = _open(path, ["a", "b"])
    assert read_parents(db._duck) == {"a": None, "b": "a"}
    db.close()


def test_tree_from_db_reads_the_tree_without_knowing_the_keys(tmp_path):
    path = tmp_path / "t.duckdb"
    _open(path, SHOP).close()
    tree = tree_from_db(path)
    assert tree.keys == tuple(FLAT)
    assert tree.parent_of["category"] == "brand"
    db = _open(path, tree)  # a tree is a valid declaration
    db.close()


def test_a_tree_whose_flat_order_is_not_first_appearance_survives_a_round_trip(tmp_path):
    """paths() can reorder; the stored tree must not."""
    keys = ["a", "b", "c", "d"]
    tree = SchemaTree.from_parents(keys, {"b": "a", "c": "a", "d": "b"})
    path = tmp_path / "t.duckdb"
    _open(path, tree).close()
    assert tree_from_db(path) == tree


def test_inspector_opens_a_branched_database_without_being_told_the_tree(tmp_path):
    from scidb.inspect import Inspector

    path = tmp_path / "t.duckdb"
    _open(path, SHOP).close()
    clear_current_database()
    inspector = Inspector.open(path)
    try:
        assert inspector._db.schema_tree.parent_of["person"] == "location"
    finally:
        inspector._db.close()


# --- change_schema_keys ----------------------------------------------------


def test_change_while_empty_accepts_branches_and_rewrites_the_parents(tmp_path):
    path = tmp_path / "t.duckdb"
    db = _open(path, ["x", "y"])
    change_schema_keys(db, SHOP, reason="test")  # closes the database
    db = _open(path, SHOP)
    assert db.dataset_schema_keys == FLAT
    assert read_parents(db._duck)["location"] == "brand"
    db.close()


def test_change_to_a_bad_tree_is_refused_with_the_key_named(tmp_path):
    db = _open(tmp_path / "t.duckdb", ["x", "y"])
    bad = [["a", "b", "c"], ["a", "d", "c"]]
    with pytest.raises(SchemaKeysChangeRefused, match="'c'"):
        change_schema_keys(db, bad)
    db.close()


def test_change_is_still_refused_once_a_used_key_would_go(tmp_path):
    db = _open(tmp_path / "t.duckdb", ["x", "y"])
    StDemo.save(1.0, x="1", y="2")
    with pytest.raises(SchemaKeysChangeRefused, match="in use"):
        change_schema_keys(db, SHOP)  # would delete x and y
    assert read_parents(db._duck) == {"x": None, "y": "x"}
    db.close()


# --- bundles ---------------------------------------------------------------


def test_a_bundle_carries_the_tree_and_the_import_rebuilds_it(tmp_path):
    root = tmp_path / "shop"
    init_project(root)
    db = _open(root / "shop.duckdb", SHOP)
    out = bundle.export_project(root, db, tmp_path / "out" / "shop", providers=[])
    db.close()
    clear_current_database()

    assert bundle.read_bundle(out).manifest["project"]["schema_parents"]["person"] == "location"

    report = bundle.import_project(out, tmp_path / "copy", providers=[])
    assert report.schema_keys == FLAT
    get_database().close()  # import leaves the new project open
    assert tree_from_db(report.db_path) == SchemaTree.from_paths(SHOP)
