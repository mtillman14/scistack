"""One save batch must not put two DIFFERENT records of one variant at one
schema location (`DatabaseManager._check_duplicate_locations`, rule in
`database._colliding_locations`; docs/claude/duplicate-location-vs-stale.md).

The shape that found it (scistack.log 2026-10-27): a CSV read once, saved per
subject, with two rows for subject 2455369. Both records were saved at the
same instant, so `_supersede_same_invocation` kept both "latest" and readers
reported a stale value. A re-run in a later batch is a different case: the
newer save wins, and nothing is refused.
"""

from __future__ import annotations

import pandas as pd
import pytest
import scifor as _scifor
from scifor import PathInput
from scifor.failures import FailureGroup, groups_from_wire, run_failure

from scidb import (
    BaseVariable,
    DuplicateLocationError,
    OutputSaveError,
    configure_database,
    for_each,
)
from scidb.database import _colliding_locations, _differing_columns


class Demo(BaseVariable):
    pass


def read_table(path):
    return pd.read_csv(path)


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "dup.duckdb", ["subject", "session"])
    yield db
    _scifor.set_schema([])
    db.close()


def _n_records(db, type_name="Demo") -> int:
    return db._duck._fetchall(
        "SELECT COUNT(*) FROM _record WHERE type = ?", [type_name]
    )[0][0]


def _csv(tmp_path, body):
    path = tmp_path / "demographics.csv"
    path.write_text(body)
    return PathInput(str(path), name="DemographicsPath")


# --- save_batch ------------------------------------------------------------


def test_two_differing_rows_for_one_subject_refuse_the_save(db):
    df = pd.DataFrame(
        {"subject": ["S1", "S1", "S2"], "age": [30, 31, 40], "group": ["a", "a", "b"]}
    )
    with pytest.raises(DuplicateLocationError) as info:
        Demo.save(df, db=db)
    assert _n_records(db) == 0, "nothing is written for a refused save"

    err = info.value
    assert err.variable == "Demo"
    assert [c["location"] for c in err.collisions] == [{"subject": "S1"}]
    assert err.collisions[0]["count"] == 2
    assert err.collisions[0]["differing_columns"] == ["age"]
    assert "subject=S1 x2" in str(err)
    assert "differ in age" in str(err)


def test_the_remedy_names_the_location_and_the_two_ways_out(db):
    df = pd.DataFrame({"subject": ["2455369", "2455369"], "score": [1.0, 2.0]})
    with pytest.raises(DuplicateLocationError) as info:
        Demo.save(df, db=db)
    remedy = info.value.remedy
    assert remedy["kind"] == "duplicate_location"
    assert "subject" in remedy["title"] and "Demo" in remedy["title"]
    assert remedy["items"] == [
        {"location": "subject=2455369", "count": 2, "differing_columns": []}
    ]
    assert remedy["n_more"] == 0
    # Schema keys are fixed at database creation: that step only explains.
    assert [s["action"] for s in remedy["steps"]] == [
        None, "reveal_function", "combine_rows"
    ]
    assert "new project database" in remedy["steps"][0]["text"]


def test_identical_rows_for_one_subject_merge_into_one_record(db):
    df = pd.DataFrame({"subject": ["S1", "S1"], "score": [1.0, 1.0]})
    Demo.save(df, db=db)
    assert _n_records(db) == 1


def test_different_branch_params_at_one_location_are_variants(db):
    db.save_batch(
        Demo,
        [
            (1.0, {"subject": "S1", "__branch_params": {"k": 1}, "k": 1}),
            (2.0, {"subject": "S1", "__branch_params": {"k": 2}, "k": 2}),
        ],
    )
    assert _n_records(db) == 2


def test_a_later_save_at_the_same_location_is_not_refused(db):
    Demo.save(pd.DataFrame({"subject": ["S1"], "score": [1.0]}), db=db)
    Demo.save(pd.DataFrame({"subject": ["S1"], "score": [2.0]}), db=db)
    assert _n_records(db) == 2


# --- for_each (the CSV shape) ----------------------------------------------


def test_for_each_csv_with_a_duplicated_subject_raises_output_save_error(db, tmp_path):
    pi = _csv(tmp_path, "subject,group\nS1,a\nS1,b\nS2,c\n")
    with pytest.raises(OutputSaveError) as info:
        for_each(read_table, {"path": pi}, [Demo])
    err = info.value
    assert any(isinstance(c, DuplicateLocationError) for c in err.causes)
    assert err.blame == "setup"
    assert err.remedy is not None and err.remedy["kind"] == "duplicate_location"
    assert err.remedy["items"][0]["location"] == "subject=S1"
    assert _n_records(db) == 0


def test_the_remedy_rides_the_failure_record_to_the_wire(db, tmp_path):
    pi = _csv(tmp_path, "subject,group\nS1,a\nS1,b\n")
    with pytest.raises(OutputSaveError) as info:
        for_each(read_table, {"path": pi}, [Demo])
    group = FailureGroup(example=run_failure(info.value, fn_name="read_table")).to_dict()
    assert group["remedy"]["kind"] == "duplicate_location"
    assert group["example"]["remedy"] == group["remedy"]
    (back,) = groups_from_wire([group])
    assert back["remedy"]["items"] == group["remedy"]["items"]


# --- the rule ----------------------------------------------------------------


def test_rule_single_item_and_items_without_schema_keys():
    keys = ["subject", "session"]
    assert _colliding_locations([(1.0, {"subject": "S1"})], keys)[0] == []
    collisions, stats = _colliding_locations([(1.0, {}), (2.0, {})], keys)
    assert collisions == []
    assert stats["no_location"] == 2


def test_rule_coarse_location_collides_on_the_keys_it_has():
    """A subject-level record has no `session`; two of them still collide."""
    keys = ["subject", "session"]
    collisions, stats = _colliding_locations(
        [(1.0, {"subject": "S1"}), (2.0, {"subject": "S1"}), (3.0, {"subject": "S1", "session": "1"})],
        keys,
    )
    assert collisions == [
        {"location": {"subject": "S1"}, "count": 2, "differing_columns": []}
    ]
    assert stats["locations"] == 2


def test_rule_numeric_and_string_keys_are_one_location():
    collisions, _ = _colliding_locations(
        [(1.0, {"subject": 1}), (2.0, {"subject": "1"})], ["subject"]
    )
    assert len(collisions) == 1


def test_differing_columns_handles_nan_and_dicts():
    a = pd.DataFrame({"x": [float("nan")], "y": [1]})
    b = pd.DataFrame({"x": [float("nan")], "y": [2]})
    assert _differing_columns([a, b]) == ["y"]
    assert _differing_columns([{"p": 1, "q": 2}, {"p": 1, "q": 3}]) == ["q"]
    assert _differing_columns([1.0, 2.0]) == []


def test_rule_ignores_branch_params_that_copy_the_data_columns():
    """for_each's flatten mode copies each spread row's scalar data columns
    into its ``__branch_params``; that must not make two differing rows of one
    subject look like two variants (the CSV case did not raise, 2026-10-27).
    A branch param that is NOT a data column still separates variants."""
    a = pd.DataFrame({"group": ["a"]})
    b = pd.DataFrame({"group": ["b"]})
    collisions, _ = _colliding_locations(
        [
            (a, {"subject": "S1", "__branch_params": {"group": "a"}}),
            (b, {"subject": "S1", "__branch_params": {"group": "b"}}),
        ],
        ["subject"],
    )
    assert collisions == [
        {"location": {"subject": "S1"}, "count": 2, "differing_columns": ["group"]}
    ]
    collisions, _ = _colliding_locations(
        [
            (a, {"subject": "S1", "__branch_params": {"f.k": 1, "group": "a"}}),
            (b, {"subject": "S1", "__branch_params": {"f.k": 2, "group": "b"}}),
        ],
        ["subject"],
    )
    assert collisions == []
