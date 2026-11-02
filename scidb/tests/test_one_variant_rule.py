"""ONE rule for what a variant is, shared by load and node state (2026-11-01).

Before, ``load(version_id="latest")`` (``_find_record``) grouped records by
function name + accumulated constants + consumed input locations, while node
state (``latest_at_location_batch``, ``current_records_by_schema_batch``)
grouped by the producing call's constants alone. The case that split them: one
function, one constant, one output location, computed from two DIFFERENT input
locations. The load kept both records as two variants; node state called the
older one superseded, so its consumers read stale forever.

The rule (``provenance_query.variant_keys_batch`` / ``current_record_ids``):
function name + accumulated constants + consumed input locations, never the
function hash ("newer version": a code edit re-run supersedes). Two more
decisions pinned here (2026-11-01): a newer record of ANOTHER variant at a
location does not make consumers stale (a constant sweep used to ping-pong),
and a newer hand save (``Var.save``) REPLACES every variant saved before it
at its location, in load and node state alike.
"""

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scidb.provenance import query as pq


class RuleIn(BaseVariable):
    pass


class RuleOut(BaseVariable):
    pass


class RuleDown(BaseVariable):
    pass


def _numbers(x):
    return np.concatenate(
        [np.ravel(np.asarray(v, dtype=float)) for v in np.asarray(x, dtype=object).ravel()]
    )


def total(x):
    return float(np.sum(_numbers(x)))


def total_edited(x):
    return float(np.sum(_numbers(x))) + 0.0


total_edited.__name__ = total_edited.__qualname__ = "total"


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "rule.duckdb", ["subject", "trial"])
    for t in ("1", "2"):
        RuleIn.save(np.arange(3.0) + int(t), db=database, subject="1", trial=t)
    yield database
    _scifor.set_schema([])
    database.close()


def _out_rids(db):
    return [r[0] for r in db._duck.fetchall("SELECT record_id FROM _record WHERE type = 'RuleOut'")]


def _latest_load(db):
    frame = db._find_record(
        "RuleOut", nested_metadata={"schema": {"subject": "1"}}, version_id="latest"
    )
    return set(frame["record_id"])


def _current_by_state(db):
    return {rid for rids in pq.current_records_by_schema_batch(db._duck, "RuleOut").values() for rid in rids}


def _run(db, fn, trial):
    for_each(
        fn,
        {"x": RuleIn},
        [RuleOut],
        schema_keys=["subject"],
        schema_filter={"subject": ["1"], "trial": [trial]},
        db=db,
    )


def test_different_input_locations_are_two_variants_everywhere(db):
    _run(db, total, "1")
    _run(db, total, "2")
    rids = set(_out_rids(db))
    assert len(rids) == 2
    assert _latest_load(db) == rids
    assert _current_by_state(db) == rids
    latest = pq.latest_at_location_batch(db._duck, list(rids))
    assert all(latest[r]["latest_variant"] == r for r in rids)


def test_a_code_edit_rerun_is_a_newer_version(db):
    _run(db, total, "1")
    (old,) = _out_rids(db)
    _run(db, total_edited, "1")
    rids = set(_out_rids(db))
    assert len(rids) == 2
    (new,) = rids - {old}
    assert _latest_load(db) == {new}
    assert _current_by_state(db) == {new}
    assert pq.latest_at_location_batch(db._duck, [old])[old]["latest_variant"] == new


def scaled(x, k):
    return float(np.sum(_numbers(x))) * k


def halve(y):
    return y / 2


def _rids(db, type_name):
    return [
        r[0] for r in db._duck.fetchall("SELECT record_id FROM _record WHERE type = ?", [type_name])
    ]


def _sweep(db, k):
    for_each(
        scaled,
        {"x": RuleIn, "k": k},
        [RuleOut],
        schema_keys=["subject"],
        schema_filter={"subject": ["1"], "trial": ["1"]},
        db=db,
    )


def test_a_newer_variant_does_not_stale_another_variants_consumers(db):
    _sweep(db, 20)
    for_each(halve, {"y": RuleOut}, [RuleDown], subject=["1"], db=db)
    (down,) = _rids(db, "RuleDown")
    _sweep(db, 30)  # a second variant at the same location, saved later
    assert pq.superseded_batch(db._duck, [down]) == {down: False}


def test_a_hand_save_replaces_the_pipeline_output(db):
    _run(db, total, "1")
    (computed,) = _out_rids(db)
    RuleOut.save(99.0, db=db, subject="1")
    (by_hand,) = set(_out_rids(db)) - {computed}
    assert _latest_load(db) == {by_hand}
    assert _current_by_state(db) == {by_hand}
    assert pq.latest_at_location_batch(db._duck, [computed])[computed]["latest_variant"] is None


def test_a_pipeline_run_after_a_hand_save_coexists_with_it(db):
    RuleOut.save(99.0, db=db, subject="1")
    (by_hand,) = _out_rids(db)
    _run(db, total, "1")
    assert _latest_load(db) == set(_out_rids(db)) and by_hand in _latest_load(db)
