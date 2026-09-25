"""Regression tests for Fix B: cross-run output-edge assignment (immutable).

Bug: ``_invocation_output`` has PK ``(invocation_id, output_num)`` and the
output-edge insert used ``ON CONFLICT DO NOTHING``. Two separate for_each runs
that share an ``invocation_id`` (same fn + same constants, no variable input —
and, since 2026-09-25, the same PathInput spec, which is now identity) but write to DISJOINT
schema locations reused the same ``output_num`` sequence — the second run's edges
collided with the first run's committed slots and were silently dropped, orphaning
those records (no producing-invocation edge).

Fix B (immutable): before assigning slots, seed the COMMITTED
``(invocation_id, output_num)`` edges into the working state so new records take
the next FREE ``output_num`` (append). Nothing committed is overwritten or
excluded; identical re-saves recognise their committed slot and re-insert the
same edge (a DO NOTHING no-op).
"""

import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each

SCHEMA = ["subject"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_rerun_edges.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


class Out(BaseVariable):
    pass


def make_val(c):
    """Constant-only function — no variable input, so the invocation_id depends
    only on (fn, constants) and is SHARED across separate runs (as is a
    PathInput-only call re-run over the SAME spec)."""
    return float(c)


def _edge_split(db):
    """{is_orphan: count} for Out records by producing-edge presence."""
    rows = db._duck._fetchall("""
        SELECT (io.output_record_id IS NULL) AS no_edge, COUNT(*)
        FROM _record r
        LEFT JOIN _invocation_output io ON io.output_record_id = r.record_id
        WHERE r.type = 'Out'
        GROUP BY 1
    """)
    return {bool(no_edge): n for no_edge, n in rows}


def test_disjoint_reruns_keep_all_edges(db):
    """Two runs sharing one invocation_id at DISJOINT locations → both records
    keep a producing-invocation edge (zero orphans)."""
    for_each(make_val, {"c": 5.0}, [Out], subject=["S01"])
    for_each(make_val, {"c": 5.0}, [Out], subject=["S02"])

    split = _edge_split(db)
    # Two Out records (S01, S02), both LINKED. Pre-fix the second would be orphaned.
    assert split == {False: 2}, (
        f"expected both records linked (no orphans); got edge split {split}"
    )
    # And they occupy distinct output slots under the shared invocation.
    slots = db._duck._fetchall("""
        SELECT DISTINCT io.invocation_id, io.output_num
        FROM _invocation_output io
        JOIN _record r ON r.record_id = io.output_record_id
        WHERE r.type = 'Out'
    """)
    assert len({inv for inv, _ in slots}) == 1, "expected a single shared invocation_id"
    assert len({onum for _, onum in slots}) == 2, (
        "expected two distinct output_num slots"
    )


def test_identical_rerun_is_idempotent(db):
    """Re-running the identical save adds no new record and no new edge (the
    content-addressed record_id is unchanged) — only the save log grows."""
    for_each(make_val, {"c": 5.0}, [Out], subject=["S01"])
    n_rec = db._duck._fetchall("SELECT COUNT(*) FROM _record WHERE type='Out'")[0][0]
    n_edge = db._duck._fetchall("SELECT COUNT(*) FROM _invocation_output")[0][0]

    for_each(make_val, {"c": 5.0}, [Out], subject=["S01"])  # identical
    assert (
        db._duck._fetchall("SELECT COUNT(*) FROM _record WHERE type='Out'")[0][0]
        == n_rec
    )
    assert db._duck._fetchall("SELECT COUNT(*) FROM _invocation_output")[0][0] == n_edge
    assert _edge_split(db) == {False: 1}
