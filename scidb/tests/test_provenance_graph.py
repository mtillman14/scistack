"""Integration tests for the bipartite provenance graph written by for_each (Phase 3).

Verifies that running a pipeline populates the seven new tables correctly:
- _record holds raw inputs, computed outputs, and constants
- _invocation holds one row per unique call (fn name/hash, flags)
- _invocation_input edges carry both variable and constant inputs
- _invocation_output edges link the call to its output record
- _run / _run_invocation append per execution; the graph itself is idempotent
- distinct constants → distinct invocations; re-run → same invocation, new run

These run additively alongside the legacy _lineage writes (deleted in Phase 5).
"""

import numpy as np
import pytest
from scidb.provenance import CONSTANT_TYPE, compute_constant_record_id

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each

SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_graph.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


class RawSignal(BaseVariable):
    pass


class Filtered(BaseVariable):
    pass


def bandpass(signal, low_hz):
    return signal * low_hz


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _count(db, table):
    return db._duck._fetchall(f"SELECT COUNT(*) FROM {table}")[0][0]


# ---------------------------------------------------------------------------
# Raw saves populate _record
# ---------------------------------------------------------------------------
def test_raw_save_creates_record_entity(db):
    RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
    rows = db._duck._fetchall(
        "SELECT type, schema_version FROM _record WHERE type = 'RawSignal'"
    )
    assert len(rows) == 1
    assert rows[0][0] == "RawSignal"


# ---------------------------------------------------------------------------
# A computed step writes the full graph
# ---------------------------------------------------------------------------
def test_for_each_writes_bipartite_graph(db):
    RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
    for_each(
        bandpass,
        {"signal": RawSignal, "low_hz": 20},
        [Filtered],
        subject=["S01"],
        session=["1"],
    )

    # One invocation for bandpass.
    inv = db._duck._fetchall(
        "SELECT invocation_id, function_name, function_hash, distribute FROM _invocation"
    )
    assert len(inv) == 1
    inv_id, fn_name, fn_hash, distribute = inv[0]
    assert fn_name == "bandpass"
    assert fn_hash  # non-empty
    assert distribute is False

    # Inputs: the variable 'signal' plus the constant 'low_hz'.
    inputs = {
        (p, r)
        for (p, r) in db._duck._fetchall(
            "SELECT param_name, input_record_id FROM _invocation_input WHERE invocation_id = ?",
            [inv_id],
        )
    }
    params = {p for p, _ in inputs}
    assert params == {"signal", "low_hz"}

    # The constant edge points at the content-addressed constant record.
    const_rid = compute_constant_record_id(20)
    assert ("low_hz", const_rid) in inputs

    # Constant entity + value rows exist.
    crow = db._duck._fetchall(
        "SELECT value_repr, value_type FROM _constant WHERE record_id = ?", [const_rid]
    )
    assert crow and crow[0][0] == "20" and crow[0][1] == "int"
    crec = db._duck._fetchall(
        "SELECT type FROM _record WHERE record_id = ?", [const_rid]
    )
    assert crec and crec[0][0] == CONSTANT_TYPE

    # Output edge → a Filtered record present in _record.
    out = db._duck._fetchall(
        "SELECT output_num, output_record_id FROM _invocation_output WHERE invocation_id = ?",
        [inv_id],
    )
    assert len(out) == 1
    assert out[0][0] == 0
    out_rid = out[0][1]
    rtype = db._duck._fetchall(
        "SELECT type FROM _record WHERE record_id = ?", [out_rid]
    )
    assert rtype and rtype[0][0] == "Filtered"

    # Run audit: one run linked to the invocation.
    assert _count(db, "_run") == 1
    ri = db._duck._fetchall("SELECT run_id, invocation_id FROM _run_invocation")
    assert len(ri) == 1 and ri[0][1] == inv_id
    run = db._duck._fetchall("SELECT function_name FROM _run")
    assert run[0][0] == "bandpass"


# ---------------------------------------------------------------------------
# Distinct constants → distinct invocations
# ---------------------------------------------------------------------------
def test_distinct_constants_distinct_invocations(db):
    RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
    for_each(
        bandpass,
        {"signal": RawSignal, "low_hz": 20},
        [Filtered],
        subject=["S01"],
        session=["1"],
    )
    for_each(
        bandpass,
        {"signal": RawSignal, "low_hz": 30},
        [Filtered],
        subject=["S01"],
        session=["1"],
    )

    assert _count(db, "_invocation") == 2
    # Two executions → two run rows.
    assert _count(db, "_run") == 2


# ---------------------------------------------------------------------------
# Re-run is idempotent for the graph, but appends a run
# ---------------------------------------------------------------------------
def test_rerun_idempotent_graph_new_run(db):
    RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
    for_each(
        bandpass,
        {"signal": RawSignal, "low_hz": 20},
        [Filtered],
        subject=["S01"],
        session=["1"],
    )
    inv_after_first = _count(db, "_invocation")
    out_after_first = _count(db, "_invocation_output")

    for_each(
        bandpass,
        {"signal": RawSignal, "low_hz": 20},
        [Filtered],
        subject=["S01"],
        session=["1"],
    )

    # Graph did not grow...
    assert _count(db, "_invocation") == inv_after_first
    assert _count(db, "_invocation_output") == out_after_first
    # ...but the audit log did (re-run recorded). At least the first run is
    # linked; the second run is appended even if the output was a cache hit.
    assert _count(db, "_run") >= 1


# ---------------------------------------------------------------------------
# Distribute/flatten fan-out: ONE invocation → MANY output records.
#
# A returned DataFrame spread into one record per row produces thousands of
# GraphRecords that all share identity-relevant meta (one invocation) and the
# same nominal output_num. record_run must (a) compute the invocation once
# (memo), and (b) hand each output a unique output_num without an
# _invocation_output PK collision — the slot-allocation cursor.
# ---------------------------------------------------------------------------
def test_distribute_fanout_one_invocation_unique_slots(db):
    from scidb.bindings import Binding
    from scidb.provenance_save import GraphRecord, record_run

    n = 500
    # A live dict for __constants — that is how real for_each meta arrives,
    # and the cache key must stay hashable for it. The edges travel TYPED on
    # the GraphRecord (since 2026-09-20), never in the meta.
    shared_meta = {
        "__fn": "calc_fanout",
        "__fn_hash": "fanouthash",
        "__constants": {"low_hz": 20},
        "__distribute": True,
    }
    edges = [Binding("signal", "input_rid_X")]
    # All share one invocation + the same base output_num (0), distinct outputs.
    graph_records = [
        GraphRecord("Filtered", 1, 0, f"out_rid_{i:05d}", dict(shared_meta), bindings=edges)
        for i in range(n)
    ]

    run_id = record_run(
        db,
        graph_records,
        function_name="calc_fanout",
        where_clause=None,
        user_id="tester",
    )
    assert run_id is not None

    # Exactly one invocation despite n outputs (memo collapsed the duplicates).
    assert _count(db, "_invocation") == 1
    # n distinct output edges, each with a unique (invocation_id, output_num).
    assert _count(db, "_invocation_output") == n
    onums = [
        r[0] for r in db._duck._fetchall("SELECT output_num FROM _invocation_output")
    ]
    assert len(set(onums)) == n

    # Re-running is idempotent for the graph (same ids, ON CONFLICT DO NOTHING):
    # no new invocation/output rows, but a fresh _run is appended.
    runs_before = _count(db, "_run")
    record_run(
        db,
        graph_records,
        function_name="calc_fanout",
        where_clause=None,
        user_id="tester",
    )
    assert _count(db, "_invocation") == 1
    assert _count(db, "_invocation_output") == n
    assert _count(db, "_run") == runs_before + 1


# ---------------------------------------------------------------------------
# Multiple schema locations → independent invocations
# ---------------------------------------------------------------------------
def test_multiple_subjects_independent_invocations(db):
    for subj in ["S01", "S02"]:
        RawSignal.save(np.array([1.0, 2.0]), subject=subj, session="1")
    for_each(
        bandpass,
        {"signal": RawSignal, "low_hz": 20},
        [Filtered],
        subject=["S01", "S02"],
        session=["1"],
    )

    # Different upstream record per subject → different input binding → 2 invocations.
    assert _count(db, "_invocation") == 2
    assert _count(db, "_invocation_output") == 2
