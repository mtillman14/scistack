"""Tests for the bipartite read side (Phase 4).

Covers the graph-backed reimplementations and new capabilities:
- get_provenance over the graph (function_name / constants)
- get_derived_branch_params (§6)
- get_pipeline reconstruction nodes + edges (§8)
- get_execution_audit (§9b)
- has_lineage true for computed, false for raw

get_upstream_provenance's full contract is exercised by test_branch_params.py;
here we add the new methods.
"""

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each

SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_read.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


class RawSignal(BaseVariable):
    pass


class Filtered(BaseVariable):
    pass


class Spikes(BaseVariable):
    pass


def bandpass(signal, low_hz):
    return signal * low_hz


def detect_spikes(signal, threshold):
    return (signal > threshold).astype(float)


def _two_step(db):
    RawSignal.save(np.array([1.0, 2.0, 3.0]), subject="S01", session="1")
    for_each(
        bandpass,
        {"signal": RawSignal, "low_hz": 20},
        [Filtered],
        subject=["S01"],
        session=["1"],
    )
    for_each(
        detect_spikes,
        {"signal": Filtered, "threshold": 0.5},
        [Spikes],
        subject=["S01"],
        session=["1"],
    )
    return Spikes.load(subject="S01", session="1")


# ---------------------------------------------------------------------------
# get_provenance
# ---------------------------------------------------------------------------
def test_get_provenance_over_graph(db):
    RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
    for_each(
        bandpass,
        {"signal": RawSignal, "low_hz": 20},
        [Filtered],
        subject=["S01"],
        session=["1"],
    )
    f = Filtered.load(subject="S01", session="1")

    prov = db.get_provenance(Filtered, version=f.record_id)
    assert prov is not None
    assert prov["function_name"] == "bandpass"
    assert prov["function_hash"]
    assert prov["constants"] == {"low_hz": 20}
    assert [i["variable_type"] for i in prov["inputs"]] == ["RawSignal"]


def test_get_provenance_none_for_raw(db):
    RawSignal.save(np.array([1.0]), subject="S01", session="1")
    raw = RawSignal.load(subject="S01", session="1")
    assert db.get_provenance(RawSignal, version=raw.record_id) is None


# ---------------------------------------------------------------------------
# get_derived_branch_params (§6)
# ---------------------------------------------------------------------------
def test_derived_branch_params_accumulates(db):
    s = _two_step(db)
    bp = db.get_derived_branch_params(s.record_id)
    assert bp == {"bandpass.low_hz": 20, "detect_spikes.threshold": 0.5}


def test_derived_branch_params_empty_for_raw(db):
    RawSignal.save(np.array([1.0]), subject="S01", session="1")
    raw = RawSignal.load(subject="S01", session="1")
    assert db.get_derived_branch_params(raw.record_id) == {}


# ---------------------------------------------------------------------------
# Batched provenance equivalence — the bulk forms used by the hot load paths
# (_find_record collapse, _assemble_df_from_records_and_data) must return
# results identical to the per-record primitives they replace, since correctness
# of variant collapse and branch_params columns depends on it.
# ---------------------------------------------------------------------------
def _multi_record_pipeline(db):
    """Two subjects × two low_hz variants → a spread of computed records sharing
    ancestry, so the batched closure is exercised on overlapping subgraphs."""
    for subj in ("S01", "S02"):
        RawSignal.save(np.array([1.0, 2.0, 3.0]), subject=subj, session="1")
    for low in (20, 30):
        for_each(
            bandpass,
            {"signal": RawSignal, "low_hz": low},
            [Filtered],
            subject=["S01", "S02"],
            session=["1"],
        )
    for_each(
        detect_spikes,
        {"signal": Filtered, "threshold": 0.5},
        [Spikes],
        subject=["S01", "S02"],
        session=["1"],
    )
    rows = db._duck._fetchall("SELECT record_id FROM _record")
    return [r[0] for r in rows]


def test_batched_provenance_matches_per_record(db):
    from scidb import provenance_query as pq

    rids = _multi_record_pipeline(db)
    duck = db._duck

    inv_map = pq.producing_invocation_batch(duck, rids)
    onum_map = pq.output_num_batch(duck, rids)
    bp_map = pq.branch_params_batch(duck, rids)

    for rid in rids:
        assert inv_map.get(rid) == pq.producing_invocation(duck, rid)
        assert onum_map.get(rid) == pq.output_num_for(duck, rid)
        # branch_params_batch omits empty maps only when a record has none;
        # the per-record form returns {} there, so normalise with .get(rid, {}).
        assert bp_map.get(rid, {}) == pq.derived_branch_params(duck, rid)


def test_batched_provenance_empty_inputs(db):
    from scidb import provenance_query as pq

    assert pq.producing_invocation_batch(db._duck, []) == {}
    assert pq.output_num_batch(db._duck, []) == {}
    assert pq.branch_params_batch(db._duck, []) == {}


def test_branch_params_batch_handles_duplicates(db):
    """Duplicate seed ids (a record appearing twice in a result set) must not
    corrupt the per-record accumulation."""
    from scidb import provenance_query as pq

    s = _two_step(db)
    once = pq.branch_params_batch(db._duck, [s.record_id])
    twice = pq.branch_params_batch(db._duck, [s.record_id, s.record_id])
    assert once == twice
    assert once[s.record_id] == pq.derived_branch_params(db._duck, s.record_id)


# ---------------------------------------------------------------------------
# get_pipeline (§8)
# ---------------------------------------------------------------------------
def test_get_pipeline_nodes_and_edges(db):
    s = _two_step(db)
    pipe = db.get_pipeline(s.record_id)

    types = [n["variable_type"] for n in pipe["nodes"]]
    assert types == ["Spikes", "Filtered", "RawSignal"]

    # Two edges: RawSignal->Filtered (signal), Filtered->Spikes (signal).
    edge_pairs = {(e["from_record_id"], e["to_record_id"]) for e in pipe["edges"]}
    assert len(edge_pairs) == 2
    for e in pipe["edges"]:
        assert e["param_name"] == "signal"


# ---------------------------------------------------------------------------
# get_execution_audit (§9b)
# ---------------------------------------------------------------------------
def test_execution_audit_records_run(db):
    RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
    for_each(
        bandpass,
        {"signal": RawSignal, "low_hz": 20},
        [Filtered],
        subject=["S01"],
        session=["1"],
    )
    f = Filtered.load(subject="S01", session="1")

    audit = db.get_execution_audit(f.record_id)
    assert len(audit) >= 1
    entry = audit[0]
    assert entry["function_name"] == "bandpass"
    assert entry["timestamp"]
    assert set(entry.keys()) == {
        "timestamp",
        "user_id",
        "where_clause",
        "function_name",
    }


# ---------------------------------------------------------------------------
# consumed_input_schema_ids (§10 "where= redesign" (B) — semantic variant match)
# ---------------------------------------------------------------------------
def test_consumed_input_schema_ids(db):
    from scidb import provenance_query

    RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
    raw = RawSignal.load(subject="S01", session="1")
    for_each(
        bandpass,
        {"signal": RawSignal, "low_hz": 20},
        [Filtered],
        subject=["S01"],
        session=["1"],
    )
    f = Filtered.load(subject="S01", session="1")

    # Filtered's producing invocation consumed the one RawSignal input; the
    # consumed set is that input's schema location.
    raw_sid = db._duck._fetchall(
        "SELECT schema_id FROM _record WHERE record_id = ?", [raw.record_id]
    )[0][0]
    consumed = provenance_query.consumed_input_schema_ids(db._duck, [f.record_id])
    assert consumed[f.record_id] == frozenset({raw_sid})


def test_consumed_input_schema_ids_empty_for_raw(db):
    from scidb import provenance_query

    RawSignal.save(np.array([1.0]), subject="S01", session="1")
    raw = RawSignal.load(subject="S01", session="1")
    # Raw records have no producing invocation → no consumed inputs → no entry.
    assert provenance_query.consumed_input_schema_ids(db._duck, [raw.record_id]) == {}


# ---------------------------------------------------------------------------
# has_lineage
# ---------------------------------------------------------------------------
def test_has_lineage_true_for_computed_false_for_raw(db):
    RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
    raw = RawSignal.load(subject="S01", session="1")
    for_each(
        bandpass,
        {"signal": RawSignal, "low_hz": 20},
        [Filtered],
        subject=["S01"],
        session=["1"],
    )
    f = Filtered.load(subject="S01", session="1")

    assert db.has_lineage(f.record_id) is True
    assert db.has_lineage(raw.record_id) is False


# ---------------------------------------------------------------------------
# identical_content_groups
# ---------------------------------------------------------------------------
# The fingerprint of a step that ran with distribute=False when it should have
# distributed: every location of the un-iterated key received the WHOLE result
# instead of its own slice, so the records are byte-identical and a plot
# colored by that key draws N overlapping traces that read as one line.
def test_identical_content_groups_finds_duplicates(db):
    from scidb import provenance_query

    same = np.array([1.0, 2.0, 3.0])
    RawSignal.save(same, subject="S01", session="1")
    RawSignal.save(same, subject="S01", session="2")

    groups = provenance_query.identical_content_groups(db._duck, "RawSignal")
    assert len(groups) == 1
    _content_hash, record_ids = groups[0]
    assert len(record_ids) == 2


def test_identical_content_groups_empty_when_all_differ(db):
    from scidb import provenance_query

    RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
    RawSignal.save(np.array([3.0, 4.0]), subject="S01", session="2")

    assert provenance_query.identical_content_groups(db._duck, "RawSignal") == []


def test_identical_content_groups_unknown_variable(db):
    from scidb import provenance_query

    assert provenance_query.identical_content_groups(db._duck, "NoSuchVar") == []
