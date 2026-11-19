"""What a Merge-fed call records in provenance (plan .claude/plan-merge-node.md,
stage diagnose_merge_provenance; docs/claude/merge-node.md "Unverified").

The GUI Merge node will be drawn from history, so it needs to know what a
code-side ``for_each(..., Merge(...))`` run leaves behind:

* ``__inputs[param]``: the Merge's ``to_key()`` spelling (the identity, and
  what the GUI history parser will read).
* lineage edges: does the output point at each constituent's record?

Until 2026-11-17 the answer was NO: a bulk-loaded Merge stripped
``__record_id`` from its constituents and got no ``InputBinding``, so the
output recorded no edge to any record it was built from. Fixed by
``scidb.run.merge_lineage`` (LINEAGE_ONLY binding under the Merge param). A
plain input of the same type (``side``) is the control: it proves the edge
reader works.

Run with ``-s`` to see the recorded edges printed.
"""

import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, Merge, configure_database, for_each
from scidb.provenance import query as pq

SCHEMA = ["subject", "trial"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_merge_provenance.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


class Demographics(BaseVariable):
    """Subject level."""


class TrialSignal(BaseVariable):
    """Trial level."""


class MergedOut(BaseVariable):
    pass


def _seed():
    for subj, age in (("S01", 60), ("S02", 70)):
        Demographics.save(pd.DataFrame({"Age": [age]}), subject=subj)
        for trial in ("1", "2"):
            TrialSignal.save(float(trial), subject=subj, trial=trial)


class AggOut(BaseVariable):
    """Subject-level output of an aggregating (trial not iterated) run."""


CALLS: list = []


def combine(merged, side):
    CALLS.append(len(merged))
    return float(len(merged))


def _run(output=MergedOut, skip_computed=False, **iterate):
    iterate = iterate or {"subject": ["S01", "S02"], "trial": ["1", "2"]}
    for_each(
        combine,
        {"merged": Merge(TrialSignal, Demographics, name="merged"), "side": Demographics},
        [output],
        skip_computed=skip_computed,
        **iterate,
    )


def _edges(db, output=MergedOut, **location) -> dict:
    out = output.load(**location)
    inv = pq.producing_invocation(db._duck, out.record_id)[0]
    edges = pq.invocation_edges_batch(db._duck, [inv])[inv]
    print(f"\n[merge provenance] {location} var_inputs:")
    for e in edges["var_inputs"]:
        print(f"    {e['param_name']:8s} {e['variable_type']:14s} {e['record_id']}")
    return edges


def _merged_rids(edges) -> set:
    return {e["record_id"] for e in edges["var_inputs"] if e["param_name"] == "merged"}


def test_inputs_key_spells_the_merge():
    """``ForEachConfig.to_config_keys`` is what the save path stamps as the
    record's version keys (``list_versions`` does not return them)."""
    from scidb.run.foreach_config import ForEachConfig

    config = ForEachConfig(
        lambda merged, side: 0.0,
        {"merged": Merge(TrialSignal, Demographics, name="merged"), "side": Demographics},
    )
    inputs = config.to_config_keys()["__inputs"]
    print(f"\n[merge provenance] __inputs = {inputs!r}")
    from scidb.provenance.graph import parse_merge_spec

    spec = parse_merge_spec(inputs["merged"])
    assert spec == {
        "name": "merged",
        "how": "exact",
        "tables": ["TrialSignal", "Demographics"],
        "types": ["TrialSignal", "Demographics"],
    }


def test_a_plain_input_is_a_lineage_edge(db):
    """Control: the edge reader sees an ordinary variable input."""
    _seed()
    _run()
    edges = _edges(db, subject="S01", trial="1")
    side = [e for e in edges["var_inputs"] if e["param_name"] == "side"]
    assert [e["record_id"] for e in side] == [Demographics.load(subject="S01").record_id]


def test_every_constituent_record_is_a_lineage_edge(db):
    """The output of a Merge-fed call names, under the Merge's param, the
    record of every constituent it read at that location: the fine one at
    that trial and the coarse (broadcast) one at that subject."""
    _seed()
    _run()
    for subject in ("S01", "S02"):
        for trial in ("1", "2"):
            assert _merged_rids(_edges(db, subject=subject, trial=trial)) == {
                TrialSignal.load(subject=subject, trial=trial).record_id,
                Demographics.load(subject=subject).record_id,
            }


def test_an_aggregating_merge_names_every_record_it_pooled(db):
    """Iterating subject only: each call joins both trials of TrialSignal
    with the subject's Demographics, and names all three records."""
    _seed()
    _run(AggOut, subject=["S01", "S02"])
    trial_rids = {
        TrialSignal.load(subject="S01", trial=t).record_id for t in ("1", "2")
    }
    assert _merged_rids(_edges(db, AggOut, subject="S01")) == trial_rids | {
        Demographics.load(subject="S01").record_id
    }


def test_a_second_identical_run_skips(db):
    """The skip gate (``skip_computed=True``; it is off by default) reads the
    same Selection the save wrote edges from: a rerun with nothing changed
    must not call the function again."""
    _seed()
    CALLS.clear()
    _run(skip_computed=True)
    first = len(CALLS)
    _run(skip_computed=True)
    assert first == 4
    assert len(CALLS) == first, f"rerun recomputed {len(CALLS) - first} call(s)"


def test_a_changed_constituent_reruns(db):
    """The point of the lineage edges: changing a constituent record makes
    the gate recompute instead of skipping.

    Changes TrialSignal (read ONLY through the Merge, unlike Demographics,
    which ``side`` also reads), so a rerun can only come from the Merge's
    edges. The exact count depends on how a re-saved raw record at the same
    location is treated (latest vs. a second variant), so it is printed,
    not pinned.
    """
    _seed()
    CALLS.clear()
    _run(skip_computed=True)
    TrialSignal.save(99.0, subject="S01", trial="1")
    before = len(CALLS)
    _run(skip_computed=True)
    reran = len(CALLS) - before
    print(f"\n[merge provenance] reran after changing TrialSignal S01/1: {reran}")
    assert reran > 0, "a changed Merge constituent was skipped as up to date"


# ---------------------------------------------------------------------------
# Identity: the Merge record (__merge__) names the Merge on the recorded side
# exactly as the forward side names it (docs/claude/merge-node.md).
# ---------------------------------------------------------------------------


def _merge_inputs():
    return {"merged": Merge(TrialSignal, Demographics, name="merged"), "side": Demographics}


def test_forward_call_id_equals_recorded_call_id(db):
    """Before the __merge__ record the recorded side named the Merge param by
    one constituent's type (whichever edge came last) or not at all."""
    from scidb.run.foreach_config import ForEachConfig

    _seed()
    _run()
    forward = ForEachConfig(combine, _merge_inputs()).to_call_id()
    recorded = {
        v["call_id"] for v in pq.pipeline_variants(db._duck) if v["function_name"] == "combine"
    }
    assert recorded == {forward}


def test_recorded_input_types_carry_the_merge_key(db):
    from scidb.provenance.graph import parse_merge_spec

    _seed()
    _run()
    [variant] = [v for v in pq.pipeline_variants(db._duck) if v["function_name"] == "combine"]
    spec = parse_merge_spec(variant["input_types"]["merged"])
    assert spec is not None and spec["name"] == "merged"
    assert spec["types"] == ["TrialSignal", "Demographics"]


def test_a_merge_fed_node_turns_green(db):
    """The expected-invocation predictor binds what the run bound (the Merge
    record + every constituent record at the location)."""
    from scidb.provenance.state import check_node_state

    _seed()
    _run()
    state = check_node_state(combine, [MergedOut], db=db)
    print(f"\n[merge provenance] node state: {state['state']} {state['counts']}")
    assert state["state"] == "green"


def test_changing_how_reruns_under_skip_computed(db):
    """how is identity: the skip gate must not reuse an exact run's outputs."""
    _seed()
    CALLS.clear()
    _run(skip_computed=True)
    before = len(CALLS)
    for_each(
        combine,
        {"merged": Merge(TrialSignal, Demographics, name="merged", how="outer"),
         "side": Demographics},
        [MergedOut],
        skip_computed=True,
        subject=["S01", "S02"],
        trial=["1", "2"],
    )
    assert len(CALLS) - before == 4
