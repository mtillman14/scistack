"""Integration tests: canvas nodes group for_each call sites by WIRING.

Since 2026-07-18 (user decision), the same function invoked with different
constant values renders as ONE canvas node (fn + loadable inputs + outputs
= the wiring), with each call site as a variant row carrying its OWN state
chip. The 2026-07-16 no-blur guarantee ("a partial run for one constant
value degrades only that call site") is preserved — it moved from separate
nodes to separate chips: scidb call_id semantics and per-call-site state
computation are untouched, only the presentation groups.

Sits at the same layer as test_api.py (TestClient + populated_db).
"""

from __future__ import annotations

import numpy as np
import pytest
import scistack_gui.db as _gui_db
from conftest import FilteredSignal, RawSignal, bandpass_filter
from fastapi.testclient import TestClient
from scidb.database import _local
from scistack_gui import registry as _registry
from scistack_gui.app import create_app
from scistack_gui.domain.graph_builder import edge_dedup_key, wiring_id
from scistack_gui.ids import fn_node_id

from scidb import BaseVariable, configure_database, for_each


@pytest.fixture
def two_call_sites_client(tmp_path):
    """A DB with two for_each call sites for bandpass_filter (low_hz=20 and 50)."""
    if hasattr(_local, "database"):
        delattr(_local, "database")
    _gui_db._db = None

    db = configure_database(tmp_path / "twosite.duckdb", ["subject", "session"])
    for subj in [1, 2]:
        for sess in ["pre", "post"]:
            RawSignal.save(np.random.randn(10), subject=subj, session=sess)

    for_each(
        bandpass_filter,
        inputs={"signal": RawSignal, "low_hz": 20},
        outputs=[FilteredSignal],
        subject=[1, 2],
        session=["pre", "post"],
    )
    for_each(
        bandpass_filter,
        inputs={"signal": RawSignal, "low_hz": 50},
        outputs=[FilteredSignal],
        subject=[1, 2],
        session=["pre", "post"],
    )

    _gui_db._db = db
    _gui_db._db_path = tmp_path / "twosite.duckdb"
    _registry._functions["bandpass_filter"] = bandpass_filter

    from scistack_gui import pipeline_store

    pipeline_store._ensure_tables(db)

    with TestClient(create_app()) as c:
        yield c

    db.close()


def _bp_group_node_id(db=None) -> str:
    """The node both call sites group into.

    They share one wiring (same fn, loadable inputs, outputs), so they are one
    node — but since 2026-09-22 that node's id is ALLOCATED, not derived from
    the wiring, so it has to be looked up (``docs/claude/node-identity.md``).
    """
    from scistack_gui import node_wiring
    from scistack_gui.db import get_db

    db = db or get_db()
    wid = wiring_id("bandpass_filter", {"signal": "RawSignal"}, {"FilteredSignal"}, {})
    node_id = node_wiring.node_for_wiring(db, wid)
    assert node_id is not None, (
        "no node has been allocated for the bandpass wiring — build the graph "
        "(GET /api/pipeline) before asking for its id"
    )
    return node_id


def _bp_nodes(nodes) -> list[dict]:
    return [
        n
        for n in nodes
        if n.get("type") == "functionNode" and n["data"]["label"] == "bandpass_filter"
    ]


def test_two_call_sites_group_into_one_node(two_call_sites_client):
    nodes = two_call_sites_client.get("/api/pipeline").json()["nodes"]
    bp_nodes = _bp_nodes(nodes)
    assert len(bp_nodes) == 1, (
        f"expected ONE wiring-grouped bandpass_filter node, got "
        f"{[n['id'] for n in bp_nodes]}"
    )
    assert bp_nodes[0]["id"] == _bp_group_node_id()


def test_grouped_node_carries_both_variants_with_states(two_call_sites_client):
    nodes = two_call_sites_client.get("/api/pipeline").json()["nodes"]
    node = _bp_nodes(nodes)[0]
    variants = node["data"]["variants"]
    by_low_hz = {
        v["constants"]["low_hz"]: v
        for v in variants
        if "low_hz" in v.get("constants", {})
    }
    assert set(by_low_hz) == {20, 50}
    # Both fully run → both chips green; the composite id suffix is the
    # wiring id, and each row keeps its own call_id.
    assert by_low_hz[20]["state"] == "green"
    assert by_low_hz[50]["state"] == "green"
    assert by_low_hz[20]["call_id"] != by_low_hz[50]["call_id"]
    assert node["id"].endswith("__" + node["data"]["call_id"])


def test_grouped_node_has_single_edge_set(two_call_sites_client):
    """RawSignal/const/output edges target the ONE grouped node (deduped)."""
    graph = two_call_sites_client.get("/api/pipeline").json()
    nid = _bp_group_node_id()

    in_edges = [
        e
        for e in graph["edges"]
        if e["source"] == "var__RawSignal" and e["target"] == nid
    ]
    const_edges = [
        e
        for e in graph["edges"]
        if e["source"] == "param__low_hz" and e["target"] == nid
    ]
    out_edges = [
        e
        for e in graph["edges"]
        if e["source"] == nid and e["target"] == "var__FilteredSignal"
    ]
    assert len(in_edges) == 1
    assert len(const_edges) == 1
    assert len(out_edges) == 1


def test_grouped_node_state_is_green_when_all_variants_current(two_call_sites_client):
    nodes = two_call_sites_client.get("/api/pipeline").json()["nodes"]
    assert _bp_nodes(nodes)[0]["data"]["run_state"] == "green"


def test_partial_run_reddens_only_its_own_variant_chip(tmp_path):
    """A partial run for one constant value degrades ONLY that variant's
    chip (the no-blur behavior the user asked for on 2026-07-16, now at
    chip level); the node border shows the worst member state."""
    if hasattr(_local, "database"):
        delattr(_local, "database")
    _gui_db._db = None

    db = configure_database(tmp_path / "split.duckdb", ["subject", "session"])
    for subj in [1, 2]:
        for sess in ["pre", "post"]:
            RawSignal.save(np.random.randn(10), subject=subj, session=sess)

    # Variant A: fully run.
    for_each(
        bandpass_filter,
        inputs={"signal": RawSignal, "low_hz": 20},
        outputs=[FilteredSignal],
        subject=[1, 2],
        session=["pre", "post"],
    )
    # Variant B: only subject=1 (partial).
    for_each(
        bandpass_filter,
        inputs={"signal": RawSignal, "low_hz": 50},
        outputs=[FilteredSignal],
        subject=[1],
        session=["pre", "post"],
    )

    _gui_db._db = db
    _gui_db._db_path = tmp_path / "split.duckdb"
    _registry._functions["bandpass_filter"] = bandpass_filter
    from scistack_gui import pipeline_store

    pipeline_store._ensure_tables(db)

    try:
        with TestClient(create_app()) as c:
            nodes = c.get("/api/pipeline").json()["nodes"]
    finally:
        db.close()

    node = _bp_nodes(nodes)[0]
    by_low_hz = {
        v["constants"]["low_hz"]: v
        for v in node["data"]["variants"]
        if "low_hz" in v.get("constants", {})
    }
    assert by_low_hz[20]["state"] == "green", "fully-run variant chip must remain green"
    assert by_low_hz[50]["state"] == "red", (
        "partial variant chip must be red (binary call-site state)"
    )
    assert node["data"]["run_state"] == "red", (
        "node border shows the worst member state"
    )


def test_legacy_call_site_position_is_adopted(two_call_sites_client):
    """One-time migration: a position saved under a pre-grouping
    per-call-site node id is adopted by the group node (same scope) and the
    legacy key is dropped."""
    from scidb.foreach_config import ForEachConfig
    from scistack_gui import layout as layout_store

    cid = ForEachConfig(
        fn=bandpass_filter,
        inputs={"signal": RawSignal, "low_hz": 20},
    ).to_call_id()
    legacy_id = fn_node_id("bandpass_filter", cid)
    # Build once so the node exists: its id is allocated, not derived, so
    # there is nothing to name before something has named it.
    two_call_sites_client.get("/api/pipeline")
    group_id = _bp_group_node_id()

    # Simulate a pre-grouping document: position keyed by the call-site id.
    layout_store.write_node_position(legacy_id, 123.0, 456.0, pipeline_id="main")
    layout_store.drop_node_positions(group_id)

    # A graph build runs the migration.
    two_call_sites_client.get("/api/pipeline")

    positions = layout_store.read_positions_by_scope()
    main_positions = positions.get("main", {})
    assert legacy_id not in main_positions, "legacy key must be dropped"
    assert main_positions.get(group_id) == {"x": 123.0, "y": 456.0}


# ---------------------------------------------------------------------------
# Manual function node vs. a same-name, differently-wired real call site
# ---------------------------------------------------------------------------
#
# Regression coverage for a real bug: placing a SECOND manual functionNode
# with the same label as an already-executed function, but wired to a
# DIFFERENT input/output variable type, must neither graduate into the
# executed call site's canonical id (merge_manual_nodes matches candidates
# by (type, label) only) nor inherit its green run state
# (_own_state_for_function's scihist.check_node_state call had no call_id,
# so it answered "has this FUNCTION NAME ever produced these outputs",
# blind to which inputs actually fed it). Found via a real GUI session:
# a second compute_rolling_vo2 wired to RawHeartRate (instead of RawVO2)
# turned green immediately, without ever being run.


class OtherSignal(BaseVariable):
    pass


class OtherFiltered(BaseVariable):
    pass


def test_differently_wired_manual_node_does_not_graduate_or_show_green(client):
    OtherSignal.save(np.zeros(5), subject=1, session="pre")

    client.put(
        "/api/layout/mv_other_in",
        json={"x": 0, "y": 0, "node_type": "variableNode", "label": "OtherSignal"},
    )
    client.put(
        "/api/layout/mf_bp2",
        json={"x": 10, "y": 0, "node_type": "functionNode", "label": "bandpass_filter"},
    )
    client.put(
        "/api/layout/mv_other_out",
        json={"x": 20, "y": 0, "node_type": "variableNode", "label": "OtherFiltered"},
    )
    client.put("/api/edges/e_in2", json={
        "source": "mv_other_in", "target": "mf_bp2", "target_handle": "in__signal",
    })
    client.put("/api/edges/e_out2", json={"source": "mf_bp2", "target": "mv_other_out"})

    nodes = client.get("/api/pipeline").json()["nodes"]
    bp_nodes = [
        n
        for n in nodes
        if n["type"] == "functionNode" and n["data"]["label"] == "bandpass_filter"
    ]

    assert len(bp_nodes) == 2, (
        "the differently-wired manual node must not graduate into (merge "
        f"with) the real bandpass_filter(RawSignal) node, got {[n['id'] for n in bp_nodes]}"
    )
    new_node = next(n for n in bp_nodes if n["id"] == "mf_bp2")
    assert new_node["data"]["run_state"] == "red", (
        "must not inherit the RawSignal call site's green state just "
        "because the function NAME matches — it has never itself been run"
    )


def test_unwired_manual_node_still_graduates_immediately(client):
    """The flip side: a manual node with NO wiring info yet (just placed,
    nothing connected) has no basis to claim it's "different" from the
    single existing candidate, so it must still graduate/show real state
    immediately — the existing, deliberate UX this fix must not break."""
    client.put(
        "/api/layout/mf_bp3",
        json={"x": 0, "y": 0, "node_type": "functionNode", "label": "bandpass_filter"},
    )

    nodes = client.get("/api/pipeline").json()["nodes"]
    bp_nodes = [
        n
        for n in nodes
        if n["type"] == "functionNode" and n["data"]["label"] == "bandpass_filter"
    ]
    assert len(bp_nodes) == 1, "an unwired same-label node must graduate, not duplicate"
    assert bp_nodes[0]["data"]["run_state"] == "green"


def test_manual_node_graduates_after_running_despite_shared_label_ambiguity(client):
    """Regression test: once a differently-wired manual node has actually
    been run (a second real call site now shares the function's label
    with the original), it must graduate into its own real node on the
    next graph build — not stay stuck as a permanent duplicate "replica"
    alongside the real one. merge_manual_nodes' (type, label) matching
    alone refuses to graduate once >1 real candidate shares a label
    (ambiguous); the fix resolves each manual function node's own wiring
    and matches against the correct candidate regardless of how many
    OTHER candidates share the label. Found via a real GUI session: a
    successfully-run, green compute_rolling_vo2(RawHeartRate) node never
    merged with its own real call site once compute_rolling_vo2(RawVO2)
    also existed — both stayed visible as separate nodes forever."""

    class OtherSignal4(BaseVariable):
        pass

    class OtherFiltered4(BaseVariable):
        pass

    OtherSignal4.save(np.zeros(5), subject=1, session="pre")

    client.put("/api/layout/mv_o4_in", json={
        "x": 0, "y": 0, "node_type": "variableNode", "label": "OtherSignal4",
    })
    client.put("/api/layout/mf_bp_other4", json={
        "x": 10, "y": 0, "node_type": "functionNode", "label": "bandpass_filter",
    })
    client.put("/api/layout/mv_o4_out", json={
        "x": 20, "y": 0, "node_type": "variableNode", "label": "OtherFiltered4",
    })
    client.put("/api/edges/e_o4_in", json={
        "source": "mv_o4_in", "target": "mf_bp_other4", "target_handle": "in__signal",
    })
    client.put("/api/edges/e_o4_out", json={"source": "mf_bp_other4", "target": "mv_o4_out"})
    client.put("/api/edges/e_o4_const", json={
        "source": "param__low_hz", "target": "mf_bp_other4", "target_handle": "in__low_hz",
    })

    # Simulate the manual node having been successfully run (what
    # /api/run + derive_target_for_node would do) — a second real
    # bandpass_filter call site now exists, sharing the label with the
    # original RawSignal-wired one.
    for_each(
        bandpass_filter,
        inputs={"signal": OtherSignal4, "low_hz": 20},
        outputs=[OtherFiltered4],
        subject=[1],
        session=["pre"],
    )

    nodes = client.get("/api/pipeline").json()["nodes"]
    bp_nodes = [
        n
        for n in nodes
        if n["type"] == "functionNode" and n["data"]["label"] == "bandpass_filter"
    ]
    assert len(bp_nodes) == 2, (
        "the manual node must graduate into its own real call site, not "
        f"stay stuck as a permanent duplicate, got {[n['id'] for n in bp_nodes]}"
    )
    assert "mf_bp_other4" not in {n["id"] for n in bp_nodes}
    graduated = next(
        n for n in bp_nodes if n["data"].get("input_params", {}).get("signal") == "OtherSignal4"
    )
    assert graduated["data"]["run_state"] == "green"


def test_first_build_after_run_has_no_duplicate_edges(client):
    """Regression test: the FIRST graph build after a manual node graduates
    must not return two edges for the same wire.

    build_edges dedups manual edges against DB-derived ones, but it runs
    BEFORE graduation — while the manual edge still names the manual node id,
    so it doesn't compare equal to the DB-derived edge. Graduation then
    rewrites its endpoints onto the DB-derived ids, which is exactly what
    makes it a duplicate, and nothing re-checked afterwards. Found in a real
    GUI session (examples/vo2max/scidb.log): the post-run build reported
    "4 total edges (2 DB-derived, 2 manual)" and the very next rebuild
    reported "2 total edges (... 2 manual superseded by DB-derived)". The
    doubled edges stay on the canvas until some unrelated refresh, and
    deleting one leaves its twin.

    Asserting on the first build is the whole point — a second GET would
    mask the bug.
    """

    class OtherSignal5(BaseVariable):
        pass

    class OtherFiltered5(BaseVariable):
        pass

    OtherSignal5.save(np.zeros(5), subject=1, session="pre")

    client.put("/api/layout/mv_o5_in", json={
        "x": 0, "y": 0, "node_type": "variableNode", "label": "OtherSignal5",
    })
    client.put("/api/layout/mf_bp_other5", json={
        "x": 10, "y": 0, "node_type": "functionNode", "label": "bandpass_filter",
    })
    client.put("/api/layout/mv_o5_out", json={
        "x": 20, "y": 0, "node_type": "variableNode", "label": "OtherFiltered5",
    })
    client.put("/api/edges/e_o5_in", json={
        "source": "mv_o5_in", "target": "mf_bp_other5", "target_handle": "in__signal",
    })
    client.put("/api/edges/e_o5_out", json={"source": "mf_bp_other5", "target": "mv_o5_out"})

    for_each(
        bandpass_filter,
        inputs={"signal": OtherSignal5, "low_hz": 20},
        outputs=[OtherFiltered5],
        subject=[1],
        session=["pre"],
    )

    graph = client.get("/api/pipeline").json()
    edges = graph["edges"]

    keys = [
        edge_dedup_key(e["source"], e["target"], e.get("targetHandle")) for e in edges
    ]
    duplicated = {k for k in keys if keys.count(k) > 1}
    assert not duplicated, (
        "the first build after graduation returned duplicate edges for "
        f"{sorted(duplicated)}: {[(e['id'], e['source'], e['target']) for e in edges]}"
    )

    # The wires must still be THERE — dropping the duplicate must not drop
    # the connection itself.
    graduated = next(
        n
        for n in graph["nodes"]
        if n["type"] == "functionNode"
        and n["data"].get("input_params", {}).get("signal") == "OtherSignal5"
    )
    assert edge_dedup_key("var__OtherSignal5", graduated["id"]) in keys
    assert edge_dedup_key(graduated["id"], "var__OtherFiltered5") in keys


def test_disconnected_duplicate_survives_wired_siblings_graduation(client):
    """Regression test: a manual functionNode left with NO wiring at all
    must not be silently deleted just because a SIBLING manual node with
    the same label graduates into the one real call site that both happen
    to (individually) match.

    merge_manual_nodes and the Pass 1 "absence of wiring is not a
    conflict" rule (see test_unwired_manual_node_still_graduates_immediately)
    each decide, per manual node, "does THIS ONE graduate" — neither knows
    about siblings. Two manual nodes sharing a label can therefore both
    resolve to the SAME target: the wired one legitimately matches it, and
    the unwired one also "graduates" since it has no wiring to contradict
    anything. graduate_manual_node only deletes the manual row (the target
    already exists from real data), so the second graduation silently
    deleted the unwired node with nothing left to show for it. Found via a
    real GUI session: two compute_rolling_vo2 placeholders, one wired to
    RawVO2 and run, one left completely disconnected — running the wired
    one made the disconnected one vanish from the canvas."""
    # A wired manual node matching the real call site `populated_db` already
    # seeded (bandpass_filter fed by RawSignal -> FilteredSignal).
    client.put(
        "/api/layout/mv_wired_in",
        json={"x": 0, "y": 0, "node_type": "variableNode", "label": "RawSignal"},
    )
    client.put(
        "/api/layout/mf_wired",
        json={"x": 10, "y": 0, "node_type": "functionNode", "label": "bandpass_filter"},
    )
    client.put(
        "/api/layout/mv_wired_out",
        json={"x": 20, "y": 0, "node_type": "variableNode", "label": "FilteredSignal"},
    )
    client.put("/api/edges/e_wired_in", json={
        "source": "mv_wired_in", "target": "mf_wired", "target_handle": "in__signal",
    })
    client.put("/api/edges/e_wired_out", json={"source": "mf_wired", "target": "mv_wired_out"})

    # A second manual node, same label, with NO edges at all.
    client.put(
        "/api/layout/mf_disconnected",
        json={"x": 100, "y": 100, "node_type": "functionNode", "label": "bandpass_filter"},
    )

    nodes = client.get("/api/pipeline").json()["nodes"]
    bp_nodes = [
        n
        for n in nodes
        if n["type"] == "functionNode" and n["data"]["label"] == "bandpass_filter"
    ]
    assert len(bp_nodes) == 2, (
        "the disconnected duplicate must survive as its own manual node "
        f"when its wired sibling graduates, got {[n['id'] for n in bp_nodes]}"
    )
    assert "mf_disconnected" in {n["id"] for n in bp_nodes}, (
        "the disconnected node's manual id must remain (demoted back to a "
        "separate manual node, not graduated and deleted)"
    )
    disconnected_node = next(n for n in bp_nodes if n["id"] == "mf_disconnected")
    assert disconnected_node["data"]["run_state"] == "red", (
        "must not inherit its wired sibling's green state"
    )
