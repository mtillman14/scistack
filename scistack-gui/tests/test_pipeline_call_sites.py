"""Integration tests: same function reused at multiple for_each call sites
must render as multiple distinct function nodes, each with its own state,
edges, and constants.

Sits at the same layer as test_api.py (TestClient + populated_db).
"""

from __future__ import annotations

import numpy as np
import pytest
from fastapi.testclient import TestClient

import scistack_gui.db as _gui_db
from scidb import configure_database, for_each
from scidb.database import _local
from scidb.foreach_config import ForEachConfig
from scistack_gui import registry as _registry
from scistack_gui.app import create_app
from scistack_gui.domain.graph_builder import fn_node_id

from conftest import RawSignal, FilteredSignal, bandpass_filter


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


def _bp_node_ids() -> tuple[str, str]:
    cid_a = ForEachConfig(
        fn=bandpass_filter, inputs={"signal": RawSignal, "low_hz": 20}
    ).to_call_id()
    cid_b = ForEachConfig(
        fn=bandpass_filter, inputs={"signal": RawSignal, "low_hz": 50}
    ).to_call_id()
    return fn_node_id("bandpass_filter", cid_a), fn_node_id("bandpass_filter", cid_b)


def test_two_call_sites_produce_two_function_nodes(two_call_sites_client):
    nodes = two_call_sites_client.get("/api/pipeline").json()["nodes"]
    bp_nodes = [n for n in nodes
                if n.get("type") == "functionNode"
                and n["data"]["label"] == "bandpass_filter"]
    assert len(bp_nodes) == 2, (
        f"expected 2 bandpass_filter call-site nodes, got {len(bp_nodes)}: "
        f"{[n['id'] for n in bp_nodes]}"
    )
    nid_a, nid_b = _bp_node_ids()
    ids = {n["id"] for n in bp_nodes}
    assert ids == {nid_a, nid_b}


def test_each_call_site_carries_its_own_call_id(two_call_sites_client):
    nodes = two_call_sites_client.get("/api/pipeline").json()["nodes"]
    bp_nodes = [n for n in nodes if n["data"].get("label") == "bandpass_filter"
                and n.get("type") == "functionNode"]
    for n in bp_nodes:
        cid = n["data"].get("call_id")
        assert cid and len(cid) == 16
        assert n["id"].endswith("__" + cid)


def test_each_call_site_has_its_own_input_edge(two_call_sites_client):
    """RawSignal should connect to BOTH call-site nodes via separate edges."""
    edges = two_call_sites_client.get("/api/pipeline").json()["edges"]
    nid_a, nid_b = _bp_node_ids()
    targets = {e["target"] for e in edges if e["source"] == "var__RawSignal"}
    assert nid_a in targets and nid_b in targets


def test_each_call_site_has_its_own_output_edge(two_call_sites_client):
    edges = two_call_sites_client.get("/api/pipeline").json()["edges"]
    nid_a, nid_b = _bp_node_ids()
    sources = {e["source"] for e in edges if e["target"] == "var__FilteredSignal"}
    assert nid_a in sources and nid_b in sources


def test_each_call_site_has_constant_edge(two_call_sites_client):
    """const__low_hz feeds both call sites independently."""
    edges = two_call_sites_client.get("/api/pipeline").json()["edges"]
    nid_a, nid_b = _bp_node_ids()
    targets = {e["target"] for e in edges if e["source"] == "const__low_hz"}
    assert nid_a in targets and nid_b in targets


def test_each_call_site_reports_independent_state(two_call_sites_client):
    """Both fully-run call sites are green independently."""
    nodes = two_call_sites_client.get("/api/pipeline").json()["nodes"]
    nid_a, nid_b = _bp_node_ids()
    a = next(n for n in nodes if n["id"] == nid_a)
    b = next(n for n in nodes if n["id"] == nid_b)
    assert a["data"]["run_state"] == "green"
    assert b["data"]["run_state"] == "green"


def test_partial_run_only_greys_its_own_call_site(tmp_path):
    """A partial run for one constant value greys ONLY that call site,
    not the other (this is the behavior the user explicitly asked for —
    they should be able to tell the two call sites apart)."""
    if hasattr(_local, "database"):
        delattr(_local, "database")
    _gui_db._db = None

    db = configure_database(tmp_path / "split.duckdb", ["subject", "session"])
    for subj in [1, 2]:
        for sess in ["pre", "post"]:
            RawSignal.save(np.random.randn(10), subject=subj, session=sess)

    # Call site A: fully run.
    for_each(
        bandpass_filter,
        inputs={"signal": RawSignal, "low_hz": 20},
        outputs=[FilteredSignal],
        subject=[1, 2], session=["pre", "post"],
    )
    # Call site B: only subject=1 (partial).
    for_each(
        bandpass_filter,
        inputs={"signal": RawSignal, "low_hz": 50},
        outputs=[FilteredSignal],
        subject=[1], session=["pre", "post"],
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

    nid_a, nid_b = _bp_node_ids()
    a = next(n for n in nodes if n["id"] == nid_a)
    b = next(n for n in nodes if n["id"] == nid_b)
    assert a["data"]["run_state"] == "green", "fully-run call site must remain green"
    assert b["data"]["run_state"] == "grey", "partial call site must be grey"
