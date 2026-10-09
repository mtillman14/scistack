"""Every edge a canvas graph returns connects two nodes that graph returns.

Found 2026-10-09 while testing libraries: a manual variable node whose
type HAS run history, placed in a submodule WITHOUT a saved position,
graduates onto its history twin's placement (``var__RawSignal::<scope>``);
graduation rewrites the edge to that id, but the response then held the
edge and not the node (the edge's source label came back as None).

A position-less manual node is not a test artefact: ``pipeline_discovery``
seeds source-defined ``scidb.Pipeline``s exactly that way ("positions are
left at (0, 0)"), so a Pipeline in code that uses an already-run variable
opens with this canvas. The invariant is checked on two consecutive builds:
the first build graduates, the second reads the graduated state.
"""

from __future__ import annotations

import pytest

from scistack_gui import pipeline_store as ps


def _graph(db, pid):
    from scistack_gui.services.pipeline_service import get_pipeline_graph

    return get_pipeline_graph(db, pid)


def _dangling(graph) -> list[dict]:
    ids = {n["id"] for n in graph["nodes"]}
    return [e for e in graph["edges"] if e["source"] not in ids or e["target"] not in ids]


def _describe(graph) -> str:
    nodes = sorted((n["id"], n["type"], n.get("data", {}).get("label")) for n in graph["nodes"])
    edges = sorted((e["source"], e["target"], e.get("targetHandle")) for e in graph["edges"])
    return f"nodes={nodes}\nedges={edges}"


def _assert_consistent(db, pid, labels):
    for build in ("first", "second"):
        g = _graph(db, pid)
        dangling = _dangling(g)
        assert dangling == [], (
            f"{build} build of {pid}: edge(s) to nodes not on the canvas: {dangling}\n{_describe(g)}"
        )
        shown = {n.get("data", {}).get("label") for n in g["nodes"]}
        assert labels <= shown, f"{build} build of {pid}: missing {labels - shown}\n{_describe(g)}"


def _canvas(db, pid, *, positions: bool):
    """RawSignal (has history in populated_db) -> custom_proc (never run)."""
    from scistack_gui import layout as layout_store

    ps.write_manual_node(db, "var__RawSignal__edgetest1", "variableNode", "RawSignal", pid)
    ps.write_manual_node(db, "fn__custom_proc__edgetest2", "functionNode", "custom_proc", pid)
    ps.write_manual_edge(db, {
        "id": "edge_edgetest", "source": "var__RawSignal__edgetest1",
        "target": "fn__custom_proc__edgetest2", "targetHandle": "in__signal",
    })
    if positions:
        layout_store.write_node_position("var__RawSignal__edgetest1", 0.0, 0.0, pipeline_id=pid)
        layout_store.write_node_position("fn__custom_proc__edgetest2", 200.0, 0.0, pipeline_id=pid)


@pytest.mark.parametrize("positions", [True, False], ids=["placed", "position-less"])
@pytest.mark.parametrize("scope", ["main", "submodule"])
def test_a_graduating_manual_node_keeps_its_edge(populated_db, scope, positions):
    db = populated_db
    pid = "main" if scope == "main" else ps.create_pipeline(db, "edge test sub")
    _canvas(db, pid, positions=positions)
    _assert_consistent(db, pid, {"RawSignal", "custom_proc"})


def test_a_bare_positioned_twin_keeps_its_position_and_its_edge(populated_db):
    """The history node is already positioned on the root canvas under its
    BARE id; a position-less manual twin graduates (to ``var__RawSignal::main``)
    and its edge is rewritten to that spelling. The node must stay the
    positioned one, and the edge must reach it."""
    from scistack_gui import layout as layout_store

    db = populated_db
    layout_store.write_node_position("var__RawSignal", 50.0, 60.0, pipeline_id="main")
    _canvas(db, "main", positions=False)
    _assert_consistent(db, "main", {"RawSignal", "custom_proc"})
    g = _graph(db, "main")
    raw = [n for n in g["nodes"] if n.get("data", {}).get("label") == "RawSignal"]
    assert [n["id"] for n in raw] == ["var__RawSignal"], _describe(g)


def test_a_source_pipeline_using_an_already_run_variable(populated_db):
    """The real-world path: a scidb.Pipeline in code, seeded by discovery."""
    from scidb import BaseVariable, Pipeline

    from scistack_gui.pipeline_discovery import discover_and_seed_pipelines
    from tests.conftest import RawSignal

    class EdgeTestOut(BaseVariable):
        pass

    def edge_test_step(signal):
        return signal

    db = populated_db
    pipe = Pipeline("edge test source")
    pipe.register_call(
        fn=edge_test_step, inputs={"signal": RawSignal}, outputs=[EdgeTestOut],
        metadata_iterables={}, options={},
    )
    try:
        assert discover_and_seed_pipelines(db)["created"] == ["edge test source"]
        pid = {p["name"]: p["pipeline_id"] for p in ps.list_pipelines(db)}["edge test source"]
        _assert_consistent(db, pid, {"RawSignal", "edge_test_step", "EdgeTestOut"})
    finally:
        BaseVariable.unregister("EdgeTestOut")
