"""``get_pipeline(run_states=False)`` — the graph the Plot Studio pickers
draw (cleanup-audit F15).

The per-node state check is most of a graph build (~6 s of 11 s on a real
843-invocation database), and a picker canvas shows no state. Without it the
build must skip the check entirely and report NO ``run_state`` — not a
default green that would read as "up to date".
"""

import pytest
from scistack_gui.api import pipeline as api_pipeline
from scistack_gui.services.pipeline_service import get_pipeline_graph


def _states(graph) -> dict:
    return {
        n["id"]: n["data"].get("run_state")
        for n in graph["nodes"]
        if n["type"] in ("functionNode", "variableNode")
    }


def test_the_canvas_still_gets_run_states(populated_db):
    graph = get_pipeline_graph(populated_db, "main")
    states = _states(graph)
    assert states and all(s is not None for s in states.values()), states


def test_without_run_states_the_check_is_skipped(populated_db, monkeypatch):
    def refuse(*_a, **_k):
        raise AssertionError("the state check ran for a graph that wants none")

    monkeypatch.setattr(api_pipeline, "_compute_run_states", refuse)
    graph = get_pipeline_graph(populated_db, "main", run_states=False)
    states = _states(graph)
    assert states, "the picker still needs the nodes"
    assert all(s is None for s in states.values()), states


def test_the_same_nodes_and_edges_either_way(populated_db):
    full = get_pipeline_graph(populated_db, "main")
    bare = get_pipeline_graph(populated_db, "main", run_states=False)
    assert sorted(n["id"] for n in full["nodes"]) == sorted(
        n["id"] for n in bare["nodes"]
    )
    assert sorted(e["id"] for e in full["edges"]) == sorted(
        e["id"] for e in bare["edges"]
    )


@pytest.mark.parametrize("flag, expected", [("false", False), (None, True)])
def test_the_request_model_reads_the_flag(flag, expected):
    params = {"pipeline_id": "main"}
    if flag is not None:
        params["run_states"] = flag
    assert api_pipeline.PipelineQuery(**params).run_states is expected
