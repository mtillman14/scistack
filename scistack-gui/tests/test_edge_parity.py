"""Canvas-vs-run parity for a function node's inputs.

The safety net for the unified edge model (.claude/plan-unified-edge-model.md,
D-2026-10-01-1). For each scenario, three readers must agree on what feeds
the node:

- the CANVAS: the edges ``GET /api/pipeline`` draws into it;
- the RUN: ``execution_service.derive_target_for_node``;
- the "why can't this run" check: ``execution_service.disconnected_reason``.

Every scenario is a bug that was found by a failed run on 2026-10-01, plus the
hide-scope case decided for step 1. Written against the code BEFORE the
refactor, so it must pass both before and after (except the xfail, which is
the known scope bug that step 1 fixes).

The MATLAB command's reader is not covered here: the fixture function is
Python. Step 1 routes the MATLAB service through the same EdgeView seam, and
its unit tests cover it there.
"""

from __future__ import annotations

import numpy as np
import pytest
from scidb import BaseVariable

from scistack_gui.db import get_db
from scistack_gui.ids import strip_placement
from scistack_gui.services.execution_service import (
    derive_target_for_node,
    disconnected_reason,
)

FN = "bandpass_filter"


class ParitySignal(BaseVariable):
    pass


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------


def _graph(client, scope="main"):
    r = client.get("/api/pipeline", params={"pipeline_id": scope})
    assert r.status_code == 200, r.text
    return r.json()


def _fn_node(graph) -> str:
    ids = [
        n["id"]
        for n in graph["nodes"]
        if n.get("type") == "functionNode" and n["data"].get("label") == FN
    ]
    assert len(ids) == 1, ids
    return ids[0]


def _history_edge(graph, node, source_prefix) -> dict:
    edges = [
        e
        for e in graph["edges"]
        if strip_placement(e["target"]) == strip_placement(node)
        and strip_placement(e["source"]).startswith(source_prefix)
        and not (e.get("data") or {}).get("manual")
    ]
    assert len(edges) == 1, edges
    return edges[0]


def _canvas_inputs(graph, node) -> tuple[dict[str, set], set]:
    """({argument: {variable labels}}, {Parameter-fed arguments}) drawn into
    *node*, whichever port spelling the edge uses."""
    variables: dict[str, set] = {}
    parameters: set = set()
    for e in graph["edges"]:
        if strip_placement(e["target"]) != strip_placement(node):
            continue
        handle = e.get("targetHandle") or ""
        arg = handle.split("__", 1)[1] if "__" in handle else handle
        source = strip_placement(e["source"])
        if source.startswith("var__"):
            variables.setdefault(arg, set()).add(source[len("var__"):])
        elif source.startswith("param__"):
            parameters.add(arg)
    return variables, parameters


def _run_inputs(node) -> tuple[list, dict[str, set], set]:
    targets = derive_target_for_node(get_db(), node)
    variables: dict[str, set] = {}
    parameters: set = set()
    for t in targets:
        for arg, types in (t.get("input_types") or {}).items():
            types = types if isinstance(types, list) else [types]
            variables.setdefault(arg, set()).update(types)
        parameters |= set((t.get("constants") or {}).keys())
    return targets, variables, parameters


def _assert_parity(client, node, scope="main"):
    """The run uses exactly what the canvas draws, and an unrunnable node
    gets a reason."""
    graph = _graph(client, scope)
    canvas_vars, canvas_params = _canvas_inputs(graph, node)
    targets, run_vars, run_params = _run_inputs(strip_placement(node))
    reason = disconnected_reason(get_db(), FN, strip_placement(node))
    if targets:
        assert run_vars == canvas_vars, (run_vars, canvas_vars)
        assert run_params == canvas_params, (run_params, canvas_params)
        assert reason is None, reason
    else:
        assert reason is not None, "the node cannot run but no reason is given"
    return targets, reason


# ---------------------------------------------------------------------------
# Scenario helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def setup(client):
    for subj in (1, 2):
        for sess in ("pre", "post"):
            ParitySignal.save(np.zeros(10), subject=subj, session=sess)
    graph = _graph(client)
    return client, _fn_node(graph), graph


def _draw(client, edge_id, source, target, handle):
    r = client.put(
        f"/api/edges/{edge_id}",
        json={"source": source, "target": target, "source_handle": None, "target_handle": handle},
    )
    assert r.status_code == 200, r.text


def _delete(client, edge):
    r = client.request(
        "DELETE",
        f"/api/edges/{edge['id']}",
        json={"edge_id": edge["id"], "source": edge["source"], "target": edge["target"]},
    )
    assert r.status_code == 200, r.text


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


def test_baseline(setup):
    client, node, _ = setup
    targets, _ = _assert_parity(client, node)
    assert targets


def test_hidden_history_edge_with_a_drawn_replacement(setup):
    client, node, graph = setup
    _delete(client, _history_edge(graph, node, "var__RawSignal"))
    _draw(client, "manual__par_new", "var__ParitySignal", node, "in__signal")
    targets, _ = _assert_parity(client, node)
    assert targets
    assert all(t["input_types"]["signal"] == "ParitySignal" for t in targets)


def test_stale_drawn_twin_under_a_hidden_history_edge(setup):
    """The GAITRiteLoaded case: a drawn copy of the history edge must not
    survive the history edge being deleted."""
    client, node, graph = setup
    _draw(client, "manual__par_twin", "var__RawSignal", node, "in__signal")
    _delete(client, _history_edge(_graph(client), node, "var__RawSignal"))
    _draw(client, "manual__par_new", "var__ParitySignal", node, "in__signal")
    targets, _ = _assert_parity(client, node)
    assert targets
    assert all(t["input_types"]["signal"] == "ParitySignal" for t in targets)


def test_hidden_with_nothing_drawn_is_disconnected(setup):
    client, node, graph = setup
    _delete(client, _history_edge(graph, node, "var__RawSignal"))
    targets, reason = _assert_parity(client, node)
    assert targets == []
    assert "signal" in reason


def test_duplicate_drawn_edges_are_one_source(setup):
    client, node, graph = setup
    _delete(client, _history_edge(graph, node, "var__RawSignal"))
    _draw(client, "manual__par_dup1", "var__ParitySignal", node, "in__signal")
    _draw(client, "manual__par_dup2", "var__ParitySignal", node, "in__signal")
    targets, _ = _assert_parity(client, node)
    assert targets
    assert all(t["input_types"]["signal"] == "ParitySignal" for t in targets)


def test_old_parameter_copy_on_an_input_port(setup):
    """The formulaNum case: deleting the history Parameter edge deletes the
    in__ copy drawn before the node ran with the Parameter."""
    client, node, graph = setup
    _draw(client, "manual__par_oldlow", "param__low_hz", node, "in__low_hz")
    _delete(client, _history_edge(_graph(client), node, "param__low_hz"))
    targets, reason = _assert_parity(client, node)
    assert targets == []
    assert "low_hz" in reason


@pytest.mark.xfail(
    strict=True,
    reason="Known before the refactor: the run path unions every scope's hidden "
    "edges, so an edge hidden in a hypothesis tab also disconnects the node for "
    "runs from main. Step 1 of plan-unified-edge-model fixes it (decision a); "
    "remove this marker then.",
)
def test_an_edge_hidden_in_another_tab_does_not_disconnect_main(setup):
    client, node, _ = setup
    r = client.post("/api/hypotheses/main/duplicate", json={"pipeline_id": "main", "name": "H"})
    assert r.status_code == 200, r.text
    other = r.json()["pipeline_id"]
    other_graph = _graph(client, other)
    other_node = _fn_node(other_graph)
    _delete(client, _history_edge(other_graph, other_node, "var__RawSignal"))
    # The other tab now draws no signal edge...
    assert "signal" not in _canvas_inputs(_graph(client, other), other_node)[0]
    # ...but main still does, and must still run.
    targets, _ = _assert_parity(client, node, "main")
    assert targets
