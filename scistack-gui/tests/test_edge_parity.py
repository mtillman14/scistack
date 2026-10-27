"""Canvas-vs-run parity for a function node's inputs.

The safety net for the unified edge model (.claude/plan-unified-edge-model.md,
D-2026-10-01-1). For each scenario, three readers must agree on what feeds
the node:

- the CANVAS: the edges ``GET /api/pipeline`` draws into it;
- the RUN: ``execution_service.derive_target_for_node``;
- the "why can't this run" check: ``execution_service.disconnected_reason``.

Every scenario is a bug that was found by a failed run on 2026-10-01, plus the
hide-scope case decided for step 1. Written against the code BEFORE the
refactor (where the hide-scope case was xfail) and kept passing through it.

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
    # Since canvas adoption (2026-10-22) every edge is a stored row (and
    # carries `data.manual`), and the canvas draws one edge per connection, so
    # "the" edge from *source_prefix* into *node* is simply the one drawn. In
    # a duplicated tab it is the copy's own edge, not the original's.
    edges = [
        e
        for e in graph["edges"]
        if strip_placement(e["target"]) == strip_placement(node)
        and strip_placement(e["source"]).startswith(source_prefix)
    ]
    assert len(edges) == 1, edges
    return edges[0]


def _drawn_ports(graph, node) -> "set[str] | None":
    """The input ports *node* is sent with (``data.input_ports``), ``None``
    when the node carries none."""
    for n in graph["nodes"]:
        if strip_placement(n["id"]) == strip_placement(node):
            ports = n["data"].get("input_ports")
            if ports is None:
                return None
            return {p["name"] if isinstance(p, dict) else p for p in ports}
    return None


def _canvas_inputs(graph, node) -> tuple[dict[str, set], set]:
    """({argument: {variable labels}}, {Parameter-fed arguments}) drawn into
    *node*, whichever port spelling the edge uses.

    What the user SEES, as React Flow renders it: an edge whose target port is
    not among the node's ``input_ports`` is dropped without a word, so it is
    not counted. Reading the raw edge list instead is how an invisible edge
    on a renamed argument passed this suite while it broke the run
    (2026-10-27, ``df_in``)."""
    variables: dict[str, set] = {}
    parameters: set = set()
    ports = _drawn_ports(graph, node)
    for e in graph["edges"]:
        if strip_placement(e["target"]) != strip_placement(node):
            continue
        handle = e.get("targetHandle") or ""
        arg = handle.split("__", 1)[1] if "__" in handle else handle
        if ports is not None and arg not in ports:
            continue
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


def test_an_edge_hidden_in_another_tab_does_not_disconnect_main(setup):
    """Was xfail before step 1: the run path unioned every scope's hidden
    edges. Run-path hides now come from the node's scope
    (edge_view.run_scope, decision a)."""
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


def test_a_duplicate_carries_the_source_tabs_hides(setup):
    """Hides are per scope, and a duplicate is drawn from the source's VISIBLE
    graph, so a hidden history edge came back in the copy once its nodes
    graduated (scistack.log 2026-10-01 23:08: formulaNum disconnected in main,
    connected in "main copy 4"). _clone_nodes now carries the hides."""
    client, node, graph = setup
    _delete(client, _history_edge(graph, node, "param__low_hz"))
    r = client.post("/api/hypotheses/main/duplicate", json={"pipeline_id": "main", "name": "H"})
    assert r.status_code == 200, r.text
    other = r.json()["pipeline_id"]
    other_graph = _graph(client, other)
    other_node = _fn_node(other_graph)
    assert "low_hz" not in _canvas_inputs(other_graph, other_node)[1]
    # The source tab is unchanged.
    assert "low_hz" not in _canvas_inputs(_graph(client), node)[1]


# ---------------------------------------------------------------------------
# A renamed argument (2026-10-27: df_in -> data_path)
# ---------------------------------------------------------------------------


def _rename_argument(monkeypatch, old, new):
    """Make FN's signature read *new* where it read *old*, through the one
    signature reader the canvas and edge_view share."""
    import inspect

    from scifor.signature import Argument

    from scistack_gui.api import pipeline as pipeline_api

    real = pipeline_api._fn_arguments

    def renamed(fn_name):
        arguments = real(fn_name)
        if fn_name != FN:
            return arguments
        return [
            Argument(new, a.kind, a.default) if a.name == old else a
            for a in arguments
        ]

    monkeypatch.setattr(pipeline_api, "_fn_arguments", renamed)
    return lambda: monkeypatch.setattr(pipeline_api, "_fn_arguments", real)


def _run_arguments(node) -> set:
    targets = derive_target_for_node(get_db(), strip_placement(node))
    names: set = set()
    for t in targets:
        names |= set((t.get("input_types") or {}).keys())
        names |= set((t.get("constants") or {}).keys())
        names |= set((t.get("path_inputs") or {}).keys())
    return names


def test_an_edge_on_a_renamed_argument_binds_nothing(setup, monkeypatch):
    """Before Refresh Code disconnects it: the edge on the old port is not
    drawn (no such port), so it must not bind either. It did, and the run
    passed ``df_in=`` to a function that takes ``data_path``."""
    from scistack_gui.domain.edge_view import effective_edges

    client, node, _ = setup
    _rename_argument(monkeypatch, "signal", "waveform")
    view = effective_edges(get_db(), "main", caller="test")
    assert [e["targetHandle"] for e in view.stale_port] == ["in__signal"]
    assert "signal" not in _run_arguments(node)
    _assert_parity(client, node)


def test_refresh_disconnects_a_renamed_argument_for_good(setup, monkeypatch):
    """Refresh Code hides the edge (user decision 2026-10-27): renaming the
    argument back does NOT bring it back; the user redraws it."""
    from scistack_gui.services.layout_service import disconnect_removed_arguments

    client, node, _ = setup
    restore = _rename_argument(monkeypatch, "signal", "waveform")
    disconnected = disconnect_removed_arguments(get_db())
    assert [d["port"] for d in disconnected] == ["signal"]
    assert disconnect_removed_arguments(get_db()) == []  # nothing left to do

    restore()
    assert "signal" not in _canvas_inputs(_graph(client), node)[0]
    targets, reason = _assert_parity(client, node)
    assert targets == []
    assert "signal" in reason


def test_redrawing_after_a_disconnect_reconnects(setup, monkeypatch):
    from scistack_gui.services.layout_service import disconnect_removed_arguments

    client, node, _ = setup
    restore = _rename_argument(monkeypatch, "signal", "waveform")
    disconnect_removed_arguments(get_db())
    restore()
    _draw(client, "manual__par_redraw", "var__RawSignal", node, "in__signal")
    targets, _ = _assert_parity(client, node)
    assert targets
    assert all(t["input_types"]["signal"] == "RawSignal" for t in targets)


def test_refresh_leaves_live_arguments_alone(setup):
    from scistack_gui.services.layout_service import disconnect_removed_arguments

    client, node, _ = setup
    assert disconnect_removed_arguments(get_db()) == []
    targets, _ = _assert_parity(client, node)
    assert targets
