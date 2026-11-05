"""An edge into a function must name the argument it feeds.

``edge_resolver`` binds an input only through its ``in__{argument}`` port, so an
edge saved without one is ignored on every read and the function runs without
that input. Found 2026-11-05: an edge drawn while ``combine_groups`` had no
arguments lost its input for good, and MATLAB said "Not enough input arguments".
"""

from __future__ import annotations

import logging
from pathlib import Path

from scistack_gui.runtime.db import get_db
from scistack_gui.stores import pipeline_store as ps

FUNCTION_NODE_TSX = (
    Path(__file__).resolve().parents[1]
    / "frontend" / "src" / "components" / "DAG" / "FunctionNode.tsx"
)


def _place(client):
    for node_id, node_type, label in (
        ("mv_in", "variableNode", "RawSignal"),
        ("mf_proc", "functionNode", "custom_proc"),
        ("mv_out", "variableNode", "FilteredSignal"),
    ):
        r = client.put(
            f"/api/layout/{node_id}",
            json={"x": 0, "y": 0, "node_type": node_type, "label": label},
        )
        assert r.status_code == 200, r.text


def _stored_ids():
    return {e["id"] for e in ps.get_manual_edges(get_db())}


def test_a_portless_edge_into_a_function_is_refused_and_not_stored(client, caplog):
    _place(client)
    with caplog.at_level(logging.WARNING, logger="scistack_gui"):
        r = client.put(
            "/api/edges/e_in",
            json={"source": "mv_in", "target": "mf_proc", "target_handle": None},
        )
    assert r.status_code == 400
    assert "custom_proc" in r.json()["detail"]
    assert "input port" in r.json()["detail"]
    assert "e_in" not in _stored_ids()
    assert any(
        "put_edge rejected" in m and "e_in" in m for m in caplog.messages
    )


def test_a_function_is_recognised_by_its_id_and_by_its_placement(client):
    r = client.put(
        "/api/edges/e_id",
        json={"source": "var__RawSignal", "target": "fn__somewhere"},
    )
    assert r.status_code == 400
    _place(client)
    r = client.put(
        "/api/edges/e_placed",
        json={"source": "mv_in", "target": "mf_proc::main"},
    )
    assert r.status_code == 400


def test_an_edge_onto_an_argument_is_stored(client):
    _place(client)
    r = client.put(
        "/api/edges/e_in",
        json={"source": "mv_in", "target": "mf_proc", "target_handle": "in__signal"},
    )
    assert r.status_code == 200, r.text
    assert "e_in" in _stored_ids()


def test_an_output_edge_needs_no_target_port(client):
    """The variable an output edge lands on is the binding."""
    _place(client)
    r = client.put(
        "/api/edges/e_out",
        json={"source": "mf_proc", "target": "mv_out", "target_handle": None},
    )
    assert r.status_code == 200, r.text
    assert "e_out" in _stored_ids()


def test_function_nodes_draw_no_input_handle_except_their_ports():
    """The canvas half of the rule. A generic target handle on a function with
    no ports is what produced the portless edge: every target handle in
    FunctionNode.tsx must carry a port id."""
    source = FUNCTION_NODE_TSX.read_text(encoding="utf-8")
    assert '<Handle type="target"' not in source
