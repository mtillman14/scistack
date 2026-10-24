"""Optional-argument ports, end to end (.claude/plan-optional-argument-ports.md).

A function node draws its required arguments and whatever feeds it; every
other defaulted argument waits behind "+ argument" (``data.optional_ports``).
Regression for the Best Popcorn walkthrough: ``pandas.read_csv`` drew ~50
handles on one short node edge, so ``sep`` could not be wired.

The rule is ``graph_builder.port_visibility`` (unit cases in
test_graph_builder.py::TestOptionalPorts); this file drives it through the
HTTP transport: drop, wire, reveal, undo.
"""

from __future__ import annotations

import json

import pytest

from scistack_gui import history
from scistack_gui.api.handlers import CHANGE_HEADER
from scistack_gui.ids import new_manual_edge_id, new_manual_node_id

FN = "pandas.read_csv"


@pytest.fixture(autouse=True)
def _fresh_history():
    history.clear()
    yield
    history.clear()


def _place(client, ntype: str, label: str) -> str:
    nid = new_manual_node_id(ntype, label)
    r = client.put(f"/api/layout/{nid}", json={
        "node_id": nid, "x": 100.0, "y": 100.0, "node_type": ntype, "label": label,
    })
    assert r.status_code == 200, r.text
    return nid


def _node(client, nid: str) -> dict:
    nodes = client.get("/api/pipeline").json()["nodes"]
    match = [n for n in nodes if n["id"] == nid]
    assert match, f"{nid} not in {[n['id'] for n in nodes]}"
    return match[0]["data"]


def _optional(data: dict) -> list[str]:
    return [o["name"] for o in data.get("optional_ports") or []]


def _show(client, nid: str, argument: str, shown: bool = True, change: str | None = None):
    headers = {CHANGE_HEADER: json.dumps({"id": change, "label": None})} if change else {}
    r = client.put(
        f"/api/layout/{nid}/arguments",
        json={"node_id": nid, "argument": argument, "shown": shown},
        headers=headers,
    )
    assert r.status_code == 200, r.text
    return r.json()


class TestDroppedReadCsv:
    def test_shows_only_the_required_argument(self, client):
        nid = _place(client, "functionNode", FN)
        data = _node(client, nid)
        assert data["input_ports"] == ["filepath_or_buffer"]
        assert "sep" in _optional(data)
        assert list(data["input_params"]) == ["filepath_or_buffer"]

    def test_drop_info_uses_the_same_rule(self, client):
        """get_function_params (the optimistic node) == the canvas build."""
        info = client.get(f"/api/function/{FN}/params").json()
        nid = _place(client, "functionNode", FN)
        data = _node(client, nid)
        assert info["input_ports"] == data["input_ports"]
        assert info["optional_ports"] == data["optional_ports"]
        # The full signature is still there for readers that need it.
        assert "sep" in info["params"]

    def test_a_drawn_edge_shows_its_argument(self, client):
        fn = _place(client, "functionNode", FN)
        var = _place(client, "variableNode", "Cols")
        eid = new_manual_edge_id()
        r = client.put(f"/api/edges/{eid}", json={
            "edge_id": eid, "source": var, "target": fn,
            "source_handle": None, "target_handle": "in__usecols",
        })
        assert r.status_code == 200, r.text
        data = _node(client, fn)
        assert data["input_ports"] == ["filepath_or_buffer", "usecols"]
        assert "usecols" not in _optional(data)


class TestReveal:
    def test_reveal_persists_across_refetch(self, client):
        nid = _place(client, "functionNode", FN)
        assert _show(client, nid, "sep")["shownArguments"] == ["sep"]
        data = _node(client, nid)
        assert data["input_ports"] == ["filepath_or_buffer", "sep"]
        assert "sep" not in _optional(data)
        # And again: a second refetch is the same (nothing ephemeral).
        assert _node(client, nid)["input_ports"] == ["filepath_or_buffer", "sep"]

    def test_hide_again(self, client):
        nid = _place(client, "functionNode", FN)
        _show(client, nid, "sep")
        _show(client, nid, "sep", shown=False)
        assert _node(client, nid)["input_ports"] == ["filepath_or_buffer"]

    def test_reveal_keeps_the_rest_of_the_config(self, client):
        nid = _place(client, "functionNode", FN)
        client.put(f"/api/layout/{nid}/config", json={
            "node_id": nid, "config": {"whereFilters": [{"key": "probe"}]},
        })
        _show(client, nid, "header")
        data = _node(client, nid)
        assert data.get("whereFilters") == [{"key": "probe"}]
        assert "header" in data["input_ports"]

    def test_reveal_is_one_undo_step(self, client):
        nid = _place(client, "functionNode", FN)
        _show(client, nid, "sep", change="show-sep")
        assert "sep" in _node(client, nid)["input_ports"]
        r = client.post("/api/history/undo", json={"change_id": "show-sep"}).json()
        assert r["status"] == "ok"
        assert _node(client, nid)["input_ports"] == ["filepath_or_buffer"]
        r = client.post("/api/history/redo", json={"change_id": "show-sep"}).json()
        assert r["status"] == "ok"
        assert "sep" in _node(client, nid)["input_ports"]
