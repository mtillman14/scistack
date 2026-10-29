"""Python output ports declared on the node (.claude/plan-python-output-ports.md).

Inputs come from the signature in both languages; a Python function declares
no outputs, so the user names its output ports on the node (``outputPorts``).
Port i is return value i; the port's EDGE decides its variable. A Python
node with declared ports is drawn and run exactly like a MATLAB signature's
outputs (same maps, same handles ``out__{port}``).
"""

from __future__ import annotations

import numpy as np
import pytest
from scidb import BaseVariable, for_each

from conftest import RawSignal
from scistack_gui import history
from scistack_gui import registry as _registry


class PortTen(BaseVariable):
    pass


class PortSix(BaseVariable):
    pass


class PortThree(BaseVariable):
    pass


def ported(signal):
    arr = np.asarray(signal, dtype=float)
    return float(arr.mean()), float(arr.max())


@pytest.fixture(autouse=True)
def _fresh_history():
    history.clear()
    yield
    history.clear()


def _code_run(outputs):
    for_each(
        ported,
        inputs={"signal": RawSignal},
        outputs=outputs,
        subject=[1, 2],
        session=["pre", "post"],
    )


def _fn_node(client, label="ported") -> dict:
    nodes = client.get("/api/pipeline").json()["nodes"]
    (node,) = [n for n in nodes if n["data"].get("label") == label]
    return node


def _set_ports(client, nid, ports, renames=None, status=200):
    r = client.put(
        f"/api/layout/{nid}/outputs",
        json={"node_id": nid, "ports": ports, "renames": renames or {}},
    )
    assert r.status_code == status, r.text
    return r.json()


def _out_edges(client, nid):
    edges = client.get("/api/pipeline").json()["edges"]
    return {e["target"]: e.get("sourceHandle") for e in edges if e["source"] == nid}


# --- Drawing ---------------------------------------------------------------


def test_automatic_ports_are_the_recorded_outputs(client):
    _code_run([PortTen, PortSix])
    assert _fn_node(client)["data"]["output_types"] == ["PortTen", "PortSix"]


def test_declared_ports_are_drawn_and_mapped_by_position_from_the_script_run(client):
    _code_run([PortTen, PortSix])
    nid = _fn_node(client)["id"]
    _set_ports(client, nid, ["mean", "peak"])
    data = _fn_node(client)["data"]
    assert data["outputPorts"] == ["mean", "peak"]
    assert data["output_types"] == ["mean", "peak"]
    assert _out_edges(client, nid) == {
        "var__PortTen": "out__mean",
        "var__PortSix": "out__peak",
    }


def test_reordering_declared_ports_reorders_handles_and_notes_the_script_order(client):
    _code_run([PortTen, PortSix])
    nid = _fn_node(client)["id"]
    _set_ports(client, nid, ["mean", "peak"])
    # Declaring wrote each port's variable as an edge, so moving peak first
    # keeps peak on PortSix (by position it would have become PortTen).
    _set_ports(client, nid, ["peak", "mean"])
    data = _fn_node(client)["data"]
    assert data["output_types"] == ["peak", "mean"]
    assert _out_edges(client, nid) == {
        "var__PortTen": "out__mean",
        "var__PortSix": "out__peak",
    }
    # The script run listed [PortTen, PortSix]; the canvas order differs (3a).
    assert "PortTen" in data["outputOrderNote"]


def test_bad_or_repeated_names_are_refused(client):
    _code_run([PortTen, PortSix])
    nid = _fn_node(client)["id"]
    r = client.put(f"/api/layout/{nid}/outputs", json={"node_id": nid, "ports": ["a b"]})
    assert r.status_code >= 400
    r = client.put(f"/api/layout/{nid}/outputs", json={"node_id": nid, "ports": ["a", "a"]})
    assert r.status_code >= 400


def test_an_empty_list_returns_to_automatic(client):
    _code_run([PortTen, PortSix])
    nid = _fn_node(client)["id"]
    _set_ports(client, nid, ["mean", "peak"])
    _set_ports(client, nid, [])
    data = _fn_node(client)["data"]
    assert "outputPorts" not in data
    assert data["output_types"] == ["PortTen", "PortSix"]


# --- Rename re-points edges ------------------------------------------------------


def test_rename_repoints_the_ports_manual_edges(client, populated_db):
    from scistack_gui import pipeline_store

    _code_run([PortTen, PortSix])
    nid = _fn_node(client)["id"]
    pipeline_store.write_manual_edge(
        populated_db,
        {"id": "e_ten", "source": nid, "target": "var__PortTen",
         "sourceHandle": "out__PortTen", "targetHandle": None},
    )
    pipeline_store.write_manual_edge(
        populated_db,
        {"id": "e_six", "source": nid, "target": "var__PortSix",
         "sourceHandle": None, "targetHandle": None},  # drawn from the default handle
    )
    out = _set_ports(
        client, nid, ["mean", "peak"], {"PortTen": "mean", "PortSix": "peak"}
    )
    assert out["repointed"] == 2
    handles = {e["id"]: e.get("sourceHandle") for e in pipeline_store.get_manual_edges(populated_db)}
    assert handles["e_ten"] == "out__mean" and handles["e_six"] == "out__peak"


# --- Mapping precedence (domain.output_order.declared_python_ports) -------------


def test_mapping_precedence_edges_then_names_then_position():
    from scistack_gui.domain.output_order import declared_python_ports

    configs = {"fn__f__abc": {"outputPorts": ["a", "b", "c"]}}
    edges = [{"source": "fn__f__abc", "target": "var__C1", "sourceHandle": "out__c"}]
    ports, mapping = declared_python_ports(
        configs,
        {},
        set(),
        {"f": {"fn__f__abc"}},
        edges,
        {"var__C1": "C1"},
        {"f": [["A1", "B1", "C1"]]},
        {"f": {"b": "B2"}},
    )
    assert ports == {"f": ["a", "b", "c"]}
    # c from its edge, b from the recorded name, a by position (C1 taken).
    assert mapping == {"f": {"c": "C1", "b": "B2", "a": "A1"}}
    # A MATLAB function never takes declared Python ports.
    assert declared_python_ports(configs, {}, {"f"}, {}, [], {}, {}, {}) == ({}, {})


def test_declared_order_note():
    from scistack_gui.domain.output_order import declared_order_note

    mapping = {"peak": "Six", "mean": "Ten"}
    assert "Ten" in declared_order_note(["peak", "mean"], mapping, [["Ten", "Six"]])
    assert declared_order_note(["mean", "peak"], mapping, [["Ten", "Six"]]) is None
    assert declared_order_note(["mean"], {"mean": "Ten"}, None) is None


# --- Run ---------------------------------------------------------------------------


def test_canvas_run_writes_ports_in_port_order_and_records_their_names(
    client, monkeypatch, populated_db
):
    from scistack_gui.api import run as run_api

    _code_run([PortTen, PortSix])
    nid = _fn_node(client)["id"]
    _set_ports(client, nid, ["mean", "peak"])
    _set_ports(client, nid, ["peak", "mean"])
    _registry._functions["ported"] = ported
    seen = []

    def fake_for_each(*a, **k):
        from scidb.intent import current_output_names

        seen.append(([o.__name__ for o in k.get("outputs") or []], current_output_names()))

    monkeypatch.setattr(run_api, "for_each", fake_for_each)
    r = client.post(
        "/api/run",
        json={"function_name": "ported", "variants": [], "node_id": nid,
              "output_types": ["peak", "mean"]},
    )
    assert r.status_code == 200
    _wait_for_threads("Thread-")
    assert seen == [(["PortSix", "PortTen"], ["peak", "mean"])]


def test_an_unconnected_port_refuses_the_run(client, populated_db):
    from scistack_gui.services.execution_service import (
        UnconnectedOutputPortError,
        declared_output_classes,
    )

    _code_run([PortTen, PortSix])
    nid = _fn_node(client)["id"]
    _set_ports(client, nid, ["mean", "peak", "spare"])
    with pytest.raises(UnconnectedOutputPortError, match="spare"):
        declared_output_classes(populated_db, nid, "ported")


def _wait_for_threads(prefix: str, timeout: float = 2.0) -> None:
    import threading
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not [t for t in threading.enumerate() if t.name.startswith(prefix)]:
            break
        time.sleep(0.05)
