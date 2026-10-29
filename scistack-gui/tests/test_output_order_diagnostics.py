"""Stage 0 of .claude/plan-output-order-owner.md: pin today's output-order
behaviour before anything changes.

Output order is the order of ``outputs=`` in the for_each call. These tests
record where the canvas and the canvas run derive something else instead.
Each says which later stage changes it.
"""

from __future__ import annotations

import numpy as np
import pytest
from scidb import BaseVariable, for_each

from scistack_gui import registry as _registry
from scistack_gui.domain.graph_builder import build_function_nodes
from conftest import RawSignal


def _cid(seed: str) -> str:
    """A stable 16-hex call_id for a test node."""
    import hashlib

    return hashlib.sha256(seed.encode()).hexdigest()[:16]


def _wait_for_threads(prefix: str, timeout: float = 2.0) -> None:
    """Wait for background run threads to finish before DB teardown."""
    import threading
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not [t for t in threading.enumerate() if t.name.startswith(prefix)]:
            break
        time.sleep(0.05)


class OrderTen(BaseVariable):
    pass


class OrderSix(BaseVariable):
    pass


def two_outputs(signal):
    """A plain two-output function: (mean, max)."""
    arr = np.asarray(signal, dtype=float)
    return float(arr.mean()), float(arr.max())


def _run_two_outputs(outputs):
    for_each(
        two_outputs,
        inputs={"signal": RawSignal},
        outputs=outputs,
        subject=[1, 2],
        session=["pre", "post"],
    )


def _node(graph, label):
    (node,) = [n for n in graph["nodes"] if n["data"].get("label") == label]
    return node


@pytest.mark.xfail(strict=True, reason="Stage 2: handles are drawn alphabetically today")
def test_handles_follow_the_recorded_outputs_order(populated_db):
    """A code run with outputs=[OrderTen, OrderSix] should draw its output
    handles in that order. Today graph_builder sorts them alphabetically
    (OrderSix, OrderTen), so a split's tooltip and picker pair groups with
    the wrong outputs."""
    from scistack_gui.api.pipeline import _build_graph

    _run_two_outputs([OrderTen, OrderSix])
    graph = _build_graph(populated_db, "main", run_states=False)
    assert _node(graph, "two_outputs")["data"]["output_types"] == ["OrderTen", "OrderSix"]


def test_canvas_run_of_a_two_output_function_today(client, monkeypatch, populated_db):
    """What a canvas Run of an ordinary two-output Python function passes to
    for_each today. Records the current behaviour (Stage 3 decides whether to
    change it): one for_each call per surviving target, each with ONE output."""
    from scistack_gui.api import run as run_api

    _run_two_outputs([OrderTen, OrderSix])
    _registry._functions["two_outputs"] = two_outputs
    calls: list[list[str]] = []

    def fake_for_each(*args, **kwargs):
        calls.append([o.__name__ for o in kwargs.get("outputs") or []])
        return None

    monkeypatch.setattr(run_api, "for_each", fake_for_each)
    r = client.post(
        "/api/run",
        json={
            "function_name": "two_outputs",
            "variants": [],
            "output_types": ["OrderSix", "OrderTen"],
        },
    )
    assert r.status_code == 200
    _wait_for_threads("Thread-")
    print(f"[diagnostic] for_each outputs per call: {calls}")
    assert calls, "for_each was never called"
    # Today (user run 2026-10-28: [['OrderTen']]): ONE call writing ONE
    # output. deduplicate_variants keeps one target per variant, so the
    # function's other output is never written by a canvas run. Stage 3 fixes.
    assert len(calls) == 1 and len(calls[0]) == 1, calls


def test_matlab_split_function_handles_today():
    """A MATLAB function whose signature has ONE output, recorded writing TWO
    variables (a for_columns split). Records the current behaviour: the node
    draws only the signature's handle, so the second variable's edge has no
    handle to attach to (graph_builder logs the 'orphan' warning). Stages 2-3
    change this."""
    cid = _cid("split-call")
    key = ("matlab_split", cid)
    nodes = build_function_nodes(
        fn_input_params={key: {}},
        fn_outputs={key: {"OrderSix", "OrderTen"}},
        fn_constants={key: set()},
        fn_variants_map={key: []},
        fn_params_map={"matlab_split": []},
        run_states={},
        matlab_functions={"matlab_split"},
        saved_configs={"matlab_split": None},
        matlab_output_order={"matlab_split": ["result"]},
        matlab_param_to_class={"matlab_split": {"result": "OrderSix"}},
    )
    print(f"[diagnostic] MATLAB split handles: {nodes[0]['data']['output_types']}")
    assert nodes[0]["data"]["output_types"] == ["result"]
