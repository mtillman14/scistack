"""One owner for a function node's output order
(.claude/plan-output-order-owner.md).

Output order is the order of ``outputs=`` in the for_each call. A run records
it (scidb ``_run_outputs``); ``domain.output_order`` turns it into the node's
handle order; the canvas run, the MATLAB command, the picker and the tooltips
read the handles. Started as the Stage 0 diagnostics that pinned where each
of those derived its own order.
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


def test_handles_follow_the_recorded_outputs_order(populated_db):
    """A code run with outputs=[OrderTen, OrderSix] draws its output handles
    in that order (domain.output_order, from _run_outputs). Until 2026-10-28
    graph_builder sorted them alphabetically, so a split's tooltip and picker
    paired groups with the wrong outputs. A later run in the other order wins."""
    from scistack_gui.api.pipeline import _build_graph

    _run_two_outputs([OrderTen, OrderSix])
    graph = _build_graph(populated_db, "main", run_states=False)
    assert _node(graph, "two_outputs")["data"]["output_types"] == ["OrderTen", "OrderSix"]

    _run_two_outputs([OrderSix, OrderTen])
    graph = _build_graph(populated_db, "main", run_states=False)
    assert _node(graph, "two_outputs")["data"]["output_types"] == ["OrderSix", "OrderTen"]


def test_canvas_run_of_a_two_output_function_writes_both(client, monkeypatch, populated_db):
    """A canvas Run of an ordinary two-output Python function writes BOTH
    outputs in one call, in the node's handle order. Until 2026-10-28 it made
    one call writing one output ([['OrderTen']], user run), so the second
    output was never written (execution_service.run_output_types)."""
    from scistack_gui.api.pipeline import _build_graph
    from scistack_gui.api import run as run_api

    _run_two_outputs([OrderTen, OrderSix])
    _registry._functions["two_outputs"] = two_outputs
    handles = _node(_build_graph(populated_db, "main", run_states=False), "two_outputs")[
        "data"
    ]["output_types"]
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
            "output_types": handles,
        },
    )
    assert r.status_code == 200
    _wait_for_threads("Thread-")
    print(f"[diagnostic] for_each outputs per call: {calls}")
    assert calls, "for_each was never called"
    assert handles == ["OrderTen", "OrderSix"]
    assert calls == [["OrderTen", "OrderSix"]], calls


def test_matlab_split_function_draws_class_handles_in_recorded_order():
    """A MATLAB function whose signature has ONE output, recorded writing TWO
    variables (a for_columns split). It draws one handle per output CLASS in
    recorded order; before 2026-10-28 it drew only the signature's handle and
    the second variable's edge had nowhere to attach."""
    from scistack_gui.api.pipeline import matlab_class_handle_functions

    recorded = {"matlab_split": [["OrderTen", "OrderSix"]], "matlab_plain": [["Time"]]}
    order = {"matlab_split": ["result"], "matlab_plain": ["time"]}
    assert matlab_class_handle_functions(
        {"matlab_split", "matlab_plain"}, order, recorded
    ) == ["matlab_split"]

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
        # What _build_graph passes once the function is a class-handle one.
        matlab_output_order={},
        matlab_param_to_class={},
        recorded_output_orders=recorded,
    )
    assert nodes[0]["data"]["output_types"] == ["OrderTen", "OrderSix"]


# --- The owner (domain.output_order) ----------------------------------------------


def test_node_output_order_rules():
    from scistack_gui.domain.output_order import node_output_order

    # Recorded order wins, restricted to what is still wired.
    assert node_output_order({"A", "B", "C"}, [["C", "A"], ["A", "B"]]) == ["C", "A", "B"]
    # Newest recorded order that names any wired output.
    assert node_output_order({"B"}, [["X"], ["B"]]) == ["B"]
    # No record: declared (MATLAB signature), then drawn edge order.
    assert node_output_order(["B", "A"], None, edge_order=["B", "A"]) == ["B", "A"]
    assert node_output_order(["B", "A"], None, edge_order=["B", "A"], declared=["A"]) == ["A", "B"]
    # Nothing knows: alphabetical, last.
    assert node_output_order({"B", "A"}) == ["A", "B"]


def test_writes_together():
    from scistack_gui.domain.output_order import writes_together

    assert not writes_together(["A"], None)
    assert writes_together(["A", "B"], None)  # never ran, two drawn outputs
    assert writes_together(["A", "B"], [["B", "A"]])
    assert not writes_together(["A", "B"], [["A"], ["B"]])  # separate runs


def test_run_output_types_follows_the_record():
    from scistack_gui.services.execution_service import run_output_types

    assert run_output_types({}, ["A", "B"], "A", [["A", "B"]]) == ["A", "B"]
    assert run_output_types({}, ["A", "B"], "B", [["A"], ["B"]]) == ["B"]
    assert run_output_types({}, ["A"], "A", None) == ["A"]


def test_manual_edges_come_back_in_drawn_order(populated_db):
    from scistack_gui import pipeline_store

    for i, target in enumerate(["var__Z", "var__A", "var__M"]):
        pipeline_store.write_manual_edge(
            populated_db,
            {"id": f"e{i}", "source": "fn__x__1", "target": target,
             "sourceHandle": f"out__{target[5:]}", "targetHandle": "in"},
        )
    targets = [e["target"] for e in pipeline_store.get_manual_edges(populated_db)]
    assert targets == ["var__Z", "var__A", "var__M"]


def test_matlab_command_lists_outputs_in_the_owner_order():
    """With history, a MATLAB run's outputs cell follows the order passed as
    output_types (domain.output_order), not variant-row order."""
    from scistack_gui.api.matlab_command import generate_matlab_command

    variants = [
        {"input_types": {"t": "Walks"}, "output_type": "SixMWT", "constants": {}},
        {"input_types": {"t": "Walks"}, "output_type": "TenMWT", "constants": {}},
    ]
    cmd = generate_matlab_command(
        "split_fn", "/tmp/x.duckdb", ["subject"], variants=variants,
        output_types=["TenMWT", "SixMWT"],
    )
    assert "{TenMWT(), SixMWT()}" in cmd
    unordered = generate_matlab_command(
        "split_fn", "/tmp/x.duckdb", ["subject"], variants=variants
    )
    assert "{SixMWT(), TenMWT()}" in unordered  # variant-row order, as before


# --- Outputs changing over time (tests 1-6 of the user's list, 2026-10-28) ----


class OrderThree(BaseVariable):
    pass


_N_OUTPUTS = [2]


def changing_outputs(signal):
    """Returns the first _N_OUTPUTS[0] of (mean, max, min): stands in for a
    function whose return count is edited between runs (the hash ignores the
    global, so it stays the same call site)."""
    arr = np.asarray(signal, dtype=float)
    return (float(arr.mean()), float(arr.max()), float(arr.min()))[: _N_OUTPUTS[0]]


def _code_run(n, outputs):
    _N_OUTPUTS[0] = n
    for_each(
        changing_outputs,
        inputs={"signal": RawSignal},
        outputs=outputs,
        subject=[1, 2],
        session=["pre", "post"],
    )


def _canvas_run(client, monkeypatch, db, label, fn):
    """(node handles, [outputs of each for_each call]) for a canvas Run."""
    from scistack_gui.api import run as run_api
    from scistack_gui.api.pipeline import _build_graph

    _registry._functions[label] = fn
    handles = _node(_build_graph(db, "main", run_states=False), label)["data"]["output_types"]
    calls: list[list[str]] = []
    monkeypatch.setattr(
        run_api,
        "for_each",
        lambda *a, **k: calls.append([o.__name__ for o in k.get("outputs") or []]),
    )
    r = client.post(
        "/api/run",
        json={"function_name": label, "variants": [], "output_types": handles},
    )
    assert r.status_code == 200
    _wait_for_threads("Thread-")
    return handles, calls


def test_1_adding_an_output_in_code(client, monkeypatch, populated_db):
    _code_run(2, [OrderTen, OrderSix])
    _code_run(3, [OrderTen, OrderSix, OrderThree])
    handles, calls = _canvas_run(client, monkeypatch, populated_db, "changing_outputs", changing_outputs)
    assert handles == ["OrderTen", "OrderSix", "OrderThree"]
    assert calls == [["OrderTen", "OrderSix", "OrderThree"]]


def test_2_removing_an_output_in_code(client, monkeypatch, populated_db):
    """The dropped output keeps a handle (its records still exist), drawn
    after the newest run's outputs; a canvas Run does NOT pass it, since the
    function no longer returns it."""
    _code_run(3, [OrderTen, OrderSix, OrderThree])
    _code_run(2, [OrderTen, OrderThree])
    handles, calls = _canvas_run(client, monkeypatch, populated_db, "changing_outputs", changing_outputs)
    assert handles == ["OrderTen", "OrderThree", "OrderSix"]
    assert calls == [["OrderTen", "OrderThree"]]


def test_3_an_output_connected_on_the_canvas_joins_after_the_recorded_ones():
    from scistack_gui.domain.output_order import node_output_order
    from scistack_gui.services.execution_service import run_output_types

    recorded = [["A", "B"]]
    assert node_output_order({"A", "B", "C"}, recorded, edge_order=["C"]) == ["A", "B", "C"]
    # Never recorded, so newly wired: passed with the recorded ones.
    assert run_output_types({}, ["A", "B", "C"], "A", recorded) == ["A", "B", "C"]


def test_5_return_order_changed_in_code_keeps_the_recorded_order(
    client, monkeypatch, populated_db
):
    """Editing `return a, b` to `return b, a` without touching outputs= is
    invisible: the function body changed, not its outputs. The canvas keeps
    the recorded order, the order the script's next run will record again."""
    _code_run(2, [OrderTen, OrderSix])
    handles, calls = _canvas_run(client, monkeypatch, populated_db, "changing_outputs", changing_outputs)
    assert handles == ["OrderTen", "OrderSix"]
    assert calls == [["OrderTen", "OrderSix"]]


def test_6_matlab_signature_reordered_after_a_run_maps_by_name():
    """A run of `function [time, force] = f` wrote [Time, Force]; the .m is
    then edited to `[force, time]`. Each name keeps its variable, and the
    MATLAB outputs cell follows the NEW signature (MATLAB pairs by position)."""
    from scistack_gui.api.pipeline import _matlab_param_to_class_from_db
    from scistack_gui.domain.output_order import matlab_output_order

    by_name = {"f": {"time": "Time", "force": "Force"}}
    mapping = _matlab_param_to_class_from_db(
        {("f", "cid"): {"variants": [
            {"output_type": "Time", "output_num": 0},
            {"output_type": "Force", "output_num": 1},
        ]}},
        {"f"},
        {"f": ["force", "time"]},  # the edited signature
        by_name,
    )
    assert mapping == {"f": {"force": "Force", "time": "Time"}}
    assert matlab_output_order(["force", "time"], mapping["f"], ["Time", "Force"]) == [
        "Force",
        "Time",
    ]
    # Without recorded names the slot numbers decide: the swap this replaces.
    by_slot = _matlab_param_to_class_from_db(
        {("f", "cid"): {"variants": [
            {"output_type": "Time", "output_num": 0},
            {"output_type": "Force", "output_num": 1},
        ]}},
        {"f"},
        {"f": ["force", "time"]},
        {},
    )
    assert by_slot == {"f": {"force": "Time", "time": "Force"}}


def test_6b_matlab_split_keeps_the_recorded_order():
    from scistack_gui.domain.output_order import matlab_output_order

    # One signature output, two variables: the recorded pairing, not the signature.
    assert matlab_output_order(["result"], {}, ["SixMWT", "TenMWT"], [["TenMWT", "SixMWT"]]) == [
        "TenMWT",
        "SixMWT",
    ]
