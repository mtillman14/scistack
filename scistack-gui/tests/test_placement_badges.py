"""A submodule placement's two badges (GitHub #27, user decisions 2026-11-19).

Pure: ``ports.open_ports_of_placement`` and ``ports.runnable_routes``. The
integration side (the badges on ``/api/pipeline``) is in
``test_submodule_ports.py::TestPortFedStepIsConnected``. Rule:
``docs/claude/submodule-ports.md``, "A placement's badges".
"""

from __future__ import annotations

from scistack_gui.domain.ports import open_ports_of_placement, runnable_routes


def _n(node_id, node_type, label, off=False):
    data = {"label": label}
    if off:
        data["disconnected"] = True
    return {"id": node_id, "type": node_type, "data": data}


def _e(source, target, sh=None, th=None):
    return {"source": source, "target": target, "sourceHandle": sh, "targetHandle": th}


# in(A) -> fcn_b -> VarC -> out(C), and in(A) -> fcn_x (disconnected) -> out(Y)
SUB_NODES = [
    _n("pin", "inputPortNode", "A"),
    _n("B", "functionNode", "fcn_b"),
    _n("C", "variableNode", "VarC"),
    _n("pout", "outputPortNode", "C"),
    _n("X", "functionNode", "fcn_x", off=True),
    _n("pouty", "outputPortNode", "Y"),
]
SUB_EDGES = [
    _e("pin", "B", th="in__a"),
    _e("B", "C"),
    _e("C", "pout"),
    _e("pin", "X", th="in__a"),
    _e("X", "pouty"),
]


class TestOpenPorts:
    def test_a_port_with_no_line_on_the_parent_is_open(self):
        got = open_ports_of_placement(
            "use_1", {"inputs": ["A", "B"], "outputs": ["C"]}, [_e("v", "use_1", th="in__A")]
        )
        assert got == {"inputs": ["B"], "outputs": ["C"]}

    def test_another_placements_lines_do_not_fill_this_one(self):
        got = open_ports_of_placement(
            "use_1", {"inputs": ["A"], "outputs": []}, [_e("v", "use_2", th="in__A")]
        )
        assert got == {"inputs": ["A"], "outputs": []}

    def test_every_port_connected_is_empty(self):
        got = open_ports_of_placement(
            "use_1",
            {"inputs": ["A"], "outputs": ["C"]},
            [_e("v", "use_1", th="in__A"), _e("use_1", "w", sh="out__C")],
        )
        assert got == {"inputs": [], "outputs": []}


class TestRunnableRoutes:
    def test_a_disconnected_branch_leaves_the_other_route(self):
        assert runnable_routes("sub", {"sub": (SUB_NODES, SUB_EDGES)}, {}) == {("A", "C")}

    def test_every_step_disconnected_is_no_route(self):
        nodes = [
            {**n, "data": {**n["data"], "disconnected": True}} if n["type"] == "functionNode" else n
            for n in SUB_NODES
        ]
        assert runnable_routes("sub", {"sub": (nodes, SUB_EDGES)}, {}) == frozenset()

    def test_a_path_input_inside_starts_a_route(self):
        """A library that loads its own data runs with no input port."""
        canvas = (
            [_n("pi", "pathInputNode", "raw"), _n("L", "functionNode", "load"),
             _n("po", "outputPortNode", "Out")],
            [_e("pi", "L", th="in__path"), _e("L", "po")],
        )
        assert runnable_routes("lib", {"lib": canvas}, {}) == {(None, "Out")}

    def test_a_nested_placement_is_traced_through(self):
        outer = (
            [_n("pin2", "inputPortNode", "P"), _n("pq", "outputPortNode", "Q")],
            [_e("pin2", "use_1", th="in__A"), _e("use_1", "pq", sh="out__C")],
        )
        canvases = {"outer": outer, "sub": (SUB_NODES, SUB_EDGES)}
        assert runnable_routes("outer", canvases, {"use_1": "sub"}) == {("P", "Q")}

    def test_a_nested_route_through_a_disconnected_step_is_no_route(self):
        """Only out__Y is wired on the outer canvas, and fcn_x feeds it."""
        outer = (
            [_n("pin2", "inputPortNode", "P"), _n("pq", "outputPortNode", "Q")],
            [_e("pin2", "use_1", th="in__A"), _e("use_1", "pq", sh="out__Y")],
        )
        canvases = {"outer": outer, "sub": (SUB_NODES, SUB_EDGES)}
        assert runnable_routes("outer", canvases, {"use_1": "sub"}) == frozenset()

    def test_a_placement_cycle_stops(self):
        canvas = (
            [_n("pin", "inputPortNode", "A"), _n("po", "outputPortNode", "B")],
            [_e("pin", "use_self", th="in__A"), _e("use_self", "po", sh="out__B")],
        )
        assert runnable_routes("loop", {"loop": canvas}, {"use_self": "loop"}) == frozenset()
