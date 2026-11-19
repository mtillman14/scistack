"""The Merge node (docs/claude/merge-node.md): canvas -> run, history ->
canvas, MATLAB and code export all spell one Merge the same way.

Pure-domain tests (no database): edge resolution, identity keys, the
history-drawn node and edges, the wiring refusals, and both renderers.
"""

from __future__ import annotations

import json

import pytest

from scistack_gui.domain.edge_resolver import (
    BINDING_MERGE,
    loadable_bindings,
    merge_binding,
    merge_binding_key,
    resolve_function_edges,
    variable_types_view,
)
from scistack_gui.ids import merge_slot_handle, merge_slot_of

FN = "fn__analyze__abc123"
MERGE = "merge__gait_force__1a2b3c4d"


def _merge_nodes(how=None):
    meta = {"type": "mergeNode", "label": "gait_force"}
    if how:
        meta["config"] = {"mergeHow": how}
    return {MERGE: meta}


def _edges(*slots):
    """Variables into the Merge's slots, then Merge -> analyze(data)."""
    edges = [
        {"id": f"e{n}", "source": f"var__{t}", "target": MERGE, "targetHandle": merge_slot_handle(n)}
        for n, t in slots
    ]
    edges.append(
        {"id": "eout", "source": MERGE, "target": FN, "sourceHandle": "out__merge",
         "targetHandle": "in__data"}
    )
    return edges


# ---------------------------------------------------------------------------
# Slots
# ---------------------------------------------------------------------------


def test_slot_handles_round_trip():
    assert merge_slot_of(merge_slot_handle(7)) == 7
    assert merge_slot_of("in__data") is None
    assert merge_slot_of(None) is None


# ---------------------------------------------------------------------------
# Canvas -> binding
# ---------------------------------------------------------------------------


def test_merge_node_binds_its_variables_in_slot_order():
    # Slot 3 drawn before slot 0: join order is the slot order, not edge order.
    resolved = resolve_function_edges({FN}, _edges((3, "Force"), (0, "Gait")), _merge_nodes(), {})
    assert resolved.bindings["data"] == merge_binding("gait_force", "exact", ["Gait", "Force"])


def test_a_closed_gap_renames_nothing():
    """Slots 0 and 2 (slot 1 disconnected): still a two-variable Merge."""
    resolved = resolve_function_edges({FN}, _edges((0, "Gait"), (2, "Force")), _merge_nodes(), {})
    assert resolved.bindings["data"]["ref"]["tables"] == ["Gait", "Force"]


def test_how_comes_from_the_node_config():
    resolved = resolve_function_edges(
        {FN}, _edges((0, "Gait"), (1, "Force")), _merge_nodes(how="left"), {}
    )
    assert resolved.bindings["data"]["ref"]["how"] == "left"


def test_one_variable_is_not_a_merge():
    resolved = resolve_function_edges({FN}, _edges((0, "Gait")), _merge_nodes(), {})
    assert "data" not in resolved.bindings


def test_a_history_merge_id_resolves_without_a_row():
    """merge__{name} before adoption stored a row: name from the id."""
    edges = [
        {"id": "a", "source": "var__Gait", "target": "merge__gm", "targetHandle": "in__m0"},
        {"id": "b", "source": "var__Force", "target": "merge__gm", "targetHandle": "in__m1"},
        {"id": "c", "source": "merge__gm", "target": FN, "targetHandle": "in__data"},
    ]
    resolved = resolve_function_edges({FN}, edges, {}, {})
    assert resolved.bindings["data"]["ref"]["name"] == "gm"


# ---------------------------------------------------------------------------
# Identity: the canvas key equals what scidb records (Merge.to_key())
# ---------------------------------------------------------------------------


def test_canvas_key_equals_scifor_to_key():
    from scifor import Merge

    class Gait:
        pass

    class Force:
        pass

    ref = {"name": "gait_force", "how": "left", "tables": ["Gait", "Force"]}
    assert merge_binding_key(ref) == Merge(Gait, Force, name="gait_force", how="left").to_key()


def test_identity_view_carries_the_merge_key():
    b = {"data": merge_binding("gm", "outer", ["A", "B"]), "x": {"kind": "variable", "ref": ["C"]}}
    view = variable_types_view(b)
    assert view["x"] == "C"
    assert json.loads(view["data"])["how"] == "outer"


def test_a_merge_is_loadable():
    b = {"data": merge_binding("gm", "exact", ["A", "B"])}
    assert loadable_bindings(b) == b


# ---------------------------------------------------------------------------
# History -> canvas
# ---------------------------------------------------------------------------


def _history():
    from scifor.merge import merge_key

    key = merge_key("gm", "left", ["Gait", "Force"])
    return {("analyze", "cid1"): {"data": key, "side": "Demo"}}


def test_history_draws_one_merge_node():
    from scistack_gui.domain.graph_builder import build_merge_nodes, merge_specs_from_history

    [node] = build_merge_nodes(merge_specs_from_history(_history()))
    assert node["id"] == "merge__gm"
    assert node["type"] == "mergeNode"
    assert node["data"]["how"] == "left"
    assert node["data"]["constituents"] == ["Gait", "Force"]
    assert "left" in node["data"]["hows"]


def test_history_edges_go_through_the_merge_node():
    from scistack_gui.domain.graph_builder import build_edges

    edges = build_edges(_history(), {}, {}, {}, [], set())
    pairs = {(e["source"], e["target"], e.get("targetHandle")) for e in edges}
    assert ("merge__gm", "fn__analyze__cid1", "in__data") in pairs
    assert ("var__Gait", "merge__gm", "in__m0") in pairs
    assert ("var__Force", "merge__gm", "in__m1") in pairs
    assert not any(s.startswith("var__{") for s, _t, _h in pairs), "the key is not a variable"


def test_two_definitions_of_one_name_are_flagged():
    from scifor.merge import merge_key

    from scistack_gui.domain.graph_builder import merge_specs_from_history

    hist = {
        ("f", "c1"): {"d": merge_key("gm", "left", ["A", "B"])},
        ("g", "c2"): {"d": merge_key("gm", "inner", ["A", "B"])},
    }
    assert merge_specs_from_history(hist)["gm"]["conflict"] is True


def test_connection_ids_agree_for_drawn_and_history_merge():
    from scistack_gui.domain.graph_builder import connection_id

    manual = {"merge__gm__abcd1234": {"type": "mergeNode", "label": "gm"}}
    drawn = connection_id("merge__gm__abcd1234", FN, "in__data", manual)
    history = connection_id("merge__gm", FN, "in__data", manual)
    assert drawn == history
    assert "mg:gm" in drawn


# ---------------------------------------------------------------------------
# Wiring refusals (layout_service.put_edge)
# ---------------------------------------------------------------------------


def _refuse(source, target, handle, existing=()):
    from scistack_gui.services.layout_service import _refuse_illegal_merge_wire

    _refuse_illegal_merge_wire(_merge_nodes(), list(existing), source, target, handle)


def test_a_variable_onto_an_empty_slot_is_fine():
    _refuse("var__Gait", MERGE, "in__m0")


@pytest.mark.parametrize(
    "source, handle, fragment",
    [
        ("pathInput__p", "in__m0", "only variables"),
        ("param__x", "in__m0", "only variables"),
        ("var__Gait", "in__data", "input ports"),
    ],
)
def test_refused_merge_inputs(source, handle, fragment):
    from scistack_gui.services.layout_service import MergeWireError

    with pytest.raises(MergeWireError, match=fragment):
        _refuse(source, MERGE, handle)


def test_a_merge_cannot_feed_a_merge():
    from scistack_gui.services.layout_service import MergeWireError, _refuse_illegal_merge_wire

    nodes = {**_merge_nodes(), "merge__other__1": {"type": "mergeNode", "label": "other"}}
    with pytest.raises(MergeWireError, match="cannot feed another Merge"):
        _refuse_illegal_merge_wire(nodes, [], "merge__other__1", MERGE, "in__m0")


def test_an_occupied_slot_is_refused():
    from scistack_gui.services.layout_service import MergeWireError

    taken = [{"source": "var__A", "target": MERGE, "targetHandle": "in__m0"}]
    with pytest.raises(MergeWireError, match="already has a variable"):
        _refuse("var__Gait", MERGE, "in__m0", taken)


def test_a_merge_fed_argument_takes_nothing_else():
    from scistack_gui.services.layout_service import MergeWireError

    fed = [{"source": MERGE, "target": FN, "targetHandle": "in__data"}]
    with pytest.raises(MergeWireError, match="fed by a Merge"):
        _refuse("var__Gait", FN, "in__data", fed)
    other = [{"source": "var__Gait", "target": FN, "targetHandle": "in__data"}]
    with pytest.raises(MergeWireError, match="takes the Merge alone"):
        _refuse(MERGE, FN, "in__data", other)


# ---------------------------------------------------------------------------
# Renderers: MATLAB command and code export
# ---------------------------------------------------------------------------


def test_matlab_command_renders_scidb_merge():
    from scistack_gui.services.matlab_command_format import _variable_input_items

    items = dict(
        _variable_input_items({"data": {"merge": {"name": "gm", "how": "left", "tables": ["A", "B"]}}})
    )
    assert items["data"] == 'scidb.Merge(A(), B(), name="gm", how="left")'


def test_matlab_history_key_renders_scidb_merge():
    from scifor.merge import merge_key

    from scistack_gui.services.matlab_command_format import collect_var_types

    variants = [{"function_name": "f", "output_type": "Out",
                 "input_types": {"data": merge_key("gm", "exact", ["A", "B"])}}]
    assert collect_var_types(variants) >= {"A", "B", "Out"}


def test_code_export_spells_python_and_matlab():
    from scifor import Merge

    from scistack_gui.services.code_export_service import _matlab_literal, _py_literal

    class A:
        pass

    class B:
        pass

    m = Merge(A, B, name="gm", how="left")
    assert _py_literal(m) == "Merge(A, B, name='gm', how='left')"
    assert _matlab_literal(m) == 'scidb.Merge(A(), B(), name="gm", how="left")'
    assert _py_literal(Merge(A, B, name="gm")) == "Merge(A, B, name='gm')"


def test_binding_kind_constant():
    assert BINDING_MERGE == "merge"
