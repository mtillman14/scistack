"""for_columns column groups in the GUI (Stage 5 of
.claude/plan-for-columns-groups.md).

* ``plan_groups`` answers the Inputs picker from scifor's owners: combine or
  split, the pairing, overlaps, and an error instead of an exception.
* A stored grouped selection rides the variable binding to the run
  (``build_run_inputs``), the MATLAB command and the code export intact.
"""

from __future__ import annotations

from scifor import ColumnSelection as SciforColumnSelection

from scistack_gui.api.matlab_command import (
    _format_variable_class,
    _variable_binding_parts,
    format_matlab_column_spec,
)
from scistack_gui.domain import column_selection as cs
from scistack_gui.domain.edge_resolver import variable_binding
from scistack_gui.services.code_export_service import _matlab_literal, _py_literal
from scistack_gui.services.execution_service import build_run_inputs

SIX = ["6MWT", "6MWT_Date"]
TEN = ["10MWT", "10MWT_Date"]
SPLIT_SEL = {"columns": [], "iterate": True, "groups": [{"columns": SIX}, {"columns": TEN}]}
COMBINE_SEL = {
    "columns": [],
    "iterate": True,
    "groups": [{"name": "6MWT", "columns": SIX}, {"name": "10MWT", "columns": TEN}],
}


# --- plan_groups (the picker's backend answer) -------------------------------


def test_plan_split_pairs_groups_with_outputs_in_order():
    plan = cs.plan_groups(SPLIT_SEL, ["SixMWT", "TenMWT"])
    assert plan["ok"] and plan["mode"] == "split"
    assert plan["groups"] == [
        {"name": "SixMWT", "columns": SIX},
        {"name": "TenMWT", "columns": TEN},
    ]
    assert plan["selection"]["groups"] == [{"columns": SIX}, {"columns": TEN}]


def test_plan_combine_names_columns():
    plan = cs.plan_groups(COMBINE_SEL, ["Obj2"])
    assert plan["ok"] and plan["mode"] == "combine"
    assert [g["name"] for g in plan["groups"]] == ["6MWT", "10MWT"]


def test_plan_errors_are_answers_not_exceptions():
    assert "counts must match" in cs.plan_groups(SPLIT_SEL, ["A", "B", "C"])["error"]
    assert "needs a name" in cs.plan_groups(SPLIT_SEL, ["Out"])["error"]
    assert "would name nothing" in cs.plan_groups(COMBINE_SEL, ["A", "B"])["error"]
    empty_group = {"columns": [], "iterate": True, "groups": [{"columns": SIX}, {"columns": []}]}
    assert not cs.plan_groups(empty_group, ["A", "B"])["ok"]


def test_plan_every_column():
    every = {"columns": [], "iterate": True}
    assert cs.plan_groups(every, ["Out"])["mode"] == "combine"
    assert "data's column order" in cs.plan_groups(every, ["A", "B"])["error"]


def test_plan_overlaps_and_non_iterate():
    shared = {"columns": [], "iterate": True, "groups": [{"columns": ["a", "d"]}, {"columns": ["b", "d"]}]}
    assert cs.plan_groups(shared, ["A", "B"])["overlaps"] == {"d": ["A", "B"]}
    assert cs.plan_groups({"columns": ["a"], "iterate": False}, ["A"])["mode"] is None


def test_plan_returns_the_stored_spelling():
    """Groups that add nothing are stored as the flat list; the picker saves
    what plan returns, so it never loops on a rewritten spelling."""
    flat_groups = {"columns": [], "iterate": True, "groups": [{"columns": ["a"]}, {"columns": ["b"]}]}
    plan = cs.plan_groups(flat_groups, ["A", "B"])
    assert plan["selection"] == {"columns": ["a", "b"], "iterate": True}
    assert plan["mode"] == "split"


# --- Binding -> run inputs ------------------------------------------------------


def test_apply_to_bindings_carries_groups():
    bindings = {"signal": variable_binding(["RawSignal"])}
    out = cs.apply_to_bindings(bindings, {"signal": SPLIT_SEL})
    assert out["signal"]["groups"] == [{"columns": SIX}, {"columns": TEN}]
    assert cs.from_binding(out["signal"])["groups"] == SPLIT_SEL["groups"]
    # A later flat selection drops stale groups.
    again = cs.apply_to_bindings(out, {"signal": {"columns": ["a"], "iterate": True}})
    assert "groups" not in again["signal"]


def test_build_run_inputs_keeps_groups(populated_db):
    target = {
        "constants": {},
        "bindings": {
            "signal": variable_binding(
                ["RawSignal"], SIX + TEN, True, groups=SPLIT_SEL["groups"]
            )
        },
    }
    sel = build_run_inputs(target, "bandpass_filter", populated_db)["signal"]
    assert isinstance(sel, SciforColumnSelection)
    assert sel.iterate and sel.column_spec == [SIX, TEN]


# --- Rendering ------------------------------------------------------------------


def test_format_matlab_column_spec():
    assert format_matlab_column_spec([]) == ""
    assert format_matlab_column_spec(["a", "b"]) == '["a", "b"]'
    assert format_matlab_column_spec([SIX, TEN]) == (
        '{["6MWT", "6MWT_Date"], ["10MWT", "10MWT_Date"]}'
    )
    assert format_matlab_column_spec({"6MWT": SIX, "10MWT": TEN}) == (
        'dictionary(["6MWT", "10MWT"], {["6MWT", "6MWT_Date"], ["10MWT", "10MWT_Date"]})'
    )


def test_matlab_command_renders_groups():
    ref = {"types": ["Walks"], "columns": SIX + TEN, "iterate": True, "groups": COMBINE_SEL["groups"]}
    names, columns, iterate, pooled = _variable_binding_parts(ref)
    assert columns == {"6MWT": SIX, "10MWT": TEN}
    assert _format_variable_class(names[0], columns, iterate, pooled) == (
        'Walks().for_columns(dictionary(["6MWT", "10MWT"], '
        '{["6MWT", "6MWT_Date"], ["10MWT", "10MWT_Date"]}))'
    )
    flat = {"types": ["Walks"], "columns": ["a", "b"], "iterate": True}
    assert _format_variable_class("Walks", _variable_binding_parts(flat)[1], True) == (
        'Walks().for_columns(["a", "b"])'
    )


def test_code_export_renders_groups():
    from scidb import BaseVariable

    class ExportWalks(BaseVariable):
        pass

    split = ExportWalks.for_columns([SIX, TEN])
    assert _py_literal(split) == "ExportWalks.for_columns([['6MWT', '6MWT_Date'], ['10MWT', '10MWT_Date']])"
    assert _matlab_literal(split) == (
        'ExportWalks().for_columns({["6MWT", "6MWT_Date"], ["10MWT", "10MWT_Date"]})'
    )
    combine = ExportWalks.for_columns({"6MWT": SIX})
    assert _py_literal(combine) == "ExportWalks.for_columns({'6MWT': ['6MWT', '6MWT_Date']})"
    # Flat selections render exactly as before.
    assert _matlab_literal(ExportWalks.for_columns(["a", "b"])) == 'ExportWalks().for_columns(["a", "b"])'


# --- A GUI run writes every output a split pairs with -------------------------


def test_run_output_types_split_uses_every_node_output():
    from scidb import BaseVariable

    from scistack_gui.services.execution_service import run_output_types

    class RunWalks(BaseVariable):
        pass

    split = {"g": RunWalks.for_columns([SIX, TEN])}
    assert run_output_types(split, ["SixMWT", "TenMWT"], "SixMWT") == ["SixMWT", "TenMWT"]
    # One node output, or a combine dict: one output per call, as before.
    assert run_output_types(split, ["SixMWT"], "SixMWT") == ["SixMWT"]
    combine = {"g": RunWalks.for_columns({"6MWT": SIX, "10MWT": TEN})}
    assert run_output_types(combine, ["A", "B"], "A") == ["A"]
    # for_columns() (every column) never splits; no for_columns at all is untouched.
    assert run_output_types({"g": RunWalks.for_columns()}, ["A", "B"], "A") == ["A"]
    assert run_output_types({"g": RunWalks}, ["A", "B"], "B") == ["B"]
    # A flat list with as many columns as outputs splits one column per output.
    flat = {"g": RunWalks.for_columns(["a", "b"])}
    assert run_output_types(flat, ["A", "B"], "A") == ["A", "B"]


# --- Output handle tooltips ----------------------------------------------------


def test_mark_output_groups_split_and_combine():
    from scistack_gui.domain.graph_builder import mark_output_groups

    split_node = {
        "type": "functionNode",
        "data": {"output_types": ["SixMWT", "TenMWT"], "columnSelections": {"g": SPLIT_SEL}},
    }
    combine_node = {
        "type": "functionNode",
        "data": {"output_types": ["Walks"], "columnSelections": {"g": COMBINE_SEL}},
    }
    broken_node = {
        "type": "functionNode",
        "data": {"output_types": ["A", "B", "C"], "columnSelections": {"g": SPLIT_SEL}},
    }
    plain_node = {"type": "functionNode", "data": {"output_types": ["A"]}}
    nodes = [split_node, combine_node, broken_node, plain_node, {"type": "variableNode", "data": {}}]
    assert mark_output_groups(nodes) == 2
    assert split_node["data"]["outputGroups"] == {
        "SixMWT": [{"name": "SixMWT", "columns": SIX}],
        "TenMWT": [{"name": "TenMWT", "columns": TEN}],
    }
    assert [g["name"] for g in combine_node["data"]["outputGroups"]["Walks"]] == ["6MWT", "10MWT"]
    assert "outputGroups" not in broken_node["data"]
    assert "outputGroups" not in plain_node["data"]
