"""
ColName on a function node's input port (``.claude/plan-colname-input-gui.md``).

With an input set to "Run once per column", an ``n/a`` row on the Inputs
section can be set to ``ColName``: that parameter then receives the current
column's name, as ``inputs={"df": MyVar.for_columns(), "col": ColName()}``
does in code. The statement is the ``colname`` intent aspect (blob key
``colNameInputs``); it reaches a run as a ``colname`` BINDING, which every
consumer only renders.
"""

from __future__ import annotations

import inspect
import logging

import pandas as pd
from scidb import BaseVariable, ColName
from scifor.signature import Argument

from scistack_gui import pipeline_store
from scistack_gui.domain import colname_inputs as cn
from scistack_gui.domain.edge_resolver import (
    BINDING_COLNAME,
    colname_binding,
    parameter_binding,
    pathinput_binding,
    variable_binding,
)
from scistack_gui.services.execution_service import (
    build_run_inputs,
    derive_target_for_node,
    variable_inputs_view,
)


def _iterating_target(**extra_bindings) -> dict:
    return {
        "constants": {},
        "bindings": {
            "signal": variable_binding(["RawSignal"], [], True),
            **extra_bindings,
        },
    }


# ---------------------------------------------------------------------------
# domain/colname_inputs.apply_to_target (pure)
# ---------------------------------------------------------------------------


class TestApplyToTarget:
    def test_an_unwired_param_is_bound_to_colname(self):
        out = cn.apply_to_target(_iterating_target(), ["label"])
        assert out["bindings"]["label"] == {"kind": BINDING_COLNAME, "ref": None}

    def test_the_input_target_is_not_mutated(self):
        target = _iterating_target()
        cn.apply_to_target(target, ["label"])
        assert "label" not in target["bindings"]

    def test_no_iterate_input_drops_it_with_a_warning(self, caplog):
        target = {"constants": {}, "bindings": {"signal": variable_binding(["RawSignal"])}}
        with caplog.at_level(logging.WARNING):
            out = cn.apply_to_target(target, ["label"])
        assert "label" not in out["bindings"]
        assert "no input runs once per column" in caplog.text

    def test_a_pathinput_or_parameter_binding_is_replaced_and_warned(self, caplog):
        target = _iterating_target(
            a=pathinput_binding("raw_files"), b=parameter_binding("SCALE")
        )
        with caplog.at_level(logging.WARNING):
            out = cn.apply_to_target(target, ["a", "b"])
        assert out["bindings"]["a"]["kind"] == BINDING_COLNAME
        assert out["bindings"]["b"]["kind"] == BINDING_COLNAME
        assert "replaces its pathinput binding" in caplog.text
        assert "replaces its parameter binding" in caplog.text

    def test_a_recorded_constant_is_removed(self, caplog):
        target = _iterating_target()
        target["constants"] = {"low_hz": 20}
        with caplog.at_level(logging.WARNING):
            out = cn.apply_to_target(target, ["low_hz"])
        assert "low_hz" not in out["constants"]
        assert target["constants"] == {"low_hz": 20}  # the original is untouched
        assert "recorded constant" in caplog.text

    def test_a_variable_bound_param_keeps_its_variable(self, caplog):
        target = _iterating_target(other=variable_binding(["Other"]))
        with caplog.at_level(logging.WARNING):
            out = cn.apply_to_target(target, ["other"])
        assert out["bindings"]["other"]["kind"] == "variable"
        assert "the variable wins" in caplog.text

    def test_nothing_stated_is_a_plain_copy(self):
        target = _iterating_target()
        assert cn.apply_to_target(target, []) == target


# ---------------------------------------------------------------------------
# Storage: put_node_config -> intent store -> get_node_config
# ---------------------------------------------------------------------------


class TestStorage:
    def test_round_trip_is_normalized(self, populated_db, bp_node_id):
        pipeline_store.update_node_config(
            populated_db, bp_node_id, {"colNameInputs": ["z", "a", "z"]}
        )
        cfg = pipeline_store.get_node_config(populated_db, bp_node_id)
        assert cfg["colNameInputs"] == ["a", "z"]

    def test_an_empty_list_clears_it(self, populated_db, bp_node_id):
        pipeline_store.update_node_config(populated_db, bp_node_id, {"colNameInputs": ["a"]})
        pipeline_store.update_node_config(populated_db, bp_node_id, {"colNameInputs": []})
        cfg = pipeline_store.get_node_config(populated_db, bp_node_id)
        assert "colNameInputs" not in cfg

    def test_saving_other_settings_leaves_it_alone(self, populated_db, bp_node_id):
        """The panel saves the whole blob; a save without the key (an older
        bundle, or another writer) must not erase the statement."""
        pipeline_store.update_node_config(populated_db, bp_node_id, {"colNameInputs": ["a"]})
        pipeline_store.update_node_config(
            populated_db, bp_node_id, {"runOptions": {"dry_run": False, "save": True}}
        )
        assert pipeline_store.get_node_config(populated_db, bp_node_id)["colNameInputs"] == ["a"]

    def test_duplicating_a_node_copies_it(self, populated_db, bp_node_id):
        from scistack_gui import intent_store

        pipeline_store.update_node_config(populated_db, bp_node_id, {"colNameInputs": ["a"]})
        intent_store.copy_subject(populated_db, bp_node_id, "fn__bandpass_filter__copy01")
        cfg = pipeline_store.get_node_config(populated_db, "fn__bandpass_filter__copy01")
        assert cfg["colNameInputs"] == ["a"]

    def test_it_comes_back_on_the_canvas_node(self, populated_db, bp_node_id):
        """Saved-config keys the graph build does not copy onto node data
        snap back on the next refetch (the 2026-09-14 bug)."""
        from scistack_gui.domain.graph_builder import _apply_saved_config

        data: dict = {}
        _apply_saved_config(data, {"colNameInputs": ["a"]})
        assert data["colNameInputs"] == ["a"]


# ---------------------------------------------------------------------------
# Derivation: the node's statement reaches the run target
# ---------------------------------------------------------------------------


class TestDerivation:
    """``bandpass_filter(signal, low_hz)`` in ``populated_db`` has history,
    with ``low_hz`` recorded as the constant 20."""

    def test_colname_replaces_the_recorded_constant(self, populated_db, bp_node_id):
        pipeline_store.update_node_config(
            populated_db,
            bp_node_id,
            {
                "columnSelections": {"signal": {"columns": [], "iterate": True}},
                "colNameInputs": ["low_hz"],
            },
        )
        targets = derive_target_for_node(populated_db, bp_node_id)
        assert targets
        for t in targets:
            assert t["bindings"]["low_hz"]["kind"] == BINDING_COLNAME
            assert "low_hz" not in t["constants"]

    def test_without_an_iterate_input_it_is_not_bound(self, populated_db, bp_node_id, caplog):
        pipeline_store.update_node_config(populated_db, bp_node_id, {"colNameInputs": ["low_hz"]})
        with caplog.at_level(logging.WARNING):
            targets = derive_target_for_node(populated_db, bp_node_id)
        assert targets
        assert all("low_hz" not in t["bindings"] or t["bindings"]["low_hz"]["kind"] != BINDING_COLNAME for t in targets)
        assert "no input runs once per column" in caplog.text

    def test_another_call_sites_statement_does_not_leak(self, populated_db, bp_node_id):
        pipeline_store.update_node_config(
            populated_db, bp_node_id, {"columnSelections": {"signal": {"columns": [], "iterate": True}}}
        )
        pipeline_store.update_node_config(
            populated_db, "fn__bandpass_filter__deadbeef", {"colNameInputs": ["low_hz"]}
        )
        for t in derive_target_for_node(populated_db, bp_node_id):
            assert (t["bindings"].get("low_hz") or {}).get("kind") != BINDING_COLNAME

    def test_the_call_id_does_not_change(self, populated_db, bp_node_id):
        """scidb leaves ColName out of version keys, so the GUI's prediction
        must too — or combo hiding breaks on every ColName node."""
        from scistack_gui.domain.variant_resolver import compute_call_id

        base = _iterating_target()
        with_colname = cn.apply_to_target(base, ["label"])
        assert compute_call_id("bandpass_filter", base) == compute_call_id(
            "bandpass_filter", with_colname
        )


# ---------------------------------------------------------------------------
# Rendering: Python run, MATLAB command, code export
# ---------------------------------------------------------------------------


class TestRendering:
    def test_build_run_inputs_gives_a_deferred_colname(self, populated_db):
        target = _iterating_target(low_hz=colname_binding())
        inputs = build_run_inputs(target, "bandpass_filter", populated_db)
        assert isinstance(inputs["low_hz"], ColName)
        assert inputs["low_hz"].is_deferred

    def test_variable_inputs_view_carries_it_for_matlab(self):
        view = variable_inputs_view([_iterating_target(label=colname_binding())])
        assert view["label"] == {"colname": True}
        assert view["signal"] == {"types": ["RawSignal"], "columns": [], "iterate": True}

    def test_matlab_items_render_scidb_colname(self):
        from scistack_gui.api.matlab_command import _variable_input_items

        items = dict(_variable_input_items({"label": {"colname": True}, "v": ["Raw"]}))
        assert items == {"label": "scidb.ColName()", "v": "Raw()"}

    def test_matlab_colname_wins_over_a_history_constant(self, caplog):
        from scistack_gui.api.matlab_command import _overlay_colname_inputs

        with caplog.at_level(logging.WARNING):
            out = _overlay_colname_inputs({"label": "20"}, {"label": {"colname": True}})
        assert out == {"label": "scidb.ColName()"}
        assert "ColName replaces" in caplog.text

    def test_matlab_type_names_skip_it(self):
        from scistack_gui.api.matlab_command import _variable_input_type_names

        assert _variable_input_type_names({"label": {"colname": True}, "v": ["Raw"]}) == ["Raw"]

    def test_code_export_literals(self):
        from scistack_gui.services.code_export_service import _matlab_literal, _py_literal

        assert _py_literal(ColName()) == "ColName()"
        assert _matlab_literal(ColName()) == "scidb.ColName()"


# ---------------------------------------------------------------------------
# Port visibility: a ColName argument counts as fed
# ---------------------------------------------------------------------------


def test_an_optional_argument_set_to_colname_stays_drawn():
    from scistack_gui.domain.graph_builder import assign_input_ports

    args = [
        Argument("value", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        Argument("label", inspect.Parameter.POSITIONAL_OR_KEYWORD, default="x"),
    ]

    def node(colnames):
        return {
            "id": "fn__f__abc",
            "type": "functionNode",
            "data": {
                "label": "f",
                "input_params": {"value": "Wide"},
                "constant_params": [],
                "colNameInputs": colnames,
            },
        }

    hidden, shown = node([]), node(["label"])
    assign_input_ports([hidden, shown], {"f": args})
    assert "label" not in hidden["data"]["input_ports"]
    assert "label" in shown["data"]["input_ports"]


# ---------------------------------------------------------------------------
# End to end: a GUI-built target runs per column with the column's name
# ---------------------------------------------------------------------------


class WideTable(BaseVariable):
    pass


class NameLengths(BaseVariable):
    pass


def name_length(value, label):
    """Proves ``label`` is the column being iterated, not a fixed value."""
    return float(len(label))


def test_a_gui_target_runs_once_per_column_with_its_name(populated_db):
    from scidb import for_each

    from scistack_gui import registry

    registry._functions["name_length"] = name_length
    for subj in [1, 2]:
        WideTable.save(
            pd.DataFrame({"a": [1.0], "bbb": [2.0]}), subject=subj, session="pre"
        )
    target = {
        "constants": {},
        "bindings": {
            "value": variable_binding(["WideTable"], [], True),
            "label": colname_binding(),
        },
    }
    inputs = build_run_inputs(target, "name_length", populated_db)
    for_each(name_length, inputs=inputs, outputs=[NameLengths], subject=[1, 2], session=["pre"])

    data = NameLengths.load(subject=1, session="pre").data
    assert float(data["a"].iloc[0]) == 1.0
    assert float(data["bbb"].iloc[0]) == 3.0
