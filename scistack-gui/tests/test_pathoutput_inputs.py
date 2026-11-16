"""
Output paths on a function node (GitHub issue #19).

A parameter can receive ``PathOutput(template)`` — stated on the node's Inputs
section, or written there by "Save … Add to pipeline". The statement is the
``pathoutput`` intent aspect (blob key ``pathOutputs``, ``{param: template}``);
it reaches a run as a ``pathoutput`` BINDING, which every consumer only
renders. Before this, a ``plot_`` node on the canvas had no way to be given
its output path at all (custom_plot_service: "never an edge in the GUI").
"""

from __future__ import annotations

import inspect
import logging

import numpy as np
from scidb import BaseVariable
from scifor import PathOutput
from scifor.signature import Argument

from scistack_gui.domain import pathoutput_inputs as po
from scistack_gui.domain.edge_resolver import (
    BINDING_PATHOUTPUT,
    colname_binding,
    parameter_binding,
    pathoutput_binding,
    variable_binding,
)
from scistack_gui.services.execution_service import (
    build_run_inputs,
    derive_target_for_node,
    variable_inputs_view,
)
from scistack_gui.stores import pipeline_store


def _target(**extra) -> dict:
    return {"constants": {}, "bindings": {"df": variable_binding(["RawSignal"]), **extra}}


# ---------------------------------------------------------------------------
# domain/pathoutput_inputs.apply_to_target (pure)
# ---------------------------------------------------------------------------


class TestApplyToTarget:
    def test_an_unwired_param_is_bound_to_its_template(self):
        out = po.apply_to_target(_target(), {"filename": "plots/{subject}.png"})
        assert out["bindings"]["filename"] == {"kind": BINDING_PATHOUTPUT, "ref": "plots/{subject}.png"}

    def test_no_iterate_input_is_needed(self):
        """Unlike ColName, an output path resolves from the combo alone."""
        out = po.apply_to_target(_target(), {"filename": "x.png"})
        assert "filename" in out["bindings"]

    def test_the_input_target_is_not_mutated(self):
        target = _target()
        po.apply_to_target(target, {"filename": "x.png"})
        assert "filename" not in target["bindings"]

    def test_other_bindings_and_constants_are_replaced_and_warned(self, caplog):
        target = _target(a=parameter_binding("SCALE"), b=colname_binding())
        target["constants"] = {"c": "old.png"}
        with caplog.at_level(logging.WARNING):
            out = po.apply_to_target(target, {"a": "a.png", "b": "b.png", "c": "c.png"})
        assert {out["bindings"][p]["kind"] for p in "abc"} == {BINDING_PATHOUTPUT}
        assert "c" not in out["constants"]
        assert "replaces its parameter binding" in caplog.text
        assert "replaces its recorded constant" in caplog.text

    def test_a_variable_bound_param_keeps_its_variable(self, caplog):
        with caplog.at_level(logging.WARNING):
            out = po.apply_to_target(_target(), {"df": "x.png"})
        assert out["bindings"]["df"]["kind"] == "variable"
        assert "the variable wins" in caplog.text

    def test_nothing_stated_is_a_plain_copy(self):
        target = _target()
        assert po.apply_to_target(target, {}) == target


# ---------------------------------------------------------------------------
# Storage: put_node_config -> intent store -> get_node_config
# ---------------------------------------------------------------------------


class TestStorage:
    def test_round_trip_is_normalized(self, populated_db, bp_node_id):
        pipeline_store.update_node_config(
            populated_db, bp_node_id, {"pathOutputs": {"z": " b.png ", "a": "a.png", "blank": ""}}
        )
        cfg = pipeline_store.get_node_config(populated_db, bp_node_id)
        assert cfg["pathOutputs"] == {"a": "a.png", "z": "b.png"}

    def test_an_empty_map_clears_it(self, populated_db, bp_node_id):
        pipeline_store.update_node_config(populated_db, bp_node_id, {"pathOutputs": {"a": "a.png"}})
        pipeline_store.update_node_config(populated_db, bp_node_id, {"pathOutputs": {}})
        assert "pathOutputs" not in pipeline_store.get_node_config(populated_db, bp_node_id)

    def test_other_settings_saves_leave_it_alone(self, populated_db, bp_node_id):
        """The panel sends only the keys it changed; an unrelated save must
        not erase the output path."""
        pipeline_store.update_node_config(populated_db, bp_node_id, {"pathOutputs": {"a": "a.png"}})
        pipeline_store.update_node_config(populated_db, bp_node_id, {"runOptions": {"distribute": False}})
        assert pipeline_store.get_node_config(populated_db, bp_node_id)["pathOutputs"] == {"a": "a.png"}

    def test_it_is_rehydrated_onto_the_canvas_node(self):
        from scistack_gui.domain.graph_builder import _SAVED_CONFIG_KEYS

        assert "pathOutputs" in _SAVED_CONFIG_KEYS


# ---------------------------------------------------------------------------
# Derivation and rendering
# ---------------------------------------------------------------------------


class TestDerivation:
    def test_a_stated_output_path_is_bound_on_every_target(self, populated_db, bp_node_id, caplog):
        pipeline_store.update_node_config(
            populated_db, bp_node_id, {"pathOutputs": {"report": "reports/{subject}.txt"}}
        )
        with caplog.at_level(logging.INFO):
            targets = derive_target_for_node(populated_db, bp_node_id)
        assert targets
        for t in targets:
            assert t["bindings"]["report"] == pathoutput_binding("reports/{subject}.txt")
        assert "output path(s) stated" in caplog.text

    def test_build_run_inputs_renders_a_pathoutput(self, populated_db):
        inputs = build_run_inputs(
            _target(filename=pathoutput_binding("plots/{subject}.png")), "f", populated_db
        )
        assert isinstance(inputs["filename"], PathOutput)
        assert str(inputs["filename"].template) == "plots/{subject}.png"

    def test_the_call_id_does_not_change(self):
        """scidb leaves PathOutput out of version keys; the GUI's prediction
        must agree, or stating a path would move the node's id."""
        from scistack_gui.domain.variant_resolver import compute_call_id

        base = _target()
        stated = po.apply_to_target(base, {"filename": "x.png"})
        assert compute_call_id("plot_x", base) == compute_call_id("plot_x", stated)

    def test_matlab_runs_say_they_drop_it(self, caplog):
        with caplog.at_level(logging.WARNING):
            view = variable_inputs_view([_target(filename=pathoutput_binding("x.png"))], "plot_x")
        assert "filename" not in view
        assert "not passed to MATLAB runs yet" in caplog.text


def test_an_optional_argument_with_an_output_path_stays_drawn():
    from scistack_gui.domain.graph_builder import assign_input_ports

    args = [
        Argument("df", inspect.Parameter.POSITIONAL_OR_KEYWORD),
        Argument("filename", inspect.Parameter.POSITIONAL_OR_KEYWORD, default=None),
    ]

    def node(paths):
        return {
            "id": "fn__plot_x__abc",
            "type": "functionNode",
            "data": {
                "label": "plot_x",
                "input_params": {"df": "RawSignal"},
                "constant_params": [],
                "pathOutputs": paths,
            },
        }

    hidden, shown = node({}), node({"filename": "x.png"})
    assign_input_ports([hidden, shown], {"plot_x": args})
    assert "filename" not in hidden["data"]["input_ports"]
    assert "filename" in shown["data"]["input_ports"]


# ---------------------------------------------------------------------------
# End to end: a GUI-built target writes its file under [outputs] folder
# ---------------------------------------------------------------------------


class SignalReport(BaseVariable):
    pass


def write_report(signal, report):
    from pathlib import Path

    Path(report).parent.mkdir(parents=True, exist_ok=True)
    Path(report).write_text(f"{float(np.mean(signal)):.3f}", encoding="utf-8")
    return str(report)


def test_a_gui_target_writes_under_the_output_folder(populated_db, tmp_path):
    from scidb import for_each
    from scifor.project import outputs, schema_order
    from scifor.project.root import get_project_root, set_project_root

    previous = get_project_root()
    (tmp_path / "scistack.toml").write_text('modules = []\n[outputs]\nfolder = "out"\n', encoding="utf-8")
    set_project_root(tmp_path)
    outputs.clear_cache()
    schema_order.clear_cache()
    try:
        target = {
            "constants": {},
            "bindings": {
                "signal": variable_binding(["RawSignal"]),
                "report": pathoutput_binding("reports/{subject}_{session}.txt"),
            },
        }
        inputs = build_run_inputs(target, "write_report", populated_db)
        for_each(write_report, inputs=inputs, outputs=[SignalReport], subject=[1], session=["pre"])
    finally:
        set_project_root(previous)
        outputs.clear_cache()
        schema_order.clear_cache()

    written = tmp_path / "out" / "reports" / "1_pre.txt"
    assert written.exists()
    assert SignalReport.load(subject=1, session="pre").data == str(written)
