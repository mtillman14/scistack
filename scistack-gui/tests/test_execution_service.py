"""
Tests for execution_service's hidden-constant-value filtering (see
.claude/plan-constant-source-of-truth-26-08-22.md item 3).

The ConstantNode.tsx checkbox persists a hidden (const_name, value) pair
via pipeline_store.hide_parameter_value. This must actually stop the
combo(s) it implies from running, not just hide it from display -- both
derive_fn_targets (name-scoped, used for pipeline Run and the node_id-less
per-node Run) and derive_target_for_node (node-scoped, used for the
node_id-specific per-node Run and pipeline compilation) must filter it out,
for both a combo with real DB history and one that's never been run.
"""

from __future__ import annotations

import json

import numpy as np
import pytest
from scidb import BaseVariable, Parameter

from scistack_gui import pipeline_store
from scistack_gui.db import get_db
from scistack_gui.services.execution_service import (
    derive_fn_targets,
    derive_target_for_node,
)


class TestDeriveFnTargetsHiddenConstantValues:
    def test_no_hidden_values_returns_real_history(self, populated_db):
        targets = derive_fn_targets(populated_db, "bandpass_filter")
        assert len(targets) == 1
        assert targets[0]["constants"] == {"low_hz": 20}

    def test_hidden_value_excludes_matching_target(self, populated_db):
        pipeline_store.hide_parameter_value(populated_db, "low_hz", "20")
        assert derive_fn_targets(populated_db, "bandpass_filter") == []

    def test_hidden_unrelated_value_keeps_target(self, populated_db):
        pipeline_store.hide_parameter_value(populated_db, "low_hz", "99")
        targets = derive_fn_targets(populated_db, "bandpass_filter")
        assert len(targets) == 1

    def test_unhide_restores_target(self, populated_db):
        pipeline_store.hide_parameter_value(populated_db, "low_hz", "20")
        pipeline_store.unhide_parameter_value(populated_db, "low_hz", "20")
        assert len(derive_fn_targets(populated_db, "bandpass_filter")) == 1


class TestDeriveTargetForNodeHiddenConstantValues:
    def test_no_hidden_values_returns_real_history(self, populated_db, bp_node_id):
        targets = derive_target_for_node(populated_db, bp_node_id)
        assert len(targets) == 1
        assert targets[0]["constants"] == {"low_hz": 20}

    def test_hidden_value_excludes_matching_target(self, populated_db, bp_node_id):
        pipeline_store.hide_parameter_value(populated_db, "low_hz", "20")
        assert derive_target_for_node(populated_db, bp_node_id) == []


class OtherSignal(BaseVariable):
    pass


class OtherFiltered(BaseVariable):
    pass


class TestNeverRunComboHiddenConstantValue:
    """A wiring that has never itself been run infers its constant's value
    from OTHER real call sites of the same function (see
    execution_service._infer_wired_constants) -- a hidden value must be
    excluded from that inferred combo too, not just from real DB history.
    """

    def test_inferred_known_value_excluded_when_hidden(self, client):
        OtherSignal.save(np.zeros(5), subject=1, session="pre")
        client.put(
            "/api/layout/mv_other_in",
            json={"x": 0, "y": 0, "node_type": "variableNode", "label": "OtherSignal"},
        )
        client.put(
            "/api/layout/mf_bp2",
            json={
                "x": 10,
                "y": 0,
                "node_type": "functionNode",
                "label": "bandpass_filter",
            },
        )
        client.put(
            "/api/layout/mv_other_out",
            json={"x": 20, "y": 0, "node_type": "variableNode", "label": "OtherFiltered"},
        )
        client.put(
            "/api/layout/mc_low_hz",
            json={"x": 5, "y": 5, "node_type": "parameterNode", "label": "low_hz"},
        )
        # Input edges name the parameter they feed; only output edges may
        # omit a handle (the target variable node IS the binding).
        client.put("/api/edges/e_in2", json={
            "source": "mv_other_in", "target": "mf_bp2", "target_handle": "in__signal",
        })
        client.put("/api/edges/e_out2", json={"source": "mf_bp2", "target": "mv_other_out"})
        client.put("/api/edges/e_low_hz", json={
            "source": "mc_low_hz", "target": "mf_bp2", "target_handle": "in__low_hz",
        })

        db = get_db()
        targets = derive_target_for_node(db, "mf_bp2")
        assert targets and targets[0]["constants"] == {"low_hz": 20}

        pipeline_store.hide_parameter_value(db, "low_hz", "20")
        assert derive_target_for_node(db, "mf_bp2") == []


class TestUnvaluedParameter:
    """A Parameter declared but not yet given a value. Both paths that could
    swallow it silently now refuse it instead."""

    def test_inference_skips_it_rather_than_contributing_an_empty_axis(self):
        """_inferred_targets takes the Cartesian PRODUCT of these lists, so
        one empty list yields ZERO targets — the node would report "nothing
        derivable", which is both the wrong diagnosis and a silent one.
        Left out, the target is still built and the run reaches
        build_run_inputs, which raises naming the parameter."""
        from scistack_gui.services.execution_service import _infer_wired_constants

        inferred = _infer_wired_constants(
            {"low_hz": "low_hz"},
            {},
            {"low_hz": Parameter()},
            log_context="'bandpass_filter'",
        )
        assert inferred == {}

    def test_declared_values_are_still_used_when_present(self):
        """The guard must not swallow the ordinary case it sits next to."""
        from scistack_gui.services.execution_service import _infer_wired_constants

        inferred = _infer_wired_constants(
            {"low_hz": "low_hz"},
            {},
            {"low_hz": Parameter(20, 30)},
            log_context="'bandpass_filter'",
        )
        assert inferred == {"low_hz": [20, 30]}

    def test_build_run_inputs_refuses_to_bind_it(self, populated_db, monkeypatch):
        """Bound as-is it is a zero-length EachOf axis: for_each would
        iterate zero times, write no records and report success."""
        from scistack_gui import registry
        from scistack_gui.domain.edge_resolver import BINDING_PARAMETER
        from scistack_gui.services.execution_service import build_run_inputs

        monkeypatch.setitem(registry._parameters, "low_hz", Parameter())
        target = {
            "constants": {},
            "bindings": {"low_hz": {"kind": BINDING_PARAMETER, "ref": "low_hz"}},
        }
        with pytest.raises(ValueError, match="has no value yet") as excinfo:
            build_run_inputs(target, "bandpass_filter", populated_db)
        # Names the DECLARED parameter the user sees on the canvas.
        assert "'low_hz'" in str(excinfo.value)


class TestDbHistoryPathInputBinding:
    """A PathInput-driven function that has ALREADY RUN has no manual edge
    on the canvas — its PathInput→fn edge is synthesised from DB history by
    graph_builder.build_edges. So the run path cannot get its binding from
    manual edges alone, and (since name matching is gone) it would otherwise
    have nothing to resolve from at all.

    _db_path_input_params inverts get_aggregated_variants()["path_inputs"]
    (keyed by PARAM name, carrying the recorded spec) into per-call-site
    {param_name: declared_name}, reusing convert_scidb_path_inputs' existing
    spec→declared-name resolution. This is what keeps every already-run
    project working across the clean break.
    """

    @staticmethod
    def _with_path_input_history(db, monkeypatch, template: str, root_folder=None):
        """*db* with ONE recorded PathInput in its aggregated history.

        Patches the real database rather than substituting a stub: this code
        path also reads the D7 name history via pipeline_store, so a fake
        exposing only get_aggregated_variants isn't enough (and a stub that
        grew to cover both would just be a second, drifting implementation
        of the store).
        """
        monkeypatch.setattr(
            db,
            "get_aggregated_variants",
            lambda *a, **k: {
                "path_inputs": {
                    "filepath_or_buffer": {
                        "template": template,
                        "root_folder": root_folder,
                        "functions": [("read_csv_like", "call1")],
                    }
                }
            },
        )
        return db

    def test_declared_name_is_recovered_from_the_recorded_spec(
        self, populated_db, monkeypatch
    ):
        from scidb import PathInput

        from scistack_gui import registry
        from scistack_gui.services.execution_service import _db_path_input_params

        monkeypatch.setattr(
            registry,
            "get_path_inputs_registry",
            lambda: {"test_pi": PathInput("{subject}/data.csv")},
        )

        by_call = _db_path_input_params(
            self._with_path_input_history(populated_db, monkeypatch, "{subject}/data.csv"),
            "read_csv_like",
        )

        # The param it filled is remembered, and the spec resolves back to
        # the name it is declared under in source.
        assert by_call == {"call1": {"filepath_or_buffer": "test_pi"}}

    def test_other_functions_call_sites_are_not_included(
        self, populated_db, monkeypatch
    ):
        from scidb import PathInput

        from scistack_gui import registry
        from scistack_gui.services.execution_service import _db_path_input_params

        monkeypatch.setattr(
            registry,
            "get_path_inputs_registry",
            lambda: {"test_pi": PathInput("{subject}/data.csv")},
        )

        by_call = _db_path_input_params(
            self._with_path_input_history(populated_db, monkeypatch, "{subject}/data.csv"),
            "some_other_fn",
        )

        assert by_call == {}

    def test_history_targets_are_given_their_bindings(
        self, populated_db, monkeypatch
    ):
        from scidb import PathInput

        from scistack_gui import registry
        from scistack_gui.services.execution_service import _attach_db_path_inputs

        monkeypatch.setattr(
            registry,
            "get_path_inputs_registry",
            lambda: {"test_pi": PathInput("{subject}/data.csv")},
        )

        targets = [
            {"input_types": {}, "output_type": "Out", "constants": {}, "call_id": "call1"},
            {"input_types": {}, "output_type": "Out", "constants": {}, "call_id": "other"},
        ]
        result = _attach_db_path_inputs(
            self._with_path_input_history(populated_db, monkeypatch, "{subject}/data.csv"),
            "read_csv_like",
            targets,
        )

        from scistack_gui.domain.edge_resolver import (
            BINDING_PARAMETER,
            BINDING_PATHINPUT,
            bindings_of_kind,
        )

        assert bindings_of_kind(result[0]["bindings"], BINDING_PATHINPUT) == {
            "filepath_or_buffer": "test_pi"
        }
        # A call site with no recorded PathInput gets an empty binding, not
        # another call site's.
        assert bindings_of_kind(result[1]["bindings"], BINDING_PATHINPUT) == {}
        # History targets carry concrete recorded constants, so nothing needs
        # looking up in the Parameter registry.
        assert all(
            bindings_of_kind(t["bindings"], BINDING_PARAMETER) == {} for t in result
        )


class TestGraduatedPathInputNodeIsRunnable:
    """Regression test: a PathInput-fed node that has run and GRADUATED must
    still resolve to its own history when Run is clicked.

    The canvas hashes ``AggregatedData.fn_input_params``, where
    ``aggregate_variants`` has already partitioned the PathInput out into
    ``path_inputs``. derive_target_for_node hashed the RAW variant's
    ``input_types``, which still carries the PathInput spec — so the same
    call site hashed two different ways, the graduated node's embedded wiring
    matched nothing, and the GUI reported "No pipeline history or output
    connections found for 'pandas.read_csv'. Connect it to an output variable
    node first." for a green, fully wired, already-run node
    (examples/vo2max/scidb.log, run_id=sx6lpngy).

    Only PathInput-fed pipelines were affected: with variable-only inputs
    both views already agreed, which is why nothing caught it.
    """

    PI_SPEC = json.dumps(
        {"__type": "PathInput", "template": "{subject}/data.csv", "root_folder": None}
    )

    def _db_with_history(self, db, monkeypatch):
        from scidb import PathInput

        from scistack_gui import registry

        monkeypatch.setattr(
            registry,
            "get_path_inputs_registry",
            lambda: {"test_pi": PathInput("{subject}/data.csv")},
        )
        monkeypatch.setattr(
            db,
            "get_aggregated_variants",
            lambda *a, **k: {
                "path_inputs": {
                    "filepath_or_buffer": {
                        "template": "{subject}/data.csv",
                        "root_folder": None,
                        "functions": [("read_csv_like", "call1")],
                    }
                }
            },
        )
        # As recorded: the PathInput spec sits in input_types next to any
        # real variable inputs.
        monkeypatch.setattr(
            db,
            "list_pipeline_variants",
            lambda *a, **k: [
                {
                    "function_name": "read_csv_like",
                    "output_type": "Out",
                    "call_id": "call1",
                    "input_types": {"filepath_or_buffer": self.PI_SPEC},
                    "constants": {},
                    "record_count": 4,
                }
            ],
        )
        return db

    def _canvas_node_id(self, placement: str | None = None) -> str:
        """The id the canvas gives this call site — hashed from the
        PARTITIONED view, exactly as group_call_sites_by_wiring does."""
        from scistack_gui.domain.graph_builder import fn_node_id, wiring_id

        node_id = fn_node_id(
            "read_csv_like",
            wiring_id(
                "read_csv_like", {}, {"Out"}, {"filepath_or_buffer": "test_pi"}
            ),
        )
        return f"{node_id}::{placement}" if placement else node_id

    def test_graduated_node_resolves_to_its_history(self, populated_db, monkeypatch):
        db = self._db_with_history(populated_db, monkeypatch)
        targets = derive_target_for_node(db, self._canvas_node_id())
        assert len(targets) == 1, (
            "the graduated node's embedded wiring must match its own recorded "
            "history — an empty list is the 'connect it to an output variable "
            "node first' error on a node that has already run"
        )
        assert targets[0]["output_type"] == "Out"

    def test_graduated_node_resolves_with_a_placement_suffix(
        self, populated_db, monkeypatch
    ):
        """graduate_manual_node targets a placement-qualified id, which is
        what /api/run actually sends."""
        db = self._db_with_history(populated_db, monkeypatch)
        targets = derive_target_for_node(db, self._canvas_node_id("main"))
        assert len(targets) == 1

    def test_a_genuinely_different_wiring_still_matches_nothing(
        self, populated_db, monkeypatch
    ):
        """The fix must not make the comparison match everything — a node
        whose wiring really isn't in history still resolves to no targets."""
        from scistack_gui.domain.graph_builder import fn_node_id, wiring_id

        db = self._db_with_history(populated_db, monkeypatch)
        other = fn_node_id(
            "read_csv_like",
            wiring_id(
                "read_csv_like", {}, {"Different"}, {"filepath_or_buffer": "test_pi"}
            ),
        )
        assert derive_target_for_node(db, other) == []


class TestDefaultSchemaLevel:
    """Which keys a run iterates when nobody said: the node's own level, else
    where the function last ran, else what its inputs imply, else every key.
    One owner for the run thread and the compiled pipeline
    (``execution_service.default_schema_level``)."""

    def _target(self, **bindings):
        return {"constants": {}, "output_type": "X", "bindings": bindings}

    def test_the_node_level_wins_outright(self, populated_db):
        from scistack_gui.services.execution_service import default_schema_level

        level, why = default_schema_level(
            populated_db, "bandpass_filter", [], stated=["session", "subject"]
        )
        assert level == ["subject", "session"]  # dataset order, not stated order
        assert why == "stated on the node"

    def test_a_function_with_history_iterates_where_it_last_ran(self, populated_db):
        from scistack_gui.services.execution_service import default_schema_level

        level, why = default_schema_level(populated_db, "bandpass_filter", [])
        assert level == ["subject", "session"]
        assert "last ran" in why

    def test_a_never_run_function_takes_its_inputs_level(self, populated_db):
        """A subject-level input implies a subject-level run — not one call
        per session with the subject's row broadcast into each."""
        from scidb import BaseVariable

        from scistack_gui.domain.edge_resolver import variable_binding
        from scistack_gui.services.execution_service import default_schema_level

        class SubjectOnly(BaseVariable):
            pass

        SubjectOnly.save(np.array([1.0]), subject="S01")
        level, why = default_schema_level(
            populated_db, "never_ran", [self._target(x=variable_binding(["SubjectOnly"]))]
        )
        assert level == ["subject"]
        assert "inputs" in why

    def test_inputs_at_different_levels_iterate_the_finer_union(self, populated_db):
        """The coarser input broadcasts; the run iterates every key any input
        carries (docs/claude/coarse-level-inputs.md)."""
        from scidb import BaseVariable

        from scistack_gui.domain.edge_resolver import variable_binding
        from scistack_gui.services.execution_service import default_schema_level

        class SubjectOnly2(BaseVariable):
            pass

        SubjectOnly2.save(np.array([1.0]), subject="S01")
        level, _ = default_schema_level(
            populated_db,
            "never_ran",
            [
                self._target(
                    coarse=variable_binding(["SubjectOnly2"]),
                    fine=variable_binding(["RawSignal"]),
                )
            ],
        )
        assert level == ["subject", "session"]

    def test_a_path_input_template_names_its_level(self, populated_db, monkeypatch):
        from scistack_gui import registry
        from scistack_gui.domain.edge_resolver import pathinput_binding
        from scistack_gui.services.execution_service import default_schema_level

        class FakePathInput:
            def placeholder_keys(self):
                return ["subject", "not_a_key"]

        monkeypatch.setattr(registry, "get_path_inputs_registry", lambda: {"files": FakePathInput()})
        level, why = default_schema_level(
            populated_db, "never_ran", [self._target(f=pathinput_binding("files"))]
        )
        assert level == ["subject"]
        assert "inputs" in why

    def test_nothing_to_go_on_means_every_key(self, populated_db):
        from scistack_gui.services.execution_service import default_schema_level

        level, why = default_schema_level(populated_db, "never_ran", [])
        assert level == ["subject", "session"]
        assert "no history" in why


class TestDatasetLevelDefault:
    """A once-per-dataset operation — a PathInput naming no schema key, a
    variable saved with none — implies NO iteration: one call. That is a
    level (`[]`), and is not the "nothing to go on" case that means every
    key. It is spelled the way `for_each` reads it: `schema_keys=None`
    iterates nothing (one call), `schema_keys=[]` iterates every key — as
    `subject=[]` means every subject."""

    def _target(self, **bindings):
        return {"constants": {}, "output_type": "X", "bindings": bindings}

    def test_a_path_input_with_no_schema_placeholder_means_one_call(
        self, populated_db, monkeypatch
    ):
        from scistack_gui import registry
        from scistack_gui.domain.edge_resolver import pathinput_binding
        from scistack_gui.services.execution_service import default_schema_level

        class DatasetFile:
            def placeholder_keys(self):
                return []

        monkeypatch.setattr(
            registry, "get_path_inputs_registry", lambda: {"config": DatasetFile()}
        )
        level, why = default_schema_level(
            populated_db, "never_ran", [self._target(f=pathinput_binding("config"))]
        )
        assert level is None
        assert "one call over the whole dataset" in why

    def test_a_dataset_level_variable_means_one_call(self, populated_db):
        from scidb import BaseVariable

        from scistack_gui.domain.edge_resolver import variable_binding
        from scistack_gui.services.execution_service import default_schema_level

        class WholeDataset(BaseVariable):
            pass

        WholeDataset.save(np.array([1.0, 2.0, 3.0]))
        level, why = default_schema_level(
            populated_db, "never_ran", [self._target(x=variable_binding(["WholeDataset"]))]
        )
        assert level is None
        assert "one call" in why

    def test_a_dataset_level_input_beside_a_finer_one_iterates_the_finer(
        self, populated_db
    ):
        from scidb import BaseVariable

        from scistack_gui.domain.edge_resolver import variable_binding
        from scistack_gui.services.execution_service import default_schema_level

        class WholeDataset2(BaseVariable):
            pass

        WholeDataset2.save(np.array([1.0]))
        level, _ = default_schema_level(
            populated_db,
            "never_ran",
            [
                self._target(
                    whole=variable_binding(["WholeDataset2"]),
                    per=variable_binding(["RawSignal"]),
                )
            ],
        )
        assert level == ["subject", "session"]

    def test_an_unbound_or_recordless_input_is_not_a_level(self, populated_db):
        """A variable with no records says nothing about level, so with no
        other input the default is still every key."""
        from scistack_gui.domain.edge_resolver import variable_binding
        from scistack_gui.services.execution_service import default_schema_level

        level, why = default_schema_level(
            populated_db, "never_ran", [self._target(x=variable_binding(["NoRecordsYet"]))]
        )
        assert level == ["subject", "session"]
        assert "no history" in why


class TestPooledBindingRoundTrip:
    """A run that pooled an input across every variant group
    (`AcrossVariants`) records `_invocation.across_variants`; a history-
    derived target carries it as `pool_variants` on the binding, and the
    run inputs the GUI builds wrap the class again — so a re-run from the
    canvas pools what the original pooled (the fact round-trips)."""

    def test_history_binding_carries_pooling_and_build_run_inputs_wraps_it(
        self, populated_db, monkeypatch
    ):
        from scidb import AcrossVariants

        from scistack_gui import registry
        from scistack_gui.services.execution_service import (
            _attach_db_path_inputs,
            build_run_inputs,
            variable_inputs_view,
        )

        monkeypatch.setattr(registry, "get_path_inputs_registry", lambda: {})
        targets = [
            {
                "input_types": {"signal": "RawSignal"},
                "selectors": {},
                "across_variants": ["signal"],
                "output_type": "FilteredSignal",
                "constants": {"low_hz": 20},
                "call_id": "c1",
            }
        ]
        (target,) = _attach_db_path_inputs(populated_db, "bandpass_filter", targets)
        assert target["bindings"]["signal"].get("pool_variants") is True

        inputs = build_run_inputs(target, "bandpass_filter", populated_db)
        assert isinstance(inputs["signal"], AcrossVariants)
        assert inputs["signal"].var_type is registry.get_variable_class("RawSignal")

        # ...and the MATLAB rendering of the same binding says so too.
        assert variable_inputs_view([target], "bandpass_filter")["signal"] == {
            "types": ["RawSignal"],
            "columns": [],
            "iterate": False,
            "pool_variants": True,
        }
