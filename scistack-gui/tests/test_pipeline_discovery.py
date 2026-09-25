"""
Tests for scistack_gui.pipeline_discovery — Direction 1 of the source<->GUI
pipeline translation (source -> GUI import). See
docs/claude/code-discovery-categories.md and
.claude/plan-pathinput-sweep-submodule-source-of-truth.md.
"""

from __future__ import annotations

from scidb import BaseVariable, Pipeline
from scistack_gui import pipeline_store as ps
from scistack_gui.pipeline_discovery import discover_and_seed_pipelines


class RawA(BaseVariable):
    pass


class FilteredA(BaseVariable):
    pass


def _bandpass(signal, low_hz):
    return signal


def _register_step(pipe, fn, inputs, outputs):
    """Directly append a StepSpec, bypassing for_each's ambient-pipeline
    detection — the unit under test only cares about the resulting
    Pipeline.steps/.uses shape, not how they got there."""
    pipe.register_call(
        fn=fn, inputs=inputs, outputs=outputs, metadata_iterables={}, options={}
    )


class TestDiscoverAndSeedPipelines:
    def test_creates_pipeline_with_function_node_and_edges(self, populated_db):
        db = populated_db
        pipe = Pipeline("gait_analysis")
        _register_step(pipe, _bandpass, {"signal": RawA, "low_hz": 20}, [FilteredA])

        result = discover_and_seed_pipelines(db)
        assert result["created"] == ["gait_analysis"]
        assert result["skipped"] == []

        pipelines = {p["name"]: p["pipeline_id"] for p in ps.list_pipelines(db)}
        assert "gait_analysis" in pipelines
        pid = pipelines["gait_analysis"]

        nodes = ps.get_manual_nodes(db, pid)
        fn_nodes = [n for n in nodes.values() if n["type"] == "functionNode"]
        assert len(fn_nodes) == 1
        assert fn_nodes[0]["label"] == "_bandpass"

        # Manual variable/constant nodes are required, not just edges --
        # build_variable_nodes/build_parameter_nodes only render from DB
        # history, so a never-run type/constant needs a manual node to
        # show up at all (see _seed_step's docstring). Node ids are
        # ARBITRARY (discovered_var_*/discovered_const_*), not the bare
        # canonical var__RawA/param__low_hz form -- see
        # _get_or_create_node's docstring for why (avoids a cross-pipeline
        # id collision on the shared _pipeline_nodes primary key). Look
        # up by (type, label) instead, same as merge_manual_nodes does.
        def _node_id(node_type, label):
            return next(
                nid for nid, n in nodes.items()
                if n["type"] == node_type and n["label"] == label
            )

        raw_a_id = _node_id("variableNode", "RawA")
        filtered_a_id = _node_id("variableNode", "FilteredA")
        low_hz_id = _node_id("parameterNode", "low_hz")

        edges = ps.get_manual_edges(db)
        source_ids = {e["source"] for e in edges}
        target_ids = {e["target"] for e in edges}
        assert raw_a_id in source_ids
        assert low_hz_id in source_ids
        assert filtered_a_id in target_ids

    def test_discarded_from_scidb_bookkeeping_after_seeding(self, populated_db):
        from scidb.pipeline import _all_pipelines

        db = populated_db
        pipe = Pipeline("p1")
        _register_step(pipe, _bandpass, {"signal": RawA, "low_hz": 20}, [FilteredA])
        assert pipe in _all_pipelines

        discover_and_seed_pipelines(db)
        assert pipe not in _all_pipelines

    def test_second_call_finds_nothing_new(self, populated_db):
        db = populated_db
        pipe = Pipeline("p2")
        _register_step(pipe, _bandpass, {"signal": RawA, "low_hz": 20}, [FilteredA])
        discover_and_seed_pipelines(db)

        result = discover_and_seed_pipelines(db)
        assert result == {"created": [], "skipped": []}

    def test_existing_local_pipeline_is_skipped_not_overwritten(self, populated_db):
        """'Create once' — matches create_variable/create_path_input's
        precedent, never overwrites hand-edited GUI state."""
        db = populated_db
        ps.create_pipeline(db, "existing")

        pipe = Pipeline("existing")
        _register_step(pipe, _bandpass, {"signal": RawA, "low_hz": 20}, [FilteredA])

        result = discover_and_seed_pipelines(db)
        assert result == {"created": [], "skipped": ["existing"]}

        pipelines = ps.list_pipelines(db)
        assert len([p for p in pipelines if p["name"] == "existing"]) == 1
        pid = next(p["pipeline_id"] for p in pipelines if p["name"] == "existing")
        assert ps.get_manual_nodes(db, pid) == {}  # untouched, no nodes seeded

    def test_compiled_pipeline_with_db_is_never_a_discovery_candidate(
        self, populated_db
    ):
        """execution_service.build_backend_pipeline's own per-request
        compiled Pipelines always set db= -- must never be mistaken for a
        source-authored pipeline (see module docstring)."""
        db = populated_db
        pipe = Pipeline("compiled_lookalike", db=db)
        _register_step(pipe, _bandpass, {"signal": RawA, "low_hz": 20}, [FilteredA])

        result = discover_and_seed_pipelines(db)
        assert result == {"created": [], "skipped": []}
        pipe.discard()

    def test_scalar_constant_wired_with_pending_value(self, populated_db):
        db = populated_db
        pipe = Pipeline("const_pipe")
        _register_step(pipe, _bandpass, {"signal": RawA, "low_hz": 42}, [FilteredA])

        discover_and_seed_pipelines(db)

        pending = ps.get_pending_constants(db)
        assert "42" in pending.get("low_hz", set())

        pid = next(
            p["pipeline_id"] for p in ps.list_pipelines(db) if p["name"] == "const_pipe"
        )
        nodes = ps.get_manual_nodes(db, pid)
        const_node_id = next(
            nid for nid, n in nodes.items()
            if n["type"] == "parameterNode" and n["label"] == "low_hz"
        )
        edges = ps.get_manual_edges(db)
        assert any(e["source"] == const_node_id for e in edges)

    def test_recursive_uses_creates_child_and_binding(self, populated_db):
        db = populated_db
        child = Pipeline("loading")
        _register_step(child, _bandpass, {"signal": RawA, "low_hz": 20}, [FilteredA])

        Pipeline("analysis", uses=[child])

        result = discover_and_seed_pipelines(db)
        assert set(result["created"]) == {"loading", "analysis"}

        pipelines = {p["name"]: p["pipeline_id"] for p in ps.list_pipelines(db)}
        uses = ps.get_pipeline_uses(db, pipelines["analysis"])
        assert len(uses) == 1
        assert uses[0]["child_pipeline_id"] == pipelines["loading"]

    def test_use_of_already_local_child_reuses_it(self, populated_db):
        """A used pipeline that already exists locally is reused by id, not
        recreated -- the parent's use edge still gets wired correctly."""
        db = populated_db
        existing_child_id = ps.create_pipeline(db, "shared_prep")

        child = Pipeline("shared_prep")
        Pipeline("analysis3", uses=[child])

        result = discover_and_seed_pipelines(db)
        assert result["created"] == ["analysis3"]
        assert result["skipped"] == ["shared_prep"]

        pipelines = {p["name"]: p["pipeline_id"] for p in ps.list_pipelines(db)}
        uses = ps.get_pipeline_uses(db, pipelines["analysis3"])
        assert uses[0]["child_pipeline_id"] == existing_child_id

    def test_bound_use_carries_key_map_and_iterate_into_binding(self, populated_db):
        db = populated_db
        child = Pipeline("loading2")
        _register_step(child, _bandpass, {"signal": RawA, "low_hz": 20}, [FilteredA])

        Pipeline(
            "analysis2",
            uses=[
                child.bind(
                    key_map={"subject": "participant"},
                    iterate={"participant": ["1", "2"]},
                )
            ],
        )

        discover_and_seed_pipelines(db)

        pipelines = {p["name"]: p["pipeline_id"] for p in ps.list_pipelines(db)}
        uses = ps.get_pipeline_uses(db, pipelines["analysis2"])
        assert uses[0]["binding"]["key_map"] == {"subject": "participant"}
        assert uses[0]["binding"]["iterate"] == {"participant": ["1", "2"]}

    def test_named_pathinput_wires_by_registered_name_not_param(self, populated_db):
        from scidb import PathInput
        from scistack_gui import registry

        db = populated_db
        raw_emg = PathInput("{subject}/{trial}.mat", name="{subject}/{trial}.mat")
        registry._path_inputs["RAW_EMG"] = raw_emg
        registry._path_input_sources["RAW_EMG"] = "test"

        def _loader(filepath):
            return filepath

        pipe = Pipeline("loader_pipe")
        _register_step(pipe, _loader, {"filepath": raw_emg}, [FilteredA])

        discover_and_seed_pipelines(db)

        edges = ps.get_manual_edges(db)
        assert any(e["source"] == "pathInput__RAW_EMG" for e in edges)

    def test_unnamed_pathinput_falls_back_to_param_name(self, populated_db):
        from scidb import PathInput

        db = populated_db

        def _loader(filepath):
            return filepath

        pipe = Pipeline("loader_pipe2")
        _register_step(
            pipe, _loader, {"filepath": PathInput("{subject}.mat", name="{subject}.mat")}, [FilteredA]
        )

        discover_and_seed_pipelines(db)

        edges = ps.get_manual_edges(db)
        assert any(e["source"] == "pathInput__filepath" for e in edges)

    def test_named_sweep_wires_by_registered_name(self, populated_db):
        from scidb import Parameter
        from scistack_gui import registry

        db = populated_db
        window = Parameter(10, 20, 30)
        registry._parameters["WINDOW"] = window
        registry._parameter_sources["WINDOW"] = "test"

        def _fn(window_seconds):
            return window_seconds

        pipe = Pipeline("sweep_pipe")
        _register_step(pipe, _fn, {"window_seconds": window}, [FilteredA])

        discover_and_seed_pipelines(db)

        edges = ps.get_manual_edges(db)
        assert any(e["source"] == "param__WINDOW" for e in edges)

    def test_no_candidates_is_a_cheap_noop(self, populated_db):
        result = discover_and_seed_pipelines(populated_db)
        assert result == {"created": [], "skipped": []}

    def test_shared_variable_across_two_pipelines_gets_independent_nodes(
        self, populated_db
    ):
        """Regression test for the cross-pipeline manual-node collision:
        _pipeline_nodes scopes a node to exactly one pipeline_id
        (last-write-wins on that column), so writing the bare canonical id
        (var__RawA) directly as the manual node id would make BOTH
        pipelines fight over one global row -- whichever was seeded last
        would silently steal it from the other's scope, leaving the first
        pipeline with a dangling edge to a node invisible in its own
        canvas. _get_or_create_node's arbitrary-id-per-pipeline design is
        what prevents that -- assert both pipelines end up with their OWN
        RawA node, each correctly wired within its own scope."""
        db = populated_db
        pipe_a = Pipeline("pipeline_a")
        _register_step(pipe_a, _bandpass, {"signal": RawA, "low_hz": 20}, [FilteredA])
        pipe_b = Pipeline("pipeline_b")
        _register_step(pipe_b, _bandpass, {"signal": RawA, "low_hz": 30}, [FilteredA])

        result = discover_and_seed_pipelines(db)
        assert set(result["created"]) == {"pipeline_a", "pipeline_b"}

        pipelines = {p["name"]: p["pipeline_id"] for p in ps.list_pipelines(db)}
        pid_a, pid_b = pipelines["pipeline_a"], pipelines["pipeline_b"]

        nodes_a = ps.get_manual_nodes(db, pid_a)
        nodes_b = ps.get_manual_nodes(db, pid_b)

        def _raw_a_id(nodes):
            return next(
                nid for nid, n in nodes.items()
                if n["type"] == "variableNode" and n["label"] == "RawA"
            )

        raw_a_in_a = _raw_a_id(nodes_a)
        raw_a_in_b = _raw_a_id(nodes_b)

        # Each pipeline has its OWN RawA node -- not the same row/id.
        assert raw_a_in_a != raw_a_in_b
        # Each pipeline's own scope query returns its own node (proves
        # neither node's pipeline_id column was overwritten by the other).
        assert raw_a_in_a in nodes_a
        assert raw_a_in_b in nodes_b
        assert raw_a_in_a not in nodes_b
        assert raw_a_in_b not in nodes_a

        # Both are correctly wired to their OWN pipeline's function node.
        edges = ps.get_manual_edges(db)
        fn_a_id = next(
            nid for nid, n in nodes_a.items() if n["type"] == "functionNode"
        )
        fn_b_id = next(
            nid for nid, n in nodes_b.items() if n["type"] == "functionNode"
        )
        assert any(
            e["source"] == raw_a_in_a and e["target"] == fn_a_id for e in edges
        )
        assert any(
            e["source"] == raw_a_in_b and e["target"] == fn_b_id for e in edges
        )

    def test_shared_variable_within_one_pipeline_reuses_one_node(self, populated_db):
        """The fix for the cross-pipeline case must not regress the
        within-one-pipeline case: two steps of the SAME pipeline
        referencing the same variable share one node (the node_cache is
        per-pipeline, reset for each new pipeline, not per-step)."""
        db = populated_db

        def _step_two(filtered):
            return filtered

        pipe = Pipeline("shared_within")
        _register_step(pipe, _bandpass, {"signal": RawA, "low_hz": 20}, [FilteredA])
        _register_step(pipe, _step_two, {"filtered": FilteredA}, [RawA])

        discover_and_seed_pipelines(db)

        pid = next(
            p["pipeline_id"] for p in ps.list_pipelines(db) if p["name"] == "shared_within"
        )
        nodes = ps.get_manual_nodes(db, pid)
        raw_a_ids = {
            nid for nid, n in nodes.items()
            if n["type"] == "variableNode" and n["label"] == "RawA"
        }
        filtered_a_ids = {
            nid for nid, n in nodes.items()
            if n["type"] == "variableNode" and n["label"] == "FilteredA"
        }
        # RawA is BOTH an input (step 1) and an output (step 2) -- one
        # shared node either way, not two.
        assert len(raw_a_ids) == 1
        assert len(filtered_a_ids) == 1
