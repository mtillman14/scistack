"""One Parameter node after a run, when its declared name differs from the
argument it feeds (cleanup-audit B1, docs/claude/cleanup-audit.md §3).

The canvas keys a Parameter by its DECLARED name (``gaitrite_config``); the run
fills the function ARGUMENT (``gaitRiteConfig``). scidb now records the
declared name and reports ``{argument: node}``; these tests pin the GUI half:
the node, the edge's source vs handle, pending-value coverage, and the two run
routes that must state the names (Python ``for_each`` and generated MATLAB).
"""

import hashlib

from scidb import Parameter

from scistack_gui.domain.graph_builder import (
    build_edges,
    build_parameter_nodes,
    edge_dedup_key,
    pending_value_group_coverage,
)

from scistack_gui.ids import param_handle, param_node_id


def aggregate_variants(
    variants,
    listed_var_names=frozenset(),
    path_input_registry=None,
    path_input_history=None,
    project_root=None,
):
    """Variant rows -> AggregatedData through the two PRODUCTION conversions
    (scidb's pure ``aggregate_pipeline_variants``, then
    ``graph_builder.aggregate_from_scidb``). The GUI's own test-only
    converter of the same name was deleted (cleanup-audit F19); this keeps the
    tests' call shape while exercising the real path. ``listed_var_names``
    adds types that exist with no variants, as the old helper did.
    """
    from scidb.database import aggregate_pipeline_variants

    from scistack_gui.domain.graph_builder import aggregate_from_scidb

    agg = aggregate_from_scidb(
        aggregate_pipeline_variants(variants),
        path_input_registry,
        path_input_history,
        project_root,
    )
    agg.all_var_types |= set(listed_var_names)
    return agg


FN = "loadGaitRiteOneFile"
ARG = "gaitRiteConfig"
DECLARED = "gaitrite_config"
CID = hashlib.sha256(b"b1-call").hexdigest()[:16]
FKEY = (FN, CID)
FN_NODE = f"fn__{FN}__{CID}"


def _variant(value=5, names=None):
    return {
        "function_name": FN,
        "output_type": "GAITRiteLoaded",
        "call_id": CID,
        "input_types": {},
        "constants": {ARG: value},
        "parameter_names": {ARG: DECLARED} if names is None else names,
        "record_count": 3,
    }


def _agg(**kw):
    return aggregate_variants([_variant(**kw)], listed_var_names=set())


class TestAggregate:
    def test_counts_are_keyed_by_the_node_and_handles_by_the_argument(self):
        agg = _agg()
        assert set(agg.const_counts) == {DECLARED}
        assert agg.const_fns[DECLARED] == {FKEY}
        assert agg.fn_constants[FKEY] == {ARG}
        assert agg.constant_node(FKEY, ARG) == DECLARED
        assert agg.constant_args(FKEY, DECLARED) == [ARG]

    def test_no_recorded_name_keeps_the_argument_as_the_node(self):
        agg = _agg(names={})
        assert set(agg.const_counts) == {ARG}
        assert agg.constant_node(FKEY, ARG) == ARG


class TestOneNodeAfterARun:
    def test_history_and_declaration_make_one_parameter_node(self):
        """The regression itself: before the fix this built two nodes,
        param__gaitRiteConfig (history) and param__gaitrite_config (source)."""
        agg = _agg()
        declared = Parameter(5)
        declared.name = DECLARED
        nodes = build_parameter_nodes(
            agg.const_counts, {}, source_parameters={DECLARED: declared}
        )
        assert [n["id"] for n in nodes] == [param_node_id(DECLARED)]

    def test_the_edge_leaves_the_node_and_lands_on_the_argument(self):
        agg = _agg()
        edges = build_edges(
            fn_input_params={},
            fn_outputs={},
            const_fns=agg.const_fns,
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
            fn_parameter_names=agg.fn_parameter_names,
        )
        [edge] = edges
        assert edge["source"] == param_node_id(DECLARED)
        assert edge["target"] == FN_NODE
        assert edge["targetHandle"] == param_handle(ARG)
        # Keyed by the argument: what hidden_wirings and the disconnected
        # report spell for this handle.
        assert edge["id"] == f"e__{ARG}__{FN}__{CID}"

    def test_the_users_manual_edge_is_the_same_wire(self):
        """The manual edge the user drew (declared node -> argument handle)
        dedups against the history edge instead of being orphaned next to a
        second node."""
        agg = _agg()
        [edge] = build_edges(
            fn_input_params={},
            fn_outputs={},
            const_fns=agg.const_fns,
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
            fn_parameter_names=agg.fn_parameter_names,
        )
        manual = edge_dedup_key(param_node_id(DECLARED), FN_NODE, param_handle(ARG))
        assert edge_dedup_key(
            edge["source"], edge["target"], edge["targetHandle"]
        ) == manual

    def test_without_names_the_edge_is_unchanged(self):
        edges = build_edges(
            fn_input_params={},
            fn_outputs={},
            const_fns={"hz": {FKEY}},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
        )
        [edge] = edges
        assert edge["source"] == "param__hz"
        assert edge["targetHandle"] == param_handle("hz")
        assert edge["id"] == f"e__hz__{FN}__{CID}"


def test_pending_values_keyed_by_the_node_see_rows_keyed_by_the_argument():
    """Pending values are written by the Parameter node's UI (declared name);
    a real run's row is keyed by the argument. Coverage must translate, or a
    value that already ran stays "pending" forever."""
    agg = _agg(value=5)
    coverage = pending_value_group_coverage({DECLARED: {"5"}}, agg)
    assert coverage[(DECLARED, "5")], "the run that recorded 5 must cover it"


def test_a_gui_run_states_the_names_from_its_wiring():
    """Parameter AND PathInput bindings: the PathInput name groups the step
    (cleanup-audit F38)."""
    from scistack_gui.services.execution_service import build_run_declared_names

    target = {
        "bindings": {
            ARG: {"kind": "parameter", "ref": DECLARED},
            "gaitRitePath": {"kind": "pathinput", "ref": "GaitRite"},
        }
    }
    assert build_run_declared_names(target) == {
        ARG: DECLARED,
        "gaitRitePath": "GaitRite",
    }


class TestGeneratedMatlab:
    def test_the_first_run_template_records_the_names(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name=FN,
            db_path="/data/experiment.duckdb",
            schema_keys=["subject", "session"],
            parameter_names={ARG: DECLARED},
        )
        assert f"'parameter_names', struct('{ARG}', '{DECLARED}')" in cmd

    def test_a_rerun_from_history_records_the_names(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name=FN,
            db_path="/data/experiment.duckdb",
            schema_keys=["subject", "session"],
            variants=[
                {
                    "input_types": {},
                    "output_type": "GAITRiteLoaded",
                    "constants": {ARG: 5},
                    "record_count": 3,
                }
            ],
            parameter_names={ARG: DECLARED},
        )
        assert f"'parameter_names', struct('{ARG}', '{DECLARED}')" in cmd

    def test_no_names_no_pair(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name=FN,
            db_path="/data/experiment.duckdb",
            schema_keys=["subject", "session"],
        )
        assert "parameter_names" not in cmd
