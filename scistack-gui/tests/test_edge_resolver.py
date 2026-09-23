"""
Unit tests for scistack_gui.domain.edge_resolver.

All functions in this module are pure (no I/O), so no fixtures are needed.
"""

from scistack_gui.domain.edge_resolver import (
    ResolvedEdges,
    infer_manual_fn_output_types,
    infer_manual_fn_param_to_class,
    node_id_to_var_label,
    resolve_function_edges,
)

# ---------------------------------------------------------------------------
# node_id_to_var_label
# ---------------------------------------------------------------------------


class TestNodeIdToVarLabel:
    def test_db_node_in_existing_labels(self):
        result = node_id_to_var_label(
            "var__RawEMG",
            existing_node_labels={"var__RawEMG": "RawEMG"},
            manual_nodes={},
        )
        assert result == "RawEMG"

    def test_db_node_fallback_to_id_parsing(self):
        # Not in existing_node_labels — fall back to splitting on "__"
        result = node_id_to_var_label(
            "var__FilteredSignal",
            existing_node_labels={},
            manual_nodes={},
        )
        assert result == "FilteredSignal"

    def test_db_node_multi_segment_id_uses_second_part(self):
        result = node_id_to_var_label(
            "var__My__Signal",
            existing_node_labels={},
            manual_nodes={},
        )
        assert result == "My"

    def test_manual_variable_node(self):
        manual_nodes = {"uuid-abc": {"type": "variableNode", "label": "RawEMG"}}
        result = node_id_to_var_label(
            "uuid-abc",
            existing_node_labels={},
            manual_nodes=manual_nodes,
        )
        assert result == "RawEMG"

    def test_manual_non_variable_node_returns_none(self):
        manual_nodes = {"uuid-abc": {"type": "functionNode", "label": "my_fn"}}
        result = node_id_to_var_label(
            "uuid-abc",
            existing_node_labels={},
            manual_nodes=manual_nodes,
        )
        assert result is None

    def test_unknown_node_returns_none(self):
        result = node_id_to_var_label(
            "unknown-id",
            existing_node_labels={},
            manual_nodes={},
        )
        assert result is None

    def test_non_var_prefix_not_in_manual_returns_none(self):
        result = node_id_to_var_label(
            "fn__my_func",
            existing_node_labels={},
            manual_nodes={},
        )
        assert result is None


# ---------------------------------------------------------------------------
# resolve_function_edges — output wiring
# ---------------------------------------------------------------------------


class TestResolveFunctionEdgesOutputs:
    def test_output_from_db_variable_node(self):
        edges = [
            {
                "source": "fn__my_func",
                "target": "var__RawEMG",
                "targetHandle": "",
                "sourceHandle": "",
            }
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__my_func"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={"var__RawEMG": "RawEMG"},
        )
        assert result.output_types == ["RawEMG"]

    def test_output_from_manual_variable_node(self):
        manual_nodes = {
            "uuid-out": {"type": "variableNode", "label": "ProcessedSignal"}
        }
        edges = [
            {
                "source": "fn__my_func",
                "target": "uuid-out",
                "targetHandle": "",
                "sourceHandle": "",
            }
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__my_func"},
            manual_edges=edges,
            manual_nodes=manual_nodes,
            existing_node_labels={},
        )
        assert result.output_types == ["ProcessedSignal"]

    def test_duplicate_output_is_deduplicated(self):
        edges = [
            {
                "source": "fn__my_func",
                "target": "var__RawEMG",
                "targetHandle": "",
                "sourceHandle": "",
            },
            {
                "source": "fn__my_func",
                "target": "var__RawEMG",
                "targetHandle": "",
                "sourceHandle": "",
            },
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__my_func"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={"var__RawEMG": "RawEMG"},
        )
        assert result.output_types == ["RawEMG"]

    def test_output_order_is_preserved(self):
        edges = [
            {
                "source": "fn__my_func",
                "target": "var__A",
                "targetHandle": "",
                "sourceHandle": "",
            },
            {
                "source": "fn__my_func",
                "target": "var__B",
                "targetHandle": "",
                "sourceHandle": "",
            },
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__my_func"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result.output_types == ["A", "B"]

    def test_non_variable_output_target_is_ignored(self):
        manual_nodes = {"uuid-fn": {"type": "functionNode", "label": "other_fn"}}
        edges = [
            {
                "source": "fn__my_func",
                "target": "uuid-fn",
                "targetHandle": "",
                "sourceHandle": "",
            }
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__my_func"},
            manual_edges=edges,
            manual_nodes=manual_nodes,
            existing_node_labels={},
        )
        assert result.output_types == []


# ---------------------------------------------------------------------------
# resolve_function_edges — input wiring (with targetHandle)
# ---------------------------------------------------------------------------


class TestResolveFunctionEdgesInputs:
    def test_named_input_via_target_handle(self):
        edges = [
            {
                "source": "var__RawEMG",
                "target": "fn__my_func",
                "targetHandle": "in__signal",
                "sourceHandle": "",
            }
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__my_func"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={"var__RawEMG": "RawEMG"},
        )
        # One view, the history shape: bare for a single type. The always-list
        # rendering is input_type_candidates, never an identity input.
        assert result.input_types == {"signal": "RawEMG"}
        assert result.input_type_candidates == {"signal": ["RawEMG"]}

    def test_multiple_inputs_same_param(self):
        edges = [
            {
                "source": "var__A",
                "target": "fn__fn",
                "targetHandle": "in__signal",
                "sourceHandle": "",
            },
            {
                "source": "var__B",
                "target": "fn__fn",
                "targetHandle": "in__signal",
                "sourceHandle": "",
            },
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert set(result.input_types["signal"]) == {"A", "B"}

    def test_duplicate_var_not_added_twice_to_same_param(self):
        edges = [
            {
                "source": "var__A",
                "target": "fn__fn",
                "targetHandle": "in__signal",
                "sourceHandle": "",
            },
            {
                "source": "var__A",
                "target": "fn__fn",
                "targetHandle": "in__signal",
                "sourceHandle": "",
            },
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result.input_types["signal"] == "A"

    def test_handleless_input_edge_is_dropped_not_positionally_assigned(self):
        # Used to be matched to the first unresolved signature param BY
        # POSITION. On a function with many params that bound whichever one
        # happened to be free, which is a guess, not a wiring.
        edges = [
            {
                "id": "e1",
                "source": "var__RawEMG",
                "target": "fn__fn",
                "targetHandle": "",
                "sourceHandle": "",
            },
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={"var__RawEMG": "RawEMG"},
        )
        assert result.input_types == {}

    def test_handleless_edge_does_not_disturb_handled_ones(self):
        edges = [
            {
                "id": "e1",
                "source": "var__Hz",
                "target": "fn__fn",
                "targetHandle": "in__low_hz",
                "sourceHandle": "",
            },
            {
                "id": "e2",
                "source": "var__RawEMG",
                "target": "fn__fn",
                "targetHandle": "",
                "sourceHandle": "",
            },
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result.input_types == {"low_hz": "Hz"}

    def test_fn_node_ids_set_matches_variant_ids(self):
        # Both "fn__fn" and a UUID variant ID should be recognised.
        edges = [
            {
                "source": "var__A",
                "target": "fn__fn__uuid1",
                "targetHandle": "in__x",
                "sourceHandle": "",
            },
            {
                "source": "fn__fn__uuid1",
                "target": "var__B",
                "targetHandle": "",
                "sourceHandle": "",
            },
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn", "fn__fn__uuid1"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result.input_types == {"x": "A"}
        assert result.output_types == ["B"]


# ---------------------------------------------------------------------------
# resolve_function_edges — Parameter wiring
# ---------------------------------------------------------------------------


class TestResolveFunctionEdgesParameters:
    def test_db_parameter_node_by_prefix(self):
        # build_edges writes the declared name into BOTH the node id and the
        # handle, so param name == declared name by construction here.
        edges = [
            {
                "source": "param__low_hz",
                "target": "fn__fn",
                "targetHandle": "param__low_hz",
                "sourceHandle": "",
            }
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result.parameter_params == {"low_hz": "low_hz"}
        assert result.input_types == {}

    def test_parameter_via_in_handle(self):
        edges = [
            {
                "source": "param__low_hz",
                "target": "fn__fn",
                "targetHandle": "in__low_hz",
                "sourceHandle": "",
            }
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result.parameter_params == {"low_hz": "low_hz"}

    def test_declared_name_differing_from_param_name_is_kept_separate(self):
        # The case name-matching could never express: a Parameter declared
        # 'test' feeding read_csv's 'sep'. The param is the binding key; the
        # declared name is what the registry and the hidden-value store are
        # keyed by.
        edges = [
            {
                "source": "param__test",
                "target": "fn__read_csv",
                "targetHandle": "in__sep",
                "sourceHandle": "",
            }
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__read_csv"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result.parameter_params == {"sep": "test"}

    def test_manual_parameter_node_uses_its_label_as_declared_name(self):
        manual_nodes = {
            "uuid-const": {"type": "parameterNode", "label": "threshold"},
        }
        edges = [
            {
                "source": "uuid-const",
                "target": "fn__fn",
                "targetHandle": "in__cutoff",
                "sourceHandle": "",
            }
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes=manual_nodes,
            existing_node_labels={},
        )
        assert result.parameter_params == {"cutoff": "threshold"}

    def test_handleless_parameter_edge_is_dropped(self):
        # Used to fall back to the source node's LABEL as the param name,
        # which is only ever right when the two names coincide.
        manual_nodes = {
            "uuid-const": {"type": "parameterNode", "label": "threshold"},
        }
        edges = [
            {
                "id": "e1",
                "source": "uuid-const",
                "target": "fn__fn",
                "targetHandle": "",
                "sourceHandle": "",
            }
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes=manual_nodes,
            existing_node_labels={},
        )
        assert result.parameter_params == {}

    def test_parameter_not_added_to_input_types(self):
        edges = [
            {
                "source": "param__low_hz",
                "target": "fn__fn",
                "targetHandle": "param__low_hz",
                "sourceHandle": "",
            }
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert "low_hz" not in result.input_types

    def test_placement_qualified_parameter_source_resolves(self):
        edges = [
            {
                "source": "param__low_hz::main",
                "target": "fn__fn",
                "targetHandle": "in__cutoff",
                "sourceHandle": "",
            }
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result.parameter_params == {"cutoff": "low_hz"}


# ---------------------------------------------------------------------------
# resolve_function_edges — PathInput wiring
# ---------------------------------------------------------------------------


class TestResolveFunctionEdgesPathInputs:
    def test_path_input_binds_to_the_param_its_handle_names(self):
        # The exact wiring from the GUI session that motivated this: a
        # PathInput declared 'test_pi' feeding read_csv's first parameter.
        # Name matching resolved nothing here and the run silently did no
        # work at all.
        edges = [
            {
                "source": "pathInput__test_pi",
                "target": "fn__read_csv",
                "targetHandle": "in__filepath_or_buffer",
                "sourceHandle": "",
            }
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__read_csv"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result.path_input_params == {"filepath_or_buffer": "test_pi"}
        assert result.input_types == {}

    def test_placement_qualified_path_input_source_resolves(self):
        edges = [
            {
                "source": "pathInput__test_pi::main",
                "target": "fn__read_csv",
                "targetHandle": "in__filepath_or_buffer",
                "sourceHandle": "",
            }
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__read_csv"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result.path_input_params == {"filepath_or_buffer": "test_pi"}

    def test_handleless_path_input_edge_is_dropped(self):
        edges = [
            {
                "id": "e1",
                "source": "pathInput__test_pi",
                "target": "fn__read_csv",
                "targetHandle": "",
                "sourceHandle": "",
            }
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__read_csv"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result.path_input_params == {}

    def test_path_input_is_never_a_variable_input(self):
        # A PathInput resolves FILES, not a versioned record, so it must not
        # leak into input_types (which becomes for_each's variable classes).
        edges = [
            {
                "source": "pathInput__test_pi",
                "target": "fn__read_csv",
                "targetHandle": "in__filepath_or_buffer",
                "sourceHandle": "",
            }
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__read_csv"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result.input_types == {}
        assert result.parameter_params == {}


# ---------------------------------------------------------------------------
# resolve_function_edges — unrelated edges are ignored
# ---------------------------------------------------------------------------


class TestResolveFunctionEdgesUnrelated:
    def test_edges_between_other_nodes_are_ignored(self):
        edges = [
            {
                "source": "var__A",
                "target": "fn__other",
                "targetHandle": "in__x",
                "sourceHandle": "",
            },
            {
                "source": "fn__other",
                "target": "var__B",
                "targetHandle": "",
                "sourceHandle": "",
            },
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__my_func"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result == ResolvedEdges(bindings={}, output_types=[])

    def test_empty_edge_list(self):
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=[],
            manual_nodes={},
            existing_node_labels={},
        )
        assert result == ResolvedEdges(bindings={}, output_types=[])


# ---------------------------------------------------------------------------
# infer_manual_fn_output_types
# ---------------------------------------------------------------------------


class TestInferManualFnOutputTypes:
    def test_basic_output_inference(self):
        edges = [
            {
                "source": "fn__fn",
                "target": "var__Out",
                "targetHandle": "",
                "sourceHandle": "",
            },
        ]
        result = infer_manual_fn_output_types(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result == ["Out"]

    def test_non_variable_target_ignored(self):
        manual_nodes = {"uuid-fn": {"type": "functionNode", "label": "other"}}
        edges = [
            {
                "source": "fn__fn",
                "target": "uuid-fn",
                "targetHandle": "",
                "sourceHandle": "",
            },
        ]
        result = infer_manual_fn_output_types(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes=manual_nodes,
            existing_node_labels={},
        )
        assert result == []

    def test_duplicate_output_deduplicated(self):
        edges = [
            {
                "source": "fn__fn",
                "target": "var__Out",
                "targetHandle": "",
                "sourceHandle": "",
            },
            {
                "source": "fn__fn",
                "target": "var__Out",
                "targetHandle": "",
                "sourceHandle": "",
            },
        ]
        result = infer_manual_fn_output_types(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result == ["Out"]

    def test_order_preserved(self):
        edges = [
            {
                "source": "fn__fn",
                "target": "var__A",
                "targetHandle": "",
                "sourceHandle": "",
            },
            {
                "source": "fn__fn",
                "target": "var__B",
                "targetHandle": "",
                "sourceHandle": "",
            },
        ]
        result = infer_manual_fn_output_types(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result == ["A", "B"]

    def test_input_edges_not_counted(self):
        edges = [
            {
                "source": "var__In",
                "target": "fn__fn",
                "targetHandle": "in__x",
                "sourceHandle": "",
            },
        ]
        result = infer_manual_fn_output_types(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result == []

    def test_multiple_fn_node_ids(self):
        edges = [
            {
                "source": "fn__fn",
                "target": "var__A",
                "targetHandle": "",
                "sourceHandle": "",
            },
            {
                "source": "fn__fn__uuid1",
                "target": "var__B",
                "targetHandle": "",
                "sourceHandle": "",
            },
        ]
        result = infer_manual_fn_output_types(
            fn_node_ids={"fn__fn", "fn__fn__uuid1"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert set(result) == {"A", "B"}


class TestInferManualFnParamToClass:
    def test_extracts_param_to_class_from_outgoing_edge(self):
        # output1 (MATLAB param) → Result (Variable class): no naming convention.
        edges = [
            {
                "source": "fn__fn_ex",
                "target": "var__Result",
                "sourceHandle": "out__output1",
                "targetHandle": "",
            },
        ]
        result = infer_manual_fn_param_to_class(
            fn_node_ids={"fn__fn_ex"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={"var__Result": "Result"},
        )
        assert result == {"output1": "Result"}

    def test_ignores_input_edges(self):
        edges = [
            {
                "source": "var__In",
                "target": "fn__fn",
                "sourceHandle": "",
                "targetHandle": "in__x",
            },
        ]
        result = infer_manual_fn_param_to_class(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert result == {}

    def test_ignores_edges_without_out_prefix(self):
        edges = [
            {
                "source": "fn__fn",
                "target": "var__A",
                "sourceHandle": "",
                "targetHandle": "",
            },
        ]
        result = infer_manual_fn_param_to_class(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={"var__A": "A"},
        )
        assert result == {}

    def test_first_mapping_wins_on_duplicate_param(self):
        edges = [
            {
                "source": "fn__fn",
                "target": "var__First",
                "sourceHandle": "out__p",
                "targetHandle": "",
            },
            {
                "source": "fn__fn",
                "target": "var__Second",
                "sourceHandle": "out__p",
                "targetHandle": "",
            },
        ]
        result = infer_manual_fn_param_to_class(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={"var__First": "First", "var__Second": "Second"},
        )
        assert result == {"p": "First"}

    def test_manual_var_node_target_resolves(self):
        edges = [
            {
                "source": "fn__fn",
                "target": "uuid-var",
                "sourceHandle": "out__output_a",
                "targetHandle": "",
            },
        ]
        manual_nodes = {
            "uuid-var": {"type": "variableNode", "label": "CustomVar"},
        }
        result = infer_manual_fn_param_to_class(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes=manual_nodes,
            existing_node_labels={},
        )
        assert result == {"output_a": "CustomVar"}


# ---------------------------------------------------------------------------
# Handle-id agreement between the canvas and the resolver
# ---------------------------------------------------------------------------


class TestHandleIdsMatchTheFrontend:
    """The handle ids FunctionNode.tsx renders must be the ones this module
    recognises, or a hand-drawn edge silently binds nothing.

    This drifted once already: the node rendered `const__{name}` while
    PARAM_ID_PREFIX (and build_edges' synthesized targetHandle) was
    `param__{name}`. It went unnoticed because resolve_function_edges had a
    fallback that guessed the parameter from the source node's LABEL, which
    is right only when the declared name and the parameter name coincide.
    """

    def test_parameter_handle_prefix_is_the_backend_constant(self):
        from pathlib import Path

        from scistack_gui.ids import PARAM_ID_PREFIX

        node = Path(__file__).parent.parent / "frontend/src/components/DAG/FunctionNode.tsx"
        source = node.read_text()
        assert f"id: `{PARAM_ID_PREFIX}${{c}}`" in source, (
            "FunctionNode.tsx's parameter handle id must use PARAM_ID_PREFIX "
            f"({PARAM_ID_PREFIX!r}) — it is what build_edges writes as the "
            "targetHandle of a DB-derived Parameter edge, and what "
            "resolve_function_edges matches on"
        )

    def test_variable_input_handle_prefix_matches(self):
        from pathlib import Path

        node = Path(__file__).parent.parent / "frontend/src/components/DAG/FunctionNode.tsx"
        assert "id: `in__${param}`" in node.read_text()


# ---------------------------------------------------------------------------
# Placement-qualified endpoints
# ---------------------------------------------------------------------------


class TestPlacementQualifiedEndpoints:
    """Endpoint matching must ignore a ``::{scope}`` placement suffix.

    Callers assemble ``fn_node_ids`` from mixed sources: ``fn_node_id()``
    returns the bare ``fn__f__{call_id}``, while manual-node keys and edge
    endpoints may carry ``::{scope}``. Graduation rewrites a manual edge's
    endpoints from the first form to the second, so an exact-match comparison
    starts failing only AFTER a function's first successful run.

    Concretely, that emptied ``matlab_param_to_class`` for a MATLAB fn: the
    fn node then rendered handle ``out__{param}`` while build_edges pointed
    its edge at ``out__{Class}``, and React Flow silently drops an edge whose
    sourceHandle does not exist — the function appeared disconnected from an
    output variable that run_state still marked green.
    """

    FN_BARE = "fn__loadDelsysEMGOneFile__076c46199b238a69"
    FN_PLACED = "fn__loadDelsysEMGOneFile__076c46199b238a69::main"

    def _edge(self, source):
        return {
            "id": "manual__ivyg79",
            "source": source,
            "target": "var__RawEMG::main",
            "sourceHandle": "out__loaded_data",
        }

    def test_param_to_class_matches_placement_qualified_source(self):
        labels = {"var__RawEMG": "RawEMG"}
        expected = {"loaded_data": "RawEMG"}

        for source in (self.FN_BARE, self.FN_PLACED):
            mapping = infer_manual_fn_param_to_class(
                fn_node_ids={self.FN_BARE},
                manual_edges=[self._edge(source)],
                manual_nodes={},
                existing_node_labels=labels,
            )
            assert mapping == expected, (
                f"edge source {source!r} against bare fn id did not resolve"
            )

    def test_param_to_class_matches_when_the_id_set_is_placed(self):
        """The mismatch is symmetric — normalize both sides, not just one."""
        mapping = infer_manual_fn_param_to_class(
            fn_node_ids={self.FN_PLACED},
            manual_edges=[self._edge(self.FN_BARE)],
            manual_nodes={},
            existing_node_labels={"var__RawEMG": "RawEMG"},
        )
        assert mapping == {"loaded_data": "RawEMG"}

    def test_output_types_match_placement_qualified_source(self):
        for source in (self.FN_BARE, self.FN_PLACED):
            out = infer_manual_fn_output_types(
                fn_node_ids={self.FN_BARE},
                manual_edges=[self._edge(source)],
                manual_nodes={},
                existing_node_labels={"var__RawEMG": "RawEMG"},
            )
            assert out == ["RawEMG"], f"edge source {source!r} did not resolve"

    def test_resolve_function_edges_matches_placement_qualified_endpoints(self):
        resolved = resolve_function_edges(
            fn_node_ids={self.FN_BARE},
            manual_edges=[
                {
                    "id": "manual__689jp0",
                    "source": "pathInput__delsysPathTemplate::main",
                    "target": self.FN_PLACED,
                    "targetHandle": "in__emgFilePath",
                },
                self._edge(self.FN_PLACED),
            ],
            manual_nodes={},
            existing_node_labels={"var__RawEMG": "RawEMG"},
        )

        assert resolved.output_types == ["RawEMG"]
        assert resolved.path_input_params == {"emgFilePath": "delsysPathTemplate"}

    def test_unrelated_function_still_does_not_match(self):
        """Normalizing must not widen matching across different call sites."""
        mapping = infer_manual_fn_param_to_class(
            fn_node_ids={"fn__otherFn__aaaaaaaaaaaaaaaa"},
            manual_edges=[self._edge(self.FN_PLACED)],
            manual_nodes={},
            existing_node_labels={"var__RawEMG": "RawEMG"},
        )
        assert mapping == {}
