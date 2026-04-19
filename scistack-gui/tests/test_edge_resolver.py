"""
Unit tests for scistack_gui.domain.edge_resolver.

All functions in this module are pure (no I/O), so no fixtures are needed.
"""

import pytest

from scistack_gui.domain.edge_resolver import (
    ResolvedEdges,
    infer_manual_fn_output_types,
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
        edges = [{"source": "fn__my_func", "target": "var__RawEMG", "targetHandle": "", "sourceHandle": ""}]
        result = resolve_function_edges(
            fn_node_ids={"fn__my_func"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={"var__RawEMG": "RawEMG"},
            sig_params=[],
        )
        assert result.output_types == ["RawEMG"]

    def test_output_from_manual_variable_node(self):
        manual_nodes = {"uuid-out": {"type": "variableNode", "label": "ProcessedSignal"}}
        edges = [{"source": "fn__my_func", "target": "uuid-out", "targetHandle": "", "sourceHandle": ""}]
        result = resolve_function_edges(
            fn_node_ids={"fn__my_func"},
            manual_edges=edges,
            manual_nodes=manual_nodes,
            existing_node_labels={},
            sig_params=[],
        )
        assert result.output_types == ["ProcessedSignal"]

    def test_duplicate_output_is_deduplicated(self):
        edges = [
            {"source": "fn__my_func", "target": "var__RawEMG", "targetHandle": "", "sourceHandle": ""},
            {"source": "fn__my_func", "target": "var__RawEMG", "targetHandle": "", "sourceHandle": ""},
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__my_func"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={"var__RawEMG": "RawEMG"},
            sig_params=[],
        )
        assert result.output_types == ["RawEMG"]

    def test_output_order_is_preserved(self):
        edges = [
            {"source": "fn__my_func", "target": "var__A", "targetHandle": "", "sourceHandle": ""},
            {"source": "fn__my_func", "target": "var__B", "targetHandle": "", "sourceHandle": ""},
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__my_func"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
            sig_params=[],
        )
        assert result.output_types == ["A", "B"]

    def test_non_variable_output_target_is_ignored(self):
        manual_nodes = {"uuid-fn": {"type": "functionNode", "label": "other_fn"}}
        edges = [{"source": "fn__my_func", "target": "uuid-fn", "targetHandle": "", "sourceHandle": ""}]
        result = resolve_function_edges(
            fn_node_ids={"fn__my_func"},
            manual_edges=edges,
            manual_nodes=manual_nodes,
            existing_node_labels={},
            sig_params=[],
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
            sig_params=["signal"],
        )
        assert result.input_types == {"signal": ["RawEMG"]}

    def test_multiple_inputs_same_param(self):
        edges = [
            {"source": "var__A", "target": "fn__fn", "targetHandle": "in__signal", "sourceHandle": ""},
            {"source": "var__B", "target": "fn__fn", "targetHandle": "in__signal", "sourceHandle": ""},
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
            sig_params=["signal"],
        )
        assert set(result.input_types["signal"]) == {"A", "B"}

    def test_duplicate_var_not_added_twice_to_same_param(self):
        edges = [
            {"source": "var__A", "target": "fn__fn", "targetHandle": "in__signal", "sourceHandle": ""},
            {"source": "var__A", "target": "fn__fn", "targetHandle": "in__signal", "sourceHandle": ""},
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
            sig_params=["signal"],
        )
        assert result.input_types["signal"] == ["A"]

    def test_positional_input_fallback_via_sig_params(self):
        # No targetHandle — match to first unresolved sig param.
        edges = [
            {"source": "var__RawEMG", "target": "fn__fn", "targetHandle": "", "sourceHandle": ""},
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={"var__RawEMG": "RawEMG"},
            sig_params=["signal", "low_hz"],
        )
        assert result.input_types == {"signal": ["RawEMG"]}

    def test_positional_fallback_skips_already_named_params(self):
        # "low_hz" is already named via handle; positional goes to "signal".
        edges = [
            {"source": "var__Hz", "target": "fn__fn", "targetHandle": "in__low_hz", "sourceHandle": ""},
            {"source": "var__RawEMG", "target": "fn__fn", "targetHandle": "", "sourceHandle": ""},
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
            sig_params=["signal", "low_hz"],
        )
        assert "signal" in result.input_types
        assert result.input_types["signal"] == ["RawEMG"]
        assert result.input_types["low_hz"] == ["Hz"]

    def test_fn_node_ids_set_matches_variant_ids(self):
        # Both "fn__fn" and a UUID variant ID should be recognised.
        edges = [
            {"source": "var__A", "target": "fn__fn__uuid1", "targetHandle": "in__x", "sourceHandle": ""},
            {"source": "fn__fn__uuid1", "target": "var__B", "targetHandle": "", "sourceHandle": ""},
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn", "fn__fn__uuid1"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
            sig_params=["x"],
        )
        assert result.input_types == {"x": ["A"]}
        assert result.output_types == ["B"]


# ---------------------------------------------------------------------------
# resolve_function_edges — constant wiring
# ---------------------------------------------------------------------------

class TestResolveFunctionEdgesConstants:
    def test_db_constant_node_by_prefix(self):
        edges = [
            {
                "source": "const__low_hz",
                "target": "fn__fn",
                "targetHandle": "const__low_hz",
                "sourceHandle": "",
            }
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
            sig_params=["signal", "low_hz"],
        )
        assert "low_hz" in result.constant_names
        assert result.input_types == {}

    def test_constant_via_in_handle(self):
        edges = [
            {
                "source": "const__low_hz",
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
            sig_params=["signal", "low_hz"],
        )
        assert "low_hz" in result.constant_names

    def test_manual_constant_node(self):
        manual_nodes = {
            "uuid-const": {"type": "constantNode", "label": "threshold"},
        }
        edges = [
            {
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
            sig_params=["signal", "threshold"],
        )
        assert "threshold" in result.constant_names

    def test_constant_not_added_to_input_types(self):
        edges = [
            {
                "source": "const__low_hz",
                "target": "fn__fn",
                "targetHandle": "const__low_hz",
                "sourceHandle": "",
            }
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
            sig_params=["signal", "low_hz"],
        )
        assert "low_hz" not in result.input_types


# ---------------------------------------------------------------------------
# resolve_function_edges — unrelated edges are ignored
# ---------------------------------------------------------------------------

class TestResolveFunctionEdgesUnrelated:
    def test_edges_between_other_nodes_are_ignored(self):
        edges = [
            {"source": "var__A", "target": "fn__other", "targetHandle": "in__x", "sourceHandle": ""},
            {"source": "fn__other", "target": "var__B", "targetHandle": "", "sourceHandle": ""},
        ]
        result = resolve_function_edges(
            fn_node_ids={"fn__my_func"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
            sig_params=["x"],
        )
        assert result == ResolvedEdges(input_types={}, output_types=[], constant_names=set())

    def test_empty_edge_list(self):
        result = resolve_function_edges(
            fn_node_ids={"fn__fn"},
            manual_edges=[],
            manual_nodes={},
            existing_node_labels={},
            sig_params=["a", "b"],
        )
        assert result == ResolvedEdges(input_types={}, output_types=[], constant_names=set())


# ---------------------------------------------------------------------------
# infer_manual_fn_output_types
# ---------------------------------------------------------------------------

class TestInferManualFnOutputTypes:
    def test_basic_output_inference(self):
        edges = [
            {"source": "fn__fn", "target": "var__Out", "targetHandle": "", "sourceHandle": ""},
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
            {"source": "fn__fn", "target": "uuid-fn", "targetHandle": "", "sourceHandle": ""},
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
            {"source": "fn__fn", "target": "var__Out", "targetHandle": "", "sourceHandle": ""},
            {"source": "fn__fn", "target": "var__Out", "targetHandle": "", "sourceHandle": ""},
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
            {"source": "fn__fn", "target": "var__A", "targetHandle": "", "sourceHandle": ""},
            {"source": "fn__fn", "target": "var__B", "targetHandle": "", "sourceHandle": ""},
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
            {"source": "var__In", "target": "fn__fn", "targetHandle": "in__x", "sourceHandle": ""},
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
            {"source": "fn__fn", "target": "var__A", "targetHandle": "", "sourceHandle": ""},
            {"source": "fn__fn__uuid1", "target": "var__B", "targetHandle": "", "sourceHandle": ""},
        ]
        result = infer_manual_fn_output_types(
            fn_node_ids={"fn__fn", "fn__fn__uuid1"},
            manual_edges=edges,
            manual_nodes={},
            existing_node_labels={},
        )
        assert set(result) == {"A", "B"}
