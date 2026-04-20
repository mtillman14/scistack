"""
Unit tests for scistack_gui.domain.graph_builder.

All functions are pure — no DB or fixtures required.
"""

import json

import pytest

from scistack_gui.domain.graph_builder import (
    AggregatedData,
    GraduationAction,
    aggregate_variants,
    auto_clean_pending_constants,
    build_constant_nodes,
    build_edges,
    build_function_nodes,
    build_manual_node,
    build_path_input_nodes,
    build_variable_nodes,
    filter_hidden,
    merge_manual_nodes,
    overlay_saved_path_inputs,
    parse_path_input,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _variant(fn, out, inputs=None, constants=None, count=1):
    return {
        "function_name": fn,
        "output_type": out,
        "input_types": inputs or {},
        "constants": constants or {},
        "record_count": count,
    }


# ---------------------------------------------------------------------------
# parse_path_input
# ---------------------------------------------------------------------------

class TestParsePathInput:
    def test_json_format(self):
        val = json.dumps({"__type": "PathInput", "template": "{subject}/raw.csv", "root_folder": "/data"})
        result = parse_path_input(val)
        assert result == {"template": "{subject}/raw.csv", "root_folder": "/data"}

    def test_json_format_no_root_folder(self):
        val = json.dumps({"__type": "PathInput", "template": "{subject}/raw.csv"})
        result = parse_path_input(val)
        assert result["template"] == "{subject}/raw.csv"
        assert result["root_folder"] is None

    def test_json_wrong_type_returns_none(self):
        val = json.dumps({"__type": "Other", "template": "x"})
        assert parse_path_input(val) is None

    def test_legacy_format(self):
        val = "PathInput('{subject}/raw.csv', root_folder=PosixPath('/data'))"
        result = parse_path_input(val)
        assert result["template"] == "{subject}/raw.csv"
        assert result["root_folder"] == "/data"

    def test_legacy_format_no_root(self):
        val = "PathInput('{subject}/raw.csv')"
        result = parse_path_input(val)
        assert result["template"] == "{subject}/raw.csv"
        assert result["root_folder"] is None

    def test_plain_string_returns_none(self):
        assert parse_path_input("RawEMG") is None

    def test_malformed_json_returns_none(self):
        assert parse_path_input("{not valid json}") is None


# ---------------------------------------------------------------------------
# aggregate_variants
# ---------------------------------------------------------------------------

class TestAggregateVariants:
    def test_basic_variant_parsed(self):
        variants = [_variant("bandpass", "Filtered", inputs={"signal": "Raw"}, constants={"hz": 20})]
        agg = aggregate_variants(variants, listed_var_names=set())
        assert "Filtered" in agg.all_var_types
        assert "Raw" in agg.all_var_types
        assert "bandpass" in agg.fn_outputs
        assert "Filtered" in agg.fn_outputs["bandpass"]
        assert agg.fn_input_params["bandpass"]["signal"] == "Raw"
        assert "hz" in agg.fn_constants["bandpass"]

    def test_const_counts_accumulated(self):
        variants = [
            _variant("f", "Out", constants={"hz": 10}, count=3),
            _variant("f", "Out", constants={"hz": 20}, count=5),
        ]
        agg = aggregate_variants(variants, listed_var_names=set())
        assert agg.const_counts["hz"]["10"] == 3
        assert agg.const_counts["hz"]["20"] == 5

    def test_path_input_parsed_and_not_added_to_var_types(self):
        pi_json = json.dumps({"__type": "PathInput", "template": "{s}/f.csv"})
        variants = [_variant("f", "Out", inputs={"path": pi_json})]
        agg = aggregate_variants(variants, listed_var_names=set())
        assert "path" in agg.path_inputs
        assert agg.path_inputs["path"]["template"] == "{s}/f.csv"
        assert "path" not in agg.all_var_types

    def test_path_input_function_set_accumulated(self):
        pi_json = json.dumps({"__type": "PathInput", "template": "{s}/f.csv"})
        variants = [
            _variant("f1", "Out1", inputs={"path": pi_json}),
            _variant("f2", "Out2", inputs={"path": pi_json}),
        ]
        agg = aggregate_variants(variants, listed_var_names=set())
        assert agg.path_inputs["path"]["functions"] == {"f1", "f2"}

    def test_listed_var_names_added(self):
        agg = aggregate_variants([], listed_var_names={"ExtraVar"})
        assert "ExtraVar" in agg.all_var_types

    def test_fn_variants_map_populated(self):
        variants = [_variant("f", "Out", constants={"k": 1})]
        agg = aggregate_variants(variants, listed_var_names=set())
        assert len(agg.fn_variants_map["f"]) == 1
        assert agg.fn_variants_map["f"][0]["constants"] == {"k": 1}

    def test_empty_variants(self):
        agg = aggregate_variants([], listed_var_names=set())
        assert agg.all_var_types == set()

    def test_pathinput_only_function_registered_in_fn_input_params(self):
        """A function with only PathInput inputs (no variable inputs) must
        still appear in fn_input_params so build_function_nodes and
        _compute_run_states don't skip it."""
        pi_json = json.dumps({"__type": "PathInput", "template": "{subject}/raw.csv"})
        variants = [_variant("loadFile", "Loaded", inputs={"filepath": pi_json})]
        agg = aggregate_variants(variants, listed_var_names=set())
        assert "loadFile" in agg.fn_input_params
        # No variable inputs, so the dict should be empty
        assert agg.fn_input_params["loadFile"] == {}
        # But the function should still be in fn_outputs
        assert "Loaded" in agg.fn_outputs["loadFile"]

    def test_pathinput_only_function_with_constants(self):
        """PathInput-only function with constants: still in fn_input_params."""
        pi_json = json.dumps({"__type": "PathInput", "template": "{subject}/raw.csv"})
        variants = [_variant("loadFile", "Loaded", inputs={"filepath": pi_json}, constants={"hz": 100})]
        agg = aggregate_variants(variants, listed_var_names=set())
        assert "loadFile" in agg.fn_input_params
        assert agg.fn_input_params["loadFile"] == {}
        assert "hz" in agg.fn_constants["loadFile"]

    def test_mixed_pathinput_and_variable_inputs(self):
        """Function with both PathInput and variable inputs: variable input in fn_input_params."""
        pi_json = json.dumps({"__type": "PathInput", "template": "{subject}/raw.csv"})
        variants = [_variant("process", "Out", inputs={"filepath": pi_json, "signal": "Raw"})]
        agg = aggregate_variants(variants, listed_var_names=set())
        assert "process" in agg.fn_input_params
        assert agg.fn_input_params["process"]["signal"] == "Raw"
        assert "filepath" not in agg.fn_input_params["process"]


# ---------------------------------------------------------------------------
# filter_hidden
# ---------------------------------------------------------------------------

class TestFilterHidden:
    def _agg(self):
        variants = [
            _variant("bandpass", "Filtered", inputs={"signal": "Raw"}, constants={"hz": 20}),
            _variant("normalize", "Normed", inputs={"signal": "Filtered"}),
        ]
        agg = aggregate_variants(variants, listed_var_names=set())
        pi_json = json.dumps({"__type": "PathInput", "template": "{s}/f.csv"})
        agg.path_inputs["mypath"] = {"template": "{s}/f.csv", "functions": {"bandpass"}}
        return agg

    def test_hide_var_removes_from_all_var_types(self):
        agg = self._agg()
        filter_hidden(agg, {"var__Raw"})
        assert "Raw" not in agg.all_var_types

    def test_hide_var_removes_from_fn_input_params(self):
        agg = self._agg()
        filter_hidden(agg, {"var__Raw"})
        assert "signal" not in agg.fn_input_params.get("bandpass", {})

    def test_hide_var_removes_from_fn_outputs(self):
        agg = self._agg()
        filter_hidden(agg, {"var__Filtered"})
        assert "Filtered" not in agg.fn_outputs.get("bandpass", set())

    def test_hide_fn_removes_params_and_outputs(self):
        agg = self._agg()
        filter_hidden(agg, {"fn__bandpass"})
        assert "bandpass" not in agg.fn_input_params
        assert "bandpass" not in agg.fn_outputs
        assert "bandpass" not in agg.fn_constants

    def test_hide_const_removes_from_const_counts(self):
        agg = self._agg()
        filter_hidden(agg, {"const__hz"})
        assert "hz" not in agg.const_counts
        assert "hz" not in agg.const_fns

    def test_hide_path_input(self):
        agg = self._agg()
        filter_hidden(agg, {"pathInput__mypath"})
        assert "mypath" not in agg.path_inputs

    def test_empty_hidden_ids_is_noop(self):
        agg = self._agg()
        before_vars = set(agg.all_var_types)
        filter_hidden(agg, set())
        assert agg.all_var_types == before_vars


# ---------------------------------------------------------------------------
# auto_clean_pending_constants
# ---------------------------------------------------------------------------

class TestAutoCleanPendingConstants:
    def test_removes_value_already_in_db(self):
        pending = {"hz": {"20", "30"}}
        const_counts = {"hz": {"20": 5}}
        cleaned, removals = auto_clean_pending_constants(pending, const_counts)
        assert "20" not in cleaned["hz"]
        assert "30" in cleaned["hz"]
        assert ("hz", "20") in removals

    def test_nothing_to_clean(self):
        pending = {"hz": {"99"}}
        const_counts = {"hz": {"20": 5}}
        cleaned, removals = auto_clean_pending_constants(pending, const_counts)
        assert cleaned["hz"] == {"99"}
        assert removals == []

    def test_empty_pending(self):
        cleaned, removals = auto_clean_pending_constants({}, {"hz": {"20": 5}})
        assert cleaned == {}
        assert removals == []


# ---------------------------------------------------------------------------
# build_variable_nodes
# ---------------------------------------------------------------------------

class TestBuildVariableNodes:
    def test_node_structure(self):
        nodes = build_variable_nodes({"RawEMG"}, record_counts={"RawEMG": 4}, run_states={})
        assert len(nodes) == 1
        n = nodes[0]
        assert n["id"] == "var__RawEMG"
        assert n["type"] == "variableNode"
        assert n["data"]["label"] == "RawEMG"
        assert n["data"]["total_records"] == 4

    def test_run_state_from_map(self):
        nodes = build_variable_nodes({"A"}, {}, run_states={"var__A": "grey"})
        assert nodes[0]["data"]["run_state"] == "grey"

    def test_default_run_state_green(self):
        nodes = build_variable_nodes({"A"}, {}, run_states={})
        assert nodes[0]["data"]["run_state"] == "green"

    def test_sorted_output(self):
        nodes = build_variable_nodes({"C", "A", "B"}, {}, {})
        labels = [n["data"]["label"] for n in nodes]
        assert labels == ["A", "B", "C"]

    def test_zero_records_when_missing(self):
        nodes = build_variable_nodes({"X"}, {}, {})
        assert nodes[0]["data"]["total_records"] == 0


# ---------------------------------------------------------------------------
# build_constant_nodes
# ---------------------------------------------------------------------------

class TestBuildConstantNodes:
    def test_node_structure(self):
        const_counts = {"hz": {"10": 3, "20": 5}}
        nodes = build_constant_nodes(const_counts, pending_constants={})
        assert len(nodes) == 1
        n = nodes[0]
        assert n["id"] == "const__hz"
        assert n["type"] == "constantNode"
        assert n["data"]["label"] == "hz"
        values = {v["value"] for v in n["data"]["values"]}
        assert values == {"10", "20"}

    def test_pending_value_appended(self):
        const_counts = {"hz": {"10": 3}}
        nodes = build_constant_nodes(const_counts, pending_constants={"hz": {"99"}})
        values = {v["value"] for v in nodes[0]["data"]["values"]}
        assert "99" in values

    def test_pending_not_duplicated_if_already_in_counts(self):
        const_counts = {"hz": {"10": 3}}
        nodes = build_constant_nodes(const_counts, pending_constants={"hz": {"10"}})
        values = [v["value"] for v in nodes[0]["data"]["values"]]
        assert values.count("10") == 1

    def test_pending_record_count_is_zero(self):
        const_counts = {"hz": {"10": 3}}
        nodes = build_constant_nodes(const_counts, pending_constants={"hz": {"99"}})
        pending_entry = next(v for v in nodes[0]["data"]["values"] if v["value"] == "99")
        assert pending_entry["record_count"] == 0


# ---------------------------------------------------------------------------
# overlay_saved_path_inputs
# ---------------------------------------------------------------------------

class TestOverlaySavedPathInputs:
    def test_updates_existing_entry(self):
        path_inputs = {"mypath": {"template": "", "root_folder": None, "functions": {"f"}}}
        saved = [{"name": "mypath", "template": "{s}/file.csv", "root_folder": "/data"}]
        result = overlay_saved_path_inputs(path_inputs, saved)
        assert result["mypath"]["template"] == "{s}/file.csv"
        assert result["mypath"]["root_folder"] == "/data"

    def test_adds_new_entry_from_saved(self):
        result = overlay_saved_path_inputs({}, [{"name": "newpath", "template": "{s}/x.csv"}])
        assert "newpath" in result
        assert result["newpath"]["functions"] == set()

    def test_does_not_overwrite_template_if_saved_empty(self):
        path_inputs = {"p": {"template": "existing", "root_folder": None, "functions": set()}}
        saved = [{"name": "p", "template": "", "root_folder": None}]
        result = overlay_saved_path_inputs(path_inputs, saved)
        # Empty template should not overwrite existing.
        assert result["p"]["template"] == "existing"


# ---------------------------------------------------------------------------
# build_path_input_nodes
# ---------------------------------------------------------------------------

class TestBuildPathInputNodes:
    def test_node_structure(self):
        path_inputs = {"mypath": {"template": "{s}/f.csv", "root_folder": "/data", "functions": set()}}
        nodes = build_path_input_nodes(path_inputs)
        assert len(nodes) == 1
        n = nodes[0]
        assert n["id"] == "pathInput__mypath"
        assert n["type"] == "pathInputNode"
        assert n["data"]["template"] == "{s}/f.csv"
        assert n["data"]["root_folder"] == "/data"


# ---------------------------------------------------------------------------
# build_function_nodes
# ---------------------------------------------------------------------------

class TestBuildFunctionNodes:
    def _make(self, **overrides):
        defaults = dict(
            fn_input_params={"bandpass": {"signal": "Raw"}},
            fn_outputs={"bandpass": {"Filtered"}},
            fn_constants={"bandpass": {"hz"}},
            fn_variants_map={"bandpass": []},
            fn_params_map={"bandpass": ["signal", "hz"]},
            run_states={"fn__bandpass": "green"},
            matlab_functions=set(),
            saved_configs={"bandpass": None},
        )
        defaults.update(overrides)
        return build_function_nodes(**defaults)

    def test_node_structure(self):
        nodes = self._make()
        assert len(nodes) == 1
        n = nodes[0]
        assert n["id"] == "fn__bandpass"
        assert n["type"] == "functionNode"
        assert n["data"]["label"] == "bandpass"

    def test_run_state_applied(self):
        nodes = self._make(run_states={"fn__bandpass": "grey"})
        assert nodes[0]["data"]["run_state"] == "grey"

    def test_matlab_language_flag(self):
        nodes = self._make(matlab_functions={"bandpass"})
        assert nodes[0]["data"]["language"] == "matlab"

    def test_non_matlab_has_no_language_flag(self):
        nodes = self._make()
        assert "language" not in nodes[0]["data"]

    def test_saved_config_applied(self):
        nodes = self._make(saved_configs={"bandpass": {"schemaFilter": {"subject": [1]}}})
        assert nodes[0]["data"]["schemaFilter"] == {"subject": [1]}

    def test_unknown_param_filled_with_empty_string(self):
        # "low_hz" is in sig params but not in input_params or constants.
        nodes = self._make(
            fn_input_params={"bandpass": {"signal": "Raw"}},
            fn_constants={},
            fn_params_map={"bandpass": ["signal", "low_hz"]},
        )
        assert nodes[0]["data"]["input_params"].get("low_hz") == ""

    def test_output_types_sorted(self):
        nodes = self._make(fn_outputs={"bandpass": {"C", "A", "B"}})
        assert nodes[0]["data"]["output_types"] == ["A", "B", "C"]


# ---------------------------------------------------------------------------
# build_edges
# ---------------------------------------------------------------------------

class TestBuildEdges:
    def test_var_to_fn_edge(self):
        edges = build_edges(
            fn_input_params={"f": {"signal": "Raw"}},
            fn_outputs={"f": set()},
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
        )
        assert any(e["source"] == "var__Raw" and e["target"] == "fn__f" for e in edges)

    def test_fn_to_var_edge(self):
        edges = build_edges(
            fn_input_params={},
            fn_outputs={"f": {"Out"}},
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
        )
        assert any(e["source"] == "fn__f" and e["target"] == "var__Out" for e in edges)

    def test_const_to_fn_edge(self):
        edges = build_edges(
            fn_input_params={},
            fn_outputs={},
            const_fns={"hz": {"f"}},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
        )
        assert any(e["source"] == "const__hz" and e["target"] == "fn__f" for e in edges)

    def test_path_input_to_fn_edge(self):
        edges = build_edges(
            fn_input_params={},
            fn_outputs={},
            const_fns={},
            path_inputs={"mypath": {"template": "", "root_folder": None, "functions": {"f"}}},
            manual_edges=[],
            hidden_ids=set(),
        )
        assert any(e["source"] == "pathInput__mypath" and e["target"] == "fn__f" for e in edges)

    def test_manual_edge_included(self):
        me = {"id": "manual-1", "source": "uuid-var", "target": "fn__f",
              "sourceHandle": "", "targetHandle": "in__x"}
        edges = build_edges({}, {}, {}, {}, [me], set())
        assert any(e["id"] == "manual-1" for e in edges)

    def test_manual_edge_skipped_if_hidden(self):
        me = {"id": "manual-1", "source": "uuid-var", "target": "fn__f",
              "sourceHandle": "", "targetHandle": ""}
        edges = build_edges({}, {}, {}, {}, [me], hidden_ids={"uuid-var"})
        assert not any(e["id"] == "manual-1" for e in edges)

    def test_no_duplicate_edges(self):
        # Same var→fn from two params should only produce one edge.
        edges = build_edges(
            fn_input_params={"f": {"a": "Raw", "b": "Raw"}},
            fn_outputs={},
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
        )
        var_to_fn = [e for e in edges if e["source"] == "var__Raw" and e["target"] == "fn__f"]
        assert len(var_to_fn) == 1

    def test_manual_edge_not_duplicated_if_already_in_db_edges(self):
        # A manual edge whose id matches a DB-derived edge id should be skipped.
        edges = build_edges(
            fn_input_params={"f": {"signal": "Raw"}},
            fn_outputs={},
            const_fns={},
            path_inputs={},
            manual_edges=[{"id": "e__Raw__f", "source": "var__Raw", "target": "fn__f",
                           "sourceHandle": "", "targetHandle": "in__signal"}],
            hidden_ids=set(),
        )
        matching = [e for e in edges if e["id"] == "e__Raw__f"]
        assert len(matching) == 1


# ---------------------------------------------------------------------------
# build_manual_node
# ---------------------------------------------------------------------------

class TestBuildManualNode:
    def test_variable_node(self):
        n = build_manual_node(
            "uuid-1", {"type": "variableNode", "label": "MyVar", "config": None},
            pending_constants={}, manual_fn_state=None,
            resolved_input_params=None, resolved_output_types=None,
            matlab_functions=set(),
        )
        assert n["id"] == "uuid-1"
        assert n["type"] == "variableNode"
        assert n["data"]["run_state"] == "red"
        assert n["data"]["total_records"] == 0

    def test_constant_node_with_pending(self):
        n = build_manual_node(
            "uuid-2", {"type": "constantNode", "label": "hz", "config": None},
            pending_constants={"hz": {"42"}},
            manual_fn_state=None, resolved_input_params=None,
            resolved_output_types=None, matlab_functions=set(),
        )
        assert n["type"] == "constantNode"
        vals = {v["value"] for v in n["data"]["values"]}
        assert "42" in vals

    def test_function_node(self):
        n = build_manual_node(
            "uuid-3", {"type": "functionNode", "label": "my_fn", "config": None},
            pending_constants={}, manual_fn_state="grey",
            resolved_input_params={"signal": "Raw"},
            resolved_output_types=["Filtered"],
            matlab_functions=set(),
        )
        assert n["type"] == "functionNode"
        assert n["data"]["run_state"] == "grey"
        assert n["data"]["input_params"] == {"signal": "Raw"}
        assert n["data"]["output_types"] == ["Filtered"]

    def test_function_node_matlab_language(self):
        n = build_manual_node(
            "uuid-4", {"type": "functionNode", "label": "my_fn", "config": None},
            pending_constants={}, manual_fn_state="red",
            resolved_input_params={}, resolved_output_types=[],
            matlab_functions={"my_fn"},
        )
        assert n["data"]["language"] == "matlab"

    def test_path_input_node(self):
        n = build_manual_node(
            "uuid-5", {"type": "pathInputNode", "label": "mypath", "config": None},
            pending_constants={}, manual_fn_state=None,
            resolved_input_params=None, resolved_output_types=None,
            matlab_functions=set(),
        )
        assert n["type"] == "pathInputNode"
        assert n["data"]["template"] == ""

    def test_function_node_default_state_red(self):
        n = build_manual_node(
            "uuid-6", {"type": "functionNode", "label": "fn", "config": None},
            pending_constants={}, manual_fn_state=None,
            resolved_input_params={}, resolved_output_types=[],
            matlab_functions=set(),
        )
        assert n["data"]["run_state"] == "red"


# ---------------------------------------------------------------------------
# merge_manual_nodes
# ---------------------------------------------------------------------------

class TestMergeManualNodes:
    def _db_node(self, node_id, ntype, label):
        return {"id": node_id, "type": ntype, "data": {"label": label}}

    def test_manual_node_not_in_db_added(self):
        existing = [self._db_node("var__Raw", "variableNode", "Raw")]
        manual = {"uuid-new": {"type": "variableNode", "label": "NewVar"}}
        to_add, _ = merge_manual_nodes(existing, manual, saved_positions={})
        assert "uuid-new" in to_add

    def test_manual_node_already_in_db_skipped(self):
        existing = [self._db_node("var__Raw", "variableNode", "Raw")]
        manual = {"var__Raw": {"type": "variableNode", "label": "Raw"}}
        to_add, _ = merge_manual_nodes(existing, manual, saved_positions={})
        assert "var__Raw" not in to_add

    def test_graduated_node_produces_graduation_action(self):
        existing = [self._db_node("var__Raw", "variableNode", "Raw")]
        manual = {"uuid-old": {"type": "variableNode", "label": "Raw"}}
        to_add, graduations = merge_manual_nodes(existing, manual, saved_positions={})
        assert "uuid-old" not in to_add
        assert len(graduations) == 1
        assert graduations[0].old_id == "uuid-old"
        assert graduations[0].new_id == "var__Raw"

    def test_graduation_skipped_if_canonical_has_saved_position(self):
        # If the canonical node already has a saved position, do not graduate.
        existing = [self._db_node("var__Raw", "variableNode", "Raw")]
        manual = {"uuid-old": {"type": "variableNode", "label": "Raw"}}
        to_add, graduations = merge_manual_nodes(
            existing, manual, saved_positions={"var__Raw": {"x": 10, "y": 20}}
        )
        assert "uuid-old" in to_add
        assert len(graduations) == 0

    def test_empty_manual_nodes(self):
        existing = [self._db_node("var__Raw", "variableNode", "Raw")]
        to_add, graduations = merge_manual_nodes(existing, {}, saved_positions={})
        assert to_add == []
        assert graduations == []


# ---------------------------------------------------------------------------
# MATLAB param-name handles (Fix B)
# ---------------------------------------------------------------------------

class TestMatlabParamNameHandles:
    """Exercises the path where MATLAB param names differ from Variable class
    names (e.g. ``output1 → Result``), to prove the graph_builder uses the
    explicit mapping rather than any naming convention."""

    def _make_nodes(self, **overrides):
        defaults = dict(
            fn_input_params={"fn_ex": {}},
            fn_outputs={"fn_ex": {"Result"}},
            fn_constants={"fn_ex": set()},
            fn_variants_map={"fn_ex": []},
            fn_params_map={"fn_ex": []},
            run_states={},
            matlab_functions={"fn_ex"},
            saved_configs={"fn_ex": None},
            matlab_output_order={"fn_ex": ["output1"]},
            matlab_param_to_class={"fn_ex": {"output1": "Result"}},
        )
        defaults.update(overrides)
        return build_function_nodes(**defaults)

    def test_handles_use_param_name_not_class_name(self):
        nodes = self._make_nodes()
        assert nodes[0]["data"]["output_types"] == ["output1"]

    def test_edges_use_param_name_in_source_handle(self):
        edges = build_edges(
            fn_input_params={},
            fn_outputs={"fn_ex": {"Result"}},
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
            matlab_param_to_class={"fn_ex": {"output1": "Result"}},
        )
        fn_to_var = [e for e in edges if e.get("source") == "fn__fn_ex"]
        assert len(fn_to_var) == 1
        assert fn_to_var[0]["target"] == "var__Result"
        assert fn_to_var[0]["sourceHandle"] == "out__output1"

    def test_multi_output_preserves_signature_order(self):
        # load_csv-style: signature order time, force_left, force_right;
        # class names are Pascal-case and alphabetically would re-order.
        nodes = build_function_nodes(
            fn_input_params={"load_csv": {}},
            fn_outputs={"load_csv": {"Time", "Force_Left", "Force_Right"}},
            fn_constants={"load_csv": set()},
            fn_variants_map={"load_csv": []},
            fn_params_map={"load_csv": []},
            run_states={},
            matlab_functions={"load_csv"},
            saved_configs={"load_csv": None},
            matlab_output_order={"load_csv": ["time", "force_left", "force_right"]},
            matlab_param_to_class={"load_csv": {
                "time": "Time",
                "force_left": "Force_Left",
                "force_right": "Force_Right",
            }},
        )
        assert nodes[0]["data"]["output_types"] == ["time", "force_left", "force_right"]

    def test_non_matlab_fn_unaffected(self):
        # Python fn with matlab_param_to_class empty keeps class-name handles.
        edges = build_edges(
            fn_input_params={},
            fn_outputs={"py_fn": {"Out"}},
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
            matlab_param_to_class={},
        )
        fn_to_var = [e for e in edges if e.get("source") == "fn__py_fn"]
        assert fn_to_var[0]["sourceHandle"] == "out__Out"

    def test_unmapped_class_falls_back_to_class_handle(self):
        # MATLAB fn where a DB variant's class has no param mapping — shouldn't
        # crash, just fall back to the class-name handle.
        edges = build_edges(
            fn_input_params={},
            fn_outputs={"fn_ex": {"Unmapped"}},
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
            matlab_param_to_class={"fn_ex": {}},
        )
        fn_to_var = [e for e in edges if e.get("source") == "fn__fn_ex"]
        assert fn_to_var[0]["sourceHandle"] == "out__Unmapped"
