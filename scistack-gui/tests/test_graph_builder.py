"""
Unit tests for scistack_gui.domain.graph_builder.

All functions are pure — no DB or fixtures required.
"""

import json
from scidb import Parameter

from scifor import PathInput

from scistack_gui.domain.graph_builder import (
    AggregatedData,
    aggregate_variants,
    auto_clean_pending_constants,
    build_parameter_nodes,
    build_edges,
    build_function_nodes,
    build_manual_node,
    build_path_input_nodes,
    build_variable_nodes,
    candidate_edge_id,
    drop_superseded_manual_edges,
    edge_dedup_key,
    filter_hidden,
    find_cycle,
    fn_node_id,
    hidden_wirings,
    inbound_edge_candidates,
    is_manual_edge,
    merge_manual_nodes,
    parse_path_input,
    path_input_display,
    pending_value_group_coverage,
    resolve_path_input_name,
    seed_undiscovered_path_inputs,
    strip_path_input_params,
    wiring_disconnected_fkeys,
    wiring_id,
    wirings_downstream_of,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cid(seed: str = "default") -> str:
    """Synthesize a 16-hex call_id for tests.  Stable per seed string."""
    import hashlib

    return hashlib.sha256(seed.encode()).hexdigest()[:16]


def _variant(fn, out, inputs=None, constants=None, count=1, call_id=None):
    return {
        "function_name": fn,
        "output_type": out,
        "call_id": call_id
        or _cid(
            f"{fn}:{json.dumps(constants or {}, sort_keys=True)}:{json.dumps(inputs or {}, sort_keys=True)}"
        ),
        "input_types": inputs or {},
        "constants": constants or {},
        "record_count": count,
    }


def _fkey(fn, *, inputs=None, constants=None) -> tuple[str, str]:
    """Build the FnKey that ``_variant(fn, ..., inputs, constants, ...)`` produces."""
    return (
        fn,
        _cid(
            f"{fn}:{json.dumps(constants or {}, sort_keys=True)}:{json.dumps(inputs or {}, sort_keys=True)}"
        ),
    )


# ---------------------------------------------------------------------------
# parse_path_input
# ---------------------------------------------------------------------------


class TestParsePathInput:
    def test_json_format(self):
        val = json.dumps(
            {
                "__type": "PathInput",
                "template": "{subject}/raw.csv",
                "root_folder": "/data",
            }
        )
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
        variants = [
            _variant(
                "bandpass", "Filtered", inputs={"signal": "Raw"}, constants={"hz": 20}
            )
        ]
        agg = aggregate_variants(variants, listed_var_names=set())
        fkey = _fkey("bandpass", inputs={"signal": "Raw"}, constants={"hz": 20})
        assert "Filtered" in agg.all_var_types
        assert "Raw" in agg.all_var_types
        assert fkey in agg.fn_outputs
        assert "Filtered" in agg.fn_outputs[fkey]
        assert agg.fn_input_params[fkey]["signal"] == "Raw"
        assert "hz" in agg.fn_constants[fkey]

    def test_const_counts_accumulated(self):
        variants = [
            _variant("f", "Out", constants={"hz": 10}, count=3),
            _variant("f", "Out", constants={"hz": 20}, count=5),
        ]
        agg = aggregate_variants(variants, listed_var_names=set())
        # Two distinct call sites — one per constant value.
        assert agg.const_counts["hz"]["10"] == 3
        assert agg.const_counts["hz"]["20"] == 5
        assert len(agg.fn_input_params) == 2

    def test_path_input_parsed_and_not_added_to_var_types(self):
        pi_json = json.dumps({"__type": "PathInput", "template": "{s}/f.csv"})
        variants = [_variant("f", "Out", inputs={"path": pi_json})]
        registry = {"path": PathInput("{s}/f.csv")}
        agg = aggregate_variants(
            variants, listed_var_names=set(), path_input_registry=registry
        )
        assert "path" in agg.path_inputs
        assert agg.path_inputs["path"]["template"] == "{s}/f.csv"
        assert "path" not in agg.all_var_types

    def test_path_input_unresolved_without_registry_match(self):
        """No registry (or no matching entry) -> falls back to a synthetic
        __unresolved__ key rather than the param name — see
        resolve_path_input_name."""
        pi_json = json.dumps({"__type": "PathInput", "template": "{s}/f.csv"})
        variants = [_variant("f", "Out", inputs={"path": pi_json})]
        agg = aggregate_variants(variants, listed_var_names=set())
        assert "path" not in agg.path_inputs
        assert "__unresolved__:{s}/f.csv" in agg.path_inputs

    def test_path_input_function_set_accumulated(self):
        pi_json = json.dumps({"__type": "PathInput", "template": "{s}/f.csv"})
        variants = [
            _variant("f1", "Out1", inputs={"path": pi_json}),
            _variant("f2", "Out2", inputs={"path": pi_json}),
        ]
        registry = {"path": PathInput("{s}/f.csv")}
        agg = aggregate_variants(
            variants, listed_var_names=set(), path_input_registry=registry
        )
        # path_inputs[name]["functions"] now holds (FnKey, param_name) tuples
        f1_key = _fkey("f1", inputs={"path": pi_json})
        f2_key = _fkey("f2", inputs={"path": pi_json})
        assert agg.path_inputs["path"]["functions"] == {
            (f1_key, "path"),
            (f2_key, "path"),
        }

    def test_listed_var_names_added(self):
        agg = aggregate_variants([], listed_var_names={"ExtraVar"})
        assert "ExtraVar" in agg.all_var_types

    def test_fn_variants_map_populated(self):
        variants = [_variant("f", "Out", constants={"k": 1})]
        agg = aggregate_variants(variants, listed_var_names=set())
        fkey = _fkey("f", constants={"k": 1})
        assert len(agg.fn_variants_map[fkey]) == 1
        assert agg.fn_variants_map[fkey][0]["constants"] == {"k": 1}

    def test_empty_variants(self):
        agg = aggregate_variants([], listed_var_names=set())
        assert agg.all_var_types == set()

    def test_pathinput_only_function_registered_in_fn_input_params(self):
        """A PathInput-only function still gets a FnKey entry."""
        pi_json = json.dumps({"__type": "PathInput", "template": "{subject}/raw.csv"})
        variants = [_variant("loadFile", "Loaded", inputs={"filepath": pi_json})]
        agg = aggregate_variants(variants, listed_var_names=set())
        fkey = _fkey("loadFile", inputs={"filepath": pi_json})
        assert fkey in agg.fn_input_params
        assert agg.fn_input_params[fkey] == {}
        assert "Loaded" in agg.fn_outputs[fkey]

    def test_pathinput_only_function_with_constants(self):
        pi_json = json.dumps({"__type": "PathInput", "template": "{subject}/raw.csv"})
        variants = [
            _variant(
                "loadFile",
                "Loaded",
                inputs={"filepath": pi_json},
                constants={"hz": 100},
            )
        ]
        agg = aggregate_variants(variants, listed_var_names=set())
        fkey = _fkey("loadFile", inputs={"filepath": pi_json}, constants={"hz": 100})
        assert fkey in agg.fn_input_params
        assert agg.fn_input_params[fkey] == {}
        assert "hz" in agg.fn_constants[fkey]

    def test_mixed_pathinput_and_variable_inputs(self):
        pi_json = json.dumps({"__type": "PathInput", "template": "{subject}/raw.csv"})
        variants = [
            _variant("process", "Out", inputs={"filepath": pi_json, "signal": "Raw"})
        ]
        agg = aggregate_variants(variants, listed_var_names=set())
        fkey = _fkey("process", inputs={"filepath": pi_json, "signal": "Raw"})
        assert fkey in agg.fn_input_params
        assert agg.fn_input_params[fkey]["signal"] == "Raw"
        assert "filepath" not in agg.fn_input_params[fkey]

    def test_call_id_threaded_through_to_fn_keys(self):
        """Two call sites of the same fn produce two distinct FnKeys."""
        variants = [
            _variant("bp", "Out", constants={"hz": 10}),
            _variant("bp", "Out", constants={"hz": 50}),
        ]
        agg = aggregate_variants(variants, listed_var_names=set())
        keys = [k for k in agg.fn_input_params if k[0] == "bp"]
        assert len(keys) == 2
        # Distinct call_ids
        assert keys[0][1] != keys[1][1]

    def test_variant_missing_call_id_skipped(self):
        """Defensive: a variant lacking call_id is dropped (with a warning)."""
        v = _variant("f", "Out")
        v.pop("call_id")
        agg = aggregate_variants([v], listed_var_names=set())
        assert agg.fn_input_params == {}


# ---------------------------------------------------------------------------
# filter_hidden
# ---------------------------------------------------------------------------


class TestFilterHidden:
    def _agg(self):
        variants = [
            _variant(
                "bandpass", "Filtered", inputs={"signal": "Raw"}, constants={"hz": 20}
            ),
            _variant("normalize", "Normed", inputs={"signal": "Filtered"}),
        ]
        agg = aggregate_variants(variants, listed_var_names=set())
        bp_key = _fkey("bandpass", inputs={"signal": "Raw"}, constants={"hz": 20})
        agg.path_inputs["mypath"] = {
            "template": "{s}/f.csv",
            "functions": {(bp_key, "signal")},
        }
        return agg

    def test_hide_var_removes_from_all_var_types(self):
        agg = self._agg()
        filter_hidden(agg, {"var__Raw"})
        assert "Raw" not in agg.all_var_types

    def test_hide_var_removes_from_fn_input_params(self):
        agg = self._agg()
        bp_key = _fkey("bandpass", inputs={"signal": "Raw"}, constants={"hz": 20})
        filter_hidden(agg, {"var__Raw"})
        assert "signal" not in agg.fn_input_params.get(bp_key, {})

    def test_hide_var_removes_from_fn_outputs(self):
        agg = self._agg()
        bp_key = _fkey("bandpass", inputs={"signal": "Raw"}, constants={"hz": 20})
        filter_hidden(agg, {"var__Filtered"})
        assert "Filtered" not in agg.fn_outputs.get(bp_key, set())

    def test_hide_fn_removes_params_and_outputs(self):
        agg = self._agg()
        bp_key = _fkey("bandpass", inputs={"signal": "Raw"}, constants={"hz": 20})
        filter_hidden(agg, {fn_node_id(*bp_key)})
        assert bp_key not in agg.fn_input_params
        assert bp_key not in agg.fn_outputs
        assert bp_key not in agg.fn_constants

    def test_hide_legacy_fn_id_is_ignored(self):
        """Legacy ``fn__{name}`` IDs (no call_id) don't match composite FnKeys
        and are silently ignored — there is no single canonical node to hide."""
        agg = self._agg()
        bp_key = _fkey("bandpass", inputs={"signal": "Raw"}, constants={"hz": 20})
        filter_hidden(agg, {"fn__bandpass"})
        assert bp_key in agg.fn_input_params

    def test_hide_const_removes_from_const_counts(self):
        agg = self._agg()
        filter_hidden(agg, {"param__hz"})
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

    def test_strip_var_type_values_false_preserves_fn_outputs(self):
        """strip_var_type_values=False: hiding an output var must NOT scrub
        it out of fn_outputs — required so wiring_id (computed from
        fn_outputs downstream) stays stable when the caller hides one of a
        function's own output variables."""
        agg = self._agg()
        bp_key = _fkey("bandpass", inputs={"signal": "Raw"}, constants={"hz": 20})
        filter_hidden(agg, {"var__Filtered"}, strip_var_type_values=False)
        assert "Filtered" in agg.fn_outputs.get(bp_key, set())

    def test_strip_var_type_values_false_preserves_fn_input_params(self):
        agg = self._agg()
        bp_key = _fkey("bandpass", inputs={"signal": "Raw"}, constants={"hz": 20})
        filter_hidden(agg, {"var__Raw"}, strip_var_type_values=False)
        assert agg.fn_input_params.get(bp_key, {}).get("signal") == "Raw"

    def test_strip_var_type_values_false_still_removes_all_var_types(self):
        """all_var_types is display-only (never feeds wiring_id) — safe to
        strip regardless of strip_var_type_values."""
        agg = self._agg()
        filter_hidden(agg, {"var__Raw"}, strip_var_type_values=False)
        assert "Raw" not in agg.all_var_types

    def test_strip_var_type_values_false_still_removes_hidden_fn(self):
        """Explicitly-hidden function call sites must still drop out
        regardless of strip_var_type_values — only the VALUE-level var-type
        scrubbing on surviving call sites is gated."""
        agg = self._agg()
        bp_key = _fkey("bandpass", inputs={"signal": "Raw"}, constants={"hz": 20})
        filter_hidden(agg, {fn_node_id(*bp_key)}, strip_var_type_values=False)
        assert bp_key not in agg.fn_input_params
        assert bp_key not in agg.fn_outputs

    def test_hiding_output_var_does_not_change_wiring_id(self):
        """Regression: hiding a function's own output variable node must not
        change the function's wiring_id, or the canvas node loses its saved
        scope placement and vanishes from non-root scopes (the bug this
        param exists to fix)."""
        agg = self._agg()
        bp_key = _fkey("bandpass", inputs={"signal": "Raw"}, constants={"hz": 20})
        before_wid = wiring_id(
            "bandpass",
            agg.fn_input_params[bp_key],
            agg.fn_outputs[bp_key],
            {},
        )
        filter_hidden(agg, {"var__Filtered"}, strip_var_type_values=False)
        after_wid = wiring_id(
            "bandpass",
            agg.fn_input_params[bp_key],
            agg.fn_outputs[bp_key],
            {},
        )
        assert before_wid == after_wid


# ---------------------------------------------------------------------------
# auto_clean_pending_constants
# ---------------------------------------------------------------------------


class TestAutoCleanPendingConstants:
    def _one_wiring_agg(self, hz_value=20):
        variants = [
            _variant(
                "bandpass",
                "Filtered",
                inputs={"signal": "Raw"},
                constants={"hz": hz_value},
            ),
        ]
        return aggregate_variants(variants, listed_var_names=set())

    def test_removes_value_already_in_db(self):
        pending = {"hz": {"20", "30"}}
        agg = self._one_wiring_agg(hz_value=20)
        cleaned, removals = auto_clean_pending_constants(pending, agg)
        assert "20" not in cleaned["hz"]
        assert "30" in cleaned["hz"]
        assert ("hz", "20") in removals

    def test_nothing_to_clean(self):
        pending = {"hz": {"99"}}
        agg = self._one_wiring_agg(hz_value=20)
        cleaned, removals = auto_clean_pending_constants(pending, agg)
        assert cleaned["hz"] == {"99"}
        assert removals == []

    def test_empty_pending(self):
        agg = self._one_wiring_agg(hz_value=20)
        cleaned, removals = auto_clean_pending_constants({}, agg)
        assert cleaned == {}

    def test_no_consuming_wiring_never_cleans(self):
        """A constant with no real call site yet must never be auto-cleaned —
        an empty required-groups set is a vacuous match, not a real one."""
        pending = {"hz": {"20"}}
        agg = aggregate_variants([], listed_var_names=set())
        cleaned, removals = auto_clean_pending_constants(pending, agg)
        assert cleaned["hz"] == {"20"}
        assert removals == []

    def test_does_not_clean_until_every_sibling_wiring_has_run(self):
        """Regression: two function nodes share a name (compute_rolling_vo2)
        but are wired to different inputs/outputs (RawVO2->RollingVO2 vs.
        RawHeartRate->RollingHR) and both consume the same constant. Once
        ONE wiring runs with the new value, the value must stay pending for
        the OTHER wiring — it hasn't been re-run and would otherwise
        silently drop its (correct) pending/yellow indicator."""
        variants = [
            _variant(
                "compute_rolling_vo2",
                "RollingVO2",
                inputs={"signal": "RawVO2"},
                constants={"window_seconds": 60},
            ),
            _variant(
                "compute_rolling_vo2",
                "RollingHR",
                inputs={"signal": "RawHeartRate"},
                constants={"window_seconds": 30},
            ),
        ]
        agg = aggregate_variants(variants, listed_var_names=set())
        pending = {"window_seconds": {"60"}}
        cleaned, removals = auto_clean_pending_constants(pending, agg)
        assert cleaned["window_seconds"] == {"60"}
        assert removals == []

    def test_cleans_once_every_sibling_wiring_has_run(self):
        """Once BOTH wirings have a real record at the staged value, it's
        safe to auto-clean."""
        variants = [
            _variant(
                "compute_rolling_vo2",
                "RollingVO2",
                inputs={"signal": "RawVO2"},
                constants={"window_seconds": 60},
            ),
            _variant(
                "compute_rolling_vo2",
                "RollingHR",
                inputs={"signal": "RawHeartRate"},
                constants={"window_seconds": 60},
            ),
        ]
        agg = aggregate_variants(variants, listed_var_names=set())
        pending = {"window_seconds": {"60"}}
        cleaned, removals = auto_clean_pending_constants(pending, agg)
        assert cleaned["window_seconds"] == set()
        assert ("window_seconds", "60") in removals


# ---------------------------------------------------------------------------
# pending_value_group_coverage
# ---------------------------------------------------------------------------


class TestPendingValueGroupCoverage:
    def _sibling_wiring_agg(self, *, vo2_hz=60, hr_hz=30):
        """Same fn name, two different wirings (RawVO2->RollingVO2 vs.
        RawHeartRate->RollingHR), sharing the constant `window_seconds` —
        the exact shape from the reported bug: running one shouldn't leave
        it waiting on the other."""
        variants = [
            _variant(
                "compute_rolling_vo2",
                "RollingVO2",
                inputs={"signal": "RawVO2"},
                constants={"window_seconds": vo2_hz},
            ),
            _variant(
                "compute_rolling_vo2",
                "RollingHR",
                inputs={"signal": "RawHeartRate"},
                constants={"window_seconds": hr_hz},
            ),
        ]
        agg = aggregate_variants(variants, listed_var_names=set())
        return agg

    def test_group_coverage_scoped_per_wiring(self):
        agg = self._sibling_wiring_agg(vo2_hz=25, hr_hz=30)
        coverage = pending_value_group_coverage({"window_seconds": {"25"}}, agg)
        covered_groups = coverage[("window_seconds", "25")]
        assert covered_groups == {
            (
                "compute_rolling_vo2",
                wiring_id("compute_rolling_vo2", {"signal": "RawVO2"}, {"RollingVO2"}, {}),
            )
        }

    def test_no_coverage_when_no_wiring_has_run_it(self):
        agg = self._sibling_wiring_agg(vo2_hz=60, hr_hz=30)
        coverage = pending_value_group_coverage({"window_seconds": {"25"}}, agg)
        assert coverage[("window_seconds", "25")] == set()


# ---------------------------------------------------------------------------
# build_variable_nodes
# ---------------------------------------------------------------------------


class TestBuildVariableNodes:
    def test_node_structure(self):
        nodes = build_variable_nodes(
            {"RawEMG"}, record_counts={"RawEMG": 4}, run_states={}
        )
        assert len(nodes) == 1
        n = nodes[0]
        assert n["id"] == "var__RawEMG"
        assert n["type"] == "variableNode"
        assert n["data"]["label"] == "RawEMG"
        assert n["data"]["total_records"] == 4

    def test_run_state_from_map(self):
        nodes = build_variable_nodes({"A"}, {}, run_states={"var__A": "pending"})
        assert nodes[0]["data"]["run_state"] == "pending"

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
# build_parameter_nodes
# ---------------------------------------------------------------------------


class TestBuildParameterNodes:
    def test_node_structure(self):
        const_counts = {"hz": {"10": 3, "20": 5}}
        nodes = build_parameter_nodes(const_counts, pending_constants={})
        assert len(nodes) == 1
        n = nodes[0]
        assert n["id"] == "param__hz"
        assert n["type"] == "parameterNode"
        assert n["data"]["label"] == "hz"
        values = {v["value"] for v in n["data"]["values"]}
        assert values == {"10", "20"}

    def test_pending_value_appended(self):
        const_counts = {"hz": {"10": 3}}
        nodes = build_parameter_nodes(const_counts, pending_constants={"hz": {"99"}})
        values = {v["value"] for v in nodes[0]["data"]["values"]}
        assert "99" in values

    def test_pending_not_duplicated_if_already_in_counts(self):
        const_counts = {"hz": {"10": 3}}
        nodes = build_parameter_nodes(const_counts, pending_constants={"hz": {"10"}})
        values = [v["value"] for v in nodes[0]["data"]["values"]]
        assert values.count("10") == 1

    def test_pending_record_count_is_zero(self):
        const_counts = {"hz": {"10": 3}}
        nodes = build_parameter_nodes(const_counts, pending_constants={"hz": {"99"}})
        pending_entry = next(
            v for v in nodes[0]["data"]["values"] if v["value"] == "99"
        )
        assert pending_entry["record_count"] == 0

    def test_new_source_value_appended_and_tagged(self):
        const_counts = {"hz": {"10": 3}}
        nodes = build_parameter_nodes(
            const_counts, pending_constants={}, source_parameters={"hz": Parameter(20)}
        )
        values = nodes[0]["data"]["values"]
        assert {v["value"] for v in values} == {"10", "20"}
        new_entry = next(v for v in values if v["value"] == "20")
        assert new_entry["record_count"] == 0
        assert new_entry["is_current_source_value"] is True
        old_entry = next(v for v in values if v["value"] == "10")
        assert "is_current_source_value" not in old_entry

    def test_source_value_matching_existing_db_history_is_tagged_in_place(self):
        const_counts = {"hz": {"10": 3}}
        nodes = build_parameter_nodes(
            const_counts, pending_constants={}, source_parameters={"hz": Parameter(10)}
        )
        values = nodes[0]["data"]["values"]
        assert len(values) == 1
        assert values[0] == {
            "value": "10",
            "record_count": 3,
            "checked": True,
            "is_current_source_value": True,
        }

    def test_source_value_matching_pending_is_tagged_in_place(self):
        nodes = build_parameter_nodes(
            {}, pending_constants={"hz": {"99"}}, source_parameters={"hz": Parameter(99)}
        )
        values = nodes[0]["data"]["values"]
        assert len(values) == 1
        assert values[0]["is_current_source_value"] is True

    def test_source_edit_leaves_stale_db_history_row_intact(self):
        # Simulates a further source edit: the registry value moved on to
        # "30", but "10" still has DB run history and must stay visible
        # (decision #2 -- DB history never vanishes because of a source edit).
        const_counts = {"hz": {"10": 3}}
        nodes = build_parameter_nodes(
            const_counts, pending_constants={}, source_parameters={"hz": Parameter(30)}
        )
        values = nodes[0]["data"]["values"]
        assert {v["value"] for v in values} == {"10", "30"}
        old_entry = next(v for v in values if v["value"] == "10")
        assert old_entry["record_count"] == 3
        assert "is_current_source_value" not in old_entry

    def test_registry_only_constant_with_no_history_still_gets_a_node(self):
        nodes = build_parameter_nodes(
            {}, pending_constants={}, source_parameters={"hz": Parameter(5)}
        )
        assert len(nodes) == 1
        assert nodes[0]["id"] == "param__hz"
        assert nodes[0]["data"]["values"] == [
            {
                "value": "5",
                "record_count": 0,
                "checked": True,
                "is_current_source_value": True,
            }
        ]

    def test_no_source_or_hidden_values_defaults_checked_true(self):
        const_counts = {"hz": {"10": 3}}
        nodes = build_parameter_nodes(const_counts, pending_constants={})
        values = nodes[0]["data"]["values"]
        assert values == [{"value": "10", "record_count": 3, "checked": True}]

    def test_hidden_value_reported_unchecked(self):
        const_counts = {"hz": {"10": 3, "20": 1}}
        nodes = build_parameter_nodes(
            const_counts, pending_constants={}, hidden_values={"hz": {"10"}}
        )
        values = {v["value"]: v["checked"] for v in nodes[0]["data"]["values"]}
        assert values == {"10": False, "20": True}

    def test_hidden_pending_value_reported_unchecked(self):
        # A pending value is only ever staged for a constant that already
        # has DB history or a source declaration (build_parameter_nodes'
        # all_names union doesn't itself cover a pending-only name -- that
        # case is a genuinely-new never-run constant, which the GUI shows
        # via a manual node instead, see pipeline_discovery._seed_step) --
        # const_counts here keeps this scenario realistic.
        nodes = build_parameter_nodes(
            {"hz": {"5": 1}},
            pending_constants={"hz": {"99"}},
            hidden_values={"hz": {"99"}},
        )
        values = {v["value"]: v["checked"] for v in nodes[0]["data"]["values"]}
        assert values == {"5": True, "99": False}

    def test_hidden_new_source_value_reported_unchecked(self):
        nodes = build_parameter_nodes(
            {},
            pending_constants={},
            source_parameters={"hz": Parameter(5)},
            hidden_values={"hz": {"5"}},
        )
        values = nodes[0]["data"]["values"]
        assert values[0]["checked"] is False
        assert values[0]["is_current_source_value"] is True


class TestParameterNodeSourceLocation:
    """source_file/source_line/declared_in_entities_file power the canvas
    context menu's "Refresh from file" / "Open source" actions (see
    docs/claude/entity-editability-model.md and PipelineDAG.tsx's
    onNodeContextMenu)."""

    def test_declared_in_entities_file_when_source_matches(self):
        p = Parameter(5)
        p.source_file = "/proj/src/scistack_entities.toml"
        p.source_line = 12
        nodes = build_parameter_nodes(
            {},
            pending_constants={},
            source_parameters={"hz": p},
            entities_file="/proj/src/scistack_entities.toml",
        )
        data = nodes[0]["data"]
        assert data["source_file"] == "/proj/src/scistack_entities.toml"
        assert data["source_line"] == 12
        assert data["declared_in_entities_file"] is True

    def test_not_declared_in_entities_file_when_source_is_a_legacy_module(self):
        # Legacy .py-declared Parameter: source_file points elsewhere, so a
        # reload of the entities file wouldn't change this value — the GUI
        # must not offer "Refresh from file" for it.
        p = Parameter(5)
        p.source_file = "/proj/src/params.py"
        p.source_line = 42
        nodes = build_parameter_nodes(
            {},
            pending_constants={},
            source_parameters={"hz": p},
            entities_file="/proj/src/scistack_entities.toml",
        )
        data = nodes[0]["data"]
        assert data["source_file"] == "/proj/src/params.py"
        assert data["source_line"] == 42
        assert data["declared_in_entities_file"] is False

    def test_no_entities_file_configured_is_never_declared_in_it(self):
        p = Parameter(5)
        p.source_file = "/proj/src/scistack_entities.toml"
        p.source_line = 12
        nodes = build_parameter_nodes(
            {}, pending_constants={}, source_parameters={"hz": p}, entities_file=None
        )
        assert nodes[0]["data"]["declared_in_entities_file"] is False

    def test_db_only_value_with_no_current_source_gets_no_location(self):
        # A const with DB history but no matching entry in source_parameters
        # (declaration was removed / never in this scan) -- see
        # build_parameter_nodes' own "DB is the record of what actually
        # ran" comment for why the node still exists.
        nodes = build_parameter_nodes(
            {"hz": {"10": 3}},
            pending_constants={},
            source_parameters={},
            entities_file="/proj/src/scistack_entities.toml",
        )
        data = nodes[0]["data"]
        assert data["source_file"] is None
        assert data["source_line"] is None
        assert data["declared_in_entities_file"] is False


class TestGeneratedValueGroups:
    """Values written in one go by the panel's "Replace values" collapse to
    ONE row with a compact label and one checkbox. Values added individually
    are untouched by any of this — that is the whole point of the split."""

    RANGE = {
        "kind": "range",
        "spec": {"start": 0, "end": 6, "step": 2},
        "values": ["0", "2", "4", "6"],
    }

    def test_collapses_to_one_row_with_a_label(self):
        nodes = build_parameter_nodes(
            {},
            pending_constants={},
            source_parameters={"hz": Parameter(0, 2, 4, 6)},
            value_groups={"hz": self.RANGE},
        )
        values = nodes[0]["data"]["values"]
        assert len(values) == 1
        assert values[0]["kind"] == "generated"
        assert values[0]["value"] == "0:2:6 — 4 values"
        assert values[0]["members"] == ["0", "2", "4", "6"]
        assert values[0]["is_current_source_value"] is True

    def test_record_counts_are_summed_across_members(self):
        nodes = build_parameter_nodes(
            {"hz": {"0": 3, "2": 4, "4": 0, "6": 1}},
            pending_constants={},
            source_parameters={"hz": Parameter(0, 2, 4, 6)},
            value_groups={"hz": self.RANGE},
        )
        assert nodes[0]["data"]["values"][0]["record_count"] == 8

    def test_one_hidden_member_unchecks_the_whole_set(self):
        nodes = build_parameter_nodes(
            {},
            pending_constants={},
            source_parameters={"hz": Parameter(0, 2, 4, 6)},
            hidden_values={"hz": {"4"}},
            value_groups={"hz": self.RANGE},
        )
        assert nodes[0]["data"]["values"][0]["checked"] is False

    def test_individually_added_value_stays_its_own_row(self):
        """Adding a value after generating leaves the set intact: one group
        row, plus the extra value rendered exactly as it always was."""
        nodes = build_parameter_nodes(
            {},
            pending_constants={},
            source_parameters={"hz": Parameter(0, 2, 4, 6, 99)},
            value_groups={"hz": self.RANGE},
        )
        values = nodes[0]["data"]["values"]
        assert len(values) == 2
        assert values[0]["kind"] == "generated"
        assert values[1] == {
            "value": "99",
            "record_count": 0,
            "checked": True,
            "is_current_source_value": True,
        }

    def test_stale_group_is_ignored_and_every_value_renders_individually(self):
        """A member removed from source (a hand edit, or the panel's ×)
        makes the grouping stale. Source is the truth for which values
        exist, so the group must never mask a value that is really
        declared — or keep claiming one that is not."""
        nodes = build_parameter_nodes(
            {},
            pending_constants={},
            source_parameters={"hz": Parameter(0, 2, 6)},  # "4" is gone
            value_groups={"hz": self.RANGE},
        )
        values = nodes[0]["data"]["values"]
        assert [v["value"] for v in values] == ["0", "2", "6"]
        assert all("kind" not in v for v in values)

    def test_no_group_leaves_values_exactly_as_before(self):
        with_group = build_parameter_nodes(
            {"hz": {"10": 3}}, pending_constants={}, value_groups={}
        )
        without = build_parameter_nodes({"hz": {"10": 3}}, pending_constants={})
        assert with_group == without

    def test_list_generation_label_shows_members(self):
        from scistack_gui.domain.graph_builder import render_value_group_label

        assert (
            render_value_group_label("list", {"members": [1, 2, 5, 10]}, 4)
            == "1, 2, 5, 10"
        )

    def test_long_list_label_truncates_with_a_count(self):
        from scistack_gui.domain.graph_builder import render_value_group_label

        label = render_value_group_label("list", {"members": list(range(12))}, 12)
        assert label == "0, 1, 2, 3, 4, 5 — 12 values"

    def test_whole_floats_render_without_a_trailing_zero(self):
        from scistack_gui.domain.graph_builder import render_value_group_label

        label = render_value_group_label(
            "range", {"start": 0.0, "end": 20.0, "step": 2.0}, 11
        )
        assert label == "0:2:20 — 11 values"


class TestParameterMerge:
    """One Parameter class, one node kind (D6). A Parameter keeps its id,
    type and position whatever its value count -- adding a value is adding
    an argument, never a change of form."""

    def test_multi_value_parameter_renders_as_a_parameter_node(self):
        nodes = build_parameter_nodes(
            {}, pending_constants={}, source_parameters={"hz": Parameter(1, 2)}
        )
        assert len(nodes) == 1
        assert nodes[0]["type"] == "parameterNode"
        assert nodes[0]["id"] == "param__hz"

    def test_one_and_many_values_share_one_id(self):
        """The id must not encode the value count, or adding a second value
        would move the node."""
        one = build_parameter_nodes({}, {}, source_parameters={"hz": Parameter(1)})
        many = build_parameter_nodes(
            {}, {}, source_parameters={"hz": Parameter(1, 2)}
        )
        assert one[0]["id"] == many[0]["id"] == "param__hz"
        assert one[0]["type"] == many[0]["type"] == "parameterNode"

    def test_all_declared_values_badged_as_current_source(self):
        nodes = build_parameter_nodes(
            {}, pending_constants={}, source_parameters={"hz": Parameter(10, 20)}
        )
        values = nodes[0]["data"]["values"]
        assert [v["value"] for v in values] == ["10", "20"]
        assert all(v["is_current_source_value"] for v in values)

    def test_multi_value_parameter_has_per_value_checkboxes(self):
        """The old build_sweep_nodes emitted no `checked` at all, so sweeps
        had no persisted include/exclude. Every Parameter value has one now,
        whatever the count."""
        nodes = build_parameter_nodes(
            {},
            pending_constants={},
            source_parameters={"hz": Parameter(10, 20)},
            hidden_values={"hz": {"20"}},
        )
        values = {v["value"]: v["checked"] for v in nodes[0]["data"]["values"]}
        assert values == {"10": True, "20": False}

    def test_keeps_db_history_values(self):
        """A value that has left source but has run history stays visible --
        the DB is the record of what actually ran (decision #2), which the
        old sweep node could not express."""
        nodes = build_parameter_nodes(
            {"hz": {"5": 3}},
            pending_constants={},
            source_parameters={"hz": Parameter(10)},
        )
        values = {v["value"]: v for v in nodes[0]["data"]["values"]}
        assert set(values) == {"5", "10"}
        assert values["5"]["record_count"] == 3
        assert "is_current_source_value" not in values["5"]
        assert values["10"]["is_current_source_value"] is True


# ---------------------------------------------------------------------------
# resolve_path_input_name / path_input_display / seed_undiscovered_path_inputs
# ---------------------------------------------------------------------------


class TestPathInputDisplay:
    def test_bare_path_input(self):
        display = path_input_display(PathInput("{s}/f.csv", root_folder="/data"))
        assert display == {
            "template": "{s}/f.csv",
            "root_folder": "/data",
            "alternate_templates": [],
        }

    def test_each_of_path_inputs_first_is_primary(self):
        from scifor import EachOf

        obj = EachOf(PathInput("{s}/a.csv"), PathInput("{s}/b.csv", root_folder="/x"))
        display = path_input_display(obj)
        assert display["template"] == "{s}/a.csv"
        assert display["alternate_templates"] == [
            {"template": "{s}/b.csv", "root_folder": "/x"}
        ]


class TestResolvePathInputName:
    def test_matches_registry_by_content(self):
        registry = {"RAW_EMG": PathInput("{s}/f.csv", root_folder="/data")}
        name, display = resolve_path_input_name(
            {"template": "{s}/f.csv", "root_folder": "/data"}, registry
        )
        assert name == "RAW_EMG"
        assert display["template"] == "{s}/f.csv"

    def test_falls_back_to_unresolved_key_on_no_match(self):
        name, display = resolve_path_input_name(
            {"template": "{s}/f.csv", "root_folder": None}, {}
        )
        assert name == "__unresolved__:{s}/f.csv"
        assert display["template"] == "{s}/f.csv"

    def test_recorded_history_reattaches_an_overwritten_template(self):
        """D7: after a GUI template edit, a run recorded against the OLD
        template must still resolve to the node instead of detaching into
        __unresolved__."""
        registry = {"RAW": PathInput("new.csv")}
        history = {("old.csv", None): "RAW"}

        name, display = resolve_path_input_name(
            {"template": "old.csv", "root_folder": None}, registry, history
        )

        assert name == "RAW"
        # Display comes from the CURRENT declaration — the node shows what
        # source says now, while keeping its history attached.
        assert display["template"] == "new.csv"

    def test_live_registry_wins_over_history(self):
        registry = {"RAW": PathInput("old.csv")}
        history = {("old.csv", None): "SOMETHING_ELSE"}

        name, _ = resolve_path_input_name(
            {"template": "old.csv", "root_folder": None}, registry, history
        )

        assert name == "RAW"

    def test_history_for_a_removed_declaration_still_unresolved(self):
        """History only re-attributes to a name that still EXISTS — a deleted
        declaration has no node to attach to."""
        history = {("old.csv", None): "GONE"}

        name, _ = resolve_path_input_name(
            {"template": "old.csv", "root_folder": None}, {}, history
        )

        assert name == "__unresolved__:old.csv"

    def test_root_folder_distinguishes_history_entries(self):
        registry = {"RAW": PathInput("new.csv")}
        history = {("old.csv", "/data"): "RAW"}

        assert resolve_path_input_name(
            {"template": "old.csv", "root_folder": "/data"}, registry, history
        )[0] == "RAW"
        assert resolve_path_input_name(
            {"template": "old.csv", "root_folder": None}, registry, history
        )[0].startswith("__unresolved__")


class TestResolvePathInputNameProjectRoot:
    """A run recorded with the project root as its ``root_folder`` belongs to
    the rootless declaration that produced it.

    Generated MATLAB commands used to substitute the project root for a missing
    ``root_folder``, so the run went into the DB under a key its own
    declaration does not have and the canvas grew an ``__unresolved__`` ghost
    node beside the real one after the first successful run. The generator no
    longer does that, but those rows are permanent — see
    ``.claude/plan-pathinput-unresolved-after-run.md``.
    """

    ROOT = "/projects/myexp"

    def test_project_rooted_history_attributes_to_rootless_declaration(self):
        registry = {"delsysEMG": PathInput("data/10MWT_{pass}.mat")}

        name, display = resolve_path_input_name(
            {"template": "data/10MWT_{pass}.mat", "root_folder": self.ROOT},
            registry,
            None,
            self.ROOT,
        )

        assert name == "delsysEMG"
        # The declaration is still the display authority: rootless is what
        # source says, and what the next run will record.
        assert display["root_folder"] is None

    def test_a_different_root_is_still_unresolved(self):
        """Only the project root is equivalent to no root. Another absolute
        root is a genuinely different input and must not be absorbed."""
        registry = {"delsysEMG": PathInput("data/10MWT_{pass}.mat")}

        name, _ = resolve_path_input_name(
            {"template": "data/10MWT_{pass}.mat", "root_folder": "/somewhere/else"},
            registry,
            None,
            self.ROOT,
        )

        assert name.startswith("__unresolved__")

    def test_exact_match_still_wins(self):
        """A declaration that really does declare the project root as its root
        must win over the rootless one — the normalization is a fallback."""
        registry = {
            "ROOTLESS": PathInput("data/f.csv"),
            "ROOTED": PathInput("data/f.csv", root_folder=self.ROOT),
        }

        name, _ = resolve_path_input_name(
            {"template": "data/f.csv", "root_folder": self.ROOT},
            registry,
            None,
            self.ROOT,
        )

        assert name == "ROOTED"

    def test_unnormalized_root_still_matches(self):
        registry = {"delsysEMG": PathInput("data/f.csv")}

        name, _ = resolve_path_input_name(
            {"template": "data/f.csv", "root_folder": f"{self.ROOT}/./"},
            registry,
            None,
            self.ROOT,
        )

        assert name == "delsysEMG"

    def test_project_rooted_history_reaches_the_name_history_table(self):
        """The two repair paths compose: a template edited in the GUI *and*
        recorded under the injected root still finds its node."""
        registry = {"RAW": PathInput("new.csv")}
        history = {("old.csv", None): "RAW"}

        name, _ = resolve_path_input_name(
            {"template": "old.csv", "root_folder": self.ROOT},
            registry,
            history,
            self.ROOT,
        )

        assert name == "RAW"

    def test_no_project_root_leaves_behavior_unchanged(self):
        """Callers that pass no project root keep the old three-strategy
        behavior — there is nothing to normalize against."""
        registry = {"delsysEMG": PathInput("data/f.csv")}

        name, _ = resolve_path_input_name(
            {"template": "data/f.csv", "root_folder": self.ROOT}, registry
        )

        assert name.startswith("__unresolved__")

    def test_convert_scidb_path_inputs_passes_the_root_through(self):
        """The shared converter must resolve the same name its callers'
        hidden-edge-id lookups expect — the root has to reach it."""
        from scistack_gui.domain.graph_builder import convert_scidb_path_inputs

        result = convert_scidb_path_inputs(
            {
                "filepath": {
                    "template": "data/f.csv",
                    "root_folder": self.ROOT,
                    "functions": [("load_file", "call1")],
                }
            },
            {"delsysEMG": PathInput("data/f.csv")},
            None,
            self.ROOT,
        )

        assert list(result) == ["delsysEMG"]


class TestSeedUndiscoveredPathInputs:
    def test_adds_registry_entries_with_no_history(self):
        result = seed_undiscovered_path_inputs(
            {}, {"newpath": PathInput("{s}/x.csv")}
        )
        assert "newpath" in result
        assert result["newpath"]["functions"] == set()

    def test_does_not_overwrite_existing_entry(self):
        path_inputs = {
            "p": {"template": "existing", "root_folder": None, "functions": set()}
        }
        seed_undiscovered_path_inputs(path_inputs, {"p": PathInput("{s}/new.csv")})
        # Already has DB-history-derived data — not clobbered by the seed step.
        assert path_inputs["p"]["template"] == "existing"


# ---------------------------------------------------------------------------
# build_path_input_nodes
# ---------------------------------------------------------------------------


class TestBuildPathInputNodes:
    def test_node_structure(self):
        path_inputs = {
            "mypath": {
                "template": "{s}/f.csv",
                "root_folder": "/data",
                "functions": set(),
            }
        }
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
    BP_CID = _cid("bp-test")
    BP_KEY = ("bandpass", BP_CID)
    BP_NODE = f"fn__bandpass__{BP_CID}"

    def _make(self, **overrides):
        defaults = {
            "fn_input_params": {self.BP_KEY: {"signal": "Raw"}},
            "fn_outputs": {self.BP_KEY: {"Filtered"}},
            "fn_constants": {self.BP_KEY: {"hz"}},
            "fn_variants_map": {self.BP_KEY: []},
            "fn_params_map": {"bandpass": ["signal", "hz"]},
            "run_states": {self.BP_NODE: "green"},
            "matlab_functions": set(),
            "saved_configs": {"bandpass": None},
        }
        defaults.update(overrides)
        return build_function_nodes(**defaults)

    def test_node_structure(self):
        nodes = self._make()
        assert len(nodes) == 1
        n = nodes[0]
        assert n["id"] == self.BP_NODE
        assert n["type"] == "functionNode"
        assert n["data"]["label"] == "bandpass"
        assert n["data"]["call_id"] == self.BP_CID

    def test_run_state_applied(self):
        nodes = self._make(run_states={self.BP_NODE: "pending"})
        assert nodes[0]["data"]["run_state"] == "pending"

    def test_matlab_language_flag(self):
        nodes = self._make(matlab_functions={"bandpass"})
        assert nodes[0]["data"]["language"] == "matlab"

    def test_non_matlab_has_no_language_flag(self):
        nodes = self._make()
        assert "language" not in nodes[0]["data"]

    def test_saved_config_applied(self):
        nodes = self._make(
            saved_configs={"bandpass": {"schemaSelection": {"include": [[["subject", "1"]]]}}}
        )
        assert nodes[0]["data"]["schemaSelection"] == {"include": [[["subject", "1"]]]}

    def test_unknown_param_filled_with_empty_string(self):
        nodes = self._make(
            fn_input_params={self.BP_KEY: {"signal": "Raw"}},
            fn_constants={},
            fn_params_map={"bandpass": ["signal", "low_hz"]},
        )
        assert nodes[0]["data"]["input_params"].get("low_hz") == ""

    def test_output_types_sorted(self):
        nodes = self._make(fn_outputs={self.BP_KEY: {"C", "A", "B"}})
        assert nodes[0]["data"]["output_types"] == ["A", "B", "C"]

    def test_constant_params_ordered_by_signature_not_alphabetically(self):
        # Regression: compute_rolling_vo2(signal, window_seconds, sample_interval)
        # — alphabetical sort would put sample_interval before window_seconds,
        # flipping the order from what the manual (pre-run) node showed.
        nodes = self._make(
            fn_constants={self.BP_KEY: {"window_seconds", "sample_interval"}},
            fn_params_map={"bandpass": ["signal", "window_seconds", "sample_interval"]},
        )
        assert nodes[0]["data"]["constant_params"] == [
            "window_seconds",
            "sample_interval",
        ]

    def test_input_params_ordered_by_signature_not_alphabetically(self):
        nodes = self._make(
            fn_input_params={self.BP_KEY: {"zeta": "Z", "alpha": "A"}},
            fn_constants={},
            fn_params_map={"bandpass": ["zeta", "alpha"]},
        )
        assert list(nodes[0]["data"]["input_params"].keys()) == ["zeta", "alpha"]

    def test_two_call_sites_produce_two_nodes(self):
        cid_a, cid_b = _cid("a"), _cid("b")
        ka, kb = ("bandpass", cid_a), ("bandpass", cid_b)
        nodes = build_function_nodes(
            fn_input_params={ka: {"signal": "Raw"}, kb: {"signal": "Raw"}},
            fn_outputs={ka: {"Filtered"}, kb: {"Filtered"}},
            fn_constants={ka: {"hz"}, kb: {"hz"}},
            fn_variants_map={
                ka: [{"constants": {"hz": 20}}],
                kb: [{"constants": {"hz": 50}}],
            },
            fn_params_map={"bandpass": ["signal", "hz"]},
            run_states={
                f"fn__bandpass__{cid_a}": "green",
                f"fn__bandpass__{cid_b}": "red",
            },
            matlab_functions=set(),
            saved_configs={"bandpass": None},
        )
        ids = {n["id"] for n in nodes}
        assert ids == {f"fn__bandpass__{cid_a}", f"fn__bandpass__{cid_b}"}
        states = {n["id"]: n["data"]["run_state"] for n in nodes}
        assert states[f"fn__bandpass__{cid_a}"] == "green"
        assert states[f"fn__bandpass__{cid_b}"] == "red"


# ---------------------------------------------------------------------------
# build_edges
# ---------------------------------------------------------------------------


class TestBuildEdges:
    F_CID = _cid("f-call")
    F_KEY = ("f", F_CID)
    F_NODE = f"fn__f__{F_CID}"

    def test_var_to_fn_edge(self):
        edges = build_edges(
            fn_input_params={self.F_KEY: {"signal": "Raw"}},
            fn_outputs={self.F_KEY: set()},
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
        )
        assert any(
            e["source"] == "var__Raw" and e["target"] == self.F_NODE for e in edges
        )

    def test_fn_to_var_edge(self):
        edges = build_edges(
            fn_input_params={},
            fn_outputs={self.F_KEY: {"Out"}},
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
        )
        assert any(
            e["source"] == self.F_NODE and e["target"] == "var__Out" for e in edges
        )

    def test_const_to_fn_edge(self):
        edges = build_edges(
            fn_input_params={},
            fn_outputs={},
            const_fns={"hz": {self.F_KEY}},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
        )
        assert any(
            e["source"] == "param__hz" and e["target"] == self.F_NODE for e in edges
        )

    def test_path_input_to_fn_edge(self):
        edges = build_edges(
            fn_input_params={},
            fn_outputs={},
            const_fns={},
            path_inputs={
                "mypath": {
                    "template": "",
                    "root_folder": None,
                    "functions": {(self.F_KEY, "filepath")},
                }
            },
            manual_edges=[],
            hidden_ids=set(),
        )
        assert any(
            e["source"] == "pathInput__mypath"
            and e["target"] == self.F_NODE
            and e["targetHandle"] == "in__filepath"
            for e in edges
        )

    def test_manual_edge_included(self):
        me = {
            "id": "manual-1",
            "source": "uuid-var",
            "target": self.F_NODE,
            "sourceHandle": "",
            "targetHandle": "in__x",
        }
        edges = build_edges({}, {}, {}, {}, [me], set())
        assert any(e["id"] == "manual-1" for e in edges)

    def test_manual_edge_skipped_if_hidden(self):
        me = {
            "id": "manual-1",
            "source": "uuid-var",
            "target": self.F_NODE,
            "sourceHandle": "",
            "targetHandle": "",
        }
        edges = build_edges({}, {}, {}, {}, [me], hidden_ids={"uuid-var"})
        assert not any(e["id"] == "manual-1" for e in edges)

    def test_no_duplicate_edges(self):
        # Same var→fn from two params should only produce one edge.
        edges = build_edges(
            fn_input_params={self.F_KEY: {"a": "Raw", "b": "Raw"}},
            fn_outputs={},
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
        )
        var_to_fn = [
            e for e in edges if e["source"] == "var__Raw" and e["target"] == self.F_NODE
        ]
        assert len(var_to_fn) == 1

    def test_manual_edge_not_duplicated_if_already_in_db_edges(self):
        edge_id = f"e__Raw__f__{self.F_CID}"
        edges = build_edges(
            fn_input_params={self.F_KEY: {"signal": "Raw"}},
            fn_outputs={},
            const_fns={},
            path_inputs={},
            manual_edges=[
                {
                    "id": edge_id,
                    "source": "var__Raw",
                    "target": self.F_NODE,
                    "sourceHandle": "",
                    "targetHandle": "in__signal",
                }
            ],
            hidden_ids=set(),
        )
        matching = [e for e in edges if e["id"] == edge_id]
        assert len(matching) == 1

    def test_manual_edge_superseded_by_db_edge_for_same_connection(self):
        """A manual edge keeps its random ``manual__xxxx`` id forever, and
        graduation rewrites its endpoints onto the DB-derived node ids — so it
        describes the same connection as an ``e__...`` edge while never
        colliding by id. Both used to render: 4 edges for 2 connections
        (examples/vo2max/scidb.log), and deleting one left its twin.
        """
        edges = build_edges(
            fn_input_params={self.F_KEY: {"signal": "Raw"}},
            fn_outputs={self.F_KEY: {"Out"}},
            const_fns={},
            path_inputs={},
            manual_edges=[
                {
                    "id": "manual__u0cfjq",
                    "source": "var__Raw",
                    "target": self.F_NODE,
                    "sourceHandle": "",
                    "targetHandle": "in__signal",
                },
                {
                    "id": "manual__n6rg9z",
                    "source": self.F_NODE,
                    "target": "var__Out",
                    "sourceHandle": "",
                    "targetHandle": "",
                },
            ],
            hidden_ids=set(),
        )
        assert len(edges) == 2, [(e["id"], e["source"], e["target"]) for e in edges]
        assert not any(e["id"].startswith("manual__") for e in edges)

    def test_manual_path_input_edge_deduped_by_source_target_and_param(self):
        edges = build_edges(
            fn_input_params={},
            fn_outputs={},
            const_fns={},
            path_inputs={
                "mypath": {
                    "template": "",
                    "root_folder": None,
                    "functions": {(self.F_KEY, "filepath")},
                }
            },
            manual_edges=[
                {
                    "id": "manual__aaa",
                    "source": "pathInput__mypath",
                    "target": self.F_NODE,
                    "sourceHandle": "",
                    "targetHandle": "in__filepath",
                }
            ],
            hidden_ids=set(),
        )
        pi_edges = [e for e in edges if e["source"] == "pathInput__mypath"]
        assert len(pi_edges) == 1
        assert not pi_edges[0]["id"].startswith("manual__")

    def test_manual_path_input_edge_to_different_param_is_kept(self):
        """Same PathInput feeding a DIFFERENT parameter is a real second
        connection, not a duplicate."""
        edges = build_edges(
            fn_input_params={},
            fn_outputs={},
            const_fns={},
            path_inputs={
                "mypath": {
                    "template": "",
                    "root_folder": None,
                    "functions": {(self.F_KEY, "filepath")},
                }
            },
            manual_edges=[
                {
                    "id": "manual__aaa",
                    "source": "pathInput__mypath",
                    "target": self.F_NODE,
                    "sourceHandle": "",
                    "targetHandle": "in__other_path",
                }
            ],
            hidden_ids=set(),
        )
        pi_edges = [e for e in edges if e["source"] == "pathInput__mypath"]
        assert len(pi_edges) == 2

    def test_hidden_db_edge_is_not_resurrected_by_a_manual_twin(self):
        """Hiding a connection must stay hidden: the DB branch records its
        endpoint key BEFORE the hidden check, so the manual duplicate is
        suppressed too."""
        edges = build_edges(
            fn_input_params={self.F_KEY: {"signal": "Raw"}},
            fn_outputs={},
            const_fns={},
            path_inputs={},
            manual_edges=[
                {
                    "id": "manual__dupe",
                    "source": "var__Raw",
                    "target": self.F_NODE,
                    "sourceHandle": "",
                    "targetHandle": "in__signal",
                }
            ],
            hidden_ids=set(),
            hidden_edge_ids={f"e__Raw__f__{self.F_CID}"},
        )
        assert not [
            e for e in edges if e["source"] == "var__Raw" and e["target"] == self.F_NODE
        ]

    def test_manual_edge_with_no_db_counterpart_still_renders(self):
        edges = build_edges(
            fn_input_params={},
            fn_outputs={},
            const_fns={},
            path_inputs={},
            manual_edges=[
                {
                    "id": "manual__lonely",
                    "source": "var__Raw",
                    "target": self.F_NODE,
                    "sourceHandle": "",
                    "targetHandle": "in__signal",
                }
            ],
            hidden_ids=set(),
        )
        assert [e["id"] for e in edges] == ["manual__lonely"]

    def test_two_call_sites_produce_distinct_edges_to_same_input(self):
        cid_a, cid_b = _cid("a"), _cid("b")
        ka, kb = ("f", cid_a), ("f", cid_b)
        edges = build_edges(
            fn_input_params={ka: {"signal": "Raw"}, kb: {"signal": "Raw"}},
            fn_outputs={ka: {"Out"}, kb: {"Out"}},
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
        )
        targets = {
            e["target"]
            for e in edges
            if e["source"] == "var__Raw" and e["target"].startswith("fn__f__")
        }
        assert targets == {f"fn__f__{cid_a}", f"fn__f__{cid_b}"}

    # --- hidden_edge_ids: excluded from rendering, never affects other categories ---

    def test_hidden_var_to_fn_edge_excluded(self):
        edges = build_edges(
            fn_input_params={self.F_KEY: {"signal": "Raw"}},
            fn_outputs={},
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
            hidden_edge_ids={f"e__Raw__f__{self.F_CID}"},
        )
        assert not any(e["source"] == "var__Raw" for e in edges)

    def test_hidden_fn_to_var_edge_excluded(self):
        edges = build_edges(
            fn_input_params={},
            fn_outputs={self.F_KEY: {"Out"}},
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
            hidden_edge_ids={f"e__f__{self.F_CID}__Out"},
        )
        assert not any(e["target"] == "var__Out" for e in edges)

    def test_hidden_const_to_fn_edge_excluded(self):
        edges = build_edges(
            fn_input_params={},
            fn_outputs={},
            const_fns={"hz": {self.F_KEY}},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
            hidden_edge_ids={f"e__hz__f__{self.F_CID}"},
        )
        assert not any(e["source"] == "param__hz" for e in edges)

    def test_hidden_path_input_to_fn_edge_excluded(self):
        edges = build_edges(
            fn_input_params={},
            fn_outputs={},
            const_fns={},
            path_inputs={
                "mypath": {
                    "template": "",
                    "root_folder": None,
                    "functions": {(self.F_KEY, "filepath")},
                }
            },
            manual_edges=[],
            hidden_ids=set(),
            hidden_edge_ids={f"e__mypath__filepath__f__{self.F_CID}"},
        )
        assert not any(e["source"] == "pathInput__mypath" for e in edges)

    def test_hidden_edge_id_only_excludes_matching_edge(self):
        # Hiding the var->fn edge id must not touch the const->fn edge on
        # the SAME function.
        edges = build_edges(
            fn_input_params={self.F_KEY: {"signal": "Raw"}},
            fn_outputs={},
            const_fns={"hz": {self.F_KEY}},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
            hidden_edge_ids={f"e__Raw__f__{self.F_CID}"},
        )
        assert not any(e["source"] == "var__Raw" for e in edges)
        assert any(e["source"] == "param__hz" for e in edges)

    def test_hidden_edge_ids_none_defaults_to_no_filtering(self):
        edges = build_edges(
            fn_input_params={self.F_KEY: {"signal": "Raw"}},
            fn_outputs={},
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
        )
        assert any(e["source"] == "var__Raw" for e in edges)


class TestDropSupersededManualEdges:
    """build_edges' endpoint dedup runs BEFORE manual nodes graduate, so a
    manual edge still naming the manual node ids doesn't compare equal to the
    DB-derived edge for the same wire. Graduation then rewrites its endpoints
    onto the DB-derived ids — creating the duplicate. This pass re-checks the
    invariant after any mid-build endpoint rewrite.
    """

    F_CID = _cid("f-call")
    F_NODE = f"fn__f__{F_CID}"

    def _db(self, source, target, **kw):
        return {"id": f"e__{source}__{target}", "source": source, "target": target, **kw}

    def _manual(self, source, target, **kw):
        return {
            "id": f"manual__{source}{target}",
            "source": source,
            "target": target,
            "data": {"manual": True},
            **kw,
        }

    def test_graduated_manual_edge_dropped(self):
        """The examples/vo2max/scidb.log case: the first build after a run
        emitted 4 edges for 2 connections; the next rebuild emitted 2."""
        edges = [
            self._db("pathInput__test_pi", self.F_NODE, targetHandle="in__filepath"),
            self._db(self.F_NODE, "var__cpet_data_raw"),
            # Endpoints just rewritten by graduation onto the DB-derived ids.
            self._manual(
                "pathInput__test_pi", self.F_NODE, targetHandle="in__filepath"
            ),
            self._manual(self.F_NODE, "var__cpet_data_raw"),
        ]
        kept, dropped = drop_superseded_manual_edges(edges)
        assert len(kept) == 2, [(e["id"], e["source"], e["target"]) for e in kept]
        assert not any(is_manual_edge(e) for e in kept)
        assert len(dropped) == 2

    def test_placement_suffix_still_matches(self):
        """graduate_manual_node targets a placement-qualified id; the
        DB-derived key is always bare."""
        edges = [
            self._db(self.F_NODE, "var__Out"),
            self._manual(f"{self.F_NODE}::main", "var__Out::main"),
        ]
        kept, dropped = drop_superseded_manual_edges(edges)
        assert len(kept) == 1
        assert len(dropped) == 1

    def test_path_input_to_different_param_is_kept(self):
        """Same PathInput feeding a DIFFERENT parameter is a real second
        connection, not a duplicate."""
        edges = [
            self._db("pathInput__p", self.F_NODE, targetHandle="in__first"),
            self._manual("pathInput__p", self.F_NODE, targetHandle="in__second"),
        ]
        kept, dropped = drop_superseded_manual_edges(edges)
        assert len(kept) == 2
        assert dropped == []

    def test_ungraduated_manual_edge_is_kept(self):
        """A manual edge to a node that has NOT graduated names a different
        connection and must survive."""
        edges = [
            self._db(self.F_NODE, "var__Out"),
            self._manual("fn__f__2qxdue", "var__Out"),
        ]
        kept, dropped = drop_superseded_manual_edges(edges)
        assert len(kept) == 2
        assert dropped == []

    def test_idempotent_on_an_already_clean_build(self):
        """Unconditional in the pipeline, so a build with no rewrites must
        come through untouched."""
        edges = [
            self._db("var__Raw", self.F_NODE, targetHandle="in__signal"),
            self._manual("var__Other", self.F_NODE, targetHandle="in__extra"),
        ]
        kept, dropped = drop_superseded_manual_edges(edges)
        assert kept == edges
        assert dropped == []
        assert drop_superseded_manual_edges(kept)[0] == edges

    def test_two_manual_edges_for_the_same_wire_both_dropped(self):
        edges = [
            self._db(self.F_NODE, "var__Out"),
            dict(self._manual(self.F_NODE, "var__Out"), id="manual__a"),
            dict(self._manual(self.F_NODE, "var__Out"), id="manual__b"),
        ]
        kept, dropped = drop_superseded_manual_edges(edges)
        assert [e["id"] for e in kept] == [f"e__{self.F_NODE}__var__Out"]
        assert {e["id"] for e in dropped} == {"manual__a", "manual__b"}

    def test_manual_only_wire_survives_with_no_db_edge(self):
        edges = [self._manual(self.F_NODE, "var__Out")]
        kept, dropped = drop_superseded_manual_edges(edges)
        assert len(kept) == 1
        assert dropped == []


class TestEdgeDedupKey:
    def test_path_input_keys_on_param(self):
        a = edge_dedup_key("pathInput__p", "fn__f", "in__one")
        b = edge_dedup_key("pathInput__p", "fn__f", "in__two")
        assert a != b
        assert len(a) == 3

    def test_non_path_input_ignores_handle(self):
        a = edge_dedup_key("var__Raw", "fn__f", "in__one")
        b = edge_dedup_key("var__Raw", "fn__f", "in__two")
        assert a == b == ("var__Raw", "fn__f")

    def test_placement_suffix_stripped_from_both_endpoints(self):
        assert edge_dedup_key("fn__f::main", "var__Out::main") == edge_dedup_key(
            "fn__f", "var__Out"
        )

    def test_missing_handle_is_not_an_error(self):
        assert edge_dedup_key("pathInput__p", "fn__f") == ("pathInput__p", "fn__f")


# ---------------------------------------------------------------------------
# Disconnected wirings — hidden_wirings, wiring_disconnected_fkeys,
# wirings_downstream_of, candidate_edge_id, inbound_edge_candidates
# ---------------------------------------------------------------------------


class TestInboundEdgeCandidates:
    def test_builds_all_three_categories(self):
        ids = inbound_edge_candidates(
            "f", "wid123", var_types=["Raw"], const_names=["hz"], path_names=["p"]
        )
        assert ids == ["e__Raw__f__wid123", "e__hz__f__wid123", "e__p__f__wid123"]

    def test_empty_by_default(self):
        assert inbound_edge_candidates("f", "wid123") == []


class TestHiddenWirings:
    F_CID = _cid("f-call")
    F_KEY = ("f", F_CID)

    def test_hidden_var_input_marks_wiring(self):
        wid = wiring_id("f", {"signal": "Raw"}, set(), {})
        result = hidden_wirings(
            fn_input_params={self.F_KEY: {"signal": "Raw"}},
            fn_outputs={},
            fn_constants={},
            path_inputs={},
            hidden_edge_ids={f"e__Raw__f__{wid}"},
        )
        assert result == {("f", wid)}

    def test_hidden_const_input_marks_wiring(self):
        wid = wiring_id("f", {}, set(), {})
        result = hidden_wirings(
            fn_input_params={self.F_KEY: {}},
            fn_outputs={},
            fn_constants={self.F_KEY: {"hz"}},
            path_inputs={},
            hidden_edge_ids={f"e__hz__f__{wid}"},
        )
        assert result == {("f", wid)}

    def test_hidden_path_input_marks_wiring(self):
        # The PathInput is part of the wiring shape now, so its id must be
        # computed WITH the binding — computing it without would produce the
        # id of a different (PathInput-less) wiring and match nothing.
        wid = wiring_id("f", {}, set(), {"filepath": "mypath"})
        result = hidden_wirings(
            fn_input_params={self.F_KEY: {}},
            fn_outputs={},
            fn_constants={},
            path_inputs={"mypath": {"functions": {(self.F_KEY, "filepath")}}},
            hidden_edge_ids={f"e__mypath__filepath__f__{wid}"},
        )
        assert result == {("f", wid)}

    def test_hidden_output_edge_does_not_mark_wiring(self):
        # fn -> var (output) hides are cosmetic only — never disconnect.
        wid = wiring_id("f", {}, {"Out"}, {})
        result = hidden_wirings(
            fn_input_params={self.F_KEY: {}},
            fn_outputs={self.F_KEY: {"Out"}},
            fn_constants={},
            path_inputs={},
            hidden_edge_ids={f"e__f__{wid}__Out"},
        )
        assert result == set()

    def test_no_hidden_edges_is_empty(self):
        result = hidden_wirings(
            fn_input_params={self.F_KEY: {"signal": "Raw"}},
            fn_outputs={},
            fn_constants={},
            path_inputs={},
            hidden_edge_ids=set(),
        )
        assert result == set()

    def test_every_call_site_of_a_wiring_recomputes_same_wiring(self):
        # Two call sites (different constant values) of the SAME wiring —
        # hiding the shared var input marks the wiring once, not per-call-site.
        ka, kb = ("f", _cid("a")), ("f", _cid("b"))
        wid = wiring_id("f", {"signal": "Raw"}, set(), {})
        result = hidden_wirings(
            fn_input_params={ka: {"signal": "Raw"}, kb: {"signal": "Raw"}},
            fn_outputs={},
            fn_constants={},
            path_inputs={},
            hidden_edge_ids={f"e__Raw__f__{wid}"},
        )
        assert result == {("f", wid)}

    def test_manual_reconnect_to_same_handle_clears_disconnected(self):
        # A manual edge onto the SAME handle (in__signal) a hidden inbound
        # edge used to feed — even with a DIFFERENT source variable — must
        # clear the disconnected state (the "stuck disconnected" bug).
        wid = wiring_id("f", {"signal": "Raw"}, set(), {})
        result = hidden_wirings(
            fn_input_params={self.F_KEY: {"signal": "Raw"}},
            fn_outputs={},
            fn_constants={},
            path_inputs={},
            hidden_edge_ids={f"e__Raw__f__{wid}"},
            manual_edges=[
                {
                    "target": fn_node_id("f", wid),
                    "targetHandle": "in__signal",
                    "source": "var__Other",
                }
            ],
        )
        assert result == set()

    def test_manual_reconnect_to_different_handle_stays_disconnected(self):
        # A manual edge onto an UNRELATED handle does not cover the hidden
        # one — the wiring must stay disconnected.
        wid = wiring_id("f", {"signal": "Raw"}, set(), {})
        result = hidden_wirings(
            fn_input_params={self.F_KEY: {"signal": "Raw"}},
            fn_outputs={},
            fn_constants={},
            path_inputs={},
            hidden_edge_ids={f"e__Raw__f__{wid}"},
            manual_edges=[
                {
                    "target": fn_node_id("f", wid),
                    "targetHandle": "in__other_param",
                    "source": "var__Other",
                }
            ],
        )
        assert result == {("f", wid)}

    def test_partial_reconnection_stays_disconnected(self):
        # Two hidden handles (var + const); only the var one is covered by
        # a manual edge. The wiring must stay disconnected until BOTH are.
        wid = wiring_id("f", {"signal": "Raw"}, set(), {})
        result = hidden_wirings(
            fn_input_params={self.F_KEY: {"signal": "Raw"}},
            fn_outputs={},
            fn_constants={self.F_KEY: {"hz"}},
            path_inputs={},
            hidden_edge_ids={f"e__Raw__f__{wid}", f"e__hz__f__{wid}"},
            manual_edges=[
                {
                    "target": fn_node_id("f", wid),
                    "targetHandle": "in__signal",
                    "source": "var__Other",
                }
            ],
        )
        assert result == {("f", wid)}

    def test_manual_edge_scope_suffixed_target_still_matches(self):
        # A manual edge targeting a scope-placed node id (canonical id ::
        # pipeline_id) must still be recognized as covering the handle.
        wid = wiring_id("f", {"signal": "Raw"}, set(), {})
        result = hidden_wirings(
            fn_input_params={self.F_KEY: {"signal": "Raw"}},
            fn_outputs={},
            fn_constants={},
            path_inputs={},
            hidden_edge_ids={f"e__Raw__f__{wid}"},
            manual_edges=[
                {
                    "target": f"{fn_node_id('f', wid)}::pipe_xyz",
                    "targetHandle": "in__signal",
                    "source": "var__Other",
                }
            ],
        )
        assert result == set()


class TestWiringDisconnectedFkeys:
    def test_maps_wirings_back_to_call_sites(self):
        ka, kb = ("f", _cid("a")), ("f", _cid("b"))
        wid = wiring_id("f", {"signal": "Raw"}, set(), {})
        result = wiring_disconnected_fkeys(
            fn_input_params={ka: {"signal": "Raw"}, kb: {"signal": "Raw"}},
            fn_outputs={},
            wirings={("f", wid)},
            path_inputs={},
        )
        assert result == {ka, kb}

    def test_unrelated_wiring_not_included(self):
        ka = ("f", _cid("a"))
        other_wid = wiring_id("f", {"signal": "Other"}, set(), {})
        result = wiring_disconnected_fkeys(
            fn_input_params={ka: {"signal": "Raw"}},
            fn_outputs={},
            wirings={("f", other_wid)},
            path_inputs={},
        )
        assert result == set()

    def test_empty_wirings_is_empty(self):
        result = wiring_disconnected_fkeys(
            fn_input_params={("f", _cid()): {}},
            fn_outputs={},
            wirings=set(),
            path_inputs={},
        )
        assert result == set()


class TestWiringsDownstreamOf:
    def test_direct_consumer_marked_downstream(self):
        a_key, b_key = ("A", _cid("a")), ("B", _cid("b"))
        wid_a = wiring_id("A", {}, {"Out"}, {})
        wid_b = wiring_id("B", {"x": "Out"}, set(), {})
        result = wirings_downstream_of(
            fn_input_params={a_key: {}, b_key: {"x": "Out"}},
            fn_outputs={a_key: {"Out"}, b_key: set()},
            seed_wirings={("A", wid_a)},
            path_inputs={},
        )
        assert result == {("B", wid_b)}

    def test_transitive_chain_marked_downstream(self):
        a_key = ("A", _cid("a"))
        b_key = ("B", _cid("b"))
        c_key = ("C", _cid("c"))
        wid_a = wiring_id("A", {}, {"Mid"}, {})
        wid_b = wiring_id("B", {"x": "Mid"}, {"Final"}, {})
        wid_c = wiring_id("C", {"y": "Final"}, set(), {})
        result = wirings_downstream_of(
            fn_input_params={a_key: {}, b_key: {"x": "Mid"}, c_key: {"y": "Final"}},
            fn_outputs={a_key: {"Mid"}, b_key: {"Final"}, c_key: set()},
            seed_wirings={("A", wid_a)},
            path_inputs={},
        )
        assert result == {("B", wid_b), ("C", wid_c)}

    def test_seed_itself_excluded_from_result(self):
        # A both produces AND consumes "Out" (self-referencing wiring) — a
        # naive BFS would re-add the seed to its own downstream set.
        a_key = ("A", _cid("a"))
        wid_a = wiring_id("A", {"x": "Out"}, {"Out"}, {})
        result = wirings_downstream_of(
            fn_input_params={a_key: {"x": "Out"}},
            fn_outputs={a_key: {"Out"}},
            seed_wirings={("A", wid_a)},
            path_inputs={},
        )
        assert result == set()

    def test_unrelated_wiring_not_marked(self):
        a_key = ("A", _cid("a"))
        unrelated_key = ("U", _cid("u"))
        wid_a = wiring_id("A", {}, {"Out"}, {})
        wid_u = wiring_id("U", {}, {"Unrelated"}, {})
        result = wirings_downstream_of(
            fn_input_params={a_key: {}, unrelated_key: {}},
            fn_outputs={a_key: {"Out"}, unrelated_key: {"Unrelated"}},
            seed_wirings={("A", wid_a)},
            path_inputs={},
        )
        assert ("U", wid_u) not in result

    def test_empty_seed_is_empty(self):
        assert wirings_downstream_of({}, {}, set(), {}) == set()


class TestCandidateEdgeId:
    F_CID = _cid("f-call")
    F_NODE = f"fn__f__{F_CID}"

    def test_var_to_fn(self):
        assert candidate_edge_id("var__Raw", self.F_NODE) == f"e__Raw__f__{self.F_CID}"

    def test_const_to_fn(self):
        assert candidate_edge_id("param__hz", self.F_NODE) == f"e__hz__f__{self.F_CID}"

    def test_path_input_to_fn(self):
        assert (
            candidate_edge_id("pathInput__mypath", self.F_NODE, "in__filepath")
            == f"e__mypath__filepath__f__{self.F_CID}"
        )

    def test_path_input_to_fn_without_handle_returns_none(self):
        # No target_handle -> can't recover the parameter name -> safe
        # degrade (reconnect creates a fresh manual edge instead of
        # auto-unhiding) rather than guessing.
        assert candidate_edge_id("pathInput__mypath", self.F_NODE) is None

    def test_fn_to_var(self):
        assert (
            candidate_edge_id(self.F_NODE, "var__Out") == f"e__f__{self.F_CID}__Out"
        )

    def test_matches_build_edges_own_id_construction(self):
        # candidate_edge_id must never drift from what build_edges actually
        # produces — round-trip through a real build_edges call.
        f_key = ("f", self.F_CID)
        edges = build_edges(
            fn_input_params={f_key: {"signal": "Raw"}},
            fn_outputs={},
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
        )
        real_id = next(e["id"] for e in edges if e["source"] == "var__Raw")
        assert candidate_edge_id("var__Raw", self.F_NODE) == real_id

    def test_path_input_matches_build_edges_own_id_construction(self):
        f_key = ("f", self.F_CID)
        edges = build_edges(
            fn_input_params={},
            fn_outputs={},
            const_fns={},
            path_inputs={
                "mypath": {
                    "template": "",
                    "root_folder": None,
                    "functions": {(f_key, "filepath")},
                }
            },
            manual_edges=[],
            hidden_ids=set(),
        )
        real_id = next(e["id"] for e in edges if e["source"] == "pathInput__mypath")
        assert (
            candidate_edge_id("pathInput__mypath", self.F_NODE, "in__filepath")
            == real_id
        )

    def test_placement_qualified_target_stripped(self):
        placed = f"{self.F_NODE}::main"
        assert (
            candidate_edge_id("var__Raw", placed) == f"e__Raw__f__{self.F_CID}"
        )

    def test_two_opaque_ids_returns_none(self):
        assert candidate_edge_id("uuid-a", "uuid-b") is None

    def test_var_to_non_fn_target_returns_none(self):
        assert candidate_edge_id("var__Raw", "param__hz") is None

    def test_two_fn_nodes_returns_none(self):
        assert candidate_edge_id(self.F_NODE, f"fn__g__{_cid('g')}") is None


# ---------------------------------------------------------------------------
# build_manual_node
# ---------------------------------------------------------------------------


class TestBuildManualNode:
    def test_variable_node(self):
        n = build_manual_node(
            "uuid-1",
            {"type": "variableNode", "label": "MyVar", "config": None},
            pending_constants={},
            manual_fn_state=None,
            resolved_input_params=None,
            resolved_output_types=None,
            matlab_functions=set(),
        )
        assert n["id"] == "uuid-1"
        assert n["type"] == "variableNode"
        assert n["data"]["run_state"] == "red"
        assert n["data"]["total_records"] == 0

    def test_constant_node_with_pending(self):
        n = build_manual_node(
            "uuid-2",
            {"type": "parameterNode", "label": "hz", "config": None},
            pending_constants={"hz": {"42"}},
            manual_fn_state=None,
            resolved_input_params=None,
            resolved_output_types=None,
            matlab_functions=set(),
        )
        assert n["type"] == "parameterNode"
        vals = {v["value"] for v in n["data"]["values"]}
        assert "42" in vals

    def test_function_node(self):
        n = build_manual_node(
            "uuid-3",
            {"type": "functionNode", "label": "my_fn", "config": None},
            pending_constants={},
            manual_fn_state="pending",
            resolved_input_params={"signal": "Raw"},
            resolved_output_types=["Filtered"],
            matlab_functions=set(),
        )
        assert n["type"] == "functionNode"
        assert n["data"]["run_state"] == "pending"
        assert n["data"]["input_params"] == {"signal": "Raw"}
        assert n["data"]["output_types"] == ["Filtered"]

    def test_function_node_matlab_language(self):
        n = build_manual_node(
            "uuid-4",
            {"type": "functionNode", "label": "my_fn", "config": None},
            pending_constants={},
            manual_fn_state="red",
            resolved_input_params={},
            resolved_output_types=[],
            matlab_functions={"my_fn"},
        )
        assert n["data"]["language"] == "matlab"

    def test_path_input_node(self):
        n = build_manual_node(
            "uuid-5",
            {"type": "pathInputNode", "label": "mypath", "config": None},
            pending_constants={},
            manual_fn_state=None,
            resolved_input_params=None,
            resolved_output_types=None,
            matlab_functions=set(),
        )
        assert n["type"] == "pathInputNode"
        assert n["data"]["template"] == ""

    def test_function_node_default_state_red(self):
        n = build_manual_node(
            "uuid-6",
            {"type": "functionNode", "label": "fn", "config": None},
            pending_constants={},
            manual_fn_state=None,
            resolved_input_params={},
            resolved_output_types=[],
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
        """Graduation targets the manual node's OWN placement
        (canonical_id::pipeline_id), not the bare canonical id — this is
        what lets two same-label manual nodes in different scopes
        graduate independently instead of racing for one shared slot."""
        existing = [self._db_node("var__Raw", "variableNode", "Raw")]
        manual = {"uuid-old": {"type": "variableNode", "label": "Raw", "pipeline_id": "main"}}
        to_add, graduations = merge_manual_nodes(existing, manual, saved_positions={})
        assert "uuid-old" not in to_add
        assert len(graduations) == 1
        assert graduations[0].old_id == "uuid-old"
        assert graduations[0].new_id == "var__Raw::main"

    def test_graduation_skipped_if_own_placement_already_exists(self):
        # If THIS scope's placement is already positioned, do not re-graduate.
        existing = [self._db_node("var__Raw", "variableNode", "Raw")]
        manual = {"uuid-old": {"type": "variableNode", "label": "Raw", "pipeline_id": "main"}}
        to_add, graduations = merge_manual_nodes(
            existing, manual, saved_positions={"var__Raw::main": {"x": 10, "y": 20}}
        )
        assert "uuid-old" in to_add
        assert len(graduations) == 0

    def test_same_label_different_scopes_graduate_independently(self):
        """The core regression test for the placement rework: two manual
        nodes with the SAME label in DIFFERENT scopes (e.g. a duplicated
        pipeline re-running identical, unedited wiring) must both
        graduate to their OWN placement, not collide over one shared slot
        (the bug: an earlier design stole the position from whichever
        scope claimed it second)."""
        existing = [self._db_node("var__Raw", "variableNode", "Raw")]
        manual = {
            "uuid-a": {"type": "variableNode", "label": "Raw", "pipeline_id": "main"},
            "uuid-b": {"type": "variableNode", "label": "Raw", "pipeline_id": "pipe_dup"},
        }
        to_add, graduations = merge_manual_nodes(existing, manual, saved_positions={})
        assert to_add == []
        assert len(graduations) == 2
        new_ids = {g.new_id for g in graduations}
        assert new_ids == {"var__Raw::main", "var__Raw::pipe_dup"}

    def test_empty_manual_nodes(self):
        existing = [self._db_node("var__Raw", "variableNode", "Raw")]
        to_add, graduations = merge_manual_nodes(existing, {}, saved_positions={})
        assert to_add == []
        assert graduations == []

    def test_function_graduation_with_single_call_site(self):
        """One DB-derived call site → manual fn graduates to that call_id
        (as its own placement in its own scope)."""
        cid = _cid("only")
        existing = [self._db_node(f"fn__bp__{cid}", "functionNode", "bp")]
        manual = {"uuid-old": {"type": "functionNode", "label": "bp", "pipeline_id": "main"}}
        to_add, graduations = merge_manual_nodes(existing, manual, saved_positions={})
        assert "uuid-old" not in to_add
        assert len(graduations) == 1
        assert graduations[0].new_id == f"fn__bp__{cid}::main"

    def test_function_no_graduation_when_multiple_call_sites(self):
        """Multiple DB-derived call sites for the same fn → manual node stays
        independent (we cannot pick a canonical target unambiguously)."""
        cid_a, cid_b = _cid("a"), _cid("b")
        existing = [
            self._db_node(f"fn__bp__{cid_a}", "functionNode", "bp"),
            self._db_node(f"fn__bp__{cid_b}", "functionNode", "bp"),
        ]
        manual = {"uuid-old": {"type": "functionNode", "label": "bp"}}
        to_add, graduations = merge_manual_nodes(existing, manual, saved_positions={})
        assert "uuid-old" in to_add
        assert graduations == []


# ---------------------------------------------------------------------------
# MATLAB param-name handles (Fix B)
# ---------------------------------------------------------------------------


class TestMatlabParamNameHandles:
    """Exercises the path where MATLAB param names differ from Variable class
    names (e.g. ``output1 → Result``), to prove the graph_builder uses the
    explicit mapping rather than any naming convention."""

    EX_CID = _cid("fn_ex-call")
    EX_KEY = ("fn_ex", EX_CID)
    EX_NODE = f"fn__fn_ex__{EX_CID}"

    def _make_nodes(self, **overrides):
        defaults = {
            "fn_input_params": {self.EX_KEY: {}},
            "fn_outputs": {self.EX_KEY: {"Result"}},
            "fn_constants": {self.EX_KEY: set()},
            "fn_variants_map": {self.EX_KEY: []},
            "fn_params_map": {"fn_ex": []},
            "run_states": {},
            "matlab_functions": {"fn_ex"},
            "saved_configs": {"fn_ex": None},
            "matlab_output_order": {"fn_ex": ["output1"]},
            "matlab_param_to_class": {"fn_ex": {"output1": "Result"}},
        }
        defaults.update(overrides)
        return build_function_nodes(**defaults)

    def test_handles_use_param_name_not_class_name(self):
        nodes = self._make_nodes()
        assert nodes[0]["data"]["output_types"] == ["output1"]

    def test_edges_use_param_name_in_source_handle(self):
        edges = build_edges(
            fn_input_params={},
            fn_outputs={self.EX_KEY: {"Result"}},
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
            matlab_param_to_class={"fn_ex": {"output1": "Result"}},
        )
        fn_to_var = [e for e in edges if e.get("source") == self.EX_NODE]
        assert len(fn_to_var) == 1
        assert fn_to_var[0]["target"] == "var__Result"
        assert fn_to_var[0]["sourceHandle"] == "out__output1"

    def test_multi_output_preserves_signature_order(self):
        cid = _cid("load_csv-call")
        key = ("load_csv", cid)
        nodes = build_function_nodes(
            fn_input_params={key: {}},
            fn_outputs={key: {"Time", "Force_Left", "Force_Right"}},
            fn_constants={key: set()},
            fn_variants_map={key: []},
            fn_params_map={"load_csv": []},
            run_states={},
            matlab_functions={"load_csv"},
            saved_configs={"load_csv": None},
            matlab_output_order={"load_csv": ["time", "force_left", "force_right"]},
            matlab_param_to_class={
                "load_csv": {
                    "time": "Time",
                    "force_left": "Force_Left",
                    "force_right": "Force_Right",
                }
            },
        )
        assert nodes[0]["data"]["output_types"] == ["time", "force_left", "force_right"]

    def test_non_matlab_fn_unaffected(self):
        cid = _cid("py-call")
        key = ("py_fn", cid)
        edges = build_edges(
            fn_input_params={},
            fn_outputs={key: {"Out"}},
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
            matlab_param_to_class={},
        )
        node_id = f"fn__py_fn__{cid}"
        fn_to_var = [e for e in edges if e.get("source") == node_id]
        assert fn_to_var[0]["sourceHandle"] == "out__Out"

    def test_unmapped_class_falls_back_to_class_handle(self):
        edges = build_edges(
            fn_input_params={},
            fn_outputs={self.EX_KEY: {"Unmapped"}},
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
            matlab_param_to_class={"fn_ex": {}},
        )
        fn_to_var = [e for e in edges if e.get("source") == self.EX_NODE]
        assert fn_to_var[0]["sourceHandle"] == "out__Unmapped"


# ---------------------------------------------------------------------------
# Wiring grouping (group_call_sites_by_wiring + migration helpers)
# ---------------------------------------------------------------------------

from scistack_gui.domain.graph_builder import (  # noqa: E402
    group_call_sites_by_wiring,
    legacy_edge_rewrites,
    legacy_position_adoptions,
    wiring_id,
)


def _two_site_agg():
    """Two call sites of one fn differing ONLY in a constant value."""
    agg = AggregatedData()
    a = ("bp", "a" * 16)
    b = ("bp", "b" * 16)
    for fkey, hz in ((a, 20), (b, 50)):
        agg.fn_input_params[fkey] = {"signal": "Raw"}
        agg.fn_outputs[fkey] = {"Filtered"}
        agg.fn_constants[fkey] = {"low_hz"}
        agg.fn_variants_map[fkey] = [{"constants": {"low_hz": hz}}]
    agg.const_fns["low_hz"] = {a, b}
    agg.all_var_types = {"Raw", "Filtered"}
    return agg, a, b


class TestWiringId:
    def test_deterministic_and_constant_independent(self):
        wid = wiring_id("bp", {"signal": "Raw"}, {"Filtered"}, {})
        assert wid == wiring_id("bp", {"signal": "Raw"}, {"Filtered"}, {})
        assert len(wid) == 16

    def test_different_wiring_different_id(self):
        base = wiring_id("bp", {"signal": "Raw"}, {"Filtered"}, {})
        assert wiring_id("bp", {"signal": "Other"}, {"Filtered"}, {}) != base
        assert wiring_id("bp", {"signal": "Raw"}, {"Smoothed"}, {}) != base
        assert wiring_id("other", {"signal": "Raw"}, {"Filtered"}, {}) != base


class TestWiringIdPathInputNormalisation:
    """The canvas hashes ``AggregatedData.fn_input_params`` (PathInputs already
    partitioned out); the run path hashes a raw variant's ``input_types``
    (PathInput spec still in it). When those disagreed, a graduated
    PathInput-fed node hashed one way on the canvas and another in
    derive_target_for_node, matched no history, and reported "No pipeline
    history or output connections found" for a green, fully wired, already-run
    node. Both views must hash identically.
    """

    PI_JSON = json.dumps(
        {
            "__type": "PathInput",
            "template": "{subject}/{subject}_{session}_CPET.csv",
            "root_folder": "examples/vo2max/data",
        }
    )
    PI_LEGACY = "PathInput('{subject}/raw.csv', root_folder=PosixPath('/data'))"

    def test_raw_and_partitioned_views_agree(self):
        canvas = wiring_id("read_csv", {}, {"cpet_data_raw"}, {"fp": "test_pi"})
        run_path = wiring_id(
            "read_csv", {"fp": self.PI_JSON}, {"cpet_data_raw"}, {"fp": "test_pi"}
        )
        assert canvas == run_path

    def test_agreement_holds_with_variable_inputs_alongside(self):
        canvas = wiring_id("process", {"signal": "Raw"}, {"Out"}, {"fp": "p"})
        run_path = wiring_id(
            "process", {"signal": "Raw", "fp": self.PI_JSON}, {"Out"}, {"fp": "p"}
        )
        assert canvas == run_path

    def test_legacy_repr_spec_also_stripped(self):
        canvas = wiring_id("load", {}, {"Out"}, {"fp": "p"})
        assert wiring_id("load", {"fp": self.PI_LEGACY}, {"Out"}, {"fp": "p"}) == canvas

    def test_path_input_term_still_discriminates(self):
        """Stripping the spec must not collapse two call sites fed by
        DIFFERENT PathInputs — that's what the path_inputs term is for."""
        a = wiring_id("load", {"fp": self.PI_JSON}, {"Out"}, {"fp": "pi_a"})
        b = wiring_id("load", {"fp": self.PI_JSON}, {"Out"}, {"fp": "pi_b"})
        assert a != b

    def test_variable_inputs_are_never_stripped(self):
        assert strip_path_input_params({"signal": "Raw", "fp": self.PI_JSON}) == {
            "signal": "Raw"
        }

    def test_non_string_values_survive(self):
        """Multi-type inputs arrive as lists; they must not trip the parser."""
        params = {"signal": ["Raw", "Filtered"], "n": 5, "missing": None}
        assert strip_path_input_params(params) == params

    def test_canvas_ids_are_unchanged_by_the_normalisation(self):
        """The canvas side never had a spec to strip, so no stored node id,
        saved position or scope membership may shift."""
        params = {"signal": "Raw"}
        assert strip_path_input_params(params) == params


class TestGroupCallSitesByWiring:
    def test_same_wiring_groups_to_one_key(self):
        agg, a, b = _two_site_agg()
        states = {
            fn_node_id(*a): "green",
            fn_node_id(*b): "red",
            "var__Filtered": "red",
        }

        grouped, node_states, member_map = group_call_sites_by_wiring(agg, states)

        wid = wiring_id("bp", {"signal": "Raw"}, {"Filtered"}, {})
        gkey = ("bp", wid)
        assert list(grouped.fn_input_params) == [gkey]
        # Variant rows keep per-call-site call_id and state.
        rows = grouped.fn_variants_map[gkey]
        by_hz = {r["constants"]["low_hz"]: r for r in rows}
        assert by_hz[20]["state"] == "green" and by_hz[20]["call_id"] == "a" * 16
        assert by_hz[50]["state"] == "red" and by_hz[50]["call_id"] == "b" * 16
        # Node state = worst member; var states pass through.
        assert node_states[fn_node_id("bp", wid)] == "red"
        assert node_states["var__Filtered"] == "red"
        # const edge targets re-keyed to the group.
        assert grouped.const_fns["low_hz"] == {gkey}
        # member map records both legacy ids.
        assert set(member_map[fn_node_id("bp", wid)]) == {
            fn_node_id(*a),
            fn_node_id(*b),
        }

    def test_different_wiring_stays_separate(self):
        agg = AggregatedData()
        k1 = ("bp", "a" * 16)
        k2 = ("bp", "b" * 16)
        agg.fn_input_params[k1] = {"signal": "Raw"}
        agg.fn_outputs[k1] = {"Filtered"}
        agg.fn_input_params[k2] = {"signal": "Other"}
        agg.fn_outputs[k2] = {"Filtered"}

        grouped, _, _ = group_call_sites_by_wiring(agg, {})

        assert len(grouped.fn_input_params) == 2

    def test_staged_pending_value_synthesizes_row_and_pending_state(self):
        agg, a, b = _two_site_agg()
        states = {fn_node_id(*a): "green", fn_node_id(*b): "green"}

        grouped, node_states, _ = group_call_sites_by_wiring(
            agg, states, pending_constants={"low_hz": {"99"}}
        )

        wid = wiring_id("bp", {"signal": "Raw"}, {"Filtered"}, {})
        rows = grouped.fn_variants_map[("bp", wid)]
        staged = [r for r in rows if r.get("staged")]
        assert staged == [
            {"constants": {"low_hz": "99"}, "state": "pending", "staged": True}
        ]
        # A green group with a staged value downgrades to pending, not red.
        assert node_states[fn_node_id("bp", wid)] == "pending"

    def test_group_already_covering_staged_value_stays_green(self):
        """Regression for the bug where running one of two compute_rolling_vo2
        nodes (sharing a constant, different signals) left it stuck on
        "pending" until the sibling node was also (re)run. Once a group's
        OWN real member already recorded the newly-staged value, that
        group must not get a synthesized pending row for it — only the
        sibling wiring that hasn't run it yet should."""
        vo2 = ("compute_rolling_vo2", "a" * 16)
        hr = ("compute_rolling_vo2", "b" * 16)
        agg = AggregatedData()
        agg.fn_input_params[vo2] = {"signal": "RawVO2"}
        agg.fn_outputs[vo2] = {"RollingVO2"}
        agg.fn_constants[vo2] = {"window_seconds"}
        agg.fn_variants_map[vo2] = [{"constants": {"window_seconds": "25"}}]
        agg.fn_input_params[hr] = {"signal": "RawHeartRate"}
        agg.fn_outputs[hr] = {"RollingHR"}
        agg.fn_constants[hr] = {"window_seconds"}
        agg.fn_variants_map[hr] = [{"constants": {"window_seconds": "30"}}]
        agg.const_fns["window_seconds"] = {vo2, hr}
        states = {fn_node_id(*vo2): "green", fn_node_id(*hr): "green"}

        grouped, node_states, _ = group_call_sites_by_wiring(
            agg, states, pending_constants={"window_seconds": {"25"}}
        )

        vo2_wid = wiring_id(
            "compute_rolling_vo2", {"signal": "RawVO2"}, {"RollingVO2"}, {}
        )
        hr_wid = wiring_id(
            "compute_rolling_vo2", {"signal": "RawHeartRate"}, {"RollingHR"}, {}
        )
        vo2_rows = grouped.fn_variants_map[("compute_rolling_vo2", vo2_wid)]
        hr_rows = grouped.fn_variants_map[("compute_rolling_vo2", hr_wid)]
        # VO2 already ran 25 -> no synthesized staged row, node stays green.
        assert not any(r.get("staged") for r in vo2_rows)
        assert node_states[fn_node_id("compute_rolling_vo2", vo2_wid)] == "green"
        # HR hasn't run 25 -> gets the staged row and downgrades to pending.
        assert any(r.get("staged") for r in hr_rows)
        assert node_states[fn_node_id("compute_rolling_vo2", hr_wid)] == "pending"


class TestLegacyMigrationHelpers:
    GROUP = fn_node_id("bp", "c" * 16)
    LEGACY_A = fn_node_id("bp", "a" * 16)
    LEGACY_B = fn_node_id("bp", "b" * 16)

    def test_first_member_position_adopted_others_dropped(self):
        member_map = {self.GROUP: [self.LEGACY_A, self.LEGACY_B]}
        positions = {
            "main": {self.LEGACY_A: {"x": 1, "y": 2}, self.LEGACY_B: {"x": 3, "y": 4}}
        }

        adoptions, drops = legacy_position_adoptions(member_map, positions)

        assert adoptions == [{"new_id": self.GROUP, "scope": "main", "x": 1, "y": 2}]
        assert set(drops) == {self.LEGACY_A, self.LEGACY_B}

    def test_scope_of_adopted_position_is_kept(self):
        member_map = {self.GROUP: [self.LEGACY_A]}
        positions = {"pipe_sub": {self.LEGACY_A: {"x": 5, "y": 6}}}

        adoptions, _ = legacy_position_adoptions(member_map, positions)

        assert adoptions[0]["scope"] == "pipe_sub"

    def test_already_placed_group_only_drops_legacy_keys(self):
        member_map = {self.GROUP: [self.LEGACY_A]}
        positions = {
            "main": {self.GROUP: {"x": 9, "y": 9}, self.LEGACY_A: {"x": 1, "y": 2}}
        }

        adoptions, drops = legacy_position_adoptions(member_map, positions)

        assert adoptions == []
        assert drops == [self.LEGACY_A]

    def test_manual_edges_rewritten_to_group_id(self):
        member_map = {self.GROUP: [self.LEGACY_A]}
        edges = [
            {"id": "m1", "source": "var__Raw", "target": self.LEGACY_A},
            {"id": "m2", "source": self.LEGACY_A, "target": "var__Filtered"},
            {"id": "m3", "source": "var__Raw", "target": "var__Filtered"},
        ]

        rewrites = legacy_edge_rewrites(member_map, edges)

        assert {r["id"] for r in rewrites} == {"m1", "m2"}
        assert all(self.GROUP in (r["source"], r["target"]) for r in rewrites)


# ---------------------------------------------------------------------------
# find_cycle
# ---------------------------------------------------------------------------


class TestFindCycle:
    def test_no_edges_no_cycle(self):
        assert find_cycle([], "a", "b") is None

    def test_self_loop_is_a_cycle(self):
        assert find_cycle([], "a", "a") == ["a", "a"]

    def test_disjoint_edges_no_cycle(self):
        edges = [{"source": "a", "target": "b"}, {"source": "c", "target": "d"}]
        assert find_cycle(edges, "b", "c") is None

    def test_direct_two_node_cycle(self):
        # a -> b already exists; adding b -> a would close it.
        edges = [{"source": "a", "target": "b"}]
        path = find_cycle(edges, "b", "a")
        assert path == ["b", "a", "b"]

    def test_transitive_cycle_detected(self):
        # a -> b -> c already exists; adding c -> a would close it.
        edges = [
            {"source": "a", "target": "b"},
            {"source": "b", "target": "c"},
        ]
        path = find_cycle(edges, "c", "a")
        assert path == ["c", "a", "b", "c"]

    def test_reverse_direction_is_not_a_cycle(self):
        # a -> b exists; adding a -> b again (or a totally different forward
        # edge) is not a cycle just because both touch the same nodes.
        edges = [{"source": "a", "target": "b"}]
        assert find_cycle(edges, "a", "c") is None

    def test_mixed_db_derived_and_manual_edges(self):
        # Real DB-derived data-wiring: fn output feeds a variable that's
        # already consumed as this fn's own input (data-lineage edge). A
        # NEW manual edge from that variable back to the function's input
        # would close the loop even though no OTHER manual edge is involved.
        edges = [
            {"source": "fn__f__abc123", "target": "var__Out"},
            {"source": "var__In", "target": "fn__f__abc123"},
        ]
        path = find_cycle(edges, "var__Out", "var__In")
        assert path == ["var__Out", "var__In", "fn__f__abc123", "var__Out"]


class TestMatlabOutputHandleInvariant:
    """A fn→var edge's ``sourceHandle`` must exist on its source node.

    This is the invariant that actually protects the canvas. React Flow
    silently drops an edge whose ``sourceHandle`` names no handle on the
    source node — no error, no warning, the wire just isn't drawn — while
    ``propagate_run_states`` still marks the target variable green because
    states propagate over node ids, not handles. The result is a function
    that looks disconnected from an output it demonstrably produced.

    ``build_function_nodes`` names a MATLAB fn's output handles after its
    SIGNATURE params (``out__loaded_data``), while ``build_edges`` falls back
    to the CLASS name (``out__RawEMG``) whenever ``matlab_param_to_class``
    has no entry. Assert the two agree rather than testing either alone.
    """

    CID = "076c46199b238a69"
    KEY = ("loadDelsysEMGOneFile", CID)
    P2C = {"loadDelsysEMGOneFile": {"loaded_data": "RawEMG"}}

    def _nodes_and_edges(self, p2c):
        from scistack_gui.domain.graph_builder import (
            build_edges,
            build_function_nodes,
        )

        common = dict(
            fn_input_params={self.KEY: {}},
            fn_outputs={self.KEY: {"RawEMG"}},
        )
        nodes = build_function_nodes(
            fn_constants={self.KEY: set()},
            fn_variants_map={self.KEY: []},
            fn_params_map={"loadDelsysEMGOneFile": ["emgFilePath"]},
            run_states={f"fn__loadDelsysEMGOneFile__{self.CID}": "green"},
            matlab_functions={"loadDelsysEMGOneFile"},
            saved_configs={"loadDelsysEMGOneFile": None},
            matlab_output_order={"loadDelsysEMGOneFile": ["loaded_data"]},
            matlab_param_to_class=p2c,
            **common,
        )
        edges = build_edges(
            const_fns={},
            path_inputs={},
            manual_edges=[],
            hidden_ids=set(),
            matlab_param_to_class=p2c,
            **common,
        )
        return nodes, edges

    def test_edge_source_handles_exist_on_their_source_node(self):
        nodes, edges = self._nodes_and_edges(self.P2C)

        handles = {
            n["id"]: {f"out__{t}" for t in n["data"]["output_types"]} for n in nodes
        }
        for e in edges:
            sh = e.get("sourceHandle")
            if sh is None or not sh.startswith("out__") or e["source"] not in handles:
                continue
            assert sh in handles[e["source"]], (
                f"edge {e['id']} points at {sh!r}, but node {e['source']} renders "
                f"{sorted(handles[e['source']])} — React Flow will drop this edge "
                f"silently and the canvas will show the function disconnected"
            )

    def test_param_to_class_is_what_makes_them_agree(self):
        """Pin the mechanism, so a regression says *why* it broke.

        With the mapping the node and edge both speak ``out__loaded_data``.
        Without it they diverge — which is the state a placement-id mismatch
        in ``infer_manual_fn_param_to_class`` used to produce.
        """
        nodes, edges = self._nodes_and_edges(self.P2C)
        assert nodes[0]["data"]["output_types"] == ["loaded_data"]
        assert edges[0]["sourceHandle"] == "out__loaded_data"

        nodes, edges = self._nodes_and_edges({})
        assert nodes[0]["data"]["output_types"] == ["loaded_data"]
        assert edges[0]["sourceHandle"] == "out__RawEMG"


class TestSavedConfigRehydration:
    """A saved setting has to come BACK on the next graph build.

    Config is keyed by node_id in _node_config. The fn_name-keyed
    ``saved_configs`` map is built from manual nodes only, so it can never
    answer for a node that has already run -- and a function that has run is
    exactly the case where the user has settings worth keeping. Without the
    node_id lookup every toggle silently reset on rebuild, which is how the
    distribute checkbox appeared not to save (2026-09-14).

    See docs/claude/gui-run-options-flow.md.
    """

    CID = _cid("rehydrate")
    KEY = ("bandpass", CID)
    NODE = f"fn__bandpass__{CID}"

    def _make(self, **overrides):
        from scistack_gui.domain.graph_builder import build_function_nodes

        defaults = {
            "fn_input_params": {self.KEY: {"signal": "Raw"}},
            "fn_outputs": {self.KEY: {"Filtered"}},
            "fn_constants": {self.KEY: {"hz"}},
            "fn_variants_map": {self.KEY: []},
            "fn_params_map": {"bandpass": ["signal", "hz"]},
            "run_states": {},
            "matlab_functions": set(),
            "saved_configs": {"bandpass": None},
        }
        defaults.update(overrides)
        return build_function_nodes(**defaults)

    def test_node_id_config_rehydrates_run_options(self):
        nodes = self._make(
            node_configs={self.NODE: {"runOptions": {"distribute": True}}}
        )
        assert nodes[0]["data"]["runOptions"] == {"distribute": True}

    def test_node_id_config_beats_the_fn_name_fallback(self):
        """Two call sites of one function can hold different settings; the
        fn_name map cannot express that, so node_id has to win."""
        nodes = self._make(
            saved_configs={"bandpass": {"runOptions": {"distribute": False}}},
            node_configs={self.NODE: {"runOptions": {"distribute": True}}},
        )
        assert nodes[0]["data"]["runOptions"] == {"distribute": True}

    def test_fn_name_fallback_still_applies_when_no_node_config(self):
        nodes = self._make(
            saved_configs={"bandpass": {"runOptions": {"distribute": True}}},
            node_configs={},
        )
        assert nodes[0]["data"]["runOptions"] == {"distribute": True}

    def test_every_key_the_panel_writes_is_rehydrated(self):
        """Rehydration must cover the same set FunctionSettingsPanel persists,
        or a setting saves and comes back missing -- indistinguishable, to the
        user, from not saving at all."""
        cfg = {
            "schemaSelection": {"exclude_levels": {"subject": ["SS01"]}},
            "schemaLevel": ["subject", "session"],
            "whereFilters": [{"variable": "x", "op": "==", "value": "1"}],
            "runOptions": {"distribute": True, "save": False},
        }
        data = self._make(node_configs={self.NODE: cfg})[0]["data"]
        for key, value in cfg.items():
            assert data[key] == value

    def test_absent_config_leaves_no_keys_behind(self):
        data = self._make(node_configs={})[0]["data"]
        assert "runOptions" not in data
        assert "schemaSelection" not in data


class TestPlacementQualifiedConfigRehydration:
    """The id the frontend saves config under is the FINAL id it sees --
    ``fn__{fn}__{cid}::{scope}`` for a node with a placement-qualified
    position -- but build_function_nodes runs before scope resolution and
    looks configs up by the bare id. apply_placement_configs is the second
    pass that closes that gap on the resolved node list.

    Symptom without it (2026-09-14, loadGaitRiteOneFile): every toggle
    writes fine (``update_node_config ... keys=['schemaLevel']`` in
    scidb.log), the DuckDB file watcher fires dag_updated ~2 s later, the
    refetched node carries no config, and the checkbox snaps back.

    See docs/claude/placement-qualified-ids.md.
    """

    CID = _cid("placed")
    KEY = ("loadGaitRiteOneFile", CID)
    BARE = f"fn__loadGaitRiteOneFile__{CID}"
    QUALIFIED = f"{BARE}::main"

    def _built(self, node_configs):
        from scistack_gui.domain.graph_builder import build_function_nodes

        return build_function_nodes(
            fn_input_params={self.KEY: {"path": "GaitRitePath"}},
            fn_outputs={self.KEY: {"GAITRiteLoaded"}},
            fn_constants={self.KEY: set()},
            fn_variants_map={self.KEY: []},
            fn_params_map={"loadGaitRiteOneFile": ["path"]},
            run_states={},
            matlab_functions={"loadGaitRiteOneFile"},
            saved_configs={"loadGaitRiteOneFile": None},
            node_configs=node_configs,
        )

    def _resolved(self, node_configs, scope="main"):
        """Build, then scope-resolve exactly as api/pipeline._build_graph
        does, with a placement-qualified position for the node."""
        from scistack_gui.domain.scope_filter import resolve_scope_view

        nodes = self._built(node_configs)
        positions_by_scope = {scope: {self.QUALIFIED: {"x": 0, "y": 0}}}
        nodes, _edges = resolve_scope_view(
            nodes, [], scope, manual_nodes={}, positions_by_scope=positions_by_scope
        )
        return nodes

    def test_resolved_id_is_placement_qualified(self):
        """Precondition for everything below: the canvas really does see the
        qualified id, so that is what it saves under."""
        nodes = self._resolved({})
        assert [n["id"] for n in nodes] == [self.QUALIFIED]

    def test_bare_lookup_alone_misses_a_qualified_config(self):
        """Documents the gap the second pass exists for: the config the
        frontend wrote is invisible to build_function_nodes."""
        nodes = self._resolved(
            {self.QUALIFIED: {"schemaLevel": ["subject", "session", "speed"]}}
        )
        assert "schemaLevel" not in nodes[0]["data"]

    def test_apply_placement_configs_rehydrates_qualified_config(self):
        from scistack_gui.domain.graph_builder import apply_placement_configs

        cfg = {
            "schemaLevel": ["subject", "session", "speed"],
            "runOptions": {"distribute": True, "save": True},
        }
        node_configs = {self.QUALIFIED: cfg}
        nodes = self._resolved(node_configs)
        applied = apply_placement_configs(nodes, node_configs)
        assert applied == 1
        assert nodes[0]["data"]["schemaLevel"] == cfg["schemaLevel"]
        assert nodes[0]["data"]["runOptions"] == cfg["runOptions"]

    def test_qualified_config_beats_bare_config(self):
        """Independent placements of one wiring may hold different settings;
        the id the user actually toggled under has to win."""
        from scistack_gui.domain.graph_builder import apply_placement_configs

        node_configs = {
            self.BARE: {"runOptions": {"distribute": False}},
            self.QUALIFIED: {"runOptions": {"distribute": True}},
        }
        nodes = self._resolved(node_configs)
        assert nodes[0]["data"]["runOptions"] == {"distribute": False}  # bare pass
        apply_placement_configs(nodes, node_configs)
        assert nodes[0]["data"]["runOptions"] == {"distribute": True}

    def test_bare_config_survives_when_no_qualified_config(self):
        """A node placed bare-then-graduated keeps its pre-graduation config
        until the user saves again under the new id."""
        from scistack_gui.domain.graph_builder import apply_placement_configs

        node_configs = {self.BARE: {"runOptions": {"distribute": True}}}
        nodes = self._resolved(node_configs)
        assert apply_placement_configs(nodes, node_configs) == 0
        assert nodes[0]["data"]["runOptions"] == {"distribute": True}

    def test_other_scopes_config_is_not_applied_here(self):
        from scistack_gui.domain.graph_builder import apply_placement_configs

        node_configs = {f"{self.BARE}::hyp1": {"runOptions": {"distribute": True}}}
        nodes = self._resolved(node_configs, scope="main")
        # Viewing main: the node is bare-placed nowhere and qualified only in
        # hyp1's config, so its main view has no config at all.
        assert apply_placement_configs(nodes, node_configs) == 0
        assert "runOptions" not in nodes[0]["data"]

    def test_orphan_config_is_warned_not_silently_ignored(self, caplog):
        """A config keyed by an id no node in the graph carries -- by exact
        OR bare id -- is the signature of an id-shape mismatch. It has to be
        visible in scidb.log; this class of bug is otherwise silent."""
        import logging

        from scistack_gui.domain.graph_builder import apply_placement_configs

        node_configs = {"fn__someOtherFn__deadbeef::main": {"runOptions": {}}}
        nodes = self._resolved(node_configs)
        with caplog.at_level(logging.WARNING, logger="scistack_gui.domain.graph_builder"):
            apply_placement_configs(nodes, node_configs)
        assert any(
            "match no node in the resolved graph" in r.getMessage()
            and "fn__someOtherFn__deadbeef::main" in r.getMessage()
            for r in caplog.records
        )

    def test_config_for_a_graph_node_is_not_an_orphan(self, caplog):
        """Neither a qualified nor a bare key for a present node may trip the
        orphan WARN, or the log fills with noise and the real one is missed."""
        import logging

        from scistack_gui.domain.graph_builder import apply_placement_configs

        node_configs = {
            self.QUALIFIED: {"runOptions": {}},
            self.BARE: {"runOptions": {}},
            f"{self.BARE}::hyp1": {"runOptions": {}},
        }
        nodes = self._resolved(node_configs)
        with caplog.at_level(logging.WARNING, logger="scistack_gui.domain.graph_builder"):
            apply_placement_configs(nodes, node_configs)
        assert not [r for r in caplog.records if "match no node" in r.getMessage()]


class TestManualInputOverrides:
    """graph_builder.manual_input_overrides — the one owner of "the edges
    visible on the DAG are the ground truth" for a history node's inputs
    (docs/claude/manual-edges-on-history-nodes.md)."""

    FN = "grSides"

    def _wid(self, inputs, outputs=("GRTable",)):
        from scistack_gui.domain.graph_builder import wiring_id

        return wiring_id(self.FN, inputs, set(outputs), {})

    def _index(self, wid, *edges):
        from scistack_gui.domain.graph_builder import fn_node_id, manual_edge_handle_index

        return manual_edge_handle_index(
            [
                {
                    "id": f"manual__{i}",
                    "source": src,
                    "target": fn_node_id(self.FN, wid) + suffix,
                    "targetHandle": handle,
                }
                for i, (src, handle, suffix) in enumerate(edges)
            ]
        )

    def test_unbound_param_takes_the_manual_variable(self):
        from scistack_gui.domain.graph_builder import manual_input_overrides

        history = {"gr": "GAITRiteLoaded"}
        wid = self._wid(history)
        index = self._index(wid, ("var__Demographics", "in__side", ""))
        assert manual_input_overrides(self.FN, wid, history, set(), index) == {
            "side": "Demographics"
        }

    def test_placement_suffixes_on_both_endpoints_are_stripped(self):
        from scistack_gui.domain.graph_builder import manual_input_overrides

        history = {"gr": "GAITRiteLoaded"}
        wid = self._wid(history)
        index = self._index(wid, ("var__Demographics::main", "in__side", "::main"))
        assert manual_input_overrides(self.FN, wid, history, set(), index) == {
            "side": "Demographics"
        }

    def test_hidden_history_edge_plus_manual_edge_is_a_reconnect(self):
        from scistack_gui.domain.graph_builder import manual_input_overrides

        history = {"gr": "GAITRiteLoaded"}
        wid = self._wid(history)
        index = self._index(wid, ("var__OtherGR", "in__gr", ""))
        hidden = {f"e__GAITRiteLoaded__{self.FN}__{wid}"}
        assert manual_input_overrides(self.FN, wid, history, set(), index, None, hidden) == {
            "gr": "OtherGR"
        }

    def test_visible_history_edge_plus_manual_edge_is_each_of(self):
        # The same picture on a fresh node resolves to EachOf; a history
        # node must not silently ignore a wire the user can see.
        from scistack_gui.domain.graph_builder import manual_input_overrides

        history = {"gr": "GAITRiteLoaded"}
        wid = self._wid(history)
        index = self._index(wid, ("var__OtherGR", "in__gr", ""))
        assert manual_input_overrides(self.FN, wid, history, set(), index) == {
            "gr": ["GAITRiteLoaded", "OtherGR"]
        }

    def test_handle_without_a_manual_edge_is_left_alone(self):
        # Hidden-and-uncovered stays "disconnected" (reconcile's business),
        # and a visible history binding is simply not in the result.
        from scistack_gui.domain.graph_builder import manual_input_overrides

        history = {"gr": "GAITRiteLoaded", "other": "X"}
        wid = self._wid(history)
        index = self._index(wid, ("var__Demographics", "in__side", ""))
        hidden = {f"e__X__{self.FN}__{wid}"}
        assert manual_input_overrides(self.FN, wid, history, set(), index, None, hidden) == {
            "side": "Demographics"
        }

    def test_non_variable_sources_are_ignored(self):
        from scistack_gui.domain.graph_builder import manual_input_overrides

        history = {"gr": "GAITRiteLoaded"}
        wid = self._wid(history)
        index = self._index(
            wid,
            ("pathInput__files", "in__side", ""),
            ("param__low_hz", "in__low_hz", ""),
            ("fn__other__abc", "in__side2", ""),
        )
        assert manual_input_overrides(self.FN, wid, history, set(), index) == {}

    def test_manual_variable_node_source_resolves_via_manual_nodes(self):
        from scistack_gui.domain.graph_builder import manual_input_overrides

        history = {"gr": "GAITRiteLoaded"}
        wid = self._wid(history)
        index = self._index(wid, ("mv_demo", "in__side", ""))
        manual_nodes = {"mv_demo": {"type": "variableNode", "label": "Demographics"}}
        assert manual_input_overrides(self.FN, wid, history, set(), index, manual_nodes) == {
            "side": "Demographics"
        }

    def test_edges_on_other_wirings_do_not_leak(self):
        from scistack_gui.domain.graph_builder import manual_input_overrides

        history = {"gr": "GAITRiteLoaded"}
        wid = self._wid(history)
        other = self._wid({"gr": "Other"})
        index = self._index(other, ("var__Demographics", "in__side", ""))
        assert manual_input_overrides(self.FN, wid, history, set(), index) == {}


class TestOverlayManualInputs:
    FN = "grSides"

    def _graph(self, history, manual_edges, hidden=frozenset(), extra_nodes=()):
        from scistack_gui.domain.graph_builder import (
            collect_manual_input_overrides,
            fn_node_id,
            wiring_id,
        )

        wid = wiring_id(self.FN, history, {"GRTable"}, {})
        node_id = fn_node_id(self.FN, wid)
        fn_input_params = {(self.FN, wid): dict(history)}
        nodes = [
            {
                "id": node_id,
                "type": "functionNode",
                "data": {
                    "label": self.FN,
                    "input_params": {**history, "side": ""},
                    "constant_params": [],
                },
            },
            {"id": "var__Demographics", "type": "variableNode", "data": {}},
            *extra_nodes,
        ]
        edges = [
            {**e, "target": e["target"].replace("{node}", node_id)} for e in manual_edges
        ]
        overrides = collect_manual_input_overrides(
            nodes, fn_input_params, {(self.FN, wid): set()}, edges, {}, hidden
        )
        return nodes, node_id, wid, fn_input_params, overrides

    def test_overlay_fills_the_unbound_handle(self):
        from scistack_gui.domain.graph_builder import overlay_manual_inputs

        nodes, node_id, _, _, overrides = self._graph(
            {"gr": "GAITRiteLoaded"},
            [
                {
                    "id": "manual__1",
                    "source": "var__Demographics",
                    "target": "{node}::main",
                    "targetHandle": "in__side",
                }
            ],
        )
        assert overrides == {node_id: {"side": "Demographics"}}
        assert overlay_manual_inputs(nodes, overrides) == 1
        data = nodes[0]["data"]
        assert data["input_params"] == {"gr": "GAITRiteLoaded", "side": "Demographics"}
        assert data["manual_inputs"] == {"side": "Demographics"}

    def test_each_of_shows_first_source_and_keeps_the_list(self):
        # input_params stays {param: str} for every frontend consumer; the
        # full list rides in manual_inputs (same first-source choice the
        # fresh-node path makes).
        from scistack_gui.domain.graph_builder import overlay_manual_inputs

        nodes, node_id, _, _, overrides = self._graph(
            {"gr": "GAITRiteLoaded"},
            [
                {
                    "id": "manual__1",
                    "source": "var__Demographics",
                    "target": "{node}",
                    "targetHandle": "in__gr",
                }
            ],
        )
        overlay_manual_inputs(nodes, overrides)
        data = nodes[0]["data"]
        assert data["input_params"]["gr"] == "GAITRiteLoaded"
        assert data["manual_inputs"] == {"gr": ["GAITRiteLoaded", "Demographics"]}

    def test_no_manual_edges_touches_nothing(self):
        from scistack_gui.domain.graph_builder import overlay_manual_inputs

        nodes, _, _, _, overrides = self._graph({"gr": "GAITRiteLoaded"}, [])
        assert overrides == {}
        assert overlay_manual_inputs(nodes, overrides) == 0
        assert "manual_inputs" not in nodes[0]["data"]


class TestSupersededManualInputOverrides:
    FN = "grSides"

    def _wid(self, inputs):
        from scistack_gui.domain.graph_builder import wiring_id

        return wiring_id(self.FN, inputs, {"GRTable"}, {})

    def test_overlay_whose_wiring_has_run_moves_the_edge(self):
        from scistack_gui.domain.graph_builder import (
            fn_node_id,
            superseded_manual_input_overrides,
        )

        old_wid = self._wid({"gr": "GAITRiteLoaded"})
        new_wid = self._wid({"gr": "GAITRiteLoaded", "side": "Demographics"})
        old_id, new_id = fn_node_id(self.FN, old_wid), fn_node_id(self.FN, new_wid)
        fn_input_params = {
            (self.FN, old_wid): {"gr": "GAITRiteLoaded"},
            (self.FN, new_wid): {"gr": "GAITRiteLoaded", "side": "Demographics"},
        }
        fn_outputs = {(self.FN, old_wid): {"GRTable"}, (self.FN, new_wid): {"GRTable"}}
        edge = {
            "id": "manual__juqmgq",
            "source": "var__Demographics::main",
            "target": old_id + "::main",
            "targetHandle": "in__side",
        }
        rewrites, superseded = superseded_manual_input_overrides(
            {old_id: {"side": "Demographics"}}, fn_input_params, fn_outputs, {}, [edge]
        )
        assert superseded == {old_id: new_id}
        assert rewrites == [{**edge, "target": new_id + "::main"}], (
            "placement suffix preserved; the rewritten edge now duplicates the "
            "DB-derived edge on the new node and build_edges' dedup folds it"
        )

    def test_overlay_whose_wiring_has_not_run_is_kept(self):
        from scistack_gui.domain.graph_builder import (
            fn_node_id,
            superseded_manual_input_overrides,
        )

        old_wid = self._wid({"gr": "GAITRiteLoaded"})
        old_id = fn_node_id(self.FN, old_wid)
        fn_input_params = {(self.FN, old_wid): {"gr": "GAITRiteLoaded"}}
        edge = {
            "id": "manual__1",
            "source": "var__Demographics",
            "target": old_id,
            "targetHandle": "in__side",
        }
        rewrites, superseded = superseded_manual_input_overrides(
            {old_id: {"side": "Demographics"}},
            fn_input_params,
            {(self.FN, old_wid): {"GRTable"}},
            {},
            [edge],
        )
        assert rewrites == [] and superseded == {}

    def test_each_of_overlay_is_superseded_by_the_manual_variables_wiring(self):
        # An EachOf run records one wiring per source; the history source's
        # wiring is this node already, so the wiring to look for is history
        # with the MANUAL edge's variable.
        from scistack_gui.domain.graph_builder import (
            fn_node_id,
            superseded_manual_input_overrides,
        )

        old_wid = self._wid({"gr": "GAITRiteLoaded"})
        new_wid = self._wid({"gr": "OtherGR"})
        old_id, new_id = fn_node_id(self.FN, old_wid), fn_node_id(self.FN, new_wid)
        fn_input_params = {
            (self.FN, old_wid): {"gr": "GAITRiteLoaded"},
            (self.FN, new_wid): {"gr": "OtherGR"},
        }
        fn_outputs = {(self.FN, old_wid): {"GRTable"}, (self.FN, new_wid): {"GRTable"}}
        edge = {
            "id": "manual__1",
            "source": "var__OtherGR",
            "target": old_id,
            "targetHandle": "in__gr",
        }
        rewrites, superseded = superseded_manual_input_overrides(
            {old_id: {"gr": ["GAITRiteLoaded", "OtherGR"]}},
            fn_input_params,
            fn_outputs,
            {},
            [edge],
        )
        assert superseded == {old_id: new_id}
        assert rewrites[0]["target"] == new_id
