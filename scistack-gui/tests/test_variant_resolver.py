"""
Unit tests for scistack_gui.domain.variant_resolver.

All functions are pure — no DB or fixtures required.
"""

import pytest

from scistack_gui.domain.variant_resolver import (
    build_inferred_variants,
    build_schema_kwargs,
    deduplicate_variants,
    filter_variants,
    merge_pending_constants,
)


# ---------------------------------------------------------------------------
# build_inferred_variants
# ---------------------------------------------------------------------------

class TestBuildInferredVariants:
    def test_no_constants_single_output(self):
        result = build_inferred_variants(
            input_types={"signal": ["RawEMG"]},
            output_types=["Filtered"],
            inferred_constants={},
        )
        assert result == [
            {"input_types": {"signal": ["RawEMG"]}, "output_type": "Filtered", "constants": {}}
        ]

    def test_no_constants_multiple_outputs(self):
        result = build_inferred_variants(
            input_types={"signal": ["Raw"]},
            output_types=["A", "B"],
            inferred_constants={},
        )
        assert len(result) == 2
        output_types = {v["output_type"] for v in result}
        assert output_types == {"A", "B"}
        assert all(v["constants"] == {} for v in result)

    def test_single_constant_cross_products_with_outputs(self):
        result = build_inferred_variants(
            input_types={},
            output_types=["Out"],
            inferred_constants={"low_hz": [10, 20]},
        )
        assert len(result) == 2
        constant_values = {v["constants"]["low_hz"] for v in result}
        assert constant_values == {10, 20}

    def test_two_constants_full_cross_product(self):
        result = build_inferred_variants(
            input_types={},
            output_types=["Out"],
            inferred_constants={"a": [1, 2], "b": ["x", "y"]},
        )
        assert len(result) == 4
        combos = {(v["constants"]["a"], v["constants"]["b"]) for v in result}
        assert combos == {(1, "x"), (1, "y"), (2, "x"), (2, "y")}

    def test_constants_with_multiple_outputs(self):
        result = build_inferred_variants(
            input_types={},
            output_types=["A", "B"],
            inferred_constants={"k": [1, 2]},
        )
        # 2 constant values × 2 outputs = 4
        assert len(result) == 4

    def test_empty_output_types_returns_empty(self):
        result = build_inferred_variants(
            input_types={"x": ["T"]},
            output_types=[],
            inferred_constants={},
        )
        assert result == []

    def test_input_types_preserved_in_all_variants(self):
        inputs = {"signal": ["Raw"], "ref": ["Ref"]}
        result = build_inferred_variants(
            input_types=inputs,
            output_types=["Out"],
            inferred_constants={"k": [1, 2]},
        )
        for v in result:
            assert v["input_types"] is inputs


# ---------------------------------------------------------------------------
# filter_variants
# ---------------------------------------------------------------------------

class TestFilterVariants:
    def _make_variants(self, const_dicts):
        return [
            {"input_types": {}, "output_type": "Out", "constants": c}
            for c in const_dicts
        ]

    def test_exact_match(self):
        variants = self._make_variants([{"hz": 10}, {"hz": 20}])
        result = filter_variants(variants, selected_variants=[{"hz": 10}])
        assert len(result) == 1
        assert result[0]["constants"]["hz"] == 10

    def test_no_match_returns_all(self):
        variants = self._make_variants([{"hz": 10}, {"hz": 20}])
        result = filter_variants(variants, selected_variants=[{"hz": 99}])
        assert result == variants

    def test_multiple_selected_matches_each(self):
        variants = self._make_variants([{"hz": 10}, {"hz": 20}, {"hz": 30}])
        result = filter_variants(variants, selected_variants=[{"hz": 10}, {"hz": 30}])
        assert len(result) == 2
        values = {v["constants"]["hz"] for v in result}
        assert values == {10, 30}

    def test_string_value_matching(self):
        # selected values stored as strings should still match typed values.
        variants = self._make_variants([{"hz": 10}])
        result = filter_variants(variants, selected_variants=[{"hz": "10"}])
        assert len(result) == 1

    def test_subset_matching(self):
        # selected is a subset of constants in each variant.
        variants = self._make_variants([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
        result = filter_variants(variants, selected_variants=[{"a": 1}])
        assert len(result) == 1
        assert result[0]["constants"]["a"] == 1


# ---------------------------------------------------------------------------
# deduplicate_variants
# ---------------------------------------------------------------------------

class TestDeduplicateVariants:
    def _make(self, consts):
        return [{"input_types": {}, "output_type": "Out", "constants": c} for c in consts]

    def test_no_duplicates_unchanged(self):
        variants = self._make([{"hz": 10}, {"hz": 20}])
        result = deduplicate_variants(variants)
        assert len(result) == 2

    def test_exact_duplicate_removed(self):
        variants = self._make([{"hz": 10}, {"hz": 10}])
        result = deduplicate_variants(variants)
        assert len(result) == 1

    def test_first_occurrence_kept(self):
        v1 = {"input_types": {}, "output_type": "A", "constants": {"hz": 10}}
        v2 = {"input_types": {}, "output_type": "B", "constants": {"hz": 10}}
        result = deduplicate_variants([v1, v2])
        assert result == [v1]

    def test_empty_list(self):
        assert deduplicate_variants([]) == []

    def test_empty_constants_deduplicated(self):
        variants = self._make([{}, {}])
        result = deduplicate_variants(variants)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# merge_pending_constants
# ---------------------------------------------------------------------------

class TestMergePendingConstants:
    def _make(self, const_dicts, out="Out"):
        return [
            {"input_types": {"x": ["T"]}, "output_type": out, "constants": c}
            for c in const_dicts
        ]

    def test_new_pending_value_added(self):
        variants = self._make([{"hz": 10}])
        result = merge_pending_constants(variants, {"hz": {"20"}})
        const_values = {v["constants"]["hz"] for v in result}
        assert 20 in const_values or "20" in const_values

    def test_existing_value_not_duplicated(self):
        variants = self._make([{"hz": 10}])
        result = merge_pending_constants(variants, {"hz": {"10"}})
        hz_values = [v["constants"]["hz"] for v in result]
        assert hz_values.count(10) + hz_values.count("10") == 1

    def test_pending_constant_not_in_fn_ignored(self):
        variants = self._make([{"hz": 10}])
        result = merge_pending_constants(variants, {"other_param": {"99"}})
        assert len(result) == 1

    def test_empty_pending_returns_unchanged(self):
        variants = self._make([{"hz": 10}])
        result = merge_pending_constants(variants, {})
        assert result == variants

    def test_empty_variants_returns_unchanged(self):
        result = merge_pending_constants([], {"hz": {"20"}})
        assert result == []

    def test_cross_product_with_other_constants(self):
        # hz: [10, 20] already exist; pending scale: ["2"]
        # Should add 2 new variants (one per existing hz value).
        variants = self._make([{"hz": 10, "scale": 1}, {"hz": 20, "scale": 1}])
        result = merge_pending_constants(variants, {"scale": {"2"}})
        scales = [v["constants"]["scale"] for v in result]
        assert 2 in scales or "2" in scales
        # The new scale value should pair with each hz value.
        new_variants = [v for v in result if str(v["constants"]["scale"]) == "2"]
        hz_vals = {v["constants"]["hz"] for v in new_variants}
        assert hz_vals == {10, 20}

    def test_coercion_numeric_string_to_int(self):
        variants = self._make([{"hz": 10}])
        result = merge_pending_constants(variants, {"hz": {"42"}})
        new = [v for v in result if v["constants"]["hz"] != 10]
        assert len(new) == 1
        assert new[0]["constants"]["hz"] == 42

    def test_coercion_non_numeric_stays_string(self):
        variants = self._make([{"mode": "fast"}])
        result = merge_pending_constants(variants, {"mode": {"slow"}})
        new = [v for v in result if v["constants"]["mode"] != "fast"]
        assert new[0]["constants"]["mode"] == "slow"


# ---------------------------------------------------------------------------
# build_schema_kwargs
# ---------------------------------------------------------------------------

class TestBuildSchemaKwargs:
    def test_no_filter_no_level_returns_all(self):
        result = build_schema_kwargs(
            schema_level=None,
            all_schema_keys=["subject", "session"],
            schema_filter=None,
            distinct_values={"subject": [1, 2], "session": ["pre", "post"]},
        )
        assert result == {"subject": [1, 2], "session": ["pre", "post"]}

    def test_schema_level_limits_keys(self):
        result = build_schema_kwargs(
            schema_level=["subject"],
            all_schema_keys=["subject", "session"],
            schema_filter=None,
            distinct_values={"subject": [1, 2], "session": ["pre", "post"]},
        )
        assert result == {"subject": [1, 2]}
        assert "session" not in result

    def test_schema_filter_narrows_values(self):
        result = build_schema_kwargs(
            schema_level=None,
            all_schema_keys=["subject", "session"],
            schema_filter={"subject": [1]},
            distinct_values={"subject": [1, 2], "session": ["pre", "post"]},
        )
        assert result["subject"] == [1]
        assert result["session"] == ["pre", "post"]

    def test_schema_filter_empty_list_falls_back_to_distinct(self):
        result = build_schema_kwargs(
            schema_level=None,
            all_schema_keys=["subject"],
            schema_filter={"subject": []},
            distinct_values={"subject": [1, 2, 3]},
        )
        assert result["subject"] == [1, 2, 3]

    def test_schema_level_and_filter_combined(self):
        result = build_schema_kwargs(
            schema_level=["subject"],
            all_schema_keys=["subject", "session"],
            schema_filter={"subject": [2]},
            distinct_values={"subject": [1, 2], "session": ["pre"]},
        )
        assert result == {"subject": [2]}

    def test_key_missing_from_distinct_returns_empty_list(self):
        result = build_schema_kwargs(
            schema_level=None,
            all_schema_keys=["subject"],
            schema_filter=None,
            distinct_values={},
        )
        assert result == {"subject": []}
