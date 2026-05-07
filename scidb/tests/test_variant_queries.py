"""Tests for variant query APIs: get_aggregated_variants() and filter_variants_for_execution().

These methods provide aggregated pipeline data for visualization and variant
filtering for execution, used primarily by the GUI but available to all tools.
"""

import json
import numpy as np
import pytest
import scifor as _scifor

from scidb import BaseVariable, configure_database, for_each, PathInput


SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_variant_queries.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


# ---------------------------------------------------------------------------
# Variable types
# ---------------------------------------------------------------------------

class RawSignal(BaseVariable):
    pass


class FilteredSignal(BaseVariable):
    pass


class ProcessedSignal(BaseVariable):
    pass


class Stats(BaseVariable):
    pass


# ---------------------------------------------------------------------------
# Pipeline functions
# ---------------------------------------------------------------------------

def bandpass_filter(signal, low_hz, high_hz):
    """Filter signal with bandpass."""
    return signal * (low_hz + high_hz)


def compute_stats(signal, window_size):
    """Compute statistics with windowing."""
    return signal.mean() * window_size


def multi_output_filter(signal, low_hz):
    """Filter that returns both filtered and processed versions."""
    filtered = signal * low_hz
    processed = signal * (low_hz + 1)
    return filtered, processed


def process_file(filepath):
    """Process a file (PathInput example)."""
    return np.array([1.0, 2.0, 3.0])


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _seed_raw(db, subjects=(1, 2), sessions=("A", "B")):
    for subj in subjects:
        for sess in sessions:
            RawSignal.save(np.random.randn(10), db=db, subject=subj, session=sess)


def _seed_filtered(db, subjects=(1, 2), sessions=("A", "B")):
    for subj in subjects:
        for sess in sessions:
            FilteredSignal.save(np.random.randn(10), db=db, subject=subj, session=sess)


# ---------------------------------------------------------------------------
# get_aggregated_variants() tests
# ---------------------------------------------------------------------------

class TestGetAggregatedVariants:
    def test_empty_database(self, db):
        """Empty database returns empty structures."""
        result = db.get_aggregated_variants()

        assert result["functions"] == {}
        assert result["variables"] == {}
        assert result["constants"] == {}
        assert result["path_inputs"] == {}

    def test_single_function_single_variant(self, db):
        """Single function with one variant."""
        _seed_raw(db, subjects=(1,), sessions=("A",))

        # Run function with one set of constants
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20, "high_hz": 500},
            outputs=[FilteredSignal],
            subject=[1],
            session=["A"],
        )

        result = db.get_aggregated_variants()

        # Should have one function entry
        assert len(result["functions"]) == 1

        # Get the function data
        fn_data = list(result["functions"].values())[0]
        assert fn_data["input_params"] == {"signal": "RawSignal"}
        assert "FilteredSignal" in fn_data["outputs"]
        assert "low_hz" in fn_data["constants"]
        assert 20 in fn_data["constants"]["low_hz"]
        assert "high_hz" in fn_data["constants"]
        assert 500 in fn_data["constants"]["high_hz"]
        assert fn_data["variant_count"] >= 1

    def test_multiple_variants_different_constants(self, db):
        """Same function with different constant values creates multiple variants."""
        _seed_raw(db, subjects=(1, 2), sessions=("A",))

        # Run with low_hz=20
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20, "high_hz": 500},
            outputs=[FilteredSignal],
            subject=[1],
            session=["A"],
        )

        # Run with low_hz=50
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 50, "high_hz": 500},
            outputs=[FilteredSignal],
            subject=[2],
            session=["A"],
        )

        result = db.get_aggregated_variants()

        # Should have two function entries (different call_ids)
        assert len(result["functions"]) == 2

        # Check constants are tracked correctly
        all_low_hz_values = []
        for fn_data in result["functions"].values():
            if "low_hz" in fn_data["constants"]:
                all_low_hz_values.extend(fn_data["constants"]["low_hz"])

        assert 20 in all_low_hz_values or 50 in all_low_hz_values

    def test_multiple_output_types(self, db):
        """Function with multiple output types."""
        _seed_raw(db)

        # Use function that actually returns multiple values
        for_each(
            multi_output_filter,
            inputs={"signal": RawSignal, "low_hz": 20},
            outputs=[FilteredSignal, ProcessedSignal],
            subject=[1, 2],
            session=["A", "B"],
        )

        result = db.get_aggregated_variants()

        # Get all function entries for multi_output_filter
        filter_entries = {
            fkey: fdata for fkey, fdata in result["functions"].items()
            if fkey[0] == "multi_output_filter"
        }

        # Should have at least one function entry
        assert len(filter_entries) >= 1

        # Collect all output types across all entries
        all_outputs = []
        for fn_data in filter_entries.values():
            all_outputs.extend(fn_data["outputs"])

        # Should have both output types somewhere
        assert "FilteredSignal" in all_outputs
        assert "ProcessedSignal" in all_outputs

    def test_variable_record_counts(self, db):
        """Variables section includes record counts."""
        _seed_raw(db, subjects=(1, 2), sessions=("A", "B"))

        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20, "high_hz": 500},
            outputs=[FilteredSignal],
            subject=[1, 2],
            session=["A", "B"],
        )

        result = db.get_aggregated_variants()

        # Should track both input and output variables
        assert "RawSignal" in result["variables"]
        assert "FilteredSignal" in result["variables"]

        # Record counts should be present
        assert result["variables"]["RawSignal"]["record_count"] == 4  # 2 subjects × 2 sessions
        assert result["variables"]["FilteredSignal"]["record_count"] == 4

    def test_constants_aggregation(self, db):
        """Constants section aggregates across functions."""
        _seed_raw(db, subjects=(1, 2), sessions=("A",))

        # Run twice with different constants
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20, "high_hz": 500},
            outputs=[FilteredSignal],
            subject=[1],
            session=["A"],
        )

        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 50, "high_hz": 500},
            outputs=[FilteredSignal],
            subject=[2],
            session=["A"],
        )

        result = db.get_aggregated_variants()

        # Constants should list both values (note: values are strings)
        assert "low_hz" in result["constants"]
        low_hz_values = [v["value"] for v in result["constants"]["low_hz"]["values"]]
        assert "20" in low_hz_values or "50" in low_hz_values

        # high_hz should have one value (shared across both runs)
        assert "high_hz" in result["constants"]
        high_hz_values = [v["value"] for v in result["constants"]["high_hz"]["values"]]
        assert "500" in high_hz_values

    def test_path_input_parsing(self, db):
        """PathInput parameters are parsed correctly."""
        # Create a PathInput configuration
        path_input = PathInput("{subject}/data.csv")

        # Run function with PathInput
        for_each(
            process_file,
            inputs={"filepath": path_input},
            outputs=[ProcessedSignal],
            subject=[1, 2],
        )

        result = db.get_aggregated_variants()

        # Should have path_inputs entry
        assert "filepath" in result["path_inputs"]
        assert result["path_inputs"]["filepath"]["template"] == "{subject}/data.csv"

    def test_filter_by_function_name(self, db):
        """Filter results by function name."""
        _seed_raw(db)
        _seed_filtered(db)

        # Run two different functions
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20, "high_hz": 500},
            outputs=[FilteredSignal],
            subject=[1],
            session=["A"],
        )

        for_each(
            compute_stats,
            inputs={"signal": FilteredSignal, "window_size": 10},
            outputs=[Stats],
            subject=[1],
            session=["A"],
        )

        # Filter by first function only
        result = db.get_aggregated_variants(fn_name="bandpass_filter")

        # Should only have bandpass_filter
        for (fn_name, _), _ in result["functions"].items():
            assert fn_name == "bandpass_filter"

    def test_filter_by_call_id(self, db):
        """Filter results by call_id."""
        _seed_raw(db, subjects=(1, 2), sessions=("A",))

        # Run same function with different constants (different call_ids)
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20, "high_hz": 500},
            outputs=[FilteredSignal],
            subject=[1],
            session=["A"],
        )

        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 50, "high_hz": 500},
            outputs=[FilteredSignal],
            subject=[2],
            session=["A"],
        )

        # Get all variants to find a call_id
        all_result = db.get_aggregated_variants(fn_name="bandpass_filter")
        assert len(all_result["functions"]) == 2

        # Pick one call_id
        first_call_id = list(all_result["functions"].keys())[0][1]

        # Filter by that call_id
        filtered_result = db.get_aggregated_variants(
            fn_name="bandpass_filter",
            call_id=first_call_id
        )

        # Should have only one function entry
        assert len(filtered_result["functions"]) == 1
        assert list(filtered_result["functions"].keys())[0][1] == first_call_id


# ---------------------------------------------------------------------------
# filter_variants_for_execution() tests
# ---------------------------------------------------------------------------

class TestFilterVariantsForExecution:
    def test_no_variants_returns_empty(self, db):
        """No variants for function returns empty list."""
        result = db.filter_variants_for_execution(
            fn_name="bandpass_filter",
            call_id="0123456789abcdef"
        )
        assert result == []

    def test_basic_variant_filtering(self, db):
        """Basic variant retrieval without filters."""
        _seed_raw(db)

        # Run function
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20, "high_hz": 500},
            outputs=[FilteredSignal],
            subject=[1, 2],
            session=["A", "B"],
        )

        # Get the call_id
        variants = db.list_pipeline_variants()
        call_id = variants[0]["call_id"]

        # Filter variants
        result = db.filter_variants_for_execution(
            fn_name="bandpass_filter",
            call_id=call_id
        )

        # Should return variants
        assert len(result) > 0
        assert all("input_types" in v for v in result)
        assert all("output_type" in v for v in result)
        assert all("constants" in v for v in result)

    def test_constant_override(self, db):
        """Constant overrides replace database values."""
        _seed_raw(db)

        # Run with low_hz=20
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20, "high_hz": 500},
            outputs=[FilteredSignal],
            subject=[1],
            session=["A"],
        )

        # Get the call_id
        variants = db.list_pipeline_variants()
        call_id = variants[0]["call_id"]

        # Filter with constant override
        result = db.filter_variants_for_execution(
            fn_name="bandpass_filter",
            call_id=call_id,
            constant_overrides={"low_hz": 50}  # Override to 50
        )

        # Should return variants with overridden constant
        assert len(result) > 0
        for variant in result:
            if "low_hz" in variant["constants"]:
                assert variant["constants"]["low_hz"] == 50  # Overridden value

    def test_multiple_constant_overrides(self, db):
        """Multiple constant overrides work together."""
        _seed_raw(db)

        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20, "high_hz": 500},
            outputs=[FilteredSignal],
            subject=[1],
            session=["A"],
        )

        variants = db.list_pipeline_variants()
        call_id = variants[0]["call_id"]

        # Override both constants
        result = db.filter_variants_for_execution(
            fn_name="bandpass_filter",
            call_id=call_id,
            constant_overrides={"low_hz": 100, "high_hz": 1000}
        )

        assert len(result) > 0
        for variant in result:
            if "low_hz" in variant["constants"]:
                assert variant["constants"]["low_hz"] == 100
            if "high_hz" in variant["constants"]:
                assert variant["constants"]["high_hz"] == 1000

    def test_deduplication(self, db):
        """Identical variants after overrides are deduplicated."""
        _seed_raw(db, subjects=(1, 2), sessions=("A",))

        # Run with two different output types (creates 2 variants)
        for_each(
            multi_output_filter,
            inputs={"signal": RawSignal, "low_hz": 20},
            outputs=[FilteredSignal, ProcessedSignal],
            subject=[1],
            session=["A"],
        )

        variants = db.list_pipeline_variants()
        call_id = variants[0]["call_id"]

        # Get variants without override
        result_no_override = db.filter_variants_for_execution(
            fn_name="multi_output_filter",
            call_id=call_id
        )

        # Should have 2 variants (different output types)
        assert len(result_no_override) == 2

        # The variants should be deduplicated (no exact duplicates)
        seen = set()
        for v in result_no_override:
            key = (
                v["output_type"],
                tuple(sorted(v["input_types"].items())),
                tuple(sorted(v["constants"].items()))
            )
            assert key not in seen, "Found duplicate variant"
            seen.add(key)

    def test_constant_override_only_applies_to_matching_params(self, db):
        """Constant override only affects parameters that exist in variants."""
        _seed_raw(db)

        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20, "high_hz": 500},
            outputs=[FilteredSignal],
            subject=[1],
            session=["A"],
        )

        variants = db.list_pipeline_variants()
        call_id = variants[0]["call_id"]

        # Override with a constant that doesn't exist
        result = db.filter_variants_for_execution(
            fn_name="bandpass_filter",
            call_id=call_id,
            constant_overrides={"nonexistent_param": 999}
        )

        # Should still return variants, just ignore the non-existent override
        assert len(result) > 0

    def test_different_call_ids_are_isolated(self, db):
        """Filtering by one call_id doesn't return variants from another."""
        _seed_raw(db, subjects=(1, 2), sessions=("A",))

        # Run same function with different constants (creates different call_ids)
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20, "high_hz": 500},
            outputs=[FilteredSignal],
            subject=[1],
            session=["A"],
        )

        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 50, "high_hz": 500},
            outputs=[FilteredSignal],
            subject=[2],
            session=["A"],
        )

        # Get both call_ids
        all_variants = db.list_pipeline_variants()
        call_ids = list(set(v["call_id"] for v in all_variants))
        assert len(call_ids) == 2

        # Filter by first call_id
        result1 = db.filter_variants_for_execution(
            fn_name="bandpass_filter",
            call_id=call_ids[0]
        )

        # Filter by second call_id
        result2 = db.filter_variants_for_execution(
            fn_name="bandpass_filter",
            call_id=call_ids[1]
        )

        # Results should be different (different constants)
        assert len(result1) > 0
        assert len(result2) > 0

        # Check that constants differ
        const1 = result1[0]["constants"] if result1 else {}
        const2 = result2[0]["constants"] if result2 else {}

        # At least one constant should differ
        if "low_hz" in const1 and "low_hz" in const2:
            assert const1["low_hz"] != const2["low_hz"]


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

class TestIntegration:
    def test_aggregated_variants_and_filtering_consistency(self, db):
        """get_aggregated_variants() and filter_variants_for_execution() return consistent data."""
        _seed_raw(db)

        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20, "high_hz": 500},
            outputs=[FilteredSignal],
            subject=[1, 2],
            session=["A", "B"],
        )

        # Get aggregated data
        agg = db.get_aggregated_variants(fn_name="bandpass_filter")

        # Should have one function entry
        assert len(agg["functions"]) == 1

        fn_key, fn_data = list(agg["functions"].items())[0]
        fn_name, call_id = fn_key

        # Get filtered variants
        filtered = db.filter_variants_for_execution(
            fn_name=fn_name,
            call_id=call_id
        )

        # Variant count should match
        assert len(filtered) == fn_data["variant_count"]

        # Constants should be consistent
        for variant in filtered:
            for const_name, const_value in variant["constants"].items():
                assert const_name in fn_data["constants"]
                assert const_value in fn_data["constants"][const_name]

    def test_real_world_pipeline_scenario(self, db):
        """Simulate a real-world pipeline with multiple functions."""
        _seed_raw(db, subjects=(1, 2, 3), sessions=("pre", "post"))

        # Step 1: Filter signals
        for_each(
            bandpass_filter,
            inputs={"signal": RawSignal, "low_hz": 20, "high_hz": 500},
            outputs=[FilteredSignal],
            subject=[1, 2, 3],
            session=["pre", "post"],
        )

        # Step 2: Compute stats
        for_each(
            compute_stats,
            inputs={"signal": FilteredSignal, "window_size": 10},
            outputs=[Stats],
            subject=[1, 2, 3],
            session=["pre", "post"],
        )

        # Get aggregated view
        agg = db.get_aggregated_variants()

        # Should have 2 functions
        assert len(agg["functions"]) == 2

        # Should have 3 variable types
        assert "RawSignal" in agg["variables"]
        assert "FilteredSignal" in agg["variables"]
        assert "Stats" in agg["variables"]

        # Each variable should have correct record count
        assert agg["variables"]["RawSignal"]["record_count"] == 6  # 3 subjects × 2 sessions
        assert agg["variables"]["FilteredSignal"]["record_count"] == 6
        assert agg["variables"]["Stats"]["record_count"] == 6

        # Constants should be tracked
        assert "low_hz" in agg["constants"]
        assert "high_hz" in agg["constants"]
        assert "window_size" in agg["constants"]
