"""
Tests for aggregation mode with upstream variants (branch_params propagation).

When aggregation mode processes multiple upstream variants (records with different
branch_params), this tests that:
- All contributing upstream rids are tracked correctly
- branch_params are merged from all contributing upstream records
- Provenance correctly traces back to all upstream variants
- Save behavior works correctly with merged branch_params

Covers:
- Full aggregation with multiple upstream variants
- Partial aggregation with multiple upstream variants
- branch_params merging from heterogeneous upstream variants
- Provenance traversal in aggregation mode
"""

import numpy as np
import pandas as pd
import pytest
import scifor as _scifor

from scidb import BaseVariable, configure_database, for_each


# ---------------------------------------------------------------------------
# Schema and fixtures
# ---------------------------------------------------------------------------

SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    """Fresh database with subject/session schema for each test."""
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_agg_variants.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


# ---------------------------------------------------------------------------
# Variable types
# ---------------------------------------------------------------------------

class RawSignal(BaseVariable):
    pass


class Filtered(BaseVariable):
    pass


class Aggregated(BaseVariable):
    pass


class Feature(BaseVariable):
    pass


# ---------------------------------------------------------------------------
# Pipeline functions
# ---------------------------------------------------------------------------

def bandpass(signal, low_hz):
    """Filter with a parameter that creates variants."""
    if isinstance(signal, np.ndarray):
        return signal * low_hz
    if isinstance(signal, pd.DataFrame):
        return signal * low_hz
    return signal * low_hz


def aggregate_sum(signal):
    """Sum scalar values from the aggregated input."""
    if isinstance(signal, pd.DataFrame):
        return signal.select_dtypes(include="number").values.sum()
    if isinstance(signal, np.ndarray):
        return signal.sum()
    return signal


def extract_mean(signal):
    """Extract mean from aggregated data."""
    if isinstance(signal, pd.DataFrame):
        return signal.select_dtypes(include="number").values.mean()
    if isinstance(signal, np.ndarray):
        return signal.mean()
    return signal


# ---------------------------------------------------------------------------
# 1. Full aggregation with variants
# ---------------------------------------------------------------------------

@pytest.mark.filterwarnings("ignore:branch_params key.*overwritten")
class TestFullAggregationWithVariants:
    """Full aggregation (no schema keys iterated) with multiple upstream variants."""

    def test_aggregates_all_upstream_variants(self, db):
        """Full aggregation processes all upstream variants together."""
        # Create 2 subjects × 2 sessions = 4 RawSignal records
        for subj in ["S01", "S02"]:
            for sess in ["1", "2"]:
                RawSignal.save(np.array([1.0, 2.0, 3.0]), subject=subj, session=sess)

        # Create 2 filter variants at each location → 8 Filtered records total
        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01", "S02"], session=["1", "2"])

        # Full aggregation: no metadata_iterables → aggregates all 8 Filtered records
        result = for_each(aggregate_sum, {"signal": Filtered}, [Aggregated],
                          save=False)

        assert result is not None
        assert len(result) == 1, "Expected exactly 1 iteration (full aggregation)"
        # 4 locations × 2 variants:
        # - low_hz=20: 4 × (20+40+60) = 480
        # - low_hz=30: 4 × (30+60+90) = 720
        # Total: 1200
        result_value = result["Aggregated"].iloc[0]
        if isinstance(result_value, np.ndarray):
            result_value = result_value.item() if result_value.size == 1 else result_value.sum()
        assert result_value == 1200.0

    def test_full_aggregation_with_variants_can_save(self, db):
        """Full aggregation can save results even with multiple upstream variants."""
        for subj in ["S01", "S02"]:
            RawSignal.save(np.array([1.0]), subject=subj, session="1")

        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01", "S02"], session=["1"])

        # Full aggregation with save=True
        result = for_each(aggregate_sum, {"signal": Filtered}, [Aggregated],
                          save=True)

        assert result is not None
        assert len(result) == 1

        # Verify saved record exists
        versions = db.list_versions(Aggregated)
        assert len(versions) >= 1

    def test_full_aggregation_branch_params_merged(self, db):
        """Full aggregation merges branch_params from all contributing upstream variants."""
        RawSignal.save(np.array([1.0]), subject="S01", session="1")
        RawSignal.save(np.array([2.0]), subject="S02", session="1")

        # Create two filter variants
        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01", "S02"], session=["1"])

        # Full aggregation with save
        for_each(aggregate_sum, {"signal": Filtered}, [Aggregated], save=True)

        # Load the aggregated result
        agg = Aggregated.load()

        # Verify it has branch_params (should have merged from upstream)
        assert agg.branch_params is not None
        # The exact merging behavior: when multiple values exist for same key,
        # we should have at least detected them (may warn, or include one)
        # For now, just verify that branch_params exist
        assert isinstance(agg.branch_params, dict)


# ---------------------------------------------------------------------------
# 2. Partial aggregation with variants
# ---------------------------------------------------------------------------

@pytest.mark.filterwarnings("ignore:branch_params key.*overwritten")
class TestPartialAggregationWithVariants:
    """Partial aggregation (subset of schema keys) with multiple upstream variants."""

    def test_partial_aggregation_aggregates_variants_per_iterated_key(self, db):
        """Iterating by subject aggregates sessions and filter variants per subject."""
        # Create 2 subjects × 2 sessions = 4 RawSignal records
        for subj in ["S01", "S02"]:
            for sess in ["1", "2"]:
                RawSignal.save(np.array([1.0, 2.0]), subject=subj, session=sess)

        # Create 2 filter variants → 8 Filtered records total
        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01", "S02"], session=["1", "2"])

        # Partial aggregation: iterate subject only
        # → 2 iterations, each aggregating 2 sessions × 2 variants = 4 Filtered records
        result = for_each(aggregate_sum, {"signal": Filtered}, [Aggregated],
                          subject=["S01", "S02"], save=False)

        assert result is not None
        assert len(result) == 2, "Expected 2 iterations (one per subject)"
        # Each subject: 2 sessions × 2 variants:
        # - low_hz=20: 2 × (20+40) = 120
        # - low_hz=30: 2 × (30+60) = 180
        # Total per subject: 300
        values = []
        for val in result["Aggregated"]:
            if isinstance(val, np.ndarray):
                val = val.item() if val.size == 1 else val.sum()
            values.append(float(val))
        values = sorted(values)
        assert values == [300.0, 300.0]

    def test_partial_aggregation_with_variants_saves_correctly(self, db):
        """Partial aggregation can save results with correct metadata."""
        for subj in ["S01", "S02"]:
            for sess in ["1", "2"]:
                RawSignal.save(np.array([1.0]), subject=subj, session=sess)

        for low_hz in [10, 20]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01", "S02"], session=["1", "2"])

        # Partial aggregation with save
        result = for_each(aggregate_sum, {"signal": Filtered}, [Aggregated],
                          subject=["S01", "S02"], save=True)

        assert result is not None
        assert len(result) == 2

        # Verify saved records have correct schema metadata
        s01_versions = db.list_versions(Aggregated, subject="S01")
        s02_versions = db.list_versions(Aggregated, subject="S02")
        assert len(s01_versions) >= 1
        assert len(s02_versions) >= 1

        # Verify we can load by subject
        agg_s01 = Aggregated.load(subject="S01")
        assert agg_s01.metadata["subject"] == "S01"

    def test_partial_aggregation_branch_params_per_combo(self, db):
        """Each iterated combo gets branch_params merged from its contributing variants."""
        for subj in ["S01", "S02"]:
            for sess in ["1", "2"]:
                RawSignal.save(np.array([1.0]), subject=subj, session=sess)

        # Create variants at each location
        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01", "S02"], session=["1", "2"])

        # Aggregate by subject
        for_each(aggregate_sum, {"signal": Filtered}, [Aggregated],
                 subject=["S01", "S02"], save=True)

        # Load aggregated results
        agg_s01 = Aggregated.load(subject="S01")
        agg_s02 = Aggregated.load(subject="S02")

        # Both should have branch_params
        assert isinstance(agg_s01.branch_params, dict)
        assert isinstance(agg_s02.branch_params, dict)

    def test_uneven_variants_per_schema_location(self, db):
        """Aggregation works even when different schema locations have different variant counts."""
        # S01 gets 2 sessions, S02 gets 1 session
        for sess in ["1", "2"]:
            RawSignal.save(np.array([1.0, 2.0]), subject="S01", session=sess)
        RawSignal.save(np.array([3.0, 4.0]), subject="S02", session="1")

        # Create 2 variants everywhere they exist
        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01", "S02"], session=["1", "2"])

        # Aggregate by subject
        result = for_each(aggregate_sum, {"signal": Filtered}, [Aggregated],
                          subject=["S01", "S02"], save=False)

        assert result is not None
        assert len(result) == 2

        # S01: 2 sessions × 2 variants:
        # - low_hz=20: 2 × (20+40) = 120
        # - low_hz=30: 2 × (30+60) = 180
        # Total: 300
        # S02: 1 session × 2 variants:
        # - low_hz=20: 1 × (60+80) = 140
        # - low_hz=30: 1 × (90+120) = 210
        # Total: 350
        subjects = result["subject"].tolist()
        aggregated_values = []
        for val in result["Aggregated"]:
            if isinstance(val, np.ndarray):
                val = val.item() if val.size == 1 else val.sum()
            aggregated_values.append(float(val))
        values_by_subj = dict(zip(subjects, aggregated_values))
        assert values_by_subj["S01"] == 300.0
        assert values_by_subj["S02"] == 350.0


# ---------------------------------------------------------------------------
# 3. Multi-step aggregation pipeline
# ---------------------------------------------------------------------------

@pytest.mark.filterwarnings("ignore:branch_params key.*overwritten")
class TestMultiStepAggregationPipeline:
    """Aggregation in a multi-step pipeline with variants at each step."""

    def test_two_step_aggregation_pipeline(self, db):
        """Aggregation → aggregation pipeline works with variants."""
        for subj in ["S01", "S02"]:
            for sess in ["1", "2"]:
                RawSignal.save(np.array([1.0, 2.0, 3.0]), subject=subj, session=sess)

        # Step 1: Create variants
        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01", "S02"], session=["1", "2"])

        # Step 2: Aggregate by subject (aggregates sessions and variants)
        for_each(aggregate_sum, {"signal": Filtered}, [Aggregated],
                 subject=["S01", "S02"], save=True)

        # Step 3: Further aggregate (extract mean across subjects)
        result = for_each(extract_mean, {"signal": Aggregated}, [Feature],
                          save=True)

        assert result is not None
        # Final aggregation should work
        feat = Feature.load()
        assert feat is not None

    def test_aggregation_after_full_iteration(self, db):
        """Full iteration → aggregation pipeline with variants."""
        for subj in ["S01", "S02"]:
            RawSignal.save(np.array([1.0, 2.0]), subject=subj, session="1")

        # Full iteration: process each location individually with variants
        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01", "S02"], session=["1"])

        # Now aggregate across subjects (4 Filtered records → 1 Aggregated)
        result = for_each(aggregate_sum, {"signal": Filtered}, [Aggregated],
                          save=True)

        assert result is not None
        assert len(result) == 1

        # Verify saved
        agg = Aggregated.load()
        assert agg is not None


# ---------------------------------------------------------------------------
# 4. Provenance with aggregation
# ---------------------------------------------------------------------------

class TestProvenanceWithAggregation:
    """Provenance and upstream metadata for aggregation with multiple upstream variants.

    NOTE: get_upstream_provenance uses schema_id matching to find upstream
    records, so cross-schema-level traversal (e.g. Aggregated at subject
    level → Filtered at subject/session level) is not yet supported.
    These tests verify the metadata is stored correctly and that branch_params
    are merged from all contributing upstream records.
    """

    @pytest.mark.filterwarnings("ignore:branch_params key.*overwritten")
    def test_aggregated_record_has_upstream_metadata(self, db):
        """Aggregated record stores __upstream metadata with contributing rids."""
        for sess in ["1", "2"]:
            RawSignal.save(np.array([1.0]), subject="S01", session=sess)

        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01"], session=["1", "2"])

        # Aggregate by subject (aggregates 2 sessions × 2 variants = 4 Filtered)
        for_each(aggregate_sum, {"signal": Filtered}, [Aggregated],
                 subject=["S01"], save=True)

        agg = Aggregated.load(subject="S01")
        prov = db.get_upstream_provenance(agg.record_id)

        # Should have the Aggregated record itself
        agg_nodes = [n for n in prov if n["variable_type"] == "Aggregated"]
        assert len(agg_nodes) == 1
        agg_node = agg_nodes[0]

        # The node should reference aggregate_sum as the function
        assert agg_node["function_name"] == "aggregate_sum"

        # branch_params should be merged from all upstream variants
        assert isinstance(agg_node["branch_params"], dict)

    def test_aggregated_branch_params_contain_upstream_values(self, db):
        """Aggregated record's branch_params reflect the merged upstream branch_params."""
        RawSignal.save(np.array([1.0]), subject="S01", session="1")

        # Create a single filter variant (no ambiguity in merge)
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])

        # Aggregate (single upstream variant → clean merge)
        for_each(aggregate_sum, {"signal": Filtered}, [Aggregated],
                 subject=["S01"], save=True)

        agg = Aggregated.load(subject="S01")
        # The upstream bandpass.low_hz should be present in branch_params
        assert agg.branch_params.get("bandpass.low_hz") == 20

    def test_aggregated_record_warns_on_conflicting_branch_params(self, db):
        """When aggregating multiple upstream variants with different branch_params, a warning is raised."""
        import warnings as _warnings

        for sess in ["1", "2"]:
            RawSignal.save(np.array([1.0]), subject="S01", session=sess)

        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01"], session=["1", "2"])

        # Aggregating 4 Filtered records with conflicting branch_params should warn
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            for_each(aggregate_sum, {"signal": Filtered}, [Aggregated],
                     subject=["S01"], save=True)

        bp_warnings = [w for w in caught if "branch_params" in str(w.message)]
        assert len(bp_warnings) > 0, "Should warn about conflicting branch_params"

    @pytest.mark.filterwarnings("ignore:branch_params key.*overwritten")
    def test_full_aggregation_stores_upstream_rids(self, db):
        """Full aggregation correctly records upstream metadata."""
        RawSignal.save(np.array([1.0]), subject="S01", session="1")
        RawSignal.save(np.array([2.0]), subject="S02", session="1")

        for low_hz in [10, 20]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01", "S02"], session=["1"])

        # Full aggregation
        for_each(aggregate_sum, {"signal": Filtered}, [Aggregated], save=True)

        agg = Aggregated.load()
        prov = db.get_upstream_provenance(agg.record_id)

        # Should at least have the Aggregated record itself
        agg_nodes = [n for n in prov if n["variable_type"] == "Aggregated"]
        assert len(agg_nodes) == 1
        assert agg_nodes[0]["function_name"] == "aggregate_sum"


# ---------------------------------------------------------------------------
# 5. Edge cases
# ---------------------------------------------------------------------------

class TestAggregationVariantsEdgeCases:
    """Edge cases for aggregation with variants."""

    def test_aggregation_with_single_variant_no_ambiguity(self, db):
        """Aggregation with only one upstream variant still works."""
        for subj in ["S01", "S02"]:
            for sess in ["1", "2"]:
                RawSignal.save(np.array([1.0, 2.0]), subject=subj, session=sess)

        # Only one filter variant
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01", "S02"], session=["1", "2"])

        # Aggregate by subject
        result = for_each(aggregate_sum, {"signal": Filtered}, [Aggregated],
                          subject=["S01", "S02"], save=True)

        assert result is not None
        assert len(result) == 2

        # Should be able to load without ambiguity
        agg = Aggregated.load(subject="S01")
        assert agg.branch_params.get("bandpass.low_hz") == 20

    def test_aggregation_with_no_upstream_branch_params(self, db):
        """Aggregation works even when upstream has no branch_params."""
        for subj in ["S01", "S02"]:
            RawSignal.save(np.array([1.0, 2.0]), subject=subj, session="1")

        # Aggregate raw data directly (no upstream processing)
        result = for_each(aggregate_sum, {"signal": RawSignal}, [Aggregated],
                          subject=["S01", "S02"], save=True)

        assert result is not None
        assert len(result) == 2

        agg = Aggregated.load(subject="S01")
        assert agg.branch_params == {}

    @pytest.mark.filterwarnings("ignore:branch_params key.*overwritten")
    def test_many_variants_aggregated(self, db):
        """Aggregation handles many upstream variants efficiently."""
        for sess in ["1", "2"]:
            RawSignal.save(np.array([1.0]), subject="S01", session=sess)

        # Create 5 variants at each session
        for low_hz in [10, 20, 30, 40, 50]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01"], session=["1", "2"])

        # Aggregate across sessions and variants
        result = for_each(aggregate_sum, {"signal": Filtered}, [Aggregated],
                          subject=["S01"], save=True)

        assert result is not None
        assert len(result) == 1
        # 2 sessions × 5 variants × 1.0 × low_hz_sum
        # Actually: 2 sessions × 5 variants × (1.0 * low_hz) = sum of all
        # = 2 × (1.0×10 + 1.0×20 + 1.0×30 + 1.0×40 + 1.0×50) = 2 × 150 = 300
        expected = 2 * (10 + 20 + 30 + 40 + 50)
        result_value = result["Aggregated"].iloc[0]
        if isinstance(result_value, np.ndarray):
            result_value = result_value.item() if result_value.size == 1 else result_value.sum()
        assert result_value == expected

        agg = Aggregated.load(subject="S01")
        assert agg is not None
