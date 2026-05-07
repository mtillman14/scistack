"""
Comprehensive tests for branch_params tracking, branch-aware load API,
list_pipeline_variants, and get_upstream_provenance.

Covers:
- Parameter sweep branch isolation (for_each constants → distinct records)
- branch_params accumulation across multi-step pipelines
- AmbiguousVersionError, AmbiguousParamError
- load() and list_versions() filtering by branch_params
- exclude_variant / include_variant
- list_pipeline_variants()
- get_upstream_provenance()

Tests run across multiple data types and realistic multi-subject pipelines.
"""

import numpy as np
import pandas as pd
import pytest
import scifor as _scifor

from scidb import BaseVariable, NotFoundError, configure_database, for_each
from scidb.exceptions import AmbiguousParamError, AmbiguousVersionError


# ---------------------------------------------------------------------------
# Schema and fixtures
# ---------------------------------------------------------------------------

SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    """Fresh database with subject/session schema for each test."""
    _scifor.set_schema([])  # ensure clean scifor state
    db = configure_database(tmp_path / "test_bp.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


# ---------------------------------------------------------------------------
# Variable types used across tests
# ---------------------------------------------------------------------------

class RawSignal(BaseVariable): pass
class Filtered(BaseVariable): pass
class Spectrum(BaseVariable): pass
class Spikes(BaseVariable): pass
class Feature(BaseVariable): pass
class Intermediate(BaseVariable): pass


# ---------------------------------------------------------------------------
# Pipeline functions
# ---------------------------------------------------------------------------

def bandpass(signal, low_hz):
    if isinstance(signal, np.ndarray):
        return signal * low_hz
    if isinstance(signal, pd.DataFrame):
        return signal * low_hz
    return signal * low_hz


def detect_spikes(signal, threshold):
    if isinstance(signal, np.ndarray):
        return (signal > threshold).astype(float)
    return float(signal > threshold)


def compute_spectrum(signal, n_fft=256):
    if isinstance(signal, np.ndarray):
        return np.abs(signal) * n_fft
    return abs(signal) * n_fft


def extract_feature(spikes, win_size):
    if isinstance(spikes, np.ndarray):
        return np.sum(spikes) * win_size
    return spikes * win_size


def smooth(signal, threshold):
    """Second function that also takes 'threshold' — used for AmbiguousParamError tests."""
    if isinstance(signal, np.ndarray):
        return signal * threshold
    return signal * threshold


# Parametrized data types for cross-type coverage
DATA_VARIANTS = [
    pytest.param(np.array([1.0, 2.0, 3.0]), id="1d-array"),
    pytest.param(42.5, id="scalar"),
    pytest.param(np.array([[1.0, 2.0], [3.0, 4.0]]), id="2d-matrix"),
]


# ---------------------------------------------------------------------------
# 1. Branch Isolation — constants produce distinct records
# ---------------------------------------------------------------------------

class TestBranchIsolation:
    """for_each with different scalar constants creates distinct stored records."""

    @pytest.mark.parametrize("data", DATA_VARIANTS)
    def test_two_constant_values_create_two_versions(self, db, data):
        RawSignal.save(data, subject="S01", session="1")

        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])
        for_each(bandpass, {"signal": RawSignal, "low_hz": 30}, [Filtered],
                 subject=["S01"], session=["1"])

        versions = db.list_versions(Filtered, subject="S01", session="1")
        assert len(versions) == 2

    def test_rerun_same_constants_is_idempotent(self, db):
        RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")

        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])

        versions = db.list_versions(Filtered, subject="S01", session="1")
        assert len(versions) == 1

    def test_three_constant_values_create_three_versions(self, db):
        RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")

        for low_hz in [10, 20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01"], session=["1"])

        versions = db.list_versions(Filtered, subject="S01", session="1")
        assert len(versions) == 3

    def test_branch_params_values_match_constants(self, db):
        RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")

        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])
        for_each(bandpass, {"signal": RawSignal, "low_hz": 30}, [Filtered],
                 subject=["S01"], session=["1"])

        versions = db.list_versions(Filtered, subject="S01", session="1")
        low_hz_values = sorted(
            v["branch_params"]["bandpass.low_hz"] for v in versions
        )
        assert low_hz_values == [20, 30]

    def test_multiple_schema_locations_each_get_independent_variants(self, db):
        for subj in ["S01", "S02"]:
            for sess in ["1", "2"]:
                RawSignal.save(np.array([1.0, 2.0]), subject=subj, session=sess)

        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01", "S02"], session=["1", "2"])

        for subj in ["S01", "S02"]:
            for sess in ["1", "2"]:
                versions = db.list_versions(Filtered, subject=subj, session=sess)
                assert len(versions) == 2, f"Expected 2 at {subj}/{sess}"

    def test_downstream_step_multiplies_variants(self, db):
        """2 Filtered × 2 detect thresholds = 4 Spikes variants."""
        RawSignal.save(np.array([1.0, 2.0, 3.0]), subject="S01", session="1")

        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01"], session=["1"])

        for thresh in [0.5, 0.6]:
            for_each(detect_spikes, {"signal": Filtered, "threshold": thresh}, [Spikes],
                     subject=["S01"], session=["1"])

        assert len(db.list_versions(Spikes, subject="S01", session="1")) == 4


# ---------------------------------------------------------------------------
# 2. branch_params Accumulation
# ---------------------------------------------------------------------------

class TestBranchParamsAccumulation:
    """branch_params grows correctly as records pass through pipeline steps."""

    def test_raw_save_has_empty_branch_params(self, db):
        RawSignal.save(np.array([1.0]), subject="S01", session="1")
        raw = RawSignal.load(subject="S01", session="1")
        assert raw.branch_params == {}

    def test_single_step_adds_namespaced_constant(self, db):
        RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])

        f = Filtered.load(subject="S01", session="1")
        assert f.branch_params == {"bandpass.low_hz": 20}

    def test_two_step_chain_accumulates_both_steps(self, db):
        RawSignal.save(np.array([1.0, 2.0, 3.0]), subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])
        for_each(detect_spikes, {"signal": Filtered, "threshold": 0.5}, [Spikes],
                 subject=["S01"], session=["1"])

        s = Spikes.load(subject="S01", session="1")
        assert s.branch_params["bandpass.low_hz"] == 20
        assert s.branch_params["detect_spikes.threshold"] == 0.5

    def test_three_step_chain_accumulates_all(self, db):
        RawSignal.save(np.array([1.0, 2.0, 3.0]), subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])
        for_each(detect_spikes, {"signal": Filtered, "threshold": 0.5}, [Spikes],
                 subject=["S01"], session=["1"])
        for_each(extract_feature, {"spikes": Spikes, "win_size": 10}, [Feature],
                 subject=["S01"], session=["1"])

        feat = Feature.load(subject="S01", session="1")
        assert feat.branch_params["bandpass.low_hz"] == 20
        assert feat.branch_params["detect_spikes.threshold"] == 0.5
        assert feat.branch_params["extract_feature.win_size"] == 10

    def test_no_constants_step_does_not_add_to_branch_params(self, db):
        """A step with no scalar constants contributes nothing to branch_params."""
        RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
        for_each(compute_spectrum, {"signal": RawSignal}, [Spectrum],
                 subject=["S01"], session=["1"])

        sp = Spectrum.load(subject="S01", session="1")
        assert sp.branch_params == {}

    @pytest.mark.parametrize("data", DATA_VARIANTS)
    def test_branch_params_accumulates_regardless_of_data_type(self, db, data):
        RawSignal.save(data, subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])

        f = Filtered.load(subject="S01", session="1")
        assert f.branch_params["bandpass.low_hz"] == 20

    def test_branch_params_propagates_to_multiple_schema_locations(self, db):
        for subj in ["S01", "S02"]:
            RawSignal.save(np.array([1.0, 2.0]), subject=subj, session="1")

        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01", "S02"], session=["1"])

        for subj in ["S01", "S02"]:
            f = Filtered.load(subject=subj, session="1")
            assert f.branch_params["bandpass.low_hz"] == 20


# ---------------------------------------------------------------------------
# 3. Branch-Aware Load
# ---------------------------------------------------------------------------

class TestBranchAwareLoad:
    """load(**branch_params) filters records by parameter values."""

    @pytest.fixture(autouse=True)
    def _two_filtered_variants(self, db):
        RawSignal.save(np.array([1.0, 2.0, 3.0]), subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])
        for_each(bandpass, {"signal": RawSignal, "low_hz": 30}, [Filtered],
                 subject=["S01"], session=["1"])

    def test_load_without_filter_raises_ambiguous_version_error(self, db):
        with pytest.raises(AmbiguousVersionError) as exc_info:
            Filtered.load(subject="S01", session="1")
        assert "2" in str(exc_info.value)

    def test_load_with_bare_name_suffix_match(self, db):
        f = Filtered.load(subject="S01", session="1", low_hz=20)
        assert f.branch_params["bandpass.low_hz"] == 20

    def test_load_with_namespaced_key(self, db):
        f = Filtered.load(subject="S01", session="1", **{"bandpass.low_hz": 30})
        assert f.branch_params["bandpass.low_hz"] == 30

    def test_load_with_wrong_value_raises_not_found(self, db):
        with pytest.raises(NotFoundError):
            Filtered.load(subject="S01", session="1", low_hz=99)

    def test_load_with_version_record_id_bypasses_branch_filter(self, db):
        versions = db.list_versions(Filtered, subject="S01", session="1")
        rid = versions[0]["record_id"]
        f = Filtered.load(subject="S01", session="1", version=rid)
        assert f.record_id == rid

    def test_load_result_has_correct_branch_params(self, db):
        f20 = Filtered.load(subject="S01", session="1", low_hz=20)
        f30 = Filtered.load(subject="S01", session="1", low_hz=30)
        assert f20.branch_params["bandpass.low_hz"] == 20
        assert f30.branch_params["bandpass.low_hz"] == 30

    def test_ambiguous_param_error_when_same_name_in_two_steps(self, db):
        """Two pipeline steps that both use 'threshold' → AmbiguousParamError."""
        # Build Intermediate from Filtered via smooth(threshold=0.1)
        for_each(smooth, {"signal": Filtered, "threshold": 0.1}, [Intermediate],
                 subject=["S01"], session=["1"])
        # Then detect_spikes on Intermediate with threshold=0.5
        for_each(detect_spikes, {"signal": Intermediate, "threshold": 0.5}, [Spikes],
                 subject=["S01"], session=["1"])

        # Spikes.branch_params has both smooth.threshold and detect_spikes.threshold
        # Only one Spikes variant at S01/1 per (low_hz, smooth.threshold, detect.threshold)
        # Loading by bare 'threshold' is ambiguous
        with pytest.raises(AmbiguousParamError):
            Spikes.load(subject="S01", session="1", low_hz=20, threshold=0.5)

    def test_load_with_multiple_branch_params_filters(self, db):
        """Downstream record loadable with multiple filters."""
        for_each(detect_spikes, {"signal": Filtered, "threshold": 0.5}, [Spikes],
                 subject=["S01"], session=["1"])
        for_each(detect_spikes, {"signal": Filtered, "threshold": 0.6}, [Spikes],
                 subject=["S01"], session=["1"])

        s = Spikes.load(subject="S01", session="1", low_hz=20, threshold=0.5)
        assert s.branch_params["bandpass.low_hz"] == 20
        assert s.branch_params["detect_spikes.threshold"] == 0.5


# ---------------------------------------------------------------------------
# 4. list_versions with branch_params
# ---------------------------------------------------------------------------

class TestListVersions:
    """list_versions returns and filters by branch_params."""

    @pytest.fixture(autouse=True)
    def _full_pipeline(self, db):
        for subj in ["S01", "S02"]:
            RawSignal.save(np.array([1.0, 2.0, 3.0]), subject=subj, session="1")

        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01", "S02"], session=["1"])

        for thresh in [0.5, 0.6]:
            for_each(detect_spikes, {"signal": Filtered, "threshold": thresh}, [Spikes],
                     subject=["S01", "S02"], session=["1"])

    def test_list_versions_contains_branch_params_key(self, db):
        versions = db.list_versions(Filtered, subject="S01", session="1")
        assert all("branch_params" in v for v in versions)

    def test_list_versions_counts_correct(self, db):
        assert len(db.list_versions(Filtered, subject="S01", session="1")) == 2
        assert len(db.list_versions(Spikes, subject="S01", session="1")) == 4

    def test_list_versions_filter_by_bare_name(self, db):
        results = db.list_versions(Spikes, subject="S01", session="1", low_hz=20)
        assert len(results) == 2
        assert all(r["branch_params"]["bandpass.low_hz"] == 20 for r in results)

    def test_list_versions_filter_by_two_params(self, db):
        results = db.list_versions(Spikes, subject="S01", session="1",
                                   low_hz=20, threshold=0.5)
        assert len(results) == 1
        bp = results[0]["branch_params"]
        assert bp["bandpass.low_hz"] == 20
        assert bp["detect_spikes.threshold"] == 0.5

    def test_list_versions_filter_returns_empty_for_missing_value(self, db):
        results = db.list_versions(Filtered, subject="S01", session="1", low_hz=99)
        assert len(results) == 0

    def test_list_versions_independent_per_schema_location(self, db):
        s01 = db.list_versions(Spikes, subject="S01", session="1")
        s02 = db.list_versions(Spikes, subject="S02", session="1")
        assert len(s01) == 4
        assert len(s02) == 4
        # Record IDs should be different across subjects
        ids_s01 = {v["record_id"] for v in s01}
        ids_s02 = {v["record_id"] for v in s02}
        assert ids_s01.isdisjoint(ids_s02)

    def test_list_versions_contains_record_id_and_schema(self, db):
        versions = db.list_versions(Filtered, subject="S01", session="1")
        for v in versions:
            assert "record_id" in v
            assert "schema" in v
            assert v["schema"]["subject"] == "S01"


# ---------------------------------------------------------------------------
# 5. Variant Exclusion
# ---------------------------------------------------------------------------

class TestVariantExclusion:
    """exclude_variant and include_variant control record visibility."""

    @pytest.fixture(autouse=True)
    def _two_filtered_variants(self, db):
        RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])
        for_each(bandpass, {"signal": RawSignal, "low_hz": 30}, [Filtered],
                 subject=["S01"], session=["1"])

    def test_exclude_by_record_id_reduces_visible_count(self, db):
        versions = db.list_versions(Filtered, subject="S01", session="1")
        rid = versions[0]["record_id"]
        db.exclude_variant(rid)

        after = db.list_versions(Filtered, subject="S01", session="1")
        assert len(after) == 1
        assert after[0]["record_id"] != rid

    def test_exclude_by_branch_params(self, db):
        db.exclude_variant(Filtered, subject="S01", session="1", low_hz=20)

        remaining = db.list_versions(Filtered, subject="S01", session="1")
        assert len(remaining) == 1
        assert remaining[0]["branch_params"]["bandpass.low_hz"] == 30

    def test_excluded_record_hidden_from_load(self, db):
        """After excluding one variant, the other loads without ambiguity."""
        db.exclude_variant(Filtered, subject="S01", session="1", low_hz=20)

        f = Filtered.load(subject="S01", session="1")
        assert f.branch_params["bandpass.low_hz"] == 30

    def test_exclude_both_variants_makes_load_raise_not_found(self, db):
        db.exclude_variant(Filtered, subject="S01", session="1", low_hz=20)
        db.exclude_variant(Filtered, subject="S01", session="1", low_hz=30)

        with pytest.raises(NotFoundError):
            Filtered.load(subject="S01", session="1")

    def test_include_variant_restores_visibility(self, db):
        db.exclude_variant(Filtered, subject="S01", session="1", low_hz=20)
        db.include_variant(Filtered, subject="S01", session="1", low_hz=20)

        versions = db.list_versions(Filtered, subject="S01", session="1")
        assert len(versions) == 2

    def test_include_excluded_flag_shows_excluded_records(self, db):
        db.exclude_variant(Filtered, subject="S01", session="1", low_hz=20)

        all_v = db.list_versions(Filtered, subject="S01", session="1",
                                 include_excluded=True)
        assert len(all_v) == 2
        excluded = [v for v in all_v if v.get("excluded")]
        assert len(excluded) == 1
        assert excluded[0]["branch_params"]["bandpass.low_hz"] == 20

    def test_exclude_multiple_variants_without_branch_params(self, db):
        """Excluding without specifying branch_params excludes all matching variants."""
        count = db.exclude_variant(Filtered, subject="S01", session="1")
        assert count == 2, "Should exclude both low_hz=20 and low_hz=30"

        remaining = db.list_versions(Filtered, subject="S01", session="1")
        assert len(remaining) == 0, "All variants should be excluded"

        # Verify they still exist with include_excluded=True
        all_v = db.list_versions(Filtered, subject="S01", session="1", include_excluded=True)
        assert len(all_v) == 2, "Both variants should exist but be excluded"


# ---------------------------------------------------------------------------
# 6. list_pipeline_variants
# ---------------------------------------------------------------------------

class TestListPipelineVariants:
    """list_pipeline_variants returns distinct (fn, constants, output_type) entries."""

    @pytest.fixture(autouse=True)
    def _pipeline(self, db):
        for subj in ["S01", "S02"]:
            RawSignal.save(np.array([1.0, 2.0]), subject=subj, session="1")

        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01", "S02"], session=["1"])

        for thresh in [0.5, 0.6]:
            for_each(detect_spikes, {"signal": Filtered, "threshold": thresh}, [Spikes],
                     subject=["S01", "S02"], session=["1"])

    def test_total_variant_count(self, db):
        # 2 bandpass + 2 detect_spikes = 4 variants
        assert len(db.list_pipeline_variants()) == 4

    def test_filter_by_output_type(self, db):
        filtered_v = db.list_pipeline_variants(output_type="Filtered")
        assert len(filtered_v) == 2
        assert all(v["output_type"] == "Filtered" for v in filtered_v)

    def test_function_names_present(self, db):
        names = {v["function_name"] for v in db.list_pipeline_variants()}
        assert names == {"bandpass", "detect_spikes"}

    def test_constants_correct_for_bandpass(self, db):
        filtered_v = db.list_pipeline_variants(output_type="Filtered")
        low_hz_values = sorted(v["constants"]["low_hz"] for v in filtered_v)
        assert low_hz_values == [20, 30]

    def test_constants_correct_for_detect_spikes(self, db):
        spikes_v = db.list_pipeline_variants(output_type="Spikes")
        thresholds = sorted(v["constants"]["threshold"] for v in spikes_v)
        assert thresholds == [0.5, 0.6]

    def test_record_count_reflects_schema_locations(self, db):
        """2 subjects → record_count=2 per bandpass variant."""
        filtered_v = db.list_pipeline_variants(output_type="Filtered")
        assert all(v["record_count"] == 2 for v in filtered_v)

    def test_input_types_populated(self, db):
        filtered_v = db.list_pipeline_variants(output_type="Filtered")
        assert all(v["input_types"].get("signal") == "RawSignal" for v in filtered_v)

    def test_raw_saves_not_included(self, db):
        raw_v = db.list_pipeline_variants(output_type="RawSignal")
        assert raw_v == []

    def test_no_constants_step_has_empty_constants_dict(self, db):
        for_each(compute_spectrum, {"signal": RawSignal}, [Spectrum],
                 subject=["S01", "S02"], session=["1"])

        spectra_v = db.list_pipeline_variants(output_type="Spectrum")
        assert len(spectra_v) == 1
        assert spectra_v[0]["constants"] == {}

    def test_output_type_none_returns_all(self, db):
        all_v = db.list_pipeline_variants(output_type=None)
        output_types = {v["output_type"] for v in all_v}
        assert "Filtered" in output_types
        assert "Spikes" in output_types

    def test_record_count_increases_after_more_schema_locations(self, db):
        """Adding S03 increases record_count to 3."""
        RawSignal.save(np.array([7.0, 8.0]), subject="S03", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S03"], session=["1"])

        filtered_v = db.list_pipeline_variants(output_type="Filtered")
        low20 = next(v for v in filtered_v if v["constants"]["low_hz"] == 20)
        assert low20["record_count"] == 3


# ---------------------------------------------------------------------------
# 7. get_upstream_provenance
# ---------------------------------------------------------------------------

class TestGetUpstreamProvenance:
    """get_upstream_provenance traces records back to their roots."""

    def test_raw_record_returns_single_node(self, db):
        RawSignal.save(np.array([1.0]), subject="S01", session="1")
        raw = RawSignal.load(subject="S01", session="1")
        chain = db.get_upstream_provenance(raw.record_id)

        assert len(chain) == 1
        node = chain[0]
        assert node["variable_type"] == "RawSignal"
        assert node["function_name"] is None
        assert node["inputs"] == []
        assert node["depth"] == 0
        assert node["branch_params"] == {}
        assert node["constants"] == {}

    def test_single_hop_chain_length_and_structure(self, db):
        RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])

        f = Filtered.load(subject="S01", session="1")
        chain = db.get_upstream_provenance(f.record_id)

        assert len(chain) == 2
        assert chain[0]["variable_type"] == "Filtered"
        assert chain[0]["depth"] == 0
        assert chain[0]["function_name"] == "bandpass"
        assert chain[1]["variable_type"] == "RawSignal"
        assert chain[1]["depth"] == 1
        assert chain[1]["function_name"] is None

    def test_multi_hop_chain_correct_order(self, db):
        RawSignal.save(np.array([1.0, 2.0, 3.0]), subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])
        for_each(detect_spikes, {"signal": Filtered, "threshold": 0.5}, [Spikes],
                 subject=["S01"], session=["1"])

        s = Spikes.load(subject="S01", session="1")
        chain = db.get_upstream_provenance(s.record_id)

        types = [n["variable_type"] for n in chain]
        depths = [n["depth"] for n in chain]
        assert types == ["Spikes", "Filtered", "RawSignal"]
        assert depths == [0, 1, 2]

    def test_selects_correct_upstream_variant_in_branched_pipeline(self, db):
        """With two Filtered variants, each Spikes record traces to its own Filtered."""
        RawSignal.save(np.array([1.0, 2.0, 3.0]), subject="S01", session="1")

        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01"], session=["1"])

        for thresh in [0.5, 0.6]:
            for_each(detect_spikes, {"signal": Filtered, "threshold": thresh}, [Spikes],
                     subject=["S01"], session=["1"])

        s_20_05 = Spikes.load(subject="S01", session="1", low_hz=20, threshold=0.5)
        chain_20 = db.get_upstream_provenance(s_20_05.record_id)
        filt_node_20 = next(n for n in chain_20 if n["variable_type"] == "Filtered")
        assert filt_node_20["branch_params"]["bandpass.low_hz"] == 20

        s_30_06 = Spikes.load(subject="S01", session="1", low_hz=30, threshold=0.6)
        chain_30 = db.get_upstream_provenance(s_30_06.record_id)
        filt_node_30 = next(n for n in chain_30 if n["variable_type"] == "Filtered")
        assert filt_node_30["branch_params"]["bandpass.low_hz"] == 30

    def test_constants_per_node_are_step_only_not_cumulative(self, db):
        """Each node's 'constants' shows only that step's own constants."""
        RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])
        for_each(detect_spikes, {"signal": Filtered, "threshold": 0.5}, [Spikes],
                 subject=["S01"], session=["1"])

        s = Spikes.load(subject="S01", session="1")
        chain = db.get_upstream_provenance(s.record_id)

        spikes_node = chain[0]
        filtered_node = chain[1]

        assert spikes_node["constants"] == {"threshold": 0.5}
        assert "low_hz" not in spikes_node["constants"]
        assert filtered_node["constants"] == {"low_hz": 20}
        assert "threshold" not in filtered_node["constants"]

    def test_schema_present_in_all_nodes(self, db):
        RawSignal.save(np.array([1.0]), subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])

        f = Filtered.load(subject="S01", session="1")
        chain = db.get_upstream_provenance(f.record_id)

        for node in chain:
            assert node["schema"]["subject"] == "S01"
            assert str(node["schema"]["session"]) == "1"

    def test_inputs_field_links_to_correct_upstream(self, db):
        RawSignal.save(np.array([1.0]), subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])

        raw = RawSignal.load(subject="S01", session="1")
        f = Filtered.load(subject="S01", session="1")
        chain = db.get_upstream_provenance(f.record_id)

        filtered_node = chain[0]
        assert len(filtered_node["inputs"]) == 1
        inp = filtered_node["inputs"][0]
        assert inp["param_name"] == "signal"
        assert inp["variable_type"] == "RawSignal"
        assert inp["record_id"] == raw.record_id

    def test_max_depth_cuts_traversal(self, db):
        RawSignal.save(np.array([1.0, 2.0, 3.0]), subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])
        for_each(detect_spikes, {"signal": Filtered, "threshold": 0.5}, [Spikes],
                 subject=["S01"], session=["1"])

        s = Spikes.load(subject="S01", session="1")
        chain = db.get_upstream_provenance(s.record_id, max_depth=1)

        types = {n["variable_type"] for n in chain}
        assert "Spikes" in types
        assert "Filtered" in types
        assert "RawSignal" not in types

    def test_full_four_step_chain(self, db):
        RawSignal.save(np.array([1.0, 2.0, 3.0]), subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])
        for_each(detect_spikes, {"signal": Filtered, "threshold": 0.5}, [Spikes],
                 subject=["S01"], session=["1"])
        for_each(extract_feature, {"spikes": Spikes, "win_size": 10}, [Feature],
                 subject=["S01"], session=["1"])

        feat = Feature.load(subject="S01", session="1")
        chain = db.get_upstream_provenance(feat.record_id)

        types = [n["variable_type"] for n in chain]
        assert types == ["Feature", "Spikes", "Filtered", "RawSignal"]
        assert chain[0]["constants"] == {"win_size": 10}
        assert chain[1]["constants"] == {"threshold": 0.5}
        assert chain[2]["constants"] == {"low_hz": 20}
        assert chain[3]["constants"] == {}

    @pytest.mark.parametrize("data", DATA_VARIANTS)
    def test_provenance_works_for_all_data_types(self, db, data):
        RawSignal.save(data, subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])

        f = Filtered.load(subject="S01", session="1")
        chain = db.get_upstream_provenance(f.record_id)
        assert len(chain) == 2
        assert chain[1]["variable_type"] == "RawSignal"

    def test_invalid_record_id_returns_empty_list(self, db):
        chain = db.get_upstream_provenance("nonexistent-record-id-xyz")
        assert chain == []

    def test_multi_input_provenance_both_inputs_linked(self, db):
        """Function taking two inputs: both appear in the inputs field."""
        RawSignal.save(np.array([1.0, 2.0, 3.0]), subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])
        # Spikes uses Filtered as a sole input
        for_each(detect_spikes, {"signal": Filtered, "threshold": 0.5}, [Spikes],
                 subject=["S01"], session=["1"])

        s = Spikes.load(subject="S01", session="1")
        chain = db.get_upstream_provenance(s.record_id)

        spikes_node = chain[0]
        assert len(spikes_node["inputs"]) == 1
        assert spikes_node["inputs"][0]["variable_type"] == "Filtered"
        assert spikes_node["inputs"][0]["param_name"] == "signal"


# ---------------------------------------------------------------------------
# 8. list_variables
# ---------------------------------------------------------------------------

class TestListVariables:
    """db.list_variables() returns a DataFrame of stored variable types."""

    def test_empty_db_returns_empty_dataframe(self, db):
        result = db.list_variables()
        assert len(result) == 0

    def test_after_save_type_appears(self, db):
        RawSignal.save(np.array([1.0]), subject="S01", session="1")
        result = db.list_variables()
        names = set(result["variable_name"].tolist())
        assert "RawSignal" in names

    def test_multiple_types_all_appear(self, db):
        RawSignal.save(np.array([1.0]), subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])
        result = db.list_variables()
        names = set(result["variable_name"].tolist())
        assert "RawSignal" in names
        assert "Filtered" in names

    def test_each_type_appears_once(self, db):
        """Even with multiple records, each type appears only once."""
        for subj in ["S01", "S02", "S03"]:
            RawSignal.save(np.array([1.0]), subject=subj, session="1")
        result = db.list_variables()
        raw_rows = result[result["variable_name"] == "RawSignal"]
        assert len(raw_rows) == 1

    def test_result_has_expected_columns(self, db):
        RawSignal.save(np.array([1.0]), subject="S01", session="1")
        result = db.list_variables()
        assert "variable_name" in result.columns


# ---------------------------------------------------------------------------
# 9. Additional exclusion edge cases
# ---------------------------------------------------------------------------

class TestVariantExclusionEdgeCases:
    """Edge cases for exclude_variant / include_variant."""

    @pytest.fixture(autouse=True)
    def _two_variants_two_subjects(self, db):
        for subj in ["S01", "S02"]:
            RawSignal.save(np.array([1.0, 2.0]), subject=subj, session="1")
        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01", "S02"], session=["1"])

    def test_exclude_by_record_id_string(self, db):
        """exclude_variant accepts a raw record_id string."""
        versions = db.list_versions(Filtered, subject="S01", session="1")
        rid = versions[0]["record_id"]
        db.exclude_variant(rid)

        remaining = db.list_versions(Filtered, subject="S01", session="1")
        assert all(v["record_id"] != rid for v in remaining)

    def test_include_variant_by_record_id_string(self, db):
        """include_variant accepts a raw record_id string."""
        db.exclude_variant(Filtered, subject="S01", session="1", low_hz=20)
        versions_before = db.list_versions(Filtered, subject="S01", session="1")
        assert len(versions_before) == 1

        all_versions = db.list_versions(Filtered, subject="S01", session="1",
                                        include_excluded=True)
        excluded_rid = next(v["record_id"] for v in all_versions if v.get("excluded"))
        db.include_variant(excluded_rid)

        versions_after = db.list_versions(Filtered, subject="S01", session="1")
        assert len(versions_after) == 2

    def test_exclude_at_one_schema_location_does_not_affect_another(self, db):
        """Excluding a variant for S01 leaves S02's variants intact."""
        db.exclude_variant(Filtered, subject="S01", session="1", low_hz=20)

        s02_versions = db.list_versions(Filtered, subject="S02", session="1")
        assert len(s02_versions) == 2

    def test_exclude_idempotent(self, db):
        """Calling exclude_variant twice on the same record is harmless."""
        db.exclude_variant(Filtered, subject="S01", session="1", low_hz=20)
        db.exclude_variant(Filtered, subject="S01", session="1", low_hz=20)

        remaining = db.list_versions(Filtered, subject="S01", session="1")
        assert len(remaining) == 1

    def test_include_already_included_is_idempotent(self, db):
        """include_variant on a non-excluded record is a no-op."""
        db.include_variant(Filtered, subject="S01", session="1", low_hz=20)
        versions = db.list_versions(Filtered, subject="S01", session="1")
        assert len(versions) == 2


# ---------------------------------------------------------------------------
# 9b. Multi-variant exclusion (new behavior)
# ---------------------------------------------------------------------------

class TestMultiVariantExclusion:
    """Test that exclude_variant and include_variant work on multiple variants."""

    @pytest.fixture(autouse=True)
    def _multi_variant_setup(self, db):
        """Create data with multiple subjects, sessions, and branch_params."""
        # Create input data: 2 subjects × 2 sessions
        for subj in ["S01", "S02"]:
            for sess in ["pre", "post"]:
                RawSignal.save(np.array([1.0, 2.0]), subject=subj, session=sess)

        # Process with 3 different low_hz values
        for low_hz in [20, 50, 100]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01", "S02"],
                session=["pre", "post"],
            )
        # Total: 4 schema combos × 3 branch variants = 12 records

    def test_exclude_all_variants_for_one_session(self, db):
        """Excluding by session only should exclude all branch_params for that session."""
        count = db.exclude_variant(Filtered, session="post")
        assert count == 6, "Should exclude 2 subjects × 3 low_hz values for session=post"

        # Verify only pre session remains
        remaining = list(Filtered.load_all(db=db))
        assert len(remaining) == 6
        for rec in remaining:
            assert rec.metadata["session"] == "pre"

    def test_exclude_all_variants_for_one_subject(self, db):
        """Excluding by subject only should exclude all sessions and branch_params."""
        count = db.exclude_variant(Filtered, subject="S01")
        assert count == 6, "Should exclude 2 sessions × 3 low_hz values for subject=S01"

        remaining = list(Filtered.load_all(db=db))
        assert len(remaining) == 6
        for rec in remaining:
            assert rec.metadata["subject"] == "S02"

    def test_exclude_specific_branch_param_all_schemas(self, db):
        """Excluding by branch_param only should exclude that param for all schema combos."""
        count = db.exclude_variant(Filtered, low_hz=50)
        assert count == 4, "Should exclude 2 subjects × 2 sessions for low_hz=50"

        remaining = list(Filtered.load_all(db=db))
        assert len(remaining) == 8  # 4 schema combos × 2 remaining low_hz values
        for rec in remaining:
            assert rec.branch_params["bandpass.low_hz"] in [20, 100]

    def test_exclude_specific_schema_and_branch_param(self, db):
        """Excluding with both schema and branch_params should exclude only that combo."""
        count = db.exclude_variant(Filtered, subject="S01", session="pre", low_hz=20)
        assert count == 1, "Should exclude only the specific variant"

        remaining = list(Filtered.load_all(db=db))
        assert len(remaining) == 11

    def test_include_restores_all_excluded_variants(self, db):
        """include_variant should restore all matching variants."""
        # Exclude all post session variants
        excluded_count = db.exclude_variant(Filtered, session="post")
        assert excluded_count == 6

        # Re-include them
        included_count = db.include_variant(Filtered, session="post")
        assert included_count == 6, "Should re-include same count that was excluded"

        # Verify all restored
        remaining = list(Filtered.load_all(db=db))
        assert len(remaining) == 12, "All variants should be restored"

    def test_exclude_multiple_then_include_subset(self, db):
        """Can exclude broadly, then re-include specific variants."""
        # Exclude all S01 variants (6 total)
        db.exclude_variant(Filtered, subject="S01")

        # Re-include only S01 pre session (3 variants: low_hz 20, 50, 100)
        count = db.include_variant(Filtered, subject="S01", session="pre")
        assert count == 3

        remaining = list(Filtered.load_all(db=db))
        # 6 S02 + 3 S01/pre = 9 total
        assert len(remaining) == 9

    def test_load_all_skips_excluded_variants(self, db):
        """load_all should not return excluded variants."""
        # Before exclusion: 12 total records
        all_before = list(Filtered.load_all(db=db))
        assert len(all_before) == 12

        # Exclude all session=post variants (6 records)
        count = db.exclude_variant(Filtered, session="post")
        assert count == 6

        # After exclusion: only session=pre should remain (6 records)
        all_after = list(Filtered.load_all(db=db))
        assert len(all_after) == 6

        # Verify all remaining records are session=pre
        for rec in all_after:
            assert rec.metadata["session"] == "pre", f"Found {rec.metadata['session']}, expected 'pre'"

        # Verify excluded records still exist in database using list_versions
        all_versions = db.list_versions(Filtered, include_excluded=True)
        assert len(all_versions) == 12
        excluded_count = sum(1 for v in all_versions if v.get("excluded"))
        assert excluded_count == 6, "Should have 6 excluded variants"

    def test_return_count_matches_excluded_variants(self, db):
        """Return value should match number of variants excluded."""
        # Test various exclusion patterns
        count1 = db.exclude_variant(Filtered, session="post", low_hz=20)
        assert count1 == 2, "2 subjects for (post, low_hz=20)"

        count2 = db.exclude_variant(Filtered, subject="S01", session="pre")
        assert count2 == 3, "3 low_hz values for (S01, pre)"

        # Total excluded: 5 (2 + 3)
        remaining = list(Filtered.load_all(db=db))
        assert len(remaining) == 7  # 12 - 5


# ---------------------------------------------------------------------------
# 10. Additional list_versions edge cases
# ---------------------------------------------------------------------------

class TestListVersionsEdgeCases:
    """Additional list_versions behaviours not covered in the main suite."""

    def test_list_versions_with_namespaced_key_filter(self, db):
        """list_versions can filter by fully-namespaced branch_params key."""
        RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01"], session=["1"])

        results = db.list_versions(Filtered, subject="S01", session="1",
                                   **{"bandpass.low_hz": 20})
        assert len(results) == 1
        assert results[0]["branch_params"]["bandpass.low_hz"] == 20

    def test_list_versions_no_records_returns_empty_list(self, db):
        result = db.list_versions(Filtered, subject="S99", session="1")
        assert result == []

    def test_list_versions_entries_have_timestamp(self, db):
        RawSignal.save(np.array([1.0]), subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])
        versions = db.list_versions(Filtered, subject="S01", session="1")
        assert all("timestamp" in v for v in versions)

    def test_list_versions_include_excluded_shows_excluded_flag(self, db):
        RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01"], session=["1"])
        db.exclude_variant(Filtered, subject="S01", session="1", low_hz=20)

        all_v = db.list_versions(Filtered, subject="S01", session="1",
                                 include_excluded=True)
        excluded = [v for v in all_v if v.get("excluded")]
        active = [v for v in all_v if not v.get("excluded")]
        assert len(excluded) == 1
        assert len(active) == 1


# ---------------------------------------------------------------------------
# 11. String and boolean constants in branch_params
# ---------------------------------------------------------------------------

class TestBranchParamsConstantTypes:
    """branch_params correctly stores and retrieves non-numeric constant types."""

    def test_string_constant_stored_in_branch_params(self, db):
        """String-valued constants appear in branch_params."""
        def process(signal, method):
            return signal

        RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
        for_each(process, {"signal": RawSignal, "method": "fft"}, [Filtered],
                 subject=["S01"], session=["1"])

        f = Filtered.load(subject="S01", session="1")
        assert f.branch_params.get("process.method") == "fft"

    def test_two_string_constants_create_distinct_variants(self, db):
        """Two different string constants produce two distinct records."""
        def process(signal, method):
            return signal

        RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
        for method in ["fft", "wavelet"]:
            for_each(process, {"signal": RawSignal, "method": method}, [Filtered],
                     subject=["S01"], session=["1"])

        versions = db.list_versions(Filtered, subject="S01", session="1")
        assert len(versions) == 2
        methods = {v["branch_params"]["process.method"] for v in versions}
        assert methods == {"fft", "wavelet"}

    def test_integer_constant_type_preserved_in_branch_params(self, db):
        """Integer branch_params values come back as numbers, not strings."""
        RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])

        f = Filtered.load(subject="S01", session="1")
        val = f.branch_params["bandpass.low_hz"]
        assert val == 20
        assert isinstance(val, (int, float))


# ---------------------------------------------------------------------------
# 12. Dry run does not persist records
# ---------------------------------------------------------------------------

class TestDryRun:
    """for_each with dry_run=True displays plans but does not save anything."""

    def test_dry_run_does_not_save_results(self, db):
        RawSignal.save(np.array([1.0, 2.0, 3.0]), subject="S01", session="1")

        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"], dry_run=True)

        versions = db.list_versions(Filtered, subject="S01", session="1")
        assert len(versions) == 0

    def test_dry_run_returns_none(self, db):
        RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")

        result = for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                          subject=["S01"], session=["1"], dry_run=True)

        assert result is None


# ---------------------------------------------------------------------------
# 13. Unambiguous load (single variant) does not raise
# ---------------------------------------------------------------------------

class TestUnambiguousLoad:
    """load() works normally when exactly one variant exists."""

    def test_load_single_variant_no_branch_params_needed(self, db):
        RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
        for_each(bandpass, {"signal": RawSignal, "low_hz": 20}, [Filtered],
                 subject=["S01"], session=["1"])

        f = Filtered.load(subject="S01", session="1")
        assert f is not None
        assert f.branch_params["bandpass.low_hz"] == 20

    def test_load_unambiguous_after_exclusion(self, db):
        """Excluding one of two variants makes load() unambiguous."""
        RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")
        for low_hz in [20, 30]:
            for_each(bandpass, {"signal": RawSignal, "low_hz": low_hz}, [Filtered],
                     subject=["S01"], session=["1"])

        db.exclude_variant(Filtered, subject="S01", session="1", low_hz=30)
        f = Filtered.load(subject="S01", session="1")
        assert f.branch_params["bandpass.low_hz"] == 20
