"""
Tests for aggregation mode with upstream variants (D1 auto-split).

Aggregation-mode for_each AUTO-SPLITS by upstream branch_param signature:
one call per distinct variant group, as if the user had written
EachOf(Variant(...), Variant(...)). Pooling distinct variants into one table
(the pre-D1 behavior) double-counts aggregates and destroys variant identity;
it is now opt-in via AcrossVariants(...), which attaches branch_params as
columns. See docs/claude/endpoints-viz-and-stats-design.md (decision D1).

Covers:
- Full (grand) and partial aggregation split one call per variant group
- Each group's output carries its own (conflict-free) branch_params
- Multi-input Cartesian expansion across split inputs
- Ragged variant groups warn and aggregate partially
- AcrossVariants pooling opt-in (branch_param columns, collision warning,
  full-iteration no-op warning, constructor rules)
- skip_computed binds each variant group to its exact consumed-rid set
  (no cross-group skipping; grown record sets recompute)
- No-variant aggregations behave exactly as before (1:1 expansion)
"""

import warnings as _warnings

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import (
    AcrossVariants,
    BaseVariable,
    ColumnSelection,
    EachOf,
    Merge,
    branch_param,
    configure_database,
    for_each,
)

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


class Scaled(BaseVariable):
    pass


class Aggregated(BaseVariable):
    pass


class Combined(BaseVariable):
    pass


class Feature(BaseVariable):
    pass


# ---------------------------------------------------------------------------
# Pipeline functions
# ---------------------------------------------------------------------------


def bandpass(signal, low_hz):
    """Filter with a parameter that creates variants."""
    return signal * low_hz


def scale(signal, k):
    """Second variant-creating step for multi-input tests."""
    return signal * k


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


def _values(result, col="Aggregated"):
    """Extract scalar output values from a result table, sorted."""
    out = []
    for val in result[col]:
        if isinstance(val, np.ndarray):
            val = val.item() if val.size == 1 else val.sum()
        out.append(float(val))
    return sorted(out)


# ---------------------------------------------------------------------------
# 1. Full (grand) aggregation auto-splits per variant group
# ---------------------------------------------------------------------------


class TestFullAggregationAutoSplit:
    """Full aggregation (no schema keys iterated) splits per variant group."""

    def test_full_aggregation_splits_per_variant_group(self, db):
        """2 upstream variants -> 2 iterations, each aggregating only its group."""
        for subj in ["S01", "S02"]:
            for sess in ["1", "2"]:
                RawSignal.save(np.array([1.0, 2.0, 3.0]), subject=subj, session=sess)

        for low_hz in [20, 30]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01", "S02"],
                session=["1", "2"],
            )

        result = for_each(aggregate_sum, {"signal": Filtered}, [Aggregated], save=False)

        assert result is not None
        assert len(result) == 2, "Expected one iteration per variant group"
        # low_hz=20: 4 locations x (20+40+60) = 480
        # low_hz=30: 4 locations x (30+60+90) = 720
        # (pre-D1 pooling produced a single 1200 -- double-counting both groups)
        assert _values(result) == [480.0, 720.0]

    def test_full_aggregation_saves_one_record_per_group(self, db):
        """Each variant group saves its own output record."""
        for subj in ["S01", "S02"]:
            RawSignal.save(np.array([1.0]), subject=subj, session="1")

        for low_hz in [20, 30]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01", "S02"],
                session=["1"],
            )

        result = for_each(aggregate_sum, {"signal": Filtered}, [Aggregated], save=True)

        assert result is not None
        assert len(result) == 2
        versions = db.list_versions(Aggregated)
        assert len(versions) >= 2, "Expected a distinct saved record per group"

    def test_group_outputs_carry_their_own_branch_params(self, db):
        """Each group's record inherits ONLY its group's branch_params."""
        RawSignal.save(np.array([1.0]), subject="S01", session="1")
        RawSignal.save(np.array([2.0]), subject="S02", session="1")

        for low_hz in [20, 30]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01", "S02"],
                session=["1"],
            )

        for_each(aggregate_sum, {"signal": Filtered}, [Aggregated], save=True)

        agg20 = Aggregated.load(**branch_param("bandpass", low_hz=20))
        agg30 = Aggregated.load(**branch_param("bandpass", low_hz=30))
        assert agg20.branch_params.get("bandpass.low_hz") == 20
        assert agg30.branch_params.get("bandpass.low_hz") == 30
        assert agg20.record_id != agg30.record_id

    def test_split_eliminates_branch_param_conflict_warnings(self, db):
        """Groups are conflict-free by construction: no 'overwritten' warnings."""
        for sess in ["1", "2"]:
            RawSignal.save(np.array([1.0]), subject="S01", session=sess)

        for low_hz in [20, 30]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01"],
                session=["1", "2"],
            )

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            result = for_each(
                aggregate_sum,
                {"signal": Filtered},
                [Aggregated],
                subject=["S01"],
                save=True,
            )

        assert len(result) == 2
        bp_warnings = [
            w
            for w in caught
            if "branch_params key" in str(w.message) and "overwritten" in str(w.message)
        ]
        assert not bp_warnings, (
            "Auto-split aggregation must not merge conflicting branch_params: "
            f"{[str(w.message) for w in bp_warnings]}"
        )


# ---------------------------------------------------------------------------
# 2. Partial aggregation auto-splits per variant group
# ---------------------------------------------------------------------------


class TestPartialAggregationAutoSplit:
    """Partial aggregation (subset of schema keys) splits per variant group."""

    def test_partial_aggregation_splits_per_subject_and_group(self, db):
        """Iterating by subject: one call per subject PER variant group."""
        for subj in ["S01", "S02"]:
            for sess in ["1", "2"]:
                RawSignal.save(np.array([1.0, 2.0]), subject=subj, session=sess)

        for low_hz in [20, 30]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01", "S02"],
                session=["1", "2"],
            )

        result = for_each(
            aggregate_sum,
            {"signal": Filtered},
            [Aggregated],
            subject=["S01", "S02"],
            save=False,
        )

        assert result is not None
        assert len(result) == 4, "Expected 2 subjects x 2 variant groups"
        # Per subject: low_hz=20 -> 2 sessions x (20+40) = 120
        #              low_hz=30 -> 2 sessions x (30+60) = 180
        assert _values(result) == [120.0, 120.0, 180.0, 180.0]

    def test_partial_aggregation_saves_per_group_with_metadata(self, db):
        for subj in ["S01", "S02"]:
            for sess in ["1", "2"]:
                RawSignal.save(np.array([1.0]), subject=subj, session=sess)

        for low_hz in [10, 20]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01", "S02"],
                session=["1", "2"],
            )

        result = for_each(
            aggregate_sum,
            {"signal": Filtered},
            [Aggregated],
            subject=["S01", "S02"],
            save=True,
        )

        assert result is not None
        assert len(result) == 4

        s01_versions = db.list_versions(Aggregated, subject="S01")
        assert len(s01_versions) >= 2, "Expected one record per group at S01"

        agg = Aggregated.load(subject="S01", **branch_param("bandpass", low_hz=10))
        assert agg.metadata["subject"] == "S01"
        assert agg.branch_params.get("bandpass.low_hz") == 10

    def test_uneven_locations_per_subject(self, db):
        """Different subjects with different session counts still split cleanly."""
        for sess in ["1", "2"]:
            RawSignal.save(np.array([1.0, 2.0]), subject="S01", session=sess)
        RawSignal.save(np.array([3.0, 4.0]), subject="S02", session="1")

        for low_hz in [20, 30]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01", "S02"],
                session=["1", "2"],
            )

        result = for_each(
            aggregate_sum,
            {"signal": Filtered},
            [Aggregated],
            subject=["S01", "S02"],
            save=False,
        )

        assert result is not None
        assert len(result) == 4
        # S01: low20 -> 2 x (20+40) = 120 ; low30 -> 2 x (30+60) = 180
        # S02: low20 -> (60+80) = 140    ; low30 -> (90+120) = 210
        by_subject = {}
        for subj, val in zip(result["subject"], result["Aggregated"], strict=False):
            if isinstance(val, np.ndarray):
                val = val.item() if val.size == 1 else val.sum()
            by_subject.setdefault(subj, []).append(float(val))
        assert sorted(by_subject["S01"]) == [120.0, 180.0]
        assert sorted(by_subject["S02"]) == [140.0, 210.0]


# ---------------------------------------------------------------------------
# 3. Multi-input Cartesian expansion
# ---------------------------------------------------------------------------


class TestMultiInputSplit:
    def test_two_split_inputs_expand_cartesian(self, db):
        """Two multi-variant inputs -> signature product (mirrors rid expansion)."""
        RawSignal.save(np.array([1.0]), subject="S01", session="1")

        for low_hz in [20, 30]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01"],
                session=["1"],
            )
        for k in [2, 3]:
            for_each(
                scale,
                {"signal": RawSignal, "k": k},
                [Scaled],
                subject=["S01"],
                session=["1"],
            )

        def add_both(a, b):
            av = (
                a.select_dtypes(include="number").values.sum()
                if isinstance(a, pd.DataFrame)
                else np.asarray(a).sum()
            )
            bv = (
                b.select_dtypes(include="number").values.sum()
                if isinstance(b, pd.DataFrame)
                else np.asarray(b).sum()
            )
            return float(av + bv)

        result = for_each(
            add_both, {"a": Filtered, "b": Scaled}, [Combined], save=False
        )

        assert result is not None
        assert len(result) == 4, "Expected 2 x 2 signature product"
        # (20,30) x (2,3): 22, 23, 32, 33
        assert _values(result, col="Combined") == [22.0, 23.0, 32.0, 33.0]


# ---------------------------------------------------------------------------
# 4. Multi-step pipelines propagate groups
# ---------------------------------------------------------------------------


class TestMultiStepAggregationPipeline:
    def test_two_step_aggregation_pipeline_stays_split(self, db):
        """Aggregation -> aggregation keeps variant groups separate end-to-end."""
        for subj in ["S01", "S02"]:
            for sess in ["1", "2"]:
                RawSignal.save(np.array([1.0, 2.0, 3.0]), subject=subj, session=sess)

        for low_hz in [20, 30]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01", "S02"],
                session=["1", "2"],
            )

        step2 = for_each(
            aggregate_sum,
            {"signal": Filtered},
            [Aggregated],
            subject=["S01", "S02"],
            save=True,
        )
        assert len(step2) == 4  # 2 subjects x 2 groups

        # Grand aggregation over Aggregated: its records carry the inherited
        # bandpass.low_hz, so the split continues -- one Feature per group.
        step3 = for_each(extract_mean, {"signal": Aggregated}, [Feature], save=True)
        assert step3 is not None
        assert len(step3) == 2

        feat20 = Feature.load(**branch_param("bandpass", low_hz=20))
        assert feat20 is not None

    def test_aggregation_after_full_iteration_splits(self, db):
        for subj in ["S01", "S02"]:
            RawSignal.save(np.array([1.0, 2.0]), subject=subj, session="1")

        for low_hz in [20, 30]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01", "S02"],
                session=["1"],
            )

        result = for_each(aggregate_sum, {"signal": Filtered}, [Aggregated], save=True)

        assert result is not None
        assert len(result) == 2
        # low20: 2 subjects x (20+40) = 120 ; low30: 2 x (30+60) = 180
        assert _values(result) == [120.0, 180.0]


# ---------------------------------------------------------------------------
# 5. Provenance per group
# ---------------------------------------------------------------------------


class TestProvenancePerGroup:
    def test_group_record_has_upstream_metadata(self, db):
        """Each group's record stores provenance for ITS contributing rids only."""
        for sess in ["1", "2"]:
            RawSignal.save(np.array([1.0]), subject="S01", session=sess)

        for low_hz in [20, 30]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01"],
                session=["1", "2"],
            )

        for_each(
            aggregate_sum,
            {"signal": Filtered},
            [Aggregated],
            subject=["S01"],
            save=True,
        )

        agg20 = Aggregated.load(subject="S01", **branch_param("bandpass", low_hz=20))
        prov = db.get_upstream_provenance(agg20.record_id)

        agg_nodes = [n for n in prov if n["variable_type"] == "Aggregated"]
        assert len(agg_nodes) == 1
        assert agg_nodes[0]["function_name"] == "aggregate_sum"
        assert agg_nodes[0]["branch_params"].get("bandpass.low_hz") == 20

    def test_introspect_surfaces_group_branch_params(self, db):
        """introspect=True exposes each row's variant-group branch_params."""
        RawSignal.save(np.array([1.0]), subject="S01", session="1")
        for low_hz in [20, 30]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01"],
                session=["1"],
            )

        result = for_each(
            aggregate_sum,
            {"signal": Filtered},
            [Aggregated],
            save=False,
            introspect=True,
        )

        assert len(result) == 2
        assert "_branch_params_signal" in result.columns
        bps = sorted(
            bp.get("bandpass.low_hz") for bp in result["_branch_params_signal"]
        )
        assert bps == [20, 30]
        # Introspection resolves everything through the row's Selection; no
        # internal column survives on the returned table.
        assert not any(str(c).startswith("__") for c in result.columns)

    def test_full_aggregation_stores_upstream_rids_per_group(self, db):
        RawSignal.save(np.array([1.0]), subject="S01", session="1")
        RawSignal.save(np.array([2.0]), subject="S02", session="1")

        for low_hz in [10, 20]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01", "S02"],
                session=["1"],
            )

        for_each(aggregate_sum, {"signal": Filtered}, [Aggregated], save=True)

        agg10 = Aggregated.load(**branch_param("bandpass", low_hz=10))
        prov = db.get_upstream_provenance(agg10.record_id)
        agg_nodes = [n for n in prov if n["variable_type"] == "Aggregated"]
        assert len(agg_nodes) == 1
        assert agg_nodes[0]["function_name"] == "aggregate_sum"


# ---------------------------------------------------------------------------
# 6. Ragged variant groups: warn and aggregate partially
# ---------------------------------------------------------------------------


class TestRaggedVariantGroups:
    def test_ragged_groups_warn_and_aggregate_partially(self, db):
        """A group covering fewer locations warns and aggregates what it has."""
        for sess in ["1", "2"]:
            RawSignal.save(np.array([1.0]), subject="S01", session=sess)

        # low_hz=20 exists at both sessions; low_hz=30 only at session 1.
        for_each(
            bandpass,
            {"signal": RawSignal, "low_hz": 20},
            [Filtered],
            subject=["S01"],
            session=["1", "2"],
        )
        for_each(
            bandpass,
            {"signal": RawSignal, "low_hz": 30},
            [Filtered],
            subject=["S01"],
            session=["1"],
        )

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            result = for_each(
                aggregate_sum,
                {"signal": Filtered},
                [Aggregated],
                subject=["S01"],
                save=False,
            )

        assert len(result) == 2
        # low20: 20+20 = 40 (both sessions); low30: 30 (session 1 only)
        assert _values(result) == [30.0, 40.0]
        ragged = [w for w in caught if "RAGGED" in str(w.message)]
        assert ragged, "Expected a ragged-variant-groups warning"


# ---------------------------------------------------------------------------
# 7. AcrossVariants: explicit pooling opt-in
# ---------------------------------------------------------------------------


class TestAcrossVariants:
    @pytest.mark.filterwarnings("ignore:branch_params key.*overwritten")
    def test_pooled_aggregation_with_branch_param_columns(self, db):
        """AcrossVariants pools all groups and attaches branch_param columns."""
        for subj in ["S01", "S02"]:
            for sess in ["1", "2"]:
                RawSignal.save(np.array([1.0, 2.0, 3.0]), subject=subj, session=sess)

        for low_hz in [20, 30]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01", "S02"],
                session=["1", "2"],
            )

        seen = {}

        def multiverse_sum(signal):
            assert isinstance(signal, pd.DataFrame)
            assert "bandpass.low_hz" in signal.columns, (
                f"branch_param column missing; columns={list(signal.columns)}"
            )
            seen["low_hz_values"] = set(signal["bandpass.low_hz"].tolist())
            # Sum the data column(s) cell-by-cell: array-valued records load
            # as ndarray CELLS (one row per record), so select_dtypes /
            # to_numeric would silently miss them.
            data = signal.drop(columns=["bandpass.low_hz"])
            total = 0.0
            for col in data.columns:
                for v in data[col]:
                    arr = np.asarray(v)
                    if arr.dtype.kind in "if":
                        total += float(arr.sum())
            return total

        result = for_each(
            multiverse_sum,
            {"signal": AcrossVariants(Filtered)},
            [Aggregated],
            save=False,
        )

        assert result is not None
        assert len(result) == 1, "AcrossVariants pools into a single call"
        assert seen["low_hz_values"] == {20, 30}
        # Pooled sum spans BOTH groups: 480 + 720 = 1200 (the pre-D1 pooled
        # value -- now explicitly opted into).
        assert _values(result) == [1200.0]

    def test_across_variants_identity_differs_from_split(self, db):
        assert AcrossVariants(Filtered).to_key() == "AcrossVariants(Filtered)"

    @pytest.mark.filterwarnings("ignore:branch_params key.*overwritten")
    def test_branch_param_column_collision_warns(self, db):
        """A stored data column named like the bp key warns and is preserved.

        The setup deliberately names a DATA column exactly like the namespaced
        branch_param key (``bandpass2.low_hz``), so the setup saves themselves
        fire the (correct) overwritten warning — filtered here; the test's
        subject is the AcrossVariants collision warning.
        """
        RawSignal.save(np.array([1.0, 2.0]), subject="S01", session="1")

        def bandpass2(signal, low_hz):
            # Data column name collides with the namespaced bp key on purpose.
            return pd.DataFrame({"bandpass2.low_hz": np.asarray(signal) * low_hz})

        for low_hz in [20, 30]:
            for_each(
                bandpass2,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01"],
                session=["1"],
            )

        def pooled(signal):
            # With the colliding bp column skipped, the pooled input is a
            # single data column again, which is delivered as an ndarray —
            # accept any shape; only the collision warning matters here.
            return 1.0

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            result = for_each(
                pooled, {"signal": AcrossVariants(Filtered)}, [Aggregated], save=False
            )

        assert len(result) == 1
        collisions = [w for w in caught if "collide" in str(w.message)]
        assert collisions, "Expected a branch_param column collision warning"

    def test_across_variants_full_iteration_warns_noop(self, db):
        RawSignal.save(np.array([1.0]), subject="S01", session="1")
        for low_hz in [20, 30]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01"],
                session=["1"],
            )

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            result = for_each(
                aggregate_sum,
                {"signal": AcrossVariants(Filtered)},
                [Aggregated],
                subject=["S01"],
                session=["1"],
                save=False,
            )

        noop = [w for w in caught if "FULL iteration" in str(w.message)]
        assert noop, "Expected a full-iteration no-op warning"
        # Behaves as bare input: rid expansion -> one combo per variant.
        assert len(result) == 2

    def test_constructor_rules(self, db):
        with pytest.raises(TypeError):
            AcrossVariants(Merge(Filtered, Scaled))
        with pytest.raises(TypeError):
            AcrossVariants(EachOf(Filtered, Scaled))
        with pytest.raises(TypeError):
            AcrossVariants(ColumnSelection(Filtered, ["a"]))
        # Idempotent nesting collapses.
        assert (
            AcrossVariants(AcrossVariants(Filtered)).to_key()
            == "AcrossVariants(Filtered)"
        )


# ---------------------------------------------------------------------------
# 8. No-variant aggregations are unchanged (parity)
# ---------------------------------------------------------------------------


class TestNoVariantParity:
    def test_single_variant_no_split(self, db):
        for subj in ["S01", "S02"]:
            for sess in ["1", "2"]:
                RawSignal.save(np.array([1.0, 2.0]), subject=subj, session=sess)

        for_each(
            bandpass,
            {"signal": RawSignal, "low_hz": 20},
            [Filtered],
            subject=["S01", "S02"],
            session=["1", "2"],
        )

        result = for_each(
            aggregate_sum,
            {"signal": Filtered},
            [Aggregated],
            subject=["S01", "S02"],
            save=True,
        )

        assert result is not None
        assert len(result) == 2, "One group -> one call per subject, as before"
        assert _values(result) == [120.0, 120.0]

        agg = Aggregated.load(subject="S01")
        assert agg.branch_params.get("bandpass.low_hz") == 20

    def test_no_upstream_branch_params(self, db):
        for subj in ["S01", "S02"]:
            RawSignal.save(np.array([1.0, 2.0]), subject=subj, session="1")

        result = for_each(
            aggregate_sum,
            {"signal": RawSignal},
            [Aggregated],
            subject=["S01", "S02"],
            save=True,
        )

        assert result is not None
        assert len(result) == 2

        agg = Aggregated.load(subject="S01")
        assert agg.branch_params == {}

    def test_grand_aggregation_no_variants_single_call(self, db):
        """Zero iterated keys + one variant group -> exactly one call."""
        for subj in ["S01", "S02"]:
            for sess in ["1", "2"]:
                RawSignal.save(np.array([1.0]), subject=subj, session=sess)

        result = for_each(aggregate_sum, {"signal": RawSignal}, [Aggregated], save=True)

        assert result is not None
        assert len(result) == 1
        assert _values(result) == [4.0]

    def test_many_variants_split_into_many_calls(self, db):
        for sess in ["1", "2"]:
            RawSignal.save(np.array([1.0]), subject="S01", session=sess)

        for low_hz in [10, 20, 30, 40, 50]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01"],
                session=["1", "2"],
            )

        result = for_each(
            aggregate_sum,
            {"signal": Filtered},
            [Aggregated],
            subject=["S01"],
            save=True,
        )

        assert result is not None
        assert len(result) == 5
        # Each group: 2 sessions x (1.0 * low_hz)
        assert _values(result) == [20.0, 40.0, 60.0, 80.0, 100.0]

        agg30 = Aggregated.load(subject="S01", **branch_param("bandpass", low_hz=30))
        assert agg30.branch_params.get("bandpass.low_hz") == 30


# ---------------------------------------------------------------------------
# 8b. distribute under aggregation (regression: vsig keys must be invisible
#     to distribute's schema-level resolution)
# ---------------------------------------------------------------------------


class TestDistributeWithAggregation:
    def test_distribute_ignores_vsig_schema_extension(self, db):
        """Aggregation extends the scifor schema with __vsig_*; distribute
        must still resolve 'session' (the real level below 'subject') as its
        target rather than seeing the discriminator as the deepest key.
        Mirrors MATLAB's test_distribute_from_loaded_variable."""
        RawSignal.save(np.array([1.0, 2.0, 3.0]), subject="S01")

        def double_values(signal):
            return np.asarray(signal).ravel() * 2

        result = for_each(
            double_values,
            {"signal": RawSignal},
            [Filtered],
            subject=["S01"],
            distribute=True,
            save=True,
        )

        assert result is not None and len(result) == 3
        # Row k of [2, 4, 6] lands at session k.
        for sess, expected in zip(["1", "2", "3"], [2.0, 4.0, 6.0], strict=False):
            rec = Filtered.load(subject="S01", session=sess)
            assert float(np.asarray(rec.data).ravel()[0]) == expected


# ---------------------------------------------------------------------------
# 9. skip_computed binds variant groups to their consumed-rid sets
# ---------------------------------------------------------------------------


class TestSkipComputedWithSplit:
    def test_no_cross_group_skip(self, db):
        """Second run skips BOTH groups; a NEW group computes (no cross-skip)."""
        for sess in ["1", "2"]:
            RawSignal.save(np.array([1.0]), subject="S01", session=sess)

        for low_hz in [20, 30]:
            for_each(
                bandpass,
                {"signal": RawSignal, "low_hz": low_hz},
                [Filtered],
                subject=["S01"],
                session=["1", "2"],
            )

        calls = {"n": 0}

        def agg_counting(signal):
            calls["n"] += 1
            return aggregate_sum(signal)

        kwargs = {
            "inputs": {"signal": Filtered},
            "outputs": [Aggregated],
            "subject": ["S01"],
            "save": True,
            "skip_computed": True,
        }

        for_each(agg_counting, **kwargs)
        assert calls["n"] == 2, "First run computes both groups"

        for_each(agg_counting, **kwargs)
        assert calls["n"] == 2, "Second identical run skips both groups"

        # Add a THIRD variant upstream: only the new group may compute.
        for_each(
            bandpass,
            {"signal": RawSignal, "low_hz": 40},
            [Filtered],
            subject=["S01"],
            session=["1", "2"],
        )

        for_each(agg_counting, **kwargs)
        assert calls["n"] == 3, (
            "New variant group must compute exactly once; existing groups skip"
        )

    def test_grown_record_set_recomputes(self, db):
        """Aggregation whose underlying record set grew must NOT skip."""
        for sess in ["1", "2"]:
            RawSignal.save(np.array([1.0]), subject="S01", session=sess)

        calls = {"n": 0}

        def agg_counting(signal):
            calls["n"] += 1
            return aggregate_sum(signal)

        kwargs = {
            "inputs": {"signal": RawSignal},
            "outputs": [Aggregated],
            "subject": ["S01"],
            "save": True,
            "skip_computed": True,
        }

        for_each(agg_counting, **kwargs)
        assert calls["n"] == 1

        for_each(agg_counting, **kwargs)
        assert calls["n"] == 1, "Unchanged record set skips"

        RawSignal.save(np.array([1.0]), subject="S01", session="3")

        for_each(agg_counting, **kwargs)
        assert calls["n"] == 2, (
            "A grown record set (new session) must recompute the aggregate"
        )


# ---------------------------------------------------------------------------
# Save-kwarg variant alignment (align, not cross-product)
# ---------------------------------------------------------------------------


class TestSaveKwargAlignment:
    """Iterating a key that is ALSO a save-time non-schema kwarg must align
    each combo with its matching ``__save__.<key>`` variant group, not
    cross-product combos with every group (regression: scihist
    test_generates_file cache-hit ran 4 calls instead of 2, pairing the
    run="A" iteration with the __save__.run="B" group and vice versa)."""

    def test_iterated_save_kwarg_aligns_not_crosses(self, db):
        # Two variants at the SAME schema location (subject=1, session=NULL),
        # discriminated only by the non-schema save kwarg `run`.
        RawSignal.save(np.array([1.0, 2.0]), subject="1", run="A")
        RawSignal.save(np.array([3.0, 4.0]), subject="1", run="B")

        calls = []

        def total_signal(data):
            vals = np.concatenate([np.asarray(v).ravel() for v in data["RawSignal"]])
            calls.append(float(vals.sum()))
            return float(vals.sum())

        for_each(
            total_signal,
            inputs={"data": RawSignal},
            outputs=[Aggregated],
            subject=["1"],
            run=["A", "B"],
        )

        # One call per (combo, matching group): run=A sees only [1,2],
        # run=B only [3,4]. The pre-fix cross-product made 4 calls, two of
        # them on the wrong group's rows.
        assert sorted(calls) == [3.0, 7.0]

    def test_second_run_skips_aligned_groups(self, db):
        """skip_computed must hit on the aligned records (the original
        scihist cache-hit failure mode)."""
        RawSignal.save(np.array([1.0, 2.0]), subject="1", run="A")
        RawSignal.save(np.array([3.0, 4.0]), subject="1", run="B")

        calls = {"n": 0}

        def total_signal(data):
            calls["n"] += 1
            vals = np.concatenate([np.asarray(v).ravel() for v in data["RawSignal"]])
            return float(vals.sum())

        kwargs = {
            "inputs": {"data": RawSignal},
            "outputs": [Aggregated],
            "subject": ["1"],
            "run": ["A", "B"],
            "skip_computed": True,
        }
        for_each(total_signal, **kwargs)
        assert calls["n"] == 2

        for_each(total_signal, **kwargs)
        assert calls["n"] == 2, "second run must skip both aligned groups"
