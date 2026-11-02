"""Integration tests: VariableFilter/ColumnFilter/InFilter applied to Merge inputs in for_each.

Coverage:
- VariableFilter at same level as Merge constituents filters rows correctly.
- VariableFilter coarser than constituents expands and filters correctly.
- NotFilter (~) with Merge gives the correct complement.
- CompoundFilter (&) with Merge filters correctly.
- Coverage error raised when filter is missing data for a schema_id that
  survives the Merge inner join.
- No false-positive error when filter is missing data for a schema_id that
  is eliminated by the Merge inner join.
- _validate_filter_coverage with target_schema_ids_override uses the override
  set rather than the target table's schema_ids.

Note on as_table=True + no explicit metadata iterables:
    Passing no subject/trial kwargs puts for_each in aggregation mode; the
    function is called ONCE with the full (filtered) Merge table rather than
    once per (subject, trial) combo. Tests therefore assert on the row count of
    captured[0], not on the number of calls.

    Unfiltered Merge (GaitData × ForceData, 2 subjects × 2 trials) = 4 rows.
    After Side=="L" (trial=1 only): 2 rows.
    After SubjectGroup=="A" (subject=1 only): 2 rows.
    After (Side=="L") & (SubjectGroup=="A"): 1 row.
"""

import sys
from pathlib import Path

import pytest

_root = Path(__file__).parent.parent
sys.path.insert(0, str(_root / "src"))

from scidb.run.foreach import Merge

from scidb import BaseVariable, Variant, branch_param, configure_database, for_each

# ===========================================================================
# Variable classes
# ===========================================================================


class GaitData(BaseVariable):
    """Trial-level gait measurement (scalar)."""

    schema_version = 1


class ForceData(BaseVariable):
    """Trial-level force measurement (scalar)."""

    schema_version = 1


class Side(BaseVariable):
    """Trial-level side label ("L" or "R")."""

    schema_version = 1


class SubjectGroup(BaseVariable):
    """Subject-level group label (coarser than trial)."""

    schema_version = 1


# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
def full_merge_db(tmp_path):
    """DB with [subject, trial] schema.

    GaitData and ForceData: subject={1,2}, trial={1,2} — all 4 combos.
    Side: trial=1→"L", trial=2→"R" for both subjects.
    SubjectGroup: subject=1→"A", subject=2→"B".

    Merge effective ids = all 4 trial combos (inner join of full sets).
    Unfiltered row count = 4.
    """
    db = configure_database(tmp_path / "test.duckdb", ["subject", "trial"])
    for subj in (1, 2):
        for trial in (1, 2):
            GaitData.save(float(subj * 10 + trial), subject=subj, trial=trial)
            ForceData.save(float(subj * 100 + trial), subject=subj, trial=trial)
    Side.save("L", subject=1, trial=1)
    Side.save("R", subject=1, trial=2)
    Side.save("L", subject=2, trial=1)
    Side.save("R", subject=2, trial=2)
    SubjectGroup.save("A", subject=1)
    SubjectGroup.save("B", subject=2)
    yield db
    db.close()


@pytest.fixture
def partial_force_db(tmp_path):
    """DB where ForceData is missing subject=2,trial=2.

    GaitData: all 4 combos.
    ForceData: subject=1,trial=1; subject=1,trial=2; subject=2,trial=1 (missing sub=2,tr=2).
    Side: sub=1,tr=1→"L"; sub=1,tr=2→"R"; sub=2,tr=1→"L" (also missing sub=2,tr=2).
    Merge effective ids = {sub=1,tr=1; sub=1,tr=2; sub=2,tr=1} — 3 rows unfiltered.
    Side covers exactly the merge result → no coverage error.
    After Side=="L": 2 rows (sub=1,tr=1; sub=2,tr=1).
    """
    db = configure_database(tmp_path / "partial.duckdb", ["subject", "trial"])
    for subj in (1, 2):
        for trial in (1, 2):
            GaitData.save(float(subj * 10 + trial), subject=subj, trial=trial)
    ForceData.save(11.0, subject=1, trial=1)
    ForceData.save(12.0, subject=1, trial=2)
    ForceData.save(21.0, subject=2, trial=1)
    # ForceData missing subject=2, trial=2 intentionally
    Side.save("L", subject=1, trial=1)
    Side.save("R", subject=1, trial=2)
    Side.save("L", subject=2, trial=1)
    # Side also missing subject=2, trial=2 — same gap as ForceData
    yield db
    db.close()


# ===========================================================================
# VariableFilter — same level as constituents
# ===========================================================================


class TestVariableFilterMerge:
    def test_variable_filter_filters_merge_rows(self, full_merge_db):
        """VariableFilter (Side=="L") must exclude trial=2 rows from Merge table.

        Unfiltered Merge = 4 rows. After Side=="L" (trial=1): 2 rows remain.
        """
        captured = []

        def collect(inputVal):
            captured.append(inputVal.copy())
            return inputVal

        for_each(
            collect,
            inputs={"inputVal": Merge(GaitData, ForceData)},
            outputs=[GaitData],
            as_table=True,
            where=Side == "L",
            save=False,
        )

        assert captured, "function was never called"
        tbl = captured[0]
        # Only trial=1 rows kept (Side=L); trial=2 (Side=R) excluded
        assert len(tbl) == 2, f"Expected 2 rows (trial=1 only), got {len(tbl)}"
        if "trial" in tbl.columns:
            assert {str(t) for t in tbl["trial"].unique()} == {"1"}, (
                f"trial=2 leaked into filtered Merge: {tbl['trial'].unique()}"
            )

    def test_variable_filter_r_side_filters_merge_rows(self, full_merge_db):
        """VariableFilter (Side=="R") keeps only trial=2 rows (2 of 4)."""
        captured = []

        def collect(inputVal):
            captured.append(inputVal.copy())
            return inputVal

        for_each(
            collect,
            inputs={"inputVal": Merge(GaitData, ForceData)},
            outputs=[GaitData],
            as_table=True,
            where=Side == "R",
            save=False,
        )

        assert captured
        tbl = captured[0]
        assert len(tbl) == 2, f"Expected 2 rows (trial=2 only), got {len(tbl)}"
        if "trial" in tbl.columns:
            assert {str(t) for t in tbl["trial"].unique()} == {"2"}, (
                f"trial=1 leaked into Side=R filtered Merge: {tbl['trial'].unique()}"
            )


# ===========================================================================
# VariableFilter — coarser than constituents (subject-level filter)
# ===========================================================================


class TestCoarserVariableFilterMerge:
    def test_coarser_filter_expands_and_filters_merge(self, full_merge_db):
        """SubjectGroup=="A" (subject-level) keeps only subject=1 rows (2 of 4)."""
        captured = []

        def collect(inputVal):
            captured.append(inputVal.copy())
            return inputVal

        for_each(
            collect,
            inputs={"inputVal": Merge(GaitData, ForceData)},
            outputs=[GaitData],
            as_table=True,
            where=SubjectGroup == "A",
            save=False,
        )

        assert captured
        tbl = captured[0]
        # subject=1 only (2 trials → 2 rows)
        assert len(tbl) == 2, f"Expected 2 rows (subject=1, trials 1+2), got {len(tbl)}"
        if "subject" in tbl.columns:
            assert {str(s) for s in tbl["subject"].unique()} == {"1"}, (
                f"subject=2 leaked into group-A filtered Merge: {tbl['subject'].unique()}"
            )


# ===========================================================================
# NotFilter with Merge
# ===========================================================================


class TestNotFilterMerge:
    def test_not_filter_gives_complement(self, full_merge_db):
        """~(Side=="L") should keep only trial=2 rows (2 of 4)."""
        captured = []

        def collect(inputVal):
            captured.append(inputVal.copy())
            return inputVal

        for_each(
            collect,
            inputs={"inputVal": Merge(GaitData, ForceData)},
            outputs=[GaitData],
            as_table=True,
            where=~(Side == "L"),
            save=False,
        )

        assert captured
        tbl = captured[0]
        assert len(tbl) == 2, f"Expected 2 rows (trial=2, NOT Side=L), got {len(tbl)}"
        if "trial" in tbl.columns:
            assert {str(t) for t in tbl["trial"].unique()} == {"2"}, (
                f"trial=1 leaked into NOT(Side=L) Merge: {tbl['trial'].unique()}"
            )


# ===========================================================================
# CompoundFilter with Merge
# ===========================================================================


class TestCompoundFilterMerge:
    def test_and_filter_narrows_merge(self, full_merge_db):
        """(Side=="L") & (SubjectGroup=="A") → only subject=1,trial=1 (1 of 4 rows)."""
        captured = []

        def collect(inputVal):
            captured.append(inputVal.copy())
            return inputVal

        for_each(
            collect,
            inputs={"inputVal": Merge(GaitData, ForceData)},
            outputs=[GaitData],
            as_table=True,
            where=(Side == "L") & (SubjectGroup == "A"),
            save=False,
        )

        assert captured
        tbl = captured[0]
        assert len(tbl) == 1, f"Expected 1 row (subject=1,trial=1 only), got {len(tbl)}"


# ===========================================================================
# Coverage error — filter missing for schema_id that survives Merge inner join
# ===========================================================================


class TestMergeFilterCoverageError:
    def test_missing_filter_data_for_merge_result_raises(self, tmp_path):
        """Side missing for sub=2,tr=* which IS in the Merge result → ValueError."""
        db = configure_database(tmp_path / "cov_err.duckdb", ["subject", "trial"])
        try:
            for subj in (1, 2):
                for trial in (1, 2):
                    GaitData.save(float(subj + trial), subject=subj, trial=trial)
                    ForceData.save(float(subj + trial + 10), subject=subj, trial=trial)
            # Side only covers sub=1 — sub=2 is missing and IS in the Merge result
            Side.save("L", subject=1, trial=1)
            Side.save("R", subject=1, trial=2)

            with pytest.raises(ValueError, match="missing data"):
                for_each(
                    lambda inputVal: inputVal,
                    inputs={"inputVal": Merge(GaitData, ForceData)},
                    outputs=[GaitData],
                    as_table=True,
                    where=Side == "L",
                    save=False,
                )
        finally:
            db.close()


# ===========================================================================
# No false-positive — filter missing only for schema_id eliminated by Merge
# ===========================================================================


class TestMergeFilterNoFalsePositive:
    def test_no_error_when_filter_gap_matches_merge_gap(self, partial_force_db):
        """Side missing sub=2,tr=2 which is also absent from Merge → no error.

        The coverage gap in Side exactly matches the ForceData gap; sub=2,tr=2
        is eliminated by the Merge inner join, so no coverage error is raised.
        """
        captured = []

        def collect(inputVal):
            captured.append(inputVal.copy())
            return inputVal

        # Must NOT raise
        for_each(
            collect,
            inputs={"inputVal": Merge(GaitData, ForceData)},
            outputs=[GaitData],
            as_table=True,
            where=Side == "L",
            save=False,
        )

        assert captured, "function was never called"
        # Merge effective = 3 rows; Side=="L" keeps 2 (sub=1,tr=1 and sub=2,tr=1)
        tbl = captured[0]
        assert len(tbl) == 2, (
            f"Expected 2 rows (trial=1 combos within partial Merge), got {len(tbl)}"
        )
        if "trial" in tbl.columns:
            assert {str(t) for t in tbl["trial"].unique()} == {"1"}, (
                f"trial=2 leaked: {tbl['trial'].unique()}"
            )


# ===========================================================================
# _validate_filter_coverage with target_schema_ids_override
# ===========================================================================


class TestValidateFilterCoverageOverride:
    def test_override_uses_provided_ids_not_table(self, tmp_path):
        """target_schema_ids_override replaces the table lookup for coverage target."""
        from scidb.model.filters import (
            _get_all_schema_ids_for_variable,
            _validate_filter_coverage,
        )

        db = configure_database(tmp_path / "cov_override.duckdb", ["subject", "trial"])
        try:
            # Side saved for subject=1 only; GaitData for both subjects
            Side.save("L", subject=1, trial=1)
            Side.save("R", subject=1, trial=2)
            GaitData.save(1.0, subject=1, trial=1)
            GaitData.save(2.0, subject=1, trial=2)
            GaitData.save(3.0, subject=2, trial=1)

            filter_ids = _get_all_schema_ids_for_variable(db, Side.table_name())

            # Without override: target (GaitData) includes sub=2,tr=1 → coverage fails
            with pytest.raises(ValueError, match="missing data"):
                _validate_filter_coverage(
                    db,
                    Side,
                    GaitData,
                    Side.table_name(),
                    GaitData.table_name(),
                    filter_level_idx=1,
                    target_level_idx=1,
                )

            # With override equal to filter_ids (sub=1 only) → no missing locations
            _validate_filter_coverage(
                db,
                Side,
                GaitData,
                Side.table_name(),
                GaitData.table_name(),
                filter_level_idx=1,
                target_level_idx=1,
                target_schema_ids_override=filter_ids,
            )
        finally:
            db.close()


# ===========================================================================
# Multiple for_each-computed variants of the SAME variable at the SAME schema
# locations, distinguished by their producing invocation's constant (``factor``).
#
# Regression for: a Merge constituent pinned with Variant(..., factor=v) must
# select the single matching variant — not every variant sharing the same schema
# keys. (Previously this was driven by the where= provenance string; §10's where=
# redesign makes variant identity semantic — by consumed inputs — so a constant,
# applied via branch_param/Variant, is what distinguishes same-input variants.)
# ===========================================================================


class Measure(BaseVariable):
    """Trial-level source measurement (scalar)."""

    schema_version = 1


class Partner(BaseVariable):
    """Trial-level merge partner (scalar)."""

    schema_version = 1


class Derived(BaseVariable):
    """Trial-level output computed by for_each in two constant (factor) variants."""

    schema_version = 1


def _derive(signal, factor):
    """Trivial pipeline fn: scale the source so each variant is distinguishable."""
    return signal * factor


@pytest.fixture
def variant_merge_db(tmp_path):
    """DB with two ``Derived`` variants over the same [subject, trial] combos.

    Running for_each twice with a different ``factor`` constant produces two
    ``Derived`` records per combo that coexist as distinct variants (distinguished
    by their producing invocation's constant, i.e. ``derived_branch_params``):

        variant factor=1   → Derived == Measure
        variant factor=100 → Derived == Measure * 100

    (Under the §10 where= redesign, two variants that consumed the *same* inputs are
    the same variant — so a constant, not a same-input where= filter, is what makes
    these two distinct. Selection is therefore by ``factor`` branch_param.)

    Partner is a single-variant trial-level variable used as the Merge partner.
    """
    db = configure_database(tmp_path / "variants.duckdb", ["subject", "trial"])
    for subj in (1, 2):
        for trial in (1, 2):
            Measure.save(float(subj * 10 + trial), subject=subj, trial=trial)
            Partner.save(float(subj), subject=subj, trial=trial)

    for factor in (1, 100):
        for_each(
            _derive,
            inputs={"signal": Measure, "factor": factor},
            outputs=[Derived],
            subject=[1, 2],
            trial=[1, 2],
        )
    yield db
    db.close()


class TestMergeSelectsVariantByBranchParam:
    def test_direct_load_selects_one_variant(self, variant_merge_db):
        """Direct .load() pinned to factor=1 returns one record per combo.

        branch_param(...) builds the namespaced dict; unpack it into the load
        kwargs (non-schema kwargs become the branch_params filter)."""
        records = Derived.load(**branch_param("_derive", factor=1))
        assert len(records) == 4, (
            f"Expected 4 records (one variant × 4 combos), got {len(records)}"
        )

    def test_merge_pins_one_variant(self, variant_merge_db):
        """Merge(Partner, Variant(Derived, factor=1)) keeps only the factor=1
        ``Derived`` variant — 4 rows, not 8."""
        captured = []

        def collect(inputVal):
            captured.append(inputVal.copy())
            return inputVal

        for_each(
            collect,
            inputs={
                "inputVal": Merge(Partner, Variant(Derived, fn="_derive", factor=1))
            },
            outputs=[Derived],
            as_table=True,
            save=False,
        )

        assert captured, "function was never called"
        tbl = captured[0]
        assert len(tbl) == 4, (
            f"Expected 4 rows (one Derived variant × 4 combos); got {len(tbl)} "
            "— the other variant leaked through (branch_param pin not applied)."
        )
        # The kept variant is factor=1 (Derived == Measure), all values < 100.
        if "Derived" in tbl.columns:
            vals = [float(v) for v in tbl["Derived"]]
            assert all(v < 100 for v in vals), (
                f"factor=100 variant leaked into the merged table: {vals}"
            )

    def test_merge_pins_other_variant(self, variant_merge_db):
        """Pinning the Derived constituent to factor=100 selects the OTHER variant,
        proving the constant — not schema keys — drives selection."""
        captured = []

        def collect(inputVal):
            captured.append(inputVal.copy())
            return inputVal

        for_each(
            collect,
            inputs={
                "inputVal": Merge(Partner, Variant(Derived, fn="_derive", factor=100))
            },
            outputs=[Derived],
            as_table=True,
            save=False,
        )

        assert captured
        tbl = captured[0]
        assert len(tbl) == 4, f"Expected 4 rows, got {len(tbl)}"
        if "Derived" in tbl.columns:
            vals = [float(v) for v in tbl["Derived"]]
            assert all(v >= 100 for v in vals), (
                f"factor=1 variant leaked into the factor=100 merged table: {vals}"
            )
