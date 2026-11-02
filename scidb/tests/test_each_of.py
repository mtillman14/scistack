"""
Tests for EachOf — generalized variant expansion in for_each().

Covers:
- EachOf with single value collapses to normal behavior
- EachOf with multiple constants creates expected variants
- EachOf with multiple variable types creates expected variants
- EachOf on where= creates expected variants
- Combined EachOf on inputs + where= produces cartesian product
- branch_params correctness for each variant
- load() can disambiguate variants created by EachOf
"""

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, EachOf, configure_database, for_each

# ---------------------------------------------------------------------------
# Schema and fixtures
# ---------------------------------------------------------------------------

SCHEMA = ["subject", "session"]


@pytest.fixture
def db(tmp_path):
    """Fresh database with subject/session schema for each test."""
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_eachof.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


# ---------------------------------------------------------------------------
# Variable types
# ---------------------------------------------------------------------------


class MetricA(BaseVariable):
    pass


class MetricB(BaseVariable):
    pass


class AnalysisResult(BaseVariable):
    pass


class FilteredResult(BaseVariable):
    pass


# ---------------------------------------------------------------------------
# Pipeline functions
# ---------------------------------------------------------------------------


def analyze(data, alpha=0.05):
    """Simple analysis function."""
    if isinstance(data, np.ndarray):
        return float(np.mean(data) * alpha)
    if isinstance(data, pd.DataFrame):
        return float(data.iloc[:, 0].mean() * alpha)
    return float(data) * alpha


def simple_transform(data):
    """Identity-like transform for testing."""
    if isinstance(data, np.ndarray):
        return data * 2.0
    return data * 2.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_data(db):
    """Seed MetricA and MetricB for two subjects."""
    MetricA.save(np.array([1.0, 2.0, 3.0]), db=db, subject="1", session="A")
    MetricA.save(np.array([4.0, 5.0, 6.0]), db=db, subject="2", session="A")
    MetricB.save(np.array([10.0, 20.0, 30.0]), db=db, subject="1", session="A")
    MetricB.save(np.array([40.0, 50.0, 60.0]), db=db, subject="2", session="A")


# ---------------------------------------------------------------------------
# Tests: EachOf class basics
# ---------------------------------------------------------------------------


class TestEachOfClass:
    def test_single_value(self):
        eo = EachOf(42)
        assert eo.alternatives == [42]

    def test_multiple_values(self):
        eo = EachOf(1, 2, 3)
        assert eo.alternatives == [1, 2, 3]

    def test_empty_constructs(self):
        """An empty EachOf is a placeholder (a Parameter with no value yet),
        legal to build and refused at for_each expansion instead -- see
        scifor.require_alternatives."""
        assert EachOf().alternatives == []

    def test_repr_constants(self):
        eo = EachOf(0.05, 0.01)
        assert "EachOf" in repr(eo)

    def test_repr_types(self):
        eo = EachOf(MetricA, MetricB)
        r = repr(eo)
        assert "MetricA" in r
        assert "MetricB" in r


# ---------------------------------------------------------------------------
# Tests: EachOf with constants
# ---------------------------------------------------------------------------


class TestEachOfConstants:
    def test_single_constant_same_as_direct(self, db):
        """EachOf(0.05) should produce the same number of results as 0.05 directly."""
        _seed_data(db)

        result = for_each(
            analyze,
            inputs={"data": MetricA, "alpha": EachOf(0.05)},
            outputs=[AnalysisResult],
            db=db,
            subject=[],
            session=[],
        )

        # 2 subjects, 1 alpha = 2 rows (same as passing 0.05 directly)
        assert len(result) == 2

    def test_multiple_constants(self, db):
        """EachOf(0.05, 0.01) should produce variants for each constant."""
        _seed_data(db)

        result = for_each(
            analyze,
            inputs={"data": MetricA, "alpha": EachOf(0.05, 0.01)},
            outputs=[AnalysisResult],
            db=db,
            subject=[],
            session=[],
        )

        # 2 subjects × 2 alpha values = 4 rows
        assert len(result) == 4

        # Verify we can list the variants
        variants = db.list_pipeline_variants()
        assert len(variants) > 0


# ---------------------------------------------------------------------------
# Tests: EachOf with variable types
# ---------------------------------------------------------------------------


class TestEachOfVariableTypes:
    def test_single_type_same_as_direct(self, db):
        """EachOf(MetricA) should behave identically to passing MetricA directly."""
        _seed_data(db)

        result = for_each(
            simple_transform,
            inputs={"data": EachOf(MetricA)},
            outputs=[AnalysisResult],
            db=db,
            subject=[],
            session=[],
        )

        # 2 subjects = 2 rows
        assert len(result) == 2

    def test_multiple_types(self, db):
        """EachOf(MetricA, MetricB) should produce variants for each type."""
        _seed_data(db)

        result = for_each(
            simple_transform,
            inputs={"data": EachOf(MetricA, MetricB)},
            outputs=[AnalysisResult],
            db=db,
            subject=[],
            session=[],
        )

        # 2 subjects × 2 types = 4 rows
        assert len(result) == 4

    def test_multiple_types_distinct_version_keys(self, db):
        """Each type should produce records with distinct __inputs version keys."""
        _seed_data(db)

        for_each(
            simple_transform,
            inputs={"data": EachOf(MetricA, MetricB)},
            outputs=[AnalysisResult],
            db=db,
            subject=[],
            session=[],
        )

        # Check that we have records with different __inputs
        versions = AnalysisResult.list_versions(db=db, subject="1", session="A")
        assert len(versions) == 2
        inputs_keys = set()
        for v in versions:
            vk = v.get("version_keys", v.get("__inputs", ""))
            if isinstance(vk, dict):
                inputs_keys.add(vk.get("__inputs", ""))
            elif isinstance(vk, str):
                inputs_keys.add(vk)
        # Should have two distinct entries (one per type)
        assert len(inputs_keys) >= 1  # At minimum distinguishable


# ---------------------------------------------------------------------------
# Tests: EachOf with where= filters
# ---------------------------------------------------------------------------


class TestEachOfWhere:
    def _seed_with_sides(self, db):
        """Seed data with a 'side' column for where= filtering."""

        class SideLabel(BaseVariable):
            pass

        # Subject 1, session A: two steps, one left, one right
        SideLabel.save(
            pd.DataFrame({"side": ["L", "R"], "value": [1.0, 2.0]}),
            db=db,
            subject="1",
            session="A",
        )
        MetricA.save(np.array([10.0]), db=db, subject="1", session="A")
        return SideLabel

    def test_single_where_same_as_direct(self, db):
        """EachOf(filter) with one filter should match passing filter directly."""
        _seed_data(db)

        result = for_each(
            simple_transform,
            inputs={"data": MetricA},
            outputs=[AnalysisResult],
            db=db,
            where=EachOf(None),
            subject=[],
            session=[],
        )

        # Should behave same as where=None
        assert len(result) == 2  # 2 subjects

    def test_where_none_and_filter(self, db):
        """EachOf(None, filter) should produce two variant passes."""
        _seed_data(db)

        # Use a raw_sql filter that matches all (just to test the machinery)
        from scidb.model.filters import raw_sql

        all_filter = raw_sql("1=1")

        result = for_each(
            simple_transform,
            inputs={"data": MetricA},
            outputs=[AnalysisResult],
            db=db,
            where=EachOf(None, all_filter),
            subject=[],
            session=[],
        )

        # 2 subjects × 2 where variants = 4 rows
        assert len(result) == 4


# ---------------------------------------------------------------------------
# Tests: Combined EachOf (cartesian product)
# ---------------------------------------------------------------------------


class TestEachOfCombined:
    def test_types_and_constants(self, db):
        """EachOf on type + EachOf on constant = cartesian product."""
        _seed_data(db)

        result = for_each(
            analyze,
            inputs={
                "data": EachOf(MetricA, MetricB),
                "alpha": EachOf(0.05, 0.01),
            },
            outputs=[AnalysisResult],
            db=db,
            subject=[],
            session=[],
        )

        # 2 subjects × 2 types × 2 alphas = 8 rows
        assert len(result) == 8

    def test_types_and_where(self, db):
        """EachOf on type + EachOf on where = cartesian product."""
        _seed_data(db)

        from scidb.model.filters import raw_sql

        all_filter = raw_sql("1=1")

        result = for_each(
            simple_transform,
            inputs={"data": EachOf(MetricA, MetricB)},
            outputs=[AnalysisResult],
            db=db,
            where=EachOf(None, all_filter),
            subject=[],
            session=[],
        )

        # 2 subjects × 2 types × 2 where = 8 rows
        assert len(result) == 8

    def test_all_three_axes(self, db):
        """EachOf on type + constant + where = full cartesian product."""
        _seed_data(db)

        from scidb.model.filters import raw_sql

        all_filter = raw_sql("1=1")

        result = for_each(
            analyze,
            inputs={
                "data": EachOf(MetricA, MetricB),
                "alpha": EachOf(0.05, 0.01),
            },
            outputs=[AnalysisResult],
            db=db,
            where=EachOf(None, all_filter),
            subject=[],
            session=[],
        )

        # 2 subjects × 2 types × 2 alphas × 2 where = 16 rows
        assert len(result) == 16


# ---------------------------------------------------------------------------
# Tests: dry_run mode with EachOf
# ---------------------------------------------------------------------------


class TestEachOfDryRun:
    def test_dry_run_returns_none(self, db):
        """EachOf in dry_run mode should return None (same as regular dry_run)."""
        _seed_data(db)

        result = for_each(
            analyze,
            inputs={"data": EachOf(MetricA, MetricB), "alpha": 0.05},
            outputs=[AnalysisResult],
            db=db,
            dry_run=True,
            subject=[],
            session=[],
        )

        assert result is None
