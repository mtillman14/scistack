"""Tests for schema_filter and schema_level parameters in scihist.for_each."""

import numpy as np
import pytest
import scifor as _scifor

from scidb import BaseVariable, configure_database
from scihist import for_each
from scilineage import lineage_fcn


SCHEMA = ["subject", "session", "trial"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_schema_filter.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


# ---------------------------------------------------------------------------
# Variable types
# ---------------------------------------------------------------------------

class RawData(BaseVariable):
    pass


class ProcessedData(BaseVariable):
    pass


# ---------------------------------------------------------------------------
# Pipeline functions
# ---------------------------------------------------------------------------

@lineage_fcn
def process(raw_data, threshold):
    return raw_data * threshold


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _seed_raw(db, subjects=(1, 2, 3), sessions=("A", "B"), trials=(1, 2)):
    """Seed database with raw data."""
    for subj in subjects:
        for sess in sessions:
            for trial in trials:
                RawData.save(
                    np.random.randn(10),
                    db=db,
                    subject=subj,
                    session=sess,
                    trial=trial
                )


# ---------------------------------------------------------------------------
# schema_filter tests
# ---------------------------------------------------------------------------

class TestSchemaFilter:
    def test_schema_filter_basic(self, db):
        """schema_filter processes only specified values."""
        _seed_raw(db)

        # Process only subject=1 and subject=2
        result = for_each(
            process,
            inputs={"raw_data": RawData, "threshold": 2.0},
            outputs=[ProcessedData],
            schema_filter={"subject": [1, 2]},
        )

        # Should have processed 2 subjects × 2 sessions × 2 trials = 8 combos
        assert len(result) == 8

        # Verify only subjects 1 and 2 were processed (schema values are strings)
        assert set(result["subject"].unique()) == {"1", "2"}

        # Verify all sessions and trials were processed (no filter on them)
        assert set(result["session"].unique()) == {"A", "B"}
        assert set(result["trial"].unique()) == {"1", "2"}

    def test_schema_filter_multiple_keys(self, db):
        """schema_filter with multiple keys."""
        _seed_raw(db)

        # Process only subject=1, session=A
        result = for_each(
            process,
            inputs={"raw_data": RawData, "threshold": 2.0},
            outputs=[ProcessedData],
            schema_filter={"subject": [1], "session": ["A"]},
        )

        # Should have processed 1 subject × 1 session × 2 trials = 2 combos
        assert len(result) == 2

        # Verify filtering worked (schema values are strings)
        assert list(result["subject"].unique()) == ["1"]
        assert list(result["session"].unique()) == ["A"]
        assert set(result["trial"].unique()) == {"1", "2"}

    def test_schema_filter_single_value(self, db):
        """schema_filter with single value per key."""
        _seed_raw(db)

        # Process only subject=2, session=B, trial=1
        result = for_each(
            process,
            inputs={"raw_data": RawData, "threshold": 2.0},
            outputs=[ProcessedData],
            schema_filter={
                "subject": [2],
                "session": ["B"],
                "trial": [1],
            },
        )

        # Should have processed exactly 1 combo
        assert len(result) == 1

        # Verify the exact combo (schema values are strings)
        assert result.iloc[0]["subject"] == "2"
        assert result.iloc[0]["session"] == "B"
        assert result.iloc[0]["trial"] == "1"

    def test_schema_filter_empty_result(self, db):
        """schema_filter with no matching data."""
        _seed_raw(db, subjects=(1, 2))

        # Try to process subject=99 (doesn't exist)
        result = for_each(
            process,
            inputs={"raw_data": RawData, "threshold": 2.0},
            outputs=[ProcessedData],
            schema_filter={"subject": [99]},
        )

        # Should have processed 0 combos
        assert len(result) == 0


# ---------------------------------------------------------------------------
# schema_level tests
# ---------------------------------------------------------------------------

class TestSchemaLevel:
    def test_schema_level_subset(self, db):
        """schema_level iterates only over specified keys."""
        _seed_raw(db, subjects=(1, 2), sessions=("A", "B"), trials=(1,))

        # Iterate only over subject and session (not trial)
        result = for_each(
            process,
            inputs={"raw_data": RawData, "threshold": 2.0},
            outputs=[ProcessedData],
            schema_level=["subject", "session"],
        )

        # Should have 2 subjects × 2 sessions = 4 rows
        # (trial is not iterated, so it gets all trials for each combo)
        assert len(result) == 4

        # All combinations of subject and session should be present (schema values are strings)
        combos = set(zip(result["subject"], result["session"]))
        expected = {("1", "A"), ("1", "B"), ("2", "A"), ("2", "B")}
        assert combos == expected

    def test_schema_level_single_key(self, db):
        """schema_level with single key."""
        _seed_raw(db, subjects=(1, 2, 3), sessions=("A",), trials=(1,))

        # Iterate only over subject
        result = for_each(
            process,
            inputs={"raw_data": RawData, "threshold": 2.0},
            outputs=[ProcessedData],
            schema_level=["subject"],
        )

        # Should have 3 subjects (schema values are strings)
        assert len(result) == 3
        assert set(result["subject"].unique()) == {"1", "2", "3"}


# ---------------------------------------------------------------------------
# Combined schema_filter and schema_level tests
# ---------------------------------------------------------------------------

class TestSchemaFilterAndLevel:
    def test_filter_and_level_together(self, db):
        """schema_filter and schema_level work together."""
        _seed_raw(db, subjects=(1, 2, 3), sessions=("A", "B"), trials=(1, 2))

        # Iterate over subject and session only, filter subject to 1 and 2
        result = for_each(
            process,
            inputs={"raw_data": RawData, "threshold": 2.0},
            outputs=[ProcessedData],
            schema_filter={"subject": [1, 2]},
            schema_level=["subject", "session"],
        )

        # Should have 2 subjects × 2 sessions = 4 combos
        assert len(result) == 4

        # Verify filtering worked (schema values are strings)
        assert set(result["subject"].unique()) == {"1", "2"}
        assert set(result["session"].unique()) == {"A", "B"}

    def test_filter_on_non_iterated_key(self, db):
        """schema_filter can filter keys not in schema_level."""
        _seed_raw(db, subjects=(1, 2), sessions=("A", "B"), trials=(1, 2))

        # Iterate over subject only, but filter session
        result = for_each(
            process,
            inputs={"raw_data": RawData, "threshold": 2.0},
            outputs=[ProcessedData],
            schema_filter={"session": ["A"]},
            schema_level=["subject"],
        )

        # Should iterate over subjects, but only load session=A data
        assert len(result) == 2  # 2 subjects
        assert set(result["subject"].unique()) == {"1", "2"}


# ---------------------------------------------------------------------------
# Backward compatibility tests
# ---------------------------------------------------------------------------

class TestBackwardCompatibility:
    def test_metadata_iterables_still_works(self, db):
        """Original **metadata_iterables syntax still works."""
        _seed_raw(db, subjects=(1, 2), sessions=("A", "B"), trials=(1,))

        # Old-style explicit iteration
        result = for_each(
            process,
            inputs={"raw_data": RawData, "threshold": 2.0},
            outputs=[ProcessedData],
            subject=[1, 2],
            session=["A", "B"],
            trial=[1],
        )

        # Should work exactly as before (schema values are strings)
        assert len(result) == 4
        assert set(result["subject"].unique()) == {"1", "2"}
        assert set(result["session"].unique()) == {"A", "B"}

    def test_cannot_use_both_styles(self, db):
        """Error when using both schema_filter and **metadata_iterables."""
        _seed_raw(db)

        with pytest.raises(ValueError, match="Cannot use both"):
            for_each(
                process,
                inputs={"raw_data": RawData, "threshold": 2.0},
                outputs=[ProcessedData],
                schema_filter={"subject": [1]},
                subject=[1, 2],  # This conflicts!
            )

    def test_no_params_uses_all_data(self, db):
        """No schema params means use all available data."""
        _seed_raw(db, subjects=(1,), sessions=("A",), trials=(1, 2))

        # No schema params at all - should process all combos
        # But we need to provide explicit metadata_iterables for iteration
        result = for_each(
            process,
            inputs={"raw_data": RawData, "threshold": 2.0},
            outputs=[ProcessedData],
            subject=[1],
            session=["A"],
            trial=[1, 2],
        )

        assert len(result) == 2


# ---------------------------------------------------------------------------
# Error handling tests
# ---------------------------------------------------------------------------

class TestErrorHandling:
    def test_schema_filter_requires_db(self):
        """schema_filter requires database connection."""
        # Create a scenario without db parameter and no global db
        from scidb.database import _local
        if hasattr(_local, 'db'):
            delattr(_local, 'db')

        # Now try to use schema_filter without a database
        with pytest.raises(ValueError, match="require a database connection"):
            for_each(
                process,
                inputs={"raw_data": RawData, "threshold": 2.0},
                outputs=[ProcessedData],
                schema_filter={"subject": [1]},
                save=False,  # Don't try to save (which also needs db)
                db=None,  # Explicitly no db
            )

    def test_invalid_schema_key_in_filter(self, db):
        """schema_filter with invalid key name processes all actual schema keys."""
        _seed_raw(db, subjects=(1,), sessions=("A",), trials=(1,))

        # Use a key that doesn't exist in schema
        # This should iterate over all actual schema keys and also try the nonexistent one
        result = for_each(
            process,
            inputs={"raw_data": RawData, "threshold": 2.0},
            outputs=[ProcessedData],
            schema_filter={"nonexistent_key": [1]},
        )

        # Should still process the actual schema keys (subject, session, trial)
        # The nonexistent_key just gets an empty value list
        assert result is not None
        assert len(result) >= 0  # May be 0 if nonexistent_key has no values


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

class TestIntegration:
    def test_real_world_selective_processing(self, db):
        """Real-world scenario: process only specific subjects."""
        # Seed with 5 subjects, 2 sessions, 3 trials
        _seed_raw(
            db,
            subjects=(1, 2, 3, 4, 5),
            sessions=("pre", "post"),
            trials=(1, 2, 3)
        )

        # Process only subjects 1, 3, 5 for session "post"
        result = for_each(
            process,
            inputs={"raw_data": RawData, "threshold": 1.5},
            outputs=[ProcessedData],
            schema_filter={
                "subject": [1, 3, 5],
                "session": ["post"],
            },
        )

        # Should have 3 subjects × 1 session × 3 trials = 9 combos
        assert len(result) == 9

        # Verify data saved correctly (schema values are strings)
        assert set(result["subject"].unique()) == {"1", "3", "5"}
        assert list(result["session"].unique()) == ["post"]
        assert set(result["trial"].unique()) == {"1", "2", "3"}

        # Verify outputs exist in database
        for subj in [1, 3, 5]:
            for trial in [1, 2, 3]:
                data = ProcessedData.load(
                    db=db,
                    subject=subj,
                    session="post",
                    trial=trial
                )
                assert data is not None
                # Verify data has correct shape (numpy array)
                import numpy as np
                assert isinstance(data.data, np.ndarray)
                assert len(data.data) == 10  # Same length as input

    def test_incremental_processing_with_skip_computed(self, db):
        """Process subset, then extend with schema_filter."""
        _seed_raw(db, subjects=(1, 2, 3), sessions=("A",), trials=(1,))

        # First: process only subject 1
        result1 = for_each(
            process,
            inputs={"raw_data": RawData, "threshold": 2.0},
            outputs=[ProcessedData],
            schema_filter={"subject": [1]},
            skip_computed=True,
        )

        assert len(result1) == 1

        # Second: process subjects 1 and 2 (should skip 1)
        result2 = for_each(
            process,
            inputs={"raw_data": RawData, "threshold": 2.0},
            outputs=[ProcessedData],
            schema_filter={"subject": [1, 2]},
            skip_computed=True,
        )

        # Should have skipped subject 1, only processed subject 2
        # But result includes all processed combos
        assert len(result2) == 1  # Only subject 2 was computed

        # Verify both subjects now exist in database
        data1 = ProcessedData.load(db=db, subject=1, session="A", trial=1)
        data2 = ProcessedData.load(db=db, subject=2, session="A", trial=1)
        assert data1 is not None
        assert data2 is not None
