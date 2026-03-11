"""Tests for @thunk(generates_file=True) — side-effect function tracking."""

import numpy as np
import pytest

from scidb import BaseVariable, configure_database as scidb_configure
from scidb.database import _local, DatabaseManager
from thunk import thunk
from scihist import configure_database, for_each, save

from conftest import DEFAULT_TEST_SCHEMA_KEYS


# --- Variable classes for testing ---

class RawSignal(BaseVariable):
    """Raw input data."""
    schema_version = 1


class Figure(BaseVariable):
    """Represents a generated file (plot, report, etc.)."""
    schema_version = 1


class Report(BaseVariable):
    """Another generated file type."""
    schema_version = 1


# --- Decorator flag tests ---

class TestDecoratorFlag:
    """Tests that generates_file flag is set correctly and does not affect hash."""

    def test_generates_file_flag_set(self):
        """@thunk(generates_file=True) sets the flag on the Thunk."""
        @thunk(generates_file=True)
        def make_plot(data):
            pass

        assert make_plot.generates_file is True

    def test_generates_file_default_false(self):
        """@thunk without generates_file defaults to False."""
        @thunk
        def process(data):
            return data * 2

        assert process.generates_file is False

    def test_generates_file_does_not_affect_hash(self):
        """generates_file should NOT change the function hash."""
        def my_func(x):
            return x * 2

        t_normal = thunk(my_func)
        t_gf = thunk(my_func, generates_file=True)

        assert t_normal.hash == t_gf.hash

    def test_generates_file_with_other_options(self):
        """generates_file works alongside unpack_output and unwrap."""
        @thunk(generates_file=True, unwrap=False)
        def make_plot(data):
            pass

        assert make_plot.generates_file is True
        assert make_plot.unwrap is False


# --- Save behavior tests ---

class TestSaveBehavior:
    """Tests that Figure.save(result) creates lineage-only records."""

    def test_save_returns_generated_id(self, db):
        """save() returns a generated: prefixed ID."""
        RawSignal.save(np.array([1, 2, 3]), subject=1)
        loaded = RawSignal.load(subject=1)

        @thunk(generates_file=True)
        def make_plot(data):
            return None  # side-effect function

        result = make_plot(loaded)
        record_id = save(Figure, result, subject=1)

        assert record_id.startswith("generated:")

    def test_no_data_in_duckdb(self, db):
        """Lineage-only save should not store data in DuckDB."""
        RawSignal.save(np.array([1, 2, 3]), subject=1)
        loaded = RawSignal.load(subject=1)

        @thunk(generates_file=True)
        def make_plot(data):
            return None

        result = make_plot(loaded)
        save(Figure, result, subject=1)

        # Trying to load data from DuckDB should fail
        from scidb.exceptions import NotFoundError
        with pytest.raises(NotFoundError):
            Figure.load(subject=1)

    def test_lineage_stored_in_pipelinedb(self, db):
        """Lineage should be saved in PipelineDB with correct function name."""
        RawSignal.save(np.array([1, 2, 3]), subject=1)
        loaded = RawSignal.load(subject=1)

        @thunk(generates_file=True)
        def make_plot(data):
            return None

        result = make_plot(loaded)
        record_id = save(Figure, result, subject=1)

        # Check lineage exists
        assert db.has_lineage(record_id)

        # Check the pipeline structure includes our function
        structure = db.get_pipeline_structure()
        function_names = [s["function_name"] for s in structure]
        assert "make_plot" in function_names

    def test_lineage_has_correct_schema_keys(self, db):
        """Schema keys should be stored correctly in lineage."""
        RawSignal.save(np.array([1, 2, 3]), subject=1, session="A")
        loaded = RawSignal.load(subject=1, session="A")

        @thunk(generates_file=True)
        def make_plot(data):
            return None

        result = make_plot(loaded)
        record_id = save(Figure, result, subject=1, session="A")

        # Verify lineage is stored by querying _record_metadata via find_by_lineage
        lineage_hash = result.pipeline_thunk.compute_lineage_hash()
        rows = db._duck._fetchall(
            "SELECT record_id FROM _record_metadata WHERE lineage_hash = ?",
            [lineage_hash],
        )
        assert rows is not None
        assert len(rows) > 0
        assert rows[0][0] == record_id


# --- Cache hit tests ---

class TestCacheHit:
    """Tests that generates_file functions skip re-execution on cache hit."""

    def test_cache_hit_skips_execution(self, db):
        """Second call with same inputs should hit cache; function not called."""
        RawSignal.save(np.array([1, 2, 3]), subject=1)

        call_count = 0

        @thunk(generates_file=True)
        def make_plot(data):
            nonlocal call_count
            call_count += 1
            return None

        # First run
        loaded = RawSignal.load(subject=1)
        result1 = make_plot(loaded)
        save(Figure, result1, subject=1)
        assert call_count == 1

        # Second run — should hit cache
        reloaded = RawSignal.load(subject=1)
        result2 = make_plot(reloaded)
        assert call_count == 1  # NOT called again
        assert result2.data is None
        assert result2.is_complete is True

    def test_cache_hit_data_is_none(self, db):
        """Cache hit result should have data=None."""
        RawSignal.save(np.array([1, 2, 3]), subject=1)

        @thunk(generates_file=True)
        def make_plot(data):
            return None

        loaded = RawSignal.load(subject=1)
        result = make_plot(loaded)
        save(Figure, result, subject=1)

        # Cache hit
        reloaded = RawSignal.load(subject=1)
        cached = make_plot(reloaded)
        assert cached.data is None
        assert cached.is_complete is True

    def test_idempotent_save(self, db):
        """Saving a cache-hit result should be a no-op (PipelineDB upsert)."""
        RawSignal.save(np.array([1, 2, 3]), subject=1)

        @thunk(generates_file=True)
        def make_plot(data):
            return None

        loaded = RawSignal.load(subject=1)
        result = make_plot(loaded)
        id1 = save(Figure, result, subject=1)

        # Cache hit
        reloaded = RawSignal.load(subject=1)
        cached = make_plot(reloaded)

        # Save the cache-hit result — should succeed (idempotent)
        id2 = save(Figure, cached, subject=1)
        assert id2.startswith("generated:")


# --- Distinct computation tests ---

class TestDistinctComputations:
    """Tests that different inputs or code produce distinct results."""

    def test_different_inputs_no_cache_hit(self, db):
        """Different inputs should not hit cache."""
        RawSignal.save(np.array([1, 2, 3]), subject=1)
        RawSignal.save(np.array([4, 5, 6]), subject=2)

        call_count = 0

        @thunk(generates_file=True)
        def make_plot(data):
            nonlocal call_count
            call_count += 1
            return None

        # First subject
        loaded1 = RawSignal.load(subject=1)
        result1 = make_plot(loaded1)
        save(Figure, result1, subject=1)
        assert call_count == 1

        # Second subject — different input, should NOT hit cache
        loaded2 = RawSignal.load(subject=2)
        result2 = make_plot(loaded2)
        assert call_count == 2

    def test_different_function_no_cache_hit(self, db):
        """Different function code should not hit cache."""
        RawSignal.save(np.array([1, 2, 3]), subject=1)

        call_count_a = 0
        call_count_b = 0

        @thunk(generates_file=True)
        def make_plot_a(data):
            nonlocal call_count_a
            call_count_a += 1
            return None

        @thunk(generates_file=True)
        def make_plot_b(data):
            nonlocal call_count_b
            call_count_b += 1
            return "different"

        loaded = RawSignal.load(subject=1)
        result_a = make_plot_a(loaded)
        save(Figure, result_a, subject=1)
        assert call_count_a == 1

        # Different function — should NOT hit cache
        reloaded = RawSignal.load(subject=1)
        result_b = make_plot_b(reloaded)
        assert call_count_b == 1  # Called (not cached)


# --- Persistence tests ---

class TestPersistence:
    """Tests that cache hits survive database reconnection."""

    def test_cache_hit_survives_reconnect(self, tmp_path):
        """Cache hit should work after closing and reopening database."""
        db_path = tmp_path / "persist.duckdb"

        call_count = 0

        @thunk(generates_file=True)
        def make_plot(data):
            nonlocal call_count
            call_count += 1
            return None

        # First connection — save data and lineage
        db1 = configure_database(db_path, DEFAULT_TEST_SCHEMA_KEYS)
        RawSignal.save(np.array([1, 2, 3]), subject=1)
        loaded = RawSignal.load(subject=1)
        result = make_plot(loaded)
        save(Figure, result, subject=1)
        assert call_count == 1
        db1.close()
        from thunk import Thunk
        Thunk.query = None

        # Second connection — cache hit should still work
        db2 = configure_database(db_path, DEFAULT_TEST_SCHEMA_KEYS)
        reloaded = RawSignal.load(subject=1)
        cached = make_plot(reloaded)
        assert call_count == 1  # NOT called again
        assert cached.data is None
        assert cached.is_complete is True
        db2.close()
        Thunk.query = None


# --- for_each integration tests ---

class TestForEachIntegration:
    """Tests for_each with generates_file functions."""

    def test_for_each_passes_metadata_to_generates_file_fn(self, db):
        """generates_file=True function auto-receives metadata kwargs in for_each."""
        RawSignal.save(np.array([1, 2, 3]), subject=1, session="A")

        received_kwargs = {}

        @thunk(generates_file=True)
        def make_plot(data, subject, session):
            nonlocal received_kwargs
            received_kwargs = {"subject": subject, "session": session}
            return None

        for_each(
            make_plot,
            inputs={"data": RawSignal},
            outputs=[Figure],
            subject=[1],
            session=["A"],
        )

        assert received_kwargs == {"subject": 1, "session": "A"}

    def test_for_each_cache_hit_on_second_run(self, db):
        """Second for_each run should hit cache for all iterations."""
        RawSignal.save(np.array([1, 2, 3]), subject=1, session="A")
        RawSignal.save(np.array([4, 5, 6]), subject=1, session="B")

        call_count = 0

        @thunk(generates_file=True)
        def make_plot(data, subject, session):
            nonlocal call_count
            call_count += 1
            return None

        # First run
        for_each(
            make_plot,
            inputs={"data": RawSignal},
            outputs=[Figure],
            subject=[1],
            session=["A", "B"],
        )
        assert call_count == 2

        # Second run — should hit cache for both
        for_each(
            make_plot,
            inputs={"data": RawSignal},
            outputs=[Figure],
            subject=[1],
            session=["A", "B"],
        )
        assert call_count == 2  # NOT called again

    def test_for_each_outputs_figure_type(self, db):
        """outputs=[Figure] should work with generates_file function."""
        RawSignal.save(np.array([1, 2, 3]), subject=1, session="A")

        @thunk(generates_file=True)
        def make_plot(data, subject, session):
            return None

        # Should not raise
        for_each(
            make_plot,
            inputs={"data": RawSignal},
            outputs=[Figure],
            subject=[1],
            session=["A"],
        )

    def test_normal_fn_no_metadata_by_default(self, db):
        """Normal (non-generates_file) functions do NOT receive metadata by default."""
        RawSignal.save(np.array([1, 2, 3]), subject=1, session="A")

        call_args = {}

        @thunk
        def process(data):
            nonlocal call_args
            call_args = {"called": True}
            return data * 2

        for_each(
            process,
            inputs={"data": RawSignal},
            outputs=[RawSignal],
            subject=[1],
            session=["A"],
        )

        # Function was called but without metadata kwargs
        assert call_args == {"called": True}
