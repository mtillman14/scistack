"""Tests for scirun.for_each (DB-backed wrapper)."""

import pytest
from io import StringIO
import sys

import numpy as np
import pandas as pd

from scirun import for_each, Fixed, ColumnSelection, Merge


class MockVariable:
    """Mock variable type for testing."""

    saved_data = []
    load_error = False

    def __init__(self, data):
        self.data = data
        self.metadata = {}

    @classmethod
    def load(cls, **metadata):
        if cls.load_error:
            raise ValueError("Mock load error")
        result = cls(f"loaded_{metadata}")
        result.metadata = metadata
        return result

    @classmethod
    def save(cls, data, **metadata):
        cls.saved_data.append({"data": data, "metadata": metadata})

    @classmethod
    def reset(cls):
        cls.saved_data = []
        cls.load_error = False


class MockVariableA(MockVariable):
    """First mock variable type."""

    saved_data = []
    load_error = False

    @classmethod
    def reset(cls):
        cls.saved_data = []
        cls.load_error = False


class MockVariableB(MockVariable):
    """Second mock variable type."""

    saved_data = []
    load_error = False

    @classmethod
    def reset(cls):
        cls.saved_data = []
        cls.load_error = False


class MockOutput(MockVariable):
    """Mock output variable type."""

    saved_data = []
    load_error = False

    @classmethod
    def reset(cls):
        cls.saved_data = []
        cls.load_error = False


class MockOutputB(MockVariable):
    """Second mock output variable type."""

    saved_data = []
    load_error = False

    @classmethod
    def reset(cls):
        cls.saved_data = []
        cls.load_error = False


@pytest.fixture(autouse=True)
def reset_mocks():
    """Reset mock state before each test."""
    MockVariable.reset()
    MockVariableA.reset()
    MockVariableB.reset()
    MockOutput.reset()
    MockOutputB.reset()
    yield


class TestForEachBasic:
    """Basic tests for for_each function."""

    def test_single_iteration(self):
        """Should execute once for single value."""

        def process(x):
            return x + "_processed"

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[1],
        )

        assert len(MockOutput.saved_data) == 1
        meta = MockOutput.saved_data[0]["metadata"]
        assert meta["subject"] == 1

    def test_multiple_iterations(self):
        """Should execute for all combinations."""

        def process(x):
            return "result"

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[1, 2],
            session=["A", "B"],
        )

        # 2 subjects * 2 sessions = 4 iterations
        assert len(MockOutput.saved_data) == 4

    def test_multiple_inputs(self):
        """Should load multiple inputs."""

        def process(a, b):
            return f"{a}_{b}"

        for_each(
            process,
            inputs={"a": MockVariableA, "b": MockVariableB},
            outputs=[MockOutput],
            subject=[1],
        )

        assert len(MockOutput.saved_data) == 1

    def test_multiple_outputs(self):
        """Should save multiple outputs."""

        def process(x):
            return ("output_a", "output_b")

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockVariableA, MockVariableB],
            subject=[1],
        )

        assert len(MockVariableA.saved_data) == 1
        assert len(MockVariableB.saved_data) == 1
        assert MockVariableA.saved_data[0]["data"] == "output_a"
        assert MockVariableB.saved_data[0]["data"] == "output_b"


class TestForEachWithFixed:
    """Tests for for_each with Fixed inputs."""

    def test_fixed_overrides_metadata(self):
        """Fixed should override iteration metadata."""
        loaded_metadata = []

        class TrackingVariable:
            @classmethod
            def load(cls, **metadata):
                loaded_metadata.append(metadata)
                result = MockVariableA(f"data_{metadata}")
                result.metadata = metadata
                return result

        def process(baseline, current):
            return "result"

        for_each(
            process,
            inputs={
                "baseline": Fixed(TrackingVariable, session="BL"),
                "current": TrackingVariable,
            },
            outputs=[MockOutput],
            subject=[1],
            session=["A", "B"],
        )

        # Check baseline always loaded with session="BL"
        baseline_loads = [m for m in loaded_metadata if m.get("session") == "BL"]
        assert len(baseline_loads) >= 1  # At least one bulk load with BL

        # Check current loaded with iteration sessions
        other_loads = [m for m in loaded_metadata if m.get("session") != "BL"]
        assert len(other_loads) >= 1


class TestForEachDryRun:
    """Tests for dry_run mode."""

    def test_dry_run_no_execution(self, capsys):
        """Dry run should not execute function or save."""

        def process(x):
            raise RuntimeError("Should not be called")

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            dry_run=True,
            subject=[1, 2],
        )

        assert len(MockOutput.saved_data) == 0

        captured = capsys.readouterr()
        assert "[dry-run]" in captured.out

    def test_dry_run_shows_iterations(self, capsys):
        """Dry run should show what would happen."""

        def my_func(x):
            return x

        for_each(
            my_func,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            dry_run=True,
            subject=[1],
            session=["A"],
        )

        captured = capsys.readouterr()
        assert "my_func" in captured.out


class TestForEachErrorHandling:
    """Tests for error handling."""

    def test_skip_on_function_error(self, capsys):
        """Should skip iteration if function raises."""

        def failing_process(x):
            raise ValueError("Processing failed")

        for_each(
            failing_process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[1],
        )

        assert len(MockOutput.saved_data) == 0

        captured = capsys.readouterr()
        assert "[skip]" in captured.out
        assert "Processing failed" in captured.out

    def test_continues_after_error(self, capsys):
        """Should continue processing after error."""
        call_count = [0]

        def sometimes_fails(x):
            call_count[0] += 1
            if call_count[0] == 1:
                raise ValueError("First call fails")
            return "result"

        for_each(
            sometimes_fails,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[1, 2, 3],
        )

        # First failed, second and third succeeded
        assert len(MockOutput.saved_data) == 2


class TestForEachOutput:
    """Tests for output normalization."""

    def test_single_output_not_tuple(self):
        """Single output should be normalized from non-tuple."""

        def process(x):
            return "single_result"  # Not a tuple

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[1],
        )

        assert len(MockOutput.saved_data) == 1
        assert MockOutput.saved_data[0]["data"] == "single_result"

    def test_output_metadata_matches_iteration(self):
        """Output should be saved with iteration metadata."""

        def process(x):
            return "result"

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[42],
            session=["XYZ"],
        )

        meta = MockOutput.saved_data[0]["metadata"]
        assert meta["subject"] == 42
        assert meta["session"] == "XYZ"


class TestForEachWithConstants:
    """Tests for constant values in inputs dict."""

    def test_constant_passed_to_function(self):
        """Constants should be passed as kwargs to the function."""
        received_args = {}

        def process(x, smoothing):
            received_args["x"] = x
            received_args["smoothing"] = smoothing
            return "result"

        for_each(
            process,
            inputs={"x": MockVariableA, "smoothing": 0.2},
            outputs=[MockOutput],
            subject=[1],
        )

        assert received_args["smoothing"] == 0.2
        assert len(MockOutput.saved_data) == 1

    def test_constant_saved_as_metadata(self):
        """Constants should be included in save metadata."""

        def process(x, smoothing):
            return "result"

        for_each(
            process,
            inputs={"x": MockVariableA, "smoothing": 0.2},
            outputs=[MockOutput],
            subject=[1],
        )

        meta = MockOutput.saved_data[0]["metadata"]
        assert meta["subject"] == 1
        assert meta["smoothing"] == 0.2

    def test_constant_with_variable_inputs(self):
        """Constants and variable inputs should work together."""
        received_args = {}

        def process(a, b, threshold):
            received_args["a"] = a
            received_args["b"] = b
            received_args["threshold"] = threshold
            return "result"

        for_each(
            process,
            inputs={"a": MockVariableA, "b": MockVariableB, "threshold": 10},
            outputs=[MockOutput],
            subject=[1, 2],
        )

        # 2 iterations, both should have threshold in metadata
        assert len(MockOutput.saved_data) == 2
        meta0 = MockOutput.saved_data[0]["metadata"]
        meta1 = MockOutput.saved_data[1]["metadata"]
        assert meta0["subject"] == 1 and meta0["threshold"] == 10
        assert meta1["subject"] == 2 and meta1["threshold"] == 10
        assert received_args["threshold"] == 10

    def test_multiple_constants(self):
        """Multiple constants should all be passed and saved."""

        def process(x, low_hz, high_hz, method):
            return f"{low_hz}-{high_hz}-{method}"

        for_each(
            process,
            inputs={
                "x": MockVariableA,
                "low_hz": 20,
                "high_hz": 450,
                "method": "bandpass",
            },
            outputs=[MockOutput],
            subject=[1],
        )

        assert MockOutput.saved_data[0]["data"] == "20-450-bandpass"
        meta = MockOutput.saved_data[0]["metadata"]
        assert meta["subject"] == 1
        assert meta["low_hz"] == 20
        assert meta["high_hz"] == 450
        assert meta["method"] == "bandpass"

    def test_constant_not_loaded(self):
        """Constants should not trigger .load() calls."""
        load_count = [0]

        class CountingVariable:
            @classmethod
            def load(cls, **metadata):
                load_count[0] += 1
                result = MockVariableA(f"data_{metadata}")
                result.metadata = metadata
                return result

        def process(x, factor):
            return "result"

        for_each(
            process,
            inputs={"x": CountingVariable, "factor": 2.5},
            outputs=[MockOutput],
            subject=[1],
        )

        # Only the variable should trigger a load, not the constant
        assert load_count[0] >= 1

    def test_constant_in_dry_run(self, capsys):
        """Dry run should display constants correctly."""

        def my_func(x, smoothing):
            return x

        for_each(
            my_func,
            inputs={"x": MockVariableA, "smoothing": 0.2},
            outputs=[MockOutput],
            dry_run=True,
            subject=[1],
        )

        captured = capsys.readouterr()
        assert "constant smoothing = 0.2" in captured.out

    def test_constant_with_fixed(self):
        """Constants should work alongside Fixed inputs."""

        def process(baseline, current, threshold):
            return "result"

        for_each(
            process,
            inputs={
                "baseline": Fixed(MockVariableA, session="BL"),
                "current": MockVariableB,
                "threshold": 5.0,
            },
            outputs=[MockOutput],
            subject=[1],
            session=["A"],
        )

        assert len(MockOutput.saved_data) == 1
        meta = MockOutput.saved_data[0]["metadata"]
        assert meta["subject"] == 1
        assert meta["session"] == "A"
        assert meta["threshold"] == 5.0


class TestForEachAllLevels:
    """Tests for empty list [] meaning 'all levels'."""

    def _make_mock_db(self, schema_values):
        """Create a mock db with distinct_schema_values support."""

        class MockDB:
            def __init__(self, values_by_key):
                self._values = values_by_key
                self.dataset_schema_keys = list(values_by_key.keys())

            def distinct_schema_values(self, key):
                if key not in self._values:
                    raise ValueError(f"'{key}' is not a schema column.")
                return self._values[key]

            def distinct_schema_combinations(self, keys):
                # Return full cartesian product so no filtering happens
                from itertools import product as _product
                lists = [self._values[k] for k in keys]
                return [tuple(str(v) for v in combo) for combo in _product(*lists)]

        return MockDB(schema_values)

    def test_empty_list_resolves_to_all_values(self):
        """subject=[] should iterate over all subjects from the database."""

        def process(x):
            return "result"

        db = self._make_mock_db({"subject": [1, 2, 3]})

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            subject=[],
        )

        assert len(MockOutput.saved_data) == 3
        saved_subjects = [d["metadata"]["subject"] for d in MockOutput.saved_data]
        assert saved_subjects == [1, 2, 3]

    def test_multiple_empty_lists(self):
        """subject=[], session=[] should resolve both from the database."""

        def process(x):
            return "result"

        db = self._make_mock_db({"subject": [1, 2], "session": ["A", "B"]})

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            subject=[],
            session=[],
        )

        # 2 subjects * 2 sessions = 4 iterations
        assert len(MockOutput.saved_data) == 4

    def test_mixed_explicit_and_empty(self):
        """Can mix explicit values with [] for different keys."""

        def process(x):
            return "result"

        db = self._make_mock_db({"subject": [1, 2, 3], "session": ["A", "B", "C"]})

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            subject=[1],       # explicit single subject
            session=[],         # all sessions from db
        )

        # 1 subject * 3 sessions = 3 iterations
        assert len(MockOutput.saved_data) == 3
        # All should have subject=1
        for d in MockOutput.saved_data:
            assert d["metadata"]["subject"] == 1
        # Sessions should be A, B, C
        saved_sessions = [d["metadata"]["session"] for d in MockOutput.saved_data]
        assert sorted(saved_sessions) == ["A", "B", "C"]

    def test_empty_db_results_in_zero_iterations(self, capsys):
        """If the database has no values for a key, 0 iterations should run."""

        def process(x):
            return "result"

        db = self._make_mock_db({"subject": []})

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            subject=[],
        )

        assert len(MockOutput.saved_data) == 0
        captured = capsys.readouterr()
        assert "[warn]" in captured.out

    def test_no_db_raises_helpful_error(self):
        """If no db is available, should raise a clear error."""
        import unittest.mock as mock

        def process(x):
            return "result"

        # Patch get_database to raise (simulating no configured database)
        with mock.patch.dict("sys.modules", {"scidb": None, "scidb.database": None}):
            with pytest.raises(ValueError, match="no database is available"):
                for_each(
                    process,
                    inputs={"x": MockVariableA},
                    outputs=[MockOutput],
                    subject=[],
                )

    def test_dry_run_with_empty_list(self, capsys):
        """dry_run should work after resolving [] to all values."""

        def process(x):
            raise RuntimeError("Should not be called")

        db = self._make_mock_db({"subject": [10, 20]})

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            dry_run=True,
            subject=[],
        )

        assert len(MockOutput.saved_data) == 0
        captured = capsys.readouterr()
        assert "[dry-run]" in captured.out
        assert "2 iterations" in captured.out


class TestForEachDistribute:
    """Tests for distribute parameter in for_each."""

    def _make_mock_db(self, schema_keys, schema_values=None):
        """Create a mock db with dataset_schema_keys and optional distinct values."""

        class MockDB:
            def __init__(self):
                self.dataset_schema_keys = schema_keys

            def distinct_schema_values(self, key):
                if schema_values and key in schema_values:
                    return schema_values[key]
                return []

        return MockDB()

    def test_distribute_numpy_1d(self):
        """1D numpy array should be split by element."""
        import numpy as np

        db = self._make_mock_db(["subject", "trial", "cycle"])

        def process(x):
            return np.array([10.0, 20.0, 30.0])

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            distribute=True,
            subject=[1],
            trial=[1],
        )

        assert len(MockOutput.saved_data) == 3
        for i, entry in enumerate(MockOutput.saved_data):
            assert entry["metadata"]["cycle"] == i + 1
            assert entry["metadata"]["subject"] == 1
            assert entry["metadata"]["trial"] == 1

    def test_distribute_numpy_2d(self):
        """2D numpy array should be split by row."""
        import numpy as np

        db = self._make_mock_db(["subject", "trial", "cycle"])

        def process(x):
            return np.array([[1, 2, 3], [4, 5, 6]])

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            distribute=True,
            subject=[1],
            trial=[1],
        )

        assert len(MockOutput.saved_data) == 2
        assert list(MockOutput.saved_data[0]["data"]) == [1, 2, 3]
        assert list(MockOutput.saved_data[1]["data"]) == [4, 5, 6]
        assert MockOutput.saved_data[0]["metadata"]["cycle"] == 1
        assert MockOutput.saved_data[1]["metadata"]["cycle"] == 2

    def test_distribute_list(self):
        """List should be split by element."""
        db = self._make_mock_db(["subject", "trial", "cycle"])

        def process(x):
            return ["a", "b", "c"]

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            distribute=True,
            subject=[1],
            trial=[1],
        )

        assert len(MockOutput.saved_data) == 3
        assert MockOutput.saved_data[0]["data"] == "a"
        assert MockOutput.saved_data[1]["data"] == "b"
        assert MockOutput.saved_data[2]["data"] == "c"

    def test_distribute_dataframe(self):
        """DataFrame should be split by row into single-row DataFrames."""
        import pandas as pd

        db = self._make_mock_db(["subject", "trial", "cycle"])

        def process(x):
            return pd.DataFrame({"col_a": [1, 2], "col_b": [3, 4]})

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            distribute=True,
            subject=[1],
            trial=[1],
        )

        assert len(MockOutput.saved_data) == 2
        assert isinstance(MockOutput.saved_data[0]["data"], pd.DataFrame)
        assert len(MockOutput.saved_data[0]["data"]) == 1
        assert MockOutput.saved_data[0]["metadata"]["cycle"] == 1
        assert MockOutput.saved_data[1]["metadata"]["cycle"] == 2

    def test_distribute_multiple_iterations(self):
        """Distribute should work across multiple iterations."""
        import numpy as np

        db = self._make_mock_db(["subject", "trial", "cycle"])

        def process(x):
            return np.array([100.0, 200.0])

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            distribute=True,
            subject=[1, 2],
            trial=[1],
        )

        # 2 iterations * 2 elements each = 4 saves
        assert len(MockOutput.saved_data) == 4
        # Check subject=1 saves
        s1_saves = [d for d in MockOutput.saved_data if d["metadata"]["subject"] == 1]
        assert len(s1_saves) == 2
        assert s1_saves[0]["metadata"]["cycle"] == 1
        assert s1_saves[1]["metadata"]["cycle"] == 2

    def test_distribute_with_constants(self):
        """Constants should appear in save metadata alongside distribute key."""
        import numpy as np

        db = self._make_mock_db(["subject", "trial", "cycle"])

        def process(x, smoothing):
            return np.array([1.0, 2.0])

        for_each(
            process,
            inputs={"x": MockVariableA, "smoothing": 0.5},
            outputs=[MockOutput],
            db=db,
            distribute=True,
            subject=[1],
            trial=[1],
        )

        assert len(MockOutput.saved_data) == 2
        for entry in MockOutput.saved_data:
            assert entry["metadata"]["smoothing"] == 0.5
            assert "cycle" in entry["metadata"]

    def test_distribute_1_based_indexing(self):
        """Distribute indices should start at 1, not 0."""
        db = self._make_mock_db(["subject", "trial", "cycle"])

        def process(x):
            return ["a", "b", "c", "d"]

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            distribute=True,
            subject=[1],
            trial=[1],
        )

        cycles = [d["metadata"]["cycle"] for d in MockOutput.saved_data]
        assert cycles == [1, 2, 3, 4]

    def test_distribute_trial_level_to_cycle(self):
        """With [subject, trial, cycle], running at trial level distributes to cycle."""
        import numpy as np

        db = self._make_mock_db(["subject", "trial", "cycle"])

        def process(x):
            return np.array([10.0, 20.0, 30.0])

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            distribute=True,
            subject=[1],
            trial=[1],
        )

        assert len(MockOutput.saved_data) == 3
        for i, entry in enumerate(MockOutput.saved_data):
            assert entry["metadata"]["cycle"] == i + 1
            assert entry["metadata"]["subject"] == 1
            assert entry["metadata"]["trial"] == 1
            assert float(entry["data"]) == (i + 1) * 10.0

    def test_distribute_subject_level_to_trial(self):
        """With [subject, trial, cycle], running at subject level distributes to trial."""
        import numpy as np

        db = self._make_mock_db(["subject", "trial", "cycle"])

        def process(x):
            return np.array([100.0, 200.0, 300.0])

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            distribute=True,
            subject=[1],
        )

        assert len(MockOutput.saved_data) == 3
        for i, entry in enumerate(MockOutput.saved_data):
            assert entry["metadata"]["trial"] == i + 1
            assert entry["metadata"]["subject"] == 1
            assert "cycle" not in entry["metadata"]

    def test_distribute_no_deeper_level(self):
        """Should raise ValueError when iterating at deepest schema level."""
        db = self._make_mock_db(["subject", "trial", "cycle"])

        def process(x):
            return [1, 2]

        with pytest.raises(ValueError, match="deepest schema key"):
            for_each(
                process,
                inputs={"x": MockVariableA},
                outputs=[MockOutput],
                db=db,
                distribute=True,
                subject=[1],
                trial=[1],
                cycle=[1, 2],
            )

    def test_distribute_validation_no_db(self):
        """Should raise ValueError with helpful message when no db available."""
        import unittest.mock as mock

        def process(x):
            return [1, 2]

        with mock.patch.dict("sys.modules", {"scidb": None, "scidb.database": None}):
            with pytest.raises(ValueError, match="no database is available"):
                for_each(
                    process,
                    inputs={"x": MockVariableA},
                    outputs=[MockOutput],
                    distribute=True,
                    subject=[1],
                )

    def test_distribute_dry_run(self, capsys):
        """Dry run should show distribute info without executing."""
        db = self._make_mock_db(["subject", "trial", "cycle"])

        def process(x):
            raise RuntimeError("Should not be called")

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            distribute=True,
            dry_run=True,
            subject=[1],
            trial=[1],
        )

        assert len(MockOutput.saved_data) == 0
        captured = capsys.readouterr()
        assert "[dry-run]" in captured.out
        assert "distribute" in captured.out
        assert "cycle" in captured.out

    def test_distribute_save_false(self):
        """When save=False, distribute should not save anything."""
        import numpy as np

        db = self._make_mock_db(["subject", "trial", "cycle"])

        call_count = [0]

        def process(x):
            call_count[0] += 1
            return np.array([1.0, 2.0, 3.0])

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            distribute=True,
            save=False,
            subject=[1],
            trial=[1],
        )

        assert call_count[0] == 1  # Function was called
        assert len(MockOutput.saved_data) == 0  # But nothing saved

    def test_distribute_unsupported_type(self, capsys):
        """Unsupported type should print error and continue."""
        db = self._make_mock_db(["subject", "trial", "cycle"])

        def process(x):
            return 42  # scalar, not distributable

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            distribute=True,
            subject=[1],
            trial=[1],
        )

        assert len(MockOutput.saved_data) == 0
        captured = capsys.readouterr()
        assert "[error]" in captured.out
        assert "does not support" in captured.out

    def test_distribute_constant_name_conflict(self):
        """Should raise ValueError if distribute key conflicts with constant input."""
        db = self._make_mock_db(["subject", "trial", "cycle"])

        def process(x, cycle):
            return [1, 2]

        with pytest.raises(ValueError, match="conflicts with a constant input"):
            for_each(
                process,
                inputs={"x": MockVariableA, "cycle": 5},
                outputs=[MockOutput],
                db=db,
                distribute=True,
                subject=[1],
                trial=[1],
            )

    def test_distribute_empty_result(self):
        """Empty list/array should result in zero saves."""
        db = self._make_mock_db(["subject", "trial", "cycle"])

        def process(x):
            return []

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            distribute=True,
            subject=[1],
            trial=[1],
        )

        assert len(MockOutput.saved_data) == 0

    def test_distribute_validation_no_schema_iterables(self):
        """Should raise ValueError if no metadata_iterables are schema keys."""
        db = self._make_mock_db(["subject", "trial", "cycle"])

        def process(x):
            return [1, 2]

        with pytest.raises(ValueError, match="requires at least one metadata_iterable"):
            for_each(
                process,
                inputs={"x": MockVariableA},
                outputs=[MockOutput],
                db=db,
                distribute=True,
                some_non_schema_key=[1, 2],
            )


class TestForEachConfigKeys:
    """Tests that for_each() config is captured in saved metadata as version keys."""

    def test_fn_name_in_metadata(self):
        """__fn should be set to the function name in saved metadata."""

        def my_process(x):
            return "result"

        for_each(
            my_process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[1],
        )

        meta = MockOutput.saved_data[0]["metadata"]
        assert meta["__fn"] == "my_process"

    def test_loadable_inputs_in_metadata(self):
        """__inputs should capture the loadable input spec as JSON string."""

        def process(x):
            return "result"

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[1],
        )

        meta = MockOutput.saved_data[0]["metadata"]
        import json
        inputs_key = json.loads(meta["__inputs"])
        assert inputs_key == {"x": "MockVariableA"}

    def test_constant_not_in_inputs_key(self):
        """Constants should NOT appear in __inputs (they're already in save_metadata)."""

        def process(x, smoothing):
            return "result"

        for_each(
            process,
            inputs={"x": MockVariableA, "smoothing": 0.2},
            outputs=[MockOutput],
            subject=[1],
        )

        meta = MockOutput.saved_data[0]["metadata"]
        import json
        inputs_key = json.loads(meta["__inputs"])
        assert "smoothing" not in inputs_key
        assert "x" in inputs_key

    def test_fixed_input_serialized_in_inputs_key(self):
        """Fixed inputs should be serialized via to_key() into __inputs."""

        def process(baseline, current):
            return "result"

        for_each(
            process,
            inputs={"baseline": Fixed(MockVariableA, session="BL"), "current": MockVariableB},
            outputs=[MockOutput],
            subject=[1],
            session=["A"],
        )

        meta = MockOutput.saved_data[0]["metadata"]
        import json
        inputs_key = json.loads(meta["__inputs"])
        assert "Fixed" in inputs_key["baseline"]
        assert "session='BL'" in inputs_key["baseline"]
        assert inputs_key["current"] == "MockVariableB"

    def test_where_not_in_metadata_when_absent(self):
        """__where should not appear in metadata when where= is not set."""

        def process(x):
            return "result"

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[1],
        )

        meta = MockOutput.saved_data[0]["metadata"]
        assert "__where" not in meta

    def test_different_fn_names_different_config_keys(self):
        """Different functions should produce different __fn values."""

        def fn_a(x):
            return "a"

        def fn_b(x):
            return "b"

        for_each(fn_a, inputs={"x": MockVariableA}, outputs=[MockOutput], subject=[1])
        meta_a = MockOutput.saved_data[0]["metadata"]["__fn"]

        MockOutput.reset()
        for_each(fn_b, inputs={"x": MockVariableA}, outputs=[MockOutput], subject=[1])
        meta_b = MockOutput.saved_data[0]["metadata"]["__fn"]

        assert meta_a == "fn_a"
        assert meta_b == "fn_b"
        assert meta_a != meta_b

    def test_schema_keys_not_affected(self):
        """Schema metadata keys (subject, trial, etc.) should be unchanged."""

        def process(x):
            return "result"

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[42],
            trial=[7],
        )

        meta = MockOutput.saved_data[0]["metadata"]
        assert meta["subject"] == 42
        assert meta["trial"] == 7


class TestForEachSchemaFiltering:
    """Tests for filtering cartesian product to existing schema combinations."""

    def _make_mock_db(self, schema_values, schema_combinations=None, schema_keys=None):

        class MockDB:
            def __init__(self, values_by_key, combos, keys):
                self._values = values_by_key
                self._combos = combos or {}
                self.dataset_schema_keys = keys or list(values_by_key.keys())

            def distinct_schema_values(self, key):
                if key not in self._values:
                    raise ValueError(f"'{key}' is not a schema column.")
                return self._values[key]

            def distinct_schema_combinations(self, keys):
                combo_key = tuple(keys)
                if combo_key in self._combos:
                    return self._combos[combo_key]
                return []

        return MockDB(schema_values, schema_combinations, schema_keys)

    def test_filtering_removes_nonexistent_combos(self):
        """Two [] keys, only subset of combos exist."""

        def process(x):
            return "result"

        db = self._make_mock_db(
            schema_values={"subject": ["1", "2"], "session": ["A", "B"]},
            schema_combinations={
                ("subject", "session"): [("1", "A"), ("2", "B")],
            },
            schema_keys=["subject", "session"],
        )

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            subject=[],
            session=[],
        )

        # Only 2 of 4 combos exist
        assert len(MockOutput.saved_data) == 2
        saved = [(d["metadata"]["subject"], d["metadata"]["session"]) for d in MockOutput.saved_data]
        assert ("1", "A") in saved
        assert ("2", "B") in saved

    def test_no_filtering_when_all_explicit(self):
        """No [] used — full cartesian product preserved (filtering skipped)."""

        def process(x):
            return "result"

        db = self._make_mock_db(
            schema_values={"subject": ["1", "2"], "session": ["A", "B"]},
            schema_combinations={
                ("subject", "session"): [("1", "A")],
            },
            schema_keys=["subject", "session"],
        )

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            subject=["1", "2"],
            session=["A", "B"],
        )

        # All explicit => no filtering, full 2x2=4 iterations
        assert len(MockOutput.saved_data) == 4

    def test_no_filtering_with_pathinput(self):
        """PathInput in inputs, [] used — full product preserved."""
        from scirun import PathInput

        def process(filepath, x):
            return "result"

        db = self._make_mock_db(
            schema_values={"subject": ["1", "2"], "session": ["A", "B"]},
            schema_combinations={
                ("subject", "session"): [("1", "A")],
            },
            schema_keys=["subject", "session"],
        )

        for_each(
            process,
            inputs={"filepath": PathInput("{subject}/data.csv"), "x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            subject=[],
            session=[],
        )

        # PathInput present => no filtering, full 2x2=4 iterations
        assert len(MockOutput.saved_data) == 4

    def test_no_filtering_with_fixed_pathinput(self):
        """Fixed(PathInput) in inputs, [] used — full product preserved."""
        from scirun import PathInput

        def process(filepath, x):
            return "result"

        db = self._make_mock_db(
            schema_values={"subject": ["1", "2"], "session": ["A", "B"]},
            schema_combinations={
                ("subject", "session"): [("1", "A")],
            },
            schema_keys=["subject", "session"],
        )

        for_each(
            process,
            inputs={
                "filepath": Fixed(PathInput("{subject}/data.csv"), session="BL"),
                "x": MockVariableA,
            },
            outputs=[MockOutput],
            db=db,
            subject=[],
            session=[],
        )

        # Fixed(PathInput) present => no filtering
        assert len(MockOutput.saved_data) == 4

    def test_mixed_resolved_and_explicit(self):
        """One key [], one explicit."""

        def process(x):
            return "result"

        db = self._make_mock_db(
            schema_values={"subject": ["1", "2", "3"], "session": ["A", "B"]},
            schema_combinations={
                ("subject", "session"): [("1", "A"), ("2", "A"), ("3", "B")],
            },
            schema_keys=["subject", "session"],
        )

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            subject=[],          # resolved from db: ["1", "2", "3"]
            session=["A", "B"],  # explicit
        )

        # Existing: (1,A),(2,A),(3,B) = 3
        assert len(MockOutput.saved_data) == 3

    def test_non_schema_keys_excluded_from_filter(self):
        """Extra iterable keys not in dataset_schema_keys should not be sent to filter."""

        def process(x, smoothing):
            return "result"

        db = self._make_mock_db(
            schema_values={"subject": ["1", "2"]},
            schema_combinations={
                ("subject",): [("1",), ("2",)],
            },
            schema_keys=["subject", "session"],
        )

        for_each(
            process,
            inputs={"x": MockVariableA, "smoothing": 0.5},
            outputs=[MockOutput],
            db=db,
            subject=[],
            extra_param=[10, 20],
        )

        assert len(MockOutput.saved_data) == 4

    def test_info_message_printed(self, capsys):
        """Verify [info] filtered... output when combos are removed."""

        def process(x):
            return "result"

        db = self._make_mock_db(
            schema_values={"subject": ["1", "2"], "session": ["A", "B"]},
            schema_combinations={
                ("subject", "session"): [("1", "A")],
            },
            schema_keys=["subject", "session"],
        )

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            subject=[],
            session=[],
        )

        captured = capsys.readouterr()
        assert "[info] filtered 3 non-existent schema combinations (from 4 to 1)" in captured.out

    def test_no_info_message_when_nothing_filtered(self, capsys):
        """All combos exist — no [info] message."""

        def process(x):
            return "result"

        db = self._make_mock_db(
            schema_values={"subject": ["1", "2"], "session": ["A", "B"]},
            schema_combinations={
                ("subject", "session"): [("1", "A"), ("1", "B"), ("2", "A"), ("2", "B")],
            },
            schema_keys=["subject", "session"],
        )

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            subject=[],
            session=[],
        )

        captured = capsys.readouterr()
        assert "[info] filtered" not in captured.out
        assert len(MockOutput.saved_data) == 4

    def test_integer_to_string_coercion(self):
        """Integer metadata values should match string DB values via _schema_str."""

        def process(x):
            return "result"

        db = self._make_mock_db(
            schema_values={"subject": [1, 2], "session": ["A", "B"]},
            schema_combinations={
                ("subject", "session"): [("1", "A"), ("2", "B")],
            },
            schema_keys=["subject", "session"],
        )

        for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            db=db,
            subject=[],
            session=[],
        )

        assert len(MockOutput.saved_data) == 2
        saved = [(d["metadata"]["subject"], d["metadata"]["session"]) for d in MockOutput.saved_data]
        assert (1, "A") in saved
        assert (2, "B") in saved


class TestForEachReturnValue:
    """Tests for the DataFrame returned by for_each()."""

    def test_returns_dataframe(self):
        """for_each should return a DataFrame."""

        def process(x):
            return 42.0

        result = for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[1],
        )

        assert isinstance(result, pd.DataFrame)

    def test_nested_mode_has_metadata_and_output_columns(self):
        """Returned DataFrame should have metadata columns and one output column."""

        def process(x):
            return 99.0

        result = for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[1],
        )

        assert "subject" in result.columns
        assert "MockOutput" in result.columns
        assert len(result) == 1
        assert result.iloc[0]["subject"] == 1
        assert result.iloc[0]["MockOutput"] == 99.0

    def test_nested_mode_multiple_combinations(self):
        """All metadata combinations should appear as rows in the result."""

        def process(x):
            return "result"

        result = for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[1, 2, 3],
        )

        assert len(result) == 3
        assert list(result["subject"]) == [1, 2, 3]

    def test_nested_mode_multiple_metadata_keys(self):
        """Multi-key metadata should all appear as columns."""

        def process(x):
            return "out"

        result = for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[1, 2],
            session=["A", "B"],
        )

        assert len(result) == 4
        assert "subject" in result.columns
        assert "session" in result.columns

    def test_nested_mode_multiple_outputs(self):
        """Each output type should become a separate column."""

        def process(x):
            return ("out_a", "out_b")

        result = for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput, MockOutputB],
            subject=[1, 2],
        )

        assert len(result) == 2
        assert "MockOutput" in result.columns
        assert "MockOutputB" in result.columns
        assert list(result["MockOutput"]) == ["out_a", "out_a"]
        assert list(result["MockOutputB"]) == ["out_b", "out_b"]

    def test_nested_mode_uses_view_name(self):
        """Output column name should use view_name() when defined."""

        class MyOutput:
            saved_data = []

            def __init__(self, data):
                self.data = data

            @classmethod
            def save(cls, data, **metadata):
                cls.saved_data.append(data)

            @classmethod
            def view_name(cls):
                return "my_custom_name"

        def process(x):
            return 7.0

        result = for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MyOutput],
            subject=[1],
        )

        assert "my_custom_name" in result.columns
        assert "MyOutput" not in result.columns

    def test_nested_mode_falls_back_to_class_name(self):
        """Output column name falls back to __name__ when view_name() absent."""

        class NoViewName:
            @classmethod
            def save(cls, data, **metadata):
                pass

        def process(x):
            return 5.0

        result = for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[NoViewName],
            subject=[1],
        )

        assert "NoViewName" in result.columns

    def test_skipped_iterations_excluded(self):
        """Rows where fn raised should not appear in the result."""
        call_count = [0]

        def sometimes_fails(x):
            call_count[0] += 1
            if call_count[0] == 2:
                raise ValueError("fail")
            return "ok"

        result = for_each(
            sometimes_fails,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[1, 2, 3],
        )

        assert len(result) == 2
        assert list(result["subject"]) == [1, 3]

    def test_all_skipped_returns_empty_dataframe(self):
        """When all iterations fail, result should be an empty DataFrame."""

        def always_fails(x):
            raise ValueError("always")

        result = for_each(
            always_fails,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[1, 2],
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_dry_run_returns_none(self):
        """dry_run=True should return None, not a DataFrame."""

        def process(x):
            return "result"

        result = for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            dry_run=True,
            subject=[1],
        )

        assert result is None

    def test_save_false_still_returns_data(self):
        """save=False should still return the computed data."""

        def process(x):
            return 123

        result = for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            save=False,
            subject=[1, 2],
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result["MockOutput"]) == [123, 123]
        assert len(MockOutput.saved_data) == 0

    def test_flatten_mode_dataframe_outputs(self):
        """When fn returns DataFrames, metadata is replicated per row."""

        def process(x):
            return pd.DataFrame({"val": [10.0, 20.0, 30.0]})

        result = for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[1, 2],
        )

        # 2 subjects * 3 rows each = 6 total rows
        assert len(result) == 6
        assert "subject" in result.columns
        assert "val" in result.columns
        # Subject 1 rows
        s1 = result[result["subject"] == 1]
        assert len(s1) == 3
        np.testing.assert_array_equal(s1["val"].values, [10.0, 20.0, 30.0])
        # Subject 2 rows
        s2 = result[result["subject"] == 2]
        assert len(s2) == 3

    def test_flatten_mode_multiple_df_outputs(self):
        """Multiple DataFrame outputs are concatenated horizontally per combination."""

        def process(x):
            df_a = pd.DataFrame({"col_a": [1.0, 2.0]})
            df_b = pd.DataFrame({"col_b": [10.0, 20.0]})
            return (df_a, df_b)

        result = for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput, MockOutputB],
            subject=[1],
        )

        # 1 subject * 2 rows = 2 rows, with both output columns inline
        assert len(result) == 2
        assert "subject" in result.columns
        assert "col_a" in result.columns
        assert "col_b" in result.columns

    def test_return_does_not_affect_saves(self):
        """Returning data should not change what gets saved."""

        def process(x):
            return "saved_value"

        result = for_each(
            process,
            inputs={"x": MockVariableA},
            outputs=[MockOutput],
            subject=[1, 2],
        )

        assert len(MockOutput.saved_data) == 2
        assert len(result) == 2
