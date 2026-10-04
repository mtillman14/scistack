"""End-to-end integration tests for SciDuck."""

import threading

import numpy as np
import pandas as pd
import pytest

from sciduckdb import SciDuck


@pytest.fixture
def duck(tmp_path):
    """Provide a fresh in-memory SciDuck instance."""
    db = SciDuck(":memory:", dataset_schema=["subject", "session", "trial"])
    yield db
    db.close()


@pytest.fixture
def file_duck(tmp_path):
    """Provide a SciDuck instance backed by a file."""
    db_path = tmp_path / "test.duckdb"
    db = SciDuck(db_path, dataset_schema=["subject", "session"])
    yield db
    db.close()


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestInit:
    """Test SciDuck initialization."""

    def test_creates_metadata_tables(self, duck):
        """Should create _schema, _variables, and _variable_groups tables."""
        tables = [
            row[0]
            for row in duck._fetchall(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            )
        ]
        assert "_schema" in tables
        assert "_variables" in tables
        assert "_variable_groups" in tables

    def test_stores_dataset_schema(self, duck):
        """Should store the dataset schema."""
        assert duck.dataset_schema == ["subject", "session", "trial"]

    def test_schema_mismatch_raises(self, tmp_path):
        """Reopening with different schema should raise ValueError."""
        db_path = tmp_path / "schema_test.duckdb"
        db1 = SciDuck(db_path, dataset_schema=["subject", "session"])
        # Save something to populate _schema
        db1.save("var1", 42, subject="S01", session="A")
        db1.close()

        with pytest.raises(ValueError, match="schema mismatch"):
            SciDuck(db_path, dataset_schema=["subject", "trial"])

    def test_repr(self, duck):
        """Should return a readable repr."""
        r = repr(duck)
        assert "SciDuck" in r
        assert "variables=" in r

    def test_context_manager(self, tmp_path):
        """Should work as a context manager."""
        db_path = tmp_path / "ctx.duckdb"
        with SciDuck(db_path, dataset_schema=["subject"]) as db:
            db.save("x", 1, subject="S01")
        # Connection should be closed after exiting


# ---------------------------------------------------------------------------
# Save / Load — Mode B (single entry via kwargs)
# ---------------------------------------------------------------------------


class TestSaveLoadModeB:
    """Test save/load with single entry via schema kwargs."""

    def test_save_and_load_int(self, duck):
        """Round-trip an integer."""
        duck.save("my_int", 42, subject="S01", session="A", trial="1")
        loaded = duck.load("my_int", subject="S01", session="A", trial="1")
        assert loaded == 42

    def test_save_and_load_float(self, duck):
        """Round-trip a float."""
        duck.save("my_float", 3.14, subject="S01", session="A", trial="1")
        loaded = duck.load("my_float", subject="S01", session="A", trial="1")
        assert abs(loaded - 3.14) < 1e-10

    def test_save_and_load_string(self, duck):
        """Round-trip a string."""
        duck.save("my_str", "hello world", subject="S01", session="A", trial="1")
        loaded = duck.load("my_str", subject="S01", session="A", trial="1")
        assert loaded == "hello world"

    def test_save_and_load_bool(self, duck):
        """Round-trip a boolean."""
        duck.save("my_bool", True, subject="S01", session="A", trial="1")
        loaded = duck.load("my_bool", subject="S01", session="A", trial="1")
        assert loaded is True

    def test_save_and_load_1d_array(self, duck):
        """Round-trip a 1D numpy array."""
        original = np.array([1.0, 2.0, 3.0, 4.0])
        duck.save("arr1d", original, subject="S01", session="A", trial="1")
        loaded = duck.load("arr1d", subject="S01", session="A", trial="1")
        np.testing.assert_array_almost_equal(loaded, original)

    def test_save_and_load_2d_array(self, duck):
        """Round-trip a 2D numpy array."""
        original = np.array([[1, 2, 3], [4, 5, 6]])
        duck.save("arr2d", original, subject="S01", session="A", trial="1")
        loaded = duck.load("arr2d", subject="S01", session="A", trial="1")
        np.testing.assert_array_equal(loaded, original)

    def test_save_and_load_list_of_floats(self, duck):
        """Round-trip a list of floats."""
        original = [1.0, 2.5, 3.7]
        duck.save("my_list", original, subject="S01", session="A", trial="1")
        loaded = duck.load("my_list", subject="S01", session="A", trial="1")
        assert loaded == pytest.approx(original)

    def test_save_and_load_list_of_strings(self, duck):
        """Round-trip a list of strings."""
        original = ["alpha", "beta", "gamma"]
        duck.save("str_list", original, subject="S01", session="A", trial="1")
        loaded = duck.load("str_list", subject="S01", session="A", trial="1")
        assert list(loaded) == original

    def test_save_and_load_dict(self, duck):
        """Round-trip a dict (stored as JSON)."""
        original = {"key1": "value1", "key2": 42, "key3": [1, 2, 3]}
        duck.save("my_dict", original, subject="S01", session="A", trial="1")
        loaded = duck.load("my_dict", subject="S01", session="A", trial="1")
        assert loaded["key1"] == "value1"
        assert loaded["key2"] == 42
        assert loaded["key3"] == [1, 2, 3]

    def test_save_and_load_dict_with_ndarray(self, duck):
        """Round-trip a dict containing numpy arrays."""
        original = {"weights": np.array([0.1, 0.2, 0.3]), "bias": 0.5}
        duck.save("params", original, subject="S01", session="A", trial="1")
        loaded = duck.load("params", subject="S01", session="A", trial="1")
        np.testing.assert_array_almost_equal(loaded["weights"], original["weights"])
        assert loaded["bias"] == 0.5


# ---------------------------------------------------------------------------
# Save / Load — Mode A (DataFrame with schema columns)
# ---------------------------------------------------------------------------


class TestSaveLoadModeA:
    """Test save/load with DataFrame containing schema columns."""

    def test_save_dataframe_with_schema_columns(self, duck):
        """Save a DataFrame that has subject/session/trial columns."""
        df = pd.DataFrame(
            {
                "subject": ["S01", "S01", "S02", "S02"],
                "session": ["A", "A", "A", "A"],
                "trial": ["1", "2", "1", "2"],
                "value": [10, 20, 30, 40],
            }
        )
        duck.save("df_var", df)

        # Load specific entry
        loaded = duck.load("df_var", subject="S01", session="A", trial="1")
        assert loaded == 10

    def test_save_multi_column_dataframe(self, duck):
        """Save DataFrame with multiple data columns."""
        df = pd.DataFrame(
            {
                "subject": ["S01", "S02"],
                "session": ["A", "A"],
                "trial": ["1", "1"],
                "x": [1.0, 2.0],
                "y": [3.0, 4.0],
            }
        )
        duck.save("multi_col", df)

        loaded_df = duck.load("multi_col", raw=False)
        assert len(loaded_df) == 2
        assert "x" in loaded_df.columns
        assert "y" in loaded_df.columns


# ---------------------------------------------------------------------------
# Per-column storage: DataFrames as data, dicts (flat & nested)
# ---------------------------------------------------------------------------


class TestPerColumnStorage:
    """Test that DataFrames and dicts are stored as native DuckDB columns."""

    # --- DataFrame as Mode B data value ---

    def test_dataframe_data_roundtrip(self, duck):
        """A pd.DataFrame saved as Mode B data should load back as a DataFrame."""
        original = pd.DataFrame(
            {
                "force": [1.0, 2.0, 3.0],
                "velocity": [4.0, 5.0, 6.0],
                "label": ["a", "b", "c"],
            }
        )
        duck.save("gait_data", original, subject="S01", session="A", trial="1")
        loaded = duck.load("gait_data", subject="S01", session="A", trial="1")

        assert isinstance(loaded, pd.DataFrame)
        assert list(loaded.columns) == ["force", "velocity", "label"]
        np.testing.assert_array_almost_equal(
            loaded["force"].to_numpy(), [1.0, 2.0, 3.0]
        )
        np.testing.assert_array_almost_equal(
            loaded["velocity"].to_numpy(), [4.0, 5.0, 6.0]
        )
        assert list(loaded["label"]) == ["a", "b", "c"]

    def test_dataframe_stored_as_separate_columns(self, duck):
        """The DuckDB table should have one column per DataFrame column."""
        original = pd.DataFrame({"x": [1.0, 2.0], "y": [3.0, 4.0]})
        duck.save("df_cols", original, subject="S01", session="A", trial="1")

        # Check the actual DuckDB table columns
        cols = [
            row[0]
            for row in duck._fetchall(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'df_cols' ORDER BY ordinal_position"
            )
        ]
        assert "x" in cols
        assert "y" in cols
        # Should NOT have a single "value" column
        assert "value" not in cols

    def test_dataframe_column_order_preserved(self, duck):
        """Column order of the original DataFrame should be preserved on load."""
        original = pd.DataFrame({"z": [1], "a": [2], "m": [3]})
        duck.save("col_order", original, subject="S01", session="A", trial="1")
        loaded = duck.load("col_order", subject="S01", session="A", trial="1")
        assert list(loaded.columns) == ["z", "a", "m"]

    # --- Flat dict with non-scalar values ---

    def test_dict_with_arrays_stored_as_columns(self, duck):
        """A dict with array values should use per-column storage, not JSON."""
        original = {"weights": np.array([0.1, 0.2, 0.3]), "bias": 0.5}
        duck.save("params", original, subject="S01", session="A", trial="1")

        # Verify per-column layout in DuckDB
        cols = [
            row[0]
            for row in duck._fetchall(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'params' ORDER BY ordinal_position"
            )
        ]
        assert "weights" in cols
        assert "bias" in cols

        loaded = duck.load("params", subject="S01", session="A", trial="1")
        np.testing.assert_array_almost_equal(loaded["weights"], original["weights"])
        assert loaded["bias"] == 0.5

    def test_dict_with_list_values_stored_as_columns(self, duck):
        """A dict with list values should use per-column storage."""
        original = {"key1": "value1", "key2": 42, "key3": [1, 2, 3]}
        duck.save("mixed_dict", original, subject="S01", session="A", trial="1")

        cols = [
            row[0]
            for row in duck._fetchall(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'mixed_dict' ORDER BY ordinal_position"
            )
        ]
        assert "key1" in cols
        assert "key2" in cols
        assert "key3" in cols

        loaded = duck.load("mixed_dict", subject="S01", session="A", trial="1")
        assert loaded["key1"] == "value1"
        assert loaded["key2"] == 42
        assert loaded["key3"] == pytest.approx([1, 2, 3])

    # --- Nested dicts ---

    def test_nested_dict_roundtrip(self, duck):
        """A nested dict should flatten to columns and unflatten on load."""
        original = {
            "a": {"b": 1, "c": np.array([1.0, 2.0, 3.0])},
            "d": "hello",
        }
        duck.save("nested", original, subject="S01", session="A", trial="1")
        loaded = duck.load("nested", subject="S01", session="A", trial="1")

        assert isinstance(loaded, dict)
        assert isinstance(loaded["a"], dict)
        assert loaded["a"]["b"] == 1
        np.testing.assert_array_almost_equal(loaded["a"]["c"], [1.0, 2.0, 3.0])
        assert loaded["d"] == "hello"

    def test_columnar_dict_roundtrip(self, duck):
        """A dict of equal-length arrays (MATLAB struct pattern) should round-trip."""
        original = {
            "force": np.arange(10, dtype=np.float64),
            "velocity": np.arange(10, 20, dtype=np.float64),
            "label": ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
        }
        duck.save("columnar", original, subject="S01", session="A", trial="1")

        # Verify per-column layout in DuckDB
        cols = [
            row[0]
            for row in duck._fetchall(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'columnar' ORDER BY ordinal_position"
            )
        ]
        assert "force" in cols
        assert "velocity" in cols
        assert "label" in cols

        loaded = duck.load("columnar", subject="S01", session="A", trial="1")
        assert isinstance(loaded, dict)
        np.testing.assert_array_almost_equal(loaded["force"], original["force"])
        np.testing.assert_array_almost_equal(loaded["velocity"], original["velocity"])
        assert list(loaded["label"]) == original["label"]
        # Each value must be a full 10-element vector, NOT a scalar
        assert len(loaded["force"]) == 10
        assert len(loaded["velocity"]) == 10
        assert len(loaded["label"]) == 10

    def test_columnar_dict_2d_roundtrip(self, duck):
        """A dict of Nx1 (2D) arrays — the shape MATLAB to_python produces — should round-trip."""
        original = {
            "force": np.arange(10, dtype=np.float64).reshape(10, 1),
            "velocity": np.arange(10, 20, dtype=np.float64).reshape(10, 1),
        }
        duck.save("matlab_struct", original, subject="S01", session="A", trial="1")

        # Verify per-column layout
        cols = [
            row[0]
            for row in duck._fetchall(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'matlab_struct' ORDER BY ordinal_position"
            )
        ]
        assert "force" in cols
        assert "velocity" in cols

        loaded = duck.load("matlab_struct", subject="S01", session="A", trial="1")
        assert isinstance(loaded, dict)
        # Must be full 10-element arrays, NOT scalars
        assert loaded["force"].shape == (10, 1)
        assert loaded["velocity"].shape == (10, 1)
        np.testing.assert_array_almost_equal(loaded["force"], original["force"])
        np.testing.assert_array_almost_equal(loaded["velocity"], original["velocity"])

    def test_nested_dict_stored_as_flat_columns(self, duck):
        """Nested dict keys should become dot-separated DuckDB column names."""
        original = {"outer": {"inner1": 10, "inner2": 20}, "top": 30}
        duck.save("nested_cols", original, subject="S01", session="A", trial="1")

        cols = [
            row[0]
            for row in duck._fetchall(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'nested_cols' ORDER BY ordinal_position"
            )
        ]
        assert "outer.inner1" in cols
        assert "outer.inner2" in cols
        assert "top" in cols


# ---------------------------------------------------------------------------
# DuckDB storage type verification
# ---------------------------------------------------------------------------


def _col_types(duck, table_name):
    """Return {col_name: data_type} for a DuckDB table, excluding schema_id."""
    rows = duck._fetchall(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = ? ORDER BY ordinal_position",
        [table_name],
    )
    return {r[0]: r[1] for r in rows if r[0] != "schema_id"}


class TestDictDuckDBTypes:
    """Verify that dict values map to the correct DuckDB column types."""

    def test_scalar_only_dict(self, duck):
        """Dict of pure scalars: int→BIGINT, float→DOUBLE, str→VARCHAR."""
        original = {"count": 42, "ratio": 3.14, "name": "hello"}
        duck.save("scalars", original, subject="S01", session="A", trial="1")

        types = _col_types(duck, "scalars")
        assert types["count"] == "BIGINT"
        assert types["ratio"] == "DOUBLE"
        assert types["name"] == "VARCHAR"

        loaded = duck.load("scalars", subject="S01", session="A", trial="1")
        assert loaded["count"] == 42
        assert loaded["ratio"] == pytest.approx(3.14)
        assert loaded["name"] == "hello"

    def test_float_array_dict(self, duck):
        """Dict with float arrays → DOUBLE[]."""
        original = {
            "force": np.array([1.0, 2.0, 3.0]),
            "velocity": np.array([4.0, 5.0, 6.0]),
        }
        duck.save("fvec", original, subject="S01", session="A", trial="1")

        types = _col_types(duck, "fvec")
        assert types["force"] == "DOUBLE[]"
        assert types["velocity"] == "DOUBLE[]"

        loaded = duck.load("fvec", subject="S01", session="A", trial="1")
        np.testing.assert_array_almost_equal(loaded["force"], original["force"])
        np.testing.assert_array_almost_equal(loaded["velocity"], original["velocity"])

    def test_int_array_dict(self, duck):
        """Dict with int arrays → BIGINT[]."""
        original = {
            "ids": np.array([10, 20, 30], dtype=np.int64),
            "counts": np.array([1, 2, 3], dtype=np.int64),
        }
        duck.save("ivec", original, subject="S01", session="A", trial="1")

        types = _col_types(duck, "ivec")
        assert types["ids"] == "BIGINT[]"
        assert types["counts"] == "BIGINT[]"

        loaded = duck.load("ivec", subject="S01", session="A", trial="1")
        np.testing.assert_array_equal(loaded["ids"], original["ids"])
        np.testing.assert_array_equal(loaded["counts"], original["counts"])

    def test_string_list_dict(self, duck):
        """Dict with list-of-strings → VARCHAR[]."""
        original = {
            "tags": ["alpha", "beta", "gamma"],
            "labels": ["x", "y"],
        }
        duck.save("strvec", original, subject="S01", session="A", trial="1")

        types = _col_types(duck, "strvec")
        assert types["tags"] == "VARCHAR[]"
        assert types["labels"] == "VARCHAR[]"

        loaded = duck.load("strvec", subject="S01", session="A", trial="1")
        assert list(loaded["tags"]) == original["tags"]
        assert list(loaded["labels"]) == original["labels"]

    def test_mixed_scalar_and_vector_dict(self, duck):
        """Dict mixing scalars and arrays → correct per-key types."""
        original = {
            "name": "experiment_1",
            "weight": 0.75,
            "scores": np.array([90.0, 85.5, 92.3]),
            "trial_ids": np.array([1, 2, 3], dtype=np.int64),
        }
        duck.save("mixed", original, subject="S01", session="A", trial="1")

        types = _col_types(duck, "mixed")
        assert types["name"] == "VARCHAR"
        assert types["weight"] == "DOUBLE"
        assert types["scores"] == "DOUBLE[]"
        assert types["trial_ids"] == "BIGINT[]"

        loaded = duck.load("mixed", subject="S01", session="A", trial="1")
        assert loaded["name"] == "experiment_1"
        assert loaded["weight"] == pytest.approx(0.75)
        np.testing.assert_array_almost_equal(loaded["scores"], original["scores"])
        np.testing.assert_array_equal(loaded["trial_ids"], original["trial_ids"])

    def test_bool_scalar_dict(self, duck):
        """Dict with bool scalar → BOOLEAN."""
        original = {"flag": True, "enabled": False}
        duck.save("bools", original, subject="S01", session="A", trial="1")

        types = _col_types(duck, "bools")
        assert types["flag"] == "BOOLEAN"
        assert types["enabled"] == "BOOLEAN"

        loaded = duck.load("bools", subject="S01", session="A", trial="1")
        assert loaded["flag"] is True
        assert loaded["enabled"] is False

    def test_2d_array_dict(self, duck):
        """Dict with 2D arrays → DOUBLE[][]."""
        original = {
            "matrix": np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]),
        }
        duck.save("mat", original, subject="S01", session="A", trial="1")

        types = _col_types(duck, "mat")
        assert types["matrix"] == "DOUBLE[][]"

        loaded = duck.load("mat", subject="S01", session="A", trial="1")
        np.testing.assert_array_almost_equal(loaded["matrix"], original["matrix"])

    def test_equal_length_arrays_stored_as_one_row(self, duck):
        """Dict-of-equal-length-arrays must be ONE record, not N rows."""
        original = {
            "force": np.arange(10, dtype=np.float64),
            "velocity": np.arange(10, 20, dtype=np.float64),
        }
        duck.save("vectors", original, subject="S01", session="A", trial="1")

        # Must be array columns (DOUBLE[]), not scalar DOUBLE
        types = _col_types(duck, "vectors")
        assert types["force"] == "DOUBLE[]"
        assert types["velocity"] == "DOUBLE[]"

        # Must be exactly one row in the table
        count = duck._fetchall('SELECT COUNT(*) FROM "vectors"')[0][0]
        assert count == 1

        loaded = duck.load("vectors", subject="S01", session="A", trial="1")
        assert len(loaded["force"]) == 10
        assert len(loaded["velocity"]) == 10


class TestDataFrameDuckDBTypes:
    """Verify that DataFrame columns map to the correct DuckDB column types.

    DataFrames are stored as one DuckDB row per DataFrame row, so each
    column is a scalar type (DOUBLE, BIGINT, VARCHAR), not an array type.
    """

    def test_float_columns(self, duck):
        """DataFrame float columns → DOUBLE (one row per DataFrame row)."""
        original = pd.DataFrame(
            {
                "force": [1.0, 2.0, 3.0],
                "velocity": [4.0, 5.0, 6.0],
            }
        )
        duck.save("df_float", original, subject="S01", session="A", trial="1")

        types = _col_types(duck, "df_float")
        assert types["force"] == "DOUBLE"
        assert types["velocity"] == "DOUBLE"

        loaded = duck.load("df_float", subject="S01", session="A", trial="1")
        assert isinstance(loaded, pd.DataFrame)
        np.testing.assert_array_almost_equal(
            loaded["force"].to_numpy(), [1.0, 2.0, 3.0]
        )
        np.testing.assert_array_almost_equal(
            loaded["velocity"].to_numpy(), [4.0, 5.0, 6.0]
        )

    def test_int_columns(self, duck):
        """DataFrame int columns → BIGINT (one row per DataFrame row)."""
        original = pd.DataFrame(
            {
                "ids": [10, 20, 30],
                "counts": [1, 2, 3],
            }
        )
        duck.save("df_int", original, subject="S01", session="A", trial="1")

        types = _col_types(duck, "df_int")
        assert types["ids"] == "BIGINT"
        assert types["counts"] == "BIGINT"

        loaded = duck.load("df_int", subject="S01", session="A", trial="1")
        assert isinstance(loaded, pd.DataFrame)
        np.testing.assert_array_equal(loaded["ids"].to_numpy(), [10, 20, 30])
        np.testing.assert_array_equal(loaded["counts"].to_numpy(), [1, 2, 3])

    def test_string_columns(self, duck):
        """DataFrame string columns → VARCHAR (one row per DataFrame row)."""
        original = pd.DataFrame(
            {
                "name": ["alice", "bob", "carol"],
                "group": ["A", "B", "A"],
            }
        )
        duck.save("df_str", original, subject="S01", session="A", trial="1")

        types = _col_types(duck, "df_str")
        assert types["name"] == "VARCHAR"
        assert types["group"] == "VARCHAR"

        loaded = duck.load("df_str", subject="S01", session="A", trial="1")
        assert isinstance(loaded, pd.DataFrame)
        assert list(loaded["name"]) == ["alice", "bob", "carol"]
        assert list(loaded["group"]) == ["A", "B", "A"]

    def test_mixed_columns(self, duck):
        """DataFrame with float, int, and string columns → correct scalar types."""
        original = pd.DataFrame(
            {
                "score": [95.5, 87.3, 91.0],
                "rank": np.array([1, 2, 3], dtype=np.int64),
                "name": ["alice", "bob", "carol"],
            }
        )
        duck.save("df_mixed", original, subject="S01", session="A", trial="1")

        types = _col_types(duck, "df_mixed")
        assert types["score"] == "DOUBLE"
        assert types["rank"] == "BIGINT"
        assert types["name"] == "VARCHAR"

        loaded = duck.load("df_mixed", subject="S01", session="A", trial="1")
        assert isinstance(loaded, pd.DataFrame)
        assert list(loaded.columns) == ["score", "rank", "name"]
        np.testing.assert_array_almost_equal(
            loaded["score"].to_numpy(), [95.5, 87.3, 91.0]
        )
        np.testing.assert_array_equal(loaded["rank"].to_numpy(), [1, 2, 3])
        assert list(loaded["name"]) == ["alice", "bob", "carol"]

    def test_single_row_dataframe(self, duck):
        """A 1-row DataFrame should still round-trip as a DataFrame."""
        original = pd.DataFrame({"x": [42.0], "label": ["only"]})
        duck.save("df_1row", original, subject="S01", session="A", trial="1")

        types = _col_types(duck, "df_1row")
        assert types["x"] == "DOUBLE"
        assert types["label"] == "VARCHAR"

        loaded = duck.load("df_1row", subject="S01", session="A", trial="1")
        assert isinstance(loaded, pd.DataFrame)
        assert len(loaded) == 1


# ---------------------------------------------------------------------------
# Save / Load — Mode C (dict with tuple keys)
# ---------------------------------------------------------------------------


class TestSaveLoadModeC:
    """Test save/load with dict mapping tuples to values."""

    def test_save_dict_with_tuple_keys(self, duck):
        """Save a dict mapping (subject, session, trial) tuples to values."""
        data = {
            ("S01", "A", "1"): 100,
            ("S01", "A", "2"): 200,
            ("S02", "A", "1"): 300,
        }
        duck.save("tuple_dict", data)

        loaded = duck.load("tuple_dict", subject="S01", session="A", trial="1")
        assert loaded == 100

        loaded = duck.load("tuple_dict", subject="S02", session="A", trial="1")
        assert loaded == 300


# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------


class TestVersioning:
    """Test version management features."""

    def test_latest_value_loaded_by_default(self, duck):
        """Re-saving to same schema entry replaces the previous value."""
        duck.save("versioned", 10, subject="S01", session="A", trial="1")
        duck.save("versioned", 20, subject="S01", session="A", trial="1")
        duck.save("versioned", 30, subject="S01", session="A", trial="1")

        loaded = duck.load("versioned", subject="S01", session="A", trial="1")
        assert loaded == 30

    def test_list_versions(self, duck):
        """list_versions should return one row per variable."""
        duck.save("versioned", 10, subject="S01", session="A", trial="1")
        duck.save("versioned", 20, subject="S01", session="A", trial="1")

        versions = duck.list_versions("versioned")
        assert len(versions) == 1


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


class TestDelete:
    """Test delete functionality."""

    def test_delete_variable(self, duck):
        """Should delete a variable and all its data."""
        duck.save("to_delete", 42, subject="S01", session="A", trial="1")
        duck.delete("to_delete")

        # Variable table should be gone
        assert not duck._table_exists("to_delete")

        # Should not appear in list_variables
        var_list = duck.list_variables()
        assert len(var_list[var_list["variable_name"] == "to_delete"]) == 0


# ---------------------------------------------------------------------------
# Groups
# ---------------------------------------------------------------------------


class TestGroups:
    """Test variable grouping functionality."""

    def test_add_to_group(self, duck):
        """Should add variables to a group."""
        duck.save("var_a", 1, subject="S01", session="A", trial="1")
        duck.save("var_b", 2, subject="S01", session="A", trial="1")

        duck.add_to_group("my_group", ["var_a", "var_b"])

        members = duck.get_group("my_group")
        assert "var_a" in members
        assert "var_b" in members

    def test_add_single_string(self, duck):
        """Should accept a single string instead of list."""
        duck.add_to_group("single", "var_x")
        members = duck.get_group("single")
        assert "var_x" in members

    def test_remove_from_group(self, duck):
        """Should remove variable from group."""
        duck.add_to_group("grp", ["var_a", "var_b"])
        duck.remove_from_group("grp", "var_a")

        members = duck.get_group("grp")
        assert "var_a" not in members
        assert "var_b" in members

    def test_list_groups(self, duck):
        """Should list all group names."""
        duck.add_to_group("group_1", "a")
        duck.add_to_group("group_2", "b")

        groups = duck.list_groups()
        assert "group_1" in groups
        assert "group_2" in groups

    def test_add_duplicate_is_idempotent(self, duck):
        """Adding same variable to same group twice should not duplicate."""
        duck.add_to_group("grp", "var_a")
        duck.add_to_group("grp", "var_a")

        members = duck.get_group("grp")
        assert members.count("var_a") == 1


# ---------------------------------------------------------------------------
# List / Inspect
# ---------------------------------------------------------------------------


class TestListInspect:
    """Test listing and inspection features."""

    def test_list_variables(self, duck):
        """Should list all variables with metadata."""
        duck.save("alpha", 1, subject="S01", session="A", trial="1")
        duck.save("beta", 2, subject="S01", session="A", trial="1")

        var_list = duck.list_variables()
        assert len(var_list) == 2
        names = list(var_list["variable_name"])
        assert "alpha" in names
        assert "beta" in names


# ---------------------------------------------------------------------------
# Error Handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    """Test error handling."""

    def test_load_nonexistent_variable_raises(self, duck):
        """Loading a non-existent variable should raise KeyError."""
        with pytest.raises(KeyError, match="not found"):
            duck.load("nonexistent")

    def test_invalid_schema_level_raises(self, duck):
        """Using invalid schema_level should raise ValueError."""
        with pytest.raises(ValueError, match="not in"):
            duck.save("bad_level", 42, schema_level="nonexistent", subject="S01")

    def test_partial_schema_keys_saves_at_deepest_provided(self, duck):
        """Providing a subset of schema keys should save at the deepest provided level."""
        duck.save("partial", 42, subject="S01")
        loaded = duck.load("partial", subject="S01")
        assert loaded == 42


class TestAutocommitFailureRecovery:
    """A failed autocommit statement (no explicit BEGIN) must not leave the
    connection stuck aborted for later, unrelated statements.

    Regression for a production bug: scistack-gui's pipeline_store issued a
    migration ALTER TABLE that failed with CatalogException on every restart
    (column already existed) and caught the Python exception with a bare
    try/except. That did not clear DuckDB's connection-level aborted-
    transaction state, so the next statement on the shared connection --
    unrelated to the migration -- failed too with TransactionException
    ("Current transaction is aborted"), and every statement after that did
    as well, for the remaining life of the process. `_execute` et al. now
    issue a best-effort ROLLBACK after any autocommit failure so a caught
    exception can't cascade like this.
    """

    def test_execute_failure_does_not_poison_next_statement(self, duck):
        with pytest.raises(Exception):
            duck._execute("SELECT * FROM this_table_does_not_exist")
        # Must succeed -- would raise TransactionException before the fix.
        duck._execute("CREATE TABLE IF NOT EXISTS _recovery_probe (x INTEGER)")

    def test_repeated_failures_do_not_poison_connection(self, duck):
        for _ in range(5):
            with pytest.raises(Exception):
                duck._execute("ALTER TABLE this_table_does_not_exist ADD COLUMN y INTEGER")
        duck._execute("CREATE TABLE IF NOT EXISTS _recovery_probe (x INTEGER)")
        assert duck._fetchall("SELECT COUNT(*) FROM _recovery_probe") == [(0,)]

    def test_fetchall_failure_does_not_poison_next_statement(self, duck):
        with pytest.raises(Exception):
            duck._fetchall("SELECT * FROM this_table_does_not_exist")
        assert duck._fetchall("SELECT 1") == [(1,)]

    def test_failure_inside_explicit_transaction_does_not_clear_tx_owner(self, duck):
        """An explicit transaction's rollback is the caller's call, not ours
        to issue out from under them -- _recover_from_autocommit_failure must
        skip while `_tx_owner` is set.

        Asserted directly on `_tx_owner` rather than on whether a later
        statement fails: DuckDB's own transaction-poisoning behavior turns
        out to vary by error type (a bind-time CatalogException from a
        SELECT on a missing table does not necessarily abort an explicit
        transaction the way a DDL catalog conflict does), and that's not
        what this guard is responsible for -- only `_commit`/`_rollback`
        should ever clear `_tx_owner`.
        """
        duck._begin()
        thread = threading.get_ident()
        assert duck._tx_owner == thread
        with pytest.raises(Exception):
            duck._execute("SELECT * FROM this_table_does_not_exist")
        assert duck._tx_owner == thread
        duck._rollback()
        assert duck._tx_owner is None
        duck._execute("SELECT 1")  # connection still usable afterward


# ---------------------------------------------------------------------------
# Dtype Preservation
# ---------------------------------------------------------------------------


class TestDtypePreservation:
    """Test that numpy dtypes are preserved through round-trip."""

    @pytest.mark.parametrize("dtype", [np.int32, np.int64, np.float32, np.float64])
    def test_1d_array_dtype(self, duck, dtype):
        """1D array dtype should be preserved."""
        original = np.array([1, 2, 3], dtype=dtype)
        duck.save(f"typed_{dtype}", original, subject="S01", session="A", trial="1")
        loaded = duck.load(f"typed_{dtype}", subject="S01", session="A", trial="1")
        assert np.asarray(loaded).dtype == dtype

    def test_2d_array_shape_preserved(self, duck):
        """2D array shape should be preserved."""
        original = np.array([[1, 2], [3, 4], [5, 6]], dtype=np.float64)
        duck.save("matrix", original, subject="S01", session="A", trial="1")
        loaded = duck.load("matrix", subject="S01", session="A", trial="1")
        assert np.asarray(loaded).shape == (3, 2)


# ---------------------------------------------------------------------------
# Schema Levels
# ---------------------------------------------------------------------------


class TestSchemaLevels:
    """Test saving at different schema levels."""

    def test_save_at_higher_level(self, duck):
        """Should save at subject level (above default trial level)."""
        duck.save("subject_var", 42, schema_level="subject", subject="S01")
        loaded = duck.load("subject_var", subject="S01")
        assert loaded == 42

    def test_save_at_middle_level(self, duck):
        """Should save at session level."""
        duck.save("session_var", 99, schema_level="session", subject="S01", session="A")
        loaded = duck.load("session_var", subject="S01", session="A")
        assert loaded == 99


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


class TestPersistence:
    """Test data persistence across connections."""

    def test_data_survives_reconnect(self, tmp_path):
        """Data should be available after close and reopen."""
        db_path = tmp_path / "persist.duckdb"
        schema = ["subject"]

        db1 = SciDuck(db_path, dataset_schema=schema)
        db1.save("persisted", 42, subject="S01")
        db1.close()

        db2 = SciDuck(db_path, dataset_schema=schema)
        loaded = db2.load("persisted", subject="S01")
        db2.close()

        assert loaded == 42

    def test_variable_survives_reconnect(self, tmp_path):
        """Variable metadata should survive reconnection."""
        db_path = tmp_path / "persist_ver.duckdb"
        schema = ["subject"]

        db1 = SciDuck(db_path, dataset_schema=schema)
        db1.save("versioned", 10, subject="S01")
        db1.close()

        db2 = SciDuck(db_path, dataset_schema=schema)
        versions = db2.list_versions("versioned")
        db2.close()

        assert len(versions) == 1


# ---------------------------------------------------------------------------
# Direct Query
# ---------------------------------------------------------------------------


class TestDirectQuery:
    """Test the query() method for arbitrary SQL."""

    def test_query_returns_dataframe(self, duck):
        """query() should return a pandas DataFrame."""
        duck.save("qvar", 42, subject="S01", session="A", trial="1")
        result = duck.query('SELECT * FROM "qvar"')
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 1


# ---------------------------------------------------------------------------
# Non-Contiguous Schema Keys
# ---------------------------------------------------------------------------


@pytest.fixture
def wide_duck():
    """Provide a SciDuck with a wider schema for non-contiguous key tests."""
    db = SciDuck(
        ":memory:",
        dataset_schema=["subject", "intervention", "timepoint", "speed", "trial"],
    )
    yield db
    db.close()


class TestNonContiguousSchemaKeys:
    """Test saving/loading with non-contiguous subsets of schema keys."""

    def test_save_with_non_contiguous_keys(self, wide_duck):
        """Should save with keys that skip intermediate levels."""
        wide_duck.save(
            "cohens_d",
            0.85,
            subject="S01",
            intervention="RMT30",
            speed="SSV",
        )

    def test_load_non_contiguous_round_trip(self, wide_duck):
        """Should round-trip data saved with non-contiguous keys."""
        wide_duck.save(
            "cohens_d",
            0.85,
            subject="S01",
            intervention="RMT30",
            speed="SSV",
        )
        loaded = wide_duck.load(
            "cohens_d",
            subject="S01",
            intervention="RMT30",
            speed="SSV",
        )
        assert abs(loaded - 0.85) < 1e-10

    def test_schema_row_has_nulls_for_skipped_keys(self, wide_duck):
        """Skipped schema keys should be NULL in _schema table."""
        wide_duck.save(
            "cohens_d",
            0.85,
            subject="S01",
            intervention="RMT30",
            speed="SSV",
        )
        row = wide_duck.query(
            "SELECT * FROM _schema WHERE schema_level = ?",
            params=["speed"],
        )
        # timepoint and trial should be NULL
        assert row["timepoint"].iloc[0] is None
        assert row["trial"].iloc[0] is None
        # provided keys should be populated
        assert row["subject"].iloc[0] == "S01"
        assert row["intervention"].iloc[0] == "RMT30"
        assert row["speed"].iloc[0] == "SSV"

    def test_schema_level_is_deepest_provided_key(self, wide_duck):
        """schema_level should be the deepest provided key in the hierarchy."""
        wide_duck.save(
            "cohens_d",
            0.85,
            subject="S01",
            intervention="RMT30",
            speed="SSV",
        )
        row = wide_duck.query(
            "SELECT schema_level FROM _variables WHERE variable_name = 'cohens_d'"
        )
        assert row["schema_level"].iloc[0] == "speed"

    def test_contiguous_keys_still_work(self, wide_duck):
        """Contiguous prefix keys should still work identically."""
        wide_duck.save(
            "contiguous_var",
            99,
            subject="S01",
            intervention="RMT30",
        )
        loaded = wide_duck.load(
            "contiguous_var",
            subject="S01",
            intervention="RMT30",
        )
        assert loaded == 99

    def test_multiple_non_contiguous_saves_create_distinct_schema_entries(
        self, wide_duck
    ):
        """Separate saves with different non-contiguous keys create distinct _schema rows."""
        wide_duck.save(
            "cohens_d",
            0.85,
            subject="S01",
            intervention="RMT30",
            speed="SSV",
        )
        wide_duck.save(
            "cohens_d",
            0.92,
            subject="S01",
            intervention="RMT30",
            speed="FV",
        )
        # Verify two distinct _schema rows were created
        schema_rows = wide_duck.query(
            "SELECT * FROM _schema WHERE schema_level = 'speed'"
        )
        assert len(schema_rows) == 2
        speeds = set(schema_rows["speed"])
        assert speeds == {"SSV", "FV"}

    def test_load_resolves_correct_schema_keys(self, wide_duck):
        """load() should return the value matching the given schema keys."""
        wide_duck.save(
            "cohens_d",
            0.85,
            subject="S01",
            intervention="RMT30",
            speed="SSV",
        )
        wide_duck.save(
            "cohens_d",
            0.92,
            subject="S01",
            intervention="RMT30",
            speed="FV",
        )
        v1 = wide_duck.load("cohens_d", subject="S01", speed="SSV")
        v2 = wide_duck.load("cohens_d", subject="S01", speed="FV")
        assert abs(v1 - 0.85) < 1e-10
        assert abs(v2 - 0.92) < 1e-10

    def test_partial_key_load_filtering(self, wide_duck):
        """Loading with a filter on a non-contiguous key should work."""
        wide_duck.save(
            "cohens_d",
            0.85,
            subject="S01",
            intervention="RMT30",
            speed="SSV",
        )
        # Load filtering by subject
        loaded = wide_duck.load("cohens_d", subject="S01")
        assert abs(loaded - 0.85) < 1e-10
        # Load filtering by non-contiguous key (speed)
        loaded = wide_duck.load("cohens_d", speed="SSV")
        assert abs(loaded - 0.85) < 1e-10

    def test_load_filter_on_non_contiguous_key(self, wide_duck):
        """Should filter on a non-contiguous key during load."""
        wide_duck.save(
            "cohens_d",
            0.85,
            subject="S01",
            intervention="RMT30",
            speed="SSV",
        )
        wide_duck.save(
            "cohens_d",
            0.92,
            subject="S01",
            intervention="RMT30",
            speed="FV",
        )
        loaded = wide_duck.load("cohens_d", subject="S01", speed="FV")
        assert abs(loaded - 0.92) < 1e-10

    def test_batch_then_single_schema_id_no_collision(self, wide_duck):
        """Schema IDs must not collide when batch and single-row paths are interleaved.

        Regression test: the batch path allocates IDs via MAX(schema_id)+1,
        while the single-row path formerly used a DuckDB sequence.  If the
        sequence fell out of sync, the single-row INSERT would hit a duplicate
        primary-key error on _schema.
        """
        # 1. Batch-create several schema entries at once
        combos = {
            ("speed", ("S01", "RMT30", "SSV")): {
                "subject": "S01",
                "intervention": "RMT30",
                "speed": "SSV",
            },
            ("speed", ("S01", "RMT30", "FV")): {
                "subject": "S01",
                "intervention": "RMT30",
                "speed": "FV",
            },
            ("speed", ("S02", "RMT30", "SSV")): {
                "subject": "S02",
                "intervention": "RMT30",
                "speed": "SSV",
            },
        }
        batch_ids = wide_duck.batch_get_or_create_schema_ids(combos)
        assert len(batch_ids) == 3

        # 2. Single-row create for a NEW combination — must not collide
        new_id = wide_duck._get_or_create_schema_id(
            "speed",
            {"subject": "S02", "intervention": "RMT30", "speed": "FV"},
        )
        assert new_id not in batch_ids.values()

        # 3. Single-row lookup for an EXISTING combination — must return same ID
        existing_id = wide_duck._get_or_create_schema_id(
            "speed",
            {"subject": "S01", "intervention": "RMT30", "speed": "SSV"},
        )
        assert existing_id == batch_ids[("speed", ("S01", "RMT30", "SSV"))]

    def test_single_then_batch_schema_id_no_collision(self, wide_duck):
        """Schema IDs must not collide when single-row creates precede a batch."""
        # 1. Single-row create
        id1 = wide_duck._get_or_create_schema_id(
            "speed",
            {"subject": "S01", "intervention": "RMT30", "speed": "SSV"},
        )

        # 2. Batch-create including an overlapping and a new combination
        combos = {
            ("speed", ("S01", "RMT30", "SSV")): {
                "subject": "S01",
                "intervention": "RMT30",
                "speed": "SSV",
            },
            ("speed", ("S01", "RMT30", "FV")): {
                "subject": "S01",
                "intervention": "RMT30",
                "speed": "FV",
            },
        }
        batch_ids = wide_duck.batch_get_or_create_schema_ids(combos)

        # Overlapping combo should reuse the same ID
        assert batch_ids[("speed", ("S01", "RMT30", "SSV"))] == id1
        # New combo should have a distinct, non-colliding ID
        assert batch_ids[("speed", ("S01", "RMT30", "FV"))] != id1


def test_is_open_follows_close_and_reopen(tmp_path):
    """SciDuck owns its connection state: a best-effort reader (scidb's
    display-only alias validation) asks is_open instead of querying a closed
    connection, which _fetchdf would log as an ERROR."""
    from sciduckdb import SciDuck

    db = SciDuck(str(tmp_path / "open.duckdb"), dataset_schema=["subject"])
    assert db.is_open
    db.close()
    assert not db.is_open
    db.reopen()
    assert db.is_open
    db.close()
