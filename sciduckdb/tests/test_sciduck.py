"""Integration tests for SciDuck: codec round trips, connection, schema ids."""

import threading

import numpy as np
import pandas as pd
import pytest

from sciduckdb import SciDuck

from .codec_roundtrip import column_types, fetch, store


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
            for row in duck.fetchall(
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
        # Populate _schema
        db1.batch_get_or_create_schema_ids(
            {("session", ("S01", "A")): {"subject": "S01", "session": "A"}}
        )
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
            store(db, "x", 1)
        # Connection should be closed after exiting


# ---------------------------------------------------------------------------
# Codec round trip: one value per row
# ---------------------------------------------------------------------------


class TestCodecRoundTrip:
    """Values stored through the codec in a real table load back unchanged."""

    def test_save_and_load_int(self, duck):
        """Round-trip an integer."""
        meta = store(duck, "my_int", 42)
        loaded = fetch(duck, "my_int", meta)
        assert loaded == 42

    def test_save_and_load_float(self, duck):
        """Round-trip a float."""
        meta = store(duck, "my_float", 3.14)
        loaded = fetch(duck, "my_float", meta)
        assert abs(loaded - 3.14) < 1e-10

    def test_save_and_load_string(self, duck):
        """Round-trip a string."""
        meta = store(duck, "my_str", "hello world")
        loaded = fetch(duck, "my_str", meta)
        assert loaded == "hello world"

    def test_save_and_load_bool(self, duck):
        """Round-trip a boolean. A single-column load returns the restored
        cell, which pandas holds as ``np.bool_`` (scidb's load does the same)."""
        meta = store(duck, "my_bool", True)
        loaded = fetch(duck, "my_bool", meta)
        assert isinstance(loaded, (bool, np.bool_))
        assert loaded == True  # noqa: E712 - np.True_ is not True

    def test_save_and_load_1d_array(self, duck):
        """Round-trip a 1D numpy array."""
        original = np.array([1.0, 2.0, 3.0, 4.0])
        meta = store(duck, "arr1d", original)
        loaded = fetch(duck, "arr1d", meta)
        np.testing.assert_array_almost_equal(loaded, original)

    def test_save_and_load_2d_array(self, duck):
        """Round-trip a 2D numpy array."""
        original = np.array([[1, 2, 3], [4, 5, 6]])
        meta = store(duck, "arr2d", original)
        loaded = fetch(duck, "arr2d", meta)
        np.testing.assert_array_equal(loaded, original)

    def test_save_and_load_list_of_floats(self, duck):
        """Round-trip a list of floats."""
        original = [1.0, 2.5, 3.7]
        meta = store(duck, "my_list", original)
        loaded = fetch(duck, "my_list", meta)
        assert loaded == pytest.approx(original)

    def test_save_and_load_list_of_strings(self, duck):
        """Round-trip a list of strings."""
        original = ["alpha", "beta", "gamma"]
        meta = store(duck, "str_list", original)
        loaded = fetch(duck, "str_list", meta)
        assert list(loaded) == original

    def test_save_and_load_dict(self, duck):
        """Round-trip a dict (stored as JSON)."""
        original = {"key1": "value1", "key2": 42, "key3": [1, 2, 3]}
        meta = store(duck, "my_dict", original)
        loaded = fetch(duck, "my_dict", meta)
        assert loaded["key1"] == "value1"
        assert loaded["key2"] == 42
        assert loaded["key3"] == [1, 2, 3]

    def test_save_and_load_dict_with_ndarray(self, duck):
        """Round-trip a dict containing numpy arrays."""
        original = {"weights": np.array([0.1, 0.2, 0.3]), "bias": 0.5}
        meta = store(duck, "params", original)
        loaded = fetch(duck, "params", meta)
        np.testing.assert_array_almost_equal(loaded["weights"], original["weights"])
        assert loaded["bias"] == 0.5


# ---------------------------------------------------------------------------
# Per-column storage: DataFrames as data, dicts (flat & nested)
# ---------------------------------------------------------------------------


class TestPerColumnStorage:
    """Test that DataFrames and dicts are stored as native DuckDB columns."""

    # --- DataFrame as the data value ---

    def test_dataframe_data_roundtrip(self, duck):
        """A pd.DataFrame value should load back as a DataFrame."""
        original = pd.DataFrame(
            {
                "force": [1.0, 2.0, 3.0],
                "velocity": [4.0, 5.0, 6.0],
                "label": ["a", "b", "c"],
            }
        )
        meta = store(duck, "gait_data", original)
        loaded = fetch(duck, "gait_data", meta)

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
        store(duck, "df_cols", original)

        # Check the actual DuckDB table columns
        cols = [
            row[0]
            for row in duck.fetchall(
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
        meta = store(duck, "col_order", original)
        loaded = fetch(duck, "col_order", meta)
        assert list(loaded.columns) == ["z", "a", "m"]

    # --- Flat dict with non-scalar values ---

    def test_dict_with_arrays_stored_as_columns(self, duck):
        """A dict with array values should use per-column storage, not JSON."""
        original = {"weights": np.array([0.1, 0.2, 0.3]), "bias": 0.5}
        meta = store(duck, "params", original)

        # Verify per-column layout in DuckDB
        cols = [
            row[0]
            for row in duck.fetchall(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'params' ORDER BY ordinal_position"
            )
        ]
        assert "weights" in cols
        assert "bias" in cols

        loaded = fetch(duck, "params", meta)
        np.testing.assert_array_almost_equal(loaded["weights"], original["weights"])
        assert loaded["bias"] == 0.5

    def test_dict_with_list_values_stored_as_columns(self, duck):
        """A dict with list values should use per-column storage."""
        original = {"key1": "value1", "key2": 42, "key3": [1, 2, 3]}
        meta = store(duck, "mixed_dict", original)

        cols = [
            row[0]
            for row in duck.fetchall(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'mixed_dict' ORDER BY ordinal_position"
            )
        ]
        assert "key1" in cols
        assert "key2" in cols
        assert "key3" in cols

        loaded = fetch(duck, "mixed_dict", meta)
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
        meta = store(duck, "nested", original)
        loaded = fetch(duck, "nested", meta)

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
        meta = store(duck, "columnar", original)

        # Verify per-column layout in DuckDB
        cols = [
            row[0]
            for row in duck.fetchall(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'columnar' ORDER BY ordinal_position"
            )
        ]
        assert "force" in cols
        assert "velocity" in cols
        assert "label" in cols

        loaded = fetch(duck, "columnar", meta)
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
        meta = store(duck, "matlab_struct", original)

        # Verify per-column layout
        cols = [
            row[0]
            for row in duck.fetchall(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'matlab_struct' ORDER BY ordinal_position"
            )
        ]
        assert "force" in cols
        assert "velocity" in cols

        loaded = fetch(duck, "matlab_struct", meta)
        assert isinstance(loaded, dict)
        # Must be full 10-element arrays, NOT scalars
        assert loaded["force"].shape == (10, 1)
        assert loaded["velocity"].shape == (10, 1)
        np.testing.assert_array_almost_equal(loaded["force"], original["force"])
        np.testing.assert_array_almost_equal(loaded["velocity"], original["velocity"])

    def test_nested_dict_stored_as_flat_columns(self, duck):
        """Nested dict keys should become dot-separated DuckDB column names."""
        original = {"outer": {"inner1": 10, "inner2": 20}, "top": 30}
        store(duck, "nested_cols", original)

        cols = [
            row[0]
            for row in duck.fetchall(
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


class TestDictDuckDBTypes:
    """Verify that dict values map to the correct DuckDB column types."""

    def test_scalar_only_dict(self, duck):
        """Dict of pure scalars: int→BIGINT, float→DOUBLE, str→VARCHAR."""
        original = {"count": 42, "ratio": 3.14, "name": "hello"}
        meta = store(duck, "scalars", original)

        types = column_types(duck, "scalars")
        assert types["count"] == "BIGINT"
        assert types["ratio"] == "DOUBLE"
        assert types["name"] == "VARCHAR"

        loaded = fetch(duck, "scalars", meta)
        assert loaded["count"] == 42
        assert loaded["ratio"] == pytest.approx(3.14)
        assert loaded["name"] == "hello"

    def test_float_array_dict(self, duck):
        """Dict with float arrays → DOUBLE[]."""
        original = {
            "force": np.array([1.0, 2.0, 3.0]),
            "velocity": np.array([4.0, 5.0, 6.0]),
        }
        meta = store(duck, "fvec", original)

        types = column_types(duck, "fvec")
        assert types["force"] == "DOUBLE[]"
        assert types["velocity"] == "DOUBLE[]"

        loaded = fetch(duck, "fvec", meta)
        np.testing.assert_array_almost_equal(loaded["force"], original["force"])
        np.testing.assert_array_almost_equal(loaded["velocity"], original["velocity"])

    def test_int_array_dict(self, duck):
        """Dict with int arrays → BIGINT[]."""
        original = {
            "ids": np.array([10, 20, 30], dtype=np.int64),
            "counts": np.array([1, 2, 3], dtype=np.int64),
        }
        meta = store(duck, "ivec", original)

        types = column_types(duck, "ivec")
        assert types["ids"] == "BIGINT[]"
        assert types["counts"] == "BIGINT[]"

        loaded = fetch(duck, "ivec", meta)
        np.testing.assert_array_equal(loaded["ids"], original["ids"])
        np.testing.assert_array_equal(loaded["counts"], original["counts"])

    def test_string_list_dict(self, duck):
        """Dict with list-of-strings → VARCHAR[]."""
        original = {
            "tags": ["alpha", "beta", "gamma"],
            "labels": ["x", "y"],
        }
        meta = store(duck, "strvec", original)

        types = column_types(duck, "strvec")
        assert types["tags"] == "VARCHAR[]"
        assert types["labels"] == "VARCHAR[]"

        loaded = fetch(duck, "strvec", meta)
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
        meta = store(duck, "mixed", original)

        types = column_types(duck, "mixed")
        assert types["name"] == "VARCHAR"
        assert types["weight"] == "DOUBLE"
        assert types["scores"] == "DOUBLE[]"
        assert types["trial_ids"] == "BIGINT[]"

        loaded = fetch(duck, "mixed", meta)
        assert loaded["name"] == "experiment_1"
        assert loaded["weight"] == pytest.approx(0.75)
        np.testing.assert_array_almost_equal(loaded["scores"], original["scores"])
        np.testing.assert_array_equal(loaded["trial_ids"], original["trial_ids"])

    def test_bool_scalar_dict(self, duck):
        """Dict with bool scalar → BOOLEAN."""
        original = {"flag": True, "enabled": False}
        meta = store(duck, "bools", original)

        types = column_types(duck, "bools")
        assert types["flag"] == "BOOLEAN"
        assert types["enabled"] == "BOOLEAN"

        loaded = fetch(duck, "bools", meta)
        assert loaded["flag"] is True
        assert loaded["enabled"] is False

    def test_2d_array_dict(self, duck):
        """Dict with 2D arrays → DOUBLE[][]."""
        original = {
            "matrix": np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]),
        }
        meta = store(duck, "mat", original)

        types = column_types(duck, "mat")
        assert types["matrix"] == "DOUBLE[][]"

        loaded = fetch(duck, "mat", meta)
        np.testing.assert_array_almost_equal(loaded["matrix"], original["matrix"])

    def test_equal_length_arrays_stored_as_one_row(self, duck):
        """Dict-of-equal-length-arrays must be ONE record, not N rows."""
        original = {
            "force": np.arange(10, dtype=np.float64),
            "velocity": np.arange(10, 20, dtype=np.float64),
        }
        meta = store(duck, "vectors", original)

        # Must be array columns (DOUBLE[]), not scalar DOUBLE
        types = column_types(duck, "vectors")
        assert types["force"] == "DOUBLE[]"
        assert types["velocity"] == "DOUBLE[]"

        # Must be exactly one row in the table
        count = duck.fetchall('SELECT COUNT(*) FROM "vectors"')[0][0]
        assert count == 1

        loaded = fetch(duck, "vectors", meta)
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
        meta = store(duck, "df_float", original)

        types = column_types(duck, "df_float")
        assert types["force"] == "DOUBLE"
        assert types["velocity"] == "DOUBLE"

        loaded = fetch(duck, "df_float", meta)
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
        meta = store(duck, "df_int", original)

        types = column_types(duck, "df_int")
        assert types["ids"] == "BIGINT"
        assert types["counts"] == "BIGINT"

        loaded = fetch(duck, "df_int", meta)
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
        meta = store(duck, "df_str", original)

        types = column_types(duck, "df_str")
        assert types["name"] == "VARCHAR"
        assert types["group"] == "VARCHAR"

        loaded = fetch(duck, "df_str", meta)
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
        meta = store(duck, "df_mixed", original)

        types = column_types(duck, "df_mixed")
        assert types["score"] == "DOUBLE"
        assert types["rank"] == "BIGINT"
        assert types["name"] == "VARCHAR"

        loaded = fetch(duck, "df_mixed", meta)
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
        meta = store(duck, "df_1row", original)

        types = column_types(duck, "df_1row")
        assert types["x"] == "DOUBLE"
        assert types["label"] == "VARCHAR"

        loaded = fetch(duck, "df_1row", meta)
        assert isinstance(loaded, pd.DataFrame)
        assert len(loaded) == 1



# ---------------------------------------------------------------------------
# Groups
# ---------------------------------------------------------------------------


class TestGroups:
    """Test variable grouping functionality."""

    def test_add_to_group(self, duck):
        """Should add variables to a group."""
        store(duck, "var_a", 1)
        store(duck, "var_b", 2)

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
        store(duck, "alpha", 1)
        store(duck, "beta", 2)

        var_list = duck.list_variables()
        assert len(var_list) == 2
        names = list(var_list["variable_name"])
        assert "alpha" in names
        assert "beta" in names



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
    as well, for the remaining life of the process. `execute` et al. now
    issue a best-effort ROLLBACK after any autocommit failure so a caught
    exception can't cascade like this.
    """

    def test_execute_failure_does_not_poison_next_statement(self, duck):
        with pytest.raises(Exception):
            duck.execute("SELECT * FROM this_table_does_not_exist")
        # Must succeed -- would raise TransactionException before the fix.
        duck.execute("CREATE TABLE IF NOT EXISTS _recovery_probe (x INTEGER)")

    def test_repeated_failures_do_not_poison_connection(self, duck):
        for _ in range(5):
            with pytest.raises(Exception):
                duck.execute("ALTER TABLE this_table_does_not_exist ADD COLUMN y INTEGER")
        duck.execute("CREATE TABLE IF NOT EXISTS _recovery_probe (x INTEGER)")
        assert duck.fetchall("SELECT COUNT(*) FROM _recovery_probe") == [(0,)]

    def test_fetchall_failure_does_not_poison_next_statement(self, duck):
        with pytest.raises(Exception):
            duck.fetchall("SELECT * FROM this_table_does_not_exist")
        assert duck.fetchall("SELECT 1") == [(1,)]

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
            duck.execute("SELECT * FROM this_table_does_not_exist")
        assert duck._tx_owner == thread
        duck._rollback()
        assert duck._tx_owner is None
        duck.execute("SELECT 1")  # connection still usable afterward


# ---------------------------------------------------------------------------
# Dtype Preservation
# ---------------------------------------------------------------------------


class TestDtypePreservation:
    """Test that numpy dtypes are preserved through round-trip."""

    @pytest.mark.parametrize("dtype", [np.int32, np.int64, np.float32, np.float64])
    def test_1d_array_dtype(self, duck, dtype):
        """1D array dtype should be preserved."""
        original = np.array([1, 2, 3], dtype=dtype)
        meta = store(duck, f"typed_{dtype}", original)
        loaded = fetch(duck, f"typed_{dtype}", meta)
        assert np.asarray(loaded).dtype == dtype

    def test_2d_array_shape_preserved(self, duck):
        """2D array shape should be preserved."""
        original = np.array([[1, 2], [3, 4], [5, 6]], dtype=np.float64)
        meta = store(duck, "matrix", original)
        loaded = fetch(duck, "matrix", meta)
        assert np.asarray(loaded).shape == (3, 2)



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
        meta = store(db1, "persisted", 42)
        db1.close()

        db2 = SciDuck(db_path, dataset_schema=schema)
        loaded = fetch(db2, "persisted", meta)
        db2.close()

        assert loaded == 42

    def test_variable_survives_reconnect(self, tmp_path):
        """Variable metadata should survive reconnection."""
        db_path = tmp_path / "persist_ver.duckdb"
        schema = ["subject"]

        db1 = SciDuck(db_path, dataset_schema=schema)
        store(db1, "registered", 10)
        db1.close()

        db2 = SciDuck(db_path, dataset_schema=schema)
        names = list(db2.list_variables()["variable_name"])
        db2.close()

        assert names == ["registered"]


# ---------------------------------------------------------------------------
# Direct Query
# ---------------------------------------------------------------------------


class TestDirectQuery:
    """Test the query() method for arbitrary SQL."""

    def test_query_returns_dataframe(self, duck):
        """query() should return a pandas DataFrame."""
        store(duck, "qvar", 42)
        result = duck.query('SELECT * FROM "qvar"')
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 1

# ---------------------------------------------------------------------------
# Schema ids (non-contiguous keys)
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


def _speed_combo(subject, speed):
    keys = {"subject": subject, "intervention": "RMT30", "speed": speed}
    return ("speed", (subject, "RMT30", speed)), keys


class TestSchemaIds:
    """batch_get_or_create_schema_ids: the one owner of _schema rows."""

    def test_schema_row_has_nulls_for_skipped_keys(self, wide_duck):
        """Skipped schema keys should be NULL in _schema table."""
        wide_duck.batch_get_or_create_schema_ids(dict([_speed_combo("S01", "SSV")]))
        row = wide_duck.query(
            "SELECT * FROM _schema WHERE schema_level = ?",
            params=["speed"],
        )
        assert row["timepoint"].iloc[0] is None
        assert row["trial"].iloc[0] is None
        assert row["subject"].iloc[0] == "S01"
        assert row["intervention"].iloc[0] == "RMT30"
        assert row["speed"].iloc[0] == "SSV"

    def test_distinct_non_contiguous_locations_get_distinct_ids(self, wide_duck):
        ids = wide_duck.batch_get_or_create_schema_ids(
            dict([_speed_combo("S01", "SSV"), _speed_combo("S01", "FV")])
        )
        assert len(set(ids.values())) == 2
        schema_rows = wide_duck.query(
            "SELECT * FROM _schema WHERE schema_level = 'speed'"
        )
        assert set(schema_rows["speed"]) == {"SSV", "FV"}

    def test_second_batch_reuses_existing_and_allocates_new_without_collision(
        self, wide_duck
    ):
        """Regression: ids are allocated from MAX(schema_id)+1, so a later
        batch must reuse existing rows and never collide with them."""
        first = wide_duck.batch_get_or_create_schema_ids(
            dict([_speed_combo("S01", "SSV"), _speed_combo("S02", "SSV")])
        )
        second = wide_duck.batch_get_or_create_schema_ids(
            dict([_speed_combo("S01", "SSV"), _speed_combo("S02", "FV")])
        )
        assert second[("speed", ("S01", "RMT30", "SSV"))] == first[
            ("speed", ("S01", "RMT30", "SSV"))
        ]
        assert second[("speed", ("S02", "RMT30", "FV"))] not in first.values()

    def test_batch_then_single_schema_id_no_collision(self, wide_duck):
        """Regression: both paths allocate from MAX(schema_id)+1. The single
        path formerly used a DuckDB sequence, which could fall out of sync and
        hit a duplicate primary key on _schema."""
        batch_ids = wide_duck.batch_get_or_create_schema_ids(
            dict([_speed_combo("S01", "SSV"), _speed_combo("S01", "FV")])
        )
        _, new_keys = _speed_combo("S02", "FV")
        new_id = wide_duck.get_or_create_schema_id("speed", new_keys)
        assert new_id not in batch_ids.values()
        key, existing_keys = _speed_combo("S01", "SSV")
        existing_id = wide_duck.get_or_create_schema_id("speed", existing_keys)
        assert existing_id == batch_ids[key]

    def test_single_then_batch_schema_id_no_collision(self, wide_duck):
        key, keys = _speed_combo("S01", "SSV")
        id1 = wide_duck.get_or_create_schema_id("speed", keys)
        batch_ids = wide_duck.batch_get_or_create_schema_ids(
            dict([_speed_combo("S01", "SSV"), _speed_combo("S01", "FV")])
        )
        assert batch_ids[key] == id1
        assert batch_ids[("speed", ("S01", "RMT30", "FV"))] != id1


def test_is_open_follows_close_and_reopen(tmp_path):
    """SciDuck owns its connection state: a best-effort reader (scidb's
    display-only alias validation) asks is_open instead of querying a closed
    connection, which fetchdf would log as an ERROR."""
    from sciduckdb import SciDuck

    db = SciDuck(str(tmp_path / "open.duckdb"), dataset_schema=["subject"])
    assert db.is_open
    db.close()
    assert not db.is_open
    db.reopen()
    assert db.is_open
    db.close()


def test_list_variables_if_open_returns_none_when_closed_without_error(
    tmp_path, caplog
):
    """The best-effort read checks and queries as one step: closed -> None,
    and no ERROR traceback (scistack.log 2026-10-19 15:08:27)."""
    import logging

    from sciduckdb import SciDuck

    db = SciDuck(str(tmp_path / "ifopen.duckdb"), dataset_schema=["subject"])
    assert db.list_variables_if_open() is not None
    db.close()
    caplog.set_level(logging.DEBUG)
    assert db.list_variables_if_open() is None
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    db.reopen()
    assert list(db.list_variables_if_open().columns) == list(
        db.list_variables().columns
    )
    db.close()


def test_close_waits_for_an_in_flight_statement(tmp_path):
    """close() takes the statement lock, so it cannot land between another
    thread's open check and its query. Before, two overlapping GUI requests
    on one refcounted connection closed it 4 ms after is_open said True
    (scistack.log 2026-10-19 15:08:27, "Connection already closed!")."""
    from sciduckdb import SciDuck

    db = SciDuck(str(tmp_path / "race.duckdb"), dataset_schema=["subject"])
    closer = threading.Thread(target=db.close)
    with db._lock:  # stands in for a statement mid-flight on another thread
        closer.start()
        closer.join(timeout=0.2)
        assert closer.is_alive(), "close() must wait for the in-flight statement"
        assert db.is_open
        # The statement that already passed the open check still runs.
        assert db.con.execute("SELECT 1").fetchone() == (1,)
    closer.join(timeout=5)
    assert not closer.is_alive()
    assert not db.is_open


_STATEMENT_CALLS = {
    "execute": lambda db: db.execute("SELECT 1"),
    "fetch_table": lambda db: db.fetch_table("SELECT 1"),
    "fetchall": lambda db: db.fetchall("SELECT 1"),
    "fetchone": lambda db: db.fetchone("SELECT 1"),
    "fetchdf": lambda db: db.fetchdf("SELECT 1"),
    "_fetchall_with_frame": lambda db: db._fetchall_with_frame(
        "SELECT * FROM _query_frame", pd.DataFrame({"a": [1]})
    ),
    "_fetchdf_with_frame": lambda db: db._fetchdf_with_frame(
        "SELECT * FROM _query_frame", pd.DataFrame({"a": [1]})
    ),
}


@pytest.mark.parametrize("op", sorted(_STATEMENT_CALLS))
def test_routine_statement_is_not_logged_but_contention_is(tmp_path, caplog, op):
    """A plain uncontended statement emits no `<op> thread=` line (these were
    ~99% of scistack-debug.log on 2026-10-27); a statement interleaved into
    another thread's open transaction still does."""
    import logging

    db = SciDuck(str(tmp_path / "quiet.duckdb"), dataset_schema=["subject"])
    caplog.set_level(logging.DEBUG, logger="sciduck")
    caplog.clear()
    _STATEMENT_CALLS[op](db)
    assert not [r for r in caplog.records if f"{op} thread=" in r.getMessage()]

    db._tx_owner = threading.get_ident() + 1  # pretend another thread owns a tx
    try:
        _STATEMENT_CALLS[op](db)
    finally:
        db._tx_owner = None
    assert [
        r for r in caplog.records
        if r.getMessage().startswith(f"{op} thread=")
        and "foreign_tx=True" in r.getMessage()
    ]
    db.close()
