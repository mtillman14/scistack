"""Integration tests for scidb - full end-to-end workflows."""

import json

import numpy as np
import pandas as pd
import pytest
from conftest import DEFAULT_TEST_SCHEMA_KEYS

from scidb import (
    BaseVariable,
    NotFoundError,
    NotRegisteredError,
    configure_database,
)


class TestEndToEndScalarWorkflow:
    """Test complete workflow with scalar values."""

    def test_save_and_load_single_scalar(self, db, scalar_class):
        """Save a scalar and load it back."""
        # Save
        original_value = 42
        record_id = scalar_class.save(original_value, subject=1, trial=1)

        # Load
        loaded = scalar_class.load(subject=1, trial=1)

        # Verify
        assert loaded.data == original_value
        assert loaded.record_id == record_id
        assert loaded.metadata == {"subject": 1, "trial": 1}

    def test_multiple_subjects_and_trials(self, db, scalar_class):
        """Save and load data for multiple subjects and trials."""
        # Save data for 3 subjects, 2 trials each
        expected_data = {}
        for subject in range(1, 4):
            for trial in range(1, 3):
                value = subject * 10 + trial
                scalar_class.save(value, subject=subject, trial=trial)
                expected_data[(subject, trial)] = value

        # Load and verify each
        for (subject, trial), expected_value in expected_data.items():
            loaded = scalar_class.load(subject=subject, trial=trial)
            assert loaded.data == expected_value

    def test_version_history(self, db, scalar_class):
        """Test that version history is maintained."""
        # Save multiple versions with different data
        record_id1 = scalar_class.save(100, subject=1, trial=1)
        record_id2 = scalar_class.save(200, subject=1, trial=1)
        record_id3 = scalar_class.save(300, subject=1, trial=1)

        # All record_ides should be different
        assert len({record_id1, record_id2, record_id3}) == 3

        # List versions should show all three
        versions = db.list_versions(scalar_class, subject=1, trial=1)
        assert len(versions) == 3

        # load() without version should return latest
        latest = scalar_class.load(subject=1, trial=1)
        assert latest.data == 300

        # Should be able to load specific version
        loaded = scalar_class.load(version=record_id2)
        assert loaded.data == 200


class TestEndToEndArrayWorkflow:
    """Test complete workflow with numpy arrays."""

    def test_save_and_load_1d_array(self, db, array_class):
        """Save and load a 1D array."""
        original = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        array_class.save(original, subject=1, measurement="signal")

        loaded = array_class.load(subject=1, measurement="signal")
        np.testing.assert_array_equal(loaded.data, original)

    def test_save_and_load_2d_array(self, db, matrix_class):
        """Save and load a 2D array."""
        original = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
        matrix_class.save(original, subject=1, type="rotation")

        loaded = matrix_class.load(subject=1, type="rotation")
        np.testing.assert_array_equal(loaded.data, original)

    def test_save_and_load_large_array(self, db, array_class):
        """Save and load a larger array."""
        original = np.random.rand(10000)
        array_class.save(original, subject=1, type="timeseries")

        loaded = array_class.load(subject=1, type="timeseries")
        np.testing.assert_array_almost_equal(loaded.data, original)

    def test_preserve_dtype(self, db, array_class):
        """Test that array dtype is preserved."""
        for dtype in [np.int32, np.int64, np.float32, np.float64]:
            original = np.array([1, 2, 3], dtype=dtype)
            array_class.save(original, subject=1, dtype=str(dtype))

            loaded = array_class.load(subject=1, dtype=str(dtype))
            assert loaded.data.dtype == dtype


class TestEndToEndDataFrameWorkflow:
    """Test complete workflow with pandas DataFrames (native, no to_db/from_db)."""

    def test_save_and_load_dataframe(self, db, dataframe_class):
        """Save and load a DataFrame without to_db/from_db."""
        original = pd.DataFrame(
            {
                "time": [0.0, 0.1, 0.2, 0.3],
                "x": [1.0, 2.0, 3.0, 4.0],
                "y": [5.0, 6.0, 7.0, 8.0],
            }
        )
        dataframe_class.save(original, subject=1, trial=1)

        loaded = dataframe_class.load(subject=1, trial=1)
        pd.testing.assert_frame_equal(loaded.data, original)

    def test_preserve_column_types(self, db, dataframe_class):
        """Test that column types are preserved."""
        original = pd.DataFrame(
            {
                "int_col": pd.array([1, 2, 3], dtype="int64"),
                "float_col": pd.array([1.1, 2.2, 3.3], dtype="float64"),
                "str_col": ["a", "b", "c"],
            }
        )
        dataframe_class.save(original, subject=1)

        loaded = dataframe_class.load(subject=1)
        for col in original.columns:
            assert loaded.data[col].dtype == original[col].dtype

    def test_10x5_dataframe_roundtrip(self, db, dataframe_class):
        """A 10x5 DataFrame should roundtrip without to_db/from_db."""
        original = pd.DataFrame(
            {
                "a": np.arange(10, dtype="float64"),
                "b": np.arange(10, 20, dtype="float64"),
                "c": np.arange(20, 30, dtype="int64"),
                "d": [f"s{i}" for i in range(10)],
                "e": np.linspace(0, 1, 10),
            }
        )
        dataframe_class.save(original, subject=1, trial=1)

        loaded = dataframe_class.load(subject=1, trial=1)
        pd.testing.assert_frame_equal(loaded.data, original)


class TestEndToEndCustomDataFrameWorkflow:
    """Test complete workflow with pandas DataFrames using custom to_db/from_db."""

    def test_save_and_load_custom_dataframe(self, db, custom_dataframe_class):
        """Save and load a DataFrame with explicit to_db/from_db."""
        original = pd.DataFrame(
            {
                "time": [0.0, 0.1, 0.2, 0.3],
                "x": [1.0, 2.0, 3.0, 4.0],
                "y": [5.0, 6.0, 7.0, 8.0],
            }
        )
        custom_dataframe_class.save(original, subject=1, trial=1)

        loaded = custom_dataframe_class.load(subject=1, trial=1)
        pd.testing.assert_frame_equal(loaded.data, original)


class TestIdempotentSaves:
    """Test that saves are idempotent."""

    def test_same_data_same_metadata_same_record_id(self, db, scalar_class):
        """Saving identical data+metadata should return same record_id."""
        record_id1 = scalar_class.save(42, subject=1, trial=1)
        record_id2 = scalar_class.save(42, subject=1, trial=1)
        record_id3 = scalar_class.save(42, subject=1, trial=1)

        assert record_id1 == record_id2 == record_id3

        # Should only have one unique record_id in database
        rows = db._duck._fetchall(
            "SELECT DISTINCT record_id FROM _record WHERE type = 'ScalarValue'"
        )
        assert len(rows) == 1

    def test_same_array_data_same_record_id(self, db, array_class):
        """Saving identical array data should return same record_id."""
        arr1 = np.array([1.0, 2.0, 3.0])
        arr2 = np.array([1.0, 2.0, 3.0])

        record_id1 = array_class.save(arr1, subject=1)
        record_id2 = array_class.save(arr2, subject=1)

        assert record_id1 == record_id2


class TestMultipleVariableTypes:
    """Test working with multiple variable types simultaneously."""

    def test_register_and_use_multiple_types(
        self, db, scalar_class, array_class, matrix_class
    ):
        """Register and use multiple variable types."""
        # Save different types
        scalar_class.save(42, subject=1, type="scalar")
        array_class.save(np.array([1, 2, 3]), subject=1, type="array")
        matrix_class.save(np.eye(3), subject=1, type="matrix")

        # Load each type
        scalar = scalar_class.load(subject=1, type="scalar")
        array = array_class.load(subject=1, type="array")
        matrix = matrix_class.load(subject=1, type="matrix")

        assert scalar.data == 42
        np.testing.assert_array_equal(array.data, [1, 2, 3])
        np.testing.assert_array_equal(matrix.data, np.eye(3))

    def test_same_metadata_different_types(self, db, scalar_class, array_class):
        """Same metadata can be used for different types."""
        # Save with same metadata but different types
        scalar_class.save(42, subject=1, trial=1)
        array_class.save(np.array([1, 2, 3]), subject=1, trial=1)

        # Load each type specifically
        scalar = scalar_class.load(subject=1, trial=1)
        array = array_class.load(subject=1, trial=1)

        assert scalar.data == 42
        np.testing.assert_array_equal(array.data, [1, 2, 3])


class TestDatabasePersistence:
    """Test that data persists across database reconnections."""

    def test_data_persists_after_reconnect(self, tmp_path, scalar_class):
        """Data should persist after closing and reopening database."""
        db_path = tmp_path / "persist_test.duckdb"

        # First connection - save data
        db1 = configure_database(db_path, DEFAULT_TEST_SCHEMA_KEYS)
        record_id = scalar_class.save(42, subject=1, trial=1)
        db1.close()

        # Second connection - load data
        db2 = configure_database(db_path, DEFAULT_TEST_SCHEMA_KEYS)
        loaded = scalar_class.load(subject=1, trial=1)
        db2.close()

        assert loaded.data == 42
        assert loaded.record_id == record_id

    def test_multiple_types_persist(self, tmp_path, scalar_class, array_class):
        """Multiple types should persist after reconnect."""
        db_path = tmp_path / "persist_test.duckdb"

        # First connection
        db1 = configure_database(db_path, DEFAULT_TEST_SCHEMA_KEYS)
        scalar_class.save(42, subject=1)
        array_class.save(np.array([1, 2, 3]), subject=1)
        db1.close()

        # Second connection
        db2 = configure_database(db_path, DEFAULT_TEST_SCHEMA_KEYS)

        scalar = scalar_class.load(subject=1)
        array = array_class.load(subject=1)
        db2.close()

        assert scalar.data == 42
        np.testing.assert_array_equal(array.data, [1, 2, 3])


class TestErrorHandling:
    """Test error handling in various scenarios."""

    def test_load_nonexistent_raises_not_found(self, db, scalar_class):
        """Loading nonexistent data should raise NotFoundError."""
        with pytest.raises(NotFoundError):
            scalar_class.load(subject=999, trial=999)

    def test_load_wrong_type_returns_empty(self, db, scalar_class, array_class):
        """Loading with wrong type should not find data from other type."""
        scalar_class.save(42, subject=1, trial=1)

        # Try to load as array - should not find it
        with pytest.raises(NotFoundError):
            array_class.load(subject=1, trial=1)

    def test_load_type_never_registered_raises_not_registered(self, db):
        """Loading a type that was never saved should raise NotRegisteredError."""

        class NeverSavedType(BaseVariable):
            pass

        with pytest.raises(NotRegisteredError, match="not registered"):
            NeverSavedType.load()

    def test_load_type_never_registered_with_metadata_raises_not_registered(self, db):
        """Loading a never-registered type with metadata should raise NotRegisteredError."""

        class NeverSavedType2(BaseVariable):
            pass

        with pytest.raises(NotRegisteredError, match="not registered"):
            NeverSavedType2.load(subject=1)

    def test_load_type_registered_but_no_records_gives_descriptive_error(self, db):
        """Loading a registered type with no records should say 'registered but has no saved records'."""

        class RegisteredButEmpty(BaseVariable):
            pass

        db.register(RegisteredButEmpty)

        with pytest.raises(NotFoundError, match="registered but has no saved records"):
            RegisteredButEmpty.load()


class TestCustomVariableType:
    """Test creating and using custom variable types."""

    def test_custom_variable_type(self, db):
        """Test a custom variable type with complex serialization."""

        class Point3D(BaseVariable):
            """Represents a 3D point."""

            schema_version = 1

            def to_db(self) -> pd.DataFrame:
                x, y, z = self.data
                return pd.DataFrame({"x": [x], "y": [y], "z": [z]})

            @classmethod
            def from_db(cls, df: pd.DataFrame) -> tuple:
                row = df.iloc[0]
                return (row["x"], row["y"], row["z"])

        # Save a point
        original = (1.0, 2.0, 3.0)
        Point3D.save(original, subject="origin")

        # Load it back
        loaded = Point3D.load(subject="origin")
        assert loaded.data == original

    def test_variable_with_nested_data(self, db):
        """Test a variable type with nested data structure."""

        class Config(BaseVariable):
            """Represents a configuration dict."""

            schema_version = 1

            def to_db(self) -> pd.DataFrame:
                return pd.DataFrame({"config_json": [json.dumps(self.data)]})

            @classmethod
            def from_db(cls, df: pd.DataFrame) -> dict:
                return json.loads(df["config_json"].iloc[0])

        original = {
            "learning_rate": 0.001,
            "layers": [64, 128, 64],
            "activation": "relu",
            "nested": {"a": 1, "b": 2},
        }
        Config.save(original, subject="test")

        loaded = Config.load(subject="test")
        assert loaded.data == original


class TestPartialSchemaKeyLoad:
    """Test load() return type: single object for one match, list for multiple."""

    def test_load_returns_list_for_partial_keys(self, db, scalar_class):
        """Loading by subject only (3 trials saved) returns a list of 3."""
        scalar_class.save(10, subject=1, trial=1)
        scalar_class.save(20, subject=1, trial=2)
        scalar_class.save(30, subject=1, trial=3)

        result = scalar_class.load(subject=1)
        assert len(result) == 3
        assert {v.data for v in result} == {10, 20, 30}

    def test_load_single_match_returns_single_object(self, db, scalar_class):
        """If only one row matches, load() returns a single BaseVariable."""
        scalar_class.save(42, subject=1, trial=1)

        loaded = scalar_class.load(subject=1, trial=1)
        assert loaded.data == 42

    def test_load_subject_level_save_returns_single_object(self, db, scalar_class):
        """Saving at subject level, loading by subject returns a single BaseVariable."""
        scalar_class.save(99, subject=1)

        loaded = scalar_class.load(subject=1)
        assert loaded.data == 99

    def test_load_partial_key_not_found_raises(self, db, scalar_class):
        """Partial key load with no matches should raise NotFoundError."""
        with pytest.raises(NotFoundError):
            scalar_class.load(subject=999)

    def test_load_by_version_returns_single_object(self, db, scalar_class):
        """Loading by version/record_id returns a single BaseVariable."""
        record_id = scalar_class.save(42, subject=1, trial=1)

        loaded = scalar_class.load(version=record_id)
        assert loaded.data == 42


class TestVariableViews:
    """Test that auto-created views join variable data with schema and version info."""

    def test_view_exists_after_first_save(self, db, scalar_class):
        """A view with the class name should be created on first save."""
        scalar_class.save(42, subject=1, trial=1)
        # View should be queryable by the clean class name
        rows = db._duck._fetchall('SELECT * FROM "ScalarValue"')
        assert len(rows) == 1

    def test_view_includes_schema_columns(self, db, scalar_class):
        """View should include expanded schema columns from _schema."""
        scalar_class.save(42, subject=1, trial=1)
        df = db._duck._fetchdf('SELECT * FROM "ScalarValue"')
        assert "subject" in df.columns
        assert "trial" in df.columns
        assert df.iloc[0]["subject"] == "1"
        assert df.iloc[0]["trial"] == "1"

    def test_view_includes_schema_level(self, db, scalar_class):
        """View should include the schema_level from _schema."""
        scalar_class.save(42, subject=1, trial=1)
        df = db._duck._fetchdf('SELECT * FROM "ScalarValue"')
        assert "schema_level" in df.columns
        assert df.iloc[0]["schema_level"] == "trial"

    def test_view_includes_variable_value(self, db, scalar_class):
        """View should still include the actual data value."""
        scalar_class.save(42, subject=1, trial=1)
        df = db._duck._fetchdf('SELECT * FROM "ScalarValue"')
        assert "value" in df.columns
        assert df.iloc[0]["value"] == 42

    def test_view_multiple_rows(self, db, scalar_class):
        """View should show all rows with their respective schema info."""
        scalar_class.save(10, subject=1, trial=1)
        scalar_class.save(20, subject=1, trial=2)
        scalar_class.save(30, subject=2, trial=1)

        df = db._duck._fetchdf('SELECT * FROM "ScalarValue" ORDER BY "value"')
        assert len(df) == 3

        row1 = df[df["value"] == 10].iloc[0]
        assert row1["subject"] == "1"
        assert row1["trial"] == "1"

        row2 = df[df["value"] == 20].iloc[0]
        assert row2["subject"] == "1"
        assert row2["trial"] == "2"

        row3 = df[df["value"] == 30].iloc[0]
        assert row3["subject"] == "2"
        assert row3["trial"] == "1"

    def test_view_subject_level_save(self, db, scalar_class):
        """Saving at subject level should show NULLs for deeper schema keys."""
        scalar_class.save(99, subject=1)
        df = db._duck._fetchdf('SELECT * FROM "ScalarValue"')
        assert df.iloc[0]["subject"] == "1"
        assert df.iloc[0]["schema_level"] == "subject"
        assert df.iloc[0]["trial"] is None or pd.isna(df.iloc[0]["trial"])

    def test_view_dataframe(self, db, dataframe_class):
        """View should work for DataFrame variables."""
        original_df = pd.DataFrame({"x": [1.0, 2.0], "y": [3.0, 4.0]})
        dataframe_class.save(original_df, subject=1, trial=1)

        df = db._duck._fetchdf('SELECT * FROM "DataFrameValue"')
        assert len(df) == 2  # Two rows from the DataFrame
        assert "subject" in df.columns
        assert "schema_level" in df.columns
        assert df.iloc[0]["subject"] == "1"

    def test_view_separate_per_variable_type(self, db, scalar_class, array_class):
        """Each variable type should get its own view."""
        scalar_class.save(42, subject=1, trial=1)
        array_class.save(np.array([1, 2, 3]), subject=1, trial=1)

        scalar_df = db._duck._fetchdf('SELECT * FROM "ScalarValue"')
        array_df = db._duck._fetchdf('SELECT * FROM "ArrayValue"')

        assert len(scalar_df) == 1
        assert len(array_df) == 1
        assert scalar_df.iloc[0]["subject"] == "1"
        assert array_df.iloc[0]["subject"] == "1"


class TestBatchLoadingAPI:
    """Test the batch loading API with version= and list-valued keys."""

    # --- version= parameter ---

    def test_load_version_all_returns_all_versions(self, db, scalar_class):
        """load(version='all') returns every stored version."""
        scalar_class.save(100, subject=1, trial=1)
        scalar_class.save(200, subject=1, trial=1)
        scalar_class.save(300, subject=1, trial=1)

        results = scalar_class.load(version="all", subject=1, trial=1)
        assert len(results) == 3
        assert {r.data for r in results} == {100, 200, 300}

    def test_load_version_latest_returns_one(self, db, scalar_class):
        """load(version='latest') returns the single latest record directly."""
        scalar_class.save(100, subject=1, trial=1)
        scalar_class.save(200, subject=1, trial=1)
        scalar_class.save(300, subject=1, trial=1)

        result = scalar_class.load(version="latest", subject=1, trial=1)
        assert result.data == 300

    # --- List-valued schema keys ---

    def test_load_list_schema_key(self, db, scalar_class):
        """load(subject=[1, 2]) matches subject 1 OR 2."""
        scalar_class.save(10, subject=1, trial=1)
        scalar_class.save(20, subject=2, trial=1)
        scalar_class.save(30, subject=3, trial=1)

        results = scalar_class.load(version="all", subject=[1, 2], trial=1)
        assert len(results) == 2
        assert {r.data for r in results} == {10, 20}

    def test_load_multiple_list_schema_keys(self, db, scalar_class):
        """load(subject=[1, 2], trial=[1, 2]) returns cartesian product."""
        for s in [1, 2, 3]:
            for t in [1, 2, 3]:
                scalar_class.save(s * 10 + t, subject=s, trial=t)

        results = scalar_class.load(version="all", subject=[1, 2], trial=[1, 2])
        assert len(results) == 4
        assert {r.data for r in results} == {11, 12, 21, 22}

    # --- List-valued version keys ---

    def test_load_list_version_key(self, db, scalar_class):
        """load(algorithm=['v1', 'v2']) matches version key in list."""
        scalar_class.save(10, subject=1, trial=1, algorithm="v1")
        scalar_class.save(20, subject=1, trial=1, algorithm="v2")
        scalar_class.save(30, subject=1, trial=1, algorithm="v3")

        results = scalar_class.load(
            version="all", subject=1, trial=1, algorithm=["v1", "v2"]
        )
        assert len(results) == 2
        assert {r.data for r in results} == {10, 20}

    # --- Cartesian product: list schema × list version × version ---

    def test_cartesian_product(self, db, scalar_class):
        """Full cartesian product: list schema keys × list version keys × version='all'."""
        # Save data: 2 subjects × 2 algorithms × 2 versions each
        for s in [1, 2]:
            for algo in ["v1", "v2"]:
                scalar_class.save(
                    s * 100 + hash(algo) % 10, subject=s, trial=1, algorithm=algo
                )
                scalar_class.save(
                    s * 100 + hash(algo) % 10 + 1, subject=s, trial=1, algorithm=algo
                )

        # Query all versions for subjects [1,2] with algorithms ["v1","v2"]
        results = scalar_class.load(
            version="all", subject=[1, 2], trial=1, algorithm=["v1", "v2"]
        )
        # 2 subjects × 2 algorithms × 2 versions = 8
        assert len(results) == 8

    def test_cartesian_product_with_latest(self, db, scalar_class):
        """Cartesian product with version='latest' returns one per param set."""
        for s in [1, 2]:
            for algo in ["v1", "v2"]:
                scalar_class.save(s * 100, subject=s, trial=1, algorithm=algo)
                scalar_class.save(s * 200, subject=s, trial=1, algorithm=algo)

        results = scalar_class.load(
            version="latest", subject=[1, 2], trial=1, algorithm=["v1", "v2"]
        )
        # 2 subjects × 2 algorithms × 1 (latest) = 4
        assert len(results) == 4

    # --- load() with plain metadata ---

    def test_load_returns_single_for_plain_metadata(self, db):
        """load(subject=1, trial=1) returns a single BaseVariable when one record matches."""

        class ListParam(BaseVariable):
            schema_version = 1

        ListParam.save([0.3, 0.5], subject=1, trial=1)

        loaded = ListParam.load(subject=1, trial=1)
        assert loaded.data == [0.3, 0.5]

    # --- load with as_df=True ---

    def test_load_as_df_with_version_latest(self, db, scalar_class):
        """load(as_df=True, version='latest') returns DataFrame with latest record."""
        scalar_class.save(100, subject=1, trial=1)
        scalar_class.save(200, subject=1, trial=1)

        df = scalar_class.load(as_df=True, version="latest", subject=1, trial=1)
        assert len(df) == 1
        assert df.iloc[0]["data"] == 200


class TestLoadAsDataFrame:
    """Test load(as_df=True) returning a DataFrame."""

    def test_load_as_df_multi_result(self, db, scalar_class):
        """load(as_df=True) with multiple matches returns DataFrame."""
        scalar_class.save(10, subject=1, trial=1)
        scalar_class.save(20, subject=1, trial=2)
        scalar_class.save(30, subject=1, trial=3)

        df = scalar_class.load(as_df=True, subject=1)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        # Check columns: schema keys + data column
        assert "subject" in df.columns
        assert "trial" in df.columns
        assert "data" in df.columns
        # Check data values
        assert set(df["data"].tolist()) == {10, 20, 30}

    def test_load_as_df_single_result(self, db, scalar_class):
        """load(as_df=True) with single match returns 1-row DataFrame."""
        scalar_class.save(42, subject=1, trial=1)

        df = scalar_class.load(as_df=True, subject=1, trial=1)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df.iloc[0]["data"] == 42

    def test_load_as_df_with_version_keys(self, db, scalar_class):
        """load(as_df=True) includes version key columns."""
        scalar_class.save(10, subject=1, trial=1, smoothing=0.2)
        scalar_class.save(20, subject=1, trial=2, smoothing=0.2)

        df = scalar_class.load(as_df=True, subject=1, smoothing=0.2)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "smoothing" in df.columns

    def test_load_as_df_array_data(self, db, array_class):
        """load(as_df=True) with array data stores arrays in data column."""
        array_class.save(np.array([1.0, 2.0]), subject=1, trial=1)
        array_class.save(np.array([3.0, 4.0]), subject=1, trial=2)

        df = array_class.load(as_df=True, subject=1)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "data" in df.columns
        # Each entry should be an array
        for val in df["data"]:
            assert isinstance(val, np.ndarray)

    def test_load_default_returns_list_for_multiple(self, db, scalar_class):
        """load() with default as_df=False and multiple matches returns a list."""
        scalar_class.save(10, subject=1, trial=1)
        scalar_class.save(20, subject=1, trial=2)

        result = scalar_class.load(subject=1)
        assert isinstance(result, list)
        assert len(result) == 2


class TestSaveBatchSingleColumn:
    """Regression tests for save_batch with single-column (bare) data values.

    The PyArrow fast path indexes data_val[col], which only works when
    data_val is a dict (multi_column mode).  Bare ndarrays/scalars must
    fall through to the generic value_to_storage_row path.
    """

    def test_save_batch_1d_ndarray_single_column(self, db, array_class):
        """save_batch must handle bare 1-D ndarray values (single_column mode).

        Before the fix, the Arrow fast path tried data_val["value"] on a
        bare numpy array and raised IndexError.
        """
        data_items = [
            (np.array([1.0, 2.0, 3.0]), {"subject": 1, "trial": 1}),
            (np.array([4.0, 5.0, 6.0]), {"subject": 1, "trial": 2}),
        ]
        record_ids = db.save_batch(array_class, data_items)
        assert len(record_ids) == 2

        loaded1 = array_class.load(subject=1, trial=1)
        np.testing.assert_array_equal(loaded1.data, [1.0, 2.0, 3.0])
        loaded2 = array_class.load(subject=1, trial=2)
        np.testing.assert_array_equal(loaded2.data, [4.0, 5.0, 6.0])

    def test_save_batch_scalar_single_column(self, db, scalar_class):
        """save_batch must handle bare scalar values (single_column mode)."""
        data_items = [
            (10.0, {"subject": 1, "trial": 1}),
            (20.0, {"subject": 1, "trial": 2}),
        ]
        record_ids = db.save_batch(scalar_class, data_items)
        assert len(record_ids) == 2

        s1 = scalar_class.load(subject=1, trial=1)
        assert s1.data == 10.0
        s2 = scalar_class.load(subject=1, trial=2)
        assert s2.data == 20.0


class TestSaveBatchSchemaValidation:
    """save_batch must skip (with a warning) records that can't fit the batch's
    storage schema, and persist the rest, instead of aborting the whole atomic
    batch insert.

    Real-world trigger: a MATLAB for_each over EMG files where two files were
    empty (loadDelsysEMGOneFile returned an empty struct -> empty dict). The
    multi_column writer did data_val['RHAM'] for every record; the empty dicts
    raised KeyError('RHAM') and took down all 268 saves.

    Skipped records get a None in the returned record_ids list, aligned to the
    original input order, so callers (e.g. foreach graph_records) can still zip
    items with record_ids.
    """

    def test_skips_empty_dict_record(self, db):
        class EmgValue(BaseVariable):
            schema_version = 1

        good = {"RHAM": np.array([1.0, 2.0]), "RVL": np.array([3.0, 4.0])}
        data_items = [
            (good, {"subject": 1, "trial": 1}),
            ({}, {"subject": 1, "trial": 2}),  # empty -> skip
            (good, {"subject": 1, "trial": 3}),
        ]
        record_ids = db.save_batch(EmgValue, data_items)

        assert len(record_ids) == 3
        assert isinstance(record_ids[0], str)
        assert record_ids[1] is None  # skipped slot
        assert isinstance(record_ids[2], str)

        # Valid records persisted and load back.
        np.testing.assert_array_equal(
            EmgValue.load(subject=1, trial=1).data["RHAM"], [1.0, 2.0]
        )
        np.testing.assert_array_equal(
            EmgValue.load(subject=1, trial=3).data["RVL"], [3.0, 4.0]
        )
        # The empty record was not saved.
        with pytest.raises(NotFoundError):
            EmgValue.load(subject=1, trial=2)

    def test_skips_partial_keys_record(self, db):
        class EmgValue2(BaseVariable):
            schema_version = 1

        # NOTE: use length>=2 arrays. infer_data_columns unwraps length-1
        # arrays to scalars (DOUBLE), which mismatches the DOUBLE[] storage row
        # — a separate pre-existing quirk unrelated to schema validation.
        full = {
            "RHAM": np.array([1.0, 9.0]),
            "RVL": np.array([2.0, 8.0]),
            "LHAM": np.array([3.0, 7.0]),
        }
        partial = {
            "RHAM": np.array([1.0, 9.0]),
            "RVL": np.array([2.0, 8.0]),
        }  # missing LHAM
        data_items = [
            (full, {"subject": 1, "trial": 1}),
            (partial, {"subject": 1, "trial": 2}),  # missing a key -> skip
            (full, {"subject": 1, "trial": 3}),
        ]
        record_ids = db.save_batch(EmgValue2, data_items)

        assert [isinstance(r, str) for r in record_ids] == [True, False, True]
        assert EmgValue2.load(subject=1, trial=1).data.keys() == full.keys()
        with pytest.raises(NotFoundError):
            EmgValue2.load(subject=1, trial=2)

    def test_skips_shape_mismatch_record(self, db):
        """A scalar where the column stores a vector (or vice versa) is skipped."""

        class EmgValue3(BaseVariable):
            schema_version = 1

        vec = {"RHAM": np.array([1.0, 2.0]), "RVL": np.array([3.0, 4.0])}
        # RHAM is a bare scalar here -> DOUBLE vs the batch's DOUBLE[] -> skip.
        scalar = {"RHAM": 1.0, "RVL": np.array([3.0, 4.0])}
        data_items = [
            (vec, {"subject": 1, "trial": 1}),
            (scalar, {"subject": 1, "trial": 2}),  # shape mismatch -> skip
        ]
        record_ids = db.save_batch(EmgValue3, data_items)

        assert isinstance(record_ids[0], str)
        assert record_ids[1] is None
        with pytest.raises(NotFoundError):
            EmgValue3.load(subject=1, trial=2)

    def test_leading_empty_dict_does_not_define_schema(self, db):
        """An empty dict as the FIRST item must not become the table schema;
        the first usable record defines it and the empty one is skipped."""

        class EmgValue4(BaseVariable):
            schema_version = 1

        good = {"RHAM": np.array([1.0, 2.0]), "RVL": np.array([3.0, 4.0])}
        data_items = [
            ({}, {"subject": 1, "trial": 1}),  # leading empty -> skip
            (good, {"subject": 1, "trial": 2}),
            (good, {"subject": 1, "trial": 3}),
        ]
        record_ids = db.save_batch(EmgValue4, data_items)

        assert record_ids[0] is None
        assert isinstance(record_ids[1], str)
        assert isinstance(record_ids[2], str)
        np.testing.assert_array_equal(
            EmgValue4.load(subject=1, trial=2).data["RHAM"], [1.0, 2.0]
        )

    def test_all_records_incompatible_returns_all_none(self, db):
        """If every record is incompatible, nothing saves and all slots None."""

        class EmgValue5(BaseVariable):
            schema_version = 1

        # Both empty -> no usable reference schema at all -> skip everything.
        data_items = [
            ({}, {"subject": 1, "trial": 1}),
            ({}, {"subject": 1, "trial": 2}),
        ]
        record_ids = db.save_batch(EmgValue5, data_items)
        assert record_ids == [None, None]


class TestSaveBatchSingleElementArrayDict:
    """Regression: a multi_column dict whose value is a length-1 ndarray.

    infer_data_columns unwraps length-1 arrays to scalars when choosing the
    column type (-> DOUBLE), so _python_to_storage must unwrap too. Before the
    fix, the row still carried np.array([x]) (DOUBLE[]) into the DOUBLE column
    and DuckDB raised: Conversion Error (DOUBLE[] -> DOUBLE).
    """

    def test_single_element_array_dict_saves_as_scalar(self, db):
        class OneSampleEmg(BaseVariable):
            schema_version = 1

        data_items = [
            (
                {"RHAM": np.array([1.5]), "RVL": np.array([2.5])},
                {"subject": 1, "trial": 1},
            ),
            (
                {"RHAM": np.array([3.5]), "RVL": np.array([4.5])},
                {"subject": 1, "trial": 2},
            ),
        ]
        record_ids = db.save_batch(OneSampleEmg, data_items)
        assert all(isinstance(r, str) for r in record_ids)

        loaded = OneSampleEmg.load(subject=1, trial=1).data
        # Length-1 arrays round-trip as scalars (the column is scalar DOUBLE).
        assert isinstance(loaded["RHAM"], float)
        assert loaded["RHAM"] == pytest.approx(1.5)
        assert loaded["RVL"] == pytest.approx(2.5)

    def test_single_element_int_array_dict_saves_as_scalar(self, db):
        class OneSampleCount(BaseVariable):
            schema_version = 1

        data_items = [
            ({"n": np.array([7])}, {"subject": 1, "trial": 1}),
        ]
        record_ids = db.save_batch(OneSampleCount, data_items)
        assert isinstance(record_ids[0], str)

        loaded = OneSampleCount.load(subject=1, trial=1).data
        assert loaded["n"] == 7


class TestSaveAutoDistribute:
    """save() auto-distributes a DataFrame whose columns include schema keys."""

    def test_single_data_col(self, db, scalar_class):
        df = pd.DataFrame(
            {"subject": [1, 2, 3], "trial": [1, 1, 1], "value": [10.0, 20.0, 30.0]}
        )
        ids = scalar_class.save(df)
        assert len(ids) == 3
        assert scalar_class.load(subject=1, trial=1).data == 10.0
        assert scalar_class.load(subject=3, trial=1).data == 30.0

    def test_single_data_col_with_common_metadata(self, db, scalar_class):
        # Only subject is in the DataFrame; trial comes from kwargs.
        df = pd.DataFrame({"subject": [1, 2], "value": [7.0, 8.0]})
        ids = scalar_class.save(df, trial=99)
        assert len(ids) == 2
        assert scalar_class.load(subject=1, trial=99).data == 7.0
        assert scalar_class.load(subject=2, trial=99).data == 8.0

    def test_multiple_data_cols(self, db, dataframe_class):
        # Two non-schema columns → each row saved as a 1-row DataFrame.
        df = pd.DataFrame(
            {
                "subject": [1, 2],
                "trial": [1, 1],
                "a": [0.1, 0.2],
                "b": [0.3, 0.4],
            }
        )
        ids = dataframe_class.save(df)
        assert len(ids) == 2
        v = dataframe_class.load(subject=1, trial=1)
        assert isinstance(v.data, pd.DataFrame)
        assert list(v.data.columns) == ["a", "b"]
        assert v.data["a"].iloc[0] == pytest.approx(0.1)

    def test_no_schema_key_cols_falls_through(self, db, dataframe_class):
        # DataFrame with no schema-key columns → saved as one record.
        df = pd.DataFrame({"x": [1.0, 2.0], "y": [3.0, 4.0]})
        result = dataframe_class.save(df, subject=1, trial=1)
        assert isinstance(result, str)
        loaded = dataframe_class.load(subject=1, trial=1)
        pd.testing.assert_frame_equal(loaded.data, df)

    def test_idempotent(self, db, scalar_class):
        df = pd.DataFrame({"subject": [1, 2], "trial": [1, 1], "value": [5.0, 6.0]})
        ids1 = scalar_class.save(df)
        ids2 = scalar_class.save(df)
        assert ids1 == ids2
