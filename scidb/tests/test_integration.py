"""Integration tests for scidb - full end-to-end workflows."""

import json
import numpy as np
import pandas as pd
import pytest

from scidb import (
    BaseVariable,
    NotFoundError,
    configure_database,
)

from conftest import DEFAULT_TEST_SCHEMA_KEYS

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
        original = pd.DataFrame({
            "time": [0.0, 0.1, 0.2, 0.3],
            "x": [1.0, 2.0, 3.0, 4.0],
            "y": [5.0, 6.0, 7.0, 8.0],
        })
        dataframe_class.save(original, subject=1, trial=1)

        loaded = dataframe_class.load(subject=1, trial=1)
        pd.testing.assert_frame_equal(loaded.data, original)

    def test_preserve_column_types(self, db, dataframe_class):
        """Test that column types are preserved."""
        original = pd.DataFrame({
            "int_col": pd.array([1, 2, 3], dtype="int64"),
            "float_col": pd.array([1.1, 2.2, 3.3], dtype="float64"),
            "str_col": ["a", "b", "c"],
        })
        dataframe_class.save(original, subject=1)

        loaded = dataframe_class.load(subject=1)
        for col in original.columns:
            assert loaded.data[col].dtype == original[col].dtype

    def test_10x5_dataframe_roundtrip(self, db, dataframe_class):
        """A 10x5 DataFrame should roundtrip without to_db/from_db."""
        original = pd.DataFrame({
            "a": np.arange(10, dtype="float64"),
            "b": np.arange(10, 20, dtype="float64"),
            "c": np.arange(20, 30, dtype="int64"),
            "d": [f"s{i}" for i in range(10)],
            "e": np.linspace(0, 1, 10),
        })
        dataframe_class.save(original, subject=1, trial=1)

        loaded = dataframe_class.load(subject=1, trial=1)
        pd.testing.assert_frame_equal(loaded.data, original)


class TestEndToEndCustomDataFrameWorkflow:
    """Test complete workflow with pandas DataFrames using custom to_db/from_db."""

    def test_save_and_load_custom_dataframe(self, db, custom_dataframe_class):
        """Save and load a DataFrame with explicit to_db/from_db."""
        original = pd.DataFrame({
            "time": [0.0, 0.1, 0.2, 0.3],
            "x": [1.0, 2.0, 3.0, 4.0],
            "y": [5.0, 6.0, 7.0, 8.0],
        })
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
            "SELECT DISTINCT record_id FROM _record_metadata WHERE variable_name = 'ScalarValue'"
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

    def test_multiple_types_persist(
        self, tmp_path, scalar_class, array_class
    ):
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
                import json
                return pd.DataFrame({"config_json": [json.dumps(self.data)]})

            @classmethod
            def from_db(cls, df: pd.DataFrame) -> dict:
                import json
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
    """Test that load() returns a list when partial schema keys match multiple rows."""

    def test_load_returns_list_for_partial_keys(self, db, scalar_class):
        """Saving at trial level, then loading by subject only should return a list."""
        scalar_class.save(10, subject=1, trial=1)
        scalar_class.save(20, subject=1, trial=2)
        scalar_class.save(30, subject=1, trial=3)

        result = scalar_class.load(subject=1)
        assert isinstance(result, list)
        assert len(result) == 3
        # Results are ordered by created_at DESC (newest first)
        assert {v.data for v in result} == {10, 20, 30}

    def test_load_returns_single_when_one_match(self, db, scalar_class):
        """If only one row matches, load() should return a single variable."""
        scalar_class.save(42, subject=1, trial=1)

        loaded = scalar_class.load(subject=1, trial=1)
        assert not isinstance(loaded, list)
        assert loaded.data == 42

    def test_load_returns_single_for_subject_level_save(self, db, scalar_class):
        """Saving at subject level, loading by subject should return single."""
        scalar_class.save(99, subject=1)

        loaded = scalar_class.load(subject=1)
        assert not isinstance(loaded, list)
        assert loaded.data == 99

    def test_load_partial_key_not_found_raises(self, db, scalar_class):
        """Partial key load with no matches should raise NotFoundError."""
        with pytest.raises(NotFoundError):
            scalar_class.load(subject=999)

    def test_load_by_version_always_returns_single(self, db, scalar_class):
        """Loading by version/record_id should always return single."""
        record_id = scalar_class.save(42, subject=1, trial=1)

        loaded = scalar_class.load(version=record_id)
        assert not isinstance(loaded, list)
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

    def test_view_includes_version_keys(self, db, scalar_class):
        """View should include version_keys from _variables."""
        scalar_class.save(42, subject=1, trial=1, processing="v2")
        df = db._duck._fetchdf('SELECT * FROM "ScalarValue"')
        assert "version_keys" in df.columns
        assert "processing" in df.iloc[0]["version_keys"]

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
    """Test the batch loading API with version_id and list-valued keys."""

    # --- version_id parameter ---

    def test_load_all_default_returns_all_versions(self, db, scalar_class):
        """load_all() with default version_id='all' returns every version."""
        scalar_class.save(100, subject=1, trial=1)
        scalar_class.save(200, subject=1, trial=1)
        scalar_class.save(300, subject=1, trial=1)

        results = list(scalar_class.load_all(subject=1, trial=1))
        assert len(results) == 3
        assert {r.data for r in results} == {100, 200, 300}

    def test_load_all_version_id_latest(self, db, scalar_class):
        """load_all(version_id='latest') returns only latest per parameter set."""
        scalar_class.save(100, subject=1, trial=1)
        scalar_class.save(200, subject=1, trial=1)
        scalar_class.save(300, subject=1, trial=1)

        results = list(scalar_class.load_all(subject=1, trial=1, version_id="latest"))
        assert len(results) == 1
        assert results[0].data == 300

    # --- List-valued schema keys ---

    def test_load_all_list_schema_key(self, db, scalar_class):
        """load_all(subject=[1, 2]) matches subject 1 OR 2."""
        scalar_class.save(10, subject=1, trial=1)
        scalar_class.save(20, subject=2, trial=1)
        scalar_class.save(30, subject=3, trial=1)

        results = list(scalar_class.load_all(subject=[1, 2], trial=1))
        assert len(results) == 2
        assert {r.data for r in results} == {10, 20}

    def test_load_all_multiple_list_schema_keys(self, db, scalar_class):
        """load_all(subject=[1, 2], trial=[1, 2]) returns cartesian product."""
        for s in [1, 2, 3]:
            for t in [1, 2, 3]:
                scalar_class.save(s * 10 + t, subject=s, trial=t)

        results = list(scalar_class.load_all(subject=[1, 2], trial=[1, 2]))
        assert len(results) == 4
        assert {r.data for r in results} == {11, 12, 21, 22}

    # --- List-valued version keys ---

    def test_load_all_list_version_key(self, db, scalar_class):
        """load_all(algorithm=['v1', 'v2']) matches version key in list."""
        scalar_class.save(10, subject=1, trial=1, algorithm="v1")
        scalar_class.save(20, subject=1, trial=1, algorithm="v2")
        scalar_class.save(30, subject=1, trial=1, algorithm="v3")

        results = list(scalar_class.load_all(subject=1, trial=1, algorithm=["v1", "v2"]))
        assert len(results) == 2
        assert {r.data for r in results} == {10, 20}

    # --- Cartesian product: list schema × list version × version_id ---

    def test_cartesian_product(self, db, scalar_class):
        """Full cartesian product: list schema keys × list version keys × version_id."""
        # Save data: 2 subjects × 2 algorithms × 2 versions each
        for s in [1, 2]:
            for algo in ["v1", "v2"]:
                scalar_class.save(s * 100 + hash(algo) % 10, subject=s, trial=1, algorithm=algo)
                scalar_class.save(s * 100 + hash(algo) % 10 + 1, subject=s, trial=1, algorithm=algo)

        # Query all versions for subjects [1,2] with algorithms ["v1","v2"]
        results = list(scalar_class.load_all(
            subject=[1, 2], trial=1, algorithm=["v1", "v2"], version_id="all"
        ))
        # 2 subjects × 2 algorithms × 2 versions = 8
        assert len(results) == 8

    def test_cartesian_product_with_latest(self, db, scalar_class):
        """Cartesian product with version_id='latest' returns one per param set."""
        for s in [1, 2]:
            for algo in ["v1", "v2"]:
                scalar_class.save(s * 100, subject=s, trial=1, algorithm=algo)
                scalar_class.save(s * 200, subject=s, trial=1, algorithm=algo)

        results = list(scalar_class.load_all(
            subject=[1, 2], trial=1, algorithm=["v1", "v2"], version_id="latest"
        ))
        # 2 subjects × 2 algorithms × 1 (latest) = 4
        assert len(results) == 4

    # --- load() treats lists as scalar literals ---

    def test_load_treats_list_as_literal(self, db):
        """load(threshold=[0.3, 0.5]) should match the literal list value."""
        class ListParam(BaseVariable):
            schema_version = 1

        ListParam.save([0.3, 0.5], subject=1, trial=1)

        loaded = ListParam.load(subject=1, trial=1)
        assert loaded.data == [0.3, 0.5]

    # --- load_all with as_df=True ---

    def test_load_all_as_df_with_version_id(self, db, scalar_class):
        """load_all(as_df=True, version_id='latest') returns DataFrame."""
        scalar_class.save(100, subject=1, trial=1)
        scalar_class.save(200, subject=1, trial=1)

        df = scalar_class.load_all(subject=1, trial=1, as_df=True, version_id="latest")
        assert len(df) == 1
        assert df.iloc[0]["data"] == 200


class TestLoadAsTable:
    """Test load(as_table=True) returning a DataFrame."""

    def test_load_as_table_multi_result(self, db, scalar_class):
        """load(as_table=True) with multiple matches returns DataFrame."""
        scalar_class.save(10, subject=1, trial=1)
        scalar_class.save(20, subject=1, trial=2)
        scalar_class.save(30, subject=1, trial=3)

        df = scalar_class.load(as_table=True, subject=1)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        # Check columns: schema keys + data column
        assert "subject" in df.columns
        assert "trial" in df.columns
        assert "ScalarValue" in df.columns
        # Check data values
        assert set(df["ScalarValue"].tolist()) == {10, 20, 30}

    def test_load_as_table_single_result(self, db, scalar_class):
        """load(as_table=True) with single match returns BaseVariable, not DataFrame."""
        scalar_class.save(42, subject=1, trial=1)

        result = scalar_class.load(as_table=True, subject=1, trial=1)
        assert not isinstance(result, pd.DataFrame)
        assert result.data == 42

    def test_load_as_table_with_version_keys(self, db, scalar_class):
        """load(as_table=True) includes parameter/version key columns."""
        scalar_class.save(10, subject=1, trial=1, smoothing=0.2)
        scalar_class.save(20, subject=1, trial=2, smoothing=0.2)

        df = scalar_class.load(as_table=True, subject=1, smoothing=0.2)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "smoothing" in df.columns

    def test_load_as_table_array_data(self, db, array_class):
        """load(as_table=True) with array data stores arrays in data column."""
        array_class.save(np.array([1.0, 2.0]), subject=1, trial=1)
        array_class.save(np.array([3.0, 4.0]), subject=1, trial=2)

        df = array_class.load(as_table=True, subject=1)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "ArrayValue" in df.columns
        # Each entry should be an array
        for val in df["ArrayValue"]:
            assert isinstance(val, np.ndarray)

    def test_load_as_table_false_returns_list(self, db, scalar_class):
        """load(as_table=False) with multiple matches returns list (default behavior)."""
        scalar_class.save(10, subject=1, trial=1)
        scalar_class.save(20, subject=1, trial=2)

        result = scalar_class.load(as_table=False, subject=1)
        assert isinstance(result, list)
        assert len(result) == 2



class TestLineageFcnPipelineMetadata:
    """@lineage_fcn saves write __fn/__inputs/__constants to _record_metadata."""

    def test_single_constant_fn_writes_version_keys(self, db):
        """A function with only constant inputs writes __fn and __constants."""
        from scilineage import lineage_fcn

        class RawSignal(BaseVariable):
            pass

        @lineage_fcn
        def load_signal(source: str):
            return np.array([1.0, 2.0, 3.0])

        result = load_signal("/data/signal.csv")
        RawSignal.save(result, subject=1)

        variants = db.list_pipeline_variants()
        assert len(variants) == 1
        v = variants[0]
        assert v["function_name"] == "load_signal"
        assert v["output_type"] == "RawSignal"
        assert v["constants"]["source"] == "/data/signal.csv"
        assert v["input_types"] == {}

    def test_chained_fn_writes_input_types(self, db):
        """Downstream save picks up _scidb_variable_type from upstream result."""
        from scilineage import lineage_fcn

        class RawData(BaseVariable):
            pass

        class ProcessedData(BaseVariable):
            pass

        @lineage_fcn
        def load_raw(path: str):
            return np.array([1.0, 2.0])

        @lineage_fcn
        def process(data, scale: float):
            return data * scale

        raw_result = load_raw("/data/raw.csv")
        RawData.save(raw_result, subject=1)

        proc_result = process(raw_result, 2.0)
        ProcessedData.save(proc_result, subject=1)

        variants = db.list_pipeline_variants()
        fn_names = {v["function_name"] for v in variants}
        assert fn_names == {"load_raw", "process"}

        proc_v = next(v for v in variants if v["function_name"] == "process")
        assert proc_v["input_types"] == {"data": "RawData"}
        assert proc_v["constants"]["scale"] == "2.0"

    def test_loaded_variable_as_input_populates_input_types(self, db):
        """A BaseVariable loaded from DB and passed to @lineage_fcn is typed correctly."""
        from scilineage import lineage_fcn

        class Raw(BaseVariable):
            pass

        class Processed(BaseVariable):
            pass

        @lineage_fcn
        def load_raw(path: str):
            return np.array([1.0, 2.0])

        @lineage_fcn
        def process(data, scale: float):
            return data * scale

        # Save raw via lineage_fcn (or plain save — doesn't matter)
        Raw.save(np.array([1.0, 2.0]), subject=1)

        # Load it back as a BaseVariable and pass to a downstream function
        loaded = Raw.load(subject=1)
        proc_result = process(loaded, 2.0)
        Processed.save(proc_result, subject=1)

        variants = db.list_pipeline_variants()
        proc_v = next(v for v in variants if v["function_name"] == "process")
        # Type name must be populated even though we used a loaded variable, not a
        # tagged LineageFcnResult
        assert proc_v["input_types"] == {"data": "Raw"}

    def test_list_pipeline_variants_sees_lineage_fcn(self, db):
        """list_pipeline_variants returns entries for @lineage_fcn-saved variables."""
        from scilineage import lineage_fcn

        class Signal(BaseVariable):
            pass

        @lineage_fcn
        def generate(amplitude: float):
            return np.ones(10) * amplitude

        result = generate(3.5)
        Signal.save(result, subject=1)

        variants = db.list_pipeline_variants()
        assert len(variants) == 1
        assert variants[0]["function_name"] == "generate"
        assert variants[0]["record_count"] == 1


class TestSaveBatchSingleColumn:
    """Regression tests for save_batch with single-column (bare) data values.

    The PyArrow fast path indexes data_val[col], which only works when
    data_val is a dict (multi_column mode).  Bare ndarrays/scalars must
    fall through to the generic _value_to_storage_row path.
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

        assert scalar_class.load(subject=1, trial=1).data == 10.0
        assert scalar_class.load(subject=1, trial=2).data == 20.0
