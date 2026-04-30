"""Tests for the MATLAB-SciStack Python bridge.

These tests verify that the proxy classes satisfy the duck-typing
contracts of scilineage without requiring MATLAB.
"""

import sys
from hashlib import sha256
from pathlib import Path

# Add source paths for the monorepo packages
_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "scilineage" / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "sciduck" / "src"))
sys.path.insert(0, str(_root / "path-gen" / "src"))
sys.path.insert(0, str(_root / "sci-matlab" / "src"))

import numpy as np
import pytest

from sci_matlab.bridge import (
    MatlabLineageFcn,
    MatlabLineageFcnInvocation,
    check_cache,
    make_lineage_fcn_result,
    register_matlab_variable,
    get_surrogate_class,
    for_each_batch_save_dataframe,
    split_flat_to_lists,
)
from scilineage.core import LineageFcnResult, LineageFcn, LineageFcnInvocation
from scilineage.inputs import classify_inputs, classify_input, InputKind
from scilineage.lineage import extract_lineage, LineageRecord
from scidb.variable import BaseVariable


# ---------------------------------------------------------------------------
# MatlabLineageFcn proxy tests
# ---------------------------------------------------------------------------

class TestMatlabLineageFcn:
    """Verify MatlabLineageFcn satisfies the LineageFcn duck-typing contract."""

    def test_has_required_attributes(self):
        t = MatlabLineageFcn("abc123", "my_function")
        assert hasattr(t, "hash")
        assert hasattr(t, "fcn")
        assert hasattr(t, "unpack_output")
        assert hasattr(t, "unwrap")
        assert hasattr(t, "invocations")
        assert t.fcn.__name__ == "my_function"
        assert t.unpack_output is False
        assert t.unwrap is True

    def test_hash_is_deterministic(self):
        t1 = MatlabLineageFcn("abc123", "f")
        t2 = MatlabLineageFcn("abc123", "f")
        assert t1.hash == t2.hash
        assert len(t1.hash) == 64  # Full SHA-256 hex

    def test_hash_changes_with_source(self):
        t1 = MatlabLineageFcn("abc123", "f")
        t2 = MatlabLineageFcn("def456", "f")
        assert t1.hash != t2.hash

    def test_hash_changes_with_unpack_output(self):
        t1 = MatlabLineageFcn("abc123", "f", unpack_output=False)
        t2 = MatlabLineageFcn("abc123", "f", unpack_output=True)
        assert t1.hash != t2.hash

    def test_hash_algorithm_matches_lineage_fcn(self):
        """Verify the hash algorithm matches LineageFcn.__init__ for the same
        source_hash and unpack_output, so the structure is the same."""
        source_hash = sha256(b"test_source").hexdigest()
        t = MatlabLineageFcn(source_hash, "f", unpack_output=False)

        # Same algorithm: sha256(f"{source_hash}-{unpack_output}")
        expected = sha256(f"{source_hash}-False".encode()).hexdigest()
        assert t.hash == expected


# ---------------------------------------------------------------------------
# MatlabLineageFcnInvocation proxy tests
# ---------------------------------------------------------------------------

class TestMatlabLineageFcnInvocation:
    """Verify MatlabLineageFcnInvocation satisfies LineageFcnInvocation duck-typing."""

    def test_has_required_attributes(self):
        mt = MatlabLineageFcn("abc", "f")
        inv = MatlabLineageFcnInvocation(mt, {"arg_0": 42})
        assert hasattr(inv, "fcn")
        assert hasattr(inv, "inputs")
        assert hasattr(inv, "outputs")
        assert hasattr(inv, "unwrap")
        assert hasattr(inv, "hash")
        assert hasattr(inv, "compute_lineage_hash")

    def test_lineage_hash_is_deterministic(self):
        mt = MatlabLineageFcn("abc", "f")
        inv1 = MatlabLineageFcnInvocation(mt, {"arg_0": 42})
        inv2 = MatlabLineageFcnInvocation(mt, {"arg_0": 42})
        assert inv1.hash == inv2.hash

    def test_lineage_hash_changes_with_inputs(self):
        mt = MatlabLineageFcn("abc", "f")
        inv1 = MatlabLineageFcnInvocation(mt, {"arg_0": 42})
        inv2 = MatlabLineageFcnInvocation(mt, {"arg_0": 99})
        assert inv1.hash != inv2.hash

    def test_lineage_hash_changes_with_function(self):
        mt1 = MatlabLineageFcn("abc", "f")
        mt2 = MatlabLineageFcn("def", "g")
        inv1 = MatlabLineageFcnInvocation(mt1, {"arg_0": 42})
        inv2 = MatlabLineageFcnInvocation(mt2, {"arg_0": 42})
        assert inv1.hash != inv2.hash

    def test_classify_inputs_works_with_constant(self):
        """classify_inputs from scilineage works on our proxy's inputs."""
        mt = MatlabLineageFcn("abc", "f")
        inv = MatlabLineageFcnInvocation(mt, {"arg_0": 42, "arg_1": "hello"})
        classified = classify_inputs(inv.inputs)
        assert len(classified) == 2
        assert all(c.kind == InputKind.CONSTANT for c in classified)

    def test_classify_inputs_works_with_base_variable(self):
        """A real BaseVariable is classified correctly as a lineage result input."""
        var = BaseVariable(np.array([1.0, 2.0, 3.0]))
        var.record_id = "abc123def456abcd"
        var.metadata = {"subject": 1}
        var.content_hash = "1234567890abcdef"

        mt = MatlabLineageFcn("abc", "f")
        inv = MatlabLineageFcnInvocation(mt, {"arg_0": var})
        classified = classify_inputs(inv.inputs)
        assert len(classified) == 1
        assert classified[0].kind == InputKind.SAVED_VARIABLE

    def test_classify_inputs_works_with_lineage_fcn_result(self):
        """A real LineageFcnResult (from make_lineage_fcn_result) is classified
        correctly when used as input to another invocation."""
        mt1 = MatlabLineageFcn("abc", "step1")
        inv1 = MatlabLineageFcnInvocation(mt1, {"arg_0": 42})
        result1 = make_lineage_fcn_result(inv1, 0, np.array([1.0]))

        mt2 = MatlabLineageFcn("def", "step2")
        inv2 = MatlabLineageFcnInvocation(mt2, {"arg_0": result1})
        classified = classify_inputs(inv2.inputs)
        assert len(classified) == 1
        assert classified[0].kind == InputKind.LINEAGE_RESULT

    def test_saved_variable_with_lineage_hash_classified_as_lineage_result(self):
        """A BaseVariable with lineage_hash is reclassified as LINEAGE_RESULT,
        matching the behaviour for Python-saved lineage-tracked results."""
        var = BaseVariable(np.array([1.0]))
        var.record_id = "abc123def456abcd"
        var.lineage_hash = "a" * 64
        var.content_hash = "1234567890abcdef"

        mt = MatlabLineageFcn("abc", "f")
        inv = MatlabLineageFcnInvocation(mt, {"arg_0": var})
        classified = classify_inputs(inv.inputs)
        assert len(classified) == 1
        # Reclassified as LINEAGE_RESULT because lineage_hash is set
        assert classified[0].kind == InputKind.LINEAGE_RESULT
        assert classified[0].hash == "a" * 64


# ---------------------------------------------------------------------------
# make_lineage_fcn_result tests
# ---------------------------------------------------------------------------

class TestMakeLineageFcnResult:
    """Verify make_lineage_fcn_result creates real LineageFcnResult instances."""

    def test_returns_real_lineage_fcn_result(self):
        mt = MatlabLineageFcn("abc", "f")
        inv = MatlabLineageFcnInvocation(mt, {"arg_0": 42})
        result = make_lineage_fcn_result(inv, 0, np.array([1.0, 2.0]))
        assert isinstance(result, LineageFcnResult)

    def test_lineage_fcn_result_attributes(self):
        mt = MatlabLineageFcn("abc", "f")
        inv = MatlabLineageFcnInvocation(mt, {"arg_0": 42})
        data = np.array([1.0, 2.0])
        result = make_lineage_fcn_result(inv, 0, data)
        assert result.is_complete is True
        assert result.output_num == 0
        assert np.array_equal(result.data, data)
        assert result.invoked is inv

    def test_lineage_fcn_result_hash_deterministic(self):
        mt = MatlabLineageFcn("abc", "f")
        inv = MatlabLineageFcnInvocation(mt, {"arg_0": 42})
        r1 = make_lineage_fcn_result(inv, 0, np.array([1.0]))
        r2 = make_lineage_fcn_result(inv, 0, np.array([1.0]))
        assert r1.hash == r2.hash

    def test_different_output_nums_different_hashes(self):
        mt = MatlabLineageFcn("abc", "f")
        inv = MatlabLineageFcnInvocation(mt, {"arg_0": 42})
        r0 = make_lineage_fcn_result(inv, 0, np.array([1.0]))
        r1 = make_lineage_fcn_result(inv, 1, np.array([2.0]))
        assert r0.hash != r1.hash


# ---------------------------------------------------------------------------
# extract_lineage compatibility tests
# ---------------------------------------------------------------------------

class TestExtractLineage:
    """Verify that scilineage's extract_lineage works on our proxy objects."""

    def test_extract_lineage_from_matlab_lineage_fcn_result(self):
        mt = MatlabLineageFcn("abc", "my_matlab_func")
        inv = MatlabLineageFcnInvocation(mt, {"arg_0": 42, "arg_1": "hello"})
        result = make_lineage_fcn_result(inv, 0, np.array([1.0]))

        lineage = extract_lineage(result)
        assert isinstance(lineage, LineageRecord)
        assert lineage.function_name == "my_matlab_func"
        assert lineage.function_hash == mt.hash
        assert len(lineage.constants) == 2  # 42 and "hello"
        assert len(lineage.inputs) == 0     # No variable inputs

    def test_extract_lineage_with_variable_input(self):
        var = BaseVariable(np.array([1.0, 2.0]))
        var.record_id = "abc123def456abcd"
        var.metadata = {"subject": 1}
        var.content_hash = "1234567890abcdef"

        mt = MatlabLineageFcn("abc", "process")
        inv = MatlabLineageFcnInvocation(mt, {"arg_0": var, "arg_1": 2.5})
        result = make_lineage_fcn_result(inv, 0, np.array([2.0, 4.0]))

        lineage = extract_lineage(result)
        assert lineage.function_name == "process"
        assert len(lineage.inputs) == 1      # The BaseVariable
        assert len(lineage.constants) == 1   # 2.5

    def test_extract_lineage_chained(self):
        """Two MATLAB functions chained: step1 -> step2."""
        mt1 = MatlabLineageFcn("abc", "step1")
        inv1 = MatlabLineageFcnInvocation(mt1, {"arg_0": 42})
        result1 = make_lineage_fcn_result(inv1, 0, np.array([84.0]))

        mt2 = MatlabLineageFcn("def", "step2")
        inv2 = MatlabLineageFcnInvocation(mt2, {"arg_0": result1})
        result2 = make_lineage_fcn_result(inv2, 0, np.array([80.0]))

        lineage = extract_lineage(result2)
        assert lineage.function_name == "step2"
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0]["source_type"] == "thunk"
        assert lineage.inputs[0]["source_function"] == "step1"


# ---------------------------------------------------------------------------
# Variable registration tests
# ---------------------------------------------------------------------------

class TestVariableRegistration:

    def test_register_creates_subclass(self):
        cls = register_matlab_variable("TestMatlabVar_1")
        assert issubclass(cls, BaseVariable)
        assert cls.__name__ == "TestMatlabVar_1"
        assert cls.schema_version == 1

    def test_register_with_schema_version(self):
        cls = register_matlab_variable("TestMatlabVar_2", schema_version=3)
        assert cls.schema_version == 3

    def test_register_idempotent(self):
        cls1 = register_matlab_variable("TestMatlabVar_3")
        cls2 = register_matlab_variable("TestMatlabVar_3")
        assert cls1 is cls2

    def test_get_surrogate_class(self):
        register_matlab_variable("TestMatlabVar_4")
        cls = get_surrogate_class("TestMatlabVar_4")
        assert cls.__name__ == "TestMatlabVar_4"

    def test_get_surrogate_class_not_registered(self):
        import pytest
        with pytest.raises(ValueError, match="not registered"):
            get_surrogate_class("NonExistentType_xyz")

    def test_registered_in_all_subclasses(self):
        register_matlab_variable("TestMatlabVar_5")
        assert "TestMatlabVar_5" in BaseVariable._all_subclasses


# ---------------------------------------------------------------------------
# Full save_variable compatibility test
# ---------------------------------------------------------------------------

class TestSaveVariableCompatibility:
    """Verify the proxy objects work with DatabaseManager.save_variable."""

    def test_save_variable_with_matlab_lineage_fcn_result(self, tmp_path):
        """End-to-end: create MATLAB proxies, make LineageFcnResult, save it."""
        from scidb.database import configure_database

        db = configure_database(
            tmp_path / "test.duckdb",
            ["subject"],
        )

        try:
            register_matlab_variable("MatlabResult")

            # Simulate a MATLAB lineage-tracked execution
            mt = MatlabLineageFcn("source_hash_abc", "matlab_filter")
            inv = MatlabLineageFcnInvocation(mt, {"arg_0": 42, "arg_1": 3.14})
            result_data = np.array([1.0, 2.0, 3.0])
            result = make_lineage_fcn_result(inv, 0, result_data)

            # save_variable should work with our LineageFcnResult
            var_class = get_surrogate_class("MatlabResult")
            record_id = db.save_variable(var_class, result, subject=1)
            assert record_id is not None
            assert len(record_id) == 16

            # Verify we can load it back
            loaded = db.load(var_class, {"subject": 1})
            assert np.array_equal(loaded.data, result_data)
            assert loaded.lineage_hash is not None

            # Verify lineage was saved
            assert db.has_lineage(record_id)
            prov = db.get_provenance(var_class, subject=1)
            assert prov["function_name"] == "matlab_filter"

        finally:
            db.close()

    def test_cache_hit_with_matlab_lineage_fcn(self, tmp_path):
        """After saving, find_by_lineage should return the cached data."""
        from scidb.database import configure_database

        db = configure_database(
            tmp_path / "test2.duckdb",
            ["subject"],
        )

        try:
            register_matlab_variable("MatlabCached")

            mt = MatlabLineageFcn("source_hash_xyz", "matlab_process")
            inv = MatlabLineageFcnInvocation(mt, {"arg_0": 100})
            result_data = np.array([10.0, 20.0])
            result = make_lineage_fcn_result(inv, 0, result_data)

            var_class = get_surrogate_class("MatlabCached")
            db.save_variable(var_class, result, subject=1)

            # Now check cache with the same computation
            inv2 = MatlabLineageFcnInvocation(mt, {"arg_0": 100})
            cached = db.find_by_lineage(inv2)
            assert cached is not None
            assert len(cached) == 1
            assert np.array_equal(cached[0], result_data)

        finally:
            db.close()


# ---------------------------------------------------------------------------
# for_each_batch_save_dataframe tests
# ---------------------------------------------------------------------------

class TestForEachBatchSaveDataframe:
    """Verify for_each_batch_save_dataframe splits, saves, and loads correctly."""

    def _make_db(self, tmp_path, type_name="BatchDfVar"):
        from scidb.database import configure_database

        db = configure_database(
            tmp_path / "test.duckdb",
            ["subject", "session"],
        )
        register_matlab_variable(type_name)
        cls = get_surrogate_class(type_name)
        return db, cls

    def test_single_row_dataframes(self, tmp_path):
        """3 items, each a 1-row DataFrame — verify round-trip load."""
        import pandas as pd

        db, cls = self._make_db(tmp_path, "BatchDf1")
        try:
            df = pd.DataFrame({"x": [1.0, 2.0, 3.0], "y": [10.0, 20.0, 30.0]})
            row_counts = np.array([1, 1, 1], dtype=np.int64)
            meta_keys = ["subject", "session"]
            meta_columns = [
                np.array([1, 2, 3]),
                "A\x1eB\x1eC",
            ]

            result = for_each_batch_save_dataframe(
                "BatchDf1", df, row_counts, meta_keys, meta_columns, db=db
            )
            rids = result.strip().split("\n")
            assert len(rids) == 3

            # Verify round-trip: load each item
            loaded = db.load(cls, {"subject": 1, "session": "A"})
            assert loaded.data.shape == (1, 2)
            assert float(loaded.data["x"].iloc[0]) == 1.0

            loaded2 = db.load(cls, {"subject": 3, "session": "C"})
            assert float(loaded2.data["y"].iloc[0]) == 30.0
        finally:
            db.close()

    def test_multi_row_dataframes(self, tmp_path):
        """2 items with 3 and 2 rows — verify shapes."""
        import pandas as pd

        db, cls = self._make_db(tmp_path, "BatchDf2")
        try:
            df = pd.DataFrame({"val": [1.0, 2.0, 3.0, 4.0, 5.0]})
            row_counts = np.array([3, 2], dtype=np.int64)
            meta_keys = ["subject"]
            meta_columns = [np.array([1, 2])]

            result = for_each_batch_save_dataframe(
                "BatchDf2", df, row_counts, meta_keys, meta_columns,
                common_metadata={"session": "X"}, db=db,
            )
            rids = result.strip().split("\n")
            assert len(rids) == 2

            loaded1 = db.load(cls, {"subject": 1, "session": "X"})
            assert loaded1.data.shape == (3, 1)
            assert list(loaded1.data["val"]) == [1.0, 2.0, 3.0]

            loaded2 = db.load(cls, {"subject": 2, "session": "X"})
            assert loaded2.data.shape == (2, 1)
            assert list(loaded2.data["val"]) == [4.0, 5.0]
        finally:
            db.close()

    def test_common_metadata_applied(self, tmp_path):
        """config/constant metadata reaches every item."""
        import pandas as pd

        db, cls = self._make_db(tmp_path, "BatchDf3")
        try:
            df = pd.DataFrame({"a": [10.0, 20.0]})
            row_counts = np.array([1, 1], dtype=np.int64)
            meta_keys = ["subject"]
            meta_columns = [np.array([1, 2])]
            common = {"session": "S1", "__fn": "test_func"}

            for_each_batch_save_dataframe(
                "BatchDf3", df, row_counts, meta_keys, meta_columns,
                common_metadata=common, db=db,
            )

            loaded = db.load(cls, {"subject": 1, "session": "S1"})
            assert loaded is not None
            assert loaded.metadata.get("__fn") == "test_func"
        finally:
            db.close()

    def test_numpy_meta_columns(self, tmp_path):
        """numpy arrays as metadata columns."""
        import pandas as pd

        db, cls = self._make_db(tmp_path, "BatchDf4")
        try:
            df = pd.DataFrame({"v": [100.0, 200.0]})
            row_counts = np.array([1, 1], dtype=np.int64)
            meta_keys = ["subject", "session"]
            meta_columns = [
                np.array([5, 6]),
                np.array([10, 20]),
            ]

            result = for_each_batch_save_dataframe(
                "BatchDf4", df, row_counts, meta_keys, meta_columns, db=db,
            )
            rids = result.strip().split("\n")
            assert len(rids) == 2
        finally:
            db.close()

    def test_string_meta_columns_via_record_separator(self, tmp_path):
        """\\x1e-joined strings as metadata columns."""
        import pandas as pd

        db, cls = self._make_db(tmp_path, "BatchDf5")
        try:
            df = pd.DataFrame({"w": [1.0, 2.0, 3.0]})
            row_counts = np.array([1, 1, 1], dtype=np.int64)
            meta_keys = ["subject", "session"]
            meta_columns = [
                "1\x1e2\x1e3",
                "alpha\x1ebeta\x1egamma",
            ]

            result = for_each_batch_save_dataframe(
                "BatchDf5", df, row_counts, meta_keys, meta_columns, db=db,
            )
            rids = result.strip().split("\n")
            assert len(rids) == 3

            loaded = db.load(cls, {"subject": "3", "session": "gamma"})
            assert float(loaded.data["w"].iloc[0]) == 3.0
        finally:
            db.close()

    def test_empty_dataframe_returns_empty(self, tmp_path):
        """Edge case: empty row_counts → no saves, returns empty string."""
        import pandas as pd

        db, _ = self._make_db(tmp_path, "BatchDf6")
        try:
            df = pd.DataFrame({"x": pd.Series(dtype=float)})
            row_counts = np.array([], dtype=np.int64)

            result = for_each_batch_save_dataframe(
                "BatchDf6", df, row_counts, [], [], db=db,
            )
            assert result == ""
        finally:
            db.close()

    def test_unregistered_type_raises(self, tmp_path):
        """ValueError for unknown type."""
        import pandas as pd

        df = pd.DataFrame({"x": [1.0]})
        row_counts = np.array([1], dtype=np.int64)

        with pytest.raises(ValueError, match="not registered"):
            for_each_batch_save_dataframe(
                "CompletelyUnknownType_xyz_999", df, row_counts, [], [],
            )


# ---------------------------------------------------------------------------
# split_flat_to_lists tests
# ---------------------------------------------------------------------------

class TestSplitFlatToLists:
    """Verify split_flat_to_lists correctly splits flat arrays into Python lists."""

    def test_float_split(self):
        """Float64 array split into 3 equal-length lists."""
        flat = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], dtype=np.float64)
        lengths = np.array([2, 2, 2], dtype=np.int64)
        result = split_flat_to_lists(flat, lengths)
        assert result == [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]
        # Verify native Python types
        assert all(isinstance(x, float) for sublist in result for x in sublist)

    def test_bool_split(self):
        """Bool array split into lists of True/False."""
        flat = np.array([True, False, True, True], dtype=bool)
        lengths = np.array([2, 2], dtype=np.int64)
        result = split_flat_to_lists(flat, lengths)
        assert result == [[True, False], [True, True]]
        assert all(isinstance(x, bool) for sublist in result for x in sublist)

    def test_variable_lengths(self):
        """Different sub-list lengths (1, 3, 2)."""
        flat = np.array([10.0, 20.0, 30.0, 40.0, 50.0, 60.0], dtype=np.float64)
        lengths = np.array([1, 3, 2], dtype=np.int64)
        result = split_flat_to_lists(flat, lengths)
        assert result == [[10.0], [20.0, 30.0, 40.0], [50.0, 60.0]]

    def test_empty_sublists(self):
        """Some zero-length entries produce empty lists."""
        flat = np.array([1.0, 2.0, 3.0], dtype=np.float64)
        lengths = np.array([0, 2, 0, 1, 0], dtype=np.int64)
        result = split_flat_to_lists(flat, lengths)
        assert result == [[], [1.0, 2.0], [], [3.0], []]

    def test_empty_input(self):
        """Empty flat array + empty lengths = empty result."""
        flat = np.array([], dtype=np.float64)
        lengths = np.array([], dtype=np.int64)
        result = split_flat_to_lists(flat, lengths)
        assert result == []

    def test_int_split(self):
        """Integer array split into lists."""
        flat = np.array([1, 2, 3, 4, 5], dtype=np.int64)
        lengths = np.array([3, 2], dtype=np.int64)
        result = split_flat_to_lists(flat, lengths)
        assert result == [[1, 2, 3], [4, 5]]
        assert all(isinstance(x, int) for sublist in result for x in sublist)

    def test_single_element_sublists(self):
        """Each sub-list has exactly one element."""
        flat = np.array([10.0, 20.0, 30.0], dtype=np.float64)
        lengths = np.array([1, 1, 1], dtype=np.int64)
        result = split_flat_to_lists(flat, lengths)
        assert result == [[10.0], [20.0], [30.0]]
