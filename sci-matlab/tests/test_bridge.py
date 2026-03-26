"""Tests for the MATLAB-SciStack Python bridge.

These tests verify that the proxy classes satisfy the duck-typing
contracts of thunk-lib without requiring MATLAB.
"""

import sys
from hashlib import sha256
from pathlib import Path

# Add source paths for the monorepo packages
_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "src"))
sys.path.insert(0, str(_root / "thunk-lib" / "src"))
sys.path.insert(0, str(_root / "canonical-hash" / "src"))
sys.path.insert(0, str(_root / "sciduck" / "src"))
sys.path.insert(0, str(_root / "pipelinedb-lib" / "src"))
sys.path.insert(0, str(_root / "path-gen" / "src"))
sys.path.insert(0, str(_root / "scirun-lib" / "src"))
sys.path.insert(0, str(_root / "sci-matlab" / "src"))

import numpy as np
import pytest

from sci_matlab.bridge import (
    MatlabThunk,
    MatlabPipelineThunk,
    check_cache,
    make_thunk_output,
    register_matlab_variable,
    get_surrogate_class,
)
from thunk.core import ThunkOutput, Thunk, PipelineThunk
from thunk.inputs import classify_inputs, classify_input, InputKind
from thunk.lineage import extract_lineage, LineageRecord
from scidb.variable import BaseVariable


# ---------------------------------------------------------------------------
# MatlabThunk proxy tests
# ---------------------------------------------------------------------------

class TestMatlabThunk:
    """Verify MatlabThunk satisfies the Thunk duck-typing contract."""

    def test_has_required_attributes(self):
        t = MatlabThunk("abc123", "my_function")
        assert hasattr(t, "hash")
        assert hasattr(t, "fcn")
        assert hasattr(t, "unpack_output")
        assert hasattr(t, "unwrap")
        assert hasattr(t, "pipeline_thunks")
        assert t.fcn.__name__ == "my_function"
        assert t.unpack_output is False
        assert t.unwrap is True

    def test_hash_is_deterministic(self):
        t1 = MatlabThunk("abc123", "f")
        t2 = MatlabThunk("abc123", "f")
        assert t1.hash == t2.hash
        assert len(t1.hash) == 64  # Full SHA-256 hex

    def test_hash_changes_with_source(self):
        t1 = MatlabThunk("abc123", "f")
        t2 = MatlabThunk("def456", "f")
        assert t1.hash != t2.hash

    def test_hash_changes_with_unpack_output(self):
        t1 = MatlabThunk("abc123", "f", unpack_output=False)
        t2 = MatlabThunk("abc123", "f", unpack_output=True)
        assert t1.hash != t2.hash

    def test_hash_algorithm_matches_thunk(self):
        """Verify the hash algorithm matches Thunk.__init__ for the same
        source_hash and unpack_output, so the structure is the same."""
        source_hash = sha256(b"test_source").hexdigest()
        t = MatlabThunk(source_hash, "f", unpack_output=False)

        # Same algorithm: sha256(f"{source_hash}-{unpack_output}")
        expected = sha256(f"{source_hash}-False".encode()).hexdigest()
        assert t.hash == expected


# ---------------------------------------------------------------------------
# MatlabPipelineThunk proxy tests
# ---------------------------------------------------------------------------

class TestMatlabPipelineThunk:
    """Verify MatlabPipelineThunk satisfies PipelineThunk duck-typing."""

    def test_has_required_attributes(self):
        mt = MatlabThunk("abc", "f")
        pt = MatlabPipelineThunk(mt, {"arg_0": 42})
        assert hasattr(pt, "thunk")
        assert hasattr(pt, "inputs")
        assert hasattr(pt, "outputs")
        assert hasattr(pt, "unwrap")
        assert hasattr(pt, "hash")
        assert hasattr(pt, "compute_lineage_hash")

    def test_lineage_hash_is_deterministic(self):
        mt = MatlabThunk("abc", "f")
        pt1 = MatlabPipelineThunk(mt, {"arg_0": 42})
        pt2 = MatlabPipelineThunk(mt, {"arg_0": 42})
        assert pt1.hash == pt2.hash

    def test_lineage_hash_changes_with_inputs(self):
        mt = MatlabThunk("abc", "f")
        pt1 = MatlabPipelineThunk(mt, {"arg_0": 42})
        pt2 = MatlabPipelineThunk(mt, {"arg_0": 99})
        assert pt1.hash != pt2.hash

    def test_lineage_hash_changes_with_function(self):
        mt1 = MatlabThunk("abc", "f")
        mt2 = MatlabThunk("def", "g")
        pt1 = MatlabPipelineThunk(mt1, {"arg_0": 42})
        pt2 = MatlabPipelineThunk(mt2, {"arg_0": 42})
        assert pt1.hash != pt2.hash

    def test_classify_inputs_works_with_constant(self):
        """classify_inputs from thunk-lib works on our proxy's inputs."""
        mt = MatlabThunk("abc", "f")
        pt = MatlabPipelineThunk(mt, {"arg_0": 42, "arg_1": "hello"})
        classified = classify_inputs(pt.inputs)
        assert len(classified) == 2
        assert all(c.kind == InputKind.CONSTANT for c in classified)

    def test_classify_inputs_works_with_base_variable(self):
        """A real BaseVariable is classified correctly as a thunk input."""
        var = BaseVariable(np.array([1.0, 2.0, 3.0]))
        var.record_id = "abc123def456abcd"
        var.metadata = {"subject": 1}
        var.content_hash = "1234567890abcdef"

        mt = MatlabThunk("abc", "f")
        pt = MatlabPipelineThunk(mt, {"arg_0": var})
        classified = classify_inputs(pt.inputs)
        assert len(classified) == 1
        assert classified[0].kind == InputKind.SAVED_VARIABLE

    def test_classify_inputs_works_with_thunk_output(self):
        """A real ThunkOutput (from make_thunk_output) is classified
        correctly when used as input to another thunk."""
        mt1 = MatlabThunk("abc", "step1")
        pt1 = MatlabPipelineThunk(mt1, {"arg_0": 42})
        to1 = make_thunk_output(pt1, 0, np.array([1.0]))

        mt2 = MatlabThunk("def", "step2")
        pt2 = MatlabPipelineThunk(mt2, {"arg_0": to1})
        classified = classify_inputs(pt2.inputs)
        assert len(classified) == 1
        assert classified[0].kind == InputKind.THUNK_OUTPUT

    def test_saved_variable_with_lineage_hash_classified_as_thunk_output(self):
        """A BaseVariable with _lineage_hash is reclassified as THUNK_OUTPUT,
        matching the behaviour for Python-saved thunk results."""
        var = BaseVariable(np.array([1.0]))
        var.record_id = "abc123def456abcd"
        var.lineage_hash = "a" * 64
        var.content_hash = "1234567890abcdef"

        mt = MatlabThunk("abc", "f")
        pt = MatlabPipelineThunk(mt, {"arg_0": var})
        classified = classify_inputs(pt.inputs)
        assert len(classified) == 1
        # Reclassified as THUNK_OUTPUT because _lineage_hash is set
        assert classified[0].kind == InputKind.THUNK_OUTPUT
        assert classified[0].hash == "a" * 64


# ---------------------------------------------------------------------------
# make_thunk_output tests
# ---------------------------------------------------------------------------

class TestMakeThunkOutput:
    """Verify make_thunk_output creates real ThunkOutput instances."""

    def test_returns_real_thunk_output(self):
        mt = MatlabThunk("abc", "f")
        pt = MatlabPipelineThunk(mt, {"arg_0": 42})
        to = make_thunk_output(pt, 0, np.array([1.0, 2.0]))
        assert isinstance(to, ThunkOutput)

    def test_thunk_output_attributes(self):
        mt = MatlabThunk("abc", "f")
        pt = MatlabPipelineThunk(mt, {"arg_0": 42})
        data = np.array([1.0, 2.0])
        to = make_thunk_output(pt, 0, data)
        assert to.is_complete is True
        assert to.output_num == 0
        assert np.array_equal(to.data, data)
        assert to.pipeline_thunk is pt

    def test_thunk_output_hash_deterministic(self):
        mt = MatlabThunk("abc", "f")
        pt = MatlabPipelineThunk(mt, {"arg_0": 42})
        to1 = make_thunk_output(pt, 0, np.array([1.0]))
        to2 = make_thunk_output(pt, 0, np.array([1.0]))
        assert to1.hash == to2.hash

    def test_different_output_nums_different_hashes(self):
        mt = MatlabThunk("abc", "f")
        pt = MatlabPipelineThunk(mt, {"arg_0": 42})
        to0 = make_thunk_output(pt, 0, np.array([1.0]))
        to1 = make_thunk_output(pt, 1, np.array([2.0]))
        assert to0.hash != to1.hash


# ---------------------------------------------------------------------------
# extract_lineage compatibility tests
# ---------------------------------------------------------------------------

class TestExtractLineage:
    """Verify that thunk-lib's extract_lineage works on our proxy objects."""

    def test_extract_lineage_from_matlab_thunk_output(self):
        mt = MatlabThunk("abc", "my_matlab_func")
        pt = MatlabPipelineThunk(mt, {"arg_0": 42, "arg_1": "hello"})
        to = make_thunk_output(pt, 0, np.array([1.0]))

        lineage = extract_lineage(to)
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

        mt = MatlabThunk("abc", "process")
        pt = MatlabPipelineThunk(mt, {"arg_0": var, "arg_1": 2.5})
        to = make_thunk_output(pt, 0, np.array([2.0, 4.0]))

        lineage = extract_lineage(to)
        assert lineage.function_name == "process"
        assert len(lineage.inputs) == 1      # The BaseVariable
        assert len(lineage.constants) == 1   # 2.5

    def test_extract_lineage_chained_thunks(self):
        """Two MATLAB thunks chained: step1 -> step2."""
        mt1 = MatlabThunk("abc", "step1")
        pt1 = MatlabPipelineThunk(mt1, {"arg_0": 42})
        to1 = make_thunk_output(pt1, 0, np.array([84.0]))

        mt2 = MatlabThunk("def", "step2")
        pt2 = MatlabPipelineThunk(mt2, {"arg_0": to1})
        to2 = make_thunk_output(pt2, 0, np.array([80.0]))

        lineage = extract_lineage(to2)
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

    def test_save_variable_with_matlab_thunk_output(self, tmp_path):
        """End-to-end: create MATLAB proxies, make ThunkOutput, save it."""
        from scidb.database import configure_database

        db = configure_database(
            tmp_path / "test.duckdb",
            ["subject"],
        )

        try:
            register_matlab_variable("MatlabResult")

            # Simulate a MATLAB thunk execution
            mt = MatlabThunk("source_hash_abc", "matlab_filter")
            pt = MatlabPipelineThunk(mt, {"arg_0": 42, "arg_1": 3.14})
            result_data = np.array([1.0, 2.0, 3.0])
            to = make_thunk_output(pt, 0, result_data)

            # save_variable should work with our ThunkOutput
            var_class = get_surrogate_class("MatlabResult")
            record_id = db.save_variable(var_class, to, subject=1)
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

    # @pytest.mark.skip(reason="lineage_hash/pipeline_lineage_hash ambiguity not yet resolved")
    def test_cache_hit_with_matlab_thunk(self, tmp_path):
        """After saving, find_by_lineage should return the cached data."""
        from scidb.database import configure_database

        db = configure_database(
            tmp_path / "test2.duckdb",
            ["subject"],
        )

        try:
            register_matlab_variable("MatlabCached")

            mt = MatlabThunk("source_hash_xyz", "matlab_process")
            pt = MatlabPipelineThunk(mt, {"arg_0": 100})
            result_data = np.array([10.0, 20.0])
            to = make_thunk_output(pt, 0, result_data)

            var_class = get_surrogate_class("MatlabCached")
            db.save_variable(var_class, to, subject=1)

            # Now check cache with the same computation
            pt2 = MatlabPipelineThunk(mt, {"arg_0": 100})
            cached = db.find_by_lineage(pt2)
            assert cached is not None
            assert len(cached) == 1
            assert np.array_equal(cached[0], result_data)

        finally:
            db.close()
