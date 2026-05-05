"""Integration tests for unified variant tracking (scidb Option 2 implementation).

Tests that scihist.for_each outputs have complete version_keys and branch_params
matching scidb.for_each structure, eliminating the dual variant tracking system.
"""

import json
import numpy as np
import pytest

from scidb import BaseVariable, Fixed
from scilineage import lineage_fcn
from scihist import for_each as scihist_for_each
import scidb

from conftest import DEFAULT_TEST_SCHEMA_KEYS


def parse_version_keys(vk):
    """Parse version_keys from database (handles both dict and JSON string)."""
    if isinstance(vk, str):
        return json.loads(vk)
    return vk


# Test variable types
class RawData(BaseVariable):
    schema_version = 1


class ProcessedData(BaseVariable):
    schema_version = 1


class FinalResult(BaseVariable):
    schema_version = 1


class IntermediateA(BaseVariable):
    schema_version = 1


class IntermediateB(BaseVariable):
    schema_version = 1


class TestVersionKeysCompleteness:
    """Test that scihist outputs have complete version_keys."""

    def test_scihist_has_all_version_keys(self, db):
        """scihist outputs should have __fn, __fn_hash, __inputs, and __constants."""
        @lineage_fcn
        def process(x, threshold):
            return x * threshold

        # Save input
        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)

        # Run scihist.for_each
        scihist_for_each(
            process,
            inputs={"x": RawData, "threshold": 2.0},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        # Check saved metadata
        con = db._duck.con
        result = con.execute("""
            SELECT version_keys
            FROM _record_metadata
            WHERE variable_name = 'ProcessedData'
        """).fetchone()

        assert result is not None, "No ProcessedData record found"
        version_keys = parse_version_keys(result[0])

        # Verify all required keys present
        assert "__fn" in version_keys, "Missing __fn"
        assert "__fn_hash" in version_keys, "Missing __fn_hash"
        assert "__inputs" in version_keys, "Missing __inputs"
        assert "__constants" in version_keys, "Missing __constants"

        # Verify content
        assert version_keys["__fn"] == "process"
        assert len(version_keys["__fn_hash"]) == 16, "Function hash should be 16 chars"

        inputs = version_keys["__inputs"]
        assert "x" in inputs, "Input 'x' not in __inputs"

        constants = version_keys["__constants"]
        assert "threshold" in constants, "Constant 'threshold' not in __constants"
        assert constants["threshold"] == 2.0

    def test_scihist_has_populated_branch_params(self, db):
        """scihist outputs should have non-empty branch_params."""
        @lineage_fcn
        def scale(x, factor):
            return x * factor

        RawData.save(np.array([5, 10, 15]), subject=1, trial=1)

        scihist_for_each(
            scale,
            inputs={"x": RawData, "factor": 3.0},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        # Check branch_params (stored in separate column)
        con = db._duck.con
        result = con.execute("""
            SELECT branch_params
            FROM _record_metadata
            WHERE variable_name = 'ProcessedData'
        """).fetchone()

        assert result is not None, "No ProcessedData record found"
        branch_params_str = result[0]
        branch_params = json.loads(branch_params_str) if isinstance(branch_params_str, str) else branch_params_str

        # Should NOT be empty (was bug in old implementation)
        assert branch_params != {}, "branch_params should not be empty"

        # Should contain function-namespaced constant
        assert "scale.factor" in branch_params, "Missing namespaced constant"
        assert branch_params["scale.factor"] == 3.0

    def test_multiple_constants_in_version_keys(self, db):
        """All constants should appear in __constants."""
        @lineage_fcn
        def compute(x, alpha, beta, gamma):
            return x * alpha + beta * gamma

        RawData.save(np.array([1, 2]), subject=1, trial=1)

        scihist_for_each(
            compute,
            inputs={"x": RawData, "alpha": 0.5, "beta": 2.0, "gamma": 3.0},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        con = db._duck.con
        result = con.execute("""
            SELECT version_keys
            FROM _record_metadata
            WHERE variable_name = 'ProcessedData'
        """).fetchone()

        version_keys = parse_version_keys(result[0])
        constants = version_keys["__constants"]

        assert len(constants) == 3
        assert constants["alpha"] == 0.5
        assert constants["beta"] == 2.0
        assert constants["gamma"] == 3.0


class TestBranchParamsAccumulation:
    """Test that branch_params accumulate correctly across pipeline stages."""

    def test_branch_params_accumulate_across_pipeline(self, db):
        """Downstream branch_params should include upstream constants."""
        @lineage_fcn
        def step1(x, param1):
            return x + param1

        @lineage_fcn
        def step2(y, param2):
            return y * param2

        # Stage 1
        RawData.save(np.array([10, 20]), subject=1, trial=1)
        scihist_for_each(
            step1,
            inputs={"x": RawData, "param1": 5},
            outputs=[IntermediateA],
            subject=[1],
            trial=[1],
        )

        # Stage 2
        scihist_for_each(
            step2,
            inputs={"y": IntermediateA, "param2": 2},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        # Check final output's branch_params
        con = db._duck.con
        result = con.execute("""
            SELECT branch_params
            FROM _record_metadata
            WHERE variable_name = 'ProcessedData'
        """).fetchone()

        branch_params_str = result[0]
        branch_params = json.loads(branch_params_str) if isinstance(branch_params_str, str) else branch_params_str

        # Should contain BOTH upstream and current constants
        assert "step1.param1" in branch_params, "Missing upstream param1"
        assert "step2.param2" in branch_params, "Missing current param2"
        assert branch_params["step1.param1"] == 5
        assert branch_params["step2.param2"] == 2

    def test_branch_params_multiple_inputs(self, db):
        """branch_params should merge from all upstream inputs."""
        @lineage_fcn
        def process_a(x, alpha):
            return x * alpha

        @lineage_fcn
        def process_b(x, beta):
            return x + beta

        @lineage_fcn
        def combine(a, b, gamma):
            return a + b + gamma

        # Create two parallel branches
        RawData.save(np.array([1, 2]), subject=1, trial=1)

        scihist_for_each(
            process_a,
            inputs={"x": RawData, "alpha": 2.0},
            outputs=[IntermediateA],
            subject=[1],
            trial=[1],
        )

        scihist_for_each(
            process_b,
            inputs={"x": RawData, "beta": 10.0},
            outputs=[IntermediateB],
            subject=[1],
            trial=[1],
        )

        # Combine both branches
        scihist_for_each(
            combine,
            inputs={"a": IntermediateA, "b": IntermediateB, "gamma": 5.0},
            outputs=[FinalResult],
            subject=[1],
            trial=[1],
        )

        # Check branch_params contains ALL upstream constants
        con = db._duck.con
        result = con.execute("""
            SELECT branch_params
            FROM _record_metadata
            WHERE variable_name = 'FinalResult'
        """).fetchone()

        branch_params_str = result[0]
        branch_params = json.loads(branch_params_str) if isinstance(branch_params_str, str) else branch_params_str

        # Should have constants from BOTH branches plus current
        assert "process_a.alpha" in branch_params
        assert "process_b.beta" in branch_params
        assert "combine.gamma" in branch_params
        assert branch_params["process_a.alpha"] == 2.0
        assert branch_params["process_b.beta"] == 10.0
        assert branch_params["combine.gamma"] == 5.0


class TestFixedInputTracking:
    """Test that Fixed inputs are tracked correctly in lineage."""

    def test_fixed_input_in_lineage(self, db):
        """Fixed inputs should appear in _lineage.inputs as rid_tracking entries."""
        @lineage_fcn
        def process(ref, value):
            return ref + value

        # Save reference data
        RawData.save(np.array([100, 200]), subject=1, trial=1)

        # Use Fixed input
        scihist_for_each(
            process,
            inputs={"ref": Fixed(RawData, subject=1, trial=1), "value": 50},
            outputs=[ProcessedData],
            subject=[2],  # Different subject
            trial=[1],
        )

        # Check that output was created
        con = db._duck.con

        # First verify the record exists in _record_metadata
        record_check = con.execute("""
            SELECT record_id
            FROM _record_metadata
            WHERE variable_name = 'ProcessedData'
        """).fetchone()
        assert record_check is not None, "No ProcessedData record found in _record_metadata"

        # Now check lineage has rid_tracking entry for Fixed input
        result = con.execute("""
            SELECT inputs
            FROM _lineage
            WHERE target = 'ProcessedData'
        """).fetchone()

        assert result is not None, "No lineage record found in _lineage"
        inputs_json = result[0]
        inputs = json.loads(inputs_json) if isinstance(inputs_json, str) else inputs_json

        # Find rid_tracking entries
        rid_tracking_entries = [
            inp for inp in inputs
            if inp.get("source_type") == "rid_tracking"
        ]

        assert len(rid_tracking_entries) > 0, "No rid_tracking entries found"

        # Should have __rid_ref for the Fixed input
        ref_entry = next(
            (e for e in rid_tracking_entries if e["name"] == "__rid_ref"),
            None
        )
        assert ref_entry is not None, "Missing __rid_ref for Fixed input"
        assert "record_id" in ref_entry, "rid_tracking entry missing record_id"

    def test_fixed_input_staleness_detection(self, db):
        """Changing a Fixed input should cause skip_computed to re-run."""
        call_count = 0

        @lineage_fcn
        def use_fixed(ref, multiplier):
            nonlocal call_count
            call_count += 1
            return ref * multiplier

        # Initial run
        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)
        scihist_for_each(
            use_fixed,
            inputs={"ref": Fixed(RawData, subject=1, trial=1), "multiplier": 2},
            outputs=[ProcessedData],
            subject=[10],
            trial=[1],
            skip_computed=True,
        )
        assert call_count == 1

        # Re-run with same Fixed input - should skip
        scihist_for_each(
            use_fixed,
            inputs={"ref": Fixed(RawData, subject=1, trial=1), "multiplier": 2},
            outputs=[ProcessedData],
            subject=[10],
            trial=[1],
            skip_computed=True,
        )
        assert call_count == 1, "Should have skipped (Fixed input unchanged)"

        # Update the Fixed input data
        RawData.save(np.array([10, 20, 30]), subject=1, trial=1)

        # Re-run - should NOT skip (Fixed input changed)
        scihist_for_each(
            use_fixed,
            inputs={"ref": Fixed(RawData, subject=1, trial=1), "multiplier": 2},
            outputs=[ProcessedData],
            subject=[10],
            trial=[1],
            skip_computed=True,
        )
        assert call_count == 2, "Should have re-run (Fixed input changed)"


class TestVariantDiscovery:
    """Test that variant discovery works correctly with unified tracking."""

    def test_multiple_constant_variants(self, db):
        """Different constant values should create distinct variants."""
        @lineage_fcn
        def scale(x, factor):
            return x * factor

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)

        # Run with multiple factor values
        for factor in [1.0, 2.0, 3.0]:
            scihist_for_each(
                scale,
                inputs={"x": RawData, "factor": factor},
                outputs=[ProcessedData],
                subject=[1],
                trial=[1],
            )

        # Should have 3 distinct outputs
        con = db._duck.con
        count = con.execute("""
            SELECT COUNT(DISTINCT record_id)
            FROM _record_metadata
            WHERE variable_name = 'ProcessedData'
        """).fetchone()[0]

        assert count == 3, f"Expected 3 variants, found {count}"

        # Check that each has different __constants
        results = con.execute("""
            SELECT version_keys
            FROM _record_metadata
            WHERE variable_name = 'ProcessedData'
        """).fetchall()

        factors = []
        for (version_keys,) in results:
            version_keys = parse_version_keys(version_keys)
            constants = version_keys["__constants"]
            factors.append(constants["factor"])

        assert sorted(factors) == [1.0, 2.0, 3.0]

    def test_variant_query_consistency(self, db):
        """Variants should be queryable via version_keys OR _lineage."""
        @lineage_fcn
        def compute(x, param):
            return x + param

        RawData.save(np.array([5]), subject=1, trial=1)

        scihist_for_each(
            compute,
            inputs={"x": RawData, "param": 10},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        con = db._duck.con

        # Query via version_keys.__constants (use proper json_extract with JSON type)
        via_version_keys = con.execute("""
            SELECT record_id
            FROM _record_metadata
            WHERE variable_name = 'ProcessedData'
              AND json_extract(version_keys, '$.__constants.param') = 10
        """).fetchall()

        # Query via _lineage (constants is array of objects, check for param constant)
        via_lineage = con.execute("""
            SELECT output_record_id
            FROM _lineage
            WHERE target = 'ProcessedData'
              AND CAST(constants AS VARCHAR) LIKE '%"name": "param"%'
        """).fetchall()

        # Should find the same record both ways
        assert len(via_version_keys) == 1
        assert len(via_lineage) == 1
        assert via_version_keys[0][0] == via_lineage[0][0]


class TestComparisonWithScidb:
    """Test that scihist and scidb outputs have similar metadata structure."""

    def test_metadata_structure_matches_scidb(self, db):
        """scihist outputs should have similar metadata to scidb outputs."""
        # Plain function for scidb
        def plain_process(x, threshold):
            return x * threshold

        # Lineage function for scihist
        @lineage_fcn
        def lineage_process(x, threshold):
            return x * threshold

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)
        RawData.save(np.array([4, 5, 6]), subject=2, trial=1)

        # Run with scidb
        scidb.for_each(
            plain_process,
            inputs={"x": RawData, "threshold": 2.0},
            outputs=[IntermediateA],
            subject=[1],
            trial=[1],
        )

        # Run with scihist
        scihist_for_each(
            lineage_process,
            inputs={"x": RawData, "threshold": 2.0},
            outputs=[IntermediateB],
            subject=[2],
            trial=[1],
        )

        con = db._duck.con

        # Get metadata from both
        scidb_meta = parse_version_keys(con.execute("""
            SELECT version_keys
            FROM _record_metadata
            WHERE variable_name = 'IntermediateA'
        """).fetchone()[0])

        scihist_meta = parse_version_keys(con.execute("""
            SELECT version_keys
            FROM _record_metadata
            WHERE variable_name = 'IntermediateB'
        """).fetchone()[0])

        # Both should have same structure (version_keys)
        assert set(scidb_meta.keys()) == set(scihist_meta.keys()), \
            "Metadata keys should match"

        # Both should have complete version_keys
        for meta in [scidb_meta, scihist_meta]:
            assert "__fn" in meta
            assert "__fn_hash" in meta
            assert "__inputs" in meta
            assert "__constants" in meta

        # Constants should match
        scidb_constants = scidb_meta["__constants"]
        scihist_constants = scihist_meta["__constants"]
        assert scidb_constants == scihist_constants

    def test_branch_params_structure_matches_scidb(self, db):
        """branch_params structure should match between scidb and scihist."""
        # Multi-stage pipeline
        def plain_step1(x, p1):
            return x + p1

        def plain_step2(y, p2):
            return y * p2

        @lineage_fcn
        def lineage_step1(x, p1):
            return x + p1

        @lineage_fcn
        def lineage_step2(y, p2):
            return y * p2

        RawData.save(np.array([10]), subject=1, trial=1)
        RawData.save(np.array([10]), subject=2, trial=1)

        # scidb pipeline
        scidb.for_each(plain_step1, {"x": RawData, "p1": 5}, [IntermediateA], subject=[1], trial=[1])
        scidb.for_each(plain_step2, {"y": IntermediateA, "p2": 2}, [ProcessedData], subject=[1], trial=[1])

        # scihist pipeline
        scihist_for_each(lineage_step1, {"x": RawData, "p1": 5}, [IntermediateB], subject=[2], trial=[1])
        scihist_for_each(lineage_step2, {"y": IntermediateB, "p2": 2}, [FinalResult], subject=[2], trial=[1])

        con = db._duck.con

        # Get branch_params from final outputs
        scidb_bp_str = con.execute("""
            SELECT branch_params
            FROM _record_metadata
            WHERE variable_name = 'ProcessedData'
        """).fetchone()[0]
        scidb_bp = json.loads(scidb_bp_str) if isinstance(scidb_bp_str, str) else scidb_bp_str

        scihist_bp_str = con.execute("""
            SELECT branch_params
            FROM _record_metadata
            WHERE variable_name = 'FinalResult'
        """).fetchone()[0]
        scihist_bp = json.loads(scihist_bp_str) if isinstance(scihist_bp_str, str) else scihist_bp_str

        # Both should have accumulated upstream params
        # Note: function names differ (plain_step1 vs lineage_step1) but structure matches
        assert len(scidb_bp) == 2, "scidb should have 2 params"
        assert len(scihist_bp) == 2, "scihist should have 2 params"

        # Check namespacing pattern (function.param)
        for bp in [scidb_bp, scihist_bp]:
            keys = list(bp.keys())
            assert all("." in k for k in keys), "All params should be namespaced"


class TestMultipleOutputs:
    """Test that multiple outputs are handled correctly."""

    def test_multiple_outputs_all_have_metadata(self, db):
        """All outputs should have complete metadata."""
        @lineage_fcn
        def split_process(x, factor):
            return x * factor, x + factor

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)

        scihist_for_each(
            split_process,
            inputs={"x": RawData, "factor": 5},
            outputs=[ProcessedData, FinalResult],
            subject=[1],
            trial=[1],
        )

        con = db._duck.con

        # Both outputs should exist
        for var_name in ["ProcessedData", "FinalResult"]:
            result = con.execute("""
                SELECT version_keys
                FROM _record_metadata
                WHERE variable_name = ?
            """, [var_name]).fetchone()

            assert result is not None, f"Missing output {var_name}"
            version_keys = parse_version_keys(result[0])

            # Check complete metadata
            assert "__fn" in version_keys
            assert "__fn_hash" in version_keys
            assert "__inputs" in version_keys
            assert "__constants" in version_keys

            constants = version_keys["__constants"]
            assert constants["factor"] == 5

    def test_multiple_outputs_same_branch_params(self, db):
        """All outputs from same call should have identical branch_params."""
        @lineage_fcn
        def dual_output(x, alpha, beta):
            return x * alpha, x + beta

        RawData.save(np.array([10, 20]), subject=1, trial=1)

        scihist_for_each(
            dual_output,
            inputs={"x": RawData, "alpha": 2.0, "beta": 5.0},
            outputs=[IntermediateA, IntermediateB],
            subject=[1],
            trial=[1],
        )

        con = db._duck.con

        # Get branch_params from both outputs
        bp_a_str = con.execute("""
            SELECT branch_params
            FROM _record_metadata
            WHERE variable_name = 'IntermediateA'
        """).fetchone()[0]
        bp_a = json.loads(bp_a_str) if isinstance(bp_a_str, str) else bp_a_str

        bp_b_str = con.execute("""
            SELECT branch_params
            FROM _record_metadata
            WHERE variable_name = 'IntermediateB'
        """).fetchone()[0]
        bp_b = json.loads(bp_b_str) if isinstance(bp_b_str, str) else bp_b_str

        # Should be identical
        assert bp_a == bp_b


class TestGeneratesFile:
    """Test that generates_file functions work correctly."""

    def test_generates_file_has_metadata(self, db):
        """generates_file outputs should have version_keys even without data."""
        @lineage_fcn(generates_file=True)
        def export_data(x, filename):
            # Side-effect only, no return value
            pass

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)

        scihist_for_each(
            export_data,
            inputs={"x": RawData, "filename": "output.csv"},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        con = db._duck.con

        # Should have metadata even though no data was saved
        result = con.execute("""
            SELECT version_keys, content_hash
            FROM _record_metadata
            WHERE variable_name = 'ProcessedData'
        """).fetchone()

        assert result is not None, "generates_file should create record"
        version_keys, content_hash = result
        version_keys = parse_version_keys(version_keys)

        # Should have complete version_keys
        assert "__fn" in version_keys
        assert "__fn_hash" in version_keys
        assert "__constants" in version_keys

        # Should NOT have content_hash (no data)
        assert content_hash is None

        # Should have lineage record
        lineage_result = con.execute("""
            SELECT output_record_id
            FROM _lineage
            WHERE target = 'ProcessedData'
        """).fetchone()

        assert lineage_result is not None


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_no_constants(self, db):
        """Function with only variable inputs should have empty __constants."""
        @lineage_fcn
        def identity(x):
            return x

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)

        scihist_for_each(
            identity,
            inputs={"x": RawData},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        con = db._duck.con
        result = con.execute("""
            SELECT version_keys
            FROM _record_metadata
            WHERE variable_name = 'ProcessedData'
        """).fetchone()

        version_keys = parse_version_keys(result[0])
        constants = version_keys["__constants"]
        assert constants == {}

    def test_empty_branch_params_first_stage(self, db):
        """First pipeline stage should have only current function's params."""
        @lineage_fcn
        def first_stage(x, alpha):
            return x * alpha

        RawData.save(np.array([5]), subject=1, trial=1)

        scihist_for_each(
            first_stage,
            inputs={"x": RawData, "alpha": 3.0},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        con = db._duck.con
        result = con.execute("""
            SELECT branch_params
            FROM _record_metadata
            WHERE variable_name = 'ProcessedData'
        """).fetchone()

        branch_params_str = result[0]
        branch_params = json.loads(branch_params_str) if isinstance(branch_params_str, str) else branch_params_str

        # Should ONLY have current function's param (no upstream)
        assert len(branch_params) == 1
        assert "first_stage.alpha" in branch_params

    def test_dry_run_no_save(self, db):
        """dry_run should not save any outputs."""
        @lineage_fcn
        def compute(x, value):
            return x + value

        RawData.save(np.array([1]), subject=1, trial=1)

        scihist_for_each(
            compute,
            inputs={"x": RawData, "value": 10},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
            dry_run=True,
        )

        # Should NOT have saved anything
        con = db._duck.con
        count = con.execute("""
            SELECT COUNT(*)
            FROM _record_metadata
            WHERE variable_name = 'ProcessedData'
        """).fetchone()[0]

        assert count == 0, "dry_run should not save outputs"

    def test_where_clause_metadata(self, db):
        """where clause should appear in version_keys.__where."""
        @lineage_fcn
        def filter_process(x, threshold):
            return x * threshold

        RawData.save(np.array([1, 2]), subject=1, trial=1)
        RawData.save(np.array([3, 4]), subject=2, trial=1)

        scihist_for_each(
            filter_process,
            inputs={"x": RawData, "threshold": 2.0},
            outputs=[ProcessedData],
            subject=[],
            trial=[1],
            where="subject == 1",
        )

        con = db._duck.con
        result = con.execute("""
            SELECT version_keys
            FROM _record_metadata
            WHERE variable_name = 'ProcessedData'
        """).fetchone()

        version_keys = parse_version_keys(result[0])
        assert "__where" in version_keys
        assert version_keys["__where"] == "subject == 1"


class TestSkipComputed:
    """Test that skip_computed works with unified variant tracking."""

    def test_skip_computed_with_constants(self, db):
        """skip_computed should work based on __constants in version_keys."""
        call_count = 0

        @lineage_fcn
        def expensive(x, param):
            nonlocal call_count
            call_count += 1
            return x * param

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)

        # First run
        scihist_for_each(
            expensive,
            inputs={"x": RawData, "param": 5},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
            skip_computed=True,
        )
        assert call_count == 1

        # Second run with same params - should skip
        scihist_for_each(
            expensive,
            inputs={"x": RawData, "param": 5},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
            skip_computed=True,
        )
        assert call_count == 1, "Should have skipped"

        # Third run with different param - should NOT skip
        scihist_for_each(
            expensive,
            inputs={"x": RawData, "param": 10},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
            skip_computed=True,
        )
        assert call_count == 2, "Should have re-run with different param"

    def test_skip_computed_with_function_change(self, db):
        """Changing function body should cause re-run due to __fn_hash."""
        RawData.save(np.array([5]), subject=1, trial=1)

        # First version of function
        @lineage_fcn
        def version1(x, factor):
            return x * factor  # Simple multiply

        scihist_for_each(
            version1,
            inputs={"x": RawData, "factor": 2},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
            skip_computed=True,
        )

        # Get function hash
        con = db._duck.con
        hash1 = parse_version_keys(con.execute("""
            SELECT version_keys
            FROM _record_metadata
            WHERE variable_name = 'ProcessedData'
        """).fetchone()[0])["__fn_hash"]

        # Second version with different implementation
        @lineage_fcn
        def version1(x, factor):
            return x * factor + 1  # Changed implementation

        scihist_for_each(
            version1,
            inputs={"x": RawData, "factor": 2},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
            skip_computed=True,
        )

        # Should have created new record with different hash
        count = con.execute("""
            SELECT COUNT(*)
            FROM _record_metadata
            WHERE variable_name = 'ProcessedData'
        """).fetchone()[0]

        assert count == 2, "Changed function should create new variant"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
