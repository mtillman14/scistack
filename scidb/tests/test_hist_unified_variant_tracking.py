"""Integration tests for unified variant tracking (scidb Option 2 implementation).

Tests that for_each(skip_computed=True) outputs have complete version_keys and branch_params
matching scidb.for_each structure, eliminating the dual variant tracking system.
"""

from scidb.provenance import query as provenance_query
from functools import partial

import numpy as np
import pytest

import scidb
from scidb import BaseVariable, Fixed, scistack
from scidb import for_each as _for_each

# skip_computed=True: these tests cover the skip-already-computed path.
skip_computed_for_each = partial(_for_each, skip_computed=True)


def derived_bp(db, variable_name):
    """Derived branch_params (§6) for the latest record of a variable type.

    branch_params is no longer a stored column — it is derived from the bipartite
    provenance graph. This helper looks up the latest record for a variable type
    (recency from the _record_save log, type from the _record entity).
    """
    row = db._duck.con.execute(
        "SELECT rm.record_id FROM _record_save rm "
        "JOIN _record r ON r.record_id = rm.record_id "
        "WHERE r.type = ? ORDER BY rm.timestamp DESC LIMIT 1",
        [variable_name],
    ).fetchone()
    if row is None:
        return None
    return provenance_query.derived_branch_params(db._duck, row[0])


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
    """Test that skip_computed outputs have complete version_keys."""

    def test_skip_computed_has_all_version_keys(self, db):
        """skip_computed outputs record function + inputs + constants in the graph."""

        @scistack
        def process(x, threshold):
            return x * threshold

        # Save input
        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)

        # Run for_each(skip_computed=True)
        skip_computed_for_each(
            process,
            inputs={"x": RawData, "threshold": 2.0},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        # Provenance comes from the bipartite graph, not a version_keys column.
        prov = db.get_provenance(ProcessedData, subject=1, trial=1)
        assert prov is not None, "No ProcessedData provenance found"

        assert prov["function_name"] == "process"
        assert len(prov["function_hash"]) == 16, "Function hash should be 16 chars"

        input_params = {i["param_name"] for i in prov["inputs"]}
        assert "x" in input_params, f"Input 'x' not in {input_params}"

        constants = prov["constants"]
        assert "threshold" in constants, "Constant 'threshold' not recorded"
        assert constants["threshold"] == 2.0

    def test_skip_computed_has_populated_branch_params(self, db):
        """skip_computed outputs should have non-empty branch_params."""

        @scistack
        def scale(x, factor):
            return x * factor

        RawData.save(np.array([5, 10, 15]), subject=1, trial=1)

        skip_computed_for_each(
            scale,
            inputs={"x": RawData, "factor": 3.0},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        # branch_params derived from the bipartite graph (§6)
        branch_params = derived_bp(db, "ProcessedData")

        assert branch_params is not None, "No ProcessedData record found"

        # Should NOT be empty (was bug in old implementation)
        assert branch_params != {}, "branch_params should not be empty"

        # Should contain function-namespaced constant
        assert "scale.factor" in branch_params, "Missing namespaced constant"
        assert branch_params["scale.factor"] == 3.0

    def test_multiple_constants_in_version_keys(self, db):
        """All constants should be recorded on the producing invocation."""

        @scistack
        def compute(x, alpha, beta, gamma):
            return x * alpha + beta * gamma

        RawData.save(np.array([1, 2]), subject=1, trial=1)

        skip_computed_for_each(
            compute,
            inputs={"x": RawData, "alpha": 0.5, "beta": 2.0, "gamma": 3.0},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        constants = db.get_provenance(ProcessedData, subject=1, trial=1)["constants"]

        assert len(constants) == 3
        assert constants["alpha"] == 0.5
        assert constants["beta"] == 2.0
        assert constants["gamma"] == 3.0


class TestBranchParamsAccumulation:
    """Test that branch_params accumulate correctly across pipeline stages."""

    def test_branch_params_accumulate_across_pipeline(self, db):
        """Downstream branch_params should include upstream constants."""

        @scistack
        def step1(x, param1):
            return x + param1

        @scistack
        def step2(y, param2):
            return y * param2

        # Stage 1
        RawData.save(np.array([10, 20]), subject=1, trial=1)
        skip_computed_for_each(
            step1,
            inputs={"x": RawData, "param1": 5},
            outputs=[IntermediateA],
            subject=[1],
            trial=[1],
        )

        # Stage 2
        skip_computed_for_each(
            step2,
            inputs={"y": IntermediateA, "param2": 2},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        # Check final output's branch_params (derived from the graph, §6)
        branch_params = derived_bp(db, "ProcessedData")

        # Should contain BOTH upstream and current constants
        assert "step1.param1" in branch_params, "Missing upstream param1"
        assert "step2.param2" in branch_params, "Missing current param2"
        assert branch_params["step1.param1"] == 5
        assert branch_params["step2.param2"] == 2

    def test_branch_params_multiple_inputs(self, db):
        """branch_params should merge from all upstream inputs."""

        @scistack
        def process_a(x, alpha):
            return x * alpha

        @scistack
        def process_b(x, beta):
            return x + beta

        @scistack
        def combine(a, b, gamma):
            return a + b + gamma

        # Create two parallel branches
        RawData.save(np.array([1, 2]), subject=1, trial=1)

        skip_computed_for_each(
            process_a,
            inputs={"x": RawData, "alpha": 2.0},
            outputs=[IntermediateA],
            subject=[1],
            trial=[1],
        )

        skip_computed_for_each(
            process_b,
            inputs={"x": RawData, "beta": 10.0},
            outputs=[IntermediateB],
            subject=[1],
            trial=[1],
        )

        # Combine both branches
        skip_computed_for_each(
            combine,
            inputs={"a": IntermediateA, "b": IntermediateB, "gamma": 5.0},
            outputs=[FinalResult],
            subject=[1],
            trial=[1],
        )

        # Check branch_params contains ALL upstream constants (derived, §6)
        branch_params = derived_bp(db, "FinalResult")

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
        """Fixed inputs should appear in _lineage.inputs as variable entries (not constants)."""

        @scistack
        def process(ref, value):
            return ref + value

        # Save reference data
        ref_rid = RawData.save(np.array([100, 200]), subject=1, trial=1)

        # Use Fixed input
        skip_computed_for_each(
            process,
            inputs={"ref": Fixed(RawData, subject=1, trial=1), "value": 50},
            outputs=[ProcessedData],
            subject=[2],  # Different subject
            trial=[1],
        )

        # Check that output was created
        con = db._duck.con

        # Verify the record exists, then read provenance from the graph.
        record_check = con.execute("""
            SELECT record_id
            FROM _record
            WHERE type = 'ProcessedData'
        """).fetchone()
        assert record_check is not None, "No ProcessedData record found in _record"

        prov = db.get_provenance(ProcessedData, version=record_check[0])
        assert prov is not None, "No provenance recorded for ProcessedData"
        inputs = prov["inputs"]  # variable inputs only
        constants = prov["constants"]  # {param_name: value}

        # Fixed input 'ref' is a VARIABLE edge (pointing at the saved RawData),
        # not a constant — Fixed inputs are real upstream records.
        ref_entry = next((e for e in inputs if e["param_name"] == "ref"), None)
        assert ref_entry is not None, f"Missing 'ref' variable entry. Found: {inputs}"
        assert ref_entry.get("variable_type") == "RawData", (
            f"Expected variable_type='RawData', got {ref_entry.get('variable_type')}"
        )
        assert ref_entry.get("record_id") == ref_rid, (
            f"Expected record_id={ref_rid}, got {ref_entry.get('record_id')}"
        )

        # value is a literal constant.
        assert "value" in constants, (
            f"Constant 'value' should be present, got {constants}"
        )

        # 'ref' must NOT appear among constants.
        assert "ref" not in constants, (
            f"'ref' should not be a constant, got {constants}"
        )

    def test_fixed_input_staleness_detection(self, db):
        """Changing a Fixed input should cause skip_computed to re-run."""
        call_count = 0

        @scistack
        def use_fixed(ref, multiplier):
            nonlocal call_count
            call_count += 1
            return ref * multiplier

        # Initial run
        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)
        skip_computed_for_each(
            use_fixed,
            inputs={"ref": Fixed(RawData, subject=1, trial=1), "multiplier": 2},
            outputs=[ProcessedData],
            subject=[10],
            trial=[1],
            skip_computed=True,
        )
        assert call_count == 1

        # Re-run with same Fixed input - should skip
        skip_computed_for_each(
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
        skip_computed_for_each(
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

        @scistack
        def scale(x, factor):
            return x * factor

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)

        # Run with multiple factor values
        for factor in [1.0, 2.0, 3.0]:
            skip_computed_for_each(
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
            FROM _record
            WHERE type = 'ProcessedData'
        """).fetchone()[0]

        assert count == 3, f"Expected 3 variants, found {count}"

        # Each variant has a distinct 'factor' constant (graph-derived).
        variants = db.list_pipeline_variants(output_type="ProcessedData")
        factors = sorted(v["constants"]["factor"] for v in variants)
        assert factors == [1.0, 2.0, 3.0]

    def test_variant_query_consistency(self, db):
        """Variants are queryable via the bipartite graph (constant input edges)."""

        @scistack
        def compute(x, param):
            return x + param

        RawData.save(np.array([5]), subject=1, trial=1)

        skip_computed_for_each(
            compute,
            inputs={"x": RawData, "param": 10},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        con = db._duck.con

        # ProcessedData records whose producing invocation consumed a constant
        # bound to the 'param' slot.
        via_graph = con.execute("""
            SELECT DISTINCT io.output_record_id
            FROM _invocation_output io
            JOIN _record r ON r.record_id = io.output_record_id
            JOIN _invocation_input ii ON ii.invocation_id = io.invocation_id
            JOIN _constant c ON c.record_id = ii.input_record_id
            WHERE r.type = 'ProcessedData' AND ii.param_name = 'param'
        """).fetchall()
        assert len(via_graph) == 1

        # And the constant value is recoverable as a branch param.
        prov = db.get_provenance(ProcessedData, subject=1, trial=1)
        assert prov["constants"]["param"] == 10


class TestComparisonWithScidb:
    """Test that skip_computed and scidb outputs have similar metadata structure."""

    def test_metadata_structure_matches_scidb(self, db):
        """skip_computed outputs should have similar metadata to scidb outputs."""

        # Plain function for scidb
        def plain_process(x, threshold):
            return x * threshold

        # Lineage function for skip_computed
        @scistack
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

        # Run with skip_computed
        skip_computed_for_each(
            lineage_process,
            inputs={"x": RawData, "threshold": 2.0},
            outputs=[IntermediateB],
            subject=[2],
            trial=[1],
        )

        # Provenance from the graph for both paths.
        scidb_prov = db.get_provenance(IntermediateA, subject=1, trial=1)
        skip_prov = db.get_provenance(IntermediateB, subject=2, trial=1)

        # Both should record function + inputs + constants identically in shape.
        for prov in (scidb_prov, skip_prov):
            assert prov["function_name"]
            assert len(prov["function_hash"]) == 16
            assert {i["param_name"] for i in prov["inputs"]} == {"x"}

        # Constants should match (same threshold value).
        assert scidb_prov["constants"] == skip_prov["constants"]

    def test_branch_params_structure_matches_scidb(self, db):
        """branch_params structure should match between scidb and skip_computed."""

        # Multi-stage pipeline
        def plain_step1(x, p1):
            return x + p1

        def plain_step2(y, p2):
            return y * p2

        @scistack
        def lineage_step1(x, p1):
            return x + p1

        @scistack
        def lineage_step2(y, p2):
            return y * p2

        RawData.save(np.array([10]), subject=1, trial=1)
        RawData.save(np.array([10]), subject=2, trial=1)

        # scidb pipeline
        scidb.for_each(
            plain_step1,
            {"x": RawData, "p1": 5},
            [IntermediateA],
            subject=[1],
            trial=[1],
        )
        scidb.for_each(
            plain_step2,
            {"y": IntermediateA, "p2": 2},
            [ProcessedData],
            subject=[1],
            trial=[1],
        )

        # skip_computed pipeline
        skip_computed_for_each(
            lineage_step1,
            {"x": RawData, "p1": 5},
            [IntermediateB],
            subject=[2],
            trial=[1],
        )
        skip_computed_for_each(
            lineage_step2,
            {"y": IntermediateB, "p2": 2},
            [FinalResult],
            subject=[2],
            trial=[1],
        )

        # Get branch_params from final outputs (derived from the graph, §6)
        scidb_bp = derived_bp(db, "ProcessedData")
        skip_bp = derived_bp(db, "FinalResult")

        # Both should have accumulated upstream params
        # Note: function names differ (plain_step1 vs lineage_step1) but structure matches
        assert len(scidb_bp) == 2, "scidb should have 2 params"
        assert len(skip_bp) == 2, "skip_computed should have 2 params"

        # Check namespacing pattern (function.param)
        for bp in [scidb_bp, skip_bp]:
            keys = list(bp.keys())
            assert all("." in k for k in keys), "All params should be namespaced"


class TestMultipleOutputs:
    """Test that multiple outputs are handled correctly."""

    def test_multiple_outputs_all_have_metadata(self, db):
        """All outputs should have complete metadata."""

        @scistack
        def split_process(x, factor):
            return x * factor, x + factor

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)

        skip_computed_for_each(
            split_process,
            inputs={"x": RawData, "factor": 5},
            outputs=[ProcessedData, FinalResult],
            subject=[1],
            trial=[1],
        )

        # Both outputs should have complete graph provenance.
        for var_cls in (ProcessedData, FinalResult):
            prov = db.get_provenance(var_cls, subject=1, trial=1)
            assert prov is not None, f"Missing output {var_cls.__name__}"
            assert prov["function_name"] == "split_process"
            assert len(prov["function_hash"]) == 16
            assert {i["param_name"] for i in prov["inputs"]} == {"x"}
            assert prov["constants"]["factor"] == 5

    def test_multiple_outputs_same_branch_params(self, db):
        """All outputs from same call should have identical branch_params."""

        @scistack
        def dual_output(x, alpha, beta):
            return x * alpha, x + beta

        RawData.save(np.array([10, 20]), subject=1, trial=1)

        skip_computed_for_each(
            dual_output,
            inputs={"x": RawData, "alpha": 2.0, "beta": 5.0},
            outputs=[IntermediateA, IntermediateB],
            subject=[1],
            trial=[1],
        )

        # Get branch_params from both outputs (derived from the graph, §6)
        bp_a = derived_bp(db, "IntermediateA")
        bp_b = derived_bp(db, "IntermediateB")

        # Should be identical
        assert bp_a == bp_b


class TestGeneratesFile:
    """Test that generates_file functions work correctly."""

    def test_generates_file_has_metadata(self, db):
        """generates_file outputs are recorded lineage-only (no data row), with
        their function + constants captured in the bipartite graph."""
        from scidb.provenance import query as provenance_query

        @scistack(generates_file=True)
        def export_data(x, filename):
            # Side-effect only, no return value
            pass

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)

        skip_computed_for_each(
            export_data,
            inputs={"x": RawData, "filename": "output.csv"},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        con = db._duck.con

        # A record is created even though no data was saved.
        result = con.execute("""
            SELECT record_id, content_hash
            FROM _record
            WHERE type = 'ProcessedData'
        """).fetchone()

        assert result is not None, "generates_file should create record"
        record_id, content_hash = result

        # No data → no content_hash, and it is a lineage-only "generated:" record.
        assert content_hash is None
        assert record_id.startswith("generated:")

        # The producing invocation carries the function identity, and the
        # PathInput/filename rides as a graph constant → branch param.
        inv = provenance_query.producing_invocation(db._duck, record_id)
        assert inv is not None, "generates_file output must have a producing invocation"
        assert inv[1] == "export_data"

        branch_params = provenance_query.derived_branch_params(db._duck, record_id)
        assert branch_params.get("export_data.filename") == "output.csv", branch_params


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_no_constants(self, db):
        """Function with only variable inputs should have empty __constants."""

        @scistack
        def identity(x):
            return x

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)

        skip_computed_for_each(
            identity,
            inputs={"x": RawData},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        # No constants recorded on the producing invocation.
        prov = db.get_provenance(ProcessedData, subject=1, trial=1)
        assert prov["constants"] == {}

    def test_empty_branch_params_first_stage(self, db):
        """First pipeline stage should have only current function's params."""

        @scistack
        def first_stage(x, alpha):
            return x * alpha

        RawData.save(np.array([5]), subject=1, trial=1)

        skip_computed_for_each(
            first_stage,
            inputs={"x": RawData, "alpha": 3.0},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
        )

        # branch_params derived from the bipartite graph (§6)
        branch_params = derived_bp(db, "ProcessedData")

        # Should ONLY have current function's param (no upstream)
        assert len(branch_params) == 1
        assert "first_stage.alpha" in branch_params

    def test_dry_run_no_save(self, db):
        """dry_run should not save any outputs."""

        @scistack
        def compute(x, value):
            return x + value

        RawData.save(np.array([1]), subject=1, trial=1)

        skip_computed_for_each(
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
            FROM _record
            WHERE type = 'ProcessedData'
        """).fetchone()[0]

        assert count == 0, "dry_run should not save outputs"

    def test_where_clause_metadata(self, db):
        """The where= filter string is recorded on the producing run for visual
        inspection only (no matching logic reads it — see §10 where= redesign)."""

        @scistack
        def filter_process(x, threshold):
            return x * threshold

        RawData.save(np.array([1, 2]), subject=1, trial=1)
        RawData.save(np.array([3, 4]), subject=2, trial=1)

        skip_computed_for_each(
            filter_process,
            inputs={"x": RawData, "threshold": 2.0},
            outputs=[ProcessedData],
            subject=[],
            trial=[1],
            where="subject == 1",
        )

        rid = db._duck.con.execute(
            "SELECT record_id FROM _record WHERE type = 'ProcessedData'"
        ).fetchone()[0]
        # where_clause survives only as a display column on _run.
        wcs = {
            r[0]
            for r in db._duck.con.execute(
                "SELECT DISTINCT run.where_clause FROM _run run "
                "JOIN _run_invocation ri ON ri.run_id = run.run_id "
                "JOIN _invocation_output io ON io.invocation_id = ri.invocation_id "
                "WHERE io.output_record_id = ?",
                [rid],
            ).fetchall()
        }
        assert "subject == 1" in wcs


class TestSkipComputed:
    """Test that skip_computed works with unified variant tracking."""

    def test_skip_computed_with_constants(self, db):
        """skip_computed should work based on __constants in version_keys."""
        call_count = 0

        @scistack
        def expensive(x, param):
            nonlocal call_count
            call_count += 1
            return x * param

        RawData.save(np.array([1, 2, 3]), subject=1, trial=1)

        # First run
        skip_computed_for_each(
            expensive,
            inputs={"x": RawData, "param": 5},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
            skip_computed=True,
        )
        assert call_count == 1

        # Second run with same params - should skip
        skip_computed_for_each(
            expensive,
            inputs={"x": RawData, "param": 5},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
            skip_computed=True,
        )
        assert call_count == 1, "Should have skipped"

        # Third run with different param - should NOT skip
        skip_computed_for_each(
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
        @scistack
        def version1(x, factor):
            return x * factor  # Simple multiply

        skip_computed_for_each(
            version1,
            inputs={"x": RawData, "factor": 2},
            outputs=[ProcessedData],
            subject=[1],
            trial=[1],
            skip_computed=True,
        )

        con = db._duck.con

        # Second version with different implementation
        @scistack
        def version1(x, factor):
            return x * factor + 1  # Changed implementation

        skip_computed_for_each(
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
            FROM _record
            WHERE type = 'ProcessedData'
        """).fetchone()[0]

        assert count == 2, "Changed function should create new variant"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
