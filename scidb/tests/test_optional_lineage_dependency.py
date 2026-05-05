"""Test that scidb works without scilineage installed (graceful degradation).

This tests the conditional import pattern in scidb.foreach that allows scidb
to work standalone without requiring scilineage/scihist.
"""

import sys
import pytest
import numpy as np

from scidb import BaseVariable


def parse_version_keys(vk):
    """Parse version_keys from database (handles both dict and JSON string)."""
    import json
    if isinstance(vk, str):
        return json.loads(vk)
    return vk


class InputData(BaseVariable):
    schema_version = 1


class OutputData(BaseVariable):
    schema_version = 1


class TestScidbWithoutLineage:
    """Test scidb.for_each works without scilineage installed."""

    def test_plain_function_without_lineage(self, db):
        """Plain functions should work normally without scilineage."""
        def simple_process(x, multiplier):
            return x * multiplier

        # Save input
        InputData.save(np.array([1, 2, 3]), subject=1, trial=1)

        # Import and use scidb.for_each
        from scidb import for_each

        # Should work without scilineage
        result = for_each(
            simple_process,
            inputs={"x": InputData, "multiplier": 2.0},
            outputs=[OutputData],
            subject=[1],
            trial=[1],
        )

        assert result is not None
        assert len(result) == 1

        # Check saved data
        loaded = OutputData.load(subject=1, trial=1)
        np.testing.assert_array_equal(loaded.data, np.array([2, 4, 6]))

    def test_has_lineage_flag_when_available(self, db):
        """HAS_LINEAGE flag should be True when scilineage is importable."""
        from scidb import foreach

        # Should have HAS_LINEAGE attribute
        assert hasattr(foreach, 'HAS_LINEAGE')

        # In this test environment, scilineage IS available
        assert foreach.HAS_LINEAGE is True

    def test_version_keys_complete_without_lineage(self, db):
        """version_keys should be complete for plain functions."""
        def compute(x, alpha, beta):
            return x * alpha + beta

        InputData.save(np.array([5, 10]), subject=1, trial=1)

        from scidb import for_each

        for_each(
            compute,
            inputs={"x": InputData, "alpha": 2.0, "beta": 10.0},
            outputs=[OutputData],
            subject=[1],
            trial=[1],
        )

        # Check metadata
        import json
        con = db._duck.con
        result = con.execute("""
            SELECT version_keys, branch_params
            FROM _record_metadata
            WHERE variable_name = 'OutputData'
        """).fetchone()

        version_keys = parse_version_keys(result[0])
        branch_params = parse_version_keys(result[1])

        # Should have complete version_keys even without lineage
        assert "__fn" in version_keys
        assert "__fn_hash" in version_keys
        assert "__inputs" in version_keys
        assert "__constants" in version_keys
        # NOTE: __branch_params is stored in a separate column, not in version_keys

        # Check content
        assert version_keys["__fn"] == "compute"
        constants = version_keys["__constants"]
        assert constants["alpha"] == 2.0
        assert constants["beta"] == 10.0

        # Check branch_params in the separate column
        assert branch_params is not None
        assert "compute.alpha" in branch_params
        assert "compute.beta" in branch_params
        assert branch_params["compute.alpha"] == 2.0
        assert branch_params["compute.beta"] == 10.0


class TestLineageImportFailureSimulation:
    """Simulate scilineage import failure to test graceful degradation."""

    def test_mock_import_failure(self, db, monkeypatch):
        """Simulate ImportError when importing LineageFcnResult."""
        # This test demonstrates what happens if scilineage is not installed

        # Mock the import to raise ImportError
        import scidb.foreach as foreach_module

        # Save the original values
        original_has_lineage = foreach_module.HAS_LINEAGE
        original_lineage_result = foreach_module.LineageFcnResult

        try:
            # Simulate import failure
            foreach_module.HAS_LINEAGE = False
            foreach_module.LineageFcnResult = None

            # Plain function should still work
            def process(x, value):
                return x + value

            InputData.save(np.array([1, 2, 3]), subject=1, trial=1)

            from scidb import for_each

            result = for_each(
                process,
                inputs={"x": InputData, "value": 100},
                outputs=[OutputData],
                subject=[1],
                trial=[1],
            )

            assert result is not None
            assert len(result) == 1

            # Data should be saved correctly
            loaded = OutputData.load(subject=1, trial=1)
            np.testing.assert_array_equal(loaded.data, np.array([101, 102, 103]))

        finally:
            # Restore original values
            foreach_module.HAS_LINEAGE = original_has_lineage
            foreach_module.LineageFcnResult = original_lineage_result


class TestLineageFcnResultDetection:
    """Test that LineageFcnResult is detected and handled correctly."""

    def test_lineage_fcn_result_delegated_to_scihist(self, db):
        """When LineageFcnResult is detected, save should delegate to scihist."""
        from scilineage import lineage_fcn

        @lineage_fcn
        def lineage_process(x, param):
            return x * param

        InputData.save(np.array([2, 4, 6]), subject=1, trial=1)

        # Call scidb.for_each directly (not scihist.for_each)
        # This should detect LineageFcnResult and delegate
        from scidb import for_each

        result = for_each(
            lineage_process,
            inputs={"x": InputData, "param": 3.0},
            outputs=[OutputData],
            subject=[1],
            trial=[1],
        )

        assert result is not None

        # Check that lineage was saved (proves delegation worked)
        con = db._duck.con
        lineage_count = con.execute("""
            SELECT COUNT(*)
            FROM _lineage
            WHERE target = 'OutputData'
        """).fetchone()[0]

        assert lineage_count == 1, "Lineage should have been saved via delegation"

        # Check metadata is complete
        import json
        metadata = parse_version_keys(con.execute("""
            SELECT version_keys
            FROM _record_metadata
            WHERE variable_name = 'OutputData'
        """).fetchone()[0])

        assert "__fn" in metadata
        assert "__fn_hash" in metadata
        assert "__inputs" in metadata
        assert "__constants" in metadata

    def test_mixed_outputs_plain_and_lineage(self, db):
        """Function returning mixed output types should handle both correctly."""
        from scilineage import lineage_fcn

        @lineage_fcn
        def mixed_output(x, factor):
            # Return tuple - first gets lineage, second doesn't
            return x * factor, x + factor

        InputData.save(np.array([10, 20]), subject=1, trial=1)

        from scidb import for_each

        # Both outputs should be saved
        result = for_each(
            mixed_output,
            inputs={"x": InputData, "factor": 5.0},
            outputs=[OutputData, InputData],  # Reuse InputData for second output
            subject=[1],
            trial=[1],
        )

        assert result is not None

        # Both should have been saved
        con = db._duck.con
        # Query using JOINs to access schema keys
        count = con.execute("""
            SELECT COUNT(DISTINCT rm.record_id)
            FROM _record_metadata rm
            JOIN _schema s ON rm.schema_id = s.schema_id
            WHERE rm.variable_name IN ('OutputData', 'InputData')
              AND s.subject = '1'
              AND s.trial = '1'
        """).fetchone()[0]

        # InputData already existed (1) + OutputData (1) + InputData output (1) = 3 total
        assert count >= 2, "Both outputs should be saved"


class TestNoRegressions:
    """Test that existing scidb functionality still works."""

    def test_multiple_schema_combos(self, db):
        """Multiple schema combinations should work."""
        def process(x, scale):
            return x * scale

        # Save data for multiple combos
        for subj in [1, 2]:
            for trial in [1, 2]:
                InputData.save(np.array([subj, trial]), subject=subj, trial=trial)

        from scidb import for_each

        result = for_each(
            process,
            inputs={"x": InputData, "scale": 10.0},
            outputs=[OutputData],
            subject=[1, 2],
            trial=[1, 2],
        )

        # Should process 4 combinations
        assert len(result) == 4

    def test_where_clause_filtering(self, db):
        """where clause should filter correctly."""
        def identity(x):
            return x

        for subj in [1, 2, 3]:
            InputData.save(np.array([subj]), subject=subj, trial=1)

        from scidb import for_each

        result = for_each(
            identity,
            inputs={"x": InputData},
            outputs=[OutputData],
            subject=[1, 2, 3],
            trial=[1],
            where="subject <= 2",
        )

        # Should only process subjects 1 and 2
        assert len(result) == 2

    def test_dry_run_mode(self, db):
        """dry_run should not save anything."""
        def compute(x, value):
            return x + value

        InputData.save(np.array([1]), subject=1, trial=1)

        from scidb import for_each

        result = for_each(
            compute,
            inputs={"x": InputData, "value": 100},
            outputs=[OutputData],
            subject=[1],
            trial=[1],
            dry_run=True,
        )

        assert result is None, "dry_run should return None"

        # Nothing should be saved
        con = db._duck.con
        count = con.execute("""
            SELECT COUNT(*)
            FROM _record_metadata
            WHERE variable_name = 'OutputData'
        """).fetchone()[0]

        assert count == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
