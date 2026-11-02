"""
Regression test to verify that both Python and MATLAB scidb.for_each() use batch save.

This test ensures that the batch save optimization (20-40x speedup) is actually
being used by both execution paths, and will catch any future regressions that
accidentally revert to sequential row-by-row saving.
"""

import hashlib
import logging

import numpy as np
import pandas as pd
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each

SCHEMA = ["subject", "trial"]


@pytest.fixture
def db(tmp_path):
    """Fresh database for each test."""
    _scifor.set_schema([])
    db = configure_database(tmp_path / "test_batch.duckdb", SCHEMA)
    yield db
    _scifor.set_schema([])
    db.close()


class Input(BaseVariable):
    pass


class Output(BaseVariable):
    pass


def process(data):
    """Simple function that doubles its input."""
    return data * 2


class TestBatchSaveRegression:
    """Verify both Python and MATLAB paths use batch save optimization."""

    def test_python_for_each_uses_batch_save(self, db, caplog):
        """Python for_each should use batch save when saving multiple records."""
        # Setup: Create input data for 3 subjects × 3 trials = 9 records
        for subj in ["S01", "S02", "S03"]:
            for trial in ["1", "2", "3"]:
                Input.save(np.array([1.0, 2.0]), subject=subj, trial=trial)

        # Run for_each (no distribute — just 9 combos saved in one batch)
        with caplog.at_level(logging.INFO, logger="scidb"):
            for_each(
                process,
                {"data": Input},
                [Output],
                subject=["S01", "S02", "S03"],
                trial=["1", "2", "3"],
            )

        # Verify batch save was used by checking logs
        batch_save_logs = [
            record.message
            for record in caplog.records
            if "[batch_save]" in record.message
        ]

        assert len(batch_save_logs) > 0, (
            "Expected [batch_save] log messages but found none. "
            "This indicates batch save optimization is not being used!"
        )

        # Verify we see the preparation and completion messages
        prep_logs = [log for log in batch_save_logs if "Preparing" in log]
        complete_logs = [log for log in batch_save_logs if "Completed" in log]

        assert len(prep_logs) > 0, "Should see batch_save preparation log"
        assert len(complete_logs) > 0, "Should see batch_save completion log"

        # Verify the batch contained multiple records (9 = 3 subjects × 3 trials)
        prep_log = prep_logs[0]
        assert "9 result row(s)" in prep_log, (
            f"Expected to batch 9 records, but log says: {prep_log}"
        )

        # Verify records were actually saved
        all_records = db.list_versions(Output)
        assert len(all_records) == 9, f"Expected 9 records but found {len(all_records)}"

    def test_matlab_bridge_uses_batch_save(self, db, caplog):
        """MATLAB bridge path should also use batch save (via _for_each_save_resolved)."""
        from scimatlab.bridge import for_each_prepare, for_each_save

        # Setup input data for 3 subjects
        for subj in ["S01", "S02", "S03"]:
            Input.save(np.array([1.0, 2.0]), subject=subj, trial="1")

        fn_hash = hashlib.sha256(b"def process(data): return data * 2").hexdigest()

        # Prepare (Phase 1 — MATLAB calls this)
        handle_result = for_each_prepare(
            fn_name="process",
            fn_hash=fn_hash,
            inputs_spec={"data": {"kind": "var_type", "type_name": "Input"}},
            output_class_names=["Output"],
            metadata_iterables={"subject": ["S01", "S02", "S03"], "trial": ["1"]},
        )

        # Build result DataFrames from the combos that for_each_prepare resolved.
        # In the real MATLAB flow, MATLAB's scifor.for_each produces this table.
        combos = handle_result["full_combos"]
        result_df = pd.DataFrame(combos)
        result_df["Output"] = [np.array([2.0, 4.0])] * len(combos)

        # Save (Phase 3 — MATLAB calls this with results)
        with caplog.at_level(logging.INFO, logger="scidb"):
            for_each_save(handle_result["handle"], result_df, save=True)

        # Verify batch save was used
        batch_save_logs = [
            record.message
            for record in caplog.records
            if "[batch_save]" in record.message
        ]

        assert len(batch_save_logs) > 0, (
            "MATLAB bridge path did not use batch save! "
            "This indicates the optimization is not working for MATLAB users."
        )

        # Verify the batch contained multiple records
        prep_logs = [log for log in batch_save_logs if "Preparing" in log]
        assert len(prep_logs) > 0, "Should see batch_save preparation log"

        prep_log = prep_logs[0]
        assert "3 result row(s)" in prep_log, (
            f"Expected to batch 3 records, but log says: {prep_log}"
        )

        # Verify records were saved
        all_records = db.list_versions(Output)
        assert len(all_records) == 3, f"Expected 3 records but found {len(all_records)}"

    def test_batch_save_preserves_branch_params(self, db):
        """Verify batch save correctly preserves branch_params for all records."""
        for subj in ["S01", "S02"]:
            Input.save(np.array([1.0]), subject=subj, trial="1")

        for_each(
            process,
            {"data": Input},
            [Output],
            subject=["S01", "S02"],
            trial=["1"],
        )

        # Verify all records have correct branch_params
        all_records = db.list_versions(Output)
        assert len(all_records) == 2, "Expected 2 records (2 subjects × 1 trial)"

        for record in all_records:
            bp = record.get("branch_params", {})
            assert isinstance(bp, dict), f"branch_params should be dict, got {type(bp)}"

    def test_batch_save_with_upstream_variants(self, db):
        """Verify batch save correctly handles records with different upstream variants."""

        class Intermediate(BaseVariable):
            pass

        def create_variants(data, param):
            return data * param

        # Create base data
        for subj in ["S01", "S02"]:
            Input.save(np.array([1.0]), subject=subj, trial="1")

        # Create upstream variants with different params
        for param in [10, 20]:
            for_each(
                create_variants,
                {"data": Input, "param": param},
                [Intermediate],
                subject=["S01", "S02"],
                trial=["1"],
            )

        # Process all upstream variants (2 subjects × 2 upstream variants = 4 records)
        for_each(
            process,
            {"data": Intermediate},
            [Output],
            subject=["S01", "S02"],
            trial=["1"],
        )

        # Should have 4 Output records (2 subjects × 2 upstream variants)
        all_records = db.list_versions(Output)
        assert len(all_records) == 4, f"Expected 4 records but found {len(all_records)}"

        # Verify branch_params include upstream variant info
        for record in all_records:
            bp = record.get("branch_params", {})
            # Should have create_variants.param in branch_params (either 10 or 20)
            assert "create_variants.param" in bp, (
                f"Missing upstream branch_params in {bp}"
            )
            assert bp["create_variants.param"] in [10, 20], (
                f"Invalid param value: {bp['create_variants.param']}"
            )

    def test_small_batches_still_work(self, db, caplog):
        """Verify batch save works correctly even with just 1 record."""
        Input.save(np.array([1.0]), subject="S01", trial="1")

        with caplog.at_level(logging.INFO, logger="scidb"):
            for_each(
                lambda data: data * 2,
                {"data": Input},
                [Output],
                subject=["S01"],
                trial=["1"],
            )

        # Should still see batch_save logs even for 1 record
        batch_save_logs = [
            record.message
            for record in caplog.records
            if "[batch_save]" in record.message
        ]
        assert len(batch_save_logs) > 0, "Batch save should be used even for 1 record"

        # Verify record was saved
        all_records = db.list_versions(Output)
        assert len(all_records) == 1


class TestBulkInsertProvenance:
    """Guard the vectorized provenance-insert path.

    The bipartite provenance tables (_record, _constant, _invocation,
    _invocation_input, _invocation_output, _run_invocation) used to be populated
    with ``con.executemany`` — DuckDB re-runs the single-row statement once per
    row, which against these PK/indexed tables cost ~497s for an 8k-record
    for_each (the `6c2_record_entities_insert` blowup). They now go through
    ``SciDuck.bulk_insert`` (one ``INSERT ... SELECT`` per table). These tests
    fail if any of them reverts to the per-row path.
    """

    def test_record_entities_use_bulk_insert(self, db, monkeypatch):
        """save_batch's _record insert must go through bulk_insert, not per-row."""
        from sciduckdb import SciDuck

        tables = []
        orig = SciDuck.bulk_insert

        def _spy(self, table, columns, rows, conflict_cols=None):
            tables.append(table)
            return orig(self, table, columns, rows, conflict_cols=conflict_cols)

        monkeypatch.setattr(SciDuck, "bulk_insert", _spy)

        for subj in ["S01", "S02", "S03"]:
            Input.save(np.array([1.0, 2.0]), subject=subj, trial="1")
        for_each(
            process,
            {"data": Input},
            [Output],
            subject=["S01", "S02", "S03"],
            trial=["1"],
        )

        # _record (entities) is the table whose per-row insert was the 497s
        # bottleneck; _invocation exercises the VARCHAR[] (as_table) list column.
        assert "_record" in tables, "save_batch did not bulk-insert _record entities"
        assert "_invocation" in tables, "graph commit did not bulk-insert _invocation"

    def test_no_per_row_executemany_in_provenance(self, db, monkeypatch):
        """No provenance table may be written via con.executemany (the slow path)."""
        from sciduckdb import SciDuck

        offenders = []
        orig = SciDuck._executemany

        def _spy(self, sql, params_list):
            offenders.append(sql)
            return orig(self, sql, params_list)

        monkeypatch.setattr(SciDuck, "_executemany", _spy)

        # Also catch direct con.executemany (bypassing the SciDuck wrapper).
        direct = []

        class _ConProxy:
            def __init__(self, con):
                self._con = con

            def executemany(self, sql, params_list):
                direct.append(sql)
                return self._con.executemany(sql, params_list)

            def __getattr__(self, name):
                return getattr(self._con, name)

        monkeypatch.setattr(db._duck, "con", _ConProxy(db._duck.con))

        for subj in ["S01", "S02", "S03"]:
            Input.save(np.array([1.0, 2.0]), subject=subj, trial="1")
        for_each(
            process,
            {"data": Input},
            [Output],
            subject=["S01", "S02", "S03"],
            trial=["1"],
        )

        all_executemany = offenders + direct
        assert not all_executemany, (
            f"Provenance/save path fell back to per-row executemany: {all_executemany}"
        )

    def test_bulk_insert_is_idempotent(self, db):
        """ON CONFLICT DO NOTHING is preserved: re-running yields no duplicates."""
        for subj in ["S01", "S02", "S03"]:
            Input.save(np.array([1.0, 2.0]), subject=subj, trial="1")

        kwargs = {"subject": ["S01", "S02", "S03"], "trial": ["1"]}
        for_each(process, {"data": Input}, [Output], **kwargs)
        first = len(db.list_versions(Output))
        # Re-run the identical computation — same content hashes → no new rows.
        for_each(process, {"data": Input}, [Output], **kwargs)
        second = len(db.list_versions(Output))

        assert first == 3, f"Expected 3 Output records, got {first}"
        assert second == first, (
            f"Re-running duplicated records ({first} -> {second}); "
            "ON CONFLICT DO NOTHING not preserved in bulk insert"
        )

    def test_bulk_insert_moderate_batch_correctness(self, db):
        """A few hundred records save correctly through the bulk path (types,
        None schema_id, list columns all round-trip)."""
        subjects = [f"S{i:03d}" for i in range(60)]
        for subj in subjects:
            Input.save(np.array([1.0, 2.0]), subject=subj, trial="1")

        for_each(
            process,
            {"data": Input},
            [Output],
            subject=subjects,
            trial=["1"],
        )

        all_records = db.list_versions(Output)
        assert len(all_records) == len(subjects), (
            f"Expected {len(subjects)} records, got {len(all_records)}"
        )


class TestBatchSavePerformance:
    """Performance characteristics of batch save (informational, not strict pass/fail)."""

    def test_batch_save_is_faster_than_sequential(self, db, caplog):
        """
        Informational test: batch save should be significantly faster than sequential.

        This test doesn't fail but prints timing information for monitoring performance.
        """
        import time

        # Create input data for 10 subjects
        for subj in [f"S{i:02d}" for i in range(10)]:
            Input.save(np.array([1.0]), subject=subj, trial="1")

        # Run for_each (10 subjects × 1 trial = 10 records)
        start_time = time.perf_counter()

        with caplog.at_level(logging.INFO, logger="scidb"):
            for_each(
                process,
                {"data": Input},
                [Output],
                subject=[f"S{i:02d}" for i in range(10)],
                trial=["1"],
            )

        elapsed_time = time.perf_counter() - start_time

        # Extract timing from logs
        timing_logs = [
            record.message for record in caplog.records if "records/s" in record.message
        ]

        print("\n=== Batch Save Performance ===")
        print(f"Saved 10 records in {elapsed_time:.3f}s")
        if timing_logs:
            print(f"Batch save throughput: {timing_logs[-1]}")

        # Verify all records were saved
        all_records = db.list_versions(Output)
        assert len(all_records) == 10

        # Informational assertion (prints warning but doesn't fail)
        if elapsed_time > 5.0:
            print(f"WARNING: Batch save took {elapsed_time:.3f}s, expected < 2s")
            print("This may indicate performance regression or slow test environment")
