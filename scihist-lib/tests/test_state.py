"""Tests for scihist.state: check_combo_state and check_node_state."""

import numpy as np
import pytest

from scidb import BaseVariable, for_each as scidb_for_each
from scilineage import lineage_fcn
from scihist import for_each
from scihist.state import check_combo_state, check_node_state

from conftest import DEFAULT_TEST_SCHEMA_KEYS  # ["subject", "trial"]


# ---------------------------------------------------------------------------
# Variable types — defined at module level for BaseVariable registry
# ---------------------------------------------------------------------------

class RawState(BaseVariable):
    schema_version = 1

class ProcessedState(BaseVariable):
    schema_version = 1

class SecondaryState(BaseVariable):
    schema_version = 1


# ---------------------------------------------------------------------------
# Pipeline functions
# ---------------------------------------------------------------------------

@lineage_fcn
def process_data(raw):
    return np.asarray(raw, dtype=float) * 2.0

@lineage_fcn
def second_step(processed):
    return np.asarray(processed, dtype=float) + 1.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_raw(db, subjects=(1, 2), trials=("A", "B")):
    for subj in subjects:
        for trial in trials:
            RawState.save(np.random.randn(5), subject=subj, trial=trial)


def _run_all(db, subjects=(1, 2), trials=("A", "B")):
    for_each(
        process_data,
        inputs={"raw": RawState},
        outputs=[ProcessedState],
        subject=list(subjects),
        trial=list(trials),
    )


# ---------------------------------------------------------------------------
# check_combo_state
# ---------------------------------------------------------------------------

class TestCheckComboState:
    def test_missing_when_no_output(self, db):
        _seed_raw(db)
        state = check_combo_state(
            process_data, [ProcessedState],
            {"subject": 1, "trial": "A"}, db=db,
        )
        assert state == "missing"

    def test_up_to_date_after_full_run(self, db):
        _seed_raw(db)
        _run_all(db)
        state = check_combo_state(
            process_data, [ProcessedState],
            {"subject": 1, "trial": "A"}, db=db,
        )
        assert state == "up_to_date"

    def test_stale_when_upstream_input_updated(self, db):
        _seed_raw(db)
        _run_all(db)
        # Overwrite subject=1/trial=A raw input → should make output stale
        RawState.save(np.ones(5) * 99, subject=1, trial="A")
        state = check_combo_state(
            process_data, [ProcessedState],
            {"subject": 1, "trial": "A"}, db=db,
        )
        assert state == "stale"

    def test_stale_when_function_hash_changed(self, db):
        _seed_raw(db)
        _run_all(db)

        # Simulate a function-hash change by creating a new LineageFcn wrapping
        # a different function body.
        @lineage_fcn
        def process_data_v2(raw):  # different bytecode → different hash
            return np.asarray(raw, dtype=float) * 3.0

        process_data_v2.__name__ = "process_data"  # same name, different hash

        state = check_combo_state(
            process_data_v2, [ProcessedState],
            {"subject": 1, "trial": "A"}, db=db,
        )
        assert state == "stale"

    def test_missing_for_unknown_combo(self, db):
        _seed_raw(db)
        _run_all(db)
        # subject=99 was never seeded or run
        state = check_combo_state(
            process_data, [ProcessedState],
            {"subject": 99, "trial": "A"}, db=db,
        )
        assert state == "missing"


# ---------------------------------------------------------------------------
# check_node_state
# ---------------------------------------------------------------------------

class TestCheckNodeState:
    def test_red_when_never_run(self, db):
        _seed_raw(db)
        # for_each never called — no output records
        result = check_node_state(process_data, [ProcessedState], db=db)
        assert result["state"] == "red"
        assert result["counts"]["up_to_date"] == 0
        assert result["counts"]["missing"] == 4  # 2 subjects × 2 trials

    def test_green_when_all_combos_up_to_date(self, db):
        _seed_raw(db)
        _run_all(db)
        result = check_node_state(process_data, [ProcessedState], db=db)
        assert result["state"] == "green"
        assert result["counts"]["up_to_date"] == 4
        assert result["counts"]["missing"] == 0
        assert result["counts"]["stale"] == 0

    def test_grey_when_partial_run(self, db):
        _seed_raw(db)
        # Only run for subject=1 → 2 of 4 combos executed
        for_each(
            process_data,
            inputs={"raw": RawState},
            outputs=[ProcessedState],
            subject=[1],
            trial=["A", "B"],
        )
        result = check_node_state(process_data, [ProcessedState], db=db)
        assert result["state"] == "grey"
        assert result["counts"]["up_to_date"] == 2
        assert result["counts"]["missing"] == 2

    def test_red_when_any_combo_stale(self, db):
        _seed_raw(db)
        _run_all(db)
        # Update one input → makes its output stale
        RawState.save(np.ones(5) * 99, subject=1, trial="A")
        result = check_node_state(process_data, [ProcessedState], db=db)
        assert result["state"] == "red"
        assert result["counts"]["stale"] >= 1

    def test_combos_list_has_schema_info(self, db):
        _seed_raw(db)
        _run_all(db)
        result = check_node_state(process_data, [ProcessedState], db=db)
        assert len(result["combos"]) == 4
        for combo in result["combos"]:
            assert "schema_combo" in combo
            assert "subject" in combo["schema_combo"]
            assert "trial" in combo["schema_combo"]
            assert combo["state"] == "up_to_date"

    def test_red_when_no_input_data_and_never_run(self, db):
        # Completely empty DB — no raw data, nothing run
        result = check_node_state(process_data, [ProcessedState], db=db)
        assert result["state"] == "red"
        assert result["counts"] == {"up_to_date": 0, "stale": 0, "missing": 0}

    def test_two_step_chain_downstream_state(self, db):
        """process_data partially run → second_step combos depend on what exists."""
        _seed_raw(db)
        # Step 1: partial run
        for_each(
            process_data,
            inputs={"raw": RawState},
            outputs=[ProcessedState],
            subject=[1],
            trial=["A", "B"],
        )
        # Step 2: run for all available ProcessedState records
        for_each(
            second_step,
            inputs={"processed": ProcessedState},
            outputs=[SecondaryState],
            subject=[1],
            trial=["A", "B"],
        )
        # process_data: grey (2 of 4 combos)
        upstream = check_node_state(process_data, [ProcessedState], db=db)
        assert upstream["state"] == "grey"

        # second_step own state: green (ran for all available ProcessedState)
        # Note: scihist.check_node_state does NOT propagate upstream staleness —
        # that is the GUI layer's responsibility. The own state reflects only
        # what second_step itself has done.
        downstream = check_node_state(second_step, [SecondaryState], db=db)
        assert downstream["state"] == "green"
        assert downstream["counts"]["up_to_date"] == 2
        # second_step only sees 2 ProcessedState records as inputs, so 2 expected
        assert downstream["counts"]["missing"] == 0


# ---------------------------------------------------------------------------
# Fallback path: scidb.for_each (no lineage, uses __fn_hash + timestamps)
# ---------------------------------------------------------------------------

class ScidbRaw(BaseVariable):
    schema_version = 1

class ScidbProcessed(BaseVariable):
    schema_version = 1


def scidb_process(raw):
    """Plain function (not a LineageFcn) for scidb.for_each tests."""
    return np.asarray(raw, dtype=float) * 3.0


class TestFnHashFallback:
    """check_combo_state fallback when output was saved via scidb.for_each.

    scidb.for_each writes __fn_hash into version_keys but no _lineage row.
    Function staleness uses __fn_hash; input freshness uses timestamps.
    """

    def test_up_to_date_after_scidb_run(self, db):
        for subj in [1, 2]:
            ScidbRaw.save(np.random.randn(5), subject=subj, trial="A")
        scidb_for_each(
            scidb_process,
            inputs={"raw": ScidbRaw},
            outputs=[ScidbProcessed],
            subject=[1, 2],
            trial=["A"],
        )
        state = check_combo_state(
            scidb_process, [ScidbProcessed],
            {"subject": 1, "trial": "A"}, db=db,
        )
        assert state == "up_to_date"

    def test_stale_when_input_re_saved_after_output(self, db):
        ScidbRaw.save(np.random.randn(5), subject=1, trial="A")
        scidb_for_each(
            scidb_process,
            inputs={"raw": ScidbRaw},
            outputs=[ScidbProcessed],
            subject=[1],
            trial=["A"],
        )
        # Re-save raw input after output was produced → timestamp now newer
        ScidbRaw.save(np.ones(5) * 42, subject=1, trial="A")
        state = check_combo_state(
            scidb_process, [ScidbProcessed],
            {"subject": 1, "trial": "A"}, db=db,
        )
        assert state == "stale"

    def test_stale_when_fn_hash_changed(self, db):
        ScidbRaw.save(np.random.randn(5), subject=1, trial="A")
        scidb_for_each(
            scidb_process,
            inputs={"raw": ScidbRaw},
            outputs=[ScidbProcessed],
            subject=[1],
            trial=["A"],
        )

        def scidb_process_v2(raw):  # different body → different __fn_hash
            return np.asarray(raw, dtype=float) * 99.0

        state = check_combo_state(
            scidb_process_v2, [ScidbProcessed],
            {"subject": 1, "trial": "A"}, db=db,
        )
        assert state == "stale"

    def test_up_to_date_with_constants(self, db):
        """check_node_state returns green after for_each run with constants.

        Regression test: branch_params keys are namespaced as "fn.param" but
        version_keys stores them un-namespaced. find_record_id must route them
        through the branch_params_filter path (suffix matching), not the
        version_keys filter path (which would fail to find the record).
        """
        class ScidbRawConst(BaseVariable):
            schema_version = 1

        class ScidbProcessedConst(BaseVariable):
            schema_version = 1

        def scidb_process_with_scale(raw, scale):
            return np.asarray(raw, dtype=float) * float(scale)

        for subj in [1, 2]:
            ScidbRawConst.save(np.random.randn(5), subject=subj, trial="A")

        scidb_for_each(
            scidb_process_with_scale,
            inputs={"raw": ScidbRawConst, "scale": 2.0},
            outputs=[ScidbProcessedConst],
            subject=[1, 2],
            trial=["A"],
        )

        result = check_node_state(scidb_process_with_scale, [ScidbProcessedConst], db=db)
        assert result["state"] == "green", (
            f"Expected green after full run with constants, got {result['state']}. "
            f"Counts: {result['counts']}"
        )
