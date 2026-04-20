"""Integration test: MATLAB PathInput-only function with partial failure → grey.

Mirrors exactly the scenario captured in ``scidb.log``:

- A MATLAB function ``load_csv`` whose only input is a ``PathInput``
  (``sub{subject}/trial{trial}.csv``).
- The function declares 3 outputs: ``Time``, ``Force_Left``, ``Force_Right``.
- ``for_each`` iterates 3 subjects × 6 trials; the filesystem discovers 16
  combos (2 missing on disk) and persists all 16 to ``_for_each_expected``.
- 15 combos complete successfully and save records for all 3 outputs.
- 1 combo raises during execution (mirrors the ``Assertion failed`` skip in
  the log) and saves nothing.

After the run, the function node AND each of its 3 output variable nodes
must show state == ``"grey"`` (partial completion).

This simulates what MATLAB's ``scidb.for_each`` does on the Python side:
it calls :func:`_persist_expected_combos` before execution, then saves
each successful combo's outputs through the scilineage bridge.
"""

from __future__ import annotations

from hashlib import sha256

import numpy as np
import pytest

from scidb import BaseVariable
from scidb.foreach import _persist_expected_combos
from sci_matlab.bridge import (
    MatlabLineageFcn,
    MatlabLineageFcnInvocation,
    make_lineage_fcn_result,
    register_matlab_variable,
)
from scihist.foreach import save as scihist_save
from scihist.state import check_node_state


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

FN_NAME = "load_csv"
FN_SOURCE_HASH = sha256(b"function [time,force_left,force_right]=load_csv(filepath)").hexdigest()

SUBJECTS = ["01", "02", "03"]
TRIALS = ["01", "02", "03", "04", "05", "06"]

# Two combos have no file on disk — filesystem discovery drops them before
# the run starts. Matches "using 16 filesystem-discovered combos" in the log.
NOT_ON_DISK = {("03", "06"), ("02", "06")}

# One combo raises during execution — matches the "[skip] subject=01,
# trial=06: Assertion failed." line in the log.
FAILED_COMBO = ("01", "06")


def _discovered_combos() -> list[dict]:
    """Return the 16 (subject, trial) combos that exist on disk."""
    return [
        {"subject": s, "trial": t}
        for s in SUBJECTS
        for t in TRIALS
        if (s, t) not in NOT_ON_DISK
    ]


def _successful_combos() -> list[dict]:
    """Return the 15 combos that completed successfully (16 discovered minus 1 failed)."""
    return [c for c in _discovered_combos()
            if (c["subject"], c["trial"]) != FAILED_COMBO]


def _save_combo_outputs(db, fn_proxy, combo: dict, output_classes) -> None:
    """Simulate one successful MATLAB for_each iteration.

    Builds a ``MatlabLineageFcnInvocation`` (with the resolved filepath as
    the single argument, matching how sci-matlab invokes load_csv), wraps
    each of the 3 outputs in a ``LineageFcnResult`` via
    :func:`make_lineage_fcn_result`, and saves via ``scihist.foreach.save``
    so a lineage record is written for each output.
    """
    filepath = f"sub{combo['subject']}/trial{combo['trial']}.csv"
    invocation = MatlabLineageFcnInvocation(fn_proxy, {"arg_0": filepath})
    # Seed with the combo's schema keys so outputs differ per combo
    # (otherwise identical data would collapse content-hashes and hide bugs).
    seed = hash((combo["subject"], combo["trial"])) & 0xFFFF
    for idx, cls in enumerate(output_classes):
        data = np.arange(4, dtype=float) + seed + idx
        result = make_lineage_fcn_result(invocation, idx, data)
        scihist_save(cls, result, db=db, **combo)


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

class TestMatlabPathInputPartialRunGoesGrey:
    """Function + all 3 output variables should turn grey after 15/16 success."""

    @pytest.fixture
    def matlab_run(self, db):
        """Set up the post-run DB state described in scidb.log."""
        Time = register_matlab_variable("Time")
        Force_Left = register_matlab_variable("Force_Left")
        Force_Right = register_matlab_variable("Force_Right")
        outputs = [Time, Force_Left, Force_Right]

        # Step 1 — MATLAB scidb.for_each persists ALL 16 discovered combos
        # to _for_each_expected, before execution begins.
        _persist_expected_combos(db, FN_NAME, _discovered_combos())

        # Step 2 — MATLAB executes the function, saving outputs for every
        # successful combo. The failed combo produces no output.
        fn_proxy = MatlabLineageFcn(FN_SOURCE_HASH, FN_NAME, unpack_output=False)
        fn_proxy.__name__ = FN_NAME  # match _build_matlab_fn_proxy in the GUI
        for combo in _successful_combos():
            _save_combo_outputs(db, fn_proxy, combo, outputs)

        return {"fn": fn_proxy, "outputs": outputs}

    def test_function_state_is_grey(self, db, matlab_run):
        """Aggregate state across all 3 outputs: 15 up_to_date, 1 missing → grey."""
        result = check_node_state(
            matlab_run["fn"], matlab_run["outputs"], db=db,
        )
        assert result["state"] == "grey", (
            f"Expected grey (15/16 succeeded), got {result['state']}. "
            f"Counts: {result['counts']}"
        )
        assert result["counts"]["up_to_date"] == 15
        assert result["counts"]["missing"] == 1
        assert result["counts"]["stale"] == 0

    @pytest.mark.parametrize("output_name", ["Time", "Force_Left", "Force_Right"])
    def test_each_output_variable_is_grey(self, db, matlab_run, output_name):
        """Each individual output variable also reports grey.

        GUI-layer DAG propagation downgrades a variable to its producing
        function's state, but the scihist layer already reports grey per
        output when queried independently — verify that directly here.
        """
        cls = BaseVariable._all_subclasses[output_name]
        result = check_node_state(matlab_run["fn"], [cls], db=db)
        assert result["state"] == "grey", (
            f"Expected {output_name} grey, got {result['state']}. "
            f"Counts: {result['counts']}"
        )
        assert result["counts"]["up_to_date"] == 15
        assert result["counts"]["missing"] == 1

    def test_expected_combos_persisted(self, db, matlab_run):
        """Sanity: _for_each_expected has 16 rows for load_csv."""
        rows = db._duck._fetchall(
            "SELECT schema_id FROM _for_each_expected WHERE function_name = ?",
            [FN_NAME],
        )
        assert len(rows) == 16

    def test_lineage_records_written_for_successes(self, db, matlab_run):
        """Sanity: exactly 15 lineage rows per output variable."""
        for output_name in ("Time", "Force_Left", "Force_Right"):
            rows = db._duck._fetchall(
                "SELECT COUNT(*) FROM _lineage l "
                "JOIN _record_metadata rm ON l.output_record_id = rm.record_id "
                "WHERE rm.variable_name = ? AND l.function_name = ?",
                [output_name, FN_NAME],
            )
            assert rows[0][0] == 15, (
                f"Expected 15 lineage rows for {output_name}, got {rows[0][0]}"
            )

    def test_grey_goes_green_after_fix_and_rerun(self, db, matlab_run):
        """Full workflow: grey → fix → re-run the failing combo → green.

        Starts from the partial-run state set up by ``matlab_run`` (15/16
        success → grey).  Then simulates the user fixing the MATLAB
        function so the previously-failing combo (``subject=01,
        trial=06``) succeeds on a second run.  The function source is
        unchanged (same hash) because the fix is external to the function
        — e.g. a repaired input file — so the existing 15 records remain
        up_to_date.  After saving the final combo, every node (the
        function and all 3 output variables) must be green.
        """
        fn_proxy = matlab_run["fn"]
        outputs = matlab_run["outputs"]

        # Sanity: confirm the starting state is grey.
        before = check_node_state(fn_proxy, outputs, db=db)
        assert before["state"] == "grey"
        assert before["counts"]["missing"] == 1

        # Pretend the fix lets the previously-failing combo run and save.
        failed = {"subject": FAILED_COMBO[0], "trial": FAILED_COMBO[1]}
        _save_combo_outputs(db, fn_proxy, failed, outputs)

        # Aggregate state across all 3 outputs: 16 up_to_date → green.
        after = check_node_state(fn_proxy, outputs, db=db)
        assert after["state"] == "green", (
            f"Expected green after fix, got {after['state']}. "
            f"Counts: {after['counts']}"
        )
        assert after["counts"]["up_to_date"] == 16
        assert after["counts"]["missing"] == 0
        assert after["counts"]["stale"] == 0

        # Each individual output variable must also be green.
        for cls in outputs:
            per_var = check_node_state(fn_proxy, [cls], db=db)
            assert per_var["state"] == "green", (
                f"Expected {cls.__name__} green, got {per_var['state']}. "
                f"Counts: {per_var['counts']}"
            )
            assert per_var["counts"]["up_to_date"] == 16
            assert per_var["counts"]["missing"] == 0
