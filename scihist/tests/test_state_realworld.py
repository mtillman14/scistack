"""Real-world scenario tests: scihist.state with CSV data.

Mirrors the aim2 pipeline: a function reading CSV files iterated across
3 subjects × 5 trials (15 combos total). Key scenarios not covered by
the existing contrived tests:

- Realistic dataset scale (3×5 vs 2×2)
- Function errors mid-run for some combos (failing during execution, not
  omitting combos from the iteration range). For a PathInput loader the
  un-run combos leave no trace, so node state stays green (binary model);
  only a never-run loader is red.
- All combos fail → red
- Multi-output functions (3 outputs like load_csv.m returning
  time, force_left, force_right)
"""

import shutil
from pathlib import Path

import numpy as np
import pandas as pd
from scihist.state import check_node_state

from scidb import BaseVariable, scistack
from scifor import PathInput
from scihist import for_each

# The tests' own fixture: sub{01..03}/trial{01..05}.csv (time, force_left,
# force_right). It lived in examples/aim2/data until 1ab0ba51 reorganised the
# example dataset and silently broke these suites -- test data is not example
# data.
DATA_DIR = Path(__file__).parent / "data" / "state_trials"
SUBJECTS = ["01", "02", "03"]
TRIALS = ["01", "02", "03", "04", "05"]


# ---------------------------------------------------------------------------
# Variable types — module-level for BaseVariable registry
# ---------------------------------------------------------------------------


class RwTime(BaseVariable):
    schema_version = 1


class RwForceLeft(BaseVariable):
    schema_version = 1


class RwForceRight(BaseVariable):
    schema_version = 1


class RwPeakForce(BaseVariable):
    schema_version = 1


# ---------------------------------------------------------------------------
# Pipeline functions
# ---------------------------------------------------------------------------


@scistack
def load_time(filepath):
    """Python equivalent of load_csv.m — returns time column."""
    df = pd.read_csv(filepath)
    return df["time"].values


@scistack
def load_force_left(filepath):
    df = pd.read_csv(filepath)
    return df["force_left"].values


@scistack
def load_force_right(filepath):
    df = pd.read_csv(filepath)
    return df["force_right"].values


@scistack
def compute_peak(force_left, force_right):
    """Downstream function: peak combined force."""
    return float(np.max(np.abs(np.asarray(force_left) + np.asarray(force_right))))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_forces(db, subjects=SUBJECTS, trials=TRIALS):
    """Seed ForceLeft and ForceRight records from real CSV data."""
    for subj in subjects:
        for trial in trials:
            path = DATA_DIR / f"sub{subj}" / f"trial{trial}.csv"
            df = pd.read_csv(path)
            RwForceLeft.save(df["force_left"].values, subject=subj, trial=trial, db=db)
            RwForceRight.save(
                df["force_right"].values, subject=subj, trial=trial, db=db
            )


def _path_input():
    return PathInput(
        "sub{subject}/trial{trial}.csv",
        root_folder=str(DATA_DIR), name="sub{subject}/trial{trial}.csv",
    )


# ---------------------------------------------------------------------------
# Tests: PathInput + real CSV data, single output
# ---------------------------------------------------------------------------


class TestCsvLoadNodeState:
    """check_node_state with PathInput over real 3-subject × 5-trial CSV data."""

    def test_green_after_full_run(self, db):
        for_each(
            load_time,
            inputs={"filepath": _path_input()},
            outputs=[RwTime],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        result = check_node_state(load_time, [RwTime], db=db)
        assert result["state"] == "green"
        assert result["counts"]["up_to_date"] == 15
        assert result["counts"]["missing"] == 0
        assert result["counts"]["stale"] == 0

    def test_green_when_one_combo_errors_loader_cannot_detect(self, db, tmp_path):
        """A PathInput loader stays green when one combo's file is missing.

        Copies all files except sub03/trial05, so that combo raises during
        execution while the other 14 succeed. A loader has no DB input to
        enumerate, so the un-run 15th combo leaves no trace — the node cannot
        detect it and reads green. (This is the accepted limitation of the
        binary model after dropping the persisted expected-combo snapshot.)
        """
        for subj in SUBJECTS:
            (tmp_path / f"sub{subj}").mkdir()
            for trial in TRIALS:
                src = DATA_DIR / f"sub{subj}" / f"trial{trial}.csv"
                dst = tmp_path / f"sub{subj}" / f"trial{trial}.csv"
                if subj == "03" and trial == "05":
                    continue  # leave this one missing → FileNotFoundError
                shutil.copy(src, dst)

        pi = PathInput("sub{subject}/trial{trial}.csv", root_folder=str(tmp_path), name="sub{subject}/trial{trial}.csv")
        for_each(
            load_time,
            inputs={"filepath": pi},
            outputs=[RwTime],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )

        result = check_node_state(load_time, [RwTime], db=db)
        assert result["state"] == "green", (
            f"Loader reads green even on partial run, got {result['state']}. "
            f"Counts: {result['counts']}"
        )
        assert result["counts"]["up_to_date"] == 14
        assert result["counts"]["missing"] == 0

    def test_green_when_multiple_combos_error_loader_cannot_detect(self, db, tmp_path):
        """A PathInput loader stays green even when several files are missing —
        the un-run combos leave no trace to count as missing."""
        missing = {("01", "03"), ("02", "04"), ("03", "01")}
        for subj in SUBJECTS:
            (tmp_path / f"sub{subj}").mkdir()
            for trial in TRIALS:
                if (subj, trial) not in missing:
                    shutil.copy(
                        DATA_DIR / f"sub{subj}" / f"trial{trial}.csv",
                        tmp_path / f"sub{subj}" / f"trial{trial}.csv",
                    )

        pi = PathInput("sub{subject}/trial{trial}.csv", root_folder=str(tmp_path), name="sub{subject}/trial{trial}.csv")
        for_each(
            load_time,
            inputs={"filepath": pi},
            outputs=[RwTime],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )

        result = check_node_state(load_time, [RwTime], db=db)
        assert result["state"] == "green"
        assert result["counts"]["up_to_date"] == 12
        assert result["counts"]["missing"] == 0

    def test_red_when_all_combos_error(self, db, tmp_path):
        """Red when no files exist — every combo raises, nothing is saved, so the
        loader has no realized invocations at all."""
        # tmp_path is empty — all 15 combos will fail
        for subj in SUBJECTS:
            (tmp_path / f"sub{subj}").mkdir()

        pi = PathInput("sub{subject}/trial{trial}.csv", root_folder=str(tmp_path), name="sub{subject}/trial{trial}.csv")
        for_each(
            load_time,
            inputs={"filepath": pi},
            outputs=[RwTime],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )

        result = check_node_state(load_time, [RwTime], db=db)
        assert result["state"] == "red"
        assert result["counts"]["up_to_date"] == 0
        assert result["counts"]["missing"] == 0


# ---------------------------------------------------------------------------
# Tests: multi-output (3 outputs, like load_csv.m)
# ---------------------------------------------------------------------------


class TestMultiOutputNodeState:
    """check_node_state with separate functions for each of load_csv.m's 3 outputs.

    load_csv.m returns [time, force_left, force_right].  In Python we model
    this as three separate @scistack functions, one per output.  The state
    of each output type is independent. These are PathInput loaders, so a
    partial run is invisible to node state (stays green); only a never-run
    loader is red.
    """

    def test_green_all_three_outputs_after_full_run(self, db):
        for_each(
            load_force_left,
            inputs={"filepath": _path_input()},
            outputs=[RwForceLeft],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        for_each(
            load_force_right,
            inputs={"filepath": _path_input()},
            outputs=[RwForceRight],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )

        left_result = check_node_state(load_force_left, [RwForceLeft], db=db)
        right_result = check_node_state(load_force_right, [RwForceRight], db=db)

        assert left_result["state"] == "green"
        assert right_result["state"] == "green"
        assert left_result["counts"]["up_to_date"] == 15
        assert right_result["counts"]["up_to_date"] == 15

    def test_green_one_output_when_partial_failure_loader_cannot_detect(
        self, db, tmp_path
    ):
        """When one file is missing, the loader outputs still read green — a
        PathInput loader cannot detect the un-run combo (no DB input to
        enumerate), so partial completion is invisible to node state."""
        missing = {("02", "03")}
        for subj in SUBJECTS:
            (tmp_path / f"sub{subj}").mkdir()
            for trial in TRIALS:
                if (subj, trial) not in missing:
                    shutil.copy(
                        DATA_DIR / f"sub{subj}" / f"trial{trial}.csv",
                        tmp_path / f"sub{subj}" / f"trial{trial}.csv",
                    )

        pi = PathInput("sub{subject}/trial{trial}.csv", root_folder=str(tmp_path), name="sub{subject}/trial{trial}.csv")
        for_each(
            load_force_left,
            inputs={"filepath": pi},
            outputs=[RwForceLeft],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        for_each(
            load_force_right,
            inputs={"filepath": pi},
            outputs=[RwForceRight],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )

        left_result = check_node_state(load_force_left, [RwForceLeft], db=db)
        right_result = check_node_state(load_force_right, [RwForceRight], db=db)

        assert left_result["state"] == "green"
        assert right_result["state"] == "green"
        assert left_result["counts"]["up_to_date"] == 14
        assert left_result["counts"]["missing"] == 0


# ---------------------------------------------------------------------------
# Tests: downstream function after partial upstream run
# ---------------------------------------------------------------------------


class TestDownstreamStateAfterPartialUpstream:
    """check_node_state for a downstream function when upstream has missing combos.

    compute_peak(force_left, force_right) depends on RwForceLeft and
    RwForceRight.  When new upstream combos exist that it has not processed,
    its own state is red (those expected invocations are missing). Any further
    propagation of upstream needs-run state is the GUI layer's concern.
    """

    def test_downstream_green_for_available_combos(self, db):
        """compute_peak runs successfully for all seeded combos."""
        _seed_forces(db)
        for_each(
            compute_peak,
            inputs={"force_left": RwForceLeft, "force_right": RwForceRight},
            outputs=[RwPeakForce],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        result = check_node_state(compute_peak, [RwPeakForce], db=db)
        assert result["state"] == "green"
        assert result["counts"]["up_to_date"] == 15

    def test_downstream_red_when_upstream_partially_seeded(self, db):
        """compute_peak is red when upstream only covers 2 of 3 subjects.

        Only sub01 and sub02 have force data seeded; sub03 data is absent.
        compute_peak ran for what was available (10 combos).  Expected
        combos are inferred from available RwForceLeft records, so 5
        sub03 combos are missing → red (binary model).
        """
        _seed_forces(db, subjects=["01", "02"])  # sub03 intentionally absent
        for_each(
            compute_peak,
            inputs={"force_left": RwForceLeft, "force_right": RwForceRight},
            outputs=[RwPeakForce],
            subject=["01", "02"],
            trial=TRIALS,
            db=db,
        )

        # Now seed sub03 data (simulates new data arriving after first run)
        _seed_forces(db, subjects=["03"])

        result = check_node_state(compute_peak, [RwPeakForce], db=db)
        assert result["state"] == "red", (
            f"Expected red (sub03 force data added after run), "
            f"got {result['state']}. Counts: {result['counts']}"
        )
        assert result["counts"]["up_to_date"] == 10
        assert result["counts"]["missing"] == 5

    def test_downstream_needs_rerun_when_upstream_updated(self, db):
        """Re-saving an upstream force record → that combo's expected invocation
        shifts to the new input record → needs-run (red).

        Membership model (§9c): a changed input surfaces as needs-run for the
        affected combo; the other 14 stay up_to_date, but any missing makes the
        node red (binary model). The superseded old input is NOT counted (latest-
        record selection), so up_to_date is 14, not 15.
        """
        _seed_forces(db)
        for_each(
            compute_peak,
            inputs={"force_left": RwForceLeft, "force_right": RwForceRight},
            outputs=[RwPeakForce],
            subject=SUBJECTS,
            trial=TRIALS,
            db=db,
        )
        # Re-save one upstream record → its combo's expected invocation is absent.
        path = DATA_DIR / "sub01" / "trial01.csv"
        df = pd.read_csv(path)
        RwForceLeft.save(df["force_left"].values * 2.0, subject="01", trial="01", db=db)

        result = check_node_state(compute_peak, [RwPeakForce], db=db)
        assert result["state"] == "red"
        assert result["counts"]["up_to_date"] == 14
        assert result["counts"]["missing"] == 1
        assert result["counts"]["stale"] == 0
