"""
Run the gait symmetry example pipeline end to end.

    cd examples/aim2
    python src/cycles/run_pipeline.py

Each step is one ``for_each`` call. Steps 1-2 load the four levels of data;
steps 3-9 process it, and several of them deliberately combine inputs
recorded at DIFFERENT levels — a per-cycle measure with its trial's speed
and its subject's height, for instance — which is the realistic case. See
``pipeline.py`` for what each step demonstrates.
"""

from __future__ import annotations

from pathlib import Path

import scidb
from scidb import Fixed

from pipeline import (  # noqa: E402 — same folder, run as a script
    ASYMMETRY_THRESHOLD,
    DEMOGRAPHICS_FILE,
    SCALE,
    SESSION_FILE,
    COUPLING_FILE,
    SYMMETRY_FILE,
    TRIAL_FILE,
    WAVEFORM_FILE,
    AnkleOverThreshold,
    CycleDeviation,
    CycleSymmetry,
    CycleWaveform,
    Demographics,
    JointCoupling,
    KneeExcursion,
    NormalizedKnee,
    ScaledTrialSymmetry,
    SessionInfo,
    SpeedChangeFromBaseline,
    SubjectProfile,
    TrialCadence,
    TrialInfo,
    TrialMeanSymmetry,
    ankle_over_threshold,
    cycle_deviation,
    knee_excursion,
    load_cycle_symmetry,
    load_cycle_waveform,
    load_demographics,
    load_joint_coupling,
    load_session_info,
    load_trial_info,
    normalized_knee,
    scale_joint,
    speed_change_from_baseline,
    subject_profile,
    trial_cadence,
    trial_mean_symmetry,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ["subject", "session", "speed", "trial", "cycle"]


def main(db_path=None, subjects=(), sessions=()) -> None:
    """Run every step.

    ``subjects`` / ``sessions`` restrict the run to part of the dataset;
    empty (the default) means "whatever is on disk", which is how the
    example is meant to be run. The integration suite passes a subset so a
    test session does not pay for all 720 cycles.
    """
    scidb.configure_database(db_path or PROJECT_ROOT / "aim2.duckdb", SCHEMA)
    subject = list(subjects)
    session = list(sessions)

    # --- load, one call per level ------------------------------------------
    # Empty lists mean "whatever is on disk": each PathInput discovers the
    # keys its own template names, so the level of the records follows from
    # the file layout rather than from a flag.

    # 1a. Cycle level: subject x session x speed x trial x cycle (720 files).
    scidb.for_each(
        load_cycle_symmetry,
        inputs={"csv_file_path": SYMMETRY_FILE},
        outputs=[CycleSymmetry],
        subject=subject,
        session=session,
        speed=[],
        trial=[],
        cycle=[],
    )

    # 1a(ii). Cycle level, 1-D: one curve per joint, same 720 combinations.
    scidb.for_each(
        load_cycle_waveform,
        inputs={"csv_file_path": WAVEFORM_FILE},
        outputs=[CycleWaveform],
        subject=subject,
        session=session,
        speed=[],
        trial=[],
        cycle=[],
    )

    # 1b. Trial level: subject x session x speed x trial (72 files).
    scidb.for_each(
        load_trial_info,
        inputs={"csv_file_path": TRIAL_FILE},
        outputs=[TrialInfo],
        subject=subject,
        session=session,
        speed=[],
        trial=[],
    )

    # 1b(ii). Trial level, 2-D: one 3 x 3 matrix per trial (72 files).
    scidb.for_each(
        load_joint_coupling,
        inputs={"csv_file_path": COUPLING_FILE},
        outputs=[JointCoupling],
        subject=subject,
        session=session,
        speed=[],
        trial=[],
    )

    # 1c. Session level: subject x session (12 files).
    scidb.for_each(
        load_session_info,
        inputs={"csv_file_path": SESSION_FILE},
        outputs=[SessionInfo],
        subject=subject,
        session=session,
    )

    # 1d. Subject level: one file each (3 files).
    scidb.for_each(
        load_demographics,
        inputs={"csv_file_path": DEMOGRAPHICS_FILE},
        outputs=[Demographics],
        subject=subject,
    )

    # --- process ------------------------------------------------------------

    # 2. One column of the cycle table, plus a parameter with two values — so
    #    this runs twice per cycle and stores two variants of one variable.
    scidb.for_each(
        ankle_over_threshold,
        inputs={"ankle": CycleSymmetry["ankle"], "threshold": ASYMMETRY_THRESHOLD},
        outputs=[AnkleOverThreshold],
        subject=subject,
        session=session,
        speed=[],
        trial=[],
        cycle=[],
    )

    # 3. THREE LEVELS IN ONE CALL. Iterating down to `cycle` makes the knee
    #    value per cycle, while the trial's speed and the subject's height are
    #    broadcast down from their own levels.
    scidb.for_each(
        normalized_knee,
        inputs={
            "knee": CycleSymmetry["knee"],                # cycle level
            "walking_speed_mps": TrialInfo["walking_speed_mps"],  # trial level
            "height_cm": Demographics["height_cm"],       # subject level
        },
        outputs=[NormalizedKnee],
        subject=subject,
        session=session,
        speed=[],
        trial=[],
        cycle=[],
    )

    # 4. A 1-D input reduced to a scalar: one joint's curve in, its
    #    peak-to-peak out.
    scidb.for_each(
        knee_excursion,
        inputs={"knee": CycleWaveform["knee"]},
        outputs=[KneeExcursion],
        subject=subject,
        session=session,
        speed=[],
        trial=[],
        cycle=[],
    )

    # 5. `cycle` is NOT iterated, so each call gets that trial's ten cycles as
    #    one DataFrame (as_table) and returns the trial's means.
    scidb.for_each(
        trial_mean_symmetry,
        inputs={"cycles": CycleSymmetry},
        outputs=[TrialMeanSymmetry],
        as_table=["cycles"],
        subject=subject,
        session=session,
        speed=[],
        trial=[],
    )

    # 6. A cycle-level TABLE beside a trial-level SCALAR: the cycles of this
    #    trial, over the duration recorded for the trial itself.
    scidb.for_each(
        trial_cadence,
        inputs={"cycles": CycleSymmetry, "duration_s": TrialInfo["duration_s"]},
        outputs=[TrialCadence],
        as_table=["cycles"],
        subject=subject,
        session=session,
        speed=[],
        trial=[],
    )

    # 7. Same coarse iteration, but the function returns one row per cycle.
    #    distribute=True files those rows back at the cycle level, addressed
    #    by the returned `cycle` column.
    scidb.for_each(
        cycle_deviation,
        inputs={"cycles": CycleSymmetry},
        outputs=[CycleDeviation],
        as_table=["cycles"],
        distribute=True,
        subject=subject,
        session=session,
        speed=[],
        trial=[],
    )

    # 8. Session level, against the subject's OWN baseline: `Fixed` pins one
    #    input to session="baseline" while the other follows the iteration.
    scidb.for_each(
        speed_change_from_baseline,
        inputs={
            "speed": SessionInfo["comfortable_speed_mps"],
            "baseline_speed": Fixed(
                SessionInfo["comfortable_speed_mps"], session="baseline"
            ),
        },
        outputs=[SpeedChangeFromBaseline],
        subject=subject,
        session=session,
    )

    # 9. Once per joint column of the trial means, reassembled into one table.
    scidb.for_each(
        scale_joint,
        inputs={"value": TrialMeanSymmetry.for_columns(), "scale": SCALE},
        outputs=[ScaledTrialSymmetry],
        subject=subject,
        session=session,
        speed=[],
        trial=[],
    )

    # 10. Subject level from everything below it: every trial mean this subject
    #    has (as_table), plus their one subject-level age.
    scidb.for_each(
        subject_profile,
        inputs={"trials": TrialMeanSymmetry, "age_years": Demographics["age_years"]},
        outputs=[SubjectProfile],
        as_table=["trials"],
        subject=subject,
    )


if __name__ == "__main__":
    main()
