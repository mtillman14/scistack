"""
scifor + scidb: what the example pipeline stored, level by level.

Every assertion here is about a ``for_each`` feature the pipeline exercises —
the counts are the contract, and each one would silently go wrong first in a
GUI run rather than a unit test.
"""

from __future__ import annotations

import pandas as pd
import pytest

from conftest import (
    CYCLES,
    cell,
    columns_of,
    JOINTS,
    N_CYCLES,
    N_SESSIONS,
    N_SUBJECTS,
    N_TRIALS,
    SESSIONS,
    SUBJECTS,
)


def _frame(variable, **metadata) -> pd.DataFrame:
    return variable.load(as_df=True, **metadata)


# --- one record per file, at the level the file's keys name -------------------


def test_every_level_has_one_record_per_file(pipeline, example_db):
    assert len(_frame(pipeline.CycleSymmetry)) == N_CYCLES
    assert len(_frame(pipeline.CycleWaveform)) == N_CYCLES
    assert len(_frame(pipeline.TrialInfo)) == N_TRIALS
    assert len(_frame(pipeline.SessionInfo)) == N_SESSIONS
    assert len(_frame(pipeline.Demographics)) == N_SUBJECTS


def test_a_coarse_record_leaves_the_finer_keys_empty(pipeline, example_db):
    """A subject-level record is filed at ``subject`` alone: session, speed,
    trial and cycle are NULL, not filled in or defaulted."""
    demographics = _frame(pipeline.Demographics)
    for key in ("session", "speed", "trial", "cycle"):
        assert demographics[key].isna().all(), key
    sessions = _frame(pipeline.SessionInfo)
    assert sessions["session"].notna().all()
    assert sessions["trial"].isna().all()


# --- inputs from several levels in one call -------------------------------------


def test_a_coarse_value_reaches_every_finer_combination(pipeline, example_db):
    """``normalized_knee`` = knee / (trial speed x subject height). Recomputed
    here from the three source variables for one cycle, it must equal the
    stored value — proving the trial-level and subject-level inputs were
    broadcast down to that cycle."""
    where = dict(subject=SUBJECTS[0], session=SESSIONS[0], speed="slow", trial="01", cycle="03")
    stored = _frame(pipeline.NormalizedKnee, **where)
    assert len(stored) == 1

    knee = float(cell(_frame(pipeline.CycleSymmetry, **where)["data"].iloc[0], "knee"))
    trial = _frame(pipeline.TrialInfo, subject=where["subject"], session=where["session"],
                   speed="slow", trial="01")["data"].iloc[0]
    height = _frame(pipeline.Demographics, subject=where["subject"])["data"].iloc[0]
    expected = knee / (float(cell(trial, "walking_speed_mps")) * float(cell(height, "height_cm")))
    assert float(stored["data"].iloc[0]) == pytest.approx(expected)


def test_every_cycle_got_a_normalized_value(pipeline, example_db):
    assert len(_frame(pipeline.NormalizedKnee)) == N_CYCLES


def test_trial_cadence_uses_the_trials_own_duration(pipeline, example_db):
    cadence = _frame(pipeline.TrialCadence)
    assert len(cadence) == N_TRIALS
    one = cadence.iloc[0]
    trial = _frame(pipeline.TrialInfo, subject=one["subject"], session=one["session"],
                   speed=one["speed"], trial=one["trial"])["data"].iloc[0]
    assert float(one["data"]) == pytest.approx(len(CYCLES) / float(cell(trial, "duration_s")))


# --- as_table, distribute, for_columns, Fixed, Parameter ------------------------


def test_trial_means_are_the_mean_of_that_trials_cycles(pipeline, example_db):
    means = _frame(pipeline.TrialMeanSymmetry)
    assert len(means) == N_TRIALS
    one = means.iloc[0]
    cycles = _frame(pipeline.CycleSymmetry, subject=one["subject"], session=one["session"],
                    speed=one["speed"], trial=one["trial"])
    for joint in JOINTS:
        values = [float(cell(data, joint)) for data in cycles["data"]]
        assert len(values) == len(CYCLES)
        assert float(cell(one["data"], joint)) == pytest.approx(sum(values) / len(values))


def test_distribute_files_each_row_at_its_own_cycle(pipeline, example_db):
    """The returned ``cycle`` column addresses the pieces — row "07" lands at
    cycle="07", the zero-padded STRING, not at ordinal 7."""
    deviation = _frame(pipeline.CycleDeviation)
    assert len(deviation) == N_CYCLES
    assert set(deviation["cycle"]) == set(CYCLES)
    # Deviations from a trial's mean sum to zero within that trial.
    one = deviation.iloc[0]
    trial_rows = _frame(pipeline.CycleDeviation, subject=one["subject"], session=one["session"],
                        speed=one["speed"], trial=one["trial"])
    knees = [float(cell(data, "knee")) for data in trial_rows["data"]]
    assert len(knees) == len(CYCLES)
    assert sum(knees) == pytest.approx(0.0, abs=1e-6)


def test_for_columns_reassembles_one_table_per_trial(pipeline, example_db):
    scaled = _frame(pipeline.ScaledTrialSymmetry)
    assert len(scaled) == N_TRIALS
    one = scaled.iloc[0]
    table = one["data"]
    assert set(JOINTS) <= set(columns_of(table)), columns_of(table)
    mean = _frame(pipeline.TrialMeanSymmetry, subject=one["subject"], session=one["session"],
                  speed=one["speed"], trial=one["trial"])["data"].iloc[0]
    for joint in JOINTS:
        assert float(cell(table, joint)) == pytest.approx(float(cell(mean, joint)) * 0.01)


def test_fixed_compares_each_session_with_the_same_subjects_baseline(pipeline, example_db):
    change = _frame(pipeline.SpeedChangeFromBaseline)
    assert len(change) == N_SESSIONS
    baseline_rows = change[change["session"] == "baseline"]
    assert (baseline_rows["data"].astype(float).abs() < 1e-9).all(), "baseline minus itself"
    for subject in SUBJECTS:
        info = _frame(pipeline.SessionInfo, subject=subject).set_index("session")["data"]
        base = float(cell(info["baseline"], "comfortable_speed_mps"))
        for session in SESSIONS:
            got = float(change[(change["subject"] == subject) & (change["session"] == session)]["data"].iloc[0])
            assert got == pytest.approx(float(cell(info[session], "comfortable_speed_mps")) - base)


def test_a_two_value_parameter_stores_two_variants_per_cycle(pipeline, example_db):
    flags = pipeline.AnkleOverThreshold.load(as_df=True, version="all")
    assert len(flags) == 2 * N_CYCLES
    # The stricter threshold can only flag a subset of what the looser one does.
    assert set(flags["data"].astype(float)) <= {0.0, 1.0}


def test_a_1d_input_reduces_to_a_scalar_per_cycle(pipeline, example_db):
    excursion = _frame(pipeline.KneeExcursion)
    assert len(excursion) == N_CYCLES
    assert (excursion["data"].astype(float) > 0).all()


def test_subject_profile_is_one_row_per_subject(pipeline, example_db):
    profiles = _frame(pipeline.SubjectProfile)
    assert len(profiles) == N_SUBJECTS
    one = profiles.iloc[0]["data"]
    assert int(cell(one, "n_trials")) == N_TRIALS // N_SUBJECTS
    assert set(JOINTS) <= set(columns_of(one))


# --- idempotence ----------------------------------------------------------------


def test_running_a_step_again_adds_nothing(pipeline, example_db):
    """Same function, same inputs, same keys: the version keys match and no
    new record is written. The one guard against a GUI re-run doubling data."""
    import scidb

    before = len(_frame(pipeline.TrialCadence))
    scidb.for_each(
        pipeline.trial_cadence,
        inputs={"cycles": pipeline.CycleSymmetry, "duration_s": pipeline.TrialInfo["duration_s"]},
        outputs=[pipeline.TrialCadence],
        as_table=["cycles"],
        subject=SUBJECTS,
        session=SESSIONS,
        speed=[],
        trial=[],
    )
    after = pipeline.TrialCadence.load(as_df=True, version="all")
    assert len(after) == before



def test_no_two_templates_claim_the_same_file(pipeline, example_db):
    """Every PathInput of the pipeline discovers a disjoint set of files.
    `{cycle}.csv` swallows any suffix, so a matrix file named
    `..._t01_coupling.csv` was once a cycle called "oupling" — every row of
    every 3 x 3 matrix loaded as symmetry data."""
    templates = {
        name: getattr(pipeline, name)
        for name in dir(pipeline)
        if name.endswith("_FILE")
    }
    for name, template in templates.items():
        for values in template.discover():
            for key in ("subject", "session", "speed", "trial", "cycle"):
                if key in values:
                    assert str(values[key]).isalnum(), f"{name}: {key}={values[key]!r} is not a level"
            if "cycle" in values:
                assert values["cycle"] in CYCLES, f"{name} discovered cycle={values['cycle']!r}"
    cycles = set(_frame(pipeline.CycleSymmetry)["cycle"].astype(str))
    assert cycles == set(CYCLES), cycles
