"""
Python pipeline over the per-cycle gait symmetry example dataset.

Standard library and pandas only — no numpy, no scipy — so the example runs
anywhere scidb does.

The dataset is on the schema ``[subject, session, speed, trial, cycle]``
(see the README), with a file at **four levels**: one per cycle (symmetry
for three joints, where 0 is perfectly symmetric), one per trial, one per
session and one per subject.

The steps below exercise one ``for_each`` feature each, so this file
doubles as a pipeline smoke test:

| step | feature |
|---|---|
| ``load_cycle_symmetry``   | ``PathInput`` — one file per cycle, keys discovered from the path |
| ``load_trial_info`` / ``load_session_info`` / ``load_demographics`` | the same, at the trial, session and subject levels |
| ``ankle_over_threshold``  | ``ColumnSelection`` (``CycleSymmetry["ankle"]``) + ``Parameter`` (two values = two variants) |
| ``trial_mean_symmetry``   | ``as_table`` — every cycle of a trial arrives as one DataFrame |
| ``cycle_deviation``       | ``as_table`` + ``distribute=True`` — one row back out per cycle |
| ``scale_joint``           | ``for_columns`` — the function runs once per joint column |
| ``normalized_knee``       | **three levels in one call** — a cycle value, its trial's speed, its subject's height |
| ``trial_cadence``         | a cycle-level table plus a trial-level column |
| ``speed_change_from_baseline`` | ``Fixed`` — this session against the subject's baseline session |
| ``subject_profile``       | every trial of one subject, plus that subject's demographics |

**Levels mix freely.** A value recorded above the combination being
computed is broadcast down to it: every cycle of subject01 sees the same
``height_cm``. That is the realistic case — a per-cycle measure normalized
by a per-trial speed and a per-subject anthropometric.

Run it with ``python src/cycles/run_pipeline.py`` from the project folder.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

import scidb
from scidb import BaseVariable

#: The joints each file carries, in file order.
JOINTS = ["ankle", "knee", "hip"]

#: examples/aim2/data — this file is examples/aim2/src/cycles/pipeline.py.
DATA_ROOT = Path(__file__).resolve().parents[2] / "data"


# ---------------------------------------------------------------------------
# Entities: one variable per pipeline step, plus the inputs they are given.
# ---------------------------------------------------------------------------


class CycleSymmetry(BaseVariable):
    """One row per cycle: symmetry for ankle, knee and hip."""


class AnkleOverThreshold(BaseVariable):
    """1.0 when a cycle's ankle symmetry exceeds the threshold, else 0.0."""


class TrialMeanSymmetry(BaseVariable):
    """One row per trial: each joint averaged over that trial's cycles."""


class CycleDeviation(BaseVariable):
    """Per cycle: how far each joint sits from its own trial's mean."""


class ScaledTrialSymmetry(BaseVariable):
    """The trial means, each joint rescaled by SCALE."""


class Demographics(BaseVariable):
    """Subject level: age, height, mass. One record per subject."""


class SessionInfo(BaseVariable):
    """Session level: comfortable speed and perceived effort."""


class TrialInfo(BaseVariable):
    """Trial level: duration and the walking speed actually held."""


class NormalizedKnee(BaseVariable):
    """Cycle level: knee symmetry per unit of speed and height."""


class TrialCadence(BaseVariable):
    """Trial level: cycles per second over the trial's duration."""


class SpeedChangeFromBaseline(BaseVariable):
    """Session level: comfortable speed minus the subject's baseline."""


class SubjectProfile(BaseVariable):
    """Subject level: mean symmetry per joint, beside the subject's age."""


class CycleWaveform(BaseVariable):
    """Cycle level, 1-D: one curve per joint over the gait cycle.

    The only ARRAY-valued variable here, and the one the plotting layer's
    1-D paths need — line and band plots, the per-cell statistic that
    reduces a curve to one number, and the transport downsampling.
    """


class KneeExcursion(BaseVariable):
    """Cycle level: peak-to-peak of the knee curve — 1-D reduced to a scalar."""


class JointCoupling(BaseVariable):
    """Trial level, 2-D: a 3 x 3 joint coupling matrix — the dataset's one
    matrix-valued variable, which the plot layer draws as a heatmap."""


#: Every key is delimited in the path, so no ``key_regex`` is needed and
#: ``for_each`` can discover all five from what is on disk.
SYMMETRY_FILE = scidb.PathInput(
    "{subject}/{session}/t{trial}/{subject}_{session}_{speed}_t{trial}_c{cycle}.csv",
    root_folder=str(DATA_ROOT),
)

#: The coarser files. Each template names only the keys its level is
#: recorded at, which is what makes the records subject-, session- and
#: trial-level rather than cycle-level.
DEMOGRAPHICS_FILE = scidb.PathInput(
    "{subject}/{subject}_demographics.csv", root_folder=str(DATA_ROOT)
)
SESSION_FILE = scidb.PathInput(
    "{subject}/{session}/{subject}_{session}_session.csv", root_folder=str(DATA_ROOT)
)
TRIAL_FILE = scidb.PathInput(
    "{subject}/{session}/t{trial}/{subject}_{session}_{speed}_t{trial}_trial.csv",
    root_folder=str(DATA_ROOT),
)

#: The waveforms live in their own subfolder, so these files cannot also
#: match SYMMETRY_FILE — two templates over one folder would otherwise both
#: claim them, and ``{cycle}.csv`` would happily swallow a longer suffix.
WAVEFORM_FILE = scidb.PathInput(
    "{subject}/{session}/t{trial}/waveforms/"
    "{subject}_{session}_{speed}_t{trial}_c{cycle}.csv",
    root_folder=str(DATA_ROOT),
)

#: One 3 x 3 matrix per trial (subject x session x speed x trial). Named
#: `_matrix.csv`, NOT `_coupling.csv`: `{cycle}.csv` in SYMMETRY_FILE's
#: template swallows any suffix, and `_c` + `oupling` was a cycle called
#: "oupling" — the loader read every matrix file as a cycle (integration
#: suite, 2026-09-19). Two templates over one folder must not share a tail.
COUPLING_FILE = scidb.PathInput(
    "{subject}/{session}/t{trial}/{subject}_{session}_{speed}_t{trial}_matrix.csv",
    root_folder=str(DATA_ROOT),
)

#: Two values, so the step runs twice and the results are two variants of one
#: variable — what the Plot Studio shows as a Variant axis.
ASYMMETRY_THRESHOLD = scidb.Parameter(
    50, 100, description="Symmetry above this counts as an asymmetric cycle"
)

#: One value: a plain configuration constant, not a fan-out.
SCALE = scidb.Parameter(0.01, description="Rescales symmetry from 0-200 to 0-2")


# ---------------------------------------------------------------------------
# Step 1 — load: one file per cycle (PathInput)
# ---------------------------------------------------------------------------


def load_cycle_symmetry(csv_file_path):
    """Read one cycle's CSV.

    Returns a one-row DataFrame, so the record is stored with one column per
    joint — which is what lets later steps select a single column, iterate the
    columns, or plot the joints as separate panels.

    The returned frame carries no schema-key column, so it is saved whole, as
    one record for the combination being loaded (the spread rule; see
    docs/claude/distribute-vs-spread.md).
    """
    return pd.read_csv(csv_file_path)


# ---------------------------------------------------------------------------
# Step 2 — one column, one parameter (ColumnSelection + Parameter)
# ---------------------------------------------------------------------------


def ankle_over_threshold(ankle, threshold):
    """Flag a cycle whose ankle symmetry exceeds ``threshold``.

    ``ankle`` arrives as the values of ONE column
    (``CycleSymmetry["ankle"]``), not the whole table: a single-column
    selection is passed as an array, so this function never sees the joints
    it does not use.
    """
    value = float(pd.Series(ankle).iloc[0])
    return 1.0 if value > float(threshold) else 0.0


# ---------------------------------------------------------------------------
# Step 3 — every cycle of a trial at once (as_table)
# ---------------------------------------------------------------------------


def trial_mean_symmetry(cycles):
    """Average each joint over a trial's cycles.

    ``cycle`` is not iterated and ``as_table`` is on, so ``cycles`` is a
    DataFrame holding that trial's ten cycles — the joint columns plus the
    schema keys, one row per cycle. Returning a one-row frame stores a trial
    record shaped exactly like a cycle record.
    """
    joints = [name for name in JOINTS if name in cycles.columns]
    return cycles[joints].mean().to_frame().T


# ---------------------------------------------------------------------------
# Step 4 — a table in, one record per cycle back out (as_table + distribute)
# ---------------------------------------------------------------------------


def cycle_deviation(cycles):
    """Each cycle's distance from its own trial's mean, per joint.

    Takes the trial's cycles as one frame and returns one row per cycle,
    keeping the ``cycle`` column. With ``distribute=True`` that column
    addresses the pieces, so row "07" is saved at ``cycle="07"`` rather than
    at a positional ordinal — which matters here because the cycles are
    zero-padded strings.
    """
    joints = [name for name in JOINTS if name in cycles.columns]
    deviations = cycles[joints] - cycles[joints].mean()
    return pd.concat([cycles[["cycle"]].reset_index(drop=True),
                      deviations.reset_index(drop=True)], axis=1)


# ---------------------------------------------------------------------------
# Step 5 — once per joint column (for_columns)
# ---------------------------------------------------------------------------


def scale_joint(value, scale):
    """Rescale one joint's trial mean.

    With ``TrialMeanSymmetry.for_columns()`` this runs once per joint —
    ``value`` is that one column — and the per-column results are reassembled
    into a one-row table with the same column names.
    """
    return float(pd.Series(value).iloc[0]) * float(scale)


# ---------------------------------------------------------------------------
# Step 6 — the coarser levels (PathInput again, with fewer keys)
# ---------------------------------------------------------------------------


def load_demographics(csv_file_path):
    """One row per SUBJECT: age, height, mass.

    Identical to the cycle loader; only the template differs. A record saved
    at ``subject`` alone is what every finer combination of that subject
    reads later.
    """
    return pd.read_csv(csv_file_path)


def load_session_info(csv_file_path):
    """One row per (subject, session): comfortable speed, perceived effort."""
    return pd.read_csv(csv_file_path)


def load_trial_info(csv_file_path):
    """One row per (subject, session, speed, trial): duration, speed held."""
    return pd.read_csv(csv_file_path)


# ---------------------------------------------------------------------------
# Step 7 — three levels in one call (cycle + trial + subject)
# ---------------------------------------------------------------------------


def normalized_knee(knee, walking_speed_mps, height_cm):
    """Knee symmetry per unit of walking speed and subject height.

    The realistic shape of a processing step: the measure is per CYCLE, the
    speed it was walked at is per TRIAL, and the height is per SUBJECT. Each
    coarser value is broadcast down to the cycle being computed, so every
    cycle of a trial sees that trial's speed and every cycle of a subject
    sees that subject's height.
    """
    value = float(pd.Series(knee).iloc[0])
    speed = float(pd.Series(walking_speed_mps).iloc[0])
    height = float(pd.Series(height_cm).iloc[0])
    return value / (speed * height)


# ---------------------------------------------------------------------------
# Step 8 — a fine table plus a coarse value (as_table + a trial-level column)
# ---------------------------------------------------------------------------


def trial_cadence(cycles, duration_s):
    """Cycles per second: a cycle-level TABLE over a trial-level SCALAR.

    ``cycles`` is every cycle of this trial (``as_table``); ``duration_s`` is
    the one value recorded for the trial itself.
    """
    duration = float(pd.Series(duration_s).iloc[0])
    return len(cycles) / duration if duration else float("nan")


# ---------------------------------------------------------------------------
# Step 9 — this session against the subject's baseline (Fixed)
# ---------------------------------------------------------------------------


def speed_change_from_baseline(speed, baseline_speed):
    """Change in comfortable speed from the subject's own baseline session.

    ``Fixed(..., session="baseline")`` pins one input to a single session
    while the other follows the iteration, so every session of a subject is
    compared with that same subject's baseline — never with another
    subject's.
    """
    return float(pd.Series(speed).iloc[0]) - float(pd.Series(baseline_speed).iloc[0])


# ---------------------------------------------------------------------------
# Step 10 — one row per subject, from everything below it
# ---------------------------------------------------------------------------


def subject_profile(trials, age_years):
    """Mean symmetry per joint for one subject, beside that subject's age.

    Iterating ``subject`` alone makes ``trials`` every trial mean that
    subject has (``as_table``), while ``age_years`` is their one
    subject-level value. The result is a subject-level record.
    """
    joints = [name for name in JOINTS if name in trials.columns]
    profile = trials[joints].mean().to_frame().T
    profile["age_years"] = float(pd.Series(age_years).iloc[0])
    profile["n_trials"] = len(trials)
    return profile


# ---------------------------------------------------------------------------
# Step 11 — 1-D data: one curve per joint, per cycle
# ---------------------------------------------------------------------------


def load_cycle_waveform(csv_file_path):
    """Read one cycle's waveform file: 51 samples x three joints.

    Returns a dict of lists rather than a DataFrame, so each joint is stored
    as ONE 1-D value per record instead of 51 rows. That is what makes this
    variable array-valued: the plot layer offers it as a line or a band, and
    a scalar plot kind reduces each curve with the cell statistic first.
    """
    frame = pd.read_csv(csv_file_path)
    return {joint: frame[joint].tolist() for joint in JOINTS if joint in frame.columns}


def knee_excursion(knee):
    """Peak-to-peak of one cycle's knee curve — a 1-D input, a scalar out.

    ``CycleWaveform["knee"]`` selects one joint, so the value handed over is
    that joint's curve. Reducing it here (rather than plotting the curve)
    is the pipeline-side counterpart of the Plot Studio's cell statistic.
    """
    curve = pd.Series(knee)
    if len(curve) == 1 and hasattr(curve.iloc[0], "__len__"):
        curve = pd.Series(curve.iloc[0])
    return float(curve.max() - curve.min())


# ---------------------------------------------------------------------------
# Step 12 — 2-D data: one matrix per trial
# ---------------------------------------------------------------------------


def load_joint_coupling(csv_file_path):
    """Read one trial's 3 x 3 coupling matrix as a list of rows.

    A nested list is stored as ONE 2-D value per record, so the variable is
    matrix-valued: the plot layer offers only a heatmap for it, averaging the
    matrices a panel collapses over, and "Save data" refuses it.
    """
    frame = pd.read_csv(csv_file_path, index_col=0)
    return frame[JOINTS].to_numpy().tolist()
