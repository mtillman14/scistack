"""
GUI test data generator — VO2 max pipeline adapted for for_each.

Seeds test_gui.duckdb with raw data for 3 subjects (RawVO2,
RawHeartRate) and registers the pipeline functions below, but runs
NOTHING — a fresh pipeline starts with an empty canvas, and the user
places/wires/runs each step themselves in the GUI (including running
compute_rolling_vo2 twice, with window_seconds=30 and 60, to get the
two-variant branch this demo is built around).

Also registers load_vo2_from_csv/load_heart_rate_from_csv — a SECOND,
alternative way to produce RawVO2/RawHeartRate, reading from
examples/vo2max/data/by_subject/{subject}/*.csv (see
EXAMPLE_VO2MAX_DATA_DIR) via a PathInput node instead of the synthetic
.save() calls below. Wiring these up (a `data_dir` PathInput, template
"{subject}", root_folder EXAMPLE_VO2MAX_DATA_DIR) is the live end-to-end
test for the PathInput execution fix — see
.claude/plan-pathinput-fresh-run-fix.md. Same "nothing pre-run"
philosophy: the loaders are registered but the user wires and runs them
by hand.

Safe to import by scistack-gui --module: all execution is guarded
by if __name__ == "__main__".

Run from the workspace root:
    python gui_test_data.py
"""

from pathlib import Path

import numpy as np
import pandas as pd

from scidb import BaseVariable, configure_database

# from scilineage.src.scilineage import lineage_fcn

# examples/vo2max/data/by_subject/{S01,S02,S03}/{vo2_ml_min,heart_rate_bpm}.csv
# — see load_vo2_from_csv/load_heart_rate_from_csv below. Deliberately its
# own subdirectory, NOT examples/vo2max/data/ directly: that folder also
# holds the original single-subject demo's flat CSVs (time_sec.csv etc.,
# used by examples/vo2max/pipeline.py's data_dir constant), and a bare
# "{subject}" PathInput template has no way to tell a subject folder
# apart from a same-level file — found by hand: those flat files got
# discovered as fake "subjects" (heart_rate_bpm.csv, ...) and crashed
# with NotADirectoryError the first time this was actually run.
EXAMPLE_VO2MAX_DATA_DIR = (
    Path(__file__).parent / "examples" / "vo2max" / "data" / "by_subject"
)

# ------------------------------------------------------------------
# Variable types — defined at module level so scistack-gui can import
# them and register them with the DB.
# ------------------------------------------------------------------


class RawVO2(BaseVariable):
    """Raw VO2 signal (mL/min) — one array per subject."""

    pass


class RollingVO2(BaseVariable):
    """Rolling average VO2. Varies by window_seconds (pipeline variant)."""

    pass


class MaxVO2(BaseVariable):
    """VO2 max scalar — mean of the two highest rolling averages."""

    pass


class MaxHeartRate(BaseVariable):
    """Peak heart rate. Demonstrates a second parallel branch."""

    pass


class VO2Summary(BaseVariable):
    """Cohort-level stat summary of MaxVO2 (stat_ endpoint output)."""

    pass


class RawHeartRate(BaseVariable):
    """Raw heart rate signal (bpm)."""

    pass


# ------------------------------------------------------------------
# Processing functions — plain functions, no decorator needed.
# Defined at module level so scistack-gui can find them by name.
# ------------------------------------------------------------------


def load_vo2_from_csv(data_dir):
    """Load VO2 data from examples/vo2max's per-subject CSV layout ->
    RawVO2. An alternative producer to the synthetic seeding below — wire
    a PathInput node (name doesn't matter to matching, only the PARAM
    NAME `data_dir` does; template "{subject}", root_folder
    EXAMPLE_VO2MAX_DATA_DIR) to this function's `data_dir` input to
    exercise PathInput-backed execution end to end."""
    df = pd.read_csv(Path(data_dir) / "vo2_ml_min.csv")
    return df.iloc[:, 0].values


def load_heart_rate_from_csv(data_dir):
    """Load heart rate data from examples/vo2max's per-subject CSV
    layout -> RawHeartRate. Same PathInput wiring as load_vo2_from_csv
    (both loaders share the `data_dir` param name, so one PathInput
    definition feeds both)."""
    df = pd.read_csv(Path(data_dir) / "heart_rate_bpm.csv")
    return df.iloc[:, 0].values


def compute_rolling_vo2(signal, window_seconds, sample_interval):
    """Rolling average of VO2 over a time window."""
    window_size = window_seconds // sample_interval
    return pd.Series(signal).rolling(window=window_size, min_periods=1).mean().values


def compute_max_vo2(rolling_vo2):
    """VO2 max: mean of the two highest rolling averages."""
    sorted_vals = np.sort(rolling_vo2)[::-1]
    return float(np.mean(sorted_vals[:2]))


def compute_max_hr(signal):
    """Peak heart rate."""
    return float(np.max(signal))


def compute_80_perc_max_hr(max_hr):
    """Max HR * 0.8"""
    return max_hr * 0.8


def compute_50_perc_max_hr(max_hr):
    """Max HR * 0.5"""
    return max_hr * 0.5


# @lineage_fcn
def compute_perc_max_hr(max_hr: int, perc: float):
    """Max HR * perc"""
    return max_hr * perc


def stat_vo2_summary(max_vo2):
    """Cohort summary of MaxVO2 — a stat_ ENDPOINT (endpoint-first GUI
    demo). stat_ functions pool their input rows (as_table default), so
    this receives a DataFrame across the iterated combos."""
    if isinstance(max_vo2, pd.DataFrame):
        vals = np.ravel([np.ravel(v) for v in max_vo2["MaxVO2"]])
    else:
        vals = np.ravel(max_vo2)
    return {
        "mean_max_vo2": float(np.mean(vals)),
        "best_max_vo2": float(np.max(vals)),
        "n_subjects": int(vals.size),
    }


# ------------------------------------------------------------------
# Data seeding — only runs when executed directly, not on import.
# ------------------------------------------------------------------

if __name__ == "__main__":
    db_path = Path("test_gui.duckdb")

    # Remove previous run so we start fresh. Node positions live inside the
    # .duckdb (no layout sidecar since 2026-10-21), so this clears them too.
    for f in Path(".").glob("test_gui.duckdb*"):
        f.unlink()

    db = configure_database(db_path, ["subject"])
    # print(f"Database: {db_path}")
    # print(f"Schema keys: {db.dataset_schema_keys}\n")

    # subjects = ["S01", "S02", "S03"]

    # rng = np.random.default_rng(42)

    # print("Seeding raw data...")
    # for subject in subjects:
    #     n = 120  # 120 samples = 10 minutes at 5-second intervals

    #     ramp = np.linspace(2000, 4200, n)
    #     noise = rng.normal(0, 150, n)
    #     vo2 = np.clip(ramp + noise, 1000, 5000)

    #     hr_ramp = np.linspace(80, 185, n)
    #     hr_noise = rng.normal(0, 5, n)
    #     hr = np.clip(hr_ramp + hr_noise, 60, 210)

    #     RawVO2.save(vo2, subject=subject)
    #     RawHeartRate.save(hr, subject=subject)
    #     print(
    #         f"  {subject}: VO2 [{vo2.min():.0f}, {vo2.max():.0f}], "
    #         f"HR [{hr.min():.0f}, {hr.max():.0f}]"
    #     )

    # print()

    # # Deliberately NOT pre-run: a real new pipeline starts with raw data
    # # seeded and its functions registered (import of this module registers
    # # them), but with an empty canvas — the user drags each function on,
    # # wires it, and clicks Run themselves. Pre-executing here would mean
    # # the GUI never actually exercises node placement/wiring/run for this
    # # demo, and it's exactly the "click Run in the GUI" path the placement-
    # # qualified-id and duplicate-pipeline work needs exercised by hand.
    # print("Raw data seeded. Nothing has been run yet — build the pipeline")
    # print("interactively in the GUI:")
    # print("  1. compute_rolling_vo2(signal=RawVO2, window_seconds, sample_interval) -> RollingVO2")
    # print("     (run it twice with window_seconds=30 and 60 to get two variants)")
    # print("  2. compute_max_vo2(rolling_vo2=RollingVO2) -> MaxVO2")
    # print("  3. compute_max_hr(signal=RawHeartRate) -> MaxHeartRate")
    # print("  4. compute_80_perc_max_hr / compute_50_perc_max_hr / compute_perc_max_hr(max_hr, perc)")
    # print("  5. stat_vo2_summary(max_vo2=MaxVO2) -> cohort summary")
    # print()
    # print("PathInput test (alternative RawVO2/RawHeartRate producers):")
    # print("  6. Create a PathInput node — name: data_dir, template: {subject},")
    # print(f"     root_folder: {EXAMPLE_VO2MAX_DATA_DIR}")
    # print("     Wire it into load_vo2_from_csv(data_dir) -> RawVO2 and/or")
    # print("     load_heart_rate_from_csv(data_dir) -> RawHeartRate, then Run")
    # print("     for subject S01/S02/S03 — exercises the PathInput execution")
    # print("     fix end to end (never-run target, no prior DB history for")
    # print("     this wiring).")
    print("\nOpen with: scistack-gui --module gui_test_data.py test_gui.duckdb")
    db.close()


class MaxHR_80Perc(BaseVariable):
    pass


class MaxHR_50Perc(BaseVariable):
    pass


class MaxHR_Perc(BaseVariable):
    pass

class RollingHR(BaseVariable):
    pass

class Speed(BaseVariable):
    pass
