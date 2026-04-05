"""
GUI test data generator — VO2 max pipeline adapted for for_each.

Uses the modern scidb API to populate test_gui.duckdb with:
  - 3 subjects
  - A branching pipeline step (window_seconds=30 vs 60) so we can
    see multiple variants in the GUI

Safe to import by scistack-gui --module: all execution is guarded
by if __name__ == "__main__".

Run from the workspace root:
    python gui_test_data.py
"""

import numpy as np
import pandas as pd
from pathlib import Path

from scidb import BaseVariable, configure_database, for_each
# from scilineage.src.scilineage import lineage_fcn

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


class RawHeartRate(BaseVariable):
    """Raw heart rate signal (bpm)."""
    pass


# ------------------------------------------------------------------
# Processing functions — plain functions, no decorator needed.
# Defined at module level so scistack-gui can find them by name.
# ------------------------------------------------------------------

def compute_rolling_vo2(signal, window_seconds, sample_interval):
    """Rolling average of VO2 over a time window."""
    window_size = window_seconds // sample_interval
    return (
        pd.Series(signal)
        .rolling(window=window_size, min_periods=1)
        .mean()
        .values
    )


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


# ------------------------------------------------------------------
# Data seeding — only runs when executed directly, not on import.
# ------------------------------------------------------------------

if __name__ == "__main__":
    db_path = Path("test_gui.duckdb")

    # Remove previous run so we start fresh
    for f in Path(".").glob("test_gui.duckdb*"):
        f.unlink()

    db = configure_database(db_path, ["subject"])
    print(f"Database: {db_path}")
    print(f"Schema keys: {db.dataset_schema_keys}\n")

    subjects = ["S01", "S02", "S03"]

    rng = np.random.default_rng(42)

    print("Seeding raw data...")
    for subject in subjects:
        n = 120  # 120 samples = 10 minutes at 5-second intervals

        ramp = np.linspace(2000, 4200, n)
        noise = rng.normal(0, 150, n)
        vo2 = np.clip(ramp + noise, 1000, 5000)

        hr_ramp = np.linspace(80, 185, n)
        hr_noise = rng.normal(0, 5, n)
        hr = np.clip(hr_ramp + hr_noise, 60, 210)

        RawVO2.save(vo2, subject=subject)
        RawHeartRate.save(hr, subject=subject)
        print(f"  {subject}: VO2 [{vo2.min():.0f}, {vo2.max():.0f}], "
              f"HR [{hr.min():.0f}, {hr.max():.0f}]")

    print()

    print("Running for_each: compute_rolling_vo2 (window_seconds=30)...")
    for_each(
        compute_rolling_vo2,
        inputs={"signal": RawVO2, "window_seconds": 30, "sample_interval": 5},
        outputs=[RollingVO2],
        subject=subjects,
    )

    print("Running for_each: compute_rolling_vo2 (window_seconds=60)...")
    for_each(
        compute_rolling_vo2,
        inputs={"signal": RawVO2, "window_seconds": 60, "sample_interval": 5},
        outputs=[RollingVO2],
        subject=subjects,
    )

    print("\nRunning for_each: compute_max_vo2...")
    for_each(
        compute_max_vo2,
        inputs={"rolling_vo2": RollingVO2},
        outputs=[MaxVO2],
        subject=subjects,
    )

    print("\nRunning for_each: compute_max_hr...")
    for_each(
        compute_max_hr,
        inputs={"signal": RawHeartRate},
        outputs=[MaxHeartRate],
        subject=subjects,
    )

    print("\nPipeline variants in DB:")
    for v in db.list_pipeline_variants():
        print(f"  {v['function_name']} -> {v['output_type']} "
              f"| constants={v['constants']} | {v['record_count']} records")

    print(f"\nDone. Open with: scistack-gui --module gui_test_data.py test_gui.duckdb")
    db.close()

class MaxHR_80Perc(BaseVariable):
    pass

class MaxHR_50Perc(BaseVariable):
    pass

class MaxHR_Perc(BaseVariable):
    pass
