"""
Benchmark for save_batch / save_from_dataframe performance.

Simulates a realistic scientific dataset:
  20 subjects x 6 interventions x 2 speeds x 2 timepoints x 3 trials x 10 cycles
  = 14,400 rows of scalar data

Usage:
    python examples/benchmark_batch_save.py
"""

import os
import sys
import tempfile
import time
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd

# Add project paths
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root / "sciduckdb" / "src"))
sys.path.insert(0, str(project_root / "thunk-lib" / "src"))

from scidb.store.database import configure_database

from scidb import BaseVariable


class BenchScalar(BaseVariable):
    """Scalar variable for benchmarking."""
    pass


def build_dataframe():
    """Build a realistic 14,400-row DataFrame."""
    subjects = range(1, 21)       # 20
    interventions = range(1, 7)   # 6
    speeds = ["slow", "fast"]     # 2
    timepoints = ["pre", "post"]  # 2
    trials = range(1, 4)          # 3
    cycles = range(1, 11)         # 10

    rows = list(product(subjects, interventions, speeds, timepoints, trials, cycles))
    df = pd.DataFrame(rows, columns=[
        "subject", "intervention", "speed", "timepoint", "trial", "cycle"
    ])
    df["value1"] = np.random.randn(len(df))
    df["value2"] = np.random.randn(len(df))
    return df


def main():
    df = build_dataframe()
    print(f"DataFrame: {len(df)} rows, {len(df.columns)} columns")
    print(f"Unique schema combos: {df.drop(columns='value1').drop_duplicates().shape[0]}")

    # root_dir = "/Users/mitchelltillman/Documents/ICNR-2026-analysis/tmp_data"
    # df_csv = os.path.join(root_dir, "value1.csv")
    # df.to_csv(df_csv, index=False)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "bench.duckdb")

        db = configure_database(
            db_path,
            dataset_schema_keys=["subject", "intervention", "speed", "timepoint", "trial", "cycle"],
        )

        metadata_cols = ["subject", "intervention", "speed", "timepoint", "trial", "cycle"]

        # --- Benchmark save_from_dataframe ---
        print("\n=== save_from_dataframe (includes DataFrame extraction + save_batch) ===")
        t_start = time.perf_counter()
        record_ids = BenchScalar.save_from_dataframe(
            df=df,
            data_column="value1",
            metadata_columns=metadata_cols,
            db=db,
        )
        t_total = time.perf_counter() - t_start
        print(f"Total wall time: {t_total:.3f}s for {len(record_ids)} records")
        print(f"Throughput: {len(record_ids) / t_total:.0f} records/s")

        # --- Benchmark with profile=True ---
        print("\n=== save_batch with profiling (fresh data to avoid idempotency skip) ===")
        df2 = build_dataframe()  # Different random values -> different hashes
        data_items = []
        for i in range(len(df2)):
            meta = {col: df2[col].iloc[i] for col in metadata_cols}
            # Convert numpy types
            meta = {k: v.item() if hasattr(v, "item") else v for k, v in meta.items()}
            data = df2["value1"].iloc[i].item()
            data_items.append((data, meta))

        record_ids2 = db.save_batch(BenchScalar, data_items, profile=True)
        print(f"Records saved: {len(record_ids2)}")

        # --- Benchmark re-saving the data with profile=True ---
        print("\n=== save_batch with profiling (same data to activate idempotency skip) ===")
        data_items = []
        for i in range(len(df2)):
            meta = {col: df2[col].iloc[i] for col in metadata_cols}
            # Convert numpy types
            meta = {k: v.item() if hasattr(v, "item") else v for k, v in meta.items()}
            data = df2["value1"].iloc[i].item()
            data_items.append((data, meta))

        record_ids2 = db.save_batch(BenchScalar, data_items, profile=True)
        print(f"Records saved: {len(record_ids2)}")


        db.close()


if __name__ == "__main__":
    main()
