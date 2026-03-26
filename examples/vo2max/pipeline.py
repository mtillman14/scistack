"""
VO2 Max Test Pipeline
=====================

Demonstrates a complete data processing pipeline using the SciStack framework.
Loads raw physiological data from a simulated VO2 max test, combines the
signals, computes rolling averages, and extracts peak metrics.

Framework features showcased:
  - BaseVariable: Type-safe data storage with automatic and custom serialization
  - configure_database: DuckDB (data) + SQLite (lineage) dual-backend setup
  - @thunk: Automatic lineage tracking and computation caching
  - ThunkOutput: Transparent data flow between processing steps
  - Provenance queries: Tracing how results were computed

Usage:
    cd examples/vo2max
    python generate_data.py   # Create dummy CSV files
    python pipeline.py        # Run the pipeline
"""

import numpy as np
import pandas as pd
from pathlib import Path

# =============================================================================
# SCIDB IMPORTS
#
# All core framework components come from the `scidb` package:
#   - BaseVariable: Base class for defining storable data types
#   - configure_database: One-call setup for DuckDB + SQLite backends
#   - thunk: Decorator that adds lineage tracking to any function
#   - ThunkOutput: Wrapper around function results that carries lineage info
# =============================================================================

from scidb import BaseVariable, configure_database, thunk, ThunkOutput


# =============================================================================
# STEP 1: DEFINE VARIABLE TYPES  [scidb.BaseVariable]
#
# Each variable type maps to a table in the DuckDB database. Subclassing
# BaseVariable automatically registers the type in a global registry, so
# configure_database() can discover and register all types at startup.
#
# Types that store simple data (scalars, numpy arrays, lists) need NO custom
# serialization — SciDuck handles them natively via DuckDB's type system.
#
# Types that store DataFrames or other complex objects should override
# to_db() and from_db() to control how data is serialized to/from rows.
# =============================================================================

class RawTime(BaseVariable):
    """Raw time data (seconds) loaded from CSV.

    Uses native SciDuck storage — no to_db()/from_db() needed for numpy arrays.
    """
    pass


class RawHeartRate(BaseVariable):
    """Raw heart rate data (bpm) loaded from CSV. Native storage."""
    pass


class RawVO2(BaseVariable):
    """Raw VO2 data (mL/min) loaded from CSV. Native storage."""
    pass


class CombinedData(BaseVariable):
    """
    Combined time series with time, HR, and VO2 columns.

    Overrides to_db()/from_db() because the data is a pandas DataFrame
    and we want to preserve the multi-column structure in storage.
    """    

    def to_db(self) -> pd.DataFrame:
        # Return the DataFrame directly — each column becomes a DuckDB column
        return self.data

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> pd.DataFrame:
        # Reconstruct the DataFrame from stored columns
        return df


class RollingVO2(BaseVariable):
    """30-second rolling average of VO2. Native storage (numpy array)."""
    pass


class MaxHeartRate(BaseVariable):
    """Peak heart rate during the test. Native storage (scalar)."""
    pass


class MaxVO2(BaseVariable):
    """
    VO2 max: mean of the two highest 30-second rolling VO2 averages.
    Native storage (scalar).
    """
    pass


# =============================================================================
# STEP 2: DEFINE PROCESSING FUNCTIONS  [scidb.thunk / @thunk decorator]
#
# The @thunk decorator wraps each function so that:
#   1. A hash of the function's bytecode is computed (detects code changes)
#   2. All inputs are classified when called (ThunkOutput vs saved variable
#      vs raw constant) and recorded for lineage
#   3. The function executes and its result is wrapped in a ThunkOutput
#   4. If caching is enabled (via configure_database), identical computations
#      are looked up by lineage hash and returned from the database
#
# By default, @thunk auto-unwraps inputs: if you pass a ThunkOutput or a
# BaseVariable, the function receives the raw .data value. This keeps
# processing logic clean — no framework types leak into your math.
# =============================================================================

@thunk
def load_csv(path: str) -> np.ndarray:
    """
    Load a single-column CSV file and return values as a numpy array.

    Even file I/O functions benefit from @thunk — the lineage record captures
    the file path as a constant, so you can trace any downstream result back
    to its source file.
    """
    df = pd.read_csv(path)
    return df.iloc[:, 0].values


@thunk
def combine_signals(
    time: np.ndarray,
    hr: np.ndarray,
    vo2: np.ndarray,
) -> pd.DataFrame:
    """
    Combine three 1D arrays into a single DataFrame.

    Because @thunk auto-unwraps inputs, `time`, `hr`, and `vo2` arrive as
    plain numpy arrays even though we pass ThunkOutputs from load_csv().
    The lineage system still tracks that these inputs came from those
    load_csv() calls.
    """
    return pd.DataFrame({
        "time_sec": time,
        "heart_rate_bpm": hr,
        "vo2_ml_min": vo2,
    })


@thunk
def compute_rolling_vo2(
    combined: pd.DataFrame,
    window_seconds: int = 30,
    sample_interval: int = 5,
) -> np.ndarray:
    """
    Compute a rolling average of VO2 over a specified time window.

    The constant arguments (window_seconds, sample_interval) are captured
    in the lineage record. If you change the window size, the lineage hash
    changes, so the framework knows it's a different computation and won't
    serve a stale cached result.
    """
    window_size = window_seconds // sample_interval  # 30s / 5s = 6 samples
    rolling_avg = (
        pd.Series(combined["vo2_ml_min"])
        .rolling(window=window_size, min_periods=1)
        .mean()
    )
    return rolling_avg.values


@thunk
def compute_max_hr(combined: pd.DataFrame) -> float:
    """Extract the peak heart rate from the combined dataset."""
    return float(combined["heart_rate_bpm"].max())


@thunk
def compute_max_vo2(rolling_vo2: np.ndarray) -> float:
    """
    Compute VO2 max as the mean of the two highest 30-second rolling averages.

    This is a standard definition in exercise physiology: VO2max is reported
    as the average of the two highest consecutive 30-second averages to reduce
    the impact of breath-by-breath noise.
    """
    sorted_vals = np.sort(rolling_vo2)[::-1]  # Descending
    return float(np.mean(sorted_vals[:2]))


# =============================================================================
# STEP 3: CONFIGURE DATABASE AND RUN PIPELINE
# =============================================================================

if __name__ == "__main__":

    # -------------------------------------------------------------------------
    # 3a. Configure the database  [scidb.configure_database]
    #
    # configure_database() sets up two storage backends in a single call:
    #   - DuckDB file: stores all variable data (via SciDuck backend)
    #   - SQLite file: stores lineage/provenance records (via PipelineDB)
    #
    # dataset_schema_keys defines which metadata keys represent the "location"
    # of data. Here, "subject" identifies which person's test this is. Any
    # other metadata keys become version parameters that distinguish different
    # computational versions of the same data.
    #
    # This call also:
    #   - Auto-registers all BaseVariable subclasses defined above
    #   - Enables thunk caching (sets Thunk.query to the DatabaseManager)
    # -------------------------------------------------------------------------

    project_folder = Path(__file__).parent

    data_dir = project_folder / "data"
    db_dir = project_folder

    data_filename = "vo2max_data.duckdb"

    # Clean up previous runs for a fresh demo
    for f in db_dir.glob(f"{data_filename}*"):
        f.unlink()

    db = configure_database(
        dataset_db_path=db_dir / data_filename,
        dataset_schema_keys=["subject"],
    )

    print("Database configured.")
    print(f"  Data storage (DuckDB): {db_dir / data_filename}")
    print(f"  Schema keys: {db.dataset_schema_keys}")
    print()

    # -------------------------------------------------------------------------
    # 3b. Load raw data from CSVs  [@thunk + BaseVariable.save]
    #
    # load_csv() is @thunk-decorated, so each call returns a ThunkOutput
    # (not a raw array). The ThunkOutput wraps:
    #   .data          — the actual numpy array
    #   .pipeline_thunk — metadata about the function call and its inputs
    #   .hash          — a lineage-based hash for cache lookups
    #
    # When you pass a ThunkOutput to BaseVariable.save(), the framework:
    #   1. Extracts the raw data
    #   2. Extracts the lineage record (function name, hash, inputs)
    #   3. Stores both in the database
    #   4. Registers the lineage hash for future cache lookups
    # -------------------------------------------------------------------------

    print("--- Loading raw data from CSVs ---")

    time_data = load_csv(str(data_dir / "time_sec.csv"))
    hr_data = load_csv(str(data_dir / "heart_rate_bpm.csv"))
    vo2_data = load_csv(str(data_dir / "vo2_ml_min.csv"))

    # Each result is a ThunkOutput, not a plain numpy array
    print(f"  time_data type:      {type(time_data).__name__}")
    print(f"  time_data.data shape: {time_data.data.shape}")
    print(f"  HR range:  [{hr_data.data.min():.0f}, {hr_data.data.max():.0f}] bpm")
    print(f"  VO2 range: [{vo2_data.data.min():.0f}, {vo2_data.data.max():.0f}] mL/min")

    # Save raw data to the database with metadata
    RawTime.save(time_data, subject="S01")
    RawHeartRate.save(hr_data, subject="S01")
    RawVO2.save(vo2_data, subject="S01")

    print("  Saved raw data for subject S01.")
    print()

    # -------------------------------------------------------------------------
    # 3c. Combine signals  [@thunk chaining + ThunkOutput flow]
    #
    # Passing ThunkOutputs to another @thunk function automatically extends
    # the lineage chain. combine_signals() receives raw numpy arrays (auto-
    # unwrapped by @thunk), but the framework records that its inputs came
    # from the three load_csv() calls.
    # -------------------------------------------------------------------------

    print("--- Combining signals ---")

    combined = combine_signals(time_data, hr_data, vo2_data)

    print(f"  Combined shape: {combined.data.shape}")
    print(f"  Columns: {list(combined.data.columns)}")

    # Save with custom to_db() serialization (CombinedData overrides it)
    CombinedData.save(combined, subject="S01")
    print("  Saved combined data.")
    print()

    # -------------------------------------------------------------------------
    # 3d. Compute rolling VO2 averages  [@thunk with constant parameters]
    #
    # window_seconds=30 and sample_interval=5 are captured as constants in
    # the lineage record. Changing them produces a different lineage hash,
    # so cached results from other window sizes won't be reused.
    # -------------------------------------------------------------------------

    print("--- Computing 30-second rolling VO2 averages ---")

    rolling_vo2 = compute_rolling_vo2(combined, window_seconds=30, sample_interval=5)

    print(f"  Rolling VO2 shape: {rolling_vo2.data.shape}")
    print(f"  Rolling VO2 range: [{rolling_vo2.data.min():.0f}, {rolling_vo2.data.max():.0f}] mL/min")

    RollingVO2.save(rolling_vo2, subject="S01")
    print("  Saved rolling VO2 averages.")
    print()

    # -------------------------------------------------------------------------
    # 3e. Compute peak metrics  [@thunk with scalar results]
    #
    # Scalar results (float, int) are stored natively by SciDuck — no need
    # for custom to_db()/from_db() overrides on MaxHeartRate or MaxVO2.
    # -------------------------------------------------------------------------

    print("--- Computing peak metrics ---")

    max_hr = compute_max_hr(combined)
    max_vo2 = compute_max_vo2(rolling_vo2)

    print(f"  Max HR:  {max_hr.data:.0f} bpm")
    print(f"  Max VO2: {max_vo2.data:.1f} mL/min")

    MaxHeartRate.save(max_hr, subject="S01")
    MaxVO2.save(max_vo2, subject="S01")
    print("  Saved peak metrics.")
    print()

    # -------------------------------------------------------------------------
    # 3f. Verify: Load data back  [BaseVariable.load]
    #
    # load() queries by metadata and returns the latest matching record.
    # The loaded variable has .data, .record_id, .metadata, and .lineage_hash.
    # -------------------------------------------------------------------------

    print("--- Verifying saved data ---")

    loaded_max_vo2 = MaxVO2.load(subject="S01")
    loaded_max_hr = MaxHeartRate.load(subject="S01")
    loaded_combined = CombinedData.load(subject="S01")

    print(f"  Loaded Max VO2:  {loaded_max_vo2.data:.1f} mL/min  (record: {loaded_max_vo2.record_id[:16]}...)")
    print(f"  Loaded Max HR:   {loaded_max_hr.data:.0f} bpm  (record: {loaded_max_hr.record_id[:16]}...)")
    print(f"  Loaded combined: {loaded_combined.data.shape}  (record: {loaded_combined.record_id[:16]}...)")
    print(f"  Max VO2 lineage hash: {loaded_max_vo2.lineage_hash[:16]}...")
    print()

    # -------------------------------------------------------------------------
    # 3g. Query provenance  [DatabaseManager.get_provenance]
    #
    # Provenance tells you which function and inputs produced a saved result.
    # This is stored in the SQLite lineage database (PipelineDB backend).
    # -------------------------------------------------------------------------

    print("--- Provenance ---")

    prov = db.get_provenance(MaxVO2, subject="S01")
    if prov:
        print(f"  MaxVO2 was computed by: {prov['function_name']}()")
        print(f"  Function hash: {prov['function_hash'][:16]}...")
        print(f"  Inputs:")
        for inp in prov["inputs"]:
            print(f"    - {inp.get('name', '?')}: kind={inp.get('kind', '?')}, type={inp.get('type', '?')}")
        for const in prov.get("constants", []):
            print(f"    - {const.get('name', '?')}: value={const.get('value_repr', '?')} (constant)")

    prov_hr = db.get_provenance(MaxHeartRate, subject="S01")
    if prov_hr:
        print(f"  MaxHeartRate was computed by: {prov_hr['function_name']}()")
    print()

    # -------------------------------------------------------------------------
    # 3h. Demonstrate caching  [Thunk.query / lineage-based cache]
    #
    # Re-running the same computation with the same inputs produces the same
    # lineage hash. The framework detects this and returns the cached result
    # from the database instead of re-executing the function.
    # -------------------------------------------------------------------------

    print("--- Caching demo ---")
    print("  Re-loading saved variables and re-running the pipeline...")

    # Re-load saved variables from the database
    reloaded_time = RawTime.load(subject="S01")
    reloaded_hr = RawHeartRate.load(subject="S01")
    reloaded_vo2 = RawVO2.load(subject="S01")

    # Re-run the pipeline — the framework checks for cache hits by lineage hash
    combined_2 = combine_signals(reloaded_time, reloaded_hr, reloaded_vo2)
    rolling_2 = compute_rolling_vo2(combined_2, window_seconds=30, sample_interval=5)
    max_vo2_2 = compute_max_vo2(rolling_2)

    print(f"  Re-computed Max VO2: {max_vo2_2.data:.1f} mL/min")
    print(f"  Original Max VO2:   {loaded_max_vo2.data:.1f} mL/min")
    print(f"  Values match: {abs(max_vo2_2.data - loaded_max_vo2.data) < 0.01}")
    print()

    # -------------------------------------------------------------------------
    # 3i. List all saved versions  [BaseVariable.list_versions]
    # -------------------------------------------------------------------------

    print("--- Saved versions ---")
    for var_type in [RawTime, RawHeartRate, RawVO2, CombinedData,
                     RollingVO2, MaxHeartRate, MaxVO2]:
        versions = var_type.list_versions(subject="S01")
        print(f"  {var_type.__name__}: {len(versions)} version(s)")

    print()
    print("Pipeline complete!")
    print(f"  Data stored in:    {db_dir / 'vo2max_data.duckdb'}")
    print(f"  Lineage stored in: {db_dir / 'vo2max_lineage.db'}")

    db.close()
