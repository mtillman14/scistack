#!/usr/bin/env python3
"""
SciStack Pipeline Demo
===================

This script demonstrates all features of the scidb framework:

Phase 1 - Core Infrastructure:
  - BaseVariable subclasses with to_db()/from_db()
  - DatabaseManager and configure_database()
  - save() and load() with metadata
  - Version history with list_versions()

Phase 2 - Thunk System & Lineage:
  - @thunk decorator for automatic lineage tracking
  - Lineage extraction and storage
  - get_provenance() to query what produced a variable
  - get_derived_from() to query what used a variable

Phase 3 - Computation Caching:
  - Automatic cache population on save
  - check_cache() for cache lookup before execution
  - was_cached property on ThunkOutput
  - Cache stats and invalidation

Run this script and step through it line by line to see all features in action!
"""

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

# Import everything from scidb
from scidb import (
    BaseVariable,
    configure_database,
    thunk,
    check_cache,
    PipelineThunk,
)


# =============================================================================
# STEP 1: Define Variable Types (Phase 1)
# =============================================================================
# Each variable type defines how to serialize/deserialize its data

class RawData(BaseVariable):
    """Represents raw CSV data as a DataFrame."""
    schema_version = 1

    def to_db(self) -> pd.DataFrame:
        # Store the DataFrame directly
        return self.data

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df


class TimeSeries(BaseVariable):
    """Represents a 1D time series as a numpy array."""
    schema_version = 1

    def to_db(self) -> pd.DataFrame:
        return pd.DataFrame({
            "index": range(len(self.data)),
            "value": self.data,
        })

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> np.ndarray:
        return df.sort_values("index")["value"].values


class Statistics(BaseVariable):
    """Represents computed statistics as a dict."""
    schema_version = 1

    def to_db(self) -> pd.DataFrame:
        # Convert dict to single-row DataFrame
        return pd.DataFrame([self.data])

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> dict:
        return df.iloc[0].to_dict()


class NormalizedSeries(BaseVariable):
    """Represents a normalized time series."""
    schema_version = 1

    def to_db(self) -> pd.DataFrame:
        return pd.DataFrame({
            "index": range(len(self.data)),
            "value": self.data,
        })

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> np.ndarray:
        return df.sort_values("index")["value"].values


# =============================================================================
# STEP 2: Define Processing Functions with @thunk (Phase 2)
# =============================================================================
# The @thunk decorator enables automatic lineage tracking

@thunk()
def extract_column(df: pd.DataFrame, column: str) -> np.ndarray:
    """Extract a single column from a DataFrame as a numpy array."""
    print(f"  [COMPUTING] Extracting column '{column}'...")
    return df[column].values


@thunk()
def normalize(data: np.ndarray) -> np.ndarray:
    """Normalize data to [0, 1] range."""
    print(f"  [COMPUTING] Normalizing data (min={data.min():.2f}, max={data.max():.2f})...")
    min_val = data.min()
    max_val = data.max()
    return (data - min_val) / (max_val - min_val)


@thunk()
def compute_stats(data: np.ndarray) -> dict:
    """Compute statistics on data."""
    print(f"  [COMPUTING] Computing statistics...")
    return {
        "mean": float(np.mean(data)),
        "std": float(np.std(data)),
        "min": float(np.min(data)),
        "max": float(np.max(data)),
        "count": int(len(data)),
    }


@thunk()
def smooth(data: np.ndarray, window: int) -> np.ndarray:
    """Apply moving average smoothing."""
    print(f"  [COMPUTING] Smoothing with window={window}...")
    kernel = np.ones(window) / window
    # Use 'same' mode to preserve length
    return np.convolve(data, kernel, mode='same')


# =============================================================================
# STEP 3: Create Sample Data
# =============================================================================

def create_sample_csv(path: Path) -> None:
    """Create a sample CSV with time series data."""
    np.random.seed(42)
    n_points = 100

    # Create time series with different characteristics
    time = np.arange(n_points)
    temperature = 20 + 5 * np.sin(2 * np.pi * time / 24) + np.random.randn(n_points) * 0.5
    humidity = 60 + 10 * np.cos(2 * np.pi * time / 24) + np.random.randn(n_points) * 2
    pressure = 1013 + np.random.randn(n_points) * 5

    df = pd.DataFrame({
        "time": time,
        "temperature": temperature,
        "humidity": humidity,
        "pressure": pressure,
    })

    df.to_csv(path, index=False)
    print(f"Created sample CSV at: {path}")


# =============================================================================
# MAIN DEMO
# =============================================================================

def main():
    print("=" * 70)
    print("SciStack Pipeline Demo")
    print("=" * 70)

    # Create temporary directory for demo
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        csv_path = tmpdir / "sensor_data.csv"
        db_path = tmpdir / "experiment.db"

        # Create sample data
        print("\n[1] Creating sample CSV data...")
        create_sample_csv(csv_path)

        # ---------------------------------------------------------------------
        # PHASE 1: Core Infrastructure
        # ---------------------------------------------------------------------
        print("\n" + "=" * 70)
        print("PHASE 1: Core Infrastructure")
        print("=" * 70)

        # Configure database
        print("\n[2] Configuring database...")
        db = configure_database(db_path)
        print(f"Database created at: {db_path}")

        # Register variable types
        print("\n[3] Registering variable types...")
        db.register(RawData)
        db.register(TimeSeries)
        db.register(Statistics)
        db.register(NormalizedSeries)
        print("Registered: RawData, TimeSeries, Statistics, NormalizedSeries")

        # Load and save raw data
        print("\n[4] Loading CSV and saving as RawData...")
        raw_df = pd.read_csv(csv_path)
        print(f"Loaded DataFrame with shape: {raw_df.shape}")
        print(f"Columns: {list(raw_df.columns)}")

        raw_record_id = RawData.save(raw_df,
            db=db,
            source="sensor_data.csv",
            experiment="demo",
            subject=1,
        )
        print(f"Saved RawData with record_id: {raw_record_id[:16]}...")

        # Demonstrate load
        print("\n[5] Loading RawData back from database...")
        loaded_raw = RawData.load(db=db, source="sensor_data.csv")
        print(f"Loaded DataFrame matches original: {loaded_raw.data.equals(raw_df)}")
        print(f"Loaded record_id: {loaded_raw.record_id[:16]}...")
        print(f"Loaded metadata: {loaded_raw.metadata}")

        # ---------------------------------------------------------------------
        # PHASE 2: Thunk System & Lineage
        # ---------------------------------------------------------------------
        print("\n" + "=" * 70)
        print("PHASE 2: Thunk System & Lineage")
        print("=" * 70)

        # Process temperature column
        print("\n[6] Processing temperature column with thunks...")
        print("    Running: extract_column -> normalize -> compute_stats")

        # Each thunk call returns an ThunkOutput with lineage info
        temp_series = extract_column(loaded_raw.data, "temperature")
        print(f"    extract_column returned: {type(temp_series).__name__}")

        temp_normalized = normalize(temp_series)
        print(f"    normalize returned: {type(temp_normalized).__name__}")

        temp_stats = compute_stats(temp_normalized)
        print(f"    compute_stats returned: {type(temp_stats).__name__}")

        # Save the results - lineage is automatically captured!
        print("\n[7] Saving results (lineage captured automatically)...")

        ts_record_id = TimeSeries.save(temp_series, db=db, column="temperature", stage="raw", subject=1)
        print(f"    Saved TimeSeries (raw): {ts_record_id[:16]}...")

        norm_record_id = NormalizedSeries.save(temp_normalized, db=db, column="temperature", stage="normalized", subject=1)
        print(f"    Saved NormalizedSeries: {norm_record_id[:16]}...")

        stats_record_id = Statistics.save(temp_stats, db=db, column="temperature", stage="stats", subject=1)
        print(f"    Saved Statistics: {stats_record_id[:16]}...")

        # Query provenance
        print("\n[8] Querying provenance (what produced each variable?)...")

        provenance = db.get_provenance(Statistics, column="temperature", stage="stats")
        if provenance:
            print(f"    Statistics was produced by: {provenance['function_name']}")
            print(f"    Function hash: {provenance['function_hash'][:16]}...")
            print(f"    Inputs: {len(provenance['inputs'])} thunk outputs")
            print(f"    Constants: {len(provenance['constants'])} values")

        # Query derived-from relationships
        print("\n[9] Querying derived-from (what used the raw TimeSeries?)...")

        derived = db.get_derived_from(TimeSeries, column="temperature", stage="raw")
        print(f"    Variables derived from raw TimeSeries: {len(derived)}")
        for d in derived:
            print(f"      - {d['type']} via {d['function']}")

        # ---------------------------------------------------------------------
        # PHASE 3: Computation Caching
        # ---------------------------------------------------------------------
        print("\n" + "=" * 70)
        print("PHASE 3: Computation Caching")
        print("=" * 70)

        # Check cache stats
        print("\n[10] Checking cache statistics...")
        stats = db.get_cache_stats()
        print(f"    Total cache entries: {stats['total_entries']}")
        print(f"    Cached functions: {stats['functions']}")
        for fn, count in stats['entries_by_function'].items():
            print(f"      - {fn}: {count} entries")

        # Demonstrate cache hit
        print("\n[11] Demonstrating cache lookup...")
        print("    Re-running the same computation...")

        # Run the same computation again
        temp_series_2 = extract_column(loaded_raw.data, "temperature")
        temp_normalized_2 = normalize(temp_series_2)
        temp_stats_2 = compute_stats(temp_normalized_2)

        # Check if it's cached
        cached = check_cache(temp_stats_2.pipeline_thunk, Statistics, db=db)
        if cached:
            print(f"    CACHE HIT! Found cached result.")
            print(f"    was_cached: {cached.was_cached}")
            print(f"    cached_id: {cached.cached_id[:16]}...")
            print(f"    Value matches: {cached.data == temp_stats_2.data}")
        else:
            print("    Cache miss (unexpected)")

        # Demonstrate cache miss with different inputs
        print("\n[12] Demonstrating cache miss with different inputs...")
        print("    Processing humidity column (not cached yet)...")

        humid_series = extract_column(loaded_raw.data, "humidity")
        humid_normalized = normalize(humid_series)

        cached = check_cache(humid_normalized.pipeline_thunk, NormalizedSeries, db=db)
        if cached:
            print("    Cache hit (unexpected)")
        else:
            print("    CACHE MISS! This is a new computation.")
            print("    Saving to populate cache...")
            NormalizedSeries.save(humid_normalized,
                db=db, column="humidity", stage="normalized", subject=1
            )

        # Verify it's now cached
        cached = check_cache(humid_normalized.pipeline_thunk, NormalizedSeries, db=db)
        if cached:
            print("    Now cached!")

        # Show updated cache stats
        print("\n[13] Updated cache statistics...")
        stats = db.get_cache_stats()
        print(f"    Total cache entries: {stats['total_entries']}")
        for fn, count in stats['entries_by_function'].items():
            print(f"      - {fn}: {count} entries")

        # ---------------------------------------------------------------------
        # Version History
        # ---------------------------------------------------------------------
        print("\n" + "=" * 70)
        print("BONUS: Version History")
        print("=" * 70)

        print("\n[14] Creating multiple versions of the same variable...")

        # Smooth with different window sizes
        for window in [3, 5, 7]:
            smoothed = smooth(temp_series, window)
            TimeSeries.save(smoothed,
                db=db,
                column="temperature",
                stage="smoothed",
                window=window,
                subject=1,
            )
            print(f"    Saved smoothed (window={window})")

        # List all versions
        print("\n[15] Listing all versions of smoothed temperature...")
        versions = db.list_versions(
            TimeSeries,
            column="temperature",
            stage="smoothed",
        )
        print(f"    Found {len(versions)} versions:")
        for v in versions:
            print(f"      - record_id: {v['record_id'][:16]}... window={v['metadata'].get('window')}")

        # Load specific version
        print("\n[16] Loading specific version (window=5)...")
        loaded = TimeSeries.load(db=db, column="temperature", stage="smoothed", window=5)
        print(f"    Loaded record_id: {loaded.record_id[:16]}...")
        print(f"    Data shape: {loaded.data.shape}")

        # ---------------------------------------------------------------------
        # Summary
        # ---------------------------------------------------------------------
        print("\n" + "=" * 70)
        print("DEMO COMPLETE!")
        print("=" * 70)
        print("""
Features demonstrated:
  ✓ BaseVariable subclasses (RawData, TimeSeries, Statistics, NormalizedSeries)
  ✓ Database configuration and type registration
  ✓ Save and load with metadata
  ✓ @thunk decorator for lineage tracking
  ✓ Automatic lineage extraction on save
  ✓ Provenance queries (get_provenance, get_derived_from)
  ✓ Computation caching (automatic population)
  ✓ Cache lookup with check_cache()
  ✓ Cache statistics
  ✓ Version history with list_versions()
  ✓ Loading specific versions
        """)


if __name__ == "__main__":
    main()
