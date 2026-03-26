"""
SciStack Debug Example 1: Core Concepts
=====================================

This script demonstrates the fundamental building blocks of scidb:
- Defining custom variable types
- Saving and loading data with metadata
- Basic lineage tracking with @thunk
- Querying provenance

Run this script in debug mode and step through to understand how scidb works.
Set a breakpoint at line 30 to start exploring.

Documentation: See docs/quickstart.md for the getting started guide.
"""

import numpy as np
from pathlib import Path

# -----------------------------------------------------------------------------
# STEP 1: Import scidb components
# Documentation: See docs/api.md for the complete API reference
# -----------------------------------------------------------------------------

from scidb import (    
    configure_database,  # Configure the global database
)

from vars import * # Import all of the variables from vars.py
from functions import * # Import all of the functions from functions.py

# Set a breakpoint here to start debugging
print("Starting scidb debug example 1: Core Concepts")


# -----------------------------------------------------------------------------
# STEP 2: Configure the Database
# Documentation: See docs/guide/database.md for configuration options
#
# Key concept: configure_database() creates/opens a SQLite database and
# sets it as the global database for all scidb operations.
# -----------------------------------------------------------------------------

# Create a temporary database for this example
db_path = Path("debug_example1.db")
if db_path.exists():
    db_path.unlink()  # Start fresh for debugging

# This creates the database and sets up internal tables
# Set a breakpoint here to inspect the db object
db = configure_database(str(db_path))
print(f"Database configured: {db_path}")


# -----------------------------------------------------------------------------
# STEP 3: Create and Save Raw Data
# Documentation: See docs/guide/variables.md section "Saving Variables"
#
# Key concepts:
# - Metadata keys (sensor, trial, condition) become queryable attributes
# - The record_id is computed from: class name + schema + content + metadata
# - Same data + metadata = same record_id (idempotent saves)
# -----------------------------------------------------------------------------

# Generate synthetic sensor data
np.random.seed(42)
raw_data = np.sin(np.linspace(0, 4*np.pi, 100)) + np.random.normal(0, 0.1, 100)

# Create a SensorReading variable
# Set a breakpoint here to inspect the variable before saving
reading = SensorReading(raw_data)

# Inspect key properties before saving:
# - reading.data: The raw numpy array
# - reading.content_hash: Hash of just the data content
# - reading.metadata: Empty dict before save

print(f"Data shape: {reading.data.shape}")
print(f"Content hash: {reading.content_hash[:16]}...")

# Save with metadata - this is how you address data in scidb
# Metadata design is flexible: use keys that match your experimental design
# Set a breakpoint on the save() call to step into the save process
record_id = SensorReading.save(raw_data, sensor="accelerometer", trial=1, condition="baseline")

print(f"Saved with record_id: {record_id}")


# -----------------------------------------------------------------------------
# STEP 4: Load Data by Metadata
# Documentation: See docs/guide/database.md section "Loading Variables"
#
# Key concept: load() queries by metadata and returns the most recent match.
# You can load with partial metadata - scidb finds the latest matching version.
# -----------------------------------------------------------------------------

# Load the data back using metadata query
# Set a breakpoint here to see how loading works
loaded_reading = SensorReading.load(sensor="accelerometer", trial=1)

print(f"Loaded data shape: {loaded_reading.data.shape}")
print(f"Data matches: {np.allclose(raw_data, loaded_reading.data)}")

# The loaded variable has the same record_id as what we saved
print(f"Loaded record_id: {loaded_reading.record_id}")


# -----------------------------------------------------------------------------
# STEP 5: Run the Processing Pipeline
# Documentation: See docs/guide/lineage.md section "Chained Pipelines"
#
# Key concept: When you call a @thunk function:
# - It executes the function
# - Returns an ThunkOutput wrapping the result
# - ThunkOutput.data gives you the actual computed value
# - ThunkOutput carries lineage info (what function + inputs produced it)
# -----------------------------------------------------------------------------

# Load the raw data (as a BaseVariable with lineage tracking)
raw = SensorReading.load(sensor="accelerometer", trial=1)

# Step 1: Apply moving average filter
# Set a breakpoint here to inspect the ThunkOutput
smoothed = apply_moving_average(raw, window_size=5)

# smoothed is an ThunkOutput, not a numpy array
# - smoothed.data: The actual numpy array result
# - smoothed.pipeline_thunk: Info about the function call
# - smoothed.was_cached: Whether this came from cache (False on first run)

print(f"\nSmoothed result type: {type(smoothed)}")
print(f"Smoothed data shape: {smoothed.data.shape}")
print(f"Was cached: {smoothed.was_cached}")

# Step 2: Normalize the smoothed signal
# Note: We pass the ThunkOutput directly - @thunk auto-unwraps it
normalized = normalize_signal(smoothed)

print(f"Normalized data range: [{normalized.data.min():.3f}, {normalized.data.max():.3f}]")

# Step 3: Compute statistics on the normalized signal
stats = compute_statistics(normalized)

print(f"Statistics: {stats.data}")


# -----------------------------------------------------------------------------
# STEP 6: Save Results with Lineage
# Documentation: See docs/guide/lineage.md section "Saving with Lineage"
#
# Key concept: When you save an ThunkOutput result:
# - The data is stored in the variable's table
# - The lineage (what function + inputs) is stored in _lineage table
# - The computation is cached in _computation_cache table
#
# This enables provenance queries and automatic cache hits on re-runs.
# -----------------------------------------------------------------------------

# Save the processed signal
# Set a breakpoint here to step through save_with_lineage
processed_record_id = ProcessedSignal.save(normalized.data,
    sensor="accelerometer",
    trial=1,
    condition="baseline",
    processing="normalized"
)

# Save with lineage tracking (pass the ThunkOutput)
# This stores BOTH the data AND the computation that produced it
processed_lineage_record_id = ProcessedSignal.save(normalized,
    sensor="accelerometer",
    trial=1,
    condition="baseline",
    processing="normalized_with_lineage"
)

print(f"\nSaved processed signal: {processed_lineage_record_id}")

# Save the statistics
stats_record_id = SignalStatistics.save(stats,
    sensor="accelerometer",
    trial=1,
    condition="baseline",
    stat_type="summary"
)

print(f"Saved statistics: {stats_record_id}")


# -----------------------------------------------------------------------------
# STEP 7: Query Provenance
# Documentation: See docs/guide/lineage.md section "Querying Provenance"
#
# Key concept: After saving with lineage, you can ask:
# - What computation produced this variable? (get_provenance)
# - What was the full chain of computations? (get_full_lineage)
# - What variables were derived from this one? (get_derived_from)
# -----------------------------------------------------------------------------

# Get provenance for the processed signal
# Set a breakpoint here to explore the provenance data structure
provenance = db.get_provenance(
    ProcessedSignal,
    sensor="accelerometer",
    trial=1,
    processing="normalized_with_lineage"
)

if provenance:
    print(f"\nProvenance for processed signal:")
    print(f"  Function: {provenance.get('function_name')}")
    print(f"  Function hash: {provenance.get('function_hash', 'N/A')[:16]}...")
    print(f"  Inputs: {provenance.get('inputs', [])}")

# Get full lineage (recursive chain of computations)
lineage = db.get_full_lineage(
    ProcessedSignal,
    sensor="accelerometer",
    trial=1,
    processing="normalized_with_lineage"
)

print(f"\nFull lineage depth: {len(str(lineage))} chars")

# Format lineage as a readable tree
formatted = db.format_lineage(
    ProcessedSignal,
    sensor="accelerometer",
    trial=1,
    processing="normalized_with_lineage"
)

print(f"\nLineage tree:\n{formatted}")


# -----------------------------------------------------------------------------
# STEP 8: Version History
# Documentation: See docs/guide/database.md section "Version History"
#
# Key concept: list_versions() shows all saved versions of a variable type,
# with metadata and timestamps. Useful for auditing and debugging.
# -----------------------------------------------------------------------------

# List all versions of SensorReading
print("\n--- Version History ---")
versions = SensorReading.list_versions()
print(f"SensorReading versions: {len(versions)}")
for v in versions:
    print(f"  {v['record_id'][:12]}... | {v.get('sensor')} | trial={v.get('trial')}")

versions = ProcessedSignal.list_versions()
print(f"ProcessedSignal versions: {len(versions)}")
for v in versions:
    print(f"  {v['record_id'][:12]}... | processing={v.get('processing')}")


# -----------------------------------------------------------------------------
# CLEANUP
# -----------------------------------------------------------------------------

print("\n--- Debug Example 1 Complete ---")
print(f"Database saved to: {db_path.absolute()}")
print("You can inspect the database with any SQLite browser.")
print("Key tables: SensorReading, ProcessedSignal, SignalStatistics, _lineage, _computation_cache")
