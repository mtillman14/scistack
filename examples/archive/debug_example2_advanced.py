"""
SciStack Debug Example 2: Advanced Features
=========================================

This script demonstrates advanced scidb capabilities:
- Automatic computation caching
- Multi-output thunk functions
- Wrapping external library functions
- Variable type inheritance (specialization)
- Batch operations
- Cache management and statistics

Run this script in debug mode after running debug_example1_basics.py.
Set a breakpoint at line 35 to start exploring.

Documentation: See docs/guide/caching.md for caching details.
"""

import numpy as np
import pandas as pd
from pathlib import Path
from scipy import signal as scipy_signal  # External library to wrap

# -----------------------------------------------------------------------------
# STEP 1: Import scidb components
# Documentation: See docs/api.md for the complete API reference
# -----------------------------------------------------------------------------

from scidb import (
    BaseVariable,
    configure_database,
    get_database,
    thunk,
    Thunk,             # For wrapping external functions
    extract_lineage,   # For inspecting lineage records
    check_cache,       # For manual cache checking
)
from scidb.database import DatabaseManager

# Set a breakpoint here to start debugging
print("Starting scidb debug example 2: Advanced Features")


# -----------------------------------------------------------------------------
# STEP 2: Variable Type Inheritance (Specialization)
# Documentation: See docs/guide/variables.md section "Type Specialization"
#
# Key concept: You can create a hierarchy of variable types:
# - Base class defines to_db/from_db once
# - Subclasses inherit storage logic but get separate database tables
# - Useful for distinguishing data at different processing stages
# -----------------------------------------------------------------------------

class TimeSeries(BaseVariable):
    """
    Base class for all time series data.

    Subclasses will inherit to_db/from_db but store in separate tables.
    This pattern is documented in docs/guide/variables.md.
    """
    schema_version = 1

    def to_db(self) -> pd.DataFrame:
        return pd.DataFrame({'value': self.data})

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> np.ndarray:
        return df['value'].values


# These subclasses inherit storage logic but are stored separately
class RawSignal(TimeSeries):
    """Raw unprocessed signal - stored in 'RawSignal' table"""
    pass


class FilteredSignal(TimeSeries):
    """Band-pass filtered signal - stored in 'FilteredSignal' table"""
    pass


class EnvelopeSignal(TimeSeries):
    """Signal envelope - stored in 'EnvelopeSignal' table"""
    pass


class AnalysisResult(BaseVariable):
    """
    Stores analysis results as a dictionary.

    Documentation: See docs/guide/variables.md section "Dictionary Pattern"
    """
    schema_version = 1

    def to_db(self) -> pd.DataFrame:
        return pd.DataFrame([self.data])

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> dict:
        return df.iloc[0].to_dict()


# -----------------------------------------------------------------------------
# STEP 3: Configure Database
# Documentation: See docs/guide/database.md
# -----------------------------------------------------------------------------

db_path = Path("debug_example2.db")
if db_path.exists():
    db_path.unlink()

db = configure_database(str(db_path))
print(f"Database configured: {db_path}")


# -----------------------------------------------------------------------------
# STEP 4: Wrapping External Library Functions
# Documentation: See docs/guide/lineage.md section "External Functions"
#
# Key concept: Use Thunk() to wrap functions from external libraries
# so they participate in lineage tracking.
# -----------------------------------------------------------------------------

# Wrap scipy's butter filter design function
# This returns filter coefficients (2 arrays: b and a)
butter_filter = Thunk(scipy_signal.butter, unpack_output=True)

# Wrap scipy's filtfilt (zero-phase filtering)
filtfilt = Thunk(scipy_signal.filtfilt)

# Wrap scipy's hilbert transform for envelope detection
hilbert = Thunk(scipy_signal.hilbert)

print("Wrapped external functions: butter, filtfilt, hilbert")


# -----------------------------------------------------------------------------
# STEP 5: Define Multi-Output Thunk Functions
# Documentation: See docs/guide/lineage.md section "Multi-Output Functions"
#
# Key concept: Set unpack_output=True for functions returning multiple values.
# Each output becomes a separate ThunkOutput with its own lineage.
# -----------------------------------------------------------------------------

@thunk(unpack_output=True)
def split_signal(signal: np.ndarray, split_point: int) -> tuple:
    """
    Split a signal into two parts.

    Returns two separate ThunkOutputs, each with independent lineage.
    Both must be saved for caching to work (docs/guide/caching.md).
    """
    return signal[:split_point], signal[split_point:]


@thunk(unpack_output=True)
def analyze_segments(signal: np.ndarray) -> tuple:
    """
    Analyze signal in three segments: early, middle, late.

    Demonstrates 3-output thunk function.
    """
    n = len(signal)
    third = n // 3

    early = signal[:third]
    middle = signal[third:2*third]
    late = signal[2*third:]

    return (
        {'segment': 'early', 'mean': float(np.mean(early)), 'std': float(np.std(early))},
        {'segment': 'middle', 'mean': float(np.mean(middle)), 'std': float(np.std(middle))},
        {'segment': 'late', 'mean': float(np.mean(late)), 'std': float(np.std(late))}
    )


# -----------------------------------------------------------------------------
# STEP 6: Define Single-Output Processing Functions
# -----------------------------------------------------------------------------

@thunk()
def compute_envelope(analytic_signal: np.ndarray) -> np.ndarray:
    """
    Compute the envelope (magnitude) of an analytic signal.

    The analytic signal comes from scipy's hilbert transform.
    """
    return np.abs(analytic_signal)


@thunk()
def detect_peaks(signal: np.ndarray, threshold: float) -> dict:
    """
    Detect peaks above a threshold.

    Returns peak information as a dictionary.
    """
    peaks = np.where(signal > threshold)[0]
    return {
        'n_peaks': len(peaks),
        'peak_indices': peaks.tolist()[:10],  # First 10 for brevity
        'threshold': threshold,
        'max_value': float(np.max(signal)) if len(signal) > 0 else 0.0
    }


# -----------------------------------------------------------------------------
# STEP 7: Create and Save Raw Data
# -----------------------------------------------------------------------------

np.random.seed(123)
fs = 1000  # Sample rate
t = np.linspace(0, 1, fs)

# Create a signal with multiple frequency components
raw_data = (
    np.sin(2 * np.pi * 10 * t) +       # 10 Hz component
    0.5 * np.sin(2 * np.pi * 50 * t) +  # 50 Hz component
    0.2 * np.random.randn(fs)            # Noise
)

# Save raw signal
raw_record_id = RawSignal.save(raw_data, subject=1, session="morning", channel="EMG")
print(f"\nSaved raw signal: {raw_record_id[:12]}...")


# -----------------------------------------------------------------------------
# STEP 8: Build Processing Pipeline with External Functions
# Documentation: See docs/guide/lineage.md section "Chained Pipelines"
#
# Key concept: Chain wrapped external functions with your @thunk functions.
# The entire computation graph is tracked for provenance.
# -----------------------------------------------------------------------------

print("\n--- Processing Pipeline (First Run) ---")

# Load the raw signal
raw_loaded = RawSignal.load(subject=1, session="morning", channel="EMG")

# Design a bandpass filter (5-30 Hz) using wrapped scipy function
# Set a breakpoint here to inspect the filter coefficients
b, a = butter_filter(N=4, Wn=[5, 30], btype='band', fs=fs)

print(f"Filter coefficients - b type: {type(b)}, a type: {type(a)}")
print(f"b.data shape: {b.data.shape}, a.data shape: {a.data.shape}")

# Apply zero-phase filtering
filtered = filtfilt(b, a, raw_loaded)
print(f"Filtered signal shape: {filtered.data.shape}")

# Compute analytic signal (for envelope detection)
analytic = hilbert(filtered)
print(f"Analytic signal shape: {analytic.data.shape}")

# Compute envelope
envelope = compute_envelope(analytic)
print(f"Envelope shape: {envelope.data.shape}")


# -----------------------------------------------------------------------------
# STEP 9: Save Results to Populate Cache
# Documentation: See docs/guide/caching.md section "How Caching Works"
#
# Key concept: Saving an ThunkOutput automatically populates the cache.
# The cache key is computed from: function hash + input hashes.
# -----------------------------------------------------------------------------

# Save the filtered signal
filtered_record_id = FilteredSignal.save(filtered,
    subject=1,
    session="morning",
    channel="EMG",
    filter_type="bandpass_5_30Hz"
)
print(f"\nSaved filtered signal: {filtered_record_id[:12]}...")

# Save the envelope
envelope_record_id = EnvelopeSignal.save(envelope,
    subject=1,
    session="morning",
    channel="EMG",
    derived_from="bandpass_filtered"
)
print(f"Saved envelope: {envelope_record_id[:12]}...")

# Check cache statistics
# Set a breakpoint here to inspect cache state
stats = db.get_cache_stats()
print(f"\nCache statistics after first run:")
print(f"  Total entries: {stats['total_entries']}")
print(f"  Functions cached: {list(stats['entries_by_function'].keys())}")


# -----------------------------------------------------------------------------
# STEP 10: Demonstrate Cache Hits
# Documentation: See docs/guide/caching.md section "Automatic Cache Hits"
#
# Key concept: Re-running the same computation with identical inputs
# will hit the cache and skip execution. The was_cached property is True.
# -----------------------------------------------------------------------------

print("\n--- Processing Pipeline (Second Run - Cache Test) ---")

# Re-run the exact same pipeline
raw_loaded2 = RawSignal.load(subject=1, session="morning", channel="EMG")

# These should all hit the cache
b2, a2 = butter_filter(N=4, Wn=[5, 30], btype='band', fs=fs)
filtered2 = filtfilt(b2, a2, raw_loaded2)
analytic2 = hilbert(filtered2)
envelope2 = compute_envelope(analytic2)

# Set a breakpoint here to inspect was_cached flags
print(f"b2.was_cached: {b2.was_cached}")
print(f"a2.was_cached: {a2.was_cached}")
print(f"filtered2.was_cached: {filtered2.was_cached}")
print(f"analytic2.was_cached: {analytic2.was_cached}")
print(f"envelope2.was_cached: {envelope2.was_cached}")

# Verify the cached results match
print(f"\nResults match original:")
print(f"  Filter b: {np.allclose(b.data, b2.data)}")
print(f"  Filter a: {np.allclose(a.data, a2.data)}")
print(f"  Filtered: {np.allclose(filtered.data, filtered2.data)}")
print(f"  Envelope: {np.allclose(envelope.data, envelope2.data)}")


# -----------------------------------------------------------------------------
# STEP 11: Multi-Output Functions and Caching
# Documentation: See docs/guide/caching.md section "Multi-Output Caching"
#
# Key concept: For multi-output functions, ALL outputs must be saved
# before caching takes effect. This ensures complete results are cached.
# -----------------------------------------------------------------------------

print("\n--- Multi-Output Function Demo ---")

# Split the signal at the midpoint
left, right = split_signal(raw_loaded, split_point=500)

print(f"Left half shape: {left.data.shape}")
print(f"Right half shape: {right.data.shape}")
print(f"Left.was_cached: {left.was_cached}")
print(f"Right.was_cached: {right.was_cached}")

# Save BOTH outputs - required for caching to work
# Set a breakpoint here to see multi-output save
class SignalHalf(TimeSeries):
    """Half of a split signal"""
    pass

SignalHalf.save(left, subject=1, session="morning", channel="EMG", half="left")

SignalHalf.save(right, subject=1, session="morning", channel="EMG", half="right")

print("Saved both halves - cache now populated")

# Re-run split - should hit cache
left2, right2 = split_signal(raw_loaded, split_point=500)
print(f"\nAfter re-run:")
print(f"Left2.was_cached: {left2.was_cached}")
print(f"Right2.was_cached: {right2.was_cached}")


# -----------------------------------------------------------------------------
# STEP 12: Manual Cache Checking
# Documentation: See docs/guide/caching.md section "Manual Cache Operations"
#
# Key concept: check_cache() lets you query the cache before execution.
# Useful for conditional processing or debugging.
# -----------------------------------------------------------------------------

print("\n--- Manual Cache Check Demo ---")

# Create a thunk for analysis
@thunk()
def expensive_analysis(signal: np.ndarray) -> dict:
    """Simulates an expensive computation"""
    import time
    time.sleep(0.1)  # Simulate work
    return {'rms': float(np.sqrt(np.mean(signal**2)))}

# Run once to populate cache
result1 = expensive_analysis(raw_loaded)
AnalysisResult.save(result1, subject=1, session="morning", analysis="rms")

# Manually check if cache would hit
# Set a breakpoint here to inspect check_cache behavior
# Note: check_cache requires the pipeline_thunk, which is available after calling
result2 = expensive_analysis(raw_loaded)

if result2.was_cached:
    print("Cache hit! Skipped expensive computation.")
    print(f"Cached result: {result2.data}")
else:
    print("Cache miss - computation was executed.")


# -----------------------------------------------------------------------------
# STEP 13: Extract and Inspect Lineage Records
# Documentation: See docs/guide/lineage.md section "LineageRecord"
#
# Key concept: extract_lineage() returns a LineageRecord with details
# about the function, inputs, and constants used in a computation.
# -----------------------------------------------------------------------------

print("\n--- Lineage Inspection ---")

# Extract lineage from the envelope computation
lineage_record = extract_lineage(envelope)
db_manager = DatabaseManager(db_path=str(db_path))
full_lineage = db_manager.get_full_lineage(type(envelope.data), subject=1, session="morning", channel="EMG")

if lineage_record:
    print(f"Lineage for envelope computation:")
    print(f"  Function name: {lineage_record.function_name}")
    print(f"  Function hash: {lineage_record.function_hash[:16]}...")
    print(f"  Number of inputs: {len(lineage_record.inputs)}")
    print(f"  Constants: {lineage_record.constants}")
else:
    print("No lineage record (data wasn't from a thunk)")


# -----------------------------------------------------------------------------
# STEP 14: Query What Was Derived From a Variable
# Documentation: See docs/guide/lineage.md section "Derived Variables"
#
# Key concept: get_derived_from() finds all variables that were computed
# using a given variable as input. Useful for impact analysis.
# -----------------------------------------------------------------------------

print("\n--- Derived Variables Query ---")

# What variables were derived from the raw signal?
derived = db.get_derived_from(RawSignal, subject=1, session="morning")

print(f"Variables derived from raw signal:")
for item in derived:
    print(f"  - {item.get('type_name', 'Unknown')}: {item.get('record_id', 'N/A')[:12]}...")


# -----------------------------------------------------------------------------
# STEP 15: Batch Operations
# Documentation: See docs/guide/variables.md section "Batch Operations"
#
# Key concept: save_from_dataframe() and load_to_dataframe() enable
# efficient bulk save/load operations for multiple variables.
# -----------------------------------------------------------------------------

print("\n--- Batch Operations Demo ---")

# Create multiple signals
signals_df = pd.DataFrame({
    'subject': [1, 1, 2, 2],
    'trial': [1, 2, 1, 2],
    'data': [
        np.random.randn(100),
        np.random.randn(100),
        np.random.randn(100),
        np.random.randn(100),
    ]
})

# Batch save
class TrialSignal(TimeSeries):
    """Signal from a single trial"""
    pass

# Set a breakpoint here to step through batch save
TrialSignal.save_from_dataframe(
    signals_df,
    data_column='data',
    metadata_columns=['subject', 'trial']
)

print(f"Batch saved {len(signals_df)} signals")

# Batch load - get all trials for subject 1
loaded_df = TrialSignal.load_to_dataframe(subject=1)
print(f"Batch loaded {len(loaded_df)} signals for subject 1")
print(f"Loaded columns: {list(loaded_df.columns)}")


# -----------------------------------------------------------------------------
# STEP 16: Cache Invalidation
# Documentation: See docs/guide/caching.md section "Cache Invalidation"
#
# Key concept: Invalidate cache entries when you need to force recomputation.
# Can invalidate by function name, function hash, or all entries.
# -----------------------------------------------------------------------------

print("\n--- Cache Invalidation Demo ---")

# Get cache stats before invalidation
stats_before = db.get_cache_stats()
print(f"Cache entries before: {stats_before['total_entries']}")

# Invalidate cache for a specific function
db.invalidate_cache(function_name="expensive_analysis")
print("Invalidated cache for 'expensive_analysis'")

stats_after = db.get_cache_stats()
print(f"Cache entries after: {stats_after['total_entries']}")

# Re-run - should miss cache now
result3 = expensive_analysis(raw_loaded)
print(f"After invalidation, was_cached: {result3.was_cached}")


# -----------------------------------------------------------------------------
# STEP 17: Three-Output Function Demo
# -----------------------------------------------------------------------------

print("\n--- Three-Output Function Demo ---")

early_stats, middle_stats, late_stats = analyze_segments(raw_loaded)

print(f"Early segment: {early_stats.data}")
print(f"Middle segment: {middle_stats.data}")
print(f"Late segment: {late_stats.data}")

# Save all three
AnalysisResult.save(early_stats, subject=1, session="morning", segment="early")
AnalysisResult.save(middle_stats, subject=1, session="morning", segment="middle")
AnalysisResult.save(late_stats, subject=1, session="morning", segment="late")

# Re-run to verify caching
early2, middle2, late2 = analyze_segments(raw_loaded)
print(f"\nAfter save and re-run:")
print(f"  early.was_cached: {early2.was_cached}")
print(f"  middle.was_cached: {middle2.was_cached}")
print(f"  late.was_cached: {late2.was_cached}")


# -----------------------------------------------------------------------------
# STEP 18: Full Lineage Tree Visualization
# Documentation: See docs/guide/lineage.md section "Lineage Visualization"
# -----------------------------------------------------------------------------

print("\n--- Full Lineage Tree ---")

# Get the formatted lineage tree for the envelope signal
tree = db.format_lineage(
    EnvelopeSignal,
    subject=1,
    session="morning",
    channel="EMG"
)

print("Lineage tree for envelope signal:")
print(tree)


# -----------------------------------------------------------------------------
# STEP 19: Database Inspection
# -----------------------------------------------------------------------------

print("\n--- Database Summary ---")

# List all registered types
print("Registered variable types:")
for type_info in db.list_registered_types():
    print(f"  - {type_info['type_name']} (table: {type_info['table_name']})")

# Version counts
print("\nVersion counts by type:")
for var_class in [RawSignal, FilteredSignal, EnvelopeSignal, SignalHalf, TrialSignal, AnalysisResult]:
    try:
        versions = var_class.list_versions()
        print(f"  - {var_class.__name__}: {len(versions)} versions")
    except Exception:
        pass

# Final cache stats
final_stats = db.get_cache_stats()
print(f"\nFinal cache statistics:")
print(f"  Total entries: {final_stats['total_entries']}")
print(f"  By function: {final_stats['by_function']}")


# -----------------------------------------------------------------------------
# CLEANUP
# -----------------------------------------------------------------------------

print("\n--- Debug Example 2 Complete ---")
print(f"Database saved to: {db_path.absolute()}")
print("\nKey tables to explore:")
print("  - RawSignal, FilteredSignal, EnvelopeSignal (variable types)")
print("  - SignalHalf, TrialSignal, AnalysisResult (more variable types)")
print("  - _lineage (provenance records)")
print("  - _computation_cache (cached computations)")
print("  - _registered_types (type registry)")
print("\nKey concepts demonstrated:")
print("  - Variable type inheritance (TimeSeries -> RawSignal, etc.)")
print("  - External function wrapping (scipy.signal functions)")
print("  - Multi-output thunks (split_signal, analyze_segments)")
print("  - Automatic caching and cache hits")
print("  - Manual cache checking and invalidation")
print("  - Batch save/load operations")
print("  - Lineage extraction and visualization")
