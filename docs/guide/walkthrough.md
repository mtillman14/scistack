# Walkthrough: VO2 Max Pipeline

This walkthrough follows the `examples/vo2max/pipeline.py` example step by step, explaining not just *how* to use SciStack but *why* the framework is designed the way it is.

The example simulates a common scientific workflow: loading raw physiological data from a VO2 max exercise test, combining signals, computing derived metrics, and saving everything with full provenance tracking.

## The Problem SciStack Solves

A typical scientific data pipeline looks like this:

1. Load raw data from files
2. Combine and align signals
3. Compute derived metrics (rolling averages, peaks)
4. Save results

Without SciStack, you'd save results as loose files (`results_v2_final_FINAL.csv`) and have no systematic way to answer: "What processing produced this number?" or "Did I already compute this?"

SciStack answers both questions automatically through two mechanisms:
- **Lineage tracking** records what function and inputs produced each result
- **Content-based caching** skips redundant computations

## Step 1: Define Variable Types

```python
from scidb import BaseVariable

class RawTime(BaseVariable):
    pass

class RawHeartRate(BaseVariable):
    pass

class RawVO2(BaseVariable):
    pass
```

### Why subclass BaseVariable?

Each subclass creates a **separate table** in the database. This is a deliberate design choice:

- `RawTime` data lives in a table called `RawTime`
- `RawHeartRate` data lives in a table called `RawHeartRate`
- There's no ambiguity about what kind of data a table holds

The alternative — storing everything in one table with a "type" column — would make queries slower and the database harder to inspect directly in tools like DBeaver.

### Why no `schema_version` or serialization methods?

For numpy arrays, scalars, lists, and dicts, SciDuck (the DuckDB backend) handles serialization automatically using native DuckDB types. You only need to override `to_db()` and `from_db()` when storing complex objects like DataFrames:

```python
class CombinedData(BaseVariable):
    """Multi-column DataFrame needs custom serialization."""

    def to_db(self) -> pd.DataFrame:
        return self.data  # Already a DataFrame

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df
```

`schema_version` defaults to 1. You only set it explicitly when you change the structure of a variable type and need to distinguish old records from new ones.

### Why separate types for each signal?

You might wonder: why not a single `RawSignal` type for all three signals? The reason is **type-safe querying**. When you write:

```python
RawHeartRate.load(subject="S01")
```

You're guaranteed to get heart rate data, not time data that happens to share the same metadata. Each type is its own namespace.

## Step 2: Define Processing Functions

```python
from scidb import thunk

@thunk
def load_csv(path: str) -> np.ndarray:
    df = pd.read_csv(path)
    return df.iloc[:, 0].values

@thunk
def combine_signals(time, hr, vo2) -> pd.DataFrame:
    return pd.DataFrame({
        "time_sec": time,
        "heart_rate_bpm": hr,
        "vo2_ml_min": vo2,
    })

@thunk
def compute_rolling_vo2(combined, window_seconds=30, sample_interval=5):
    window_size = window_seconds // sample_interval
    rolling_avg = (
        pd.Series(combined["vo2_ml_min"])
        .rolling(window=window_size, min_periods=1)
        .mean()
    )
    return rolling_avg.values

@thunk
def compute_max_hr(combined: pd.DataFrame) -> float:
    return float(combined["heart_rate_bpm"].max())

@thunk
def compute_max_vo2(rolling_vo2: np.ndarray) -> float:
    sorted_vals = np.sort(rolling_vo2)[::-1]
    return float(np.mean(sorted_vals[:2]))
```

### Why `@thunk` and not a custom decorator?

The `@thunk` decorator does three things that would be tedious to implement per-function:

1. **Hashes the function's bytecode** — so if you change the function body, the framework knows it's a different computation
2. **Classifies inputs** — each argument is recorded as either a ThunkOutput (from another `@thunk` call), a saved BaseVariable, or a raw constant
3. **Returns a ThunkOutput** instead of raw data — this wrapper carries the lineage information forward

The key insight: your function code stays clean. `combine_signals` receives plain numpy arrays, not framework wrapper types. The `@thunk` decorator handles all the bookkeeping at the boundary.

### Why does `@thunk` auto-unwrap inputs?

When you call `combine_signals(time_data, hr_data, vo2_data)` where each argument is a ThunkOutput from `load_csv()`, the decorator automatically extracts the `.data` from each before calling your function. This means:

- Your function signature uses normal types (`np.ndarray`, `pd.DataFrame`)
- You can test functions outside SciStack with plain data
- The lineage tracking is completely transparent to the function body

### Why are constants recorded in lineage?

In `compute_rolling_vo2`, the `window_seconds=30` and `sample_interval=5` arguments are recorded as **constants** in the lineage record. This is important because:

- If you re-run with `window_seconds=60`, it produces a **different lineage hash**
- The cached result for `window_seconds=30` won't be returned for `window_seconds=60`
- You can later query provenance and see exactly what parameters were used

## Step 3: Configure the Database

```python
db = configure_database(
    dataset_db_path="vo2max_data.duckdb",
    dataset_schema_keys=["subject"],
)
```

SciStack stores both data and lineage in a single DuckDB file. Data is stored in columnar format with native array types, and the lineage DAG is stored alongside it for transactional consistency.

### What are `dataset_schema_keys`?

Schema keys define the **location** within your dataset. In this example, `["subject"]` means "subject" identifies where in the dataset a record belongs. Any additional metadata you pass to `save()` becomes a **version key** — it distinguishes different computational variants at the same location.

For example:
- `MaxVO2.save(result, subject="S01")` — "S01" is the dataset location
- If you later add `stage="filtered"` — "filtered" becomes a version key

This split matters for queries: `load(subject="S01")` returns the latest version at that location, regardless of what version keys exist.

### What does `configure_database` actually do?

Behind the scenes, this single call:

1. Creates a `DatabaseManager` that bridges DuckDB and SQLite
2. **Auto-registers** every `BaseVariable` subclass defined so far (creates their tables)
3. Sets `Thunk.query` to the `DatabaseManager` — this is what enables caching

The last point is subtle but important: `Thunk.query` is a **class variable** on `Thunk`. Every `@thunk`-decorated function checks this before executing. If it's set, the function looks up its lineage hash in the database before running.

## Step 4: Run the Pipeline

### Loading and saving raw data

```python
time_data = load_csv(str(data_dir / "time_sec.csv"))
hr_data = load_csv(str(data_dir / "heart_rate_bpm.csv"))
vo2_data = load_csv(str(data_dir / "vo2_ml_min.csv"))

RawTime.save(time_data, subject="S01")
RawHeartRate.save(hr_data, subject="S01")
RawVO2.save(vo2_data, subject="S01")
```

Each `load_csv()` call returns a **ThunkOutput**, not a raw array. The ThunkOutput wraps:
- `.data` — the actual numpy array
- `.pipeline_thunk` — metadata about the function call and its inputs
- `.hash` — a lineage-based hash for cache lookups

When you pass a ThunkOutput to `BaseVariable.save()`, the framework:
1. Extracts the raw data from `.data`
2. Extracts the lineage record (function name, hash, inputs, constants)
3. Stores the data in DuckDB and the lineage in SQLite
4. Registers the lineage hash for future cache lookups
5. Returns a deterministic `record_id` (hash of content + metadata)

### Chaining computations

```python
combined = combine_signals(time_data, hr_data, vo2_data)
CombinedData.save(combined, subject="S01")

rolling_vo2 = compute_rolling_vo2(combined, window_seconds=30, sample_interval=5)
RollingVO2.save(rolling_vo2, subject="S01")

max_hr = compute_max_hr(combined)
max_vo2 = compute_max_vo2(rolling_vo2)
MaxHeartRate.save(max_hr, subject="S01")
MaxVO2.save(max_vo2, subject="S01")
```

Notice that `compute_rolling_vo2(combined, ...)` receives a ThunkOutput from `combine_signals`. The `@thunk` decorator:

1. Records that the input came from `combine_signals` (by its lineage hash)
2. Unwraps `combined` to the raw DataFrame before calling the function
3. Wraps the result in a new ThunkOutput with updated lineage

This builds a **computation graph** automatically:

```
load_csv("time_sec.csv")  ─┐
load_csv("hr_bpm.csv")    ─┼─> combine_signals ─┬─> compute_rolling_vo2 ─> compute_max_vo2
load_csv("vo2_ml_min.csv") ─┘                    └─> compute_max_hr
```

You never construct this graph explicitly. It emerges from normal function calls.

## Step 5: Query Provenance

```python
loaded_max_vo2 = MaxVO2.load(subject="S01")
print(loaded_max_vo2.data)         # 3854.2 mL/min
print(loaded_max_vo2.lineage_hash) # "a1b2c3..."

prov = db.get_provenance(MaxVO2, subject="S01")
print(prov["function_name"])  # "compute_max_vo2"
print(prov["inputs"])         # [{name: "rolling_vo2", kind: "thunk_output", ...}]
```

### Two levels of provenance

SciStack provides two complementary views, both stored in the same SQLite database:

**Schema-blind (pipeline structure):** What does the computation graph look like in general?

```python
structure = db.get_pipeline_structure()
# ['RawTime', 'RawHeartRate', 'RawVO2'] --[combine_signals]--> CombinedData
# ['CombinedData'] --[compute_rolling_vo2]--> RollingVO2
# ['RollingVO2'] --[compute_max_vo2]--> MaxVO2
```

This ignores specific data instances. It answers: "What's my pipeline topology?"

**Schema-aware (instance provenance):** What exact data produced this specific result?

```python
prov = db.get_provenance(MaxVO2, subject="S01")
# function: compute_max_vo2
# inputs: [{record_id: "abc123", type: "RollingVO2", content_hash: "..."}]
```

This traces the full chain for a specific subject/trial/condition.

## Step 6: Caching in Action

```python
# Re-load saved variables
reloaded_time = RawTime.load(subject="S01")
reloaded_hr = RawHeartRate.load(subject="S01")
reloaded_vo2 = RawVO2.load(subject="S01")

# Re-run the pipeline
combined_2 = combine_signals(reloaded_time, reloaded_hr, reloaded_vo2)
rolling_2 = compute_rolling_vo2(combined_2, window_seconds=30, sample_interval=5)
max_vo2_2 = compute_max_vo2(rolling_2)
```

The second run **skips execution** for every function. Here's how:

1. `combine_signals(reloaded_time, reloaded_hr, reloaded_vo2)` computes a lineage hash from:
   - The function's bytecode hash
   - The `record_id` of each loaded input variable
2. The framework checks `Thunk.query` (the DatabaseManager) for this hash
3. A match is found in the lineage database — the previously saved `CombinedData` record
4. The data is loaded from DuckDB and returned without executing the function
5. The same happens for `compute_rolling_vo2` and `compute_max_vo2`

### Why cache by lineage hash, not just content?

Content hashing alone can't distinguish between "same output from different computations." Lineage hashing captures the full computation identity: same function + same inputs = same hash. This means:

- Changing the function body invalidates the cache (bytecode hash changes)
- Changing input data invalidates the cache (input record_ids change)
- Changing constant parameters invalidates the cache (constants are part of the hash)
- Identical re-runs hit the cache (everything matches)

### Why must you save before caching works?

Caching is tied to `save()`, not to function execution. The cache is populated when you save a ThunkOutput — that's when the lineage record is written to SQLite. This is intentional:

- You choose what to cache by choosing what to save
- Intermediate results you don't save don't consume database space
- You have full control over what's cached

## Summary: The Design Philosophy

| Principle | Implementation |
|---|---|
| **Separation of concerns** | Data in DuckDB, lineage in SQLite, logic in Python |
| **Transparency** | `@thunk` is invisible to function bodies; data flows as normal Python types |
| **Type safety** | Each `BaseVariable` subclass is its own namespace and table |
| **Content addressing** | Deterministic hashes ensure identical data produces identical record_ids |
| **Lineage as a side effect** | Provenance tracking requires no explicit graph construction |
| **Opt-in caching** | Cache is populated on save, not on execution — you control what's cached |
| **Inspectable storage** | DuckDB files are browsable in standard tools (DBeaver, DuckDB CLI) |
| **Minimal boilerplate** | Native types need no serialization; `schema_version` defaults to 1 |

The overall goal: scientific data pipelines should be **reproducible by default** without requiring researchers to learn a new programming paradigm. Write normal Python functions, decorate with `@thunk`, save with `BaseVariable.save()`, and the framework handles versioning, provenance, and caching.
