# Walkthrough: VO2 Max Pipeline

<!-- Ground truth: this walkthrough follows examples/vo2max/pipeline.py (the real,
     current example) and is reconciled against source/tests. Verified against:
     examples/vo2max/pipeline.py (uses scidb.for_each with PLAIN undecorated functions +
       inputs=/outputs=, NOT @lineage_fcn + manual save(); configure_database(dataset_db_path=,
       dataset_schema_keys=["subject"]) 2 args; db.list_pipeline_variants(); BaseVariable.
       list_versions(**metadata) classmethod; load -> .data/.record_id);
     scidb/src/scidb/model/variable.py:507 list_versions classmethod; database.py:3344
       list_pipeline_variants -> dicts {function_name, output_type, input_types, constants,
       record_count}; configure_database 2 args; one DuckDB file (no SQLite lineage db).
     NOTE: the example's own comments/prints mentioning "SQLite (lineage)"/"vo2max_lineage.db"
     are stale — storage is a single DuckDB file. This page describes the for_each approach the
     code actually uses; for the @lineage_fcn-by-hand style see the Lineage guide. -->

This walkthrough follows the runnable example at `examples/vo2max/pipeline.py`,
explaining not just *how* to use SciStack but *why* it's built this way. The
pipeline simulates a common workflow: load raw physiological signals from a VO2
max exercise test, combine them, compute derived metrics, and save everything with
provenance — driven entirely by [`for_each`](for_each.md).

To run it yourself:

```bash
cd examples/vo2max
python generate_data.py   # create dummy CSV files
python pipeline.py        # run the pipeline
```

## The problem SciStack solves

A typical pipeline loads raw data, combines and aligns signals, computes metrics,
and saves results. Done by hand, you end up with loose files
(`results_v2_final_FINAL.csv`) and no systematic way to answer *"what produced this
number?"* or *"did I already compute this?"*. SciStack answers both automatically
through lineage tracking and content-based caching.

## Step 1 — Define variable types

```python
from scidb import BaseVariable

class RawTime(BaseVariable): pass
class RawHeartRate(BaseVariable): pass
class RawVO2(BaseVariable): pass
class RollingVO2(BaseVariable): pass
class MaxHeartRate(BaseVariable): pass
class MaxVO2(BaseVariable): pass
```

Each subclass becomes its **own table**, so a table is never ambiguous about what
it holds, and `RawHeartRate.load(subject="S01")` is guaranteed to return heart-rate
data — each type is its own namespace. Scalars, numpy arrays, lists, and dicts need
no serialization code.

The one DataFrame type overrides `to_db` / `from_db` to keep its columns:

```python
class CombinedData(BaseVariable):
    def to_db(self) -> pd.DataFrame:
        return self.data           # already a DataFrame
    @classmethod
    def from_db(cls, df: pd.DataFrame) -> pd.DataFrame:
        return df
```

`schema_version` defaults to `1`; set it explicitly when you change a type's
structure.

## Step 2 — Define plain processing functions

The functions are **ordinary Python** — no decorator. `for_each` handles loading
inputs, unwrapping them to plain arrays/DataFrames, passing constants through, and
saving outputs:

```python
def load_time(data_dir: str) -> np.ndarray:
    return pd.read_csv(Path(data_dir) / "time_sec.csv").iloc[:, 0].values

def combine_signals(time, hr, vo2) -> pd.DataFrame:
    return pd.DataFrame({"time_sec": time, "heart_rate_bpm": hr, "vo2_ml_min": vo2})

def compute_rolling_vo2(combined, window_seconds=30, sample_interval=5) -> np.ndarray:
    window = window_seconds // sample_interval
    return pd.Series(combined["vo2_ml_min"]).rolling(window, min_periods=1).mean().values

def compute_max_vo2(rolling_vo2) -> float:
    return float(np.mean(np.sort(rolling_vo2)[::-1][:2]))
```

Your math sees plain numpy/pandas, never framework wrapper types — so these
functions are trivially testable on their own.

!!! note "Two ways to track lineage"
    This example uses `for_each`, which records provenance for you around plain
    functions. You can instead decorate functions with `@lineage_fcn` and call them
    directly — see [Tracking Lineage](lineage.md). Both record the same kind of
    provenance; `for_each` is the batch-oriented path.

## Step 3 — Configure the database

```python
from scidb import configure_database

db = configure_database(
    dataset_db_path="vo2max_data.duckdb",
    dataset_schema_keys=["subject"],
)
```

Two arguments: the DuckDB file (which holds **both** data and lineage — there is no
separate lineage file) and the schema keys. `["subject"]` declares that `subject`
identifies a record's *location*; any other metadata passed to a save becomes a
*version key* distinguishing variants at that location. This call also
auto-registers every `BaseVariable` subclass defined above.

## Step 4 — Run the pipeline with `for_each`

Each stage is a `for_each` call. Constants (like `data_dir`) are passed in `inputs`
alongside variable types and recorded as part of the output's provenance.

```python
data_dir = "data"

# Load raw signals from CSV (data_dir is a constant input)
for_each(load_time,       inputs={"data_dir": data_dir}, outputs=[RawTime],      subject=["S01"])
for_each(load_heart_rate, inputs={"data_dir": data_dir}, outputs=[RawHeartRate], subject=["S01"])
for_each(load_vo2,        inputs={"data_dir": data_dir}, outputs=[RawVO2],       subject=["S01"])

# Combine: for_each loads the three Raw* variables for each subject and joins them
for_each(combine_signals,
         inputs={"time": RawTime, "hr": RawHeartRate, "vo2": RawVO2},
         outputs=[CombinedData], subject=["S01"])

# Derived metrics; window_seconds / sample_interval are constants -> a tracked variant
for_each(compute_rolling_vo2,
         inputs={"combined": CombinedData, "window_seconds": 30, "sample_interval": 5},
         outputs=[RollingVO2], subject=["S01"])
for_each(compute_max_hr,  inputs={"combined": CombinedData},  outputs=[MaxHeartRate], subject=["S01"])
for_each(compute_max_vo2, inputs={"rolling_vo2": RollingVO2}, outputs=[MaxVO2],       subject=["S01"])
```

A few things happen automatically here:

- **Inputs load and join by schema key.** `combine_signals` runs once per subject
  with that subject's `RawTime` / `RawHeartRate` / `RawVO2` data.
- **Outputs save with provenance.** Each result is stored as its output type, with
  the function, its inputs' `record_id`s, and any constants recorded.
- **The computation graph emerges.** You never build it explicitly — it falls out
  of which variable types each `for_each` consumes and produces:

```
load_time   ─┐
load_hr     ─┼─> combine_signals ─┬─> compute_rolling_vo2 ─> compute_max_vo2
load_vo2    ─┘                     └─> compute_max_hr
```

Changing a constant (say `window_seconds=60`) creates a **separate variant** rather
than overwriting the `30` result.

## Step 5 — Load results and inspect provenance

`load` returns the latest record at a location, with its data and id:

```python
loaded = MaxVO2.load(subject="S01")
loaded.data        # e.g. 3854.2 (mL/min)
loaded.record_id   # content-addressed id

# What produced this result?
prov = db.get_provenance(MaxVO2, subject="S01")
prov["function_name"]   # "compute_max_vo2"
prov["inputs"]          # the variable inputs it consumed
prov["constants"]       # any constant parameters
```

The example also lists every pipeline variant that ran — the same data the GUI uses
to draw the graph:

```python
for v in db.list_pipeline_variants():
    print(v["function_name"], "->", v["output_type"],
          v["input_types"], v["constants"], f"[{v['record_count']} record(s)]")
```

## Step 6 — Versions and re-runs

Each variable type can report its stored versions at a location:

```python
for var_type in [RawTime, RawHeartRate, RawVO2, CombinedData,
                 RollingVO2, MaxHeartRate, MaxVO2]:
    print(var_type.__name__, len(var_type.list_versions(subject="S01")))
```

Run the whole pipeline again and, because every output is already current,
[`skip_computed`](caching.md) means nothing recomputes — until you change an
input's data, a function's code, or a constant, at which point only the affected
results rebuild.

## Design philosophy, in one table

| Principle | How it shows up here |
|---|---|
| **Enter at any level** | The whole pipeline is plain functions + `for_each` |
| **Transparency** | Functions see plain numpy/pandas, never framework types |
| **Type safety** | Each `BaseVariable` subclass is its own table and namespace |
| **Content addressing** | Identical data + metadata → identical `record_id` (dedup) |
| **Lineage as a side effect** | Provenance is recorded by `for_each`; no graph built by hand |
| **Caching** | Current results are reused; only real changes recompute |
| **Inspectable storage** | One DuckDB file, browsable in DBeaver |

The goal: pipelines that are **reproducible by default** without a new programming
paradigm — write normal functions, run them through `for_each`, and the framework
handles versioning, provenance, and caching.

**Next:** [Batch Processing (for_each)](for_each.md) ·
[Tracking Lineage](lineage.md) · [Computation Caching](caching.md) ·
[Concepts](../concepts/index.md)
