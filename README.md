# SciStack

## Better Research Tools, Better Research Outcomes

SciStack is a database framework purpose-built for scientific data analysis. It gives you a structured, versioned, and queryable home for every piece of data your pipeline produces — from raw signals to final results — with near zero infrastructure code on your part, and **zero changes to your analysis code**.

It works natively in both **Python** and **MATLAB**.

## The Problem

Every scientist who writes analysis code eventually builds the same thing: a tangle of folders, naming conventions, and bookkeeping scripts to track which data came from where, which version of a function produced it, and whether it's already been computed.

That infrastructure code is never the point. But it eats weeks of your time, it's fragile, and it's different in every lab.

**Scientists want to focus on the science, not data management.**

SciStack replaces all of it with three ideas:

- **Named variable types** — instead of files on disk, your data lives in typed database tables you can query by metadata
- **Automatic lineage** — a simple decorator records exactly what function and inputs produced each result
- **Computation caching** — if you've already computed something, SciStack knows and skips it

With SciStack, your analysis scripts contain _only_ analysis logic. The infrastructure is handled for you.

## Quick Start

### Installation

```bash
pip install scidb
```

This pulls in all core dependencies (`sciduckdb`, `thunk`, `scipathgen`, `canonicalhash`, `scirun`).

For development (editable installs of all packages):

```bash
git clone https://github.com/mtillman14/general-sqlite-database
cd general-sqlite-database
./dev-install.sh
```

### One-Time Setup

Every project starts by configuring a database. You do this once.

```python
from scidb import configure_database

db = configure_database(
    "my_experiment_data.duckdb",                  # DuckDB file for data + lineage
    dataset_schema_keys=["subject", "session"],   # How your dataset is organized
)
```

`dataset_schema_keys` describes the structure of your experiment. If your data is organized by subject and session, say so — SciStack uses this to let you save and query data naturally.

### Define Your Variable Types

Each kind of data in your pipeline gets its own type. This is just a one-liner:

```python
from scidb import BaseVariable

class RawEMG(BaseVariable):
    pass

class FilteredEMG(BaseVariable):
    pass

class MaxActivation(BaseVariable):
    pass
```

That's it. No configuration, no serialization code. SciStack handles numpy arrays, scalars, lists, dicts, and DataFrames natively.

### Save and Load Data

```python
import numpy as np

# Save with metadata that matches your schema
RawEMG.save(np.random.randn(1000), subject=1, session="baseline")

# Load it back — by the metadata you care about
raw = RawEMG.load(subject=1, session="baseline")
print(raw.data)  # your numpy array
```

### Track Lineage Automatically

Which function created that variable? Were the most recent settings used the last time I ran this?

Wrap your analysis functions with `@thunk` and SciStack records which functions produced what **and the input variable values** — automatically:

```python
from scidb import thunk

@thunk
def bandpass_filter(signal, low_hz, high_hz):
    # your filtering logic
    return filtered_signal

@thunk
def compute_max(signal):
    return float(np.max(np.abs(signal)))

# Run the pipeline — lineage is tracked behind the scenes
raw = RawEMG.load(subject=1, session="baseline")
filtered = bandpass_filter(raw, low_hz=20, high_hz=450)
max_val = compute_max(filtered)

# Save results
FilteredEMG.save(filtered, subject=1, session="baseline")
MaxActivation.save(max_val, subject=1, session="baseline")

# Later: "What function produced this, and with what settings?"
provenance = db.get_provenance(FilteredEMG, subject=1, session="baseline")
print(provenance["function_name"])  # "bandpass_filter"
print(provenance["constants"])      # {"low_hz": 20, "high_hz": 450}
```

Your functions stay clean, no boilerplate required. They receive normal numpy arrays and return normal values. The `@thunk` decorator handles all the bookkeeping at the boundary.

If the `@thunk` decorator is still too close to your code for your test, wrap it in a `Thunk()` call later on:

```python

from scidb.thunk import Thunk

compute_max = Thunk(compute_max)
```

**Run the same pipeline again and every step is skipped** — SciStack recognizes the same function + same inputs and returns the cached result instantly.

## Scaling Up with `for_each()`

Real experiments can have dozens of subjects and conditions, or thousands. SciStack can handle it all, using `for_each()` runs your pipeline over every combination automatically:

```python
from scidb import for_each

# 5 subjects
for_each(
    bandpass_filter,
    inputs={"signal": RawEMG},
    outputs=[FilteredEMG],
    subject=[1, 2, 3, 4, 5],
    session=["baseline", "post"],
)

# 10,000 subjects
for_each(
    bandpass_filter,
    inputs={"signal": RawEMG},
    outputs=[FilteredEMG],
    subject=range(1,10000),
    session=["baseline", "post"],
)

# Specify subject list of any size
subject_list = config["subjects"] # Load from some configuration file
for_each(
    bandpass_filter,
    inputs={"signal": RawEMG},
    outputs=[FilteredEMG],
    subject=subject_list,
    session=["baseline", "post"],
)
```

This loads `RawEMG` for each subject/session combination, runs `bandpass_filter`, and saves the result as `FilteredEMG` — multiple iterations, zero boilerplate. If a subject is missing data, that iteration is skipped gracefully. In the future, logging support is planned to document what ran successfully and what failed, and why.

Need one input to stay fixed while others iterate? Use `Fixed`:

```python
from scidb import Fixed

for_each(
    compare_to_baseline,
    inputs={
        "baseline": Fixed(RawEMG, session="baseline"),  # always load baseline
        "current": RawEMG,                               # iterates normally
    },
    outputs=[Delta],
    subject=[1, 2, 3, 4, 5],
    session=["post_1", "post_2", "post_3"],
)
```

## Powerful Querying

Because your data lives in a real database (not scattered files), querying is simple and powerful:

```python
# Load one specific record
emg = FilteredEMG.load(subject=3, session="post")

# Load all sessions for a subject — returns a list
all_sessions = FilteredEMG.load(subject=3)
for var in all_sessions:
    print(var.metadata["session"], var.data.shape)

# Load everything as a DataFrame for analysis
import pandas as pd
df = MaxActivation.load_all(as_df=True)
#   subject  session    data
#   1        baseline   0.82
#   1        post       1.47
#   2        baseline   0.91
#   ...
```

No folder traversal. No filename parsing. No `results_v2_final_FINAL.csv`. Just ask for what you want by the metadata that matters.

### Your Data Is Not Locked Away

Worried that putting data in a database means you can't see or inspect it? Don't be. SciStack uses [DuckDB](https://duckdb.org/) under the hood, and every variable type gets a human-readable **view** that you can query directly with SQL — in DBeaver, the DuckDB CLI, or any tool that speaks SQL.

For example, the `MaxActivation` view looks like this:

| subject | session  | value |
| ------- | -------- | ----- |
| 1       | baseline | 0.82  |
| 1       | post     | 1.47  |
| 2       | baseline | 0.91  |
| 2       | post     | 1.38  |
| 3       | baseline | 0.76  |
| 3       | post     | 1.22  |

You can query it directly:

```sql
SELECT subject, session, value
FROM MaxActivation;
```

Or use database viewer tools like [DBeaver](https://dbeaver.com) to view the database directly.

Your data is always one SQL query or visualization away — no Python or MATLAB required.

## Works in MATLAB Too

SciStack isn't Python-only. The entire framework works in MATLAB with a nearly identical API:

```matlab
% One-time setup
scidb.configure_database("my_experiment.duckdb", ["subject", "session"], "pipeline.db");

% Save and load
RawEMG().save(randn(1000, 1), subject=1, session="baseline");
raw = RawEMG().load(subject=1, session="baseline");

% Lineage-tracked functions
filter_fn = scidb.Thunk(@bandpass_filter);
filtered = filter_fn(raw, 20, 450);
FilteredEMG().save(filtered, subject=1, session="baseline");

% Batch processing
scidb.for_each(@bandpass_filter, ...
    struct('signal', RawEMG()), ...
    {FilteredEMG()}, ...
    subject=[1 2 3 4 5], ...
    session=["baseline" "post"]);
```

Data saved from MATLAB can be loaded in Python and vice versa. Lineage chains are continuous across languages. Use whichever language fits the task.

## What a Full Pipeline Looks Like

Here's a complete, realistic pipeline from setup to results:

```python
from scidb import BaseVariable, configure_database, thunk, for_each

# --- Setup (once per project) ---
db = configure_database("gait_study.duckdb",                # Where your data is stored
                        ["subject", "session", "trial"],    # How your dataset is organized
                         "pipeline.db"                      # Where lineage is tracked
                    )

# --- Define variable types ---
class RawKinematicData(BaseVariable):
    pass

class StepLength(BaseVariable):
    pass

class MeanStepLength(BaseVariable):
    pass

class RawForce(BaseVariable):
    pass

class FilteredForce(BaseVariable):
    pass

# --- Define processing functions ---
@thunk
def extract_step_length(kinematic_data):
    # your biomechanics logic here
    return step_lengths

@thunk
def compute_mean(values):
    return float(np.mean(values))

# --- Run the pipeline ---
for_each(
    extract_step_length,
    inputs={"kinematic_data": RawKinematicData},
    outputs=[StepLength],
    subject=[1, 2, 3],
    session=["pre", "post"],
    trial=[1, 2, 3, 4, 5],
)

for_each(
    compute_mean,
    inputs={"values": StepLength},
    outputs=[MeanStepLength],
    subject=[1, 2, 3],
    session=["pre", "post"],
    trial=[1, 2, 3, 4, 5],
)

for_each(
    filter_data,
    inputs={"values": RawForce, "smoothing": 0.2},
    outputs=[FilteredForce],
    subject=[1, 2, 3],
    session=["pre", "post"],
    trial=[1, 2, 3, 4, 5],
)

# --- Analyze results ---
df = MeanStepLength.load_all(as_df=True)
print(df.groupby("session")["data"].mean())
```

That's the entire pipeline. No file I/O code. No path management. No version tracking logic. Just the science.

Want to change the function logic? SciStack will automatically detect the change, and will re-run that processing step on the next run of the script. Want to change a setting to the function? SciStack will detect that too, and re-run the processing step. Data will be saved to the database, **and the previous data will be preserved**. Understanding the effect of analysis decisions on our results has never been easier.

```python
for_each(
    filter_data,
    inputs={"values": RawForce, "smoothing": 0.3}, # Changed from smoothing=0.2
    outputs=[FilteredForce],
    subject=[1, 2, 3],
    session=["pre", "post"],
    trial=[1, 2, 3, 4, 5],
)

# Get the FilteredForce created with smoothing=0.2
filtered_force0_2 = FilteredForce.load(smoothing=0.2) # Returns all subjects, sessions, and trials.

# Get the FilteredForce created with smoothing=0.3
filtered_force0_3 = FilteredForce.load(smoothing=0.3) # Returns all subjects, sessions, and trials.
```

## The Bigger Picture: Shareable Pipelines

By abstracting away all infrastructure — file paths, storage formats, naming conventions — SciStack decouples your analysis logic from your local environment. Your pipeline code contains _only_ the scientific computation.

This opens the door to **truly portable, shareable data processing pipelines.** When a pipeline is just a sequence of typed functions with declared inputs and outputs, it can be shared, reproduced, and built upon by anyone — regardless of how their data is organized on disk.

Today, sharing a pipeline means sharing a pile of scripts with hardcoded paths and implicit assumptions. With SciStack, the pipeline _is_ the science, and the infrastructure adapts to wherever it runs.

## Learn More

- [Quickstart Guide](docs/quickstart.md) — Get running in 5 minutes
- [VO2 Max Walkthrough](docs/guide/walkthrough.md) — Full example pipeline with design explanations
- [Variables Guide](docs/guide/variables.md) — Deep dive into variable types
- [Lineage Guide](docs/guide/lineage.md) — How provenance tracking works
- [API Reference](docs/api.md) — Complete API documentation
