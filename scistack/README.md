# SciStack

```bash
pip install scistack # Installs tools for Python and MATLAB
```

# Better Research Tools, Better Research Outcomes

SciStack is a suite of tools built by scientists for scientists of nearly any discipline to enhance data analysis pipelines. SciStack focuses on the common case of [embarrassingly parallel](https://en.wikipedia.org/wiki/Embarrassingly_parallel) workflows used to process nested datasets. It has three core goals:

1. **Minimalism:** Minimize wasted time and effort, with a focus on reducing boilerplate code and offering advanced capabilities.
2. **Reusability:** With a near-zero boilerplate format and support for cookie-cutter pipelines, SciStack pipelines are highly interoperable between projects. Thus, SciStack encourages scientific code reuse, an essential yet underutilized scientific work product.
3. **Openness:** Minimize lock-in to the SciStack framework. SciStack code never touches scientific code, so you can easily change your data processing pipeline architecture at any time.

# Minimal Example

Imagine that I am running a study involving a dataset containing Subjects each performing multiple Conditions, and each Condition contains multiple Trials (a common setup in my home field of biomechanics). Below is an example minimal data processing pipeline showcasing SciStack's features.

```python
import scifor
import numpy as np
import pandas as pd

schema = ["subject", "condition", "trial"] # The structure of the dataset
scifor.set_schema(schema) # Tell SciStack about the schema

def example_loading_function(filepath: str) -> np.ndarray:
  """Data loading logic here."""
  return np.ndarray(pd.read_csv(filepath))

def example_processing_function(val: np.ndarray, const: float) -> np.ndarray:
  """Do some processing on some inputs."""
  return val + const

# Load every file matching "path/to/{subject}/{condition}/{trial}_data.ext"
# Returns a df with columns ["subject", "condition", "trial", "loaded_variable"]
loaded_df = scifor.for_each(example_loading_function,
  filepath=scifor.PathInput("path/to/{subject}/{condition}/{trial}_data.ext", name="trial_data"),
  outputs=["loaded_variable"]
)

# Process every subject, condition, & trial combination.
# Returns a df with columns ["subject", "condition", "trial", "procesed_variable"]
processed_df = scifor.for_each(example_processing_function,
  val=scifor.ColumnSelection(loaded_df, "variable_to_process"), const=5,
  subject=[], condition=[], trial=[], # Tell SciStack that `my_processing_function` should receive only one trial's data at a time.
  outputs=["processed_variable"]
)
```

# SciStack Design

For any project, it's essential to pick the right tool for the job. SciStack - as the name implies - contains a suite of tools in a "stack" of increasing complexity and weight. There are three main tools in this stack:

- `scifor`: The lightest-weight level. Syntactical sugar around nested `for` loops. Operates on in-memory variables only (no file IO) just like standard functions.
- `scidb`: Wraps `scifor`, adds a SQL database for data save/load and an auditable data processing history
- `scistack` GUI: Wraps `scidb`, adds a GUI to manage complex pipelines.

## `scifor`: syntactic sugar around `for` loops

Imagine you are conducting a study of human subjects walking. Each Subject comes in to the lab for multiple Sessions, and in each Session they perform multiple Trials of walking. During each Trial, you measure their speed every 0.1 seconds and store that data to one .csv file for each trial.

### Example `scifor` Pipeline Data Loading Step

```python
import scifor
import pandas as pd

# Tell `scifor` about the structure of this dataset
scifor.set_schema(["subject", "session", "trial"]) # ordered one-to-many

def load_data(file_path: str) -> pd.DataFrame:
    """Example logic to load the data"""
    return pd.read_csv(file_path)

path_template = scifor.PathInput("path/to/data/{subject}/{session}/{trial}.csv", name="trial_data")
loaded_df = scifor.for_each(load_data,
    file_path=path_template,
    subject=[], session=[], trial=[],
    output_names=["Loaded"]
)
```

In this example step, after defining a basic `load_data()` function, we:

1. Defined a `scifor.PathInput`, providing a template to load all of the files of interest.
2. Invoked the main command `scifor.for_each()`, providing `load_data` as the function, `file_path` as the input variable, and specifying to run `load_data` once over every combination of `subject`, `session`, and `trial` that match the `scifor.PathInput` path template.
3. `loaded_df` is a `pd.DataFrame` with one row per `subject`, `session`, and `trial` combination. The data is stored into the `"Loaded"` field specified in the optional `output_names` parameter (default output name: `"value"`).

### Example `scifor` Pipeline Data Processing Step

```python
import numpy as np

def process_data(speed: np.ndarray) -> np.ndarray:
    """Square every data point"""
    return np.square(speed)

squared_df = scifor.for_each(process_data,
    speed=loaded_df,
    subject=[], session=[], trial=[]
)
```

`scifor.for_each` automatically parses `loaded_df`, repeatedly inputting only the `speed` values for one combination of `subject`, `session`, `trial`, allowing `process_data()` to remain very simple and ignore the structure of this project's dataset.

To see more, refer to the `scifor` docs.

## `scidb`

## `scistack` GUI

<!--
## Three Layers, Use What You Need

SciStack replaces all of it with three ideas:

- **Named variable types** — instead of files on disk, your data lives in typed database tables you can query by metadata
- **Automatic lineage** — a simple decorator records exactly what function and inputs produced each result
- **Computation caching** — if you've already computed something, SciStack knows and skips it

And because analysis doesn't end at processing, **figures and statistics are pipeline steps too**: `plot_`/`stat_` functions get the same caching and lineage, every figure carries an embedded provenance stamp back to its exact data, and `scidb report` collects everything into a shareable page ([details below](#from-pipeline-to-paper-visualization--statistics)).

With SciStack, your analysis scripts contain _only_ analysis logic. The infrastructure is handled for you.

## Package Architecture

SciStack is a stack of libraries. You can enter at any level — each layer adds more features at the cost of a bit more setup.

```

┌────────────────────────────────────────────────────────────────────┐
│ scihist │
│ One-import entry point: for_each() with automatic DB load/save │
│ and lineage tracking. Re-exports everything from the layers below │
│ deps: scidb + scilineage │
├───────────────────────────────┬────────────────────────────────────┤
│ scidb │ scilineage │
│ Typed variable storage; │ Wraps any function to record its │
│ configure_database(), │ full computational lineage. │
│ for_each() that loads from │ Enables caching and provenance │
│ DB and saves results back │ queries — no DB required. │
│ deps: scifor + sciduck + │ deps: canonical-hash │
│ canonical-hash + │ │
│ path-gen │ │
├───────────────────────────────┴────────────────────────────────────┤
│ scifor │
│ Batch execution on plain tables / DataFrames — iterates over │
│ condition combinations, slices data, collects results. │
│ No database, no tracking, no dependencies. │
└────────────────────────────────────────────────────────────────────┘

````

**scifor** is the foundation: a standalone batch execution engine that works with plain MATLAB tables or pandas DataFrames. There is no setup overhead — just give it a function, your data, and the experimental conditions to iterate over.

**scidb** adds typed, versioned database storage so your variables live in queryable DuckDB tables instead of scattered files on disk. It provides `configure_database()`, `BaseVariable`, and a `for_each()` that automatically loads inputs from the database and saves results back.

**scilineage** is an independent parallel track that has no database dependency. It wraps functions with `@lineage_fcn` to record exactly what function and inputs produced each result. This unlocks caching (skip re-running a computation whose inputs haven't changed) and provenance queries.

**scihist** brings everything together. Its `for_each()` automatically wraps your functions in `@lineage_fcn`, loads inputs from the database, and saves outputs back with full lineage attached. It also re-exports the entire API from the layers below, so most users can `from scihist import *` and have everything they need.

Each layer can be used independently. `scifor` is useful when your data is already in memory and you just want structured batch processing. `scilineage` can be dropped into any pipeline to add provenance without touching your storage layer. `scidb` gives you the database without requiring lineage tracking. `scihist` is the recommended starting point for new pipelines that want the full feature set.

## Quick Start

### Installation

```bash
pip install scistack
````

This installs `scistack-db` (the `scidb` package) and `scimatlab` directly, which in turn pull in `sciduckdb`, `scipathgen`, `scicanonicalhash`, `scifor`, and `scistacklog`. `scihist` is a separate, higher-level package (it depends on `scidb` + `scilineage`) and is not installed automatically — install it explicitly if you want its `for_each()` with automatic lineage tracking:

```bash
pip install scihist
```

For development (editable installs of all packages):

```bash
git clone https://github.com/mtillman14/scistack
cd scistack
./dev-install.sh
```

### One-Time Setup

Every project starts by configuring a database. You do this once.

```python
from scifor import set_schema, for_each

set_schema(["subject", "session"])

results = for_each(
    bandpass_filter,
    inputs={"signal": raw_df, "low_hz": 20},
    subject=[1, 2, 3],
    session=["pre", "post"],
)
```

`dataset_schema_keys` describes the structure of your experiment. If your data is organized by subject and session, say so — SciStack uses this to let you save and query data naturally.

results = scifor.for_each(@bandpass_filter, ...
struct('signal', raw_tbl, 'low_hz', 20), ...
subject=[1, 2, 3], session=["pre", "post"]);

````

You tell scifor which columns identify your experimental conditions (the "schema"), and it loops over every combination, filters each table to matching rows, calls your function, and collects the results into a clean output table.

scifor is standalone and has no dependencies beyond standard data structures. It can be dropped into any project.

See the [scifor README](scifor/README.md) for more.

### Layer 2: scidb — Database Storage

Wraps scifor with a database layer. Instead of working with tables you've already loaded, scidb loads inputs from a DuckDB database and saves results back. You define named variable types for each kind of data in your pipeline, and scidb gives you structured, queryable storage with metadata-based addressing.

```python
from scidb import BaseVariable, configure_database, for_each

# One-time setup
db = configure_database("experiment.duckdb", ["subject", "session"])

# Define variable types (one-liners)
class RawEMG(BaseVariable):
    pass

class FilteredEMG(BaseVariable):
    pass

class MaxActivation(BaseVariable):
    pass
````

That's it. No configuration, no serialization code. SciStack handles numpy arrays, scalars, lists, dicts, and DataFrames natively.

### Save and Load Data

```python
import numpy as np
RawEMG.save(np.random.randn(1000), subject=1, session="pre")

# Load it back
raw = RawEMG.load(subject=1, session="pre")
print(raw.data)  # your numpy array

# Batch processing — loads from DB, runs function, saves results
for_each(
    bandpass_filter,
    inputs={"signal": RawEMG, "low_hz": 20},
    outputs=[FilteredEMG],
    subject=[1, 2, 3],
    session=["pre", "post"],
)
```

````matlab
scidb.configure_database("experiment.duckdb", ["subject", "session"]);

RawEMG().save(randn(1000, 1), subject=1, session="pre");
raw = RawEMG().load(subject=1, session="pre");

Wrap your analysis functions with `@lineage_fcn` and SciStack records which functions produced what **and the input variable values** — automatically:

```python
from scilineage import lineage_fcn

@lineage_fcn
def bandpass_filter(signal, low_hz, high_hz):
    # your filtering logic
    return filtered_signal

# Run the pipeline — lineage is tracked automatically
raw = RawEMG.load(subject=1, session="pre")
filtered = bandpass_filter(raw, low_hz=20, high_hz=450)
FilteredEMG.save(filtered, subject=1, session="pre")

# Later: "What function produced this?"
provenance = db.get_provenance(FilteredEMG, subject=1, session="pre")
print(provenance["function_name"])  # "bandpass_filter"
print(provenance["constants"])      # {"low_hz": 20, "high_hz": 450}

# Run the same pipeline again — cache hit, no recomputation
filtered = bandpass_filter(raw, low_hz=20, high_hz=450)  # returns instantly
````

Your functions stay clean, no boilerplate required. They receive normal numpy arrays and return normal values. The `@lineage_fcn` decorator handles all the bookkeeping at the boundary.

If the `@lineage_fcn` decorator is still too close to your code for your test, wrap it in a `Thunk()` call later on:

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

This loads `RawEMG` for each subject/session combination, runs `bandpass_filter`, and saves the result as `FilteredEMG` — multiple iterations, zero boilerplate. If a subject is missing data, that iteration is skipped gracefully, and every run is documented as it happens: the console shows a concise narrative (run banner, periodic progress, and an end-of-run summary of what completed, what failed and why, and what was skipped as already up to date), while `scidb.log` next to the database records the same story with timestamps and the originating scistack layer on every line. For debugging, set `SCIDB_LOG_LEVEL=DEBUG` (or `Log.set_level("DEBUG", sink="file")`) to capture the full execution trace — per-iteration detail, internal steps with durations, and `[timing]` breakdowns of the hot paths.

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
df = MaxActivation.load(as_df=True)
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
db = scihist.configure_database("experiment.duckdb", ["subject", "session"]);

filter_fn = scidb.Thunk(@bandpass_filter);
raw = RawEMG().load(subject=1, session="pre");
filtered = filter_fn(raw, 20, 450);
FilteredEMG().save(filtered, subject=1, session="pre");
```

Your functions stay as plain functions — they receive normal arrays and return normal values. The `@lineage_fcn` decorator handles the bookkeeping at the boundary.

## What a Full Pipeline Looks Like

Here's a complete pipeline using all three layers:

```python
from scidb import BaseVariable, configure_database, for_each
from scilineage import lineage_fcn

# --- Setup ---
db = configure_database("gait_study.duckdb", ["subject", "session", "trial"])

# --- Variable types ---
class RawKinematicData(BaseVariable):
    pass

class StepLength(BaseVariable):
    pass

class MeanStepLength(BaseVariable):
    pass

# --- Processing functions ---
@lineage_fcn
def extract_step_length(kinematic_data):
    # your biomechanics logic here
    return step_lengths

@lineage_fcn
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
)

# --- Analyze results ---
df = MeanStepLength.load(as_df=True)
print(df.groupby("session")["data"].mean())
```

No file I/O code. No path management. No version tracking logic.

Want to change the function logic? SciStack will automatically detect the change, and will re-run that processing step on the next run of the script. Want to change a setting to the function? SciStack will detect that too, and re-run the processing step. Data will be saved to the database, **and the previous data will be preserved**. Understanding the effect of analysis decisions on our results has never been easier.

```python
# Load one record
emg = FilteredEMG.load(subject=3, session="post")

# Load all sessions for a subject
all_sessions = FilteredEMG.load(subject=3)

# Load everything as a DataFrame
df = FilteredEMG.load(as_df=True)
```

```sql
SELECT subject, session, value
FROM FilteredEMG;
```

By abstracting away all infrastructure — file paths, storage formats, naming conventions — SciStack decouples your analysis logic from your local environment. Your pipeline code contains _only_ the scientific computation.

Data saved from Python can be loaded in MATLAB and vice versa. The MATLAB API mirrors the Python API closely:

Today, sharing a pipeline means sharing a pile of scripts with hardcoded paths and implicit assumptions. With SciStack, the pipeline _is_ the science, and the infrastructure adapts to wherever it runs.

## From Pipeline to Paper: Visualization & Statistics

Data analysis is more than processing — it ends in **figures** and
**statistics**. SciStack treats both as first-class pipeline endpoints: your
plotting and stats functions run through the same `for_each`, with the same
caching, lineage, and variant tracking as every other step.

### Figures (`plot_` functions)

Name a function with the `plot_` prefix, return a figure, and tell SciStack
where the file goes. The framework renders, saves, and tracks it:

```python
def plot_timeseries(signal, filename):
    fig, ax = plt.subplots()
    ax.plot(signal)
    return fig    # SciStack saves it to `filename` and closes it

for_each(
    plot_timeseries,
    inputs={"signal": FilteredEMG,
            "filename": PathOutput("plots/{subject}_{trial}.png")},
    outputs=[EMGFigure],
    subject=[1, 2, 3], trial=[1, 2, 3],
)
```

**Draft by default.** Styling a figure takes dozens of iterations — so by
default nothing is recorded while you tweak. When the figure is right, add
`finalized=True` and the path is stored as a queryable record with full
lineage (and `skip_computed=True` skips unchanged figures on re-runs).

**Every figure knows where it came from.** SciStack embeds a provenance
stamp _inside_ the image file (PNG/SVG/PDF): the record it belongs to, the
function and exact input records that produced it. Find a figure in a paper
draft two years from now and trace it straight back to its data:

```python
from scidb import read_artifact_stamp
read_artifact_stamp("plots/1_2.png")
# {'record_id': 'a1b2c3…', 'function': 'plot_timeseries',
#  'inputs': {'signal': ['9e4bdc…']}, 'schema': {'subject': '1', 'trial': '2'}, …}
```

### Statistics (`stat_` functions)

Name a function with the `stat_` prefix and return a result dict — it is
stored as a queryable JSON record. Your stats receive the **long-format
table** (schema keys as columns), which is exactly what stats packages
expect:

```python
def stat_step_length(df, filename):
    from csvstats.ttest import ttest_dep
    return ttest_dep(df, "session", "StepLength",
                     repeated_measures_column="subject", filename=filename)

for_each(stat_step_length,
         inputs={"df": StepLength,
                 "filename": PathOutput("reports/step_length_ttest.pdf")},
         outputs=[StepLengthTTest],
         finalized=True)        # no iteration keys: all subjects fan in
```

Drafts print the result instead of saving it — explore interactively, then
finalize.

### One result per analysis decision — automatically

Ran your filter at two cutoffs? SciStack already keeps both variants of the
data. Every endpoint (and every aggregation) runs **once per variant**: two
`low_hz` settings mean two t-tests and two figures, each labeled with its
settings — never silently pooled. Path templates can name the variant so
files don't collide:

```python
PathOutput("plots/{subject}_{low_hz}.png")   # one file per filter setting
```

(and if a template _would_ make two variants overwrite each other's file,
SciStack refuses and tells you the one-line fix). To deliberately pool
variants — e.g. a robustness/multiverse analysis across all your pipeline
decisions — wrap the input in `AcrossVariants(...)` and the settings arrive
as columns.

### Your report, one command

When the figures and stats are finalized, collect them all:

```bash
scidb report db experiment.duckdb -o reports/paper1
```

This writes a self-contained HTML page — every figure (grouped by function,
labeled with its subject/session and settings), every stats result as a
table, plus a `stats.csv` you can paste into a manuscript and a
`manifest.json` for tooling. Artifacts are copied in and stamp-verified: if
a figure file was overwritten since its record was saved, the report flags
it as **stale** instead of letting you publish it.

All of it works identically from MATLAB — `plot_`/`stat_` functions,
`finalized`, `scifor.PathOutput`, `scidb.AcrossVariants`, and the same
report command.

## Learn More

- [Quickstart Guide](docs/quickstart.md) — Get running in 5 minutes
- [VO2 Max Walkthrough](docs/guide/walkthrough.md) — Full example pipeline with design explanations
- [Variables Guide](docs/guide/variables.md) — Deep dive into variable types
- [Lineage Guide](docs/guide/lineage.md) — How provenance tracking works
- [Batch Processing Guide](docs/guide/for_each.md) — for_each in depth
- [API Reference](docs/api.md) — Complete API documentation -->
