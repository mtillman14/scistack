# How `scidb.for_each()` Works — A Detailed Walkthrough

## Why this document exists

`scidb.for_each()` is the database-backed batch execution engine for SciStack. It wraps `scifor.for_each()` (documented separately in `scifor-for-each-internals.md`) by adding automatic data loading from DuckDB, variant tracking, version-key fingerprinting, and result saving. If you have read the scifor document, you know what the inner loop does; this document explains everything that happens *around* it.

All source references are to `/workspace/scidb/src/scidb/foreach.py` unless otherwise noted.

---

## What problem does it solve?

`scifor.for_each()` operates on in-memory DataFrames and returns an in-memory DataFrame. It knows nothing about databases. In a real scientific workflow you want to:

1. Load input data from a database by variable type and metadata
2. Run a function across all experimental conditions
3. Save the results back to the database with metadata that records *how* the output was produced (which function, which inputs, which parameter values)

`scidb.for_each()` does all three. It converts your high-level declaration — "run `bandpass_filter` on `RawEMG`, saving as `FilteredEMG`, for all subjects and sessions" — into the full load-compute-save pipeline.

---

## Prerequisites: the database tables

Before diving into the steps, you need to understand the database tables that `scidb.for_each()` reads from and writes to. These tables are created by `configure_database()` and live in the DuckDB file.

### `_schema` table

```sql
CREATE TABLE IF NOT EXISTS _schema (
    schema_id   INTEGER PRIMARY KEY,
    schema_level VARCHAR NOT NULL,
    -- one VARCHAR column per schema key, e.g.:
    subject     VARCHAR,
    session     VARCHAR,
    trial       VARCHAR
)
```

Source: `/workspace/sciduck/src/sciduckdb/sciduckdb.py`, lines 522–528.

Each row represents a unique **data location** — a specific combination of schema key values. For example, `(subject="1", session="A")` gets its own `schema_id`. The `schema_level` column records the deepest key that was provided (e.g., `"session"` if subject and session were given but not trial).

All schema values are stored as VARCHAR strings, regardless of the original Python type.

### `_record_metadata` table

```sql
CREATE TABLE IF NOT EXISTS _record_metadata (
    record_id       VARCHAR NOT NULL,
    timestamp       VARCHAR NOT NULL,
    variable_name   VARCHAR NOT NULL,
    schema_id       INTEGER NOT NULL,
    version_keys    VARCHAR DEFAULT '{}',
    content_hash    VARCHAR,
    lineage_hash    VARCHAR,
    schema_version  INTEGER,
    user_id         VARCHAR,
    branch_params   VARCHAR DEFAULT '{}',
    excluded        BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (record_id, timestamp)
)
```

Source: `/workspace/scidb/src/scidb/database.py`, lines 580–597.

Each row is one saved record. Key columns:

- **`record_id`**: A 16-character hex string that uniquely identifies this record's content and location. It is **content-addressed** — computed from `SHA-256(class_name | schema_version | content_hash | canonical_hash(metadata))`, truncated to 16 hex characters. Identical data saved at the same location with the same metadata always produces the same `record_id`. Different data or different metadata produces a different `record_id`. (Source: `/workspace/canonical-hash/src/canonicalhash/hashing.py`, lines 114–147.)

- **`timestamp`**: ISO-format timestamp of when this record was saved. The composite primary key `(record_id, timestamp)` allows the same logical record to be saved multiple times (audit trail), with `version_id="latest"` selecting the most recent.

- **`variable_name`**: The class name of the variable type (e.g., `"FilteredEMG"`).

- **`schema_id`**: Foreign key to `_schema`, identifying where in the experimental hierarchy this record lives.

- **`version_keys`**: JSON dict of non-schema metadata that distinguishes computational variants. When `for_each` saves a record, this includes `__fn`, `__fn_hash`, `__inputs`, `__constants`, etc. — everything from `ForEachConfig.to_version_keys()`.

- **`content_hash`**: SHA-256 of the data itself (first 16 hex chars), used for content-addressed deduplication.

- **`branch_params`**: JSON dict tracking which upstream pipeline choices led to this record. Explained in detail in the variant tracking section below.

### `_variables` table

```sql
CREATE TABLE IF NOT EXISTS _variables (
    variable_name VARCHAR PRIMARY KEY,
    schema_level  VARCHAR NOT NULL,
    dtype         VARCHAR,
    created_at    TIMESTAMP DEFAULT current_timestamp,
    description   VARCHAR DEFAULT ''
)
```

Source: `/workspace/sciduck/src/sciduckdb/sciduckdb.py`, lines 536–544.

One row per variable type. The `dtype` column is a JSON dict describing how the variable's data is serialized (mode: `"single_column"`, `"multi_column"`, or `"dataframe"`; column types; numpy dtype; etc.).

### `_for_each_expected` table

```sql
CREATE TABLE IF NOT EXISTS _for_each_expected (
    function_name  VARCHAR NOT NULL,
    schema_id      INTEGER NOT NULL,
    branch_params  VARCHAR DEFAULT '{}',
    PRIMARY KEY (function_name, schema_id, branch_params)
)
```

Source: `/workspace/scidb/src/scidb/database.py`, lines 622–629.

Stores the set of combos that a `for_each` call was *expected* to produce. Used by `check_node_state()` to determine whether a pipeline step is complete or has missing outputs.

### Data tables (e.g., `FilteredEMG_data`)

Created dynamically per variable type. Format depends on the data:

- **Scalar/array (native path)**: `record_id VARCHAR PRIMARY KEY` + data columns with DuckDB-inferred types (e.g., `DOUBLE[]` for numpy arrays)
- **DataFrame (custom path)**: `record_id VARCHAR NOT NULL` + one column per DataFrame column; multiple rows allowed per record_id

---

## The function signature

```python
def for_each(
    fn: Callable,
    inputs: dict[str, Any],
    outputs: list[type],
    dry_run: bool = False,
    save: bool = True,
    as_table: list[str] | bool | None = None,
    db=None,
    distribute: bool = False,
    where=None,
    _inject_combo_metadata: bool = False,
    _pre_combo_hook: Callable[[dict], bool] | None = None,
    _progress_fn: Callable[[dict], None] | None = None,
    _cancel_check: Callable[[], bool] | None = None,
    **metadata_iterables: list[Any],
) -> pd.DataFrame | None
```

Source: lines 78–93.

The `inputs` dict maps **function parameter names** to their sources. Keys are the names your function expects as keyword arguments. Values can be:

- **Variable types** (classes like `RawEMG`): loaded from the database
- **`Fixed` wrappers** (`Fixed(RawEMG, session="BL")`): loaded with overridden metadata
- **`Merge` wrappers** (`Merge(ForceData, KinematicData)`): multiple types combined column-wise
- **`ColumnSelection` wrappers** (`GaitData["step_length"]`): specific columns extracted after loading
- **`PathInput`** (`PathInput("{subject}/trial.mat")`): file path resolved from metadata
- **`EachOf`** (`EachOf(StepLength, StepTime)`): iterated over as separate full runs
- **Constants** (scalars, strings, numpy arrays): passed through unchanged every iteration

Compared to `scifor.for_each()`, three major parameters are added:

| Parameter | Purpose |
|---|---|
| `outputs` | List of output variable types (classes with `.save()`). scifor has no outputs parameter because it doesn't save. |
| `save` | Whether to actually save results to the database (default `True`). |
| `db` | Optional explicit database instance; if omitted, the global database from `configure_database()` is used. |

The `_inject_combo_metadata` and `_pre_combo_hook` parameters are internal hooks used by the `scihist` layer above for `generates_file` functions and `skip_computed` logic, respectively.

---

## Step-by-step execution

### Step 1: EachOf expansion (lines 131–173)

Before anything else, scidb checks whether any input value or the `where=` parameter is an `EachOf` wrapper.

`EachOf` is a scidb-only concept — scifor has no equivalent. The reason is that `EachOf` expresses variation at the level of *what variable types to load* or *what filter to apply*, which are database-layer concerns. By the time scifor runs, all inputs are already concrete in-memory DataFrames, so there is nothing left to vary.

`EachOf` expresses "run this entire `for_each` call once for each alternative."

Source for EachOf class: `/workspace/scidb/src/scidb/each_of.py`

```python
for_each(
    my_fn,
    inputs={"metric": EachOf(StepLength, StepTime), "alpha": EachOf(0.05, 0.01)},
    outputs=[Result],
    subject=[1, 2, 3],
)
```

This produces `2 types x 2 alphas = 4` recursive calls to `for_each()`, each with concrete (non-EachOf) values. The Cartesian product of all EachOf axes is computed via `itertools.product`, and for each element of that product, `for_each()` is called recursively with the concrete values substituted in. The results from all recursive calls are concatenated into a single DataFrame.

If `_cancel_check` fires during the EachOf loop, remaining alternatives are skipped — the cancel propagates upward.

If no `EachOf` wrappers are present, this step is a no-op and execution continues to Step 2.

### Step 2: Resolve empty-list metadata iterables from the database (lines 178–202)

When you pass `subject=[]`, scidb interprets that as "use all distinct values of `subject` that exist in the database." It calls `db.distinct_schema_values(key)` to fetch them.

`distinct_schema_values()` (source: `/workspace/sciduck/src/sciduckdb/sciduckdb.py`, lines 1102–1114) executes:

```sql
SELECT DISTINCT "subject" FROM _schema
WHERE "subject" IS NOT NULL
ORDER BY "subject"
```

This returns all values that have ever been saved for that schema key — for example, `["1", "2", "3"]`. Since `_schema` stores VARCHAR, the results are strings.

This is different from scifor's empty-list resolution (which scans in-memory DataFrames). scidb resolves against the database because the data hasn't been loaded into memory yet.

If no database is available (`db=None` and no global database configured), a `ValueError` is raised explaining the situation.

**Important limitation:** At this stage, the resolution is per-key: each empty-list key is resolved independently. The result is the set of *all* distinct values for that key, not filtered to combinations that actually exist across keys. The filtering to actually-existing combinations happens later in Step 9. Between Steps 2 and 9, the metadata iterables may contain combinations that don't exist in the database (e.g., if subject 2 has no trial 1, the combination `{subject: 2, trial: 1}` would still be in the Cartesian product until Step 9 removes it).

### Step 9 (cross-input filtering)

Step 9 (described fully below) uses `db.distinct_schema_combinations(filter_keys)` to query only actually-existing combinations:

```sql
SELECT DISTINCT "subject", "session" FROM _schema
WHERE "subject" IS NOT NULL AND "session" IS NOT NULL
ORDER BY "subject", "session"
```

Source: `/workspace/sciduck/src/sciduckdb/sciduckdb.py`, lines 1116–1141.

This query runs against the `_schema` table, which contains entries for *all* variable types. It does not filter per-variable — if subject 1 has data for `RawEMG` but not for `ForceData`, the combination is still considered "existing" because the `_schema` entry exists. If both `RawEMG` and `ForceData` are listed as inputs, the missing `ForceData` for that combo will cause scifor to skip it at iteration time (graceful skip-and-continue).

Per-input data availability is not checked at the combo level during planning. Instead, missing data is handled at iteration time: if the loaded DataFrame for an input has no rows matching a particular combo, scifor's filtering produces an empty result, and the function call typically fails and is skipped.

### Step 3: PathInput filesystem discovery (lines 204–235)

If any input is a `PathInput` (a file-path template like `"{subject}/trial_{trial}.mat"`), scidb checks whether metadata iterables need to be populated from the filesystem rather than (or in addition to) the database.

**Case 1: No metadata keys provided at all** (lines 210–219)

If the caller passes no `**metadata_iterables` — i.e., they rely entirely on discovery — scidb calls `PathInput.discover()` to walk the filesystem. Discovery splits the template into path segments (e.g., `["{subject}", "trial_{trial}.mat"]`), converts placeholder segments into regexes with named capture groups (e.g., `(?P<subject>[^/\\]+)` and `trial_(?P<trial>[^/\\]+)\.mat`), and recursively matches against actual directory entries starting from the root folder. Each complete match produces a metadata dict like `{"subject": "1", "trial": "2"}`.

The discovered dicts are used to populate `metadata_iterables`: for each key, all distinct values are collected. The full list of concrete dicts is stored as `_discovered_combos` so that later steps can use it directly instead of forming a Cartesian product (which might produce non-existent combinations — e.g., subject "1" might have trials 1–3 while subject "2" has only trial 1).

**Case 2: Some keys still empty after database resolution** (lines 222–235)

If Step 2 resolved some keys from the database but others are still empty (perhaps the database has no values for `trial` because this is the first pipeline step), discovery fills the remaining gaps using the same mechanism. Again, `_discovered_combos` is set if discovery succeeds.

### Step 4: Propagate schema keys to scifor (line 238)

scifor needs to know the schema keys (set via `scifor.set_schema()`) so that DataFrame filtering and `distribute` validation work correctly. `_propagate_schema()` (lines 1269–1291) copies `db.dataset_schema_keys` into scifor's module-level schema registry.

If `distribute=True` and no database is available to provide schema keys, a `ValueError` is raised.

### Step 5: Stringify metadata iterables (lines 240–257)

Database values may come back as typed Python objects (e.g., `numpy.int64(1)` instead of `"1"`). But the DataFrames produced by bulk loading (Step 10) have their schema columns stringified. For scifor's combo filtering to work — which compares metadata values to DataFrame column values via `==` — the metadata iterable values must also be strings.

scidb uses `_schema_str()` (source: `/workspace/scidb/src/scidb/database.py`, lines 41–50) to convert all schema-key values to strings. This function has a special case: whole-number floats are converted to ints first (`1.0` → `"1"` instead of `"1.0"`), because MATLAB sends all numbers as floats, and without this conversion, `"1.0" != "1"` would cause lookups to fail.

### Step 6: Build output names (line 260)

Each output type is converted to a display name string via `_output_name()` (lines 1260–1266). This uses `view_name()` if available, otherwise falls back to `__name__`. These become the column names in scifor's result DataFrame.

`view_name()` is a classmethod on `BaseVariable` (source: `/workspace/scidb/src/scidb/variable.py`, lines 197–208) that simply returns `cls.__name__` — the class name as a string. For example, `FilteredEMG.view_name()` returns `"FilteredEMG"`. It exists as a method rather than using `__name__` directly because DuckDB views for each variable type are named after this value (the view joins the data table with `_schema` and `_variables` for direct SQL querying).

### Step 7: Dry-run shortcut (lines 263–275)

If `dry_run=True`, scidb converts the inputs to display-only forms (no actual data loading) and delegates immediately to `scifor.for_each(dry_run=True)`. This prints what would happen — which combinations, which inputs, which constants — without touching the database or executing the function. Returns `None`.

The display conversion (`_convert_inputs_for_display()`, lines 634–656) creates dummy DataFrames and scifor wrappers that have the right names for printing but contain no actual data. A `_DryRunMerge` subclass (lines 55–71) bypasses Merge's validation to provide correct display names.

### Step 8: Build ForEachConfig version keys (lines 277–285)

**What this is:** A computation fingerprinting system. Every `for_each` call produces outputs, and those outputs need to be distinguishable from outputs produced by *different* calls with different parameters. If you run `bandpass_filter` with `low_hz=20` and then later with `low_hz=30`, you want both sets of results to coexist in the database, not overwrite each other. The version keys are the mechanism that makes this work: they are a serialized snapshot of the computation's configuration, stored alongside every output record.

`ForEachConfig` (source: `/workspace/scidb/src/scidb/foreach_config.py`) captures everything about the `for_each()` call that affects the output's identity:

```python
config = ForEachConfig(fn=fn, inputs=inputs, where=where, distribute=distribute, as_table=as_table)
config_keys = config.to_version_keys()
```

`to_version_keys()` (lines 52–72 of `foreach_config.py`) produces a dict with these keys:

| Key | Value | Purpose |
|---|---|---|
| `__fn` | Function name string | Identifies which function produced this output |
| `__fn_hash` | SHA-256 of function source (16 hex chars) | Detects if the function body changed |
| `__inputs` | JSON of loadable input specs | Detects if input variable types changed |
| `__constants` | JSON of constant input values | Detects if parameter values changed |
| `__where` | Filter expression string | Detects if row filtering changed |
| `__distribute` | `True` | Present only when distribute is active |
| `__as_table` | `True` or sorted list | Present only when as_table is non-default |

**On hash truncation:** The function hash is computed by `_compute_fn_hash()` (lines 9–21 of `foreach_config.py`): it takes `inspect.getsource(fn)`, hashes it with SHA-256, and truncates to 16 hex characters (64 bits). This is the same truncation used for `record_id` and `content_hash` throughout the system. 16 hex characters give 2^64 possible values (~1.8 x 10^19). For context, Git originally used 7-character hex prefixes (28 bits) for commit display and has since moved to longer prefixes as repositories grew, but Git's full SHA-1 hashes are 40 hex characters (160 bits). The 64-bit space used here is large enough that collisions are astronomically unlikely in any realistic scientific workflow (you would need ~4 billion distinct function bodies before a collision becomes probable via the birthday paradox). The hash is also never used in isolation — it is always stored alongside the function name, so a collision would require the same function name with the same 64-bit hash but different source code.

Constants are identified by `_get_direct_constants()` (lines 74–77): any input value that is not loadable (not a type, not a Fixed/Merge/ColumnSelection/PathInput/DataFrame) is a constant.

These config keys are merged into every saved record's `version_keys` in `_record_metadata`, so that changing any aspect of the computation — the function code, the input types, the constant values, the filter — creates a new version group rather than overwriting existing results.

### Step 9: Pre-filter to existing schema combinations (lines 287–316)

When empty lists were resolved from the database (Step 2), the Cartesian product of all resolved values might include combinations that don't actually exist in the database (e.g., subject 3 has no session "B" data).

scidb queries `db.distinct_schema_combinations(filter_keys)` to get the set of actually-existing `(schema_key_1, schema_key_2, ...)` tuples from the `_schema` table. It then filters the Cartesian product to keep only combinations that exist.

This avoids wasting time attempting to load data for non-existent combinations. The number of removed combos is printed as an info message: `[info] filtered 12 non-existent schema combinations (from 30 to 18)`.

This filtering only happens when empty lists were resolved from the database AND the inputs are not PathInput-based (PathInput combos come from the filesystem, not the database).

### Step 10: Load all inputs into DataFrames (line 319)

`_convert_inputs()` (lines 571–597) iterates over every entry in the `inputs` dict and either loads it from the database or passes it through as a constant.

The `inputs` dict is the one provided by the caller:

```python
inputs={"signal": RawEMG, "low_hz": 20, "high_hz": 450}
```

Here, `"signal"` maps to a variable type (loadable), while `"low_hz"` and `"high_hz"` map to scalars (constants). The dict keys become the function's keyword argument names.

The loading logic lives in `_load_input()` (lines 742–811). Each input type is handled differently:

**Variable type (class with `.load()`):**
- If the class has a `load_all()` method → bulk-load all records into a single DataFrame via `_load_var_type_all()` (lines 834–918)
- If not → wrap in a `PerComboLoader` sentinel (loaded individually per-combo during iteration)

**`Fixed` wrapper:**
- Cannot wrap a `Merge` (raises `TypeError`)
- Load the inner variable type
- If the inner type needs per-combo loading → wrap the entire Fixed spec in `PerComboLoader`
- Otherwise → strip `__record_id` and `__branch_params` from the loaded DataFrame (Fixed inputs are not part of the variant tracking system — see Step 11 for why), stringify fixed metadata values for schema keys, wrap in `scifor.Fixed`

**`ColumnSelection` wrapper:**
- If the inner type has `load_all()` → bulk-load, wrap in `scifor.ColumnSelection`
- Otherwise → `PerComboLoader`

**`Merge` wrapper:**
- If any constituent lacks `load_all()` → `PerComboLoaderMerge` (all constituents loaded per-combo)
- Otherwise → load each constituent, wrap in `scifor.Merge`

**`PathInput`:**
- Always wrapped in `PerComboLoader` (paths are resolved per-combo by definition)

**`ColName`:**
- Resolved via `_resolve_colname_from_db()` (lines 659–739), which queries the `_variables` table for dtype metadata to determine the data column name

**Everything else:**
- Treated as a constant — passed through unchanged

#### Bulk loading: `_load_var_type_all()` (lines 834–918)

This is the primary loading path. It calls `var_type.load_all(version_id="latest")` to fetch all records for a variable type, then assembles them into a single pandas DataFrame with both metadata columns and data columns.

**What `load_all(version_id="latest")` does:** (source: `/workspace/scidb/src/scidb/variable.py`, lines 412–500, delegating to `/workspace/scidb/src/scidb/database.py`, lines 1313–1333)

It executes a SQL query against `_record_metadata` with a window function:

```sql
WITH ranked AS (
  SELECT rm.*, schema_cols...,
    ROW_NUMBER() OVER (
      PARTITION BY rm.variable_name, rm.schema_id, rm.version_keys
      ORDER BY rm.timestamp DESC
    ) as rn
  FROM _record_metadata rm
  LEFT JOIN _schema s ON rm.schema_id = s.schema_id
  WHERE rm.variable_name = 'RawEMG'
) SELECT * FROM ranked WHERE rn = 1
```

This partitions by `(variable_name, schema_id, version_keys)` and takes the most recent timestamp in each partition — "latest" means "most recently saved version for each unique combination of data location and version keys." It returns one row per unique (location, version_keys) pair.

For each returned row, the actual data is fetched from the data table (e.g., `RawEMG_data`) in batch chunks of 500 record IDs.

**Assembly into a single DataFrame:**

Three assembly modes based on the data type:

1. **DataFrame data** (each record's `.data` is a DataFrame): For each record, the metadata dict is replicated for every row of the data DataFrame, and the two are concatenated column-wise. All records are then stacked vertically.

2. **Scalar/array data**: One row per record, with metadata columns plus a single data column named after the variable's `view_name()` (i.e., the class name).

3. **Raw objects without `.data`**: One row per record with the object in the data column.

**In all cases, two internal columns are added:**

- **`__record_id`**: The 16-character hex string from `_record_metadata.record_id`. This is the content-addressed identifier for this specific record. It uniquely identifies this piece of data at this schema location with these version keys. Two records at the same location with different data (or different version keys) have different `record_id` values. Two identical saves of the same data at the same location produce the same `record_id`.

- **`__branch_params`**: A JSON string from `_record_metadata.branch_params`. This is the accumulated set of upstream pipeline choices that led to this record. For example, `'{"bandpass.low_hz": 20}'` tells you this record was produced (directly or indirectly) by a `bandpass` function call with `low_hz=20`. The format is a flat dict where keys are `"function_name.param_name"` and values are the parameter values. An empty dict `'{}'` means no upstream variant choices.

These two columns are the foundation of the variant tracking system described in Steps 11–12.

**Additional cleanup during assembly:**

- Schema-key columns are stringified via `_schema_str()` (to match the stringified metadata iterables from Step 5)
- Version-key columns starting with `__` are stripped (they are internal metadata, not data)
- Constants that were stored in version keys (recorded in `__constants`) are also stripped — if `low_hz=20` was saved as a version key, it would appear as a column named `low_hz` in the loaded DataFrame, which would confuse scifor's data-column detection (scifor would see `low_hz` as a data column instead of recognizing it as metadata)

### Step 11: Build variant tracking (rid-to-branch_params mapping) (lines 321–353)

**The problem this step solves:**

When a pipeline has multiple upstream variants, the loaded input DataFrame from Step 10 contains records from *all* variants mixed together. Consider this scenario:

```
Previous run:  for_each(bandpass, inputs={"signal": RawEMG, "low_hz": 20}, outputs=[FilteredEMG], ...)
Previous run:  for_each(bandpass, inputs={"signal": RawEMG, "low_hz": 50}, outputs=[FilteredEMG], ...)
Current run:   for_each(compute_rms, inputs={"signal": FilteredEMG}, outputs=[RMS], ...)
```

When `compute_rms` loads `FilteredEMG`, the loaded DataFrame contains rows from *both* the `low_hz=20` run and the `low_hz=50` run. At subject=1, session="A", there are two rows — one for each variant. scidb needs to:

1. Run `compute_rms` separately for each variant (not mix them together)
2. Record which specific upstream variant each `RMS` output came from
3. Propagate the upstream choice (`low_hz=20` or `low_hz=50`) so that it can be queried later

The variant tracking system solves all three problems using `record_id` as the discriminator.

**What this step does concretely:**

For each loaded DataFrame input that has a `__record_id` column:

1. **Build `rid_to_bp`**: a dict mapping `record_id → branch_params_dict`. For each row in the DataFrame, parse the `__branch_params` JSON string into a Python dict and store it keyed by the `__record_id` value.

   After this step, `rid_to_bp` might look like:
   ```python
   {
       "a1b2c3d4e5f6g7h8": {"bandpass.low_hz": 20},
       "x9y8z7w6v5u4t3s2": {"bandpass.low_hz": 50},
   }
   ```

   This mapping is used later in Step 19 (save) to inherit upstream branch params.

2. **Rename `__record_id` → `__rid_{param_name}`**: For example, if the input parameter is named `"signal"`, the column becomes `__rid_signal`. This renaming is necessary because a function might have multiple database inputs (e.g., `signal` and `reference`), each with their own `__record_id` column. Renaming to `__rid_signal` and `__rid_reference` keeps them unambiguous.

3. **Strip `__branch_params`**: The `__branch_params` column is dropped from all DataFrames. Its information has been captured in `rid_to_bp` and is no longer needed in the DataFrame. Leaving it in would confuse scifor's data-column detection.

### Step 12: Expand combos with record-ID variants (lines 355–458)

**The problem this step solves:**

At this point, the loaded DataFrame for `FilteredEMG` (continuing the example from Step 11) has a column `__rid_signal` containing record IDs like `"a1b2c3d4e5f6g7h8"` and `"x9y8z7w6v5u4t3s2"`. The base combos from Step 9 are just `{subject: "1", session: "A"}`, `{subject: "1", session: "B"}`, etc. — they say nothing about *which variant* to use.

scidb needs to expand each base combo into one combo per variant. If subject 1, session A has two variants of `FilteredEMG` (one with record ID `a1b2...` and one with `x9y8...`), then `{subject: "1", session: "A"}` must become two combos:
- `{subject: "1", session: "A", __rid_signal: "a1b2c3d4e5f6g7h8"}`
- `{subject: "1", session: "A", __rid_signal: "x9y8z7w6v5u4t3s2"}`

Later, when scifor filters the DataFrame for each combo, the `__rid_signal` column acts as an additional filter — selecting exactly one row (one variant) per combo.

**But first: aggregation mode detection (lines 368–389):**

There is one important exception. If you are intentionally *not* iterating over all schema keys — for example, iterating over `subject` but not `session` when the schema is `[subject, session]` — you are performing an aggregation. You *want* the function to receive multiple rows (all sessions for a subject) as a single multi-row DataFrame.

In this case, separating records by variant would defeat the purpose. If you are computing "mean across all sessions for subject 1", you want all rows for subject 1, regardless of which upstream variant they came from. So aggregation mode:

- Strips `__rid_*` columns from all DataFrames (the function should not see them)
- Skips rid expansion entirely
- Uses `base_combos` directly (no variant disambiguation)

The detection is simple: if the set of iterated schema keys is a strict subset of all schema keys, enter aggregation mode.

**Full iteration mode (lines 390–458):**

When all schema keys are being iterated, scidb performs variant expansion:

1. **Build `rid_per_combo`**: For each `__rid_{param}` column in each loaded DataFrame:
   - Group the DataFrame by the lookup keys (all schema keys + any non-schema metadata keys being iterated)
   - For each group (each unique schema location), collect the list of record IDs present
   - Store as a nested dict: `rid_per_combo[rid_col_name][schema_tuple] = [list_of_rids]`

   For example, with `__rid_signal`:
   ```python
   rid_per_combo["__rid_signal"] = {
       ("1", "A"):  ["a1b2c3d4e5f6g7h8", "x9y8z7w6v5u4t3s2"],  # 2 variants
       ("1", "B"):  ["d4e5f6g7h8i9j0k1"],                        # 1 variant
       ("2", "A"):  ["m2n3o4p5q6r7s8t9"],                        # 1 variant
       # subject 2, session B: no entry (no data)
   }
   ```

2. **Expand each base combo**: For each base combo:
   - Look up the schema values in each rid dimension's mapping
   - If any dimension has zero entries for this combo → drop the combo (no data exists)
   - Otherwise, take the Cartesian product across all rid dimensions and create one full combo per product element

   With two inputs (`signal` and `reference`), each potentially having multiple variants, the product can be large. But in practice, most schema locations have exactly one variant per input, so expansion changes nothing.

**Example with two inputs:**

Combo `{subject: "1", session: "A"}` with:
- `__rid_signal` has record IDs `["abc...", "def..."]` (2 upstream variants)
- `__rid_reference` has record ID `["xyz..."]` (1 variant)

Expansion: `2 x 1 = 2` full combos:
```python
{subject: "1", session: "A", __rid_signal: "abc...", __rid_reference: "xyz..."}
{subject: "1", session: "A", __rid_signal: "def...", __rid_reference: "xyz..."}
```

The expansion count is logged: `expanded 4 base combos -> 8 full combos (rid variants)`.

### Step 13: Persist expected combos (lines 460–465)

**Why this is needed:** The GUI's pipeline status system (`check_node_state`) needs to know how many combos a function *should* have produced, so it can show whether a pipeline step is complete, partially complete, or has missing outputs. For database-backed inputs, this can be inferred from the existing data. But for `PathInput`-only functions (where inputs come from the filesystem, not the database), there are no database records to infer from. So scidb explicitly persists the expected set.

Before any combos are filtered by `skip_computed`, scidb writes the full expected combo set to the `_for_each_expected` table via `_persist_expected_combos()` (lines 1294–1355).

For each combo in `full_combos`:
1. Extract only the schema-key values from the combo dict (ignore `__rid_*` keys)
2. Look up or create a `schema_id` in the `_schema` table for those values
3. Insert `(function_name, schema_id, "{}")` into `_for_each_expected`

Old entries for this function are deleted first (`DELETE FROM _for_each_expected WHERE function_name = ?`), then the new set is inserted. This ensures the expected set reflects the current run's combos, not stale ones from a previous run.

### Step 14: Apply pre-combo hook (skip_computed) (lines 467–476)

**Why this is needed:** In a pipeline with many steps, you often want to re-run only the steps whose inputs have changed. The `skip_computed` system (implemented in the `scihist` layer) checks each combo to see whether a valid output already exists with matching upstream provenance. If so, the combo is skipped.

If a `_pre_combo_hook` was provided (by `scihist.for_each` implementing `skip_computed`), it is called for every combo in `full_combos`. If the hook returns `True` for a combo, that combo is removed from the list — it will not be iterated.

The number of skipped combos is printed: `skip_computed: 47/50 combos skipped`.

This filtering happens *after* expected combos are persisted (Step 13), so the expected set reflects all combos that *should* exist, regardless of whether they were skipped in this particular run. This is necessary because `check_node_state` needs to know the full set to detect missing outputs, even if those outputs were computed in a previous run.

### Step 15: Extend scifor's schema with rid keys (lines 478–489)

**Why this is needed:** The `__rid_signal` column in the loaded DataFrame is an internal tracking column. scifor doesn't know about it — scifor only knows about "schema keys" (which it uses for filtering) and "data columns" (which it passes to the function). If scifor sees `__rid_signal` as a data column, it will try to pass it to the function, and it will not filter on it — meaning the function might receive multiple rows from different variants in a single call.

By temporarily adding `__rid_signal` to the schema, we tell scifor: "treat this column like a schema key — filter on it, strip it from the function's input." This is what makes the variant expansion from Step 12 actually work: each combo has a specific `__rid_signal` value, and scifor filters the DataFrame to the row matching that value.

scifor's schema is temporarily extended to include the `__rid_*` column names:

```python
scifor.set_schema(["subject", "session", "__rid_signal"])
```

The metadata iterables are also extended with all distinct rid values per rid column, so scifor's print banner and internal bookkeeping work correctly.

**This extension is temporary.** After scifor finishes (Step 17), the schema is restored to its original form in Step 18.

### Step 16: Wrap fn for PerComboLoader resolution and metadata injection (lines 491–517)

**Why this is needed:** Some inputs could not be bulk-loaded in Step 10 (they lack `load_all()`) and were replaced with `PerComboLoader` sentinel objects. These sentinels travel through scifor's loop as opaque constants — scifor doesn't know what they are and doesn't try to filter them. They need to be resolved into actual data just before the function is called.

Additionally, `generates_file` functions (used via scihist) need to know the current combo's metadata (e.g., the current subject and session) so they can construct file paths. The `_inject_combo_metadata` flag enables this.

If any input was wrapped in `PerComboLoader` or `PerComboLoaderMerge`, or if `_inject_combo_metadata=True`, the original function is wrapped in a closure.

The wrapper:

1. Uses a call counter (`_call_idx`) to track which combo is currently executing — it indexes into the ordered `full_combos` list. This is how it knows the current combo's metadata without scifor explicitly passing it.
2. Extracts the current combo's non-internal metadata (`{k: v for k, v in combo.items() if not k.startswith("__")}`).
3. For each `PerComboLoader` kwarg: calls `_resolve_per_combo_loader()` (lines 925–951) to load the variable via `spec.load(**effective_kw)`, extracting `.data` from the loaded value. Handles Fixed overrides and ColumnSelection extraction.
4. For each `PerComboLoaderMerge` kwarg: calls `_resolve_per_combo_merge()` (lines 954–1001) to load each constituent, convert to DataFrame, apply column selection, and merge column-wise.
5. If `_inject_combo_metadata=True`: adds current combo metadata keys as extra kwargs (so `generates_file` functions know the current subject/session).
6. Calls the original function with the resolved kwargs.

### Step 17: Delegate core loop to scifor (lines 529–542)

With all inputs loaded, combos built, and the function possibly wrapped, scidb delegates the actual iteration to `scifor.for_each()`:

```python
result_tbl = _scifor_for_each(
    fn,
    loaded_inputs,
    dry_run=False,
    as_table=as_table,
    distribute=distribute,
    output_names=output_names,
    _all_combos=full_combos,
    _log_fn=Log.info,
    _progress_fn=_tracking_progress_fn,
    _cancel_check=_cancel_check,
    **extended_metadata_iterables,
)
```

Key points:
- `_all_combos=full_combos` — scifor uses the pre-built combo list directly (no Cartesian product of its own)
- `_log_fn=Log.info` — scifor's per-combo log messages go to the scidb log file
- The progress function is wrapped in `_tracking_progress_fn` (lines 520–527) to track final completed/skipped counts for logging

scifor now executes its loop as documented in `scifor-for-each-internals.md`: for each combo, filter the loaded DataFrames to matching rows (including filtering by `__rid_*` columns thanks to the extended schema), call the function, collect results, return a DataFrame.

### Step 18: Restore scifor's schema (lines 549–551)

The schema extension from Step 15 was temporary — it was needed only for scifor's filtering during the loop. Now that scifor has finished, the schema is restored to its original form:

```python
scifor.set_schema(["subject", "session"])  # remove __rid_signal etc.
```

If this were not done, subsequent `for_each` calls in the same Python session would see `__rid_signal` as a schema key, which would break their filtering logic — they would expect a `__rid_signal` column in their input DataFrames, which they wouldn't have. The schema is a module-level global in scifor, so cleanup is essential.

### Step 19: Save results (lines 556–563)

If `save=True`, the output list is non-empty, and the result table is not empty, scidb saves each result row back to the database via `_save_results()` (lines 1041–1193).

For each row in the result table:

#### 19a. Collect upstream branch params (lines 1072–1088)

**Purpose:** Inherit the pipeline history from the input records so the output carries a complete record of all upstream choices.

For each `__rid_*` column in the row (e.g., `__rid_signal`), extract the record ID value (e.g., `"a1b2c3d4e5f6g7h8"`), look it up in `rid_to_bp` (built in Step 11), and merge the resulting branch params dict into `merged_bp`.

For example, if `rid_to_bp["a1b2c3d4e5f6g7h8"]` is `{"bandpass.low_hz": 20}`, then `merged_bp` starts as `{"bandpass.low_hz": 20}`.

If a function has two inputs and their upstream branch params define the same key with different values, a warning is issued — this indicates a pipeline configuration problem (two conflicting branches being merged).

#### 19b. Add constant-based branch params (lines 1090–1092)

**Purpose:** Record the current function's choices so downstream consumers can distinguish this output's variant.

Constants from the `for_each` call are added to `merged_bp` namespaced by the function name. For example, if the function is `compute_rms` and it has a constant `window=100`:

```python
merged_bp["compute_rms.window"] = 100
```

The namespacing (`fn_name.param_name`) prevents collisions with upstream params and with params from other functions in the pipeline.

#### 19c. Add dynamic discriminators (lines 1094–1115)

Non-schema, non-internal metadata columns with scalar values are added to `merged_bp`. These are columns that distinguish records beyond the schema keys and the explicit constants — for example, columns that arose from EachOf expansion or from non-schema metadata iterables.

#### 19d. Build save metadata (lines 1117–1144)

The save metadata dict is assembled from:
- All non-internal metadata columns from the result row (schema keys, etc.)
- The config keys from Step 8 (`__fn`, `__fn_hash`, `__inputs`, `__constants`, etc.) — these become `version_keys` in `_record_metadata`
- `__branch_params` as a JSON string of `merged_bp`
- `__upstream` as a JSON string of upstream record IDs (so records from different upstream variants get distinct `record_id` values — since `record_id` is computed from `canonical_hash(metadata)`, including upstream IDs in the metadata ensures that different upstream variants produce different output record IDs)
- Direct constant values unpacked as top-level keys (so downstream consumers like scihist can see them in the metadata dict)

#### 19e. Call output_obj.save() (lines 1146–1193)

For each output type, the corresponding column value is extracted from the result row and saved via `output_obj.save(data, **save_metadata)`.

Inside `save()` (source: `/workspace/scidb/src/scidb/database.py`, lines 1701–1846):

1. Schema keys are separated from version keys in the metadata
2. A `schema_id` is looked up or created in the `_schema` table
3. A `content_hash` is computed from the data via `canonical_hash()`
4. A `record_id` is generated from `SHA-256(class_name | schema_version | content_hash | canonical_hash(metadata))`
5. A row is inserted into `_record_metadata` with all the metadata
6. The data is serialized and stored in the variable's data table
7. All writes happen in a single transaction for atomicity

Two paths based on scifor's result format:

1. **Named output column exists** (scalar mode — the output column name from `output_names` is present in the result): save the value directly with the full save metadata.

2. **Named output column missing** (flatten mode — scifor flattened DataFrame outputs into the result table's columns): build a single-row DataFrame from the non-schema, non-internal data columns and save that.

Each save call prints: `[save] subject=1, session=A, low_hz=20, high_hz=450: FilteredEMG -> record_id=abc123def456 (ndarray shape=(100,)) in 0.003s`.

If a save fails, the error is printed and logged but does not stop other saves.

---

## The variant tracking system — a complete picture

The `record_id`, `branch_params`, `rid_to_bp`, and `__rid_*` mechanisms described across Steps 10–12 and 19 can be confusing because they are spread across many steps. This section ties them together into a single narrative.

### The core problem

A scientific pipeline is a DAG of processing steps:

```
RawEMG → bandpass(low_hz=20)  → FilteredEMG (record_id: "aaaa...")
RawEMG → bandpass(low_hz=50)  → FilteredEMG (record_id: "bbbb...")
FilteredEMG → compute_rms     → RMS
```

When `compute_rms` loads `FilteredEMG`, it gets both records. Without variant tracking, `compute_rms` would either:
- Run once and somehow receive both records (wrong — they should be processed separately)
- Run once and pick one arbitrarily (wrong — the other variant is lost)

### The solution: record_id as discriminator

**`record_id`** is a 16-character hex string that uniquely identifies a saved record. It is deterministic — the same data at the same location with the same metadata always produces the same `record_id`. It is stored in the `_record_metadata` table as the primary key (paired with timestamp for audit trail).

When `_load_var_type_all()` loads `FilteredEMG`, it includes `__record_id` as a column in the resulting DataFrame:

| subject | session | FilteredEMG | __record_id | __branch_params |
|---|---|---|---|---|
| 1 | A | [0.1, 0.2, ...] | aaaa... | {"bandpass.low_hz": 20} |
| 1 | A | [0.5, 0.6, ...] | bbbb... | {"bandpass.low_hz": 50} |
| 1 | B | [0.3, 0.4, ...] | cccc... | {"bandpass.low_hz": 20} |
| 1 | B | [0.7, 0.8, ...] | dddd... | {"bandpass.low_hz": 50} |

### Step 11 captures the mapping and renames the column

`rid_to_bp` captures the record_id → branch_params relationship:

```python
rid_to_bp = {
    "aaaa...": {"bandpass.low_hz": 20},
    "bbbb...": {"bandpass.low_hz": 50},
    "cccc...": {"bandpass.low_hz": 20},
    "dddd...": {"bandpass.low_hz": 50},
}
```

The `__record_id` column is renamed to `__rid_signal` (because the input parameter is named `signal`).

### Step 12 expands combos to include the record_id

Base combos are `[{subject: "1", session: "A"}, {subject: "1", session: "B"}]`. After expansion:

```python
[
    {subject: "1", session: "A", __rid_signal: "aaaa..."},
    {subject: "1", session: "A", __rid_signal: "bbbb..."},
    {subject: "1", session: "B", __rid_signal: "cccc..."},
    {subject: "1", session: "B", __rid_signal: "dddd..."},
]
```

### Step 15 makes scifor filter on the record_id

The schema becomes `["subject", "session", "__rid_signal"]`. Now when scifor processes combo `{subject: "1", session: "A", __rid_signal: "aaaa..."}`, it filters the DataFrame to the single row where all three match — the `low_hz=20` variant.

### Step 19 inherits upstream branch_params

When saving the RMS output, scidb looks up `rid_to_bp["aaaa..."]` → `{"bandpass.low_hz": 20}`, merges in the current function's constants, and saves:

```python
branch_params = {"bandpass.low_hz": 20, "compute_rms.window": 50}
```

This output now carries the full pipeline history. A three-step pipeline would accumulate three functions' worth of branch params.

### Why `record_id` works as a discriminator

The key insight is that `record_id` is **content-addressed**: it is deterministically computed from the data and metadata. Two records at the same schema location with different upstream variants will necessarily have different data (since they were computed from different inputs), and therefore different `content_hash` values, and therefore different `record_id` values.

This means `record_id` naturally distinguishes variants without needing to carry the variant metadata itself. The `branch_params` carry the human-readable variant history; the `record_id` carries the machine-usable identity.

---

## The scidb wrapper types

scidb has its own `Fixed`, `Merge`, `ColumnSelection`, and `ColName` classes that parallel scifor's, but wrap variable *types* (classes) rather than DataFrames.

### `scidb.Fixed(var_type, **fixed_metadata)`

Source: `/workspace/scidb/src/scidb/fixed.py`

Same concept as `scifor.Fixed`, but wraps a variable class instead of a DataFrame. The inner `var_type` is loaded from the database with the fixed metadata overrides.

```python
Fixed(StepLength, session="BL")
# During loading: StepLength.load_all() is called, then the resulting
# DataFrame is wrapped in scifor.Fixed with the stringified fixed metadata
```

Has a `to_key()` method for version-key serialization, producing strings like `Fixed(StepLength, session='BL')`.

Key property: `var_type` (not `data` as in scifor.Fixed).

**Why Fixed inputs have `__record_id` stripped (Step 10):** Fixed inputs are not part of the variant expansion system. A `Fixed(StepLength, session="BL")` input always loads the same baseline data regardless of the current iteration's session — it is *intentionally* not variant-dependent. Including its record IDs in the rid expansion would create spurious combos (one per baseline record), which is incorrect. So `__record_id` and `__branch_params` are stripped during loading, and the Fixed input passes through scifor as a plain DataFrame.

### `scidb.Merge(*var_specs)`

Source: `/workspace/scidb/src/scidb/merge.py`

Wraps 2+ variable types (or Fixed/ColumnSelection wrappers) for combined loading. Each constituent is loaded independently and merged column-wise.

Key property: `var_specs` (not `tables` as in scifor.Merge).

If any constituent lacks `load_all()`, the entire Merge becomes a `PerComboLoaderMerge` — loaded individually per combo.

### `scidb.ColumnSelection(var_type, columns)`

Source: `/workspace/scidb/src/scidb/column_selection.py`

Created automatically by `BaseVariable.__class_getitem__` when using bracket syntax:

```python
GaitData["step_length"]           # single column -> numpy array
GaitData[["left_step", "right"]]  # multiple -> DataFrame subset
```

Has a `load()` method that delegates to the inner var_type, and comparison operators (`==`, `!=`, `<`, etc.) that produce `ColumnFilter` objects for database-level filtering.

Key property: `var_type` (not `data` as in scifor.ColumnSelection).

### `scidb.ColName(var_type)`

Source: `/workspace/scidb/src/scidb/colname.py`

Resolves to the data column name of a variable type by querying the `_variables` table in the database for dtype metadata — rather than inspecting a DataFrame's columns as scifor.ColName does.

### `scidb.EachOf(*alternatives)`

Source: `/workspace/scidb/src/scidb/each_of.py`

Unique to scidb (no scifor equivalent). Expresses "run this for_each call once for each alternative." Alternatives can be variable types, constants, or `where=` filters. Multiple EachOf axes produce a Cartesian product of recursive `for_each()` calls.

### `PathInput(path_template, root_folder=None)`

Source: `/workspace/scifor/src/scifor/pathinput.py`

Defined in scifor but used primarily through scidb. Resolves a format-string template to a filesystem path per-combo. Has a `discover()` method that walks the filesystem to find all matching files and extract metadata values from path components.

Always becomes a `PerComboLoader` (paths cannot be bulk-loaded).

---

## The PerComboLoader mechanism

Some variable types cannot be bulk-loaded (they lack a `load_all()` method). For these, scidb creates sentinel objects that travel through scifor's loop as opaque constants and are resolved just before the function is called.

### `PerComboLoader` (lines 26–40)

Wraps a single input spec (plain class, Fixed, ColumnSelection, or PathInput). When the wrapped function encounters it, `_resolve_per_combo_loader()` calls `spec.load(**combo_metadata)` to load the data for the current combo.

### `PerComboLoaderMerge` (lines 43–52)

Wraps a `scidb.Merge` where some constituents lack `load_all()`. When encountered, `_resolve_per_combo_merge()` loads each constituent individually and merges them.

The wrapper function (Step 16) checks each kwarg for these sentinels before calling the original function, resolving them transparently.

---

## How scidb.for_each relates to scihist.for_each

`scihist.for_each()` sits above `scidb.for_each()` and adds lineage tracking. It:

1. Wraps the user's function (which must be a `@lineage_fcn`) so that lineage metadata is recorded on each call
2. Provides a `_pre_combo_hook` implementing `skip_computed` — checking whether an output already exists with matching upstream provenance
3. Sets `_inject_combo_metadata=True` when the function has `generates_file=True` (so the function knows the current subject/session for file generation)
4. After `scidb.for_each()` returns, the lineage records have already been saved via the wrapped function

The layering is:
```
scihist.for_each()   -- lineage + skip_computed
  |-> scidb.for_each()  -- DB load + version keys + save
       |-> scifor.for_each()  -- pure iteration loop
```

Each layer adds one concern and delegates the rest downward.

---

## Logging

scidb logs to a file (set by `configure_database()`) via the `Log` class (source: `/workspace/scidb/src/scidb/log.py`). Every significant operation is logged:

- Input loading: type, row count, timing
- Empty-list resolution: key, number of values resolved
- Combo filtering: how many combos removed
- Rid expansion: base combos vs. expanded combos
- Skip_computed: how many combos skipped
- Save: record ID, data shape, timing
- Errors: full messages

scifor's per-combo log lines (`[run]`, `[done]`, `[skip]`) are forwarded to the same log file via `_log_fn=Log.info`.

The log format is `[HH:MM:SS.FFF] [LEVEL] message`, written to `scidb.log` next to the database file.

---

## Error handling

scidb inherits scifor's skip-and-continue philosophy for per-combo errors. Additionally:

- **Save failures** (lines 1169–1174, 1188–1193): If `output_obj.save()` raises an exception, the error is printed and logged but does not stop other saves or subsequent combos.
- **Load failures** in per-combo mode: Raised inside the wrapped function, caught by scifor's try/except, logged as `[skip]`.
- **Missing data during rid expansion** (lines 436–438): If a rid dimension has zero records for a combo, the combo is silently dropped — no error, no skip message.
- **EachOf cancellation** (lines 170–172): If cancel is detected between EachOf alternatives, remaining alternatives are skipped.

---

## A concrete end-to-end example

```python
from scidb import configure_database, BaseVariable, for_each

db = configure_database("experiment.duckdb", ["subject", "session"])

class RawEMG(BaseVariable):
    schema_version = 1

class FilteredEMG(BaseVariable):
    schema_version = 1

def bandpass(signal, low_hz, high_hz):
    # ... apply bandpass filter ...
    return filtered_signal

for_each(
    bandpass,
    inputs={"signal": RawEMG, "low_hz": 20, "high_hz": 450},
    outputs=[FilteredEMG],
    subject=[1, 2],
    session=["A", "B"],
)
```

What happens:

1. **EachOf expansion**: No EachOf wrappers — skip.

2. **Empty-list resolution**: No empty lists — skip.

3. **PathInput discovery**: No PathInput — skip.

4. **Propagate schema**: `scifor.set_schema(["subject", "session"])`.

5. **Stringify metadata**: `subject=["1", "2"], session=["A", "B"]`.

6. **Output names**: `["FilteredEMG"]` (from `FilteredEMG.view_name()`, which returns `"FilteredEMG"`).

7. **Dry-run**: Not dry-run — skip.

8. **ForEachConfig**: Produces:
   ```python
   {
       "__fn": "bandpass",
       "__fn_hash": "a1b2c3d4e5f6g7h8",
       "__inputs": '{"signal": "RawEMG"}',
       "__constants": '{"high_hz": 450, "low_hz": 20}',
   }
   ```

9. **Pre-filter combos**: No empty lists resolved — skip.

10. **Load inputs**:
    - `signal`: `RawEMG` has `load_all()` → bulk-load all records → assemble into DataFrame:
      ```
      | subject | session | RawEMG       | __record_id      | __branch_params |
      |---------|---------|--------------|------------------|-----------------|
      | 1       | A       | [0.1, 0.2..] | e3f4a5b6c7d8e9f0 | {}              |
      | 1       | B       | [0.3, 0.4..] | g1h2i3j4k5l6m7n8 | {}              |
      | 2       | A       | [0.5, 0.6..] | o9p0q1r2s3t4u5v6 | {}              |
      | 2       | B       | [0.7, 0.8..] | w7x8y9z0a1b2c3d4 | {}              |
      ```
      (Schema columns stringified; `__branch_params` is `{}` because RawEMG is raw data with no upstream pipeline.)
    - `low_hz`: constant 20 → pass through
    - `high_hz`: constant 450 → pass through

11. **Variant tracking**: DataFrame has `__record_id` → rename to `__rid_signal`. Build `rid_to_bp`:
    ```python
    rid_to_bp = {
        "e3f4a5b6c7d8e9f0": {},  # no upstream variants
        "g1h2i3j4k5l6m7n8": {},
        "o9p0q1r2s3t4u5v6": {},
        "w7x8y9z0a1b2c3d4": {},
    }
    ```
    Strip `__branch_params` column.

12. **Expand combos**: 4 base combos. Each schema location has exactly 1 record ID → no expansion needed (4 → 4).

13. **Persist expected combos**: Write 4 entries to `_for_each_expected`.

14. **Skip_computed**: No hook — skip.

15. **Extend schema**: `scifor.set_schema(["subject", "session", "__rid_signal"])`.

16. **Wrap fn**: No PerComboLoader inputs, no metadata injection — skip.

17. **Delegate to scifor**: scifor iterates over each combo, filters the DataFrame to the matching row (by subject + session + `__rid_signal`), drops schema columns, extracts the scalar signal value, calls `bandpass(signal=<array>, low_hz=20, high_hz=450)`, collects results.

18. **Restore schema**: `scifor.set_schema(["subject", "session"])` — remove `__rid_signal` so subsequent calls are not affected.

19. **Save results**: For each result row:
    - Look up `__rid_signal` in `rid_to_bp` → `{}` (no upstream branch params for raw data)
    - Add `{"bandpass.low_hz": 20, "bandpass.high_hz": 450}` to branch_params
    - Build save metadata including `__fn`, `__fn_hash`, `__inputs`, `__constants`, `__branch_params`, `__upstream`
    - Call `FilteredEMG.save(filtered_value, **save_metadata)`
    - The save generates a `record_id` via SHA-256 of the class name, schema version, content hash, and metadata
    - A row is inserted into `_record_metadata` and the data is written to `FilteredEMG_data`
    - Print: `[save] subject=1, session=A, low_hz=20, high_hz=450: FilteredEMG -> record_id=abc123... (ndarray shape=(100,)) in 0.003s`

Now if a second `for_each` call runs with `low_hz=50`, new `FilteredEMG` records are created with different `record_id` values and different `branch_params` (`{"bandpass.low_hz": 50}`). Both variants coexist in the database. A downstream `for_each` loading `FilteredEMG` will see both variants and, through the rid expansion mechanism, process each one separately.
