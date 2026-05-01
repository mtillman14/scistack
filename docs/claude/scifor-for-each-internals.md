# How `scifor.for_each()` Works — A Detailed Walkthrough

## Why this document exists

`scifor.for_each()` is the core iteration engine behind the entire SciStack batch processing system. If you use `scidb.for_each()`, you are using `scifor.for_each()` under the hood. This document explains exactly what happens when you call it, step by step, with references to the source code so you can verify every claim.

All source references are to `/workspace/scifor/src/scifor/foreach.py` unless otherwise noted.

---

## What problem does it solve?

Scientific data is almost always organized by experimental conditions — subjects, sessions, trials, limbs, speeds. Analyzing it means writing nested loops that slice a table to the right rows, call your analysis function, and collect the results. The loop logic obscures the analysis, and the boilerplate multiplies with each additional condition.

`scifor.for_each()` replaces that boilerplate. You hand it:

1. A function
2. A dict of inputs (DataFrames and/or constants)
3. The experimental conditions to iterate over

It generates every combination of conditions, filters each DataFrame input to the matching rows, calls your function with the filtered data and the constants, and collects the results into a single output DataFrame.

It is a **pure loop orchestrator**. It does no I/O — no database loading, no file saving. All inputs must already be in memory as pandas DataFrames or plain Python values. (Database loading and saving are handled by the `scidb` layer above, which delegates the iteration itself to `scifor`.)

---

## The function signature

```python
def for_each(
    fn: Callable,
    inputs: dict[str, Any],
    dry_run: bool = False,
    as_table: list[str] | bool | None = None,
    distribute: bool = False,
    where=None,
    output_names: list[str] | int | None = None,
    _all_combos: list[dict] | None = None,
    _log_fn: Callable[[str], None] | None = None,
    _progress_fn: Callable[[dict], None] | None = None,
    _cancel_check: Callable[[], bool] | None = None,
    **metadata_iterables: list[Any],
) -> pd.DataFrame | None
```

Source: lines 15–28.

Parameters prefixed with `_` are internal hooks used by the `scidb` wrapper and the GUI. The user-facing parameters are `fn`, `inputs`, `dry_run`, `as_table`, `distribute`, `where`, `output_names`, and the `**metadata_iterables`.

---

## Step-by-step execution

### Step 1: Resolve output column names (lines 60–67)

The `output_names` parameter controls what the output columns are called in the result DataFrame:

- `None` (default) → `["output"]` — a single output column named "output"
- An integer `N` → `["output_1", "output_2", ..., "output_N"]`
- A list of strings → used as-is

This determines `n_outputs`, which is used later when collecting results.

### Step 2: Resolve empty-list metadata iterables (lines 69–78)

If you pass `subject=[]` (an empty list), scifor interprets that as "use every distinct value of `subject` that exists in the input DataFrames." It scans all DataFrame inputs for the column named `subject`, collects distinct non-null values, sorts them, and substitutes the result back into `metadata_iterables`.

This resolution only happens in standalone mode (when `_all_combos is None`). When called from `scidb.for_each()`, the database layer resolves empty lists via SQL queries instead and passes pre-built combos via `_all_combos`.

The scan logic lives in `_distinct_values_from_inputs()` (lines 691–707). It unwraps `Fixed` and `ColumnSelection` wrappers to reach the raw DataFrame, checks for the column, and collects unique values. If no input DataFrame has the column, it raises a `ValueError`.

### Step 3: Validate the `distribute` parameter (lines 80–98)

`distribute=True` means "my function returns a vector or table, and I want each element/row to become a separate record at the next-deeper schema level."

For this to work, scifor needs to know what schema level to expand into. It:

1. Finds which of the `**metadata_iterables` keys are also schema keys (set via `scifor.set_schema()`), in schema order.
2. Takes the deepest (last) one — this is the level you are currently iterating at.
3. Looks one level deeper in the schema — this is the distribute target.

If the deepest iterated key is already the deepest schema key (there is no level below), it raises a `ValueError`.

**Concrete example:** Schema is `["subject", "session", "trial"]`. You iterate over `subject` and `session`. The deepest iterated schema key is `session`. The next level down is `trial`. So if your function returns a 5-element array, scifor will expand it into 5 rows with `trial=1, 2, 3, 4, 5`.

### Step 4: Resolve `ColName` wrappers (line 101)

`ColName(df)` is a small utility wrapper. It resolves to the name (as a string) of the single non-schema data column in a DataFrame. This is useful when your function needs to know the column name programmatically — for example, to label a plot axis.

`_resolve_colnames()` (lines 422–457) iterates over all inputs and replaces any `ColName` instance with the resolved string. If the DataFrame has zero or more than one data column (after excluding schema keys), it raises a `ValueError`.

After this step, `ColName` wrappers are gone; they have been replaced by plain strings in the `inputs` dict.

### Step 5: Classify inputs as data vs. constant (lines 103–110)

Every entry in the `inputs` dict is classified as either a **data input** or a **constant input**.

A value is a data input if it is:
- A pandas DataFrame
- A `Fixed` wrapper
- A `Merge` wrapper
- A `ColumnSelection` wrapper

Everything else is a constant — scalars, strings, objects, numpy arrays that are not wrapped, etc.

The classification function is `_is_data_input()` (lines 400–406).

Data inputs are filtered per iteration. Constants pass through to the function unchanged every time.

**Important subtlety:** A DataFrame that has no schema key columns is still classified as a data input by `_is_data_input()`, but it will not be filtered — `_prepare_input()` detects this via `_is_per_combo_df()` (line 464) and passes it through unchanged. So in practice, a DataFrame without schema columns behaves like a constant, but it is still processed through the data-input path.

### Step 6: Resolve `as_table` to a set of input names (lines 118–124)

The `as_table` parameter controls whether schema-key columns are stripped from DataFrames before they reach your function.

- `True` → all data inputs keep their schema columns (you get a full DataFrame with subject, session, etc. columns present)
- A list of strings → only the named inputs keep schema columns
- `False` / `None` (default) → all data inputs have schema columns stripped

The resolved set is stored as `as_table_set` and consulted during per-combo input preparation.

### Step 7: Build the combination list (lines 126–133)

If `_all_combos` is provided (by the `scidb` wrapper), it is used directly.

Otherwise, scifor builds the list by taking the Cartesian product (via `itertools.product`) of all the value lists in `**metadata_iterables`.

**Example:**
```python
subject=[1, 2], session=["pre", "post"]
```
produces:
```python
[
    {"subject": 1, "session": "pre"},
    {"subject": 1, "session": "post"},
    {"subject": 2, "session": "pre"},
    {"subject": 2, "session": "post"},
]
```

Four combinations. The function will be called (at most) four times.

### Step 8: Print the summary banner (lines 138–182)

Before any iteration begins, scifor prints a formatted header showing:
- The function name and total iteration count
- A summary of metadata dimensions (e.g., `subject=[3 values], session=[2 values]`)
- The actual metadata values
- Any non-default options (`dry_run`, `distribute`, `as_table`, `where`)

This banner is always printed — it is not gated by a verbosity flag.

### Step 9: The main loop (lines 197–347)

For each combination in `all_combos`:

#### 9a. Cancellation check (lines 199–216)

If a `_cancel_check` callable was provided (by the GUI), it is called before each iteration. If it returns `True`, the loop breaks immediately. This enables cooperative cancellation from a user interface.

#### 9b. Progress callback — start (lines 220–228)

If a `_progress_fn` was provided, it is called with an event dict: `{"event": "combo_start", "current": N, "total": M, "completed": C, "skipped": S, "metadata": {...}}`. This is how the GUI updates its progress bar.

#### 9c. Dry-run short-circuit (lines 230–233)

If `dry_run=True`, scifor prints what *would* happen for this combination (which inputs would be filtered, which constants would be passed) and moves to the next combo. No data is filtered, no function is called.

#### 9d. Input preparation — the core data-slicing step (lines 235–272)

For each data input, scifor calls `_prepare_input()` (lines 519–556). This is where the actual data filtering happens. The process depends on the input type:

**Plain DataFrame:**
1. Check whether the DataFrame has any schema-key columns (`_is_per_combo_df()`, line 464). If not, pass it through unchanged — it is effectively a constant table.
2. Filter rows to match the current combination's metadata (`_filter_df_for_combo()`, lines 469–480). For each schema key that exists as both a column in the DataFrame and a key in the current combination, only rows where the column value equals the metadata value are kept.
3. Apply any `where` filter on top (`_apply_where_filter()`, lines 483–488).
4. Extract data (`_extract_data()`, lines 491–512):
   - If `as_table=True` for this input: return the full filtered DataFrame with schema columns.
   - Otherwise, drop schema-key columns. Then:
     - **1 row, 1 data column** → extract the scalar value (not a DataFrame, not an array — the raw value).
     - **Multiple rows, 1 data column** → return a numpy column vector with shape `(N, 1)`.
     - **Multiple data columns** → return a sub-DataFrame with only the data columns.

**`Fixed` wrapper:**
The fixed metadata overrides are merged into the effective metadata before filtering. For example, if the current combo is `{subject: 1, session: "post"}` and the input is `Fixed(df, session="pre")`, the DataFrame is filtered with `{subject: 1, session: "pre"}` — the fixed session overrides the iterated one.

**`ColumnSelection` wrapper:**
After filtering (and possibly after `_extract_data`), only the specified columns are extracted. A single column becomes a numpy array; multiple columns become a sub-DataFrame. (`_apply_column_selection()`, lines 585–595.)

**`Merge` wrapper:**
Each constituent of the Merge is filtered independently (applying `Fixed` and `ColumnSelection` to individual constituents as needed). Schema-key columns are dropped from each. Then the parts are checked for row-count compatibility — all multi-row parts must have the same length. Single-row parts are broadcast (repeated) to match. Finally, all parts are concatenated column-wise (`pd.concat(..., axis=1)`) into a single DataFrame. Column names must be unique across all constituents or an error is raised.

The Merge logic lives in `_prepare_merge()` (lines 602–644) and `_merge_parts()` (lines 647–684).

**Error handling during filtering:** If any input's filtering/preparation raises an exception, the combo is skipped. The error is printed as `[skip]`, logged to `/tmp/scihist_diag.log`, and the loop continues to the next combination. No partial results are collected for failed combos.

#### 9e. Function call (lines 274–314)

The filtered data inputs and the constant inputs are merged into a single kwargs dict, and the function is called:

```python
result = fn(**kwargs)
```

That's it. There is no output-count dispatch — `_call_fn()` (line 370–372) simply calls `fn(**kwargs)` and returns whatever the function returns.

If the function raises an exception, the combo is skipped with the same graceful-skip logic as filtering errors: print `[skip]`, log the traceback, continue.

Each successful call prints `[done] subject=1, session=pre: my_fn completed in 0.042s` with wall-clock timing.

#### 9f. Normalize single output to tuple (lines 316–318)

If the function returned a single value (not a tuple), it is wrapped in a 1-tuple for uniform handling.

#### 9g. Handle `distribute` (lines 320–336)

If `distribute_key` is set, each output value is split into individual pieces by `_split_for_distribute()` (lines 768–798):

| Return type | Split behavior |
|---|---|
| pandas DataFrame | One row → one piece (each piece is a single-row DataFrame) |
| numpy 1D array | Each element → one piece (scalar) |
| numpy 2D array | Each row → one piece (1D array) |
| Python list | Each element → one piece |
| Anything else | `TypeError` is raised |

Each piece is collected as a separate row with the distribute key set to a 1-based index. So a 5-element array from combo `{subject: 1, session: "pre"}` becomes 5 rows: `{subject: 1, session: "pre", trial: 1}` through `{..., trial: 5}`.

If distribute is not active, the result tuple is collected as a single row.

#### 9h. Progress callback — done (lines 338–347)

After a successful function call, the progress callback fires with `"event": "combo_done"`.

### Step 10: Build the output DataFrame (lines 349–367)

After all combos have been processed (or cancelled), scifor calls `_results_to_output_dataframe()` (lines 725–761) to assemble the final result.

There are two assembly modes, chosen automatically based on the output types:

**Scalar mode** (at least one output is not a DataFrame):

Each combo becomes one row. The row contains all metadata keys as columns, plus one column per output name. The output values are placed directly in the cells.

| subject | session | output |
|---|---|---|
| 1 | pre | 0.82 |
| 1 | post | 1.47 |
| 2 | pre | 0.91 |

**Flatten mode** (all outputs are DataFrames):

The output DataFrames are concatenated column-wise per combo. The metadata values are replicated for every row of the output. The result has one row per output-DataFrame-row, not one row per combo.

| subject | session | metric_a | metric_b |
|---|---|---|---|
| 1 | pre | 0.1 | 0.3 |
| 1 | pre | 0.2 | 0.4 |
| 2 | pre | 0.5 | 0.6 |

If all combos were skipped or cancelled and no results were collected, an empty DataFrame is returned.

If `dry_run=True`, the function returns `None`.

---

## The input wrapper types

### `Fixed(data, **fixed_metadata)`

Source: `/workspace/scifor/src/scifor/fixed.py`

Overrides specific metadata keys during filtering. The DataFrame is still filtered per combo, but the specified keys use the fixed values instead of the current iteration's values.

```python
Fixed(df, session="pre")
```

For combo `{subject: 1, session: "post"}`, the DataFrame is filtered with `{subject: 1, session: "pre"}`.

`Fixed` can wrap a plain DataFrame or a `ColumnSelection`. It cannot wrap a `Merge` — use `Merge(Fixed(...), ...)` instead.

### `Merge(*tables)`

Source: `/workspace/scifor/src/scifor/merge.py`

Combines 2+ DataFrames (or wrapped DataFrames) into a single DataFrame by column-wise concatenation after filtering each constituent independently.

Requirements:
- At least 2 constituents
- No duplicate column names across constituents
- All multi-row constituents must have the same row count; single-row constituents are broadcast
- Cannot nest a `Merge` inside another `Merge`

### `ColumnSelection(data, columns)`

Source: `/workspace/scifor/src/scifor/column_selection.py`

Extracts specific columns from a DataFrame after filtering.

- Single column → numpy array of values
- Multiple columns → sub-DataFrame

### `ColName(data)`

Source: `/workspace/scifor/src/scifor/colname.py`

Resolves to the string name of the single non-schema data column in a DataFrame. This is not a runtime wrapper — it is resolved once during Step 4 and replaced with a plain string in the inputs dict.

### `Col(column_name)`

Source: `/workspace/scifor/src/scifor/filters.py`

Entry point for building row-level filters used with the `where=` parameter:

```python
Col("speed") > 1.5                          # ColFilter
(Col("side") == "R") & (Col("speed") > 1.5) # CompoundFilter
~(Col("side") == "R")                       # NotFilter
```

These are applied *after* combo-based metadata filtering, as an additional row-level predicate.

---

## How a DataFrame is determined to be "per-combo"

This is a common source of confusion. A DataFrame is filtered per-combo if and only if **at least one of its column names matches a schema key** (`_is_per_combo_df()`, line 464–466).

```python
set_schema(["subject", "session"])

# Per-combo: has "subject" column → will be filtered
df1 = pd.DataFrame({"subject": [1, 2], "emg": [0.1, 0.2]})

# NOT per-combo: no schema columns → passed through unchanged
df2 = pd.DataFrame({"freq_low": [10], "freq_high": [100]})
```

There is no explicit flag to mark a DataFrame as "iterate" vs. "constant." The schema column check is the sole mechanism.

---

## Why scifor has no EachOf expansion

`EachOf` is a scidb-only concept (source: `/workspace/scidb/src/scidb/each_of.py`). It expresses "run this entire `for_each` call once for each alternative" — for example, `EachOf(StepLength, StepTime)` means "run the same pipeline once with StepLength as input, once with StepTime." This is resolved at the top of `scidb.for_each()` by recursively calling `for_each()` with each concrete alternative.

scifor does not need this because scifor operates on already-loaded in-memory DataFrames, not variable types. By the time data reaches scifor, all `EachOf` alternatives have already been resolved into concrete values by scidb. The recursive decomposition happens entirely above the scifor layer.

---

## PathInput — resolving file paths from metadata

`PathInput` (source: `/workspace/scifor/src/scifor/pathinput.py`) allows a `for_each` input to be a file path constructed from the current combo's metadata. It is defined in the scifor package but is primarily used through the scidb layer, where it is wrapped in a `PerComboLoader` sentinel and resolved per-combo.

**How it works:**

A `PathInput` is constructed with a format-string template and an optional root folder:

```python
PathInput("{subject}/trial_{trial}.mat", root_folder="/data")
```

On each iteration, `PathInput.load(**metadata)` (lines 70–83 of `pathinput.py`) substitutes the current combo's metadata into the template and returns the resolved `Path`:

```python
# For combo {subject: 1, trial: 2}:
Path("/data/1/trial_2.mat")
```

If no `root_folder` is given and the path is relative, it searches upward from the current directory for a `pyproject.toml` or `scistack.toml` to find the project root.

**Filesystem discovery** (`PathInput.discover()`, lines 95–118):

PathInput can also walk the filesystem to *find* all matching files and extract metadata from the path components. This is used by `scidb.for_each()` when metadata iterables are empty or unspecified — the filesystem itself tells scidb which subjects and trials exist.

Discovery works by splitting the template into path segments, converting each segment containing `{placeholders}` into a regex with named capture groups (e.g., `{subject}_data` becomes `(?P<subject>[^/\\]+)_data`), and recursively matching against actual directory entries. Each complete path match produces a metadata dict (e.g., `{"subject": "1", "trial": "2"}`).

**In scifor's context**, PathInput is not directly handled by `scifor.for_each()`. When scidb encounters a PathInput, it wraps it in a `PerComboLoader` sentinel, which scifor passes through as an opaque constant. The sentinel is resolved by a wrapper function around `fn` just before the actual function call — so scifor never sees or processes PathInput directly.

---

## How scifor relates to scidb

`scidb.for_each()` (in `/workspace/scidb/src/scidb/foreach.py`) is the database-backed wrapper. It:

1. Resolves empty `[]` metadata lists by querying the database for stored values
2. Loads all input variables from DuckDB into in-memory DataFrames
3. Builds version keys that fingerprint the computation (function hash, input specification, constant values)
4. Pre-filters the combination list to only combinations that exist in the database
5. **Delegates the actual iteration loop to `scifor.for_each()`** — passing the loaded DataFrames, the pre-built combo list (via `_all_combos`), and callbacks (via `_log_fn`, `_progress_fn`)
6. After `scifor.for_each()` returns, saves results back to the database with appropriate metadata and version keys

The separation means that `scifor` can be used independently with plain DataFrames (no database required), while `scidb` adds the persistence layer on top.

---

## Error handling philosophy

scifor uses a **skip-and-continue** model. If a combo fails — whether during input filtering or during the function call — it is skipped. The error is:

1. Printed to stdout as `[skip] subject=1, session=pre: reason`
2. Logged with a full traceback to `/tmp/scihist_diag.log`
3. Also printed to stderr via `traceback.print_exc()`

The loop continues to the next combo. The final summary shows how many combos completed vs. skipped.

This design choice reflects the reality of experimental data: missing combinations are normal (a subject might not have a particular session), and a single failure should not prevent the other 149 combinations from running.

---

## A concrete end-to-end example

```python
import pandas as pd
from scifor import set_schema, for_each

set_schema(["subject", "session"])

data = pd.DataFrame({
    "subject":  [1,   1,    2,   2],
    "session":  ["A", "B", "A", "B"],
    "emg":      [0.1, 0.2, 0.3, 0.4],
})

def double(emg):
    return emg * 2

result = for_each(
    double,
    inputs={"emg": data},
    subject=[1, 2],
    session=["A", "B"],
)
```

What happens:

1. **Output names** → `["output"]` (default)
2. **No empty lists** to resolve
3. **No distribute** validation needed
4. **No ColName** wrappers to resolve
5. **Classify inputs**: `emg` is a DataFrame → data input. No constants.
6. **as_table** → `set()` (default: strip schema columns)
7. **Build combos**: `[{subject:1, session:"A"}, {subject:1, session:"B"}, {subject:2, session:"A"}, {subject:2, session:"B"}]`
8. **Print banner**: `for_each(double) — 4 iterations`
9. **Loop**:
   - Combo `{subject: 1, session: "A"}`:
     - Filter `data` → 1 row where subject=1 and session="A": `emg=0.1`
     - Drop schema columns → 1 row, 1 data column → extract scalar: `0.1`
     - Call `double(emg=0.1)` → returns `0.2`
     - Collect: `({subject: 1, session: "A"}, (0.2,))`
   - Combo `{subject: 1, session: "B"}`:
     - Filter → `emg=0.2` → scalar → `double(0.2)` → `0.4`
   - Combo `{subject: 2, session: "A"}`:
     - Filter → `emg=0.3` → scalar → `double(0.3)` → `0.6`
   - Combo `{subject: 2, session: "B"}`:
     - Filter → `emg=0.4` → scalar → `double(0.4)` → `0.8`
10. **Build output DataFrame** (scalar mode — outputs are floats, not DataFrames):

| subject | session | output |
|---|---|---|
| 1 | A | 0.2 |
| 1 | B | 0.4 |
| 2 | A | 0.6 |
| 2 | B | 0.8 |

The function `double` never needed to know about subjects or sessions. It received a single float and returned a single float. scifor handled all the slicing and assembly.
