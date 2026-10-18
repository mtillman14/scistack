# scifor

`scifor` runs a function once for every combination of your experimental conditions (subject, session, trial, ...). It slices each input table to the matching rows, calls your function, and collects the results into one table. It needs no database: inputs are plain pandas DataFrames (Python) or tables (MATLAB).

Reach for `scifor` when you want the loop without the storage. When you want inputs loaded from and outputs saved to a database, use [`scidb.for_each`](scidb.md#batch-processing), which runs `scifor` underneath and takes the same input wrappers.

```python
from scifor import set_schema, for_each, Fixed, Col
```

## Schema

The schema names the columns that identify *where* a value belongs. Every other column is data.

| Name | Purpose |
|---|---|
| `set_schema(keys)` | Declare the schema keys, outermost first, e.g. `["subject", "session"]`. |
| `get_schema()` | Return the current schema keys. |
| `expand_schema_keys(keys, metadata_iterables)` | Turn `schema_keys=[...]` into `key=[]` iterables. Used internally by both `scifor` and `scidb`. |

## for_each

```python
for_each(fn, inputs, outputs=None, dry_run=False, as_table=None, distribute=False,
         where=None, locations=None, schema_keys=None,
         **metadata_iterables)
```

Runs `fn` once per combination of the `metadata_iterables` and returns a DataFrame with the schema columns first, then one column per output.

/// tab | Python
```python
set_schema(["subject", "session"])

results = for_each(
    my_analysis,
    inputs={"emg": data_table, "cutoff_hz": 20},
    subject=[1, 2, 3],
    session=["pre", "post"],
)
```
///

/// tab | MATLAB
```matlab
scifor.set_schema(["subject", "session"]);

results = scifor.for_each(@my_analysis, ...
    struct('emg', data_table, 'cutoff_hz', 20), ...
    subject=[1, 2, 3], session=["pre", "post"]);
```
///

**Inputs.** Each entry of `inputs` is a DataFrame (sliced per combination), an input wrapper (below), or a constant (passed through unchanged on every call).

**Iteration.** Pass each schema key as a list of values. An empty list (`subject=[]`) means "every value found in the data". `schema_keys=["subject"]` is shorthand for `subject=[]`; `schema_keys=None` with no iterables runs `fn` once on all rows.

| Option | Use it to |
|---|---|
| `dry_run=True` | Print the combinations and what each call would receive, without calling `fn`. |
| `as_table=["x"]` / `True` | Pass those inputs as the full sliced DataFrame (schema columns included) instead of just the data column(s). |
| `distribute=True` | Split a returned vector or table into one row per element, one schema level deeper (e.g. a trial into gait cycles). |
| `where=` | Keep only rows matching a [column filter](#filters), on top of the per-combination slicing. |
| `locations=` | Run only selected schema locations. See [`LocationFilter`](#location-selection). |
| `outputs=` | Name the output columns: names, classes, or a count for a function with several returns. Also the third positional argument, as in `scidb.for_each`. |

**Output column names.** Each entry of `outputs` names one column: a string is used as is; a class (or instance) uses its `column_name` class attribute if set, otherwise its class name. In MATLAB, define `column_name` as a `Constant` property. `scidb.for_each` names its columns by the same rule (`scifor.output_column_name`). The name only applies when an output fits in one cell; a returned DataFrame whose rows spread keeps its own columns.

**Calls written for `scidb.for_each`.** The database-only options (`save`, `db`, `introspect`, `skip_computed`, `schema_filter`, `finalized`, `glue`, `parameter_names`, `pipeline`, listed in `scifor.DB_ONLY_KWARGS`) are accepted and ignored, with one INFO log line, so a `scidb.for_each` call runs here unchanged once its inputs are DataFrames. MATLAB also ignores `preload` and `parallel`. Those names cannot be used as schema keys.

If `fn` returns a DataFrame, its columns are flattened into the result. If one combination raises, the run continues; failures are counted and summarised at the end. A combination with no matching rows is skipped and reported as "no data", not as a failure.

**Errors.** `NoDataError` (no rows for an input), `ColumnFunctionError` and `ForColumnsError` (a per-column run failed or was set up inconsistently). `spread_nested_results(df)` expands a result column whose cells are themselves tables.

## Input wrappers

Wrappers change how one input is sliced for each call. They nest, and the same classes are used by `scidb.for_each`.

| Wrapper | What it does | Typical use |
|---|---|---|
| `Fixed(data, **meta)` | Slice this input with fixed values for some keys instead of the current ones. | Compare every session to `Fixed(df, session="BL")`. |
| `Merge(a, b, ...)` | Slice each constituent, then join them column-wise into one DataFrame. | A function that needs kinematics and force together. |
| `ColumnSelection(data, cols, iterate=False)` | Pass only some columns. With `iterate=True`, call `fn` once per column and reassemble. | Run one function over every channel of a table. |
| `ColName(df)` / `ColName()` | A constant that resolves to a column *name*: the single data column of `df`, or the column currently being iterated. | Label a result with the channel name. |
| `EachOf(a, b, ...)` | Alternatives for an input, constant, or `where=`. Each alternative is a separate run; several `EachOf`s form a Cartesian product. | Try `EachOf(10, 20, 30)` cutoffs in one call. |
| `PathInput(template, root_folder=None, *, name)` | Resolve a file path from the current combination, e.g. `"{subject}/trial_{trial}.mat"`. Your function receives the path. | Load raw files laid out by condition. |
| `PathOutput(template)` | Resolve an output path from the combination (and `{ColName}` during per-column runs). | Save one figure per subject. |

`PathInput` also supports `aliases=` (several on-disk spellings of one value, e.g. `"Baseline"` for `"BL"`), `key_regex=` (split adjacent placeholders like `"{speed}{trial}"`), and `.discover()` (list every combination the files on disk provide). Relative templates resolve against the project root: `set_project_root(path)`, `get_project_root()`, `project_root()`, `clear_project_root()`. Directory listings are cached; `clear_listing_cache()` resets the cache after files change.

`require_alternatives(each_of, kind=...)` raises when an `EachOf` has no alternatives. `for_each` calls it for you, so an empty axis fails loudly instead of running zero times.

## Filters

Build row filters for `where=` from `Col`. They combine with `&`, `|` and `~`.

```python
where = (Col("side") == "R") & (Col("speed") > 1.2)
```

`Col(name)` supports `==`, `!=`, `<`, `<=`, `>`, `>=` and produces a `ColFilter`. Combinations produce `CompoundFilter` and `NotFilter`.

## Location selection

`where=` filters rows *within* a combination. `locations=` chooses *which* combinations run.

```python
from scifor import LocationFilter

locations = LocationFilter.build(
    include=[[("subject", "01")], [("subject", "02"), ("trial", "3")]],
    exclude_levels={"session": ["BL"]},
)
```

- `include` lists location prefixes, outermost key first. Use it to select uneven sets, such as all of subject 01 plus trial 3 of subject 02. Empty means everything.
- `exclude_levels` is a standing rule. `{"session": ["BL"]}` drops BL everywhere, including in data added later.

`LocationFilter.of(value)` accepts `None`, a filter, or the plain-dict form `{"include": ..., "exclude_levels": ...}`. `filter_combos(combos, locations)` applies a filter to a list of combination dicts.

## Discovery helpers

The SciStack GUI and `scidb` use these to scan a project's Python files for functions and declarations without running them. They are public for tools built on SciStack. Most users never call them.

| Name | Purpose |
|---|---|
| `walk_package(package_name, on_module)` → `WalkResult` | Import every module under a package, call `on_module` on each, collecting per-module failures (`ModuleWalkError`) instead of stopping. |
| `is_test_path` / `is_test_modname` | The single rule for excluding test files from discovery. |
| `find_top_level_side_effects(source)` → `[TopLevelSideEffect]` | Flag module-level code that would run on import (`BENIGN_TOPLEVEL_CALLS` lists the allowed calls). |
| `sibling_import_dirs`, `PathInsert`, `PathInsertAll`, `purge_module` | Manage `sys.path` and `sys.modules` while importing loose project files. |
| `read_project_name`, `headless_matplotlib` | Read the project name from `pyproject.toml`; force a non-interactive matplotlib backend during scans. |

## Logging

`scifor` logs a start banner, periodic progress and an end-of-run summary that groups failures by reason. Turn on per-combination detail with `scifor.Log.set_level("DEBUG")` in MATLAB, or the `SCIDB_LOG_LEVEL=DEBUG` environment variable in either language.
