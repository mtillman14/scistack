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
| `set_schema(keys)` | Declare the schema keys, outermost first, e.g. `["subject", "session"]`. For a branched schema pass one list per branch, `[["brand", "category", "item"], ["brand", "location", "person"]]`. |
| `get_schema()` | Return the current schema keys, flat, in order. |
| `get_schema_tree()` | The `SchemaTree`: `keys`, `parent_of`, and the rules (`chain`, `children`, `leaves`, `is_chain`, `coherent`, `default_level`, `offered`). Pure. A plain list is one branch. |
| `parse_schema_text(text)` / `format_schema_text(decl)` | The one-text-box form: commas within a branch, semicolons between branches (`"brand, category, item; brand, location, person"`). |
| `expand_schema_keys(keys, metadata_iterables)` | Turn `schema_keys=[...]` into `key=[]` iterables. Used internally by both `scifor` and `scidb`. |

## for_each

/// tab | Python
```python
for_each(
    fn,                        # the function to run once per combination
    inputs,                    # {arg_name: DataFrame | wrapper | constant}
    outputs=None,              # list[str | class] of column names, an int N, or None (= ["output"])
    dry_run=False,             # print the plan, call nothing
    as_table=None,             # True | [input names]: keep schema-key columns on those inputs
    distribute=False,          # split each return into rows one schema level deeper
    where=None,                # Col filter on rows within each combination
    locations=None,            # LocationFilter | {"include": ..., "exclude_levels": ...} | JSON text
    share_limits=None,         # {input_name: [keys held fixed]} -> fn gets `<input>_limits=(min, max)`
    schema_keys=None,          # ["subject", ...] = shorthand for subject=[] ...; not with **iterables
    schema_key_types=None,     # {key: "numeric" | "string"}
    on_error="continue",       # "continue" | "stop"
    project_root=None,         # folder a rootless PathInput resolves against
    hooks=None,                # scifor.LoopHooks; only for layers built on scifor
    **metadata_iterables,      # subject=[1, 2], session=[] ([] = every value in the data)
) -> pd.DataFrame | None       # None when dry_run=True
```
///

/// tab | MATLAB
```matlab
result = scifor.for_each(@fn, inputs, Name=Value, ...)
result = scifor.for_each(@fn, inputs, outputs, Name=Value, ...)
[r1, r2] = scifor.for_each(@fn, inputs, Name=Value, ...)   % several outputs

% inputs:  struct, field order = argument order
% outputs: cell array of names and/or objects, or a count N
% Name=Value: dry_run, as_table (true | string array), distribute, where,
%   on_error ("continue" | "stop"), schema_keys (string array),
%   categorical (true: metadata columns of the result become categorical),
%   outputs, locations (JSON text), project_root, schema_key_types (struct),
%   share_limits (struct); scidb-only options (save, db, ...) are ignored;
%   any other name = a metadata iterable,
%   e.g. subject=[1 2 3], session=["pre" "post"]
```
///

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

**Calls written for `scidb.for_each`.** The database-only options (`save`, `db`, `introspect`, `skip_computed`, `schema_filter`, `glue`, `parameter_names`, `pipeline`, listed in `scifor.DB_ONLY_KWARGS`) are accepted and ignored, with one INFO log line, so a `scidb.for_each` call runs here unchanged once its inputs are DataFrames. MATLAB also ignores `preload` and `parallel`. Those names cannot be used as schema keys.

If `fn` returns a DataFrame, its columns are flattened into the result. If one combination raises, the run continues; failures are counted and summarised at the end. A combination with no matching rows is skipped and reported as "no data", not as a failure.

**Errors.** `NoDataError` (no rows for an input), `ColumnFunctionError` and `ForColumnsError` (a per-column run failed or was set up inconsistently). `spread_nested_results(df)` expands a result column whose cells are themselves tables.

## Input wrappers

A wrapper changes how one input is sliced for each call. Wrappers nest (`Merge(Fixed(a, session="BL"), b, name="ab")`), and `scidb.for_each` takes the same classes, with variable classes in place of DataFrames.

| Wrapper | Use it to |
|---|---|
| [`Fixed`](#fixed) | Read one input at a fixed location, e.g. a baseline session. |
| [`Merge`](#merge) | Join several tables into one input. |
| [`ColumnSelection`](#columnselection) | Pass only some columns, or run once per column. |
| [`ColName`](#colname) | Pass a column's *name* as a constant. |
| [`EachOf`](#eachof) | Try several alternatives in one call. |
| [`PathInput`](#pathinput) | Pass the path of an existing file for each combination. |
| [`PathOutput`](#pathoutput) | Pass the path of a file to write for each combination. |

### Fixed

```python
Fixed(data, **fixed_metadata)
```

Slice `data` with the given schema-key values instead of the current ones. `data` is a DataFrame, `Merge` or `ColumnSelection`. Every keyword is a schema key.

/// tab | Python
```python
inputs={"baseline": Fixed(df, session="BL"), "current": df}
```
///

/// tab | MATLAB
```matlab
struct('baseline', scifor.Fixed(tbl, session="BL"), 'current', tbl)
```
///

### Merge

```python
Merge(*tables, name, how="exact")
```

Slice each table, then join them on their shared schema keys into one DataFrame. Takes two or more tables, which may be `Fixed` or `ColumnSelection`. A `Merge` can't contain another `Merge`. A data column present in two tables is an error.

- `name` (required) identifies the Merge: it is its node on the GUI canvas and part of the consuming function's identity.
- `how` decides what happens to a location present on only one side: `"exact"` (default) raises an error, `"inner"` drops it, `"outer"` keeps every location with missing values as NaN, `"left"` / `"right"` keep every location of the first / last table. Dropped and NaN-padded rows are logged as warnings. Changing `how` makes a new variant.

/// tab | Python
```python
inputs={"both": Merge(gait_df, Fixed(force_df, session="BL"), name="gait_force", how="left")}
```
///

/// tab | MATLAB
```matlab
struct('both', scifor.Merge(gait_tbl, scifor.Fixed(force_tbl, session="BL"), name="gait_force", how="left"))
```
///

### ColumnSelection

```python
ColumnSelection(data, columns=None, iterate=False, excl_columns=None)
```

| Argument | Default | Meaning |
|---|---|---|
| `columns` | `None` | Column names. `None` or `[]` means every data column (every column that is not a schema key). With `iterate=True`, also **groups**: a list holding sublists (`[["a", "a_date"], "b"]`, matched to `outputs` by position) or a dict (`{"A": ["a", "a_date"]}`, naming one output's columns). |
| `iterate` | `False` | `False`: pass the columns as one argument. `True`: call `fn` once per column (or group) and reassemble the results into one row. All iterating inputs in one call must have the same columns. |
| `excl_columns` | `None` | Columns to leave out, typically after selecting every column. Not allowed with groups. Python only. |

`fn` receives one column as its value when the combination has one row, and as a numpy array otherwise (`column_value`). Several columns, or a group, arrive as a DataFrame. A group with no data at a location is not called, so its output gets no record there. See [Columns that belong together](../guide/for_each.md#columns-that-belong-together).

/// tab | Python
```python
ColumnSelection(df, ["speed"])
ColumnSelection(df, iterate=True, excl_columns=["notes"])
ColumnSelection(df, {"A": ["a", "a_date"], "B": ["b", "b_date"]}, iterate=True)
```
///

/// tab | MATLAB
```matlab
scifor.ColumnSelection(tbl)                          % every data column
scifor.ColumnSelection(tbl, ["speed" "cadence"])
scifor.ColumnSelection(tbl, [], true)                % one call per column
scifor.ColumnSelection(tbl, {["a" "a_date"], ["b" "b_date"]}, true)
```
///

### ColName

```python
ColName(data=None)
```

A constant that becomes a column *name*. `ColName(df)` is the single data column of `df`; it raises if `df` has none, or more than one. `ColName()` (or the bare class `ColName`) is the column of the current per-column run, so it needs an input with `iterate=True`.

/// tab | Python
```python
inputs={"x": ColumnSelection(df, iterate=True), "label": ColName()}
```
///

/// tab | MATLAB
```matlab
struct('x', scifor.ColumnSelection(tbl, [], true), 'label', scifor.ColName())
```
///

### EachOf

```python
EachOf(*alternatives)
```

Each alternative is a separate run, and several `EachOf`s multiply. Use it for a data input, a constant, or `where=`. `EachOf` must be the outermost wrapper. A single alternative behaves like the plain value. One with no alternatives raises when `for_each` runs (`require_alternatives`), so an empty axis never "succeeds" by running zero times.

/// tab | Python
```python
inputs={"x": df, "cutoff_hz": EachOf(10, 20, 30)}
for_each(fn, inputs, where=EachOf(Col("side") == "L", Col("side") == "R", None))
```
///

/// tab | MATLAB
```matlab
struct('x', tbl, 'cutoff_hz', scifor.EachOf(10, 20, 30))
```
///

### PathInput

```python
PathInput(path_template, *, name, root_folder=None, aliases=None, key_regex=None, regex=False)
```

Resolves the path of an existing file from the current combination, and `fn` receives that path. Everything after the template is a keyword argument, written the same way in Python, MATLAB and TOML.

| Argument | Default | Meaning |
|---|---|---|
| `path_template` | required | Path with `{key}` placeholders, e.g. `"{subject}/{session}_emg.csv"`. A key may appear more than once. |
| `name` | required | The PathInput's identity: the only part that reaches record and run ids. Must be non-empty, with no leading or trailing spaces. Moving the files or changing any other argument re-runs nothing. |
| `root_folder` | `None` | Folder a relative template resolves against. `None`: the `for_each(project_root=)` folder, else the project root (`set_project_root`), else the current directory. |
| `aliases` | `None` | `{key: {canonical: [spelling, ...]}}`: several on-disk spellings of one schema-key value. See below. |
| `key_regex` | `None` | `{key: pattern}`: a regex fragment (no capturing group) that a placeholder must match while discovering files. See below. |
| `regex` | `False` | Treat the resolved **last** path segment as a regular expression matched against the file names in its folder. Exactly one file must match. |

/// tab | Python
```python
PathInput("{subject}/trial_{trial}.mat", name="raw", root_folder="/data")

PathInput(
    "{session}/Xsens/{subject}_{session}.xlsx",       # Baseline/Xsens/S01_BL.xlsx
    name="xsens",
    aliases={"session": {"BL": ["Baseline"], "FU": ["FollowUp", "Follow-up"]}},
)

PathInput("{subject}_{speed}{trial}.csv", name="walks",
          key_regex={"speed": r"[A-Za-z]+", "trial": r"\d+"})
```
///

/// tab | MATLAB
```matlab
scifor.PathInput("{subject}/trial_{trial}.mat", name="raw", root_folder="/data")

scifor.PathInput("{session}/Xsens/{subject}_{session}.xlsx", name="xsens", ...
    aliases=struct('session', struct('BL', "Baseline", 'FU', ["FollowUp" "Follow-up"])))
% A canonical that is not a valid field name ("01") needs JSON text:
%   aliases='{"session": {"01": ["Baseline"]}}'

scifor.PathInput("{subject}_{speed}{trial}.csv", name="walks", ...
    key_regex=struct('speed', '[A-Za-z]+', 'trial', '\d+'))
```

`scidb.PathInput` is the same class.
///

/// tab | TOML
```toml
# src/scistack_entities.toml. The key is the name.
[path_inputs]
RAW   = "{subject}/trial_{trial}.mat"
XSENS = { template = "{session}/Xsens/{subject}_{session}.xlsx", root_folder = "/data", aliases = { session = { BL = ["Baseline"], "01" = "Pre" } } }
WALKS = { template = "{subject}_{speed}{trial}.csv", key_regex = { speed = '[A-Za-z]+', trial = '\d+' } }
```

A single spelling may be a bare string. Single-quote regex patterns so TOML keeps the backslashes. An array of these tables declares alternate templates for one name (an `EachOf`). `regex` can't be written in TOML; declare that PathInput in Python.
///

**`aliases`.** The canonical value (`"BL"`) is what's stored as the schema key, and it matches as itself without being listed.

- Every key in `aliases` must be a placeholder in the template, and no spelling may belong to two canonicals. Otherwise construction raises `ValueError`.
- Aliases are used **only to find files**. They never change the paths you write or the identity of a record, so editing them is always safe.
- Each occurrence of a placeholder is matched on its own, so a `Baseline/` folder holding `*_BL.xlsx` files is one session, `BL`. Two different canonicals in one path (`Baseline/.../S01_FU.xlsx`) don't match.
- If a value can be matched as a number (zero-padding: `1` finds `01`), that rule applies before aliases.
- If two spellings both exist on disk for one location (`Baseline/` and `1. Baseline/`), loading raises "matched N files".

**`key_regex`.** By default each placeholder takes as many characters as it can, so `"{speed}{trial}"` splits `SSV10` as `SSV1` + `0`. `key_regex={"speed": r"[A-Za-z]+", "trial": r"\d+"}` gives `SSV` + `10`. A file that doesn't fit the pattern is skipped rather than split wrongly. When the same key also has aliases, a file matches if it fits the pattern **or** one of the alias spellings, and the match is then mapped to its canonical value.

**Helpers.** `.discover()` lists every combination the files on disk provide. The project root: `set_project_root(path)`, `get_project_root()`, `project_root()`, `clear_project_root()`. Directory listings are cached; `clear_listing_cache()` resets the cache after files change.

### PathOutput

```python
PathOutput(template)
```

Resolves the path of a file for `fn` to write. `{key}` takes the current combination's value. `{ColName}` takes the current column in a per-column run, so it needs an input with `iterate=True`. A `str` template gives a `str`, a `pathlib.Path` gives a `Path`.

/// tab | Python
```python
inputs={"x": df, "out": PathOutput("figures/{subject}_{session}.png")}
```
///

/// tab | MATLAB
```matlab
struct('x', tbl, 'out', scifor.PathOutput("figures/{subject}_{session}.png"))
```
///

## Filters

Build row filters for `where=` from `Col`. They combine with `&`, `|` and `~`.

```python
where = (Col("side") == "R") & (Col("speed") > 1.2)
```

`Col(name)` supports `==`, `!=`, `<`, `<=`, `>`, `>=` and produces a `ColFilter`. Combinations produce `CompoundFilter` and `NotFilter`.

## Location selection

`where=` filters rows *within* a combination. `locations=` chooses *which* combinations run.

```python
LocationFilter.build(include=(), exclude_levels=None)
LocationFilter.of(value)        # None | LocationFilter | {"include": ..., "exclude_levels": ...} | JSON text
```

```python
from scifor import LocationFilter      # also scidb.LocationFilter

locations = LocationFilter.build(
    include=[[("subject", "01")], [("subject", "02"), ("trial", "3")]],
    exclude_levels={"session": ["BL"]},
)
```

- `include` lists location prefixes, outermost key first. Use it to select uneven sets, such as all of subject 01 plus trial 3 of subject 02. Empty means everything.
- `exclude_levels` is a standing rule. `{"session": ["BL"]}` drops BL everywhere, including in data added later.

`for_each(locations=)` accepts anything `LocationFilter.of` does; MATLAB takes the JSON text. `filter_combos(combos, locations)` applies a filter to a list of combination dicts.

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
