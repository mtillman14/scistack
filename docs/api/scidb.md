# scidb

`scidb` is the main user-facing package. It stores typed **variables** in a single DuckDB file, addressed by schema keys (subject, session, ...). Its `for_each` loads inputs from the database, runs your function, and saves the outputs. Each saved record keeps its provenance: the function that produced it, the input records it read, and the constants it was given.

```python
from scidb import configure_database, BaseVariable, for_each, Fixed, Parameter
```

`scidb` re-exports the `scifor` wrappers (`Fixed`, `EachOf`, `ColName`, `PathInput`, `PathOutput`, `Col`, `LocationFilter`, `set_schema`, `get_schema`), so one import covers both layers. See [scifor](scifor.md) for those wrappers.

## Setup

| Name | Purpose |
|---|---|
| `configure_database(path, schema_keys, schema_key_types=None)` | Open (or create) the database and register every known variable type. Returns the `DatabaseManager`. Also starts `scistack.log` (the journal) and `scistack-debug.log` next to the file. |
| `get_database()` | The current `DatabaseManager`. Raises `DatabaseNotConfiguredError` if none is configured. |

```python
db = configure_database("experiment.duckdb", ["subject", "session", "trial"],
                        schema_key_types={"trial": "numeric"})
```

`schema_keys` are the location of a value, outermost first. Any other keyword you save with is treated as a *version* parameter, which tells apart different computations at the same location. `schema_key_types` marks a key as `"numeric"`, so that `"001"`, `1` and `1.0` are the same trial. Undeclared keys are compared as strings, so `"01"` and `"1"` are different values.

**Branched schemas.** Real datasets branch (`brand > category > item` beside `brand > location > person`). Declare one list per branch: `configure_database(path, [["brand", "category", "item"], ["brand", "location", "person"]])`. A plain list is one branch. The database stores the tree, and reopening it with a different tree raises `SchemaKeysMismatchError` naming the keys whose parent differs. The tree is only a default (it picks which level checkboxes to offer and where `distribute` goes): a record may name any set of keys, including keys from both branches. `db.schema_tree` is the `scifor.SchemaTree`. See `docs/claude/branched-schema-keys.md`.

A dataset that already holds data can still **grow**: `scidb.store.schema_keys_change.extend_schema_keys(db, paths)` adds a branch or a deeper level (existing records name no such key, so nothing they mean changes), and `change_schema_keys` can rename, re-parent or delete keys that no record or run uses (`scidb.store.emptiness.key_usage` says which). Both close the database; reopen it with the new keys.

Every function below that touches the database also accepts `db=` to target a database other than the current one.

## Variables

Subclass `BaseVariable` once per kind of data. The class name becomes the table name.

```python
class StepLength(BaseVariable):
    pass

StepLength.save(0.72, subject="S01", session="BL", trial=1)
var = StepLength.load(subject="S01", session="BL", trial=1)
var.data, var.record_id, var.metadata
```

| Method | Purpose |
|---|---|
| `save(data, **meta)` → `record_id` | Save a value at a location. A DataFrame with schema-key columns saves one record per row and returns a list of ids. Saving identical content again is detected and not duplicated. |
| `save_from_dataframe(df, data_column, metadata_columns, **common)` | Save each row of a DataFrame as its own record, using the columns you name. |
| `load(*, version="latest", where=None, as_df=False, **meta)` | Load matching records. Returns one `BaseVariable` when one record matches, a list when several match, or a DataFrame when `as_df=True`. List values in `**meta` mean "any of". `version="all"` returns every stored version; a `record_id` returns that exact record. |
| `list_versions(**meta)` | List every version saved at a location (record id, version parameters, time). |
| `head(n=5, **meta)` | Show the first few records as a DataFrame. |
| `to_csv(filename, **meta)` | Export to a flat CSV, one row per location. Also works on `MyVar["col"]` and `Merge(A, B)`. |

**Version parameters vs. variants.** When the same function runs with different settings, for example `low_hz=10` and `low_hz=20`, both results are kept side by side as **variants** of one variable. `load()` then needs to know which one you want. Pass the setting (`low_hz=20`), or use `branch_param("bandpass", low_hz=20)` when two steps share a parameter name. If the request is ambiguous, `load()` raises `AmbiguousVersionError` or `AmbiguousParamError`.

**Column access.** For table-valued variables:

- `MyVar["col"]` / `MyVar[["a", "b"]]` passes only those columns to a function.
- `MyVar.for_columns([...])` runs the function once per column and reassembles the results into one table.
- `MyVar["col"] == 0` builds a filter (see [Filters](#filters)).

The columns of a variable are fixed by its first save. A later save with a different set of columns raises `ColumnSetChangedError`, and the message explains how to fix it.

## Batch processing

/// tab | Python
```python
for_each(
    fn,                        # the function to run once per combination
    inputs,                    # {arg_name: VariableClass | wrapper | PathInput | constant}
    outputs,                   # [OutputClass, ...], one per return value
    dry_run=False,             # print what would load/run/save, do nothing
    save=True,                 # False: record nothing (a plot_/stat_ endpoint makes a draft)
    as_table=None,             # True | [input names]: pass those inputs as full tables
    db=None,                   # a DatabaseManager other than the current one
    distribute=False,          # save each piece of a return one schema level deeper
    where=None,                # filter on which records load (see Filters)
    introspect=False,          # add _record_id_*, _branch_params_*, _call_id columns to the result
    skip_computed=False,       # skip combinations whose outputs and history are unchanged
    schema_filter=None,        # {key: [values]}; not with **metadata_iterables
    schema_keys=None,          # keys to iterate over every DB value; [] = all keys; None = one call
    schema_key_types=None,     # {key: "numeric" | "string"}, recorded in the database
    on_error="continue",       # "continue" | "stop"
    locations=None,            # LocationFilter | {"include": ..., "exclude_levels": ...} | JSON text
    share_limits=None,         # {input_name: [keys held fixed]} -> fn gets `<input>_limits=(min, max)`
    project_root=None,         # folder a rootless PathInput resolves against
    glue=None,                 # {input_name: glue_fn | GlueSpec | [GlueSpec, ...]}
    parameter_names=None,      # {arg_name: declared Parameter name}, for history only
    step=None,                 # a step name recorded on the run (not identity)
    pipeline=<unset>,          # omit: register if a pipeline is active; None: run now; a Pipeline
    hooks=None,                # scifor.LoopHooks(progress_fn=..., cancel_check=...)
    **metadata_iterables,      # subject=[1, 2], session=[] ([] = every value in the database)
) -> pd.DataFrame | Step | None    # Step when registered into a pipeline; None on dry_run
```

`step` and `pipeline` are reserved and cannot be schema keys.
///

/// tab | MATLAB
```matlab
result = scidb.for_each(@fn, inputs, outputs, Name=Value, ...)
result = scidb.for_each(@fn, inputs, outputs={Out1(), Out2()}, Name=Value, ...)

% inputs:  struct, field order = argument order
% outputs: cell array of BaseVariable instances
% Name=Value: dry_run, save, distribute, db, where, as_table, introspect,
%   skip_computed, on_error, schema_keys (string array), schema_filter (struct),
%   glue (struct of glue_ function handles), step, parameter_names (struct),
%   locations (JSON text), project_root, share_limits (struct),
%   schema_key_types (struct), pipeline, categorical; any other name = a metadata iterable,
%   e.g. subject=[1 2 3], session=["A" "B"]
```
///

`for_each` adds the database to [`scifor.for_each`](scifor.md#for_each). Inputs given as variable classes are loaded per combination. Each return value is saved as the matching class in `outputs`, and provenance is recorded for every saved record.

The returned table names each output's column after its class, or after its `column_name` class attribute if you set one (a `Constant` property in MATLAB). The record is still saved as the class itself. Every `scifor.for_each` option is accepted here too, so a `scifor` call becomes a `scidb` call by swapping DataFrames for variable classes. In MATLAB, `outputs` may also be passed as `outputs={...}`, and `categorical=true` converts the returned table's metadata columns after saving.

```python
class StepLength(BaseVariable):
    column_name = "step_len"     # result column; still saved as StepLength
```

/// tab | Python
```python
for_each(
    bandpass,
    inputs={"signal": RawEMG, "low_hz": Parameter(10, 20)},
    outputs=[FilteredEMG],
    subject=[], session=[],          # [] = every value in the database
)
```
///

/// tab | MATLAB
```matlab
scidb.for_each(@bandpass, ...
    struct('signal', RawEMG(), 'low_hz', scidb.Parameter(10, 20)), ...
    {FilteredEMG()}, subject=[], session=[]);
```
///

| Option | Use it to |
|---|---|
| `skip_computed=True` | Skip combinations whose outputs exist and whose entire upstream history (code, inputs, constants) is unchanged. This is how you re-run a pipeline cheaply. |
| `schema_keys=[...]` | Iterate those keys over every value in the database. `[]` iterates every key; `None` (the default) runs once over the whole dataset, e.g. for a group statistic. |
| `schema_filter={key: [...]}` | Restrict values without spelling every iterable. For a key you are not iterating, it limits which records are loaded. |
| `locations=` | Run only selected locations ([`LocationFilter`](scifor.md#location-selection)). |
| `where=` | Load only records that pass a [filter](#filters). |
| `glue={param: fn}` | Reshape one loaded input in memory before `fn` sees it. Nothing is saved; see [Glue](#glue). |

### Input wrappers

`scidb.for_each` takes every [scifor wrapper](scifor.md#input-wrappers), with variable classes in place of DataFrames. The three below exist only in scidb: they decide **which stored records** load. All of them nest with `Fixed`, `Merge` and column selection.

| Wrapper | Use it to |
|---|---|
| [`Fixed`](scifor.md#fixed), [`Merge`](scifor.md#merge), [`EachOf`](scifor.md#eachof), [`ColName`](scifor.md#colname), [`PathInput`](scifor.md#pathinput), [`PathOutput`](scifor.md#pathoutput) | As in scifor. `Merge` here also has `.to_csv(path, **meta)`. |
| [Column selection](#column-selection) | `MyVar["c"]`, `MyVar.for_columns(...)`: the scidb spelling of `ColumnSelection`. |
| [`Parameter`](#parameter) | A named constant with one or more values. |
| [`Variant`](#variant) | Load one specific variant of an input. |
| [`AcrossVariants`](#acrossvariants) | Pool every variant of an input into one table. |

/// tab | Python
```python
inputs={
    "baseline": Fixed(StepLength, session="BL"),
    "both":     Merge(Kinematics, Fixed(Force, session="BL"), name="kin_force"),
    "speed":    GaitData["speed"],
    "cutoff":   EachOf(10, 20),
    "raw":      PathInput("{subject}/raw.csv", name="raw"),
}
```
///

/// tab | MATLAB
```matlab
struct( ...
    'baseline', scidb.Fixed(StepLength(), session="BL"), ...
    'both',     scidb.Merge(Kinematics(), scidb.Fixed(Force(), session="BL"), name="kin_force"), ...
    'speed',    GaitData("speed"), ...
    'cutoff',   scifor.EachOf(10, 20), ...
    'raw',      scidb.PathInput("{subject}/raw.csv", name="raw"))
```
///

#### Column selection

```python
MyVar["col"]                    # one column
MyVar[["a", "b"]]               # several, as one DataFrame
MyVar.for_columns(columns=None) # one call per column; None or [] = every data column
MyVar.for_columns({"A": ["a", "a_date"]})        # column groups
ColumnSelection(MyVar, columns=None, iterate=False, excl_columns=None)   # the long form
```

The arguments are those of [scifor's `ColumnSelection`](scifor.md#columnselection). `MyVar["col"] == 0` builds a [filter](#filters). In MATLAB: `MyVar("col")`, `MyVar(["a" "b"])`, `MyVar().for_columns()`, `MyVar().for_columns({["a" "a_date"], ["b" "b_date"]})`.

#### Parameter

```python
Parameter(*values, description="")
```

Each value is a separate run (a `Parameter` is an `EachOf`). Declare it once, so the GUI and the history show it by name. A Parameter may be declared with no values yet, but running with none raises. `description` is shown in the GUI.

/// tab | Python
```python
WINDOW = Parameter(10, 20, description="window, s")
```
///

/// tab | MATLAB
```matlab
window = scidb.Parameter(10, 20, description="window, s");
```
///

/// tab | TOML
```toml
[parameters]
WINDOW = [10, 20]
```
///

#### Variant

```python
Variant(var_type, *, fn=None, code_version=None, run_options=None, **branch_params)
```

| Argument | Meaning |
|---|---|
| `var_type` | A variable class, a column selection, a `Fixed`, or another `Variant`. Not a `Merge` (pin each constituent instead: `Merge(Variant(A, low_hz=20), B, name="ab")`), and not an `EachOf` (`EachOf` stays outermost). |
| `**branch_params` | The constants to pin, e.g. `low_hz=20`. A bare name matches whichever upstream step has a constant of that name. |
| `fn` | The producing function, for when two upstream steps share a constant name: `fn="bandpass", low_hz=20` pins `bandpass.low_hz`. It applies only to this level of a nested `Variant`. |
| `code_version` | Pin the code that produced the record: `"v1"`, `"v2"`, ... (per function) or `"latest"`. A named version drops locations that never ran it; `"latest"` is decided per location and drops none. |
| `run_options` | Pin the `for_each` flags the producer ran with, e.g. `"distribute=true"` or `"distribute=false, as_table=[df]"`, or `"latest"`. |

At least one pin is required. A nested `Variant` with a conflicting value for the same key raises.

/// tab | Python
```python
Variant(Filtered, low_hz=20)
Variant(Filtered, fn="bandpass", low_hz=20)
Variant(Filtered, code_version="v1")
EachOf(Variant(Filtered, low_hz=10), Variant(Filtered, low_hz=20))
```
///

/// tab | MATLAB
```matlab
scidb.Variant(Filtered(), low_hz=20)
scidb.Variant(Filtered(), fn="bandpass", low_hz=20)
scidb.Variant(Filtered(), code_version="v1")
scifor.EachOf(scidb.Variant(Filtered(), low_hz=10), scidb.Variant(Filtered(), low_hz=20))
```
///

To pick a variant in `load()` instead, pass the constant (`Filtered.load(subject="S01", low_hz=20)`), or `**branch_param("bandpass", low_hz=20)` when two steps share the name.

#### AcrossVariants

```python
AcrossVariants(var_type)
```

Pools every variant into one table, with each upstream constant as a column (e.g. `bandpass.low_hz`), for multiverse or robustness analysis. Without it, a run that aggregates is split per variant automatically. `var_type` is a variable class, a `Fixed` or a `Variant`; a column selection, a `Merge` or an `EachOf` is an error. In MATLAB: `scidb.AcrossVariants(Filtered())`.

### Function roles and helpers

The prefix of a function's name selects its role. `function_role(name)` returns it, and `FUNCTION_ROLES` lists the roles.

- `plot_...` functions draw a figure to a `PathOutput`.
- `stat_...` functions produce a statistics result and receive their inputs as tables by default.
- `glue_...` functions are reshaping steps.
- Any other name is an ordinary processing step.

`stamp_artifact(path, blob)` and `read_artifact_stamp(path)` embed provenance in a saved figure (PNG, SVG or PDF) and read it back.

**`@scistack`** marks a function as a pipeline step without changing it. `@scistack(generates_file="{subject}/out.csv")` declares a function whose output is a file it writes.

**`save(VarClass, data, **meta)`** is the module-level form of `VarClass.save`.

## Pipelines

A pipeline collects `for_each` calls without running them, then runs them in dependency order.

```python
with db.pipeline("emg") as pipe:    # inside the block, for_each registers
    for_each(bandpass, ..., outputs=[Filtered])
    for_each(rms, inputs={"x": Filtered}, outputs=[RMS])
# after the block, for_each runs immediately again (also after an error)

pipe.for_each(zscore, inputs={"x": RMS}, outputs=[RMSz])   # or register explicitly

pipe.run_until(RMS)       # RMS and whatever it needs
pipe.run_all()            # every step (skip_computed=True by default)
```

| Name | Purpose |
|---|---|
| `db.pipeline(name, uses=())` / `Pipeline(name, db=)` | Create a pipeline. It registers nothing by itself: use `with pipe:` or `pipe.for_each(...)`. |
| `with pipe:` | Inside the block, `for_each` calls register into `pipe`; leaving it (also on an error) ends that. Blocks nest. |
| `for_each(fn, inputs, outputs, **kw)` | Register one call into this pipeline, no block needed. |
| `plan(target=None)` / `execution_order()` | Show what would run, and in what order. |
| `run_all()` / `run_until(target)` / `run_endpoints()` | Run every step, a target plus its ancestors, or every `plot_`/`stat_` endpoint with its ancestors. |
| `show(target)` | Draft-run one endpoint to look at it: nothing is recorded; returns figure paths or stat results. |
| `use(other)` / `bind(key_map=, params=, iterate=)` → `PipelineBinding` | Compose pipelines. `bind` adapts another pipeline's schema keys or parameters without modifying it. |
| `last_run_report` | Per-step completed, failed and no-data counts from the most recent run. |
| `active_pipeline()` | The pipeline currently receiving registrations. A registered call returns a `Step` handle, not data. |

Pass `pipeline=None` to `for_each` to run immediately even while a pipeline is active.

## Filters

Filters select records for `where=` and for `load(where=...)`. Combine them with `&`, `|` and `~`, and store them in variables to reuse.

```python
clean = GaitCycle["StepLength"] != 0
for_each(..., where=clean & (Side == "L"))
```

| Form | Matches |
|---|---|
| `MyVar["col"] > 1.2` | A column value of another variable at the same location. |
| `Side == "L"` | The value of a scalar variable at the same location. |
| `schema_key("session").isin(["BL", "POST"])` | Schema-key values. |
| `raw_sql('"Side" = \'L\'')` | A raw SQL condition on the variable's table. Use when nothing above can express the condition. |

## Exclusions

Some data should be left out of *every* analysis, for example a failed recording session. Mark it once and every `for_each` skips it:

```python
exclude_schema(reason="sensor fault", subject="S03", session="BL")
include_schema(reason="re-checked; valid", subject="S03", session="BL")
list_exclusions()          # DataFrame of what is currently excluded
```

Omitted keys act as wildcards. History is kept: re-including a location adds a new row and does not delete the old one. Changing exclusions marks the affected cached results as out of date.

## Provenance and run state

| Name | Purpose |
|---|---|
| `check_node_state(fn, outputs, inputs)` | Whether a step is up to date, out of date or never run, across every combination. |
| `check_combo_state(fn, outputs, schema_combo)` | The same check for one location. |
| `check_pathinput_node_state(...)`, `check_multiple_nodes_state(nodes)` | The same for file-loading steps; a batched form for many steps. |
| `db.get_provenance(...)`, `provenance_query.upstream_provenance(db, record_id)` | What produced a record, one step back or the whole chain. |
| `db.list_variables()`, `db.distinct_schema_values(key)` | What the database holds. |
| `db.exclude_variant(...)` / `db.include_variant(...)` | Hide or restore one variant of a variable. |

For deeper inspection, `scidb.inspect.Inspector.open(path)` is a read-only API. It is the same one behind the CLI below. Its methods include `overview()`, `variables()`, `variants(name)`, `trace(...)`, `provenance(...)`, `runs()`, `locations(...)`, `records(...)` and `sql(query)`.

## Data inventory

Find schema combinations that have no data:

```python
from scidb.provenance.inventory import schema_inventory, write_inventory_csv

inv = schema_inventory(db, ["subject", "session"])
inv.counts      # {"present": 10, "missing": 2, "excluded": 0}
write_inventory_csv(inv, "missing.csv", missing_only=True)
```

Pass `within=["group"]` to check each study arm separately, so a subject is not reported missing at sessions that belong only to another arm.

## Project configuration

| Module / file | What it holds |
|---|---|
| `scidb.project.entities` (`src/scistack_entities.toml`) | Declared variables, `[parameters]` and `[path_inputs]`, shared by Python, MATLAB and the GUI. `entities.WINDOW` resolves a declared name. |
| `scifor.project.schema_order` (`[schema_keys]` in `scistack.toml`) | The display order of each key's levels, e.g. `BL, POST, FU`. |
| `scifor.project.aliases` (`[aliases]`) | Display names for keys, levels and variables. |
| `scifor.project.colors` (`[colors]`) | Fixed colours per level in plots. |
| `scan_project(root)`, `scan_package(name)`, `discover_module(mod)` | Find the variables, parameters and functions a project defines without running its pipelines. |

## Schema keys of an empty dataset

`scidb.is_empty(db=None)` is true when no variable holds a saved record (run history and declarations are not data). While it is true, the schema keys can still change: `scidb.store.schema_keys_change.change_schema_keys(db, new_keys, renames={old: new})` rebuilds the key-shaped tables in one transaction, keeps exclusions, expectation marks and key types (a new key reads NULL = every value), then **closes** the database; reopen it with `configure_database(path, new_keys)`. It raises `SchemaKeysChangeRefused` when a key a record or run names would be renamed, moved or deleted (a dataset that holds data can only grow, and change keys nothing uses), for invalid keys, or when an exclusion names a key that would be deleted. The GUI offers it in the duplicate-location popup (details: `docs/claude/changing-schema-keys.md`).

## Glue

`GlueSpec` describes a short reshaping function applied to one input between steps, for example renaming columns or pivoting a table. A glue function may change the columns but not the rows, and its result is never saved. Only a provenance node is recorded, so editing a glue function still marks downstream results as out of date. Violations raise `GlueRowsChangedError`, `GlueSchemaKeysAlteredError` and the other `GlueError` subclasses.

```python
GlueSpec(name, fn=None, per_schema_key=False, pipe_param=None, extra_inputs={})
for_each(fn, inputs, outputs, glue={"x": glue_tidy})        # a function alone is enough
```

| Argument | Meaning |
|---|---|
| `name`, `fn` | The glue step's name and its function. Passing the function alone fills both. |
| `per_schema_key` | `False`: run once on the whole loaded table. `True`: run on each combination's slice, just before `fn`. |
| `pipe_param` | Which of the glue function's arguments receives the input being reshaped. Default: its first. |
| `extra_inputs` | `{glue_arg: value}` for the glue function's other arguments. |

`language`, `source_file` and `source_text` are set by the GUI and the MATLAB bridge. In MATLAB, `glue=` takes a struct of named `glue_` function handles.

## Command line

Installing `scidb` adds a `scidb` command for inspecting a database. Most commands only read; the ones that write are marked below. Add `--json` to any command for machine-readable output, and `--db PATH` to choose the database (otherwise it is discovered from the current directory).

| Command | Shows / does |
|---|---|
| `status`, `vars [NAME]`, `schema` | Overview, variable types, schema hierarchy. |
| `pipeline`, `state`, `runs` | The step graph, which steps are up to date, the run log. |
| `variants NAME`, `trace`, `show TYPE ...`, `pick TYPE ...` | Variants side by side, the full upstream history of a record, the records at a location, one record id. |
| `locations TYPE`, `inventory`, `exclusions` | Status per location, missing combinations, active exclusions. |
| `sql "SELECT ..."` | Read-only SQL. |
| `report` | Collect recorded `plot_`/`stat_` results into an HTML report folder. |
| `verify --against BUNDLE` | Compare a re-run with an exported history (runs no code). |
| `exclude`, `include`, `pin`, `unpin`, `delete-variant`, `clear-data` | **Write.** Exclusions, a variable's default variant, permanent variant deletion, and `clear-data`: delete every record and its run history while keeping the pipeline, the run log and every declaration (`--backup` copies the database first; refused for a GUI project for now). Both deletes print a plan and stop unless you pass `--yes`. |

## Logging

SciStack writes two files beside the database. `scistack.log` is the journal: one entry per run, saying what ran, what failed, and whether the failure is in your code, your function's values, your setup, or SciStack itself (see [Reading your log](../guide/reading-your-log.md)). `scistack-debug.log` records everything, at full detail, for debugging SciStack.

`Log.set_level("DEBUG")` turns on full detail on the console too. `Log.set_level("INFO", sink="file")` makes the debug file quieter. The `SCIDB_LOG_LEVEL` environment variable and `scidb -v` set both.

## Exceptions

All errors derive from `SciStackError`. The ones you are most likely to see:

| Exception | Raised when |
|---|---|
| `NotFoundError` | `load()` matched nothing. |
| `AmbiguousVersionError` / `AmbiguousParamError` | Several variants match; pin one. |
| `DatabaseNotConfiguredError` | No `configure_database()` call yet. |
| `DatabaseLockedError` | Another process holds the database file. |
| `NotRegisteredError` | A variable type is unknown to the database. |
| `ReservedMetadataKeyError` | A metadata key collides with a reserved name (`record_id`, `id`, ...). |
| `ColumnSetChangedError` | A table variable was saved with different columns. |
| `OutputSaveError` | A `for_each` output could not be saved. |
| `PipelineCycleError` | A pipeline's steps depend on each other in a loop. |
| `SchemaKeysMismatchError` | `configure_database` got different schema keys than the database has (a `ValueError`; the message shows both lists). |
