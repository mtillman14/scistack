# scidb

`scidb` is the main user-facing package. It stores typed **variables** in a single DuckDB file, addressed by schema keys (subject, session, ...). Its `for_each` loads inputs from the database, runs your function, and saves the outputs. Each saved record keeps its provenance: the function that produced it, the input records it read, and the constants it was given.

```python
from scidb import configure_database, BaseVariable, for_each, Fixed, Parameter
```

`scidb` re-exports the `scifor` wrappers (`Fixed`, `EachOf`, `ColName`, `PathInput`, `PathOutput`, `Col`, `set_schema`, `get_schema`), so one import covers both layers. See [scifor](scifor.md) for those wrappers.

## Setup

| Name | Purpose |
|---|---|
| `configure_database(path, schema_keys, schema_key_types=None)` | Open (or create) the database and register every known variable type. Returns the `DatabaseManager`. Also starts `scidb.log` next to the file. |
| `get_database()` | The current `DatabaseManager`. Raises `DatabaseNotConfiguredError` if none is configured. |

```python
db = configure_database("experiment.duckdb", ["subject", "session", "trial"],
                        schema_key_types={"trial": "numeric"})
```

`schema_keys` are the location of a value, outermost first. Any other keyword you save with is treated as a *version* parameter, which tells apart different computations at the same location. `schema_key_types` marks a key as `"numeric"`, so that `"001"`, `1` and `1.0` are the same trial. Undeclared keys are compared as strings, so `"01"` and `"1"` are different values.

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

```python
for_each(fn, inputs, outputs, *, where=None, skip_computed=False,
         distribute=False, as_table=None, dry_run=False, save=True,
         schema_keys=None, schema_filter=None, locations=None,
         finalized=False, glue=None, **metadata_iterables)
```

`for_each` adds the database to [`scifor.for_each`](scifor.md#for_each). Inputs given as variable classes are loaded per combination. Each return value is saved as the matching class in `outputs`, and provenance is recorded for every saved record.

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
| `finalized=` | `plot_` / `stat_` functions only: `False` draws or prints a draft and records nothing; `True` records the result with provenance. |
| `glue={param: fn}` | Reshape one loaded input in memory before `fn` sees it. Nothing is saved; see [Glue](#glue). |
| `track_lineage=False` | Run without recording provenance. |

**Input wrappers specific to scidb** (all nest with `Fixed`, `Merge` and column selection):

| Wrapper | Purpose |
|---|---|
| `Parameter(*values, description="")` | A named constant with one or more values; each value is a separate run. Declare it at module level (`WINDOW = Parameter(10, 20)`) or in the [entities file](#project-configuration) so it appears by name in the GUI and in history. A `Parameter` is an `EachOf`. |
| `Variant(MyVar, **branch_params)` | Load one specific variant of an input, e.g. `Variant(Filtered, low_hz=20)`. |
| `AcrossVariants(MyVar)` | Pool *every* variant into one table, with the settings as columns. For multiverse or robustness analysis. Without it, an aggregating run is split per variant automatically. |
| `Merge(A, B)`, `ColumnSelection` | The `scifor` wrappers, extended with database loading and `to_csv`. Comparing a column selection (`MyVar["c"] == 1`) yields a filter. |

**Function roles.** The prefix of a function's name selects its role. `function_role(name)` returns it, and `FUNCTION_ROLES` lists the roles.

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
pipe = db.pipeline("emg")     # activates: for_each calls now register, not run
for_each(bandpass, ..., outputs=[Filtered])
for_each(rms, inputs={"x": Filtered}, outputs=[RMS])

pipe.run_until(RMS)       # RMS and whatever it needs
pipe.run_all()            # every step (skip_computed=True by default)
```

| Name | Purpose |
|---|---|
| `db.pipeline(name, uses=())` / `Pipeline(name, db=)` | Create a pipeline and start registering `for_each` calls into it. |
| `plan(target=None)` / `execution_order()` | Show what would run, and in what order. |
| `run_all()` / `run_until(target)` / `run_endpoints()` | Run every step, a target plus its ancestors, or every `plot_`/`stat_` endpoint with its ancestors. |
| `show(target)` | Draft-run one endpoint to look at it: nothing is recorded; returns figure paths or stat results. |
| `deactivate()` | Stop registering; later `for_each` calls run immediately again. |
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
| `db.get_provenance(...)`, `db.get_upstream_provenance(record_id)` | What produced a record, one step back or the whole chain. |
| `db.list_variables()`, `db.distinct_schema_values(key)` | What the database holds. |
| `db.exclude_variant(...)` / `db.include_variant(...)` | Hide or restore one variant of a variable. |

For deeper inspection, `scidb.inspect.Inspector.open(path)` is a read-only API. It is the same one behind the CLI below. Its methods include `overview()`, `variables()`, `variants(name)`, `trace(...)`, `provenance(...)`, `runs()`, `locations(...)`, `records(...)` and `sql(query)`.

## Data inventory

Find schema combinations that have no data:

```python
from scidb.inventory import schema_inventory, write_inventory_csv

inv = schema_inventory(db, ["subject", "session"])
inv.counts      # {"present": 10, "missing": 2, "excluded": 0}
write_inventory_csv(inv, "missing.csv", missing_only=True)
```

Pass `within=["group"]` to check each study arm separately, so a subject is not reported missing at sessions that belong only to another arm.

## Project configuration

| Module / file | What it holds |
|---|---|
| `scidb.entities` (`src/scistack_entities.toml`) | Declared variables, `[parameters]` and `[path_inputs]`, shared by Python, MATLAB and the GUI. `entities.WINDOW` resolves a declared name. |
| `scidb.schema_order` (`[schema_keys]` in `scistack.toml`) | The display order of each key's levels, e.g. `BL, POST, FU`. |
| `scidb.aliases` (`[aliases]`) | Display names for keys, levels and variables. |
| `scidb.colors` (`[colors]`) | Fixed colours per level in plots. |
| `scan_project(root)`, `scan_package(name)`, `discover_module(mod)` | Find the variables, parameters and functions a project defines without running its pipelines. |

## Glue

`GlueSpec` describes a short reshaping function applied to one input between steps, for example renaming columns or pivoting a table. A glue function may change the columns but not the rows, and its result is never saved. Only a provenance node is recorded, so editing a glue function still marks downstream results as out of date. Violations raise `GlueRowsChangedError`, `GlueSchemaKeysAlteredError` and the other `GlueError` subclasses.

## Command line

Installing `scidb` adds a `scidb` command for inspecting a database. Most commands only read; the ones that write are marked below. Add `--json` to any command for machine-readable output, and `--db PATH` to choose the database (otherwise it is discovered from the current directory).

| Command | Shows / does |
|---|---|
| `status`, `vars [NAME]`, `schema` | Overview, variable types, schema hierarchy. |
| `pipeline`, `state`, `runs` | The step graph, which steps are up to date, the run log. |
| `variants NAME`, `trace`, `show TYPE ...`, `pick TYPE ...` | Variants side by side, the full upstream history of a record, the records at a location, one record id. |
| `locations TYPE`, `inventory`, `exclusions` | Status per location, missing combinations, active exclusions. |
| `sql "SELECT ..."` | Read-only SQL. |
| `report` | Collect finalized `plot_`/`stat_` results into an HTML report folder. |
| `verify --against BUNDLE` | Compare a re-run with an exported history (runs no code). |
| `exclude`, `include`, `pin`, `unpin`, `delete-variant` | **Write.** Exclusions, a variable's default variant, and permanent variant deletion. `delete-variant` prints a plan and stops unless you pass `--yes`. |

## Logging

`Log.set_level("DEBUG")` turns on full detail on the console and in `scidb.log`. Pass `sink="file"` to raise only the log file. The `SCIDB_LOG_LEVEL` environment variable and `scidb -v` do the same.

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
