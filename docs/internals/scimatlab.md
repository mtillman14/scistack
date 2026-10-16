# scimatlab

/// info | The MATLAB bridge
In MATLAB you write `scifor.*` and `scidb.*` directly; you never call `scimatlab` by name. Installing `scimatlab` puts those MATLAB packages on your path. For first-time setup, see [MATLAB Setup](../matlab-setup.md).
///

The MATLAB API mirrors the Python one. MATLAB runs the loop and calls your function. Everything else is delegated to the same Python code a Python pipeline uses, through MATLAB's `py.` interface: loading, saving, hashing and provenance. A MATLAB step can therefore consume a Python-produced variable, and the reverse, and both appear in one history.

**Requirements:** MATLAB R2021b+ (for `name=value` syntax), Python 3.10+ with `scidb` and `scimatlab` installed, and `pyenv` pointing at that Python.

## Variables

A variable type is a one-line class file:

```matlab
% StepLength.m
classdef StepLength < scidb.BaseVariable
end
```

Variables declared in the project's entities file need no class file. `scidb.entities()` writes a stub class for each declared name MATLAB can't find and adds the stubs to the path. It never replaces a class you wrote yourself.

| Method | Purpose |
|---|---|
| `StepLength().save(data, subject=1, session="A")` | Save a value; returns the `record_id`. |
| `StepLength().save_from_table(tbl, data_col, meta_cols)` | Save one record per table row. |
| `StepLength().load(subject=1)` | Load the latest match(es). `'version', 'all'` returns every version; `'version', record_id` returns one record. Results have `.data`, `.record_id`, `.metadata`, `.content_hash`, `.branch_params`. |
| `list_versions(...)`, `head(n, ...)`, `to_csv(file, ...)` | Same as Python. |
| `provenance(...)` | What produced a record: `function_name`, `function_hash`, `inputs`, `constants`. |
| `StepLength("col")`, `for_columns(cols)` | Column selection and per-column runs. |
| `StepLength() == value` (and `~=`, `<`, ...) | Build a filter for `where=`. |

## Functions

| Python | MATLAB |
|---|---|
| `configure_database(path, keys, schema_key_types=)` | `scidb.configure_database(path, keys, schema_key_types=struct('trial','numeric'))` |
| `scidb.for_each(fn, inputs, outputs, ...)` | `scidb.for_each(@fn, struct(...), {Out()}, subject=[], ...)` with the same options (`where`, `distribute`, `as_table`, `finalized`, `glue`, `locations`, `dry_run`, ...). |
| `scifor.for_each(fn, inputs, ...)` | `scifor.for_each(@fn, struct(...), subject=[...])`, the database-free loop on MATLAB tables. |
| Wrappers | `scidb.Fixed`, `scidb.Merge`, `scidb.Variant`, `scidb.AcrossVariants`, `scidb.Parameter`, `scidb.ColName`, `scifor.EachOf`, `scifor.PathInput`, `scifor.PathOutput`, `scifor.ColumnSelection` |
| Filters | `scifor.Col("side") == "R"`, `scidb.schema_key("session")`, `scidb.raw_sql(...)`, combined with `&`, `|`, `~` |
| Pipelines | `pipe = scidb.Pipeline("name")`, then `pipe.run_all()`, `pipe.run_until(Target())`, `pipe.run_endpoints()`, `pipe.plan()` |
| Exclusions | `scidb.exclude_schema(reason, 'subject', 1)`, `scidb.include_schema(...)`, `scidb.list_exclusions()` |
| Variable groups | `scidb.add_to_var_group`, `remove_from_var_group`, `list_var_groups`, `get_var_group` |
| Misc. | `scidb.get_database()`, `scidb.close_database()`, `scidb.register_variable(Type(), schema_version=N)`, `scidb.Log.set_level("DEBUG")` |

## Function identity

A MATLAB function's hash is computed from the raw bytes of its `.m` file. Any edit to the file, including a comment change, marks its results out of date. Python functions are hashed from their parsed code instead ([scilineage](scilineage.md)), so a Python function and a MATLAB function never share a hash.

## Python-side API

The `scimatlab` Python package is called by the MATLAB classes. Its functions are not meant to be called from your code: `scimatlab.bridge` (`for_each_prepare` / `for_each_save` run the steps before and after MATLAB's loop), `scimatlab.source_hash`, and `scimatlab.stubs`.
