# FAQ & Troubleshooting

<!-- Ground truth (tests/source win over prose). Each entry verified against:
     scidb/src/scidb/model/variable.py (load returns single|list|DataFrame; no load_all; reserved keys;
       schema_version), variable.py:271 (BaseVariable.save does NOT record lineage);
     scihist/__init__ + database.py (configure_database wires cache backend; for_each/save);
     scihist/tests/test_cache_hit.py, test_skip_computed.py (cache hits / skip_computed);
     scilineage/core.py (lineage_fcn -> LineageFcnResult.data; unwrap pass-the-variable);
     scidb/tests/test_optional_lineage_dependency.py (scidb works without scilineage);
     scimatlab configure_database.m (2 args); scidb/tests/test_filters.py (& | ~ filters);
     scidb exceptions (DatabaseNotConfiguredError/NotRegisteredError/NotFoundError/...). -->

Common questions and fixes. Most "it didn't work" cases come from a handful of
recurring mismatches, collected here.

## Usage

??? question "Why does my `@lineage_fcn` return an object instead of my value?"
    A `@lineage_fcn` call returns a `LineageFcnResult` that carries provenance.
    The computed value is on **`.data`** — use `result.data`. The result also
    compares equal to the raw value and prints as it, but to use the value
    directly, read `.data`. See [Tracking Lineage](../guide/lineage.md).

??? question "I saved a result but its provenance wasn't recorded. Why?"
    A plain `VariableClass.save(result, ...)` stores the *data* but **not** the
    lineage. To persist a tracked result with its provenance, use
    `scihist.save(VariableClass, result, ...)` — or run the work through
    [`for_each`](../guide/for_each.md), which saves with lineage automatically.

??? question "My pipeline doesn't get cache hits across runs."
    Import `configure_database` from **`scihist`**, not `scidb` — the scihist
    version registers the database as the lineage cache backend. Also pass the
    *loaded variable* (not `.data`) into your functions, so inputs are keyed by a
    stable `record_id`. See [Caching Computations](../guide/caching.md).

??? question "`load()` returned a list when I expected one value (or vice versa)."
    `load()` returns a **single** `BaseVariable` when exactly one record matches, a
    **list** when several match, and a **DataFrame** when `as_df=True`. Narrow the
    metadata to address a single record, or expect a list. There is no separate
    `load_all` method — use `load(..., version="all")` for full history and
    `load(where=...)` to filter. See [Defining Variables](../guide/variables.md).

??? question "Should I pass `var` or `var.data` to my function?"
    Pass the **`BaseVariable` instance** (`var`). The `@lineage_fcn` decorator
    unwraps it to the raw data for you (default `unwrap=True`) while preserving the
    link to its `record_id` for lineage and caching. Passing `var.data` loses that
    link.

??? question "`ReservedMetadataKeyError` — what metadata keys are off-limits?"
    `record_id`, `id`, `created_at`, `schema_version`, `index`, `loc`, and `iloc`
    are reserved and can't be used as addressing metadata. Rename the offending
    key.

??? question "What's the difference between schema keys and other metadata?"
    The keys you pass to `configure_database` are the **dataset schema keys** —
    they identify a record's *location*. Any other metadata on a save (e.g. a
    constant `factor=2.0`) is a **version key** that distinguishes computational
    *variants* at that location (it lands in `branch_params`). See
    [Database & Configuration](../guide/database.md).

??? question "Do I have to install the whole stack?"
    No. Each user-facing layer is usable on its own — `scifor` alone for batch
    iteration, `scidb` for storage, `scihist` for the full pipeline with lineage.
    `scidb` even runs without the lineage engine installed (lineage is optional
    behind a feature flag). See
    [Choosing Your Layer](../getting-started/choosing-a-layer.md).

## Errors

??? failure "`DatabaseNotConfiguredError`"
    You called `get_database()`, `save`, or `load` before `configure_database(...)`.
    Configure the database once at startup.

??? failure "`NotRegisteredError`"
    The variable type was never registered. `configure_database` auto-registers all
    *defined* subclasses; if a class is defined later, call `db.register(MyVar)`
    (or just use it — using it registers it).

??? failure "`NotFoundError`"
    No stored record matches the query. Check the metadata values and that the data
    was actually saved at those coordinates.

??? failure "`AmbiguousVersionError`"
    `load(version="latest")` found multiple variants at one location (e.g. several
    constant variants). Narrow the query — pass the distinguishing version key, or
    a specific `version=<record_id>`.

## MATLAB

??? failure "MATLAB: *\"Conversion to logical from scidb.Filter is not possible\"*"
    You combined filters with `&&` / `||`. MATLAB only allows operator overloading
    for the element-wise `&` / `|`, so write `(A) & (B)` and `(A) | (B)` — always
    parenthesized. See [Filtering & Selection](../guide/filters.md).

??? question "MATLAB: `configure_database` argument count"
    The MATLAB `scidb.configure_database` takes **two** arguments — the database
    path and the schema-key string array:
    `scidb.configure_database("experiment.duckdb", ["subject", "session"])`. There
    is no third argument. See [MATLAB Setup](../matlab-setup.md).

??? question "MATLAB can't find `scidb.BaseVariable`."
    The `+scidb` MATLAB package isn't on your path. Add
    `scimatlab/src/scimatlab/matlab` with `addpath`, and confirm MATLAB's Python
    environment points at the interpreter where `scidb` is installed
    (`py.importlib.import_module('scidb')`). See [MATLAB Setup](../matlab-setup.md).

**Next:** [Quickstart](../quickstart.md) · [Concepts](../concepts/index.md) ·
[Contributing & Dev Setup](contributing.md)
