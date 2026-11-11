# Plan: `@lineage_fcn` writes to `_record_metadata` (GUI pipeline visibility)

## Goal
Make `@lineage_fcn`-decorated functions appear as nodes in the scistack-gui
pipeline graph, the same way `for_each()` calls do.

## Root cause
The GUI reads `_record_metadata.version_keys` (written only by `for_each`) to
build function/variable/constant nodes.  `@lineage_fcn` writes only to the
SQLite lineage DB (pipelinedb-lib) — invisible to the GUI.

## Design

### Tag-and-build mechanism
When `BaseVariable.save(lineage_result, subject=...)` is called:

1. Detect `LineageFcnResult` input (already done in `save_variable`).
2. Build `version_keys` with `__fn`, `__fn_hash`, `__inputs`, `__constants`
   in the exact format `for_each` uses — so `list_pipeline_variants()` and
   `_build_graph()` need no changes.
3. Tag the result object: `lineage_result._scidb_variable_type = cls.__name__`
   so that when this result is passed as input to a *downstream* function,
   the save of that downstream result can look up the type name.
4. Merge the version_keys into `metadata` before calling `save()` — since
   `_split_metadata` puts non-schema keys into `version`, they land in
   `_record_metadata.version_keys` automatically.

### Proper parameter names in `LineageFcnInvocation`
Currently positional args are stored as `arg_0`, `arg_1`, etc.  The GUI shows
these as edge handles, so proper names matter.

Fix: use `inspect.signature(fcn.fcn).bind(*args, **kwargs)` in
`LineageFcnInvocation.__init__` to get actual parameter names.  Fall back to
`arg_N` only if binding fails (e.g. `*args` functions).

## Files changed

### `scilineage/src/scilineage/core.py`
- `LineageFcnInvocation.__init__`: use `inspect.signature.bind()` to capture
  proper parameter names instead of `arg_N`.

### `scidb/src/scidb/database.py`
- Add module-level helper `_build_lineage_version_keys(result)` that extracts
  `fn_name`, `fn_hash`, `input_types` (from `_scidb_variable_type` tags), and
  `constants` from a `LineageFcnResult`.
- `save_variable()`: when `data` is `LineageFcnResult`:
  - Tag `data._scidb_variable_type = variable_class.__name__`
  - Call `_build_lineage_version_keys(data)` to get version_keys
  - Merge into `metadata` before calling `save()`

## Tests

### `scilineage/tests/test_core.py`
- Test that positional args are bound to proper param names.
- Test fallback to `arg_N` for `*args` functions.

### `scidb/tests/`
- Test that `save_variable()` with a `LineageFcnResult` writes `__fn`,
  `__inputs`, `__constants` to `_record_metadata.version_keys`.
- Test that a two-step chain (A → B via lineage) produces correct `__inputs`
  in B's `_record_metadata`.
- Test that `list_pipeline_variants()` returns both steps.

## Non-changes
- `@lineage_fcn` decorator itself: unchanged.
- GUI (`_build_graph`, `list_pipeline_variants`): unchanged.
- SQLite lineage DB writes: unchanged (still happen in parallel).
