# for_each() Kwargs Reference

`scidb.for_each()` is the main entry point for running pipeline functions. It lives in `scidb/src/scidb/run/foreach.py`.

## Signature

Abridged. The owner of the full keyword list is
`scifor.options.FOR_EACH_OPTIONS` + `scidb.run.foreach_call.SCIDB_HOOK_OPTIONS`;
`scidb/tests/test_foreach_options.py` holds the signature to it.

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
    # ... every option scifor.options.FOR_EACH_OPTIONS declares ...
    hooks: LoopHooks | None = None,
    **metadata_iterables: list[Any],
) -> pd.DataFrame | None
```

## Kwargs by Category

### Core I/O (already handled by GUI via node connections)
- **fn**: The function to execute.
- **inputs**: Dict mapping parameter names to variable types, Fixed wrappers, Merge wrappers, ColumnSelection wrappers, PathInput, or scalar constants.
- **outputs**: List of output variable types with `.save()`.

### Schema / Metadata Iterables
- **\*\*metadata_iterables**: Keyword arguments where each key is a schema key name (e.g. `subject`, `session`) and each value is a list of values to iterate over. `for_each` builds the Cartesian product of all these lists and runs the function once per combo.
- If a list is empty (`[]`), `for_each` auto-resolves it to all distinct values via `db.distinct_schema_values(key)`.
- The GUI currently passes ALL distinct values for every schema key, meaning every combo runs.

### Run Behavior
- **dry_run** (`bool`, default `False`): If True, prints what would happen (which combos, which inputs) without actually executing the function or saving anything. Returns `None`.
- **save** (`bool`, default `True`): If True, saves each function call's output via `OutputCls.save()`. Set to False to run without persisting (useful for testing/debugging).
- **distribute** (`bool`, default `False`): When True, splits the function's output and saves each piece at the schema level below the deepest iterated key. Used when a function produces data that should be stored at a finer granularity than the iteration level.

### Advanced / Lower Priority
- **as_table** (`list[str] | bool | None`): Controls which inputs are passed as full DataFrames to the function rather than as scalars. When `True`, all inputs are DataFrames. When a list of param names, only those are DataFrames.
- **where**: Optional filter expression passed to `.load()` calls on DB-backed inputs. Filters rows within the loaded data.
- **db**: Optional explicit database instance. Usually resolved automatically from thread-local storage.

### Hooks (callers that drive the loop: the GUI, Pipeline)
- **hooks**: `scifor.LoopHooks(progress_fn=..., cancel_check=...)`, the same object `scifor.for_each` takes. `progress_fn` gets one dict per loop event (the GUI's progress bar, the Pipeline step report); `cancel_check` is asked before each combo. Any other LoopHooks field raises (`foreach_call.check_caller_hooks`): scidb decides those from the database.
- Until 2026-11-10 these were the private keywords `_progress_fn` and `_cancel_check`; a leftover `_name=` now raises `TypeError` (`scifor.options.reject_private_options`) instead of becoming a metadata axis.
- `inject_combo_metadata` (generates_file / endpoint functions) and `pre_combo_hook` (`skip_computed`, the MATLAB bridge) are set inside scidb on `RunContext`; they are no longer keywords.

## How the GUI Currently Calls for_each

In `scistack-gui/scistack_gui/services/run_service.py`, the `_run_in_thread` function:

1. Resolves inputs and outputs from the node graph (edges, registry)
2. Builds `schema_kwargs` as ALL distinct values for every key (lines 307-311)
3. Calls `for_each(fn, inputs=..., outputs=..., hooks=LoopHooks(progress_fn=..., cancel_check=...), ...)` in `_run_target`

The schema filter and run options (dry_run, save, distribute) are not yet exposed to the user.
