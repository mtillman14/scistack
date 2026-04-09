# for_each() Kwargs Reference

`scidb.for_each()` is the main entry point for running pipeline functions. It lives in `scidb/src/scidb/foreach.py`.

## Signature

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

### Internal (not for GUI)
- **_inject_combo_metadata**: Used by scihist for `generates_file` — injects current combo metadata as extra kwargs to fn.
- **_pre_combo_hook**: Used by scihist to implement `skip_computed` — called per combo, returns True to skip.
- **_progress_fn**: Progress callback for GUI integration — already used by the GUI's run system.

## How the GUI Currently Calls for_each

In `scistack-gui/scistack_gui/api/run.py`, the `_run_in_thread` function:

1. Resolves inputs and outputs from the node graph (edges, registry)
2. Builds `schema_kwargs` as ALL distinct values for every key (lines 307-311)
3. Calls `for_each(fn, inputs=inputs, outputs=[OutputCls], _progress_fn=_progress_fn, **schema_kwargs)` (line 365)

The schema filter and run options (dry_run, save, distribute) are not yet exposed to the user.
