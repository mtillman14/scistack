# Plan: Merge `load()` and `load_all()` into a unified `load()`

## Goal

Simplify the `BaseVariable` API in both Python and MATLAB by merging `load_all()` into
`load()`. Both languages share the same `version=` parameter semantics. The only
intentional language difference is the output format parameter (`as_df=` in Python,
`as_table=` in MATLAB) and the fact that MATLAB cannot return generators.

---

## Unified `version=` semantics (both languages)

| `version=` value | Meaning |
|---|---|
| `"latest"` (default) | Latest version per schema/version-key combination |
| `"all"` | Every stored version |
| any other string | Specific record_id — loads exactly that record |

---

## Python changes

### New `BaseVariable.load()` signature

```python
@classmethod
def load(
    cls,
    as_df: bool = False,
    include_record_id: bool = False,
    version: str = "latest",
    where=None,
    db=None,
    **metadata,
) -> Generator | pd.DataFrame:
```

`loc`, `iloc`, `as_table`, and `version_id` are all removed.

### Path behavior

**`as_df=False` (generator, default)**

1. Determine `version_id` from `version`: `"latest"` → `"latest"`, `"all"` → `"all"`,
   anything else → treat as specific record_id (single-record fast path).
2. For specific record_id: yield that one record from `_db.load(...)`, return 1-item generator.
3. For `"latest"` / `"all"`: materialize via `list(_db.load_all(..., version_id=...))` (required
   for the AmbiguousVersionError check; `_db.load_all()` bulk-fetches anyway so no meaningful
   regression).
4. Raise `NotFoundError` if empty.
5. If all results share the same schema key values (differ only by branch_params) → raise
   `AmbiguousVersionError` (same logic as today).
6. Return via private `_load_generator(results)` helper that yields from the list.

**`as_df=True` (DataFrame, fast path)**

`as_df=True` always returns a DataFrame regardless of result count — a single record
produces a 1-row DataFrame, matching MATLAB's `as_table=true` always returning a table.

1. For specific record_id: load single record via `_db.load(...)`, assemble 1-row DataFrame.
2. For `"latest"` / `"all"`: call `_db.load_all_as_df(...)` directly (vectorised bulk path).
3. Raise `NotFoundError` if result is empty.

### Removed from `variable.py`

- `BaseVariable.load_all()` — deleted
- `BaseVariable._results_to_dataframe()` — deleted (was used by old `as_table` path)
- `BaseVariable._load_all_generator()` — replaced by `_load_generator(results)` helper

### Updated in `variable.py`

- `VariableMeta` docstring: remove reference to `load_all()`

### Other Python files

| File | Change |
|---|---|
| `scidb/src/scidb/foreach.py:1525` | `var_type.load_all(version_id="latest", ...)` → `var_type.load(version="latest", ...)` |
| `scidb/src/scidb/filters.py` | Update docstring examples (`load_all(...)` → `load(...)`) |

### Python tests to update

**`scidb/tests/test_integration.py`**

- `TestLoadAsTable` class:
  - Rename `as_table=` → `as_df=` at all call sites
  - `test_load_as_table_single_result`: was `isinstance(result, BaseVariable)` → update to assert 1-item generator
  - `test_load_as_table_false_returns_list`: update to assert generator (not list)

**`scidb/tests/test_branch_params.py`**

- All `list(Filtered.load_all(...))` → `list(Filtered.load(...))`; check each call for `version_id` usage

**`scidb/tests/test_where.py`**

- All `list(StepLength.load_all(where=...))` → `list(StepLength.load(where=...))`
- `load_all(as_df=True, where=...)` → `load(as_df=True, where=...)`

**`scidb/tests/test_load_all_ordering.py`**

- All `list(TestData.load_all(...))` → `list(TestData.load(...))`; check `version_id` usage

---

## MATLAB changes

### New `BaseVariable.load()` behavior

Same `version=` semantics as Python. `as_table=false` is the default — matching
Python's `as_df=False` default. Both languages default to "give me the raw objects";
the formatted output (DataFrame / table) is the explicit opt-in in both.

Note: the current MATLAB `load()` has `as_table=true` as its default. Changing it
to `false` is an additional breaking change, but required for conceptual alignment.

MATLAB cannot return generators, so `as_df=` does not exist. The output format is
controlled by `as_table=`:

| `as_table=` | `n == 0` | `n == 1` | `n > 1` |
|---|---|---|---|
| `false` (default) | error | single `BaseVariable` | `BaseVariable` array |
| `true` | error | 1-row MATLAB table | MATLAB table |

### Routing in `load()` (updated `split_load_args` logic)

```matlab
if version == "latest" || version == "all"
    % Bulk path: use load_and_extract
    bulk = py.sci_matlab.bridge.load_and_extract(
        py_class, py_metadata,
        pyargs('version_id', char(version), 'db', py_db [, 'where', ...]));
    n = int64(bulk{'n'});
    if n == 0
        error('scidb:NotFoundError', 'No %s found matching the given metadata.', type_name);
    end
    results_arr = scidb.BaseVariable.wrap_py_vars_batch(bulk);
    if as_table
        result = multi_result_to_table(results_arr, type_name, categorical_flag);
    elseif n == 1
        result = results_arr(1);
    else
        result = results_arr;  % default: BaseVariable array
    end
else
    % Specific record_id fast path (unchanged)
    py_var = py_db.load(py_class, py_metadata, version=char(version));
    result = scidb.BaseVariable.wrap_py_var(py_var);
end
```

`split_load_args` default changes: `as_table` default changes from `true` to `false`.

### Removed from MATLAB

- `BaseVariable.load_all()` method — deleted
- `+scidb/+internal/split_load_all_args.m` — deleted (was only used by `load_all`)

### Updated

- `split_load_args` (local function in `BaseVariable.m`): recognise `"all"` as a non-record-id value for `version=`
- `BaseVariable.m` header comment: update API reference table

### MATLAB tests to update

**`sci-matlab/tests/matlab/scidb/TestSaveLoad.m`**

- `test_load_all_returns_all_matching`: → `load(version="all", ...)`
- `test_load_all_each_is_thunk_output`: → `load(version="all", ...)`
- `test_load_all_empty_returns_empty_array`: behavior changes — `load()` now raises error on empty; update assertion to `verifyError`
- `test_load_all_filtered_by_metadata`: → `load(subject=1)` (version="latest" default)
- Tests in `% --- Record properties via load_all ---` section: → `load(version="all", ...)`
- Any existing test that calls `load()` and expects a table by default (e.g. `TestConfigureDatabase.m:73,81`): add explicit `as_table=true` or update expectation to a `BaseVariable`

**`sci-matlab/tests/matlab/scidb/TestForEachReturnValue.m`**

- Two `ProcessedSignal().load_all()` calls → `ProcessedSignal().load()`

**`sci-matlab/tests/matlab/scidb/TestMerge.m`**

- Two `MergedResult().load_all()` calls → `MergedResult().load()`

---

## Documentation

- Update `sci-matlab/README.md` API reference table: remove `load_all` row, update `load` description
- Update both `BaseVariable` docstrings

---

## Out of scope

- `as_table=` in `for_each()` — completely unrelated parameter, not changed
- `DatabaseManager.load_all()` — internal method, stays as-is
- `sci_matlab/bridge.py`'s `load_and_extract()` — already accepts `version_id`, no change needed
- `AmbiguousVersionError` in MATLAB — not currently checked, not added in this change
