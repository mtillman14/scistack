# Plan: Multi-Result DataFrame/Table Return for load() and for_each()

## Context

When `for_each()` iterates at a schema level higher than a variable's save level (e.g. iterating over `subject` but the variable is saved per `trial`), `load()` returns multiple results. Currently these come back as a list of `BaseVariable` objects, which is awkward for the function to work with.

The user wants a tabular return format: `pd.DataFrame` in Python, MATLAB `table` in MATLAB.

## Design Decisions

- **Opt-in for `load()`**: Add `as_table=True` parameter to `BaseVariable.load()`. Default is `False` (no breaking change).
- **Opt-in for `for_each()`**: Add `as_table` parameter (list of input names) to `for_each()`. Only specified inputs get DataFrame/table treatment. No automatic conversion.
- **Non-scalar data**: Arrays/objects are allowed in the data column (object dtype).
- **DataFrame columns**:
  - Schema key columns: one per key (e.g. `subject`, `trial`)
  - `version_id`: single column (integer)
  - Parameter key columns: one per parameter kwarg (e.g. `smoothing`, `filter_hz`)
  - Data column: named after the variable's `view_name()` (i.e. the class name)
- **Thunk interaction**: Out of scope for initial implementation.

## Changes by File

### 1. `src/scidb/variable.py` — `BaseVariable`

**Add `version_id` and `parameter_id` properties** (populated during load):

```python
def __init__(self, data):
    self.data = data
    self.record_id = None
    self.metadata = None
    self.content_hash = None
    self.lineage_hash = None
    self.version_id = None       # NEW
    self.parameter_id = None     # NEW
```

**Add `as_table` parameter to `load()`**:

```python
@classmethod
def load(cls, version="latest", loc=None, iloc=None, as_table=False, **metadata):
```

When `as_table=True` and multiple results:
- Build a `pd.DataFrame` with schema key columns + `version_id` column + parameter key columns + data column (named `cls.view_name()`)
- Return the DataFrame
- When `as_table=True` and single result: still return single `BaseVariable`

```python
# In the multi-result branch:
if as_table:
    rows = []
    for var in results:
        row = dict(var.metadata) if var.metadata else {}
        row["version_id"] = var.version_id
        row[cls.view_name()] = var.data
        rows.append(row)
    return pd.DataFrame(rows)
else:
    return results  # list, as before
```

### 2. `src/scidb/database.py` — `DatabaseManager._load_by_record_row()`

Populate `version_id` and `parameter_id` on the loaded instance:

```python
instance = variable_class(data)
instance.record_id = record_id
instance.metadata = flat_metadata
instance.content_hash = content_hash
instance.lineage_hash = lineage_hash
instance.version_id = version_id        # NEW (already available from row)
instance.parameter_id = parameter_id    # NEW (already available from row)
```

### 3. `scirun-lib/src/scirun/foreach.py` — `for_each()`

**Add `as_table` parameter** (list of input names):

```python
def for_each(
    fn, inputs, outputs,
    dry_run=False, save=True, pass_metadata=None,
    as_table=None,          # NEW: list of input names to load as DataFrame
    **metadata_iterables,
):
```

**In the load loop**, after loading an input, check if that input name is in `as_table`. If so, and if the result is a list, convert to DataFrame:

```python
as_table_set = set(as_table) if as_table else set()

# ... in the load loop:
loaded_inputs[param_name] = var_type.load(**load_metadata)

if param_name in as_table_set and isinstance(loaded_inputs[param_name], list):
    loaded_inputs[param_name] = _multi_result_to_dataframe(
        loaded_inputs[param_name], var_type
    )
```

**Add helper**:
```python
def _multi_result_to_dataframe(results, var_type):
    import pandas as pd
    view_name = var_type.view_name() if hasattr(var_type, 'view_name') else var_type.__name__
    rows = []
    for var in results:
        row = dict(var.metadata) if var.metadata else {}
        row["version_id"] = getattr(var, "version_id", None)
        row[view_name] = var.data
        rows.append(row)
    return pd.DataFrame(rows)
```

### 4. `scidb-matlab/src/scidb_matlab/matlab/+scidb/BaseVariable.m` — `load()`

Add `as_table` option parsing from varargin. When `as_table` is true and `n > 1`, construct a MATLAB `table`.

### 5. `scidb-matlab/src/scidb_matlab/matlab/+scidb/for_each.m`

Add `as_table` option to `split_options`. After loading, if input name is in `as_table` and result is array, convert to table.

### 6. MATLAB ThunkOutput — expose `version_id`

Add `version_id` and `parameter_id` properties. Populate in `wrap_py_var()`.

### 7. Tests

- `load(as_table=True)` with multi-result, single result, version keys, array data
- `for_each(as_table=["values"])` with level mismatch
- MATLAB equivalents

## Example Usage

### Python
```python
# Direct load
df = StepLength.load(as_table=True, subject=1)
#   subject trial  version_id  StepLength
# 0       1     1           1        0.1
# 1       1     2           1        0.2

# for_each
for_each(compute_mean, inputs={"steps": StepLength}, outputs=[MeanStepLength],
         as_table=["steps"], subject=[1, 2, 3])
```

## Implementation Order

1. Add `version_id` / `parameter_id` to BaseVariable + populate in `_load_by_record_row()`
2. Add `as_table` to `BaseVariable.load()` (Python)
3. Add `as_table` to `for_each()` (Python)
4. Python tests
5. MATLAB: ThunkOutput version_id, BaseVariable.load as_table, for_each as_table
6. MATLAB tests
