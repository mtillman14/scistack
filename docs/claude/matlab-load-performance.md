# MATLAB Load Performance Optimizations

## Problem

`for_each()` was slow due to two compounding bottlenecks in the MATLAB-Python bridge:

1. **Per-iteration database queries**: Each `load()` call in the `for_each` loop ran a separate database query. For 108 iterations with 1 input, that was 108 queries (~86s total, ~800ms each).

2. **Per-variable wrapping overhead**: Converting each Python BaseVariable to a MATLAB BaseVariable wrapper required ~34 MATLAB-Python boundary crossings (attribute accesses for record_id, content_hash, lineage_hash, version_id, parameter_id, plus ~4 crossings per metadata key). For 8328 variables with 6 metadata keys each, this was ~250K boundary crossings (~20.7s).

## Solution: Two-Part Optimization

### Optimization 1: Batch Wrapping via Python Bridge

**Files**: `bridge.py`, `BaseVariable.m`

Instead of crossing the MATLAB-Python boundary ~34 times per variable, a Python bridge function (`wrap_batch_bridge`) extracts all scalar fields in bulk:

- Record IDs, content hashes, lineage hashes, version/parameter IDs are packed into newline-joined strings (1 crossing each to transfer, then parsed in pure MATLAB via `splitlines`)
- All metadata dicts are serialized as a single JSON array string, parsed by MATLAB's native C `jsondecode` (1 crossing total for all metadata)
- Data values and Python object references are returned as Python lists (still 1 crossing per element for data conversion, which is unavoidable since data types vary)

This reduces boundary crossings from ~34 per variable to ~3 per variable (data, py_obj reference, and the fixed overhead of bulk field extraction).

**Used by**: `BaseVariable.load()` (multi-result and `as_df`/`as_table` paths) and `for_each` preloading.

### Optimization 2: Pre-loading in for_each

**File**: `for_each.m`

Instead of calling `load()` per iteration (one DB query each), `for_each` now pre-loads all data for each input variable type in a single query before the main loop:

1. For each loadable input (BaseVariable or Fixed, not PathInput), the iteration metadata values are passed as arrays to `load_all_as_df()`, which uses SQL `IN` clauses for matching.
2. Results are batch-wrapped via Optimization 1.
3. A `containers.Map` lookup table is built, keyed by a sorted metadata string (e.g., `"session=A|subject=1"`).
4. In the main loop, results are looked up from the map instead of querying the database.

**Fixed inputs** are handled by overriding specific metadata keys in the query (e.g., `session="BL"` replaces the iterated session values) and adjusting the lookup key accordingly.

**Preload option**: `preload` defaults to `true`. Users can disable it with `preload=false` for datasets too large to fit in memory simultaneously:

```matlab
scidb.for_each(@fn, inputs, outputs, preload=false, subject=[1 2 3], ...);
```

When `preload=false`, the original per-iteration `load()` behavior is used (also the fallback for PathInput and empty metadata).

## Key Implementation Details

### Lookup Key Construction

The lookup key is built from sorted metadata key=value pairs joined by `|`. Three helper functions handle this:

- `build_meta_key(keys, vals)` — core key builder from parallel arrays
- `result_meta_key(metadata_struct, query_keys)` — extracts values from a loaded result's metadata struct using only the query keys
- `combo_meta_key(meta_keys, combo, fixed_meta, query_keys)` — builds the key for a specific iteration, applying Fixed overrides

The key only includes the metadata keys that were part of the query, not all metadata keys on the result. This ensures Fixed inputs (which query with different keys) produce matching lookup keys.

### JSON Metadata Type Consistency

`jsondecode` returns string values as `char` arrays, while the original `pydict_to_struct` returns MATLAB `string` type. The batch wrapper post-processes jsondecoded structs to convert `char` fields to `string` for consistency.

### Fallback Behavior

The preloaded path is skipped (falls back to per-iteration `load()`) when:
- `preload=false`
- `dry_run=true`
- No metadata keys are specified (empty iteration space)
- The input is a `PathInput` (not a database load)

### Optimization 3: Bulk Loading for Custom-Dtype Records

**File**: `database.py` (`load_all_as_df()`, `_deserialize_custom_subdf()`, `_build_bulk_where()`)

The `load_all_as_df()` bulk path originally fell back to per-record SQL queries (`_load_by_record_row()`) when:
1. The variable class had custom serialization (`to_db`/`from_db` overrides), or
2. The `dtype_meta` had `custom=True` (e.g., DataFrame variables stored as raw columns)

This meant types like tables (DataFrames stored with `custom=True` dtype) would issue N individual `SELECT * WHERE pid=? AND vid=? AND sid=?` queries — one per record. For 2741 records, this was ~5500 SQL queries taking ~10s.

**Solution**: `load_all_as_df()` now partitions parameter_ids into `custom_pids` and `native_pids`, then handles each with a single bulk SQL query:

1. **Custom pids**: One `SELECT * FROM "table" WHERE ...` fetches all data rows at once. Results are grouped by `(parameter_id, version_id, schema_id)` via `DataFrame.groupby()`. Each group is passed to `_deserialize_custom_subdf()`, which dispatches to the correct deserialization path:
   - `dict_of_arrays`: reconstruct dict of numpy arrays with dtype/shape restoration
   - `from_db()`: class-level custom deserialization (called per-record sub-DataFrame)
   - `struct_columns`: unflatten dot-separated columns back to nested dicts
   - Raw DataFrame: return sub-DataFrame as-is (most common case for tables)

2. **Native pids**: Existing bulk path (column-specific SELECT, type restoration via `restore_types`)

3. Both lookups are merged in a single instance-construction loop.

The WHERE clause building is extracted into `_build_bulk_where()`, reused by both paths. It uses per-dimension `IN` clauses when the key set is a full cross product, otherwise falls back to tuple-IN with `VALUES`.

**Key detail**: Custom bulk uses `SELECT *` (not column-specific) because custom dtypes don't store column metadata in `dtype_meta["columns"]`. Internal columns (`parameter_id`, `version_id`, `schema_id`) are dropped after fetch, before deserialization.

## Pitfall: numpy array → double conversion requires ascontiguousarray

When extracting numpy arrays from a Python dict returned by the bridge (e.g. `bulk{'scalar_data'}`), MATLAB's `double()` **cannot** directly convert a `py.numpy.ndarray`:

```
Error using double
Conversion to double from py.numpy.ndarray is not possible.
```

The correct pattern (matching `from_python.m` line 12) is to call `py.numpy.ascontiguousarray()` first:

```matlab
% WRONG — raises 'MATLAB:invalidConversion'
all_scalar_data = double(bulk{'scalar_data'});

% CORRECT
all_scalar_data = double(py.numpy.ascontiguousarray(bulk{'scalar_data'}));
```

This applies anywhere a numpy array crosses the Python→MATLAB boundary outside of `from_python.m`. Both `scalar_data` (float64) and `concat_df_row_counts` (int64) in `wrap_py_vars_batch` need this treatment.

## Performance Impact

| Component | Before | After |
|---|---|---|
| DB queries (scalars) | ~86s (108 queries) | ~1-2s (1 query per input type) |
| Variable wrapping | ~20.7s (~250K crossings) | ~5-8s (~17K crossings) |
| DB queries (tables/custom) | ~10s (5500 queries for 2741 records) | <1s (3 queries) |
| **Total** | **~107s** | **~7-10s** |


## Optimization 4: from_python Auto-Conversion Early Exit

### Problem: MATLAB Auto-Converts Python Scalars Before from_python Sees Them

When MATLAB extracts elements from a Python list via `cell(py.list(...))`, it
auto-converts certain Python types to native MATLAB types:

- `py.float` → MATLAB `double`
- `py.bool` → MATLAB `logical`
- `py.str` → MATLAB `string` (in some MATLAB versions)

This means that by the time `from_python(elem)` is called, the argument is already
a plain MATLAB `double`, not a `py.float`. The original `from_python` code had no
early check for native MATLAB types — the value would fall through all 10+
`isa(py_obj, 'py.*')` checks and reach the `else` fallback branch, which:

1. Tries `py.builtins.isinstance(py_obj, py.pandas.DataFrame)` — throws a
   `PyException` because you can't pass a MATLAB double to a Python function
   expecting a Python object
2. Catches the exception silently
3. Tries `double(py_obj)` — succeeds (it's already a double)

**Each auto-converted element = 1 PyException construction + 2 stack trace operations.**

### Impact on Wide DOUBLE[] Tables

For a table like GAITRiteLoaded (54 columns, many of type DOUBLE[]):

- The concat_df optimization successfully merges all records into one DataFrame
- `convert_dataframe()` processes each column:
  - DOUBLE columns → `from_python(col.to_numpy())` → fast numeric bulk path
  - DOUBLE[] columns → pandas `object` dtype → element-by-element `from_python`
- Each DOUBLE[] cell is a Python list of floats
- `from_python` on the list → `cell(py.list)` → elements are auto-converted to
  MATLAB doubles → recursive `from_python` on each double → exception fallback

With ~30 DOUBLE[] columns × N records × ~15 elements per array = ~50K exceptions:

| Overhead source | Calls | Self Time |
|---|---|---|
| PyException constructor | 50,836 | 14.8s |
| PyException.getStack | 101,672 | 31.8s |
| ExternalException | 50,836 | 7.6s |
| **Total exception overhead** | — | **54.2s** |

The actual user function only needed 0.374s; the rest was all interop overhead.

### Fix 1: Native Type Early-Exit Guards (from_python.m lines 12-23)

Added at the very top of `from_python`, before any `py.*` checks:

```matlab
cl = class(py_obj);
is_python = numel(cl) >= 3 && cl(1) == 'p' && cl(2) == 'y' && cl(3) == '.';
if ~is_python
    if islogical(py_obj) || isnumeric(py_obj) || isstring(py_obj)
        data = py_obj;
        return;
    end
end
```

**Critical pitfall**: `isnumeric()` and `isstring()` can return `true` for Python
proxy objects (`py.int`, `py.str`) in some MATLAB versions.  The class-name prefix
check (`cl(1:3) == 'py.'`) is the only reliable way to distinguish native MATLAB
types from Python proxy objects.  An earlier version used `~isa(py_obj, 'py.type')`
which was insufficient — `py.int(5)` is an *instance* not a *type*, so
`isa(py.int(5), 'py.type')` returns `false`.

This eliminates **all exception overhead** (~54s) for auto-converted values. The
`islogical` check was already present but lacked a `return` statement, so it fell
into the subsequent `elseif` chain instead of exiting early.

### Fix 2: Bulk List-to-Numpy Conversion (from_python.m lines 70-86)

For `py.list` objects, try converting the entire list to a numpy array in one
Python call before falling back to element-by-element conversion:

```matlab
elseif isa(py_obj, 'py.list')
    try
        py_arr = py.numpy.asarray(py_obj);
        dtype_kind = string(py_arr.dtype.kind);
        if dtype_kind ~= "O"
            data = scidb.internal.from_python(py_arr);
            return;
        end
    catch
    end
    % ... original element-by-element fallback ...
```

When a Python list contains homogeneous numeric values (the common case for
DOUBLE[] columns from DuckDB), `numpy.asarray()` converts the entire list to a
typed ndarray in one call. This replaces N boundary crossings (one per element)
with 1 boundary crossing for the whole array.

Falls through gracefully for:
- Heterogeneous lists (mixed types) → numpy returns object array → `dtype_kind == "O"` → skip
- Lists of nested structures → numpy raises → caught and falls through

### Combined Impact

For the GAITRiteLoaded case (54 columns, many DOUBLE[], 221 iterations):

| Component | Before fixes | After fixes |
|---|---|---|
| Exception overhead | ~54s | ~0s |
| Element-by-element from_python | ~30s | ~2s (bulk numpy) |
| Remaining interop (to_python, etc.) | ~10s | ~10s (unchanged) |
| **Total** | **~142s** | **~12-15s** |

### Why the Second for_each Call Was Already Fast

```matlab
scidb.for_each(@calculateLRSymmetryTwoVectorsNoTable, ...
    struct('v1', GAITRiteLoadedCycle("StepLengths_GR"), 'formulaNum', 6), ...
    {StepLengthSymmetry()}, ...
    subject=[], session=[], speed=[], trial=[], distribute=true);
```

This selects a **single column** (`"StepLengths_GR"`) via the column selection
syntax `GAITRiteLoadedCycle("StepLengths_GR")`. The preloaded concat_df has only
1 column instead of 54, so the recursive from_python overhead scales down ~54x.
Even without the fix, it was tolerable.

### Diagnostic Pattern

When `from_python` is a bottleneck, the MATLAB Profiler will show these correlated
signatures:

- `from_python`: very high call count (10K-100K+)
- `PyException.getStack`: ~2× the `PyException` call count
- `flipud`: same count as `PyException.getStack` (MATLAB flips Python stack traces)
- `ExternalException`: same count as `PyException`

If these appear, check whether auto-converted MATLAB native types are reaching the
`else` fallback branch in `from_python`.
