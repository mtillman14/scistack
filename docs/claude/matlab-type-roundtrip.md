# MATLAB ↔ Python Type Round-Trip (to_python / from_python)

## Overview

The files `+scidb/+internal/to_python.m` and `+scidb/+internal/from_python.m` handle
bidirectional conversion between MATLAB native types and Python objects. These are used
when saving/loading data through the scidb-matlab bridge. The goal is 100% fidelity:
saving a MATLAB value and loading it back should produce the identical type and shape.

## Type Mapping Table

| MATLAB type | Python type | Storage | Round-trip fidelity |
|---|---|---|---|
| `double` / numeric scalar | `py.float` / `py.int` | native | Exact |
| Numeric vector (Nx1 or 1xN) | 1-D numpy ndarray | native | Exact |
| Numeric matrix (NxM, M>1) | list of 1-D ndarrays (object column in DataFrame) | JSON list-of-lists when inside dict; object column when in table | Exact — `try_stack_numeric` reconstructs the matrix on load |
| `string` scalar | `py.str` (via `char`) | native | Exact (see pandas 3.0 note below) |
| `string` array | `py.list` of strings | native | Exact — all-string lists are collapsed back to string arrays (see pandas 3.0 note below) |
| `logical` scalar | `py.bool` | native | Exact |
| `logical` array | numpy bool ndarray | native | Exact |
| `char` | `py.str` | native | Loads back as `string`, not `char` |
| `datetime` | ISO 8601 string | VARCHAR | Exact (if format matches) |
| `cell` array | `py.list` (recursive) | varies | Exact per element |
| `table` | `pandas.DataFrame` | multi-column DuckDB or custom | See per-column rules below |
| Scalar `struct` | `py.dict` (recursive) | one DuckDB column per field (`multi_column`), or JSON when `nested` | Exact — but see below |
| Struct array | `py.list` of `py.dict` | JSON list | Loads back as cell array of structs (not struct array) |
| `categorical` | converted to `string` before save | VARCHAR | Loads back as `string`, not `categorical` |

### Scalar struct: exact, but the shape depends on WHO loads it

A flat scalar struct is stored `multi_column` — one DuckDB column per field —
not as JSON. Two loaders read that back differently, and only one of them used
to give a struct:

| Loader | Result |
|---|---|
| `Type().load(...)`, single record | struct (`sciduckdb._load_record` rebuilds the dict) |
| `for_each` input, `nested` struct | struct (routes through `_load_as_df_via_iterator`, which keeps the dict in one column) |
| `for_each` input, flat struct | **was a 1×N table**; now a struct |

The last row is the one that was inconsistent. `for_each` loads inputs through
the *spread* layout, which deliberately puts each field in its own column so
that `Type("col")` column selection, `ColName`, `Merge`, `as_table=true`,
`share_limits` and `for_columns` all work on them. The cost is that one struct
record then looks exactly like a one-row table, and `extract_data` only unwraps
a single row when there is a *single* data column — a struct has as many
columns as fields, so it fell through and the function got a table whose field
accesses returned `1×1` cells.

The columns cannot simply be collapsed at load time without breaking every
feature listed above, so the reassembly happens per combo, after column
selection has had its chance: scidb marks the input via
`DatabaseManager.mapping_data_columns` → `_resolve_mapping_inputs` →
`for_each_prepare`'s `mapping_inputs` → `+scifor/for_each.m`'s
`'_mapping_inputs'`. scifor cannot infer the mark — "one row, N data columns"
is also exactly what a genuine table-valued variable looks like. A field that
loaded alongside other records comes back with its saved orientation (`N×1`,
not the `1×N` row that `try_stack_numeric`'s matrix stacking would otherwise
leave). `as_table` and column selection still win.

## Table Column Handling (to_python.m → pandas DataFrame)

When a MATLAB table is converted to a pandas DataFrame, each column is extracted
with `data.(col_name)` and processed:

| Column type | Conversion | pandas dtype | Notes |
|---|---|---|---|
| Numeric Nx1 vector | `to_python(col)` → 1-D ndarray | float64/int64 | Standard path |
| Numeric NxM matrix (M>1) | Split into cell of 1xM row vectors → list of 1-D ndarrays | object | pandas rejects 2-D ndarrays as column values (see below) |
| String array | `to_python(col)` → list of strings | object (pandas <3.0) or StringDtype (pandas 3.0+) | See pandas 3.0 section |
| Datetime array | Format to ISO string first | object (string) | — |
| Categorical | Convert to string first | object | — |
| Struct array | `to_python(col)` → list of dicts | object | Each struct → dict recursively |
| Cell array | `to_python(col)` → list (recursive) | object | — |

### Why NxM matrices need special handling

pandas.DataFrame rejects 2-D numpy arrays as column values:

```python
# pandas source check:
elif isinstance(val, np.ndarray) and val.ndim > 1:
    raise ValueError("Per-column arrays must each be 1-dimensional")
```

The fix in `to_python.m` detects multi-column numeric table variables
(`isnumeric && ismatrix && ~isvector && ~isscalar`) and splits them into a
cell array of row vectors before conversion. Each 1xM row vector becomes a
1-D ndarray, which pandas accepts as an object-dtype column element.

## DataFrame → Table Load Path (from_python.m)

When loading a pandas DataFrame back to a MATLAB table:

| pandas dtype | Conversion | MATLAB result |
|---|---|---|
| numeric (float64, int64, etc.) | `from_python(col.to_numpy())` → `double()` | Numeric column vector |
| datetime64 | ISO string extraction → `datetime()` | Datetime column |
| object (dicts) | Element-by-element `from_python` → cell of structs | Cell column of structs |
| object (uniform numeric lists) | Element-by-element `from_python` → cell of row vectors → `try_stack_numeric` → `vertcat` | NxM numeric matrix |
| object (strings) | Element-by-element `from_python` → cell of strings → `try_stack_strings` → `vertcat` | String column vector |
| object (mixed types) | Element-by-element `from_python` | Cell column |
| StringDtype (pandas 3.0+) | `from_python(col.to_numpy())` → cell → `try_stack_strings` | String column vector |

### try_stack_numeric

Local helper function in `from_python.m` that enables matrix round-tripping.
After an object column is converted to a cell array, this function checks whether
ALL elements are numeric with identical `size()`. If so, it `vertcat`s them into
a matrix. Otherwise it returns the cell array unchanged.

The `args{i} = args{i}(:)` column-vector enforcement is also guarded with
`isvector(args{i})` so that reconstructed NxM matrices are not flattened.

## Struct Round-Trip Details

### Scalar struct (direct save, not inside a table)

Save path: `to_python` → `py.dict` with recursive field conversion →
`_infer_duckdb_type` returns `("JSON", {python_type: "dict", ndarray_keys: {...}})` →
`_python_to_storage` calls `json.dumps(_convert_for_json(value))` which converts
all ndarrays to nested lists.

Load path: JSON string → `json.loads` → dict → `storage_to_python` restores
top-level `ndarray_keys` to numpy arrays → `from_python(py.dict)` →
`pydict_to_struct` → MATLAB struct.

**Nested ndarrays caveat:** Only top-level dict keys listed in `ndarray_keys`
are restored to numpy arrays. Arrays inside nested dicts come back as Python
lists, which `from_python` converts to cell arrays (not double matrices).
This is a sciduck limitation in `_infer_duckdb_type` — it only tracks one level
of `ndarray_keys`.

### Struct array (inside a table column)

Save: struct array → `py.list` of `py.dict` (new non-scalar struct handler) →
object-dtype column in DataFrame.

Load: object column → cell array of structs via element-by-element `from_python`.
Note: the original struct array becomes a **cell array of structs**, not a struct
array. This is a minor fidelity loss (cell vs struct array), but field contents
are preserved exactly.

### Struct metadata columns (save_from_table)

In `BaseVariable.save_from_table`, struct metadata columns are converted to
JSON strings via `jsonencode` before crossing to Python. Each struct element
becomes a separate JSON string. This is handled in the metadata column loop
in `BaseVariable.m`.

## pandas 3.0 StringDtype Compatibility

### Problem

pandas 3.0 changed the default string storage from `object` dtype to `StringDtype`.
When DuckDB's `fetchdf()` returns VARCHAR columns, they now have a dtype name like
`"string"` instead of `"object"`.

In `convert_dataframe` (inside `from_python.m`), the dtype dispatch has three branches:

1. `startsWith(dtype_str, "datetime")` → datetime path
2. `dtype_str == "object"` → object path (includes `try_stack_strings`, `try_stack_numeric`, etc.)
3. `else` → fallback: `from_python(col.to_numpy())`

With pandas <3.0, string columns had `object` dtype and went through branch 2, which
called `try_stack_strings` to coalesce cell arrays of strings into MATLAB string arrays.

With pandas 3.0+, string columns have `StringDtype` (dtype name `"string"`), so they
fall through to branch 3. The fallback calls `from_python(col.to_numpy())`, which
returns a cell array of strings (e.g. `{["1"]}`) but does NOT call `try_stack_strings`.
This causes string columns in loaded tables to come back as cell arrays instead of
string arrays.

### Symptom

A MATLAB table with a string column saved to DuckDB and loaded back returns the column
as a cell array (`{["1"]}`) instead of a string (`"1"`). This breaks `verifyEqual`
assertions and any code that expects `result.data.column` to be a string type.

### Fix

Added `try_stack_strings` to the `else` branch in `convert_dataframe`:

```matlab
else
    args{i} = scidb.internal.from_python(col.to_numpy());
    if iscell(args{i})
        args{i} = try_stack_strings(args{i});
    end
end
```

This ensures that cell arrays of strings produced by the fallback path are coalesced
into MATLAB string arrays, matching the behavior of the `object` dtype path.

### Future considerations

If pandas 3.0+ introduces other new default dtypes (e.g. nullable `Int64` instead of
`int64`), similar issues may arise in the fallback path. The pattern is always the same:
`from_python(col.to_numpy())` returns a cell array that needs post-processing via
`try_stack_*` helpers. If new dtype issues appear, check whether the `else` branch
needs additional stacking helpers.

## Key Files

- `sci-matlab/src/sci_matlab/matlab/+scidb/+internal/to_python.m` — MATLAB → Python
- `sci-matlab/src/sci_matlab/matlab/+scidb/+internal/from_python.m` — Python → MATLAB
- `sci-matlab/src/sci_matlab/matlab/+scidb/BaseVariable.m` — save_from_table struct metadata
- `sciduck/src/sciduck/sciduck.py` — `_convert_for_json`, `_python_to_storage`, `storage_to_python`, `_infer_duckdb_type`
