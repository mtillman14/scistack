# Plan: Fix DataFrame list column round-trip through DuckDB (native numeric storage)

## Context

When a MATLAB table with mixed column types (scalars, numeric cell arrays, nested structs) is saved to DuckDB and loaded back, list-valued columns don't round-trip correctly. The save path stores them as VARCHAR (DuckDB uses Python's `str()` repr), and the load path returns them as raw strings instead of numeric arrays. This prevents `from_python.m` from correctly converting the DataFrame to a native MATLAB table.

The fix uses DuckDB's native `DOUBLE[]` column type for numeric list columns — no JSON serialization or extra metadata needed for the common case.

## Files to Modify

- `src/scidb/database.py` — `_save_columnar()` and `_unflatten_struct_columns()`

## Changes

### 1. `_save_columnar` — Use `DOUBLE[]` for numeric list columns

In column type inference during table creation, detect object columns with numeric lists → `DOUBLE[]`.

### 2. `_unflatten_struct_columns` — Convert lists to numpy arrays + backwards compat

After struct column reconstruction, sweep object columns to ensure list-valued cells are numpy arrays. Also parse VARCHAR strings from old saves.

### 3. No MATLAB changes needed

`from_python.m` already handles numpy arrays in DataFrame object columns correctly.
