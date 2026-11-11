# Fix: as_table + column selection interaction

## Problem

When `as_table=True` and a column is selected (e.g., `MyVar["col_a"]`), column selection overrides `as_table` and only a raw vector is returned. The two features should be orthogonal:

- `as_table` controls **whether** the result is a table (with metadata columns) or raw data
- Column selection controls **which data columns** appear

## Root Cause

In both Python (`foreach.py`) and MATLAB (`for_each.m`), the operations happen sequentially:

1. `_multi_result_to_dataframe` / `fe_multi_result_to_table` → builds table with metadata + ALL data columns
2. `_apply_column_selection` / `apply_column_selection` → extracts ONLY selected data columns, discarding metadata

For single column selection, this returns a raw numpy array/MATLAB vector instead of a table.

## Fix Approach

When both `column_selection` and `as_table` are active for the same input:
1. Apply column selection to each individual variable's `.data` BEFORE building the table
2. Then build the table normally (metadata + filtered data columns)

When only one is active, use existing behavior unchanged.

## Files Changed

1. **`scirun-lib/src/scirun/foreach.py`** (Python)
   - Restructure lines 230-240 into 3 branches: both active, only as_table, only column_selection
   - Add `_apply_column_selection_to_vars()` helper that filters var.data in-place (keeps DataFrame form)

2. **`scidb-matlab/src/scidb_matlab/matlab/+scidb/for_each.m`** (MATLAB)
   - Same restructuring in serial loop (lines 486-503) and parallel loop (lines 759-776)
   - Apply column selection to each ThunkOutput.data before `fe_multi_result_to_table`
