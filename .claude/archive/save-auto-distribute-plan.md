# Plan: Auto-distribute save() when given a table with schema-key columns

## Problem
`SubjectGrouping().save(tbl)` where `tbl` has `[subject, intervention]` columns saves
the whole table as one record. Users had to call `save_from_table()` explicitly.
`distribute=true` was never wired to `.save()` — it only works inside `for_each`.

## Solution
Add table-detection inside `save()` in `BaseVariable.m`. No Python changes needed.

### Detection condition
1. `istable(data)`
2. At least one table column name matches a configured `dataset_schema_keys`

### Dispatch
- `meta_cols` = table columns that ARE in schema keys
- `data_cols` = table columns that are NOT in schema keys
- 0 data cols → error
- 1 data col → `save_from_table(data, data_cols(1), meta_cols, varargin{:})`
- Multiple data cols → wrap each row's sub-table in a cell, add temp column
  `scidb_row_data_`, dispatch to `save_from_table`
- No meta cols (no schema-key columns in table) → fall through, save whole table
  as one record (existing behavior preserved)

## Files changed
- `sci-matlab/src/sci_matlab/matlab/+scidb/BaseVariable.m` — ~20 lines added to `save()`
- `sci-matlab/tests/matlab/scidb/TestSaveFromTable.m` — new test methods for auto-dispatch

## Tests added
- `test_auto_distribute_single_data_col` — basic case
- `test_auto_distribute_multiple_data_cols` — multi-col sub-table per row
- `test_auto_distribute_no_schema_cols_falls_through` — whole-table fallthrough
- `test_auto_distribute_with_common_metadata` — varargin pass-through
