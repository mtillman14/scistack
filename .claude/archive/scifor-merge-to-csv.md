# Plan: `scifor.Merge.to_csv()`

## Problem
`scidb.Merge` has `.to_csv()` (`scidb/src/scidb/merge.py:42` → `csv_export.export_csv`),
but `scifor.Merge` does not. Calling `.to_csv()` on a `scifor.Merge` raises
`AttributeError: 'Merge' object has no attribute 'to_csv'` (observed in
`stats_main.py:96`, `best_df.to_csv(...)` where `best_df` is a `scifor.Merge`).

## Goal
Add `to_csv(filename, where=None, **metadata)` to `scifor.Merge` that writes the
column-wise merge of its constituents to a flat CSV.

## Semantics (confirmed with user)
- **Inner join** of the constituents.
- Keep exactly **one copy** of the schema columns (the join keys).
- Non-schema columns are assumed **non-overlapping** (no suffixing needed).
- NOTE: the `for_each` path (`_prepare_merge`/`_merge_parts`) is *not* literally an
  inner join — it combo-filters then positionally `concat`s after dropping schema
  cols. `to_csv` has no iteration, so it needs its own real `pd.merge(how="inner")`.
  End result matches the per-combo equivalence the user expects.

## Join-key detection
Because non-schema cols don't overlap, the columns common to two constituents *are*
the shared schema cols. At each fold step:
`join_keys = [c for c in acc.columns if c in part.columns]`, intersected with
`get_schema()` when schema is configured (belt-and-suspenders). Empty join_keys →
clear error.

## Implementation
1. New module `scifor/src/scifor/csv_export.py`:
   - `export_merge_csv(merge, filename, where=None, _log_fn=None, **metadata)`.
   - Validate `filename` ends with `.csv`.
   - For each constituent: resolve via `_resolve_data_spec` (handles plain DF,
     `Fixed`, `ColumnSelection`); apply `**metadata` row filters (scalar or list
     membership) on schema cols present; apply `where` via `_apply_where_filter`;
     keep schema cols + selected/all data cols (minus exclusions).
   - Fold constituents with `pd.merge(acc, part, on=join_keys, how="inner")`.
   - Log (NOTE 2): per-constituent shape, detected join keys, final shape before write.
   - `out.to_csv(filename, index=False)`.
   - Reject `Fixed(Merge(...))` analog / nested issues with clear messages reusing
     existing patterns.
2. `scifor/src/scifor/merge.py`: add `to_csv` method delegating to `export_merge_csv`
   (lazy import to avoid the `foreach -> merge` cycle).

## Tests (NOTE 2 — regression)
New `scifor/tests/test_merge_to_csv.py`:
- two DataFrames sharing one schema col → inner join, one copy of schema col, all
  data cols, correct rows.
- inner-join drops non-matching rows.
- `where=` filter.
- metadata kwarg filter (scalar and list).
- `ColumnSelection` constituent selects subset but still joins on schema cols.
- non-`.csv` filename → error.
- no shared schema col → clear error.
- (verified by user; assistant has no Python).

## Files touched
- `scifor/src/scifor/csv_export.py` (new)
- `scifor/src/scifor/merge.py` (add method)
- `scifor/tests/test_merge_to_csv.py` (new)
