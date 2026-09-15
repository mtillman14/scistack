# Plan: coarse-level inputs must broadcast, not fail

## Problem

`grSides` at trial level: `completed=0, failed=420, no_data=158`. All 420
failures are `failed to filter side: Conversion from cell failed. Element 1
must be convertible to a string scalar.` at `filter_table_for_combo`
(`+scifor/for_each.m:1585`).

`side` is `Demographics["PareticSide"]`; Demographics is subject-level. The
spread loader emits one column per *dataset* schema key, so `session`, `speed`
and `trial` arrive all-NULL, cross the bridge as cells of `0×0 double`, and
`string(col_data)` throws. Python has the same bug with a quieter symptom
(`None == "BL"` → zero rows → `scifor:NoData` every combo).

Neither ColumnSelection nor MATLAB specific: bare `Demographics` fails the
same way, and this is fixed today only for `Merge` constituents.

## Approach

One owner in scidb, at the point the artifact is created.

1. `docs/claude/coarse-level-inputs.md` — the contract, both failure modes,
   the three prior local fixes and the gap between them. **Done.**
2. `_drop_unpopulated_schema_columns(df, schema_keys, context)` in
   `scidb/src/scidb/foreach.py`: drop schema-key columns that are *entirely*
   null; INFO-log what was dropped and what remains.
3. Apply in `_load_input` to every input kind — plain variable type,
   `ColumnSelection`, `Merge` constituents (replacing the inline drop),
   `Fixed` via recursion.
4. `scidb/tests/test_coarse_input_broadcast.py` — subject-level input under
   full subject/session iteration, for each input kind; assert the function is
   called once per combo with the broadcast value, and assert mixed-granularity
   columns are NOT dropped.

## Decisions

- **Only fully-null columns.** A row that genuinely has no session differs from
  a variable with no session axis; partial columns keep filtering.
- **scidb, not scifor.** The all-NULL column is scidb's artifact; fixing it in
  scifor means writing it twice (Python + MATLAB).
- **Leave `drop_all_empty_cell_columns` in `+scidb/for_each.m`.** Redundant
  after this, harmless, and defensive for the merge path.
- **Leave the `_aggregation_mode` drop and the two `schema_cols_in_df`
  exclusions.** Now redundant rather than wrong.

## Not doing

- Changing what the spread loader emits (`_assemble_df_from_records_and_data`
  keeps one column per dataset schema key — other consumers rely on it).
- Touching `filter_table_for_combo` in either scifor.

## Round 2 (after first test run)

`test_bare_coarse_variable_broadcasts` and the Merge test (which also binds
bare `side: Demographics`) returned **0 calls**, while the ColumnSelection and
Fixed tests passed. Second, independent defect: full-iteration rid expansion
looked up `rid_per_combo` by the full combo key, but a coarse input's mapping
keys carry `""` in unpopulated positions → never matched → every combo pruned.
ColumnSelection escaped because it is pruning-only and `_colsel_combo_present`
already compares on populated positions.

Fix: `rid_populated_idx` recorded beside `rid_per_combo`; `_rid_probe_key`
blanks the same positions before probing. Fully-populated inputs probe
unchanged (byte-identical to before). Aggregation mode untouched — it keys by
iterated positions only and was never affected.
