# Plan: Speed up `flatten_nested_table_outputs` (MATLAB for_each)

## Problem (from profiler)
`flatten_nested_table_outputs` is the longest part of MATLAB for_each. Hot lines:

| Line | Code | Calls | Time |
|------|------|-------|------|
| 502 | `if ismember(icn, meta_block.Properties.VariableNames)` | 120,600 | 8.379 s |
| 536 | `pieces{end+1} = [meta_block, nested_block];` | 8,040 | 8.119 s |
| 516 | `nested_col_data{end+1} = cell_val.(icn);` | 120,600 | 5.600 s |

~8,040 rows, ~15 inner columns/row. Cost is O(rows × inner_cols) of dynamic
table subsref + repeated `Properties.VariableNames` extraction + per-row horzcat.

## Root causes
1. **Line 502** rebuilds the `VariableNames` cell array and runs `ismember`
   per inner column. But `meta_block`'s columns are *always* `meta_cols`
   (it's `repmat` of `meta_row`), so this set is constant for the whole loop.
2. **Lines 512/516** extract every inner column by name only to rebuild an
   equivalent table at line 533 — pure round-trip of the inner table.
3. **Line 536** horizontally concats two tables once per row (8,040×).

## Fix (3 changes, no behavior change)
### A. Hoist the meta-name lookup out of the inner loops
- Compute `meta_var_names = cellstr(meta_cols)` once before the row loop.
- Replace the per-inner-column `ismember(icn, meta_block.Properties.VariableNames)`
  with a single vectorized `ismember(innames, meta_var_names)` per nested
  *table* (8,040 calls instead of 120,600), iterating only over actual hits.

### B. Use the inner table whole instead of decomposing into columns
- Drop the per-column `nested_col_data`/`table(...)` rebuild.
- For each nested table: pop meta-override columns into `meta_block`, drop
  them from `inner`, rename collisions vs. already-seen names by prefixing,
  then `nested_block = [nested_block, inner]`. Common case (1 nested col,
  no collisions) does **zero** per-column extraction and no inner horzcat.
- Collision tracking via a growing `seen_names` cellstr seeded with
  `meta_var_names` (matches current `nested_name_set` + meta semantics:
  meta cols are removed first, so remaining collisions are nested-vs-nested).

### C. Defer the per-row horzcat
- Accumulate `meta_pieces{r}` and `nested_pieces{r}` separately (in row order).
- Assemble once: `out = [vertcat(meta_pieces{:}), vertcat(nested_pieces{:})]`.
- This is valid because the final `vertcat(pieces{:})` already requires a
  uniform schema across expanded rows.
- Pass-through rows (inner_h == 0) have a *different* schema (full output
  cols); the original `vertcat` already cannot mix them with expanded rows,
  so in any working dataset it is all-expanded or all-passthrough. Keep a
  `passthrough_pieces` list and: all-passthrough -> vertcat those; otherwise
  fast path. Preserves existing behavior/ordering for realistic inputs.

## Expected impact
- Line 502: 8.4 s -> ~0 (120,600 -> 8,040 vectorized calls, no Properties churn).
- Line 516: 5.6 s -> ~0 (no per-column extraction in common case).
- Line 536: 8.1 s -> ~one horzcat total.

## Risk / verification
- Semantics preserved: meta-override (inner value wins), collision prefixing
  `nc_name_icn`, non-table nested cell wrapped as `repmat({cell_val}, h, 1)`.
- Row ordering preserved for the realistic (non-mixed) case.
- User to run MATLAB for_each tests + re-profile (no Python/MATLAB in agent env).
