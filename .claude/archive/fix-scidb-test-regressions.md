# Fix 4 scidb test regressions after scifor refactor

## Problem
Commit `004f900` ("All scifor tests pass") made changes to `scifor.for_each` that broke 4 scidb tests:

1. `TestForEach/test_path_input_resolves_template` — PathInput no longer auto-resolved
2. `TestForEach/test_distribute_true_saves_to_multiple_rows` — `subject` column stripped from distributed data
3. `TestForEachSchemaFiltering/test_no_filtering_with_pathinput` — PathInput not resolved → 0 results
4. `TestForEachSchemaFiltering/test_no_filtering_with_fixed_pathinput` — Same as #3 for Fixed(PathInput)

## Root Causes

### PathInput resolution removed (tests 1, 3, 4)
The scifor refactor removed PathInput auto-resolution from the constant branch. Scifor tests were updated so functions receive raw PathInput objects. But scidb tests expect resolved path strings.

### Distribute metadata stripping (test 2)
New code strips columns that overlap with metadata keys from distributed data tables. This is needed in scifor's flatten mode (to avoid column conflicts in `build_single_output_table`), but scidb uses nested mode (`_nest_table_outputs=true`) where no conflict occurs.

## Fix

### `scifor/for_each.m`
1. Added `_resolve_pathinput` internal option (default false)
2. When true, PathInput and Fixed(PathInput) constants are resolved per-combo using `val.load(meta_nv{:})`
3. Made distribute metadata stripping conditional: only strip when NOT in `nest_table_outputs` mode

### `scidb/for_each.m`
1. Pass `_resolve_pathinput=true` when inputs contain PathInput
2. Updated comment on convert_input PathInput handling

## Impact
- scifor tests: unaffected (don't use `_resolve_pathinput`)
- scidb tests: PathInput resolved as before; distribute preserves all data columns in nested mode
