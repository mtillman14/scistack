# Fix: MATLAB PathInput-only Functions Show Green Instead of Grey

## Problem
After a MATLAB pipeline run where 7/8 combos succeed and 1 fails, the function node shows green instead of grey.

## Root Cause
The Python `scidb.for_each` calls `_persist_expected_combos()` to write ALL attempted combos (including ones that will fail) to the `_for_each_expected` table BEFORE execution starts. This table is used by `check_node_state` to detect missing combos.

The MATLAB `scidb.for_each` is a separate implementation that does NOT call `_persist_expected_combos`. So `_for_each_expected` is never populated for MATLAB runs. When `check_node_state` queries it, there are no rows, so expected combos = empty, and only actual (successful) combos are counted → green.

Additionally, for PathInput-only functions, `_get_lineage_variants` returns empty (because PathInput inputs have no variable/rid_tracking entries in lineage) which means the lineage-based expected combo enumeration also produces nothing.

## Fix
Three changes:

### 1. MATLAB `scihist.for_each` → pass real fn name
Pass `'_fn_name', func2str(fn)` to `scidb.for_each` so expected combos are stored with the correct function name (not the anonymous wrapper).

### 2. MATLAB `scidb.for_each` → add `_fn_name` option
Recognize `_fn_name` in `split_options` and use it to override `fn_name` for persist calls.

### 3. MATLAB `scidb.for_each` → persist expected combos
After `all_combos` is finalized, call `py.scidb.foreach._persist_expected_combos()` to populate the `_for_each_expected` table. Use the real fn_name.

## Files Changed
- `sci-matlab/src/sci_matlab/matlab/+scihist/for_each.m` — pass `_fn_name`
- `sci-matlab/src/sci_matlab/matlab/+scidb/for_each.m` — add `_fn_name` option + persist call
