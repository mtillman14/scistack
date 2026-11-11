# Plan: PathInput.discover() — Filesystem-driven metadata discovery

## Status: IMPLEMENTED

## Files Modified

1. **`scifor/src/scifor/pathinput.py`** — Added `placeholder_keys()` and `discover()` methods
2. **`sci-matlab/src/sci_matlab/matlab/+scifor/PathInput.m`** — Added `placeholder_keys()` and `discover()` methods + local helper functions
3. **`scidb/src/scidb/foreach.py`** — Added `_find_pathinput()` helper; integrated discovery fallback when DB returns empty
4. **`sci-matlab/src/sci_matlab/matlab/+scidb/for_each.m`** — Added `find_pathinput()` helper; integrated discovery fallback when DB returns empty

## Files Created

5. **`scifor/tests/test_pathinput_discover.py`** — Python tests for discover()
6. **`sci-matlab/tests/matlab/scifor/TestPathInput.m`** — MATLAB tests added to existing file

## Design

### discover() Algorithm
1. Split template into path segments by `/`
2. Recursively walk from root_folder (or cwd)
3. Literal segments must match exactly (descend if directory, accept if final file)
4. Placeholder segments are converted to regexes with named capture groups
5. Consistency check: if a placeholder was already bound earlier, the new value must match
6. Returns list of dicts (Python) / cell array of structs (MATLAB)

### for_each Integration
Two cases handled:
- **Case 1: No metadata keys at all** — call discover(), populate both keys and values
- **Case 2: Some keys resolved to empty from DB** — call discover() to fill empty keys
- When discovered_combos is set, use it directly as base_combos (avoids invalid Cartesian products)
