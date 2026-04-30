# Fix: format_save_meta crash on struct constants

## Problem
When a struct constant (e.g. `gaitRiteConfig`) is passed to `scidb.for_each`, the save phase crashes with:
```
Error using string — Conversion to string from struct is not possible.
```

## Root Cause (two bugs)
1. `format_save_meta` uses `string(val)` for non-numeric values (line 1087), which doesn't work on structs
2. `format_save_meta` is called inside `catch` blocks (lines 964, 979, 1016) without its own error protection — so when it throws, the original save error is masked

`is_metadata_compatible` returns `true` for structs (line 1527), putting the struct into `constant_nv` → `save_nv`. `metadata_to_pydict` handles structs fine (via `jsonencode`), so the struct itself isn't the problem for saving — only for formatting.

## Fix
1. **`format_save_meta`**: add `isstruct(val)` branch that displays `key=<struct>` (or a truncated jsonencode)
2. **Catch blocks**: wrap `format_save_meta` calls in try-catch so the original error always surfaces
3. **Test**: add `test_struct_constant_input_saves_correctly` to TestForEach.m with a struct constant input
4. **Helper**: add `scale_with_config.m` test helper
