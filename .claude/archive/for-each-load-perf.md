# for_each Load Performance Optimization Plan

## Problem
`for_each()` takes ~107s for 108 iterations due to:
1. 108 separate database queries (86s) — one per iteration per loadable input
2. 8328 `wrap_py_var` calls (20.7s) — ~34 MATLAB↔Python boundary crossings each

## Optimization 1: Batch Wrapping via Python Bridge

### Changes
- **bridge.py**: Add `wrap_batch_bridge(py_vars_list)` function that:
  - Extracts record_id, content_hash, lineage_hash as newline-joined strings
  - Extracts version_id, parameter_id as Python lists of ints
  - Serializes all metadata dicts as one JSON string
  - Returns data list and original py_vars list for reference

- **BaseVariable.m**: Add `wrap_py_vars_batch(py_list, n)` static method that:
  - Calls `wrap_batch_bridge` once (1 crossing)
  - Uses `jsondecode` for metadata (native C, no crossings)
  - Uses `splitlines` for ID strings (1 crossing each)
  - Still calls `from_python()` per data element (unavoidable)

- **BaseVariable.m load() / load_all()**: Use batch wrapping when n > 1

### Expected improvement: ~20.7s → ~5-8s (eliminates ~250K boundary crossings)

## Optimization 2: Pre-load Inputs in for_each

### Changes
- **bridge.py**: Add `preload_bridge(type_name, meta_keys, meta_values_lists, fixed_overrides, db)` that:
  - Builds metadata dict with list values (uses SQL IN clause)
  - Applies Fixed overrides per-input
  - Runs ONE `load_all(version_id='latest')` query
  - Returns bulk-extracted results via `wrap_batch_bridge`

- **for_each.m**: Before the main loop:
  1. For each loadable input, compute effective metadata (with Fixed overrides)
  2. Call `preload_bridge` once per input type
  3. Build `containers.Map` lookup keyed by metadata combination string
  4. In the main loop, look up pre-loaded results instead of calling `load()`

### Edge cases
- Fixed inputs: Override specific metadata keys in the query
- Missing combinations: Skip iteration (same as current error handling)
- PathInput: Not a DB load, handled separately (no change)
- Constants: Not loaded (no change)

### Expected improvement: ~86s → ~1-2s (1 query per input type instead of 108)

## Total expected improvement: ~107s → ~7-10s (10-15x faster)
