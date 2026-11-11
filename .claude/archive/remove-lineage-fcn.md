# Plan: remove `@lineage_fcn` / `LineageFcnResult` entirely

Branch: `dev-hist`. User decision: delete the scilineage lineage-wrapper system
(`@lineage_fcn`, `LineageFcn`, `LineageFcnResult`, `LineageFcnInvocation`,
`manual`, tuple-unpacking wrapper) from the Python **and** MATLAB codebases. Do
this BEFORE finishing the `version_keys` migration — it removes the manual
lineage-save path, which is the source of the "non-schema kwargs on produced
records" gap that blocked slice 1.

## Why this helps the migration

for_each already builds the full graph from its OWN bindings (`save_metadata`:
`__upstream` input record_ids + `__constants` + `__fn`/`__fn_hash`) for plain
functions (the batch path → `record_run`). The LineageFcnResult sequential path
(`record_run_from_lineage`) is redundant inside for_each. Once the manual
`r = fn(x); save(r)` path is gone, non-schema kwargs only appear on raw direct
saves (P0 already anchors those) → `version_keys` becomes cleanly removable.

## Blast radius (verified)

Source: `scilineage/core.py` (definitions), `scidb/foreach.py` (21 refs — lineage
output path), `scidb/lineage_save.py` (manual save path), `scidb/database.py`
(save lineage branch, find_by_lineage), `scidb/discover.py` (GUI marker),
`scimatlab/bridge.py` (MatlabLineageFcn + check_cache). Public API:
`scilineage/__init__`, `scidb/__init__`, `scihist/__init__` export these.
Tests: 17 files use `lineage_fcn`; `LineageFcnResult` in 6. Some test files are
ENTIRELY about the feature (`scilineage/tests/test_core.py`, `test_lineage.py`,
`scihist/tests/test_cache_hit.py`, lineage parts of `scimatlab/tests/test_bridge.py`)
→ delete; the rest use `@lineage_fcn` only to DEFINE pipeline functions → rewrite
to plain functions.

## Decisions (user-approved 2026-06-19)
1. **Discovery** → lightweight `@pipeline` tag decorator (no-op at call time; tags
   a plain function so `discover.py` finds it; also carries `unpack_output` /
   `generates_file` metadata for for_each).
2. **Rerun cache** → REMOVE entirely (`find_by_lineage` + MATLAB `check_cache` +
   `test_cache_hit`). for_each `skip_computed` (graph-based) stays.
3. **generates_file** → KEEP, reimplemented graph-native (no LineageFcnResult).
4. **MATLAB handle** → plain `(function_name, function_hash)` object.

## Four sub-features that need a decision (see questions)

1. **GUI discovery marker.** `discover.py` finds pipeline functions via
   `isinstance(obj, LineageFcn)`. Plain functions aren't discoverable today.
   Need a replacement.
2. **Rerun cache.** `find_by_lineage` (public API) + MATLAB `check_cache` skip
   recomputation when a result already exists. Independent of for_each's
   `skip_computed` (which is graph-based and stays). Keep or drop?
3. **MATLAB function handle.** `MatlabLineageFcn` is the fn-handle proxy passed to
   `check_node_state` and used by `check_cache`. Needs a non-LineageFcn
   representation.
4. **`generates_file`.** Side-effect outputs tracked via the lineage save path
   (`scidb/lineage_save.py`, `test_generates_file.py`). Need a graph-native
   equivalent or removal.

## Defaults I'll take unless told otherwise
- scilineage package SURVIVES as a hashing/inputs utility only
  (`compute_function_hash`, `canonical_hash`, input classification) — those are
  used everywhere (graph function_hash). Only the lineage-wrapper classes are
  deleted.
- MATLAB fn handle → a plain `(function_name, function_hash)` object (node-state
  already keys on those; no LineageFcn base needed).

## STATUS (2026-06-19)

**Done & user-verified green (test layer + @pipeline foundation):**
- `scidb/pipeline.py`: `@pipeline` marker (+ `is_pipeline_function`), exported from scidb.
- `discover.py`: recognizes `@pipeline` (alongside legacy `LineageFcn`).
- `foreach.py` auto-wrap honors `@pipeline` `unpack_output`/`generates_file` attrs.
- Test conversions `@lineage_fcn`→`@pipeline`: test_state, test_state_realworld,
  test_state_pathinput, test_skip_computed, test_unified_variant_tracking,
  test_pipeline_visibility, test_schema_filter_params, test_state_workflows,
  test_discover (fixtures + `.fcn.__name__`→`.__name__`).
- test_integration: deleted `TestLineageFcnPipelineMetadata` (manual-save path).
- test_generates_file: rewritten to for_each-only `TestForEachIntegration`.
- DELETED: test_cache_hit.py, test_optional_lineage_dependency.py.
- Key enabling facts confirmed: scifor spreads tuple returns natively; for_each
  builds the graph from bindings (not from LineageFcnResult); `compute_function_hash`
  unwraps + works on plain fns; `unpack_output` is a no-op inside for_each.

**REMAINING = 3 coupled source clusters (large; partly unverifiable here):**

1. **MATLAB bridge + cache** (slice E). Entangled & PARTLY UNVERIFIABLE (no MATLAB
   in this env). `scimatlab/bridge.py`: `MatlabLineageFcn` duck-types `LineageFcn`,
   `MatlabLineageFcnInvocation` duck-types `LineageFcnInvocation`,
   `make_lineage_fcn_result` builds a REAL `LineageFcnResult`, `check_cache` calls
   `find_by_lineage`. MATLAB `+scidb/LineageFcn.m` calls `check_cache` +
   `make_lineage_fcn_result`. test_bridge.py tests all of these. Cache
   (`find_by_lineage`/`find_by_lineage_hash` in database.py + scidb/scihist exports)
   is reachable ONLY via `check_cache`, so it removes WITH this slice. Decision:
   handle→plain `(name,hash)`; cache→removed. Risk: MATLAB `.m` edits can't be run.

2. **for_each plain-native + generates_file graph-native + manual-save removal**
   (slices A+D+G, coupled). Removing the auto-wrap (foreach.py:403) + unpacking
   wrapper makes plain fns flow through with NO LineageFcnResult — which forces:
   (a) deleting the LineageFcnResult batch-detection (foreach.py:2953, 3070), and
   (b) reimplementing `generates_file` WITHOUT LineageFcnResult (it currently rides
   the `gf_items`→`save_lineage_result`→`record_run_from_lineage` path). Then the
   manual save path (`lineage_save._save_lineage_fcn_result`, `save()` LineageFcnResult
   branch, `database.save(lineage=...)` branch) can go. NOTE: `record_run_from_lineage`
   itself is the graph writer; generates_file's graph-native version can reuse it
   (it's not LineageFcn-specific). Verifiable via Python tests.

3. **scilineage class deletion** (slice F). Delete `LineageFcn`/`LineageFcnResult`/
   `LineageFcnInvocation`/`manual`/`make_tuple_unpacking_wrapper` from core.py +
   `lineage.py`; trim `scilineage/__init__` to hashing/inputs (KEEP
   `compute_function_hash`, `canonical_hash`, classify_input). Delete
   scilineage/tests/test_core.py + test_lineage.py (KEEP test_hashing.py). Drop
   `manual` import/export from scidb. Must come AFTER 1 & 2 (they still import the
   classes). Verifiable via Python tests.

Recommended order: **2 → 1 → 3** (cluster 2 is fully Python-verifiable and removes
the most; cluster 1's MATLAB .m needs the user's MATLAB env to verify; cluster 3
is the final cleanup once nothing imports the classes).

## Phasing (each slice independently green-able; user runs tests)
- **A. for_each plain-only.** Delete the LineageFcnResult detection/sequential
  path in `foreach.py`; for_each builds the graph from bindings for all functions.
  Rewrite source/tests that pass `@lineage_fcn` to use plain functions.
- **B. Manual save path + cache.** Remove `lineage_save._save_lineage_fcn_result`
  / `save_lineage_result`, the `save(lineage=...)` branch in `database.save`, and
  (per decision) `find_by_lineage`. Delete `test_cache_hit.py`.
- **C. Discovery.** Replace the `LineageFcn` marker per decision.
- **D. generates_file.** Per decision.
- **E. MATLAB.** Replace `MatlabLineageFcn`/`check_cache`/`make_lineage_fcn_result`
  with the plain handle; rewrite `scimatlab/tests/test_bridge.py`.
- **F. scilineage.** Delete `core.py` lineage classes + `lineage.py`; trim
  `__init__` to hashing/inputs; delete `scilineage/tests/test_core.py`,
  `test_lineage.py`. Drop exports from `scidb`/`scihist` `__init__`.
- **G. Resume version_keys migration** (slice 1 now stands; continue P1/P2).

## Risk
Largest change in this effort; touches the whole test suite, GUI discovery, and
MATLAB. Cannot run pytest here — land one phase at a time, user verifies. Keep
`compute_function_hash` (do NOT remove — it's the graph's function identity).
