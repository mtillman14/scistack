# Plan: Add comprehensive logging to scihist.for_each execution path

## Changes Made

### 1. Logging additions (4 files)

**`scihist-lib/src/scihist/foreach.py`**
- Removed `_diag()` function and all 12 `_diag()` calls
- Replaced with `logger.debug()` using same messages (minus `[DIAG]` prefix)
- Added `logger.info` at: auto-wrap, delegation result, save entry
- Added `logger.debug` at: already-a-LineageFcn, skip hook built/skipped
  (with reason), delegation entry, input classification summary

**`scilineage/src/scilineage/core.py`**
- Added `import logging` + `logger`
- Added `logger.debug` at: `LineageFcn.__init__`, `__call__` cache
  lookup/hit/miss, `LineageFcnInvocation.__call__` before/after,
  `compute_lineage_hash`

**`scilineage/src/scilineage/inputs.py`**
- Added `import logging`, `from collections import Counter`, `logger`
- Added `logger.debug` at each `classify_input` return (6 branches) and
  summary in `classify_inputs`

**`scilineage/src/scilineage/lineage.py`**
- Added `import logging` + `logger`
- Added `logger.debug` in `extract_lineage` after classification

### 2. Bug fix: fn hash staleness for Python functions

**`scihist-lib/src/scihist/state.py`** — `_check_via_lineage()`

Previously, the function's own hash mismatch was deliberately ignored
(logged for traceability only). This was done as a blanket workaround for
MATLAB proxy hash recipe false positives.

Fix: re-enabled fn hash staleness **only for Python `LineageFcn`**
instances (via `isinstance` check). MATLAB proxies still get the
traceability-only log. This lets Python function code changes trigger
proper staleness detection while avoiding the MATLAB false-positive issue.

**`scihist-lib/tests/test_state.py`**

Updated `test_function_hash_change_not_stale_via_lineage` →
`test_function_hash_change_stale_via_lineage` to assert `"stale"`
instead of `"up_to_date"`, matching the new behavior.
