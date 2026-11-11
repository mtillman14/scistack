# Comprehensive Logging for for_each Pipeline — Implementation Record

## Status: IMPLEMENTED

## Files Modified

1. **`scifor/src/scifor/foreach.py`**
   - Added `import time`
   - Added `_describe_result()` helper for compact value descriptions
   - Wrapped `_call_fn` with `time.perf_counter()` timing
   - Added `[done]` log line with metadata, function name, and elapsed time

2. **`scidb/src/scidb/foreach.py`**
   - Added `_describe_save_data()` helper
   - Updated both save paths (flatten-mode and standard-mode) in `_save_results()`:
     - Captures returned `record_id` from `.save()`
     - Logs `[save]` with record_id (first 12 chars), data description, and timing
   - Added overall save timing around `_save_results()` call in `for_each()`

3. **`scidb/src/scidb/database.py`**
   - `save_variable()`: INFO entry/exit with metadata summary and record_id
   - `save()`: DEBUG after record_id generation (hash, serialization type); DEBUG on commit; ERROR on rollback
   - `_save_columnar()`: DEBUG on table creation and insert/skip
   - `_save_native()`: DEBUG on table creation, insert (dataframe/scalar), or skip
   - `_save_record_metadata()`: DEBUG with variable_name, record_id, schema_id
   - `_save_lineage()`: DEBUG with output_type, function_name, lineage_hash
   - `load()`: INFO entry with metadata summary; INFO on success with record_id; WARN on not-found
   - `load_all()`: INFO entry with metadata summary; INFO with record count; INFO on empty result
   - `_find_record()`: DEBUG with match count

4. **`scihist-lib/src/scihist/foreach.py`**
   - Added `from scidb.log import Log` import (with try/except for standalone use)
   - Added `import time`
   - `for_each()`: Log.info at start with skip_computed status
   - `_should_skip()`: All `[skip]` and `[recompute]` messages now also emit via Log.info
   - `_save_with_lineage()`: Timing around saves; Log.info with record_id and elapsed time; Log.error on failure
   - `_save_lineage_fcn_result()`: Log.info after successful save with record_id; Log.debug with lineage hash details

5. **`sci-matlab/src/sci_matlab/matlab/+scidb/for_each.m`**
   - Overall save_results timing (tic/toc)
   - LineageFcnResult save: timing and `[save]` log with elapsed time
   - Batch flush: timing and enhanced `[save]` log with elapsed time
   - Parallel batch save: timing and enhanced `[save]` log with elapsed time

## Phase 2: DEBUG Level + Bridge + RPC Tracing (IMPLEMENTED)

6. **`scidb/src/scidb/log.py`**
   - Changed default `_level` from `INFO` to `DEBUG`
   - Added `bridge_python_logging()` classmethod with inner `_ScidbLogHandler` class
   - Handler maps Python `logging.DEBUG/INFO/WARNING/ERROR/CRITICAL` → scidb `DEBUG/INFO/WARN/ERROR`
   - Bridges `scistack_gui` and `scihist` logger hierarchies → `scidb.log`
   - Idempotent (safe to call multiple times)

7. **`scistack-gui/scistack_gui/server.py`**
   - Added `_summarize_params()` helper for compact RPC param display (max 120 chars)
   - Added `RPC >> method(params)` entry logging and `RPC << method OK/FAILED (Xms)` exit logging with timing in `_handle_request()`
   - Called `Log.bridge_python_logging()` after `init_db()` in `main()`

8. **`scistack-gui/scistack_gui/api/pipeline.py`**
   - Added `logging.getLogger(__name__)` and `import time`
   - `_own_state_for_function()`: logs each function's state with up_to_date/stale/missing counts
   - `_compute_run_states()`: logs summary with timing and green/grey/red counts
   - `_build_graph()`: logs graph statistics (node count by type, edge count)

9. **`scistack-gui/scistack_gui/__main__.py`**
   - Called `Log.bridge_python_logging()` after `init_db()` (FastAPI mode)

## Log Level Guidelines
- **INFO**: Major milestones — load/save success/failure, timing, iteration progress, skip/recompute decisions
- **DEBUG**: Internal details — record_ids, content hashes, serialization paths, lineage hashes, RPC tracing, state computation
- **WARN**: Missing data, not-found conditions
- **ERROR**: Save failures, transaction rollbacks, RPC failures
