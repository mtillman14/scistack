Plan: Fast-path for redundant save_batch() calls

Context

When re-running a pipeline that saves CSV data to the database, columns that have already been saved take just as long as the first save (~0.5s/column on Mac, ~3.5s/column on mini PC). The data is
not being duplicated (idempotency check at phase 6 works correctly), but the expensive per-row work (phases 2-5) runs unconditionally before discovering all records already exist.

For a 15,000-row CSV with ~60 columns, this means ~30s (Mac) or ~210s (mini PC) wasted on re-runs.

Root Cause

In save_batch() (src/scidb/database.py:544), the execution order is:

1.  Setup (fast)
2.  Split metadata for all rows (moderate)
3.  Batch resolve schema_ids (moderate)
4.  Resolve parameter_id (fast)
5.  Per-row hashing: canonical_hash() + generate_record_id() for ALL rows (expensive)
6.  Idempotency check: query which record_ids already exist (moderate)
7.  Batch inserts: skipped when all records exist (fast)

The bottleneck is phase 5 running unconditionally. The single-record save() method has an early exit (line ~1289), but save_batch() does not.

Approach

Add a fast-path check after phase 4 that detects full-batch re-saves and returns existing record_ids without computing hashes for all 15,000 rows.

Algorithm

After parameter resolution (phase 4), if the parameter already exists (is_new_parameter == False):

1.  Count existing records for this (variable_name, parameter_id) in \_record_metadata
2.  If count < len(data_items), skip fast path (new data being added)
3.  Compute input schema_ids (reuse already-computed all_nested + schema_id_cache from phases 2-3)
4.  If input schema_ids are not unique per row, skip fast path (ambiguous mapping)
5.  Query existing (schema_id, record_id, content_hash) from \_record_metadata for this variable/parameter
6.  Map each input row to an existing record via its schema_id
7.  If any input row's schema_id has no match, skip fast path
8.  Sample-verify content hashes for ~10 randomly selected rows (compute canonical_hash only for the sample, not all 15,000)
9.  If all samples match, return the existing record_ids in input order

Cost for the fast path (re-save case)

- 2 SQL queries (count + fetch existing records) instead of 15,000 hash computations
- ~10 hash computations for sample verification
- ~0.01s instead of ~0.4s

Cost for the slow path (new data)

- 2 additional SQL queries (negligible overhead)
- Falls through to the existing phases 5-7

File Changes

src/scidb/database.py — Add fast-path in save_batch()

Insert new code block between phase 4 (line 696) and phase 5 (line 698):

# --- Fast path: skip hashing if all records already exist ---

if not is_new_parameter: # 1. Count existing records
existing_count = self.\_duck.\_fetchall(
"SELECT COUNT(\*) FROM \_record_metadata "
"WHERE variable_name = ? AND parameter_id = ?",
[table_name, parameter_id],
)[0][0]

     if existing_count >= len(data_items):
         # 2. Compute input schema_ids from already-resolved data
         input_schema_ids = []
         for nested in all_nested:
             schema_keys = nested.get("schema", {})
             schema_level = self._infer_schema_level(schema_keys)
             if schema_level is not None and schema_keys:
                 key_tuple = tuple(
                     str(schema_keys.get(k, "")) for k in self.dataset_schema_keys
                     if k in schema_keys
                 )
                 input_schema_ids.append(schema_id_cache[(schema_level, key_tuple)])
             else:
                 input_schema_ids.append(0)

         # 3. Only use fast path if schema_ids are unique per row
         if len(set(input_schema_ids)) == len(input_schema_ids):
             # 4. Query existing records
             existing_records = self._duck._fetchall(
                 "SELECT schema_id, record_id, content_hash "
                 "FROM _record_metadata "
                 "WHERE variable_name = ? AND parameter_id = ? "
                 "ORDER BY schema_id, version_id DESC",
                 [table_name, parameter_id],
             )
             existing_by_schema = {}
             for sid, rid, chash in existing_records:
                 if sid not in existing_by_schema:
                     existing_by_schema[sid] = (rid, chash)

             # 5. Map each input row to existing record
             result_ids = []
             all_matched = True
             for sid in input_schema_ids:
                 if sid in existing_by_schema:
                     result_ids.append(existing_by_schema[sid][0])
                 else:
                     all_matched = False
                     break

             if all_matched:
                 # 6. Sample-verify content hashes
                 import random
                 sample_size = min(max(10, len(data_items) // 100), len(data_items))
                 sample_indices = random.sample(range(len(data_items)), sample_size)

                 sample_ok = True
                 for idx in sample_indices:
                     data_val = data_items[idx][0]
                     actual_hash = canonical_hash(data_val)
                     expected_hash = existing_by_schema[input_schema_ids[idx]][1]
                     if actual_hash != expected_hash:
                         sample_ok = False
                         break

                 if sample_ok:
                     if profile:
                         timings["fast_path"] = time.perf_counter() - t0
                         print(f"\n--- save_batch() FAST PATH "
                               f"({len(data_items)} items, all exist) ---")
                         for phase, elapsed in timings.items():
                             print(f"  {phase:30s} {elapsed:8.3f}s")
                         print()
                     return result_ids

No other files need to change - the optimization is entirely within save_batch().

Verification

1.  Run existing integration tests: cd /workspace && python -m pytest tests/ -x
2.  Run MATLAB bridge tests: cd /workspace && python -m pytest scidb-matlab/tests/ -x
3.  Run the benchmark example to confirm fast-path behavior: python examples/benchmark_batch_save.py
4.  Manual test: save_batch the same data twice with profile=True and verify the second call prints "FAST PATH" and completes much faster
