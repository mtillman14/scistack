Optimize load_all bulk path (13.5s → ~0.5s)

Context

load_all() in database.py takes 13.5s for 15,613 scalar records. Profiling shows 4 bottlenecks:
┌──────────────────────────────┬──────┬───────────────────────────────────────────────────────┐
│ Step │ Time │ Root Cause │
├──────────────────────────────┼──────┼───────────────────────────────────────────────────────┤
│ Build key lists │ 1.7s │ iterrows() on 15K-row DataFrame │
├──────────────────────────────┼──────┼───────────────────────────────────────────────────────┤
│ Bulk data fetch │ 9.1s │ 15,613 OR conditions (pid=? AND vid=? AND sid=?) │
├──────────────────────────────┼──────┼───────────────────────────────────────────────────────┤
│ Restore types + build lookup │ 1.5s │ Per-row restored.loc[idx] creates 15K Series objects │
├──────────────────────────────┼──────┼───────────────────────────────────────────────────────┤
│ Construct instances │ 1.2s │ iterrows() + per-row \_reconstruct_metadata_from_row() │
└──────────────────────────────┴──────┴───────────────────────────────────────────────────────┘
The \_find_record query itself is 0.05s — DuckDB is fast. The problem is entirely in how we use the results.

Plan — 4 changes, all in database.py load_all() (line ~1584)

1.  Vectorize key extraction (1.7s → ~0.01s)

Before: 3x iterrows() to extract pid/vid/sid lists
After: Direct .values access on DataFrame columns + set(zip(...)) for unique keys

pids = records["parameter_id"].values.astype(int)
vids = records["version_id"].values.astype(int)
sids = records["schema_id"].values.astype(int)
unique_keys = set(zip(pids.tolist(), vids.tolist(), sids.tolist()))

2.  Replace 15K OR conditions with efficient IN clause (9.1s → ~0.1s)

Before: One (parameter_id = ? AND version_id = ? AND schema_id = ?) per row
After: Use per-dimension IN clauses when the cross product is exact (common case), or tuple-IN VALUES fallback

For the user's case (1 pid, 1 vid, 15K sids), the cross product 1 × 1 × 15613 = 15613 matches unique key count, so:
WHERE parameter_id IN (1) AND version_id IN (1) AND schema_id IN (1, 2, ..., 15613)

Fallback for cases with partial cross products (multiple pids × multiple vids where not all combos exist):
WHERE (parameter_id, version_id, schema_id) IN (VALUES (1,1,1), (1,1,2), ...)

3.  Vectorize data lookup building (1.5s → ~0.2s)

Before: Per-row restored.loc[idx] → creates a pandas Series per row (expensive)
After: For single-column mode (common), use .tolist() to extract all values at once, store raw values in lookup dict

# Single column mode: extract all values as a list

col_values = restored[col_name].tolist()
for i, idx in enumerate(pid_df.index):
key = (int(pids_arr[idx]), int(vids_arr[idx]), int(sids_arr[idx]))
data_lookup[key] = col_values[i]

For multi-column mode: use per-column .tolist() and build dicts.

4.  Vectorize instance construction (1.2s → ~0.2s)

Before: iterrows() + \_reconstruct_metadata_from_row() per row
After: itertuples() (10-100x faster) + inline metadata reconstruction

for row in records.itertuples(index=False): # Direct attribute access on named tuple (fast)
flat_metadata = {}
for key in self.dataset_schema_keys:
val = getattr(row, key, None)
if val is not None and not (isinstance(val, float) and pd.isna(val)):
flat_metadata[key] = str(val)
vk_raw = getattr(row, 'version_keys', None)
if vk_raw is not None and isinstance(vk_raw, str):
flat_metadata.update(json.loads(vk_raw)) # ... construct instance

Files to modify

- /workspace/src/scidb/database.py — load_all() method (lines ~1584-1730)

Verification

- Run the same preload that produced the 13.5s timing
- The timing output should show ~0.5s total instead of 13.5s
- Existing tests in src/tests/ should pass
- Remove timing instrumentation after confirming (or keep as opt-in debug)
