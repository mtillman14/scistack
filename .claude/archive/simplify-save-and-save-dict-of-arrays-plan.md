Plan: Simplify Save Pathways & Fix Dict-of-Arrays Performance

Context

MATLAB scalar structs (Python dicts with N x 1 numpy arrays per field) are being stored as JSON blobs via \_python_to_storage(). For a struct with 10 fields of 40K elements each, this creates an
11MB JSON string per save, taking ~0.35s for serialization + ~1.2s for DuckDB to commit. Over 28 saves, this means ~317MB of JSON written to disk, causing memory pressure and ~1.6s per save.

The root cause is that \_infer_duckdb_type() classifies all dicts as JSON, and the save path has no way to route dict-of-arrays to efficient columnar storage. Additionally, the save pathways have
unnecessary duplication — \_save_native_direct and \_save_native_with_schema are nearly identical, differing only in schema_id resolution.

Goal: Reduce save paths from 3 to 2, auto-detect dict-of-arrays for columnar storage, and ensure save_batch handles this case too.

Current State: 3 Save Paths

save()
├─ custom to_db() OR DataFrame → \_save_custom_to_sciduck() [columnar, efficient]
├─ native + schema keys → \_save_native_with_schema() [single "value" col]
└─ native, no schema → \_save_native_direct() [single "value" col, schema_id=0]

\_save_native_with_schema and \_save_native_direct are ~95% identical code.
Dict-of-arrays always hits the native paths → JSON serialization → slow.

Proposed State: 2 Save Paths

save()
├─ custom to_db() OR DataFrame OR dict-of-arrays → \_save_columnar() [multi-column]
└─ everything else (scalars, arrays, lists, dicts) → \_save_single_value() [single "value" col]

Changes

1.  Add \_is_tabular_dict() helper — database.py

New module-level function:

def \_is_tabular_dict(data):
"""Return True if data is a dict where ALL values are 1D numpy arrays of equal length."""
if not isinstance(data, dict) or len(data) == 0:
return False
lengths = set()
for v in data.values():
if not (isinstance(v, np.ndarray) and v.ndim == 1):
return False
lengths.add(len(v))
return len(lengths) == 1

2.  Rename \_save_custom_to_sciduck() → \_save_columnar() — database.py:346-441

- Rename only (no logic change)
- Update all call sites (2 in save(), potentially in save_batch)

3.  Merge \_save_native_direct() + \_save_native_with_schema() → \_save_single_value() — database.py:443-574

Merge into one function with signature:
def \_save_single_value(self, table_name, variable_class, data, content_hash,
schema_level=None, schema_keys=None, version_keys=None):

- If schema_level is not None and schema_keys: resolve schema_id via \_get_or_create_schema_id
- Otherwise: schema_id = 0
- Rest of logic identical (already is)
- Delete old \_save_native_direct and \_save_native_with_schema

4.  Update save() decision tree — database.py:1446-1513

New flow:
if self.\_has_custom_serialization(type(variable)):
df = variable.to_db() # ... index handling ...
\_save_columnar(table_name, type(variable), df, schema_level, schema_keys,
content_hash, version_keys=version_keys)

elif isinstance(variable.data, pd.DataFrame):
df = variable.data.copy() # ... index handling ...
\_save_columnar(table_name, type(variable), df, schema_level, schema_keys,
content_hash, version_keys=version_keys)

elif \_is_tabular_dict(variable.data):
df = pd.DataFrame(variable.data)
\_save_columnar(table_name, type(variable), df, schema_level, schema_keys,
content_hash, version_keys=version_keys,
dict_of_arrays=True, ndarray_keys={
k: {"dtype": str(v.dtype), "shape": list(v.shape)}
for k, v in variable.data.items()
})

else:
\_save_single_value(table_name, type(variable), variable.data, content_hash,
schema_level=schema_level, schema_keys=schema_keys,
version_keys=version_keys)

5.  Update \_save_columnar() for dict-of-arrays metadata — database.py

Add optional dict_of_arrays and ndarray_keys params to \_save_columnar(). When dict_of_arrays=True, store dtype metadata as:
{"custom": True, "dict_of_arrays": True, "ndarray_keys": {...}}

This reuses the existing custom=True flag so the load path's columnar query works unchanged. The dict_of_arrays flag tells the load side to convert back to a dict.

6.  Update \_load_by_record_row() — database.py:1103-1188

In the is_custom branch (line 1142), after loading the DataFrame and before returning:

if dtype_meta.get("dict_of_arrays"):
ndarray_keys = dtype_meta.get("ndarray_keys", {})
data = {}
for col in df.columns:
arr = df[col].values
if col in ndarray_keys:
arr = arr.astype(np.dtype(ndarray_keys[col]["dtype"]))
data[col] = arr
elif self.\_has_custom_serialization(variable_class):
data = variable_class.from_db(df)
else:
data = df

7.  Update save_batch() for dict-of-arrays — database.py:576-954

Currently save_batch assumes single-column storage. Add dict-of-arrays detection:

- After inferring type from first item (line 612), check \_is_tabular_dict(first_data)
- If True: create multi-column table (columns = dict keys + tracking cols)
- In the per-row loop (line 829), expand each dict to DataFrame rows with tracking cols
- Batch insert uses the multi-row-per-item approach
- Store dtype = {"custom": True, "dict_of_arrays": True, "ndarray_keys": {...}}

8.  Remove timing diagnostics — database.py

Remove all print(f" [save]...") and print(f" [\_save_native...") timing statements added during diagnosis. Keep the transaction wrapping (BEGIN/COMMIT).

Files Modified
┌────────────────────────────────┬────────────────────────────────────────────────────────────────┐
│ File │ Changes │
├────────────────────────────────┼────────────────────────────────────────────────────────────────┤
│ src/scidb/database.py │ All save path changes, load changes, timing cleanup │
├────────────────────────────────┼────────────────────────────────────────────────────────────────┤
│ sciduck/src/sciduck/sciduck.py │ No changes needed (type inference stays for single-value path) │
└────────────────────────────────┴────────────────────────────────────────────────────────────────┘
Verification

1.  Run existing test suite: python -m pytest tests/ -x
2.  Run sciduck tests: python -m pytest sciduck/tests/ -x
3.  Run pipelinedb tests: python -m pytest pipelinedb-lib/tests/ -x
4.  Manual test: save a dict-of-arrays and verify it round-trips correctly:
    data = {"a": np.array([1.0, 2.0, 3.0]), "b": np.array([4.0, 5.0, 6.0])}
    MyVar.save(data, subject=1)
    loaded = MyVar.load(subject=1)
    assert isinstance(loaded.data, dict)
    assert np.array_equal(loaded.data["a"], data["a"])
5.  Verify MATLAB struct saves now show ~0.01s for data write instead of ~0.5s

Expected Performance Impact

- Dict-of-arrays save: ~1.6s → ~0.05s (11MB JSON → ~3.2MB columnar float64)
- Memory pressure: eliminated (no 400K temporary Python float objects per save)
- Code paths: 3 → 2 (simpler to maintain and reason about)
