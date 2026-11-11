Plan: Bulk Save Path for save_from_dataframe()

Context

Saving 15,000 rows × 80 columns of scalar features is infeasible with the current approach. Each save_from_dataframe() row calls BaseVariable.save(), which executes ~6 SQL queries + 2 INSERTs per
row, all auto-committed. For one column that's ~120,000 DuckDB operations; across 80 columns it's ~9.6M operations. Measured: ~45s per 2,000 rows for one column.

Goal: Replace the per-row loop in save_from_dataframe() with a bulk path that amortizes setup work and batches SQL operations, targeting ~100x speedup.

Approach

Add a DatabaseManager.save_batch() method that does one-time setup, pre-computes everything in Python, then uses executemany for bulk inserts. save_from_dataframe() calls this instead of looping
save().

Changes

1.  src/scidb/database.py — Add save_batch() method

New method DatabaseManager.save_batch(variable_class, data_items, version_keys=None) where data_items is a list of (data_value, metadata_dict) tuples.

One-time setup (amortized across all rows):

- \_ensure_registered(variable_class) — once
- \_split_metadata() on the first item to determine schema vs version key pattern — once
- \_infer_duckdb_type() on the first data value — once
- \_table_exists() + CREATE TABLE if needed — once
- \_resolve_parameter_slot() — once (all rows share the same variable type + version_keys)
- Register in \_variables if new parameter — once

Per-row Python computation (no SQL):

- canonical_hash(data) — pure Python, fast for scalars
- generate_record_id(...) — pure Python
- \_python_to_storage(data, col_meta) — pure Python
- \_get_or_create_schema_id() — batch this (see step 2 below)

Batch schema_id resolution:

- Collect all unique schema key combinations from the data_items
- Query \_schema table once with SELECT schema_id, <key_cols> FROM \_schema WHERE schema_level = ?
- Build a lookup dict {(key_tuple): schema_id}
- For any missing combos, batch-INSERT into \_schema using executemany, then re-query to get assigned IDs

Batch idempotency check:

- Collect all computed record_id values
- Query SELECT record_id FROM \_record_metadata WHERE record_id IN (?, ?, ...) once
- Filter out already-existing records from the batch

Batch inserts (using executemany inside a single transaction):
BEGIN TRANSACTION
executemany: INSERT INTO "<data_table>" (schema_id, parameter_id, version_id, "value") VALUES (?, ?, ?, ?)
executemany: INSERT INTO \_record_metadata (...) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING
COMMIT

version_id handling: Since all rows in the batch share the same parameter_id and version_keys, they all get the same version_id (the next version for this parameter slot). Each row has a different
schema_id (different metadata), so they're distinct records at the same version.

2.  src/scidb/variable.py — Update save_from_dataframe() to use bulk path

Change save_from_dataframe() to:

1.  Build the data_items list: [(row[data_column], {col: row[col] for col in metadata_columns, \*\*common_metadata}) for each row]
2.  Call db.save_batch(cls, data_items) instead of looping cls.save()
3.  Return the list of record_ids from save_batch()

4.  sciduck/src/sciduck/sciduck.py — Add \_executemany() helper

Add a thin wrapper matching the existing \_execute() pattern:
def \_executemany(self, sql, params_list):
return self.con.executemany(sql, params_list)

4.  sciduck/src/sciduck/sciduck.py — Add \_begin() and \_commit() transaction helpers

def \_begin(self):
self.con.execute("BEGIN TRANSACTION")

def \_commit(self):
self.con.execute("COMMIT")

Files Modified
┌────────────────────────────────┬────────────────────────────────────────────────────────┐
│ File │ Change │
├────────────────────────────────┼────────────────────────────────────────────────────────┤
│ src/scidb/database.py │ Add save_batch() method (~80 lines) │
├────────────────────────────────┼────────────────────────────────────────────────────────┤
│ src/scidb/variable.py │ Rewrite save_from_dataframe() body (~10 lines changed) │
├────────────────────────────────┼────────────────────────────────────────────────────────┤
│ sciduck/src/sciduck/sciduck.py │ Add \_executemany(), \_begin(), \_commit() (~10 lines) │
└────────────────────────────────┴────────────────────────────────────────────────────────┘
What stays unchanged

- BaseVariable.save() — single-save path untouched
- DatabaseManager.save() — untouched
- DatabaseManager.save_variable() — untouched
- All lineage/thunk code — not relevant for raw data batch saves
- canonical_hash, generate_record_id — called from Python, no changes

Verification

1.  Run existing integration tests: pytest tests/test_integration.py — ensure nothing breaks
2.  Run sciduck tests: pytest sciduck/tests/
3.  Manual test: create a DataFrame with 1,000+ rows and verify save_from_dataframe() completes in <1s
4.  Verify saved data loads correctly via load() and load_all()
5.  Verify idempotency: calling save_from_dataframe() twice with same data doesn't create duplicates
