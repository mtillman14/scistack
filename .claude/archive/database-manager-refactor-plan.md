Plan: Refactor DatabaseManager to Delegate Data Storage to SciDuck

Context

DatabaseManager currently bypasses SciDuck's save/load entirely, creating its own table schema with columns like \_record_id, \_content_hash, \_lineage_hash, \_metadata, \_user_id, \_created_at,
\_dtype_meta, \_schema_version, + schema key columns + data columns. Meanwhile SciDuck creates tables with schema_id, version_id, + data columns and maintains \_schema, \_variables, \_variable_groups
metadata tables. This duplication causes SciDuck's metadata tables to be empty and logic to be duplicated.

Goal: DatabaseManager delegates core data storage to SciDuck (populating \_schema and \_variables automatically). DatabaseManager keeps record-level metadata in its own \_record_metadata side table.
SciDuck is NOT modified.

File to modify

/workspace/src/scidb/database.py — only this file changes.

Steps

1.  Add \_record_metadata table creation

Add \_ensure_record_metadata_table() called from **init**():

CREATE TABLE IF NOT EXISTS \_record_metadata (
record_id VARCHAR PRIMARY KEY,
variable_name VARCHAR NOT NULL,
version_id INTEGER NOT NULL,
schema_id INTEGER NOT NULL,
content_hash VARCHAR,
lineage_hash VARCHAR,
schema_version INTEGER,
version_keys VARCHAR, -- JSON of non-schema metadata
metadata VARCHAR, -- JSON of full nested {schema, version}
user_id VARCHAR,
created_at VARCHAR NOT NULL
)

2.  Add \_infer_schema_level(schema_keys: dict) -> str

Walk self.dataset_schema_keys top-down; return deepest key where all keys from top are contiguously present. E.g. schema=["subject","session","trial"], keys={subject:1} → "subject". Defaults to
deepest level if no keys provided.

3.  Add \_save_record_metadata() helper

Insert into \_record_metadata with ON CONFLICT (record_id) DO NOTHING for idempotency.

4.  Modify save() — Native path (no custom to_db/from_db)

Replace \_save_native_row() call with:

1.  Check if record_id already exists in \_record_metadata → early return (idempotent)
2.  version_id = self.\_duck.save(table_name, data, schema_level=level, force=True, \*\*schema_keys) — delegates data table creation and \_schema/\_variables registration to SciDuck
3.  Get schema_id via self.\_duck.\_get_or_create_schema_id(level, schema_keys)
4.  Insert into \_record_metadata

Using force=True ensures each save gets a unique version_id (SciDuck normally deduplicates by hash).

5.  Modify save() — Custom path (has to_db/from_db)

Replace \_save_dataframe() call with \_save_custom_to_sciduck():

1.  Check \_record_metadata for existing record_id → early return
2.  Get schema_id and version_id from SciDuck internal APIs
3.  Create table in SciDuck's format: schema_id INTEGER, version_id INTEGER, [data_cols]
4.  Insert all DataFrame rows with same (schema_id, version_id) — all rows represent one variable instance
5.  Register in \_variables with dtype = {"custom": True}
6.  Insert into \_record_metadata

Can't use SciDuck.save() directly because it treats each DataFrame row as a separate schema entry, but custom to_db() produces multiple rows belonging to ONE entry.

6.  Add \_find_record() — Query \_record_metadata

Supports two modes:

- By record_id: direct primary key lookup
- By metadata: JOIN \_record_metadata with \_schema on schema_id, filter by schema key columns, optionally filter by version_keys JSON, order by created_at DESC

Returns a DataFrame of matching rows (or empty).

7.  Modify load() — Use \_find_record() then SciDuck

Replace current direct-table query with:

1.  \_find_record() to get version_id, schema_id, and metadata
2.  Check \_variables.dtype for {"custom": True} to decide deserialization path:

- Native: self.\_duck.load(table_name, version_id=vid, raw=True, \*\*schema_keys) — SciDuck handles type restoration
- Custom: Direct query WHERE version_id = ? AND schema_id = ?, drop schema_id/version_id columns, pass to from_db()

3.  Construct BaseVariable instance with metadata from \_record_metadata

4.  Add \_load_by_record_row() — Shared load logic

Extract common load logic used by both load() and load_all() into a private helper to avoid duplication.

9.  Modify load_all() — Query \_record_metadata

Query \_record_metadata for all matching records (not just latest), then yield each via \_load_by_record_row().

10. Modify list_versions() — Query \_record_metadata

Query \_record_metadata joined with \_schema instead of querying data table columns.

11. Remove deprecated methods

- \_save_native_row() — replaced by SciDuck.save()
- \_ensure_native_table() — SciDuck creates tables
- \_register_variable_version() — SciDuck.save() handles for native, manual for custom
- \_save_dataframe() — replaced by \_save_custom_to_sciduck()
- \_load_dataframe() — replaced by \_find_record() + SciDuck.load()

12. Simplify \_df_to_variable() → thin factory

Now just constructs a BaseVariable from pre-deserialized data + pre-extracted metadata. No more mixed data/metadata column parsing.

Expected test breakage (tests NOT changed)
┌───────────────────────────────────────────────────────────────────────────────────────┬──────┬─────────────────────────────────────────────────────────────────────┐
│ Test │ Line │ Reason │
├───────────────────────────────────────────────────────────────────────────────────────┼──────┼─────────────────────────────────────────────────────────────────────┤
│ test_integration.py::TestIdempotentSaves::test_same_data_same_metadata_same_record_id │ 144 │ Queries \_record_id column from data table — column no longer exists │
└───────────────────────────────────────────────────────────────────────────────────────┴──────┴─────────────────────────────────────────────────────────────────────┘
All other tests should continue to pass. The lineage tests query PipelineDB (SQLite), not DuckDB data tables. The generates_file path exits early in save_variable() without touching data storage.

Verification

1.  Run pytest tests/test_integration.py -v — expect 1 failure (line 144)
2.  Run pytest tests/test_lineage_mode.py -v — expect all pass
3.  Run pytest tests/test_generates_file.py -v — expect all pass
4.  Inspect DuckDB with db.\_duck.\_fetchall("SELECT _ FROM \_schema") and db.\_duck.\_fetchall("SELECT _ FROM \_variables") — both should be populated
5.  Inspect db.\_duck.\_fetchall("SELECT \* FROM \_record_metadata") — should contain all saved records
