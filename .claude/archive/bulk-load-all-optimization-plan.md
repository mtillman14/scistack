Plan: Bulk load_all() Optimization

Context

for_each() pre-loads all input data via bridge.py:load_and_extract(), which calls DatabaseManager.load_all(). For 8328 records, this takes 81.78s (~9.8ms/record).

Root cause: load_all() calls \_load_by_record_row() per record, which executes 3 separate SQL queries per record:

1.  SELECT dtype FROM \_variables — dtype lookup (~7-8ms) at database.py:1098
2.  Inside SciDuck.load(): another SELECT schema_level, dtype FROM \_variables (~2ms) at sciduck.py:784
3.  Inside SciDuck.load(): data fetch from the data table (~1ms) at sciduck.py:820

Total: ~25,000 queries for 8328 records → 81.78s

Approach

Replace per-record SQL with 3 total queries:

1.  \_find_record() — 1 query for all metadata rows (already exists, fast)
2.  dtype lookup — 1 query per unique parameter_id (typically 1)
3.  Data fetch — 1 query for ALL data rows from the data table

Then match data to metadata rows in-memory and construct BaseVariable instances.

Expected result: 81.78s → ~1-3s

Changes

File: /workspace/src/scidb/database.py

Modify load_all() (lines 1563-1593) — replace the per-record \_load_by_record_row loop with bulk loading:

1.  Call \_find_record() as-is (returns DataFrame of all matching metadata rows)
2.  Determine if variable_class uses custom serialization (\_has_custom_serialization)
3.  Bulk dtype lookup: For each unique parameter_id in the results, query \_variables once for dtype. Cache in a dict.
4.  Bulk data fetch: Build one SQL query:
    SELECT parameter_id, version_id, schema_id, data_col1, data_col2, ...
    FROM "TableName"
    WHERE parameter_id IN (...) AND version_id IN (...)
5.  No JOIN with \_schema needed — we already know the exact schema_id from \_find_record.
6.  Type restoration: Call self.\_duck.\_restore_types(all_data_df, dtype_meta) once on the bulk DataFrame.
7.  Build lookup: Index data rows by (parameter_id, version_id, schema_id) tuple → row index.
8.  Construct instances: For each metadata row from \_find_record, look up the data row, extract the value (single_column or multi_column mode), construct BaseVariable instance.

Edge cases:

- Custom serialization path (to_db/from_db overrides): fall back to per-record \_load_by_record_row
- Different dtypes per parameter_id: verify all parameter_ids share the same dtype structure; if not, fall back to per-record
- lineage_hash NaN normalization: handle DuckDB NaN→None conversion (as in \_load_by_record_row line 1093-1094)
- Data not found for a record: skip gracefully (matches current behavior)

No other files need changes

bridge.py:load_and_extract() calls \_db.load_all() and materializes via list(gen). The bulk optimization is entirely internal to load_all() — same interface, same return values, just faster.

Verification

1.  Run existing integration tests: pytest /workspace/tests/
2.  Manual MATLAB test: run for_each with timing and verify the [load_and_extract] print shows ~1-3s instead of ~81s
3.  Verify data correctness: loaded BaseVariable instances should have identical record_id, metadata, data, content_hash, lineage_hash, version_id, parameter_id as before
