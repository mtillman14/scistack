Schema Cleanup: Normalize \_variables and \_record_metadata

Context

The \_record_metadata table has redundant columns (version_keys, metadata) that duplicate information derivable from \_schema and \_variables. Meanwhile, \_variables is missing version_keys and has an
unused data_hash column. This cleanup normalizes the schema so each fact is stored in one place.

Changes Summary

1.  \_variables: add version_keys, remove data_hash
2.  \_record_metadata: remove version_keys and metadata columns
3.  \_find_record(): rewrite to use JOINs to \_schema and \_variables instead of JSON column filtering
4.  \_load_by_record_row() and list_versions(): reconstruct metadata from JOINed row data

Files to Modify
┌────────────────────────────────┬──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ File │ Changes │
├────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ sciduck/src/sciduck/sciduck.py │ _variables DDL, remove data_hash dedup from save(), update list_versions() │
├────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ src/scidb/database.py │ \_record_metadata DDL, \_save_record_metadata(), three \_save_\* helpers, \_find_record(), \_load_by_record_row(), list_versions(), remove \_flatten_metadata() │
├────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ sciduck/tests/test_sciduck.py │ Update dedup tests that relied on data_hash │
├────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ tests/test_integration.py │ Update metadata assertion for string types │
└────────────────────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

---

Step-by-step Plan

Step 1: sciduck.py — Update \_variables table DDL

In \_init_metadata_tables() (line ~332):

- Replace data_hash VARCHAR, with version_keys VARCHAR DEFAULT '{}',

Step 2: sciduck.py — Remove data_hash dedup from save()

- Lines 539-550: Remove data_hash = self.\_compute_hash(entries) and the dedup check block
- Lines 577-582: Remove data_hash from the \_variables INSERT (keep other columns, don't add version_keys here — SciDuck standalone doesn't know about version_keys, the default '{}' applies)
- Remove \_compute_hash() and \_hash_value() methods (now unused)

Step 3: sciduck.py — Update list_versions()

- Line ~818: Replace data_hash with version_keys in SELECT

Step 4: database.py — Update \_record_metadata DDL

In \_ensure_record_metadata_table() (line ~202):

- Remove version_keys VARCHAR, and metadata VARCHAR, from CREATE TABLE

Step 5: database.py — Update \_save_record_metadata()

- Remove version_keys and nested_metadata parameters
- Remove them from INSERT column list and VALUES (11 → 9 placeholders)

Step 6: database.py — Thread version_keys to \_variables INSERTs

All three save helpers gain a version_keys: dict parameter:

6a: \_save_custom_to_sciduck() — Replace data_hash with version_keys in \_variables INSERT, use json.dumps(version_keys, sort_keys=True)

6b: \_save_native_direct() — Same change

6c: \_save_native_with_schema() — Same change

Step 7: database.py — Update save() call sites

- Pass version*keys to the three \_save*\* helpers
- Remove version_keys and nested_metadata from \_save_record_metadata() call

Step 8: database.py — Rewrite \_find_record()

New approach: always LEFT JOIN all three tables:
SELECT rm.\*, s."{col1}", s."{col2}", ..., v.version_keys
FROM \_record_metadata rm
LEFT JOIN \_schema s ON rm.schema_id = s.schema_id
LEFT JOIN \_variables v ON rm.variable_name = v.variable_name AND rm.version_id = v.version_id
WHERE ...
ORDER BY rm.created_at DESC

Filtering:

- All schema keys: filter via s."{key}" = ? in SQL WHERE (no more contiguous/non-contiguous split for schema key filtering)
- Version keys: filter via Python-side JSON parsing of v.version_keys column in the returned DataFrame

Step 9: database.py — Add \_reconstruct_metadata_from_row() helper

Takes a JOINed row, returns (flat_metadata, nested_metadata):

- Schema keys: read from schema columns in the row (strings, filtered for non-NULL)
- Version keys: parse JSON from version_keys column

Step 10: database.py — Update \_load_by_record_row()

Replace json.loads(row["metadata"]) with self.\_reconstruct_metadata_from_row(row)

Step 11: database.py — Update list_versions()

Replace json.loads(row["metadata"]) with self.\_reconstruct_metadata_from_row(row)

Step 12: database.py — Remove \_flatten_metadata()

No longer called anywhere after Steps 10-11.

Step 13: Update tests

- sciduck/tests/test_sciduck.py: Remove/update test_duplicate_hash_skips_save and test_force_overrides_dedup (SciDuck-level dedup is gone)
- tests/test_integration.py line 30: Update metadata assertion for string values (schema keys from \_schema are always VARCHAR)

---

Behavioral Change

Schema key values in loaded.metadata will always be strings after this change (they come from \_schema VARCHAR columns). Previously they preserved original Python types via JSON roundtrip in the
metadata column. Version keys preserve their JSON types.

Verification

1.  Run pytest tests/test_integration.py -v — all tests pass
2.  Run pytest sciduck/tests/test_sciduck.py -v — all tests pass
3.  Run pytest tests/ -v — full test suite passes
