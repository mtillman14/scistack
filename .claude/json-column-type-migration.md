# Migration Plan: VARCHAR to JSON Column Type

## Context

Currently `version_keys` and `branch_params` in `_record_metadata` use VARCHAR to store JSON data. After testing, we found that DuckDB's native JSON type provides:

- **1.46x faster** query performance with `json_extract()`
- **INSERT-time validation** (catches invalid JSON immediately vs. at query time)
- **Same functionality** (both return strings, support `json_extract()`, `->` operator)

## Changes Required

### Phase 1: Update Schema (database.py)

**File**: `scidb/src/scidb/database.py`

In `_ensure_record_metadata_table()` (lines 583-597):

```python
# BEFORE:
version_keys VARCHAR DEFAULT '{}',
branch_params VARCHAR DEFAULT '{}',

# AFTER:
version_keys JSON DEFAULT '{}',
branch_params JSON DEFAULT '{}',
```

**Similarly for `_ensure_lineage_table()`** (lines 599-614):

```python
# BEFORE:
inputs VARCHAR NOT NULL DEFAULT '[]',
constants VARCHAR NOT NULL DEFAULT '[]',

# AFTER:
inputs JSON NOT NULL DEFAULT '[]',
constants JSON NOT NULL DEFAULT '[]',
```

### Phase 2: Update Tests

Tests currently use raw SQL with `parse_version_keys()` helper. We have two options:

**Option 2A: Keep raw SQL, update helper (minimal change)**
- Keep `parse_version_keys()` helper in tests
- It will continue to work (JSON type still returns strings)
- No test changes needed beyond schema

**Option 2B: Use DatabaseManager methods (cleaner)**
- Replace raw SQL with `db._get_version_keys(record_id)`
- Remove `parse_version_keys()` helper
- More refactoring but cleaner abstraction

**Recommendation**: Start with 2A (no test changes), optionally do 2B later.

### Phase 3: Migration Considerations

**Existing databases**:
- DuckDB will handle the migration automatically when table is recreated
- VARCHAR strings containing valid JSON will convert to JSON type seamlessly
- If any database has invalid JSON (unlikely), it will fail at migration

**No code changes needed**:
- ✅ Write path: `json.dumps()` works with JSON columns
- ✅ Read path: JSON columns return strings, so `json.loads()` still works
- ✅ Queries: `json_extract()` works on both types

## Testing Strategy

1. Run existing tests - should pass without changes
2. Verify performance improvement with benchmark
3. Test that invalid JSON is rejected at INSERT time
4. Check that `IS NOT DISTINCT FROM` still works for deduplication

## Rollback Plan

If issues arise, revert schema change:
```python
version_keys VARCHAR DEFAULT '{}',
branch_params VARCHAR DEFAULT '{}',
```

Data will convert back to VARCHAR strings automatically.

## Benefits Summary

- ✅ 46% faster query performance
- ✅ Better data integrity (fail-fast)
- ✅ Semantically correct schema
- ✅ Minimal code changes
- ✅ No breaking changes to API
