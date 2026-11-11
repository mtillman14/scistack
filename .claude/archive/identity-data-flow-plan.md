# Plan: Comprehensive Identity & Data Flow Diagram for scidb

## Status: IMPLEMENTED

## Files Created/Modified

- **Created**: `/workspace/docs/claude/scidb-identity-and-data-flow.md` -- the new identity system reference document
- **Modified**: `/workspace/docs/claude/scidb-for-each-internals.md` -- added cross-reference in `_record_metadata` section

## Document Structure (as implemented)

1. **Overview Diagram** -- ASCII flow showing user code -> save path -> database tables, load path, and for_each orchestration
2. **SQL Tables** -- Complete DDL + column-by-column descriptions for `_schema`, `_record_metadata`, `_variables`, `_for_each_expected`, and data tables
3. **The Identity Hierarchy** -- Diagram + table of all six identity concepts: schema_id, content_hash, version_keys, record_id, branch_params, call_id
4. **The Save Path** -- Step-by-step trace for both direct saves and for_each saves, with ASCII flow diagrams
5. **The Load Path** -- Step-by-step trace for both direct loads and for_each bulk loads
6. **User-Facing API vs Internal Machinery** -- Clear delineation of what users touch vs internal mechanics
7. **Edge Cases and Discrimination Gaps** -- How records stay unique across upstream variants, __upstream field, constants in version_keys vs branch_params

## Source Files Referenced

- `/workspace/scidb/src/scidb/foreach.py` -- _save_results, variant tracking
- `/workspace/scidb/src/scidb/foreach_config.py` -- ForEachConfig, call_id
- `/workspace/scidb/src/scidb/database.py` -- save(), _find_record(), _split_metadata(), DDL
- `/workspace/scidb/src/scidb/variable.py` -- BaseVariable.save(), load()
- `/workspace/canonical-hash/src/canonicalhash/hashing.py` -- generate_record_id(), canonical_hash()
- `/workspace/sciduck/src/sciduckdb/sciduckdb.py` -- _get_or_create_schema_id(), _schema table
