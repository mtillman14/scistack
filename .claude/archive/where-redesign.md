# Plan: #4 `where=` redesign (round 1 of 4–6)

Goal: `where_clause` string becomes **display-only** (no logic/matching consumer).
Variant selection becomes **semantic**: match by the *consumed input schema_id set*.
Add `select=` as the explicit row/schema selector (role split, #4A). Matching rule
(user-confirmed): **subset + raw fallback**.

## Matching rule (unified — collapses old Strategy 1 + Strategy 2)
Resolve `where=` variable-level portion → `S_var` (schema_ids). A candidate record matches iff:
- it has a producing invocation AND every schema_id that invocation consumed ⊆ `S_var`; OR
- it has **no** producing invocation (raw/direct save) AND its own schema_id ∈ `S_var`.
Then intersect with row restriction `allowed_ids` = (`select=` ∪ SchemaKey-portion-of-`where=`).resolve().
`where=None` ⇒ variant filter passes everything; `select=None` ⇒ no row restriction.

Consequence (accepted): two *all-pass* filters (FlagU=='U' vs FlagA=='A' over the same
combos) are now the **same** variant — distinguish such variants by constants/branch_params,
not by where=. Tests encoding all-pass-flag distinction get rewritten with genuinely-disjoint
filters.

## Edits
1. `database.py::_load_with_where` — rewrite to the unified semantic rule. Drop
   `record_where_clauses`, `__where` augmented key, `_where_key_from_filter` call.
2. `database.py` — add `select=` kwarg to `load`/`load_all`/`find_record_id`/`_find_record`
   plumbing; resolve it to `allowed_ids` and apply as a row selector.
3. Remove `_where_key_from_filter` (database.py), `_merge_constituent_where_key` +
   `_PreresolvedFilter._where_key`/`to_key` string (foreach.py). Merge constituents carry the
   **variable-level filter** for semantic variant matching + pre-resolved ids for row restriction.
4. `provenance_query.py` — drop `record_where_clauses` (no remaining logic consumer); keep
   `get_execution_audit` where_clause (display). Drop `where_clause` from `pipeline_variants`
   group key + `vk["__where"]`.
5. `foreach_config.py` — drop `__where` from `_CALL_ID_INCLUDED_KEYS` + `to_version_keys`
   (where= no longer part of call_id identity).
6. KEEP the `where_clause` **write** path (`record_run`, `_run.where_clause`, get_execution_audit)
   purely for visual inspection.
7. Rewrite affected tests (variant_merge all-pass-flag tests, the record_where_clauses test,
   any pipeline_variants/__where assertions).

## Then (later rounds)
- #5 slim & rename `_record_metadata`.
- #6 PathInput template in identity.
