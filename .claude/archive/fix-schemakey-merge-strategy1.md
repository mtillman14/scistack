# Fix: SchemaKey filter ignored on Merge inputs (for_each where=)

## Symptom
`for_each(..., where=schema_key("session").isin([...]) & VarFilter & ColFilter)`
with a `Merge(...)` input returns **all** sessions — the SchemaKey portion is
silently dropped.

## Root cause
Merge inputs pre-resolve the where= filter per constituent into a
`_PreresolvedFilter(matching_ids, where_key=<var-level key>)`
(`foreach.py:1585-1603`). `matching_ids` correctly includes the SchemaKey
restriction.

But `_load_with_where` (`database.py`) **Strategy 1** matches records by the
`__where` provenance key and then applies a schema-id row selector **only** from
a structural `sk_filter` (a real `SchemaKeyInFilter`/`SchemaKeyCompareFilter`).
A `_PreresolvedFilter` is neither, so `sk_filter` is `None` and the pre-resolved
ids are never applied. When the constituent has a matching stored `__where`
(i.e. it was itself computed by `for_each`), Strategy 1 returns every schema_id
sharing that variant — ignoring the session filter.

Commit `81ad8ef` fixed the direct `.load(where=...)` path but not the
pre-resolved Merge-constituent path.

## Fix
1. `foreach.py` — mark `_PreresolvedFilter` with
   `_restrict_to_resolved_ids = True` (its `_schema_ids` are authoritative and
   must restrict rows in **every** strategy). Update the misleading docstring.
2. `database.py` `_load_with_where` Strategy 1 — apply a schema-id row selector
   from `sk_filter` when present, OR from `where.resolve()` when the filter is
   flagged pre-resolved. Add debug logging of the before/after row counts.

`load_all_as_df` already catches `NotFoundError` from `_load_with_where` and
returns an empty DataFrame, so a constituent with no rows in the allowed
sessions is handled gracefully by the Merge inner-join.

## Tests
Add regression to `scidb/tests/test_schema_key_filter.py`: a Merge whose
constituent is `for_each`-computed (so it has a stored `__where`) loaded with
`schema_key(...) & VarFilter` must return only the selected schema keys.
