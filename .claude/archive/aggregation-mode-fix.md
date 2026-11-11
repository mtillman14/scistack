# Aggregation Mode Fix — Schema Level Subset in for_each()

## Problem

When the GUI's Schema Level section has a subset of schema keys selected (or none),
`for_each()` produces 0 iterations because the rid (record ID) expansion logic fails
to match empty/partial combos against the per-schema-key grouping.

**Root cause:** `scidb/src/scidb/foreach.py` lines 274-342. The `_lookup_keys` include
ALL schema keys. The rid_per_combo mapping is grouped by those keys. But when the combo
is `{}` (no iterated keys), lookup produces `("",)` which matches nothing → 0 full combos.

## Fix

In `scidb/src/scidb/foreach.py`, after the rid_per_combo mapping is built (line 312)
and before the combo expansion (line 314):

1. **Detect aggregation mode:** `_iterated_schema_keys` is a strict subset of
   `current_schema_keys`.
2. **Skip rid expansion:** set `full_combos = base_combos`, clear `rid_keys`,
   clear `rid_per_combo`.
3. **Strip `__rid_*` columns** from loaded DataFrames so the user's function
   doesn't see internal tracking columns.
4. **Log** that aggregation mode was triggered.

The existing rid expansion code (lines 314-348) moves into an `else` branch.

## Files Changed

- `scidb/src/scidb/foreach.py` — aggregation mode detection + skip rid expansion

## Future: Non-Iterated Schema Filtering

Eventually, Schema Filter selections for non-iterated keys (e.g., filtering specific
trials when iterating at the subject level) will need to restrict which rows appear
in the aggregated DataFrame. This filter would slot in between "skip rid expansion"
and "pass to scifor" — a simple DataFrame row mask on non-iterated schema columns.

Likely takes the form of a new `for_each()` parameter like
`metadata_filter: dict[str, list]`, applied to loaded DataFrames after rid handling
but before the scifor call.
