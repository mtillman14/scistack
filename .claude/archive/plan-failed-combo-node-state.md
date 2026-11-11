# Plan: Fix Node Turning Green When a Combo Fails

## Problem Statement

When a MATLAB function runs 64 repetitions and 63 succeed / 1 fails, the
function node turns **green** instead of **grey**. This is a first-time run
(no prior outputs).

## Root Cause (Confirmed)

The function's only input is a **PathInput** (file path generator), not a
DB-backed variable. The staleness system can't determine expected combos:

1. `_get_expected_combos()` in `scihist/state.py` determines expected combos
   by querying the DB for schema_ids of the function's **variable inputs**.
2. PathInput is NOT a DB variable. It's classified as CONSTANT by scilineage.
   No `rid_tracking` entries are created for it in `_lineage.inputs`.
3. `_get_lineage_variants()` returns a variant with **empty `input_types`**.
4. The loop `for itype in input_types.values()` never executes.
5. Expected combos = **empty set**.
6. With 0 expected and 63 actual up-to-date combos: missing=0 → **green**.

The 64th failed combo is invisible because the system has no way to know
it should exist.

## Chosen Fix: Persist Expected Combos

See full plan at: `/home/node/.claude/plans/sparkling-yawning-wigderson.md`

Summary: scidb.for_each already knows the full combo set via `_discovered_combos`.
Persist it to a new `_for_each_expected` table BEFORE skip_computed runs, so
`_get_expected_combos` can use it as a fallback when no variable inputs exist.

## Files Involved

- `scidb/src/scidb/database.py` — new table `_for_each_expected`
- `scidb/src/scidb/foreach.py` — persist expected combos after full_combos computed
- `scihist-lib/src/scihist/state.py` — fallback read in `_get_expected_combos`
