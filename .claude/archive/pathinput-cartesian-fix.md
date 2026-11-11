# PathInput re-run: invented Cartesian combos

## Symptom

Running the same MATLAB `scidb.for_each(...)` twice against a function whose only data input is a `PathInput`:

- **Run 1**: 16 iterations (filesystem has 16 matching files; 2 template-shaped combos have no file on disk). Behavior is correct.
- **Run 2**: 18 iterations. The extra 2 iterations are the full Cartesian-product combos whose files don't exist — they fail with `Unable to find or open '…/sub02/trial06.csv'`.

The second run "invents" combos that the filesystem never had, and the iteration order flips from subject-outer to trial-outer.

## Root cause

`/workspace/sci-matlab/src/sci_matlab/matlab/+scidb/for_each.m` gates `PathInput.discover()` behind two cases (lines 135–183):

- **Case 1** — `isempty(meta_keys)` (no metadata arguments at all).
- **Case 2** — `any(still_empty)` where `still_empty(i) = isempty(meta_values{i})` AFTER the DB resolution block at lines 107–133.

### Why run 1 works

DB has no lineage yet for this function's schema keys, so `distinct_schema_values('subject')` and `distinct_schema_values('trial')` both return empty. `meta_values` stays empty → `still_empty = [true, true]` → Case 2 triggers → `pi.discover()` returns 16 combos → `discovered_combos = 16` → line 362–365 copies it into `all_combos`.

### Why run 2 breaks

Run 1 persisted lineage rows for 15 successful combos. On run 2, `distinct_schema_values` returns `[01,02,03]` and `[01..06]`, so `meta_values` is populated from the DB. `still_empty = [false, false]` → **Case 2 does not trigger**, `discovered_combos` stays empty. The DB pre-filter block at line 286 is also skipped because its guard is `any(needs_resolve) && ~has_pathinput(inputs)`. `all_combos` stays `[]`, so `scifor.for_each` runs the full Cartesian product = 3 × 6 = 18.

The two invented combos (`sub02/trial06`, `sub03/trial06`) are the two files missing from disk.

## Fix

When `has_pathinput(inputs)`, the filesystem is the source of truth for which combos exist. Replace the two-case block with a single unconditional path:

1. Call `pi.discover()` once.
2. For each key the template covers whose `meta_values{i}` is empty, populate it from the discovered combos.
3. Build `discovered_combos` as the discovery result, filtered so every user-provided key's value is a member of that key's user list (only applies when the user passed explicit values — empty/unspecified lists don't filter).
4. Assign `all_combos = discovered_combos` (the existing fallback at line 362–365 already does this when the DB pre-filter didn't populate it).

This makes re-runs deterministic regardless of DB state: the iteration count, the combo set, and the iteration order all come from the filesystem walk.

## Test plan

Add a regression test that simulates the exact run 1 → run 2 transition:

- Seed the DB with lineage rows as if run 1 had just finished (so `distinct_schema_values` returns the full subject/trial grid).
- Call the MATLAB-side combo-resolution path (or its Python equivalent) with `subject=[]`, `trial=[]` and a `PathInput` whose template matches only 16 of the 18 possible combos.
- Assert: the resulting combo list has 16 entries (not 18), and the two filesystem-missing combos are absent.

A lightweight Python-side unit test covering `PathInput.discover()` + combo filtering behavior is sufficient; the MATLAB wrapper logic is thin. If there's a convenient harness in `sci-matlab/tests/matlab/` that exercises the full wrapper, add a matching case there too.
