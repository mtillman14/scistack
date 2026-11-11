# Plan: PathInput discovery should respect explicit metadata iterables

## Problem

Three scihist `test_state_realworld.py` tests fail:

- `test_grey_when_one_combo_errors` — expected grey (14/15), got green (14 up_to_date, 0 missing)
- `test_grey_when_multiple_combos_error` — expected grey (12/15), got green
- `test_grey_one_output_when_partial_failure` — expected grey, got green

All three follow the same pattern: user calls scihist `for_each` with a
`PathInput` and explicit `subject=[...]`, `trial=[...]` lists where one or
more files are intentionally missing on disk. The expectation is that the
function is invoked for every user-listed combo, the missing files cause
runtime `FileNotFoundError`, and the node aggregates as grey
(14 succeeded + 1 missing = grey).

Actual: only the 14 existing combos are ever attempted. `_for_each_expected`
records 14 entries, so `check_node_state` sees `expected ⊆ actual` and
reports green with `missing=0`.

## Root cause

`scidb/foreach.py` Step 3 (PathInput discovery, lines 447–522). When the
user supplies explicit values for any template key (`user_filter_seen=True`),
the code:

1. Intersects discovered filesystem combos with user-provided values.
2. **Resets `metadata_iterables` to only the values present in the
   filtered (file-exists) result** — silently dropping
   user-requested combos that have no file.
3. Sets `_discovered_combos = filtered`, which downstream becomes
   `base_combos`.

The intersection-then-reset behavior is at odds with the existing MATLAB
test `TestForEachSchemaFiltering.test_no_filtering_when_all_explicit`,
which asserts that explicit user values must NOT be filtered ("3 of 4
combos are skipped … but all 4 are attempted").

The DB-resolved (`subject=[]`) path correctly filters discovered combos to
prevent inventing non-existent ones — that part should stay. The bug is
specific to the explicit-values path.

## Fix

In `scidb/foreach.py` Step 3, when `user_filter_seen=True`:

- **Do not** intersect discovered combos with user values.
- **Do not** reset `metadata_iterables` to filesystem-discovered values.
- **Do not** set `_discovered_combos`. Leaving it `None` causes Step 12 to
  fall through to the Cartesian product of `metadata_iterables`, giving
  the user-intended set of combos.

Result: every user-listed `(subject, trial)` is attempted. Missing files
raise `FileNotFoundError` inside the function; scifor's per-combo
`try/except` records the failure as a skip and continues. The combo has
no output record → `_get_expected_combos` (via `_for_each_expected`) sees
all 15, `_get_output_combos` sees 14, `missing = 1`, state = grey.

When `user_filter_seen=False` (every key is `[]`, fully discovered),
the existing `_discovered_combos = combos` path is kept — preserves the
"don't invent non-existent combos" guarantee for the all-`[]` case.

## Files to modify

- `scidb/src/scidb/foreach.py` — Step 3 PathInput discovery block (lines
  ~474–520). Remove the filtering/reset branch under `user_filter_seen`;
  keep the `[]`-fill loop and the `not user_filter_seen` discovery path.

## Verification

- `scihist-lib/tests/test_state_realworld.py` — three currently-failing
  tests should pass.
- `scihist-lib/tests/test_state_realworld.py::test_red_when_all_combos_error`
  — already passes; should continue to pass (no files matched → discovery
  returns empty, falls through to Cartesian unchanged).
- `sci-matlab/tests/matlab/scidb/TestForEachSchemaFiltering.m`:
  - `test_no_filtering_when_all_explicit` — already documents the
    intended behavior; should still pass (DB-variable input, doesn't
    touch this code path).
  - `test_no_filtering_with_pathinput`, `test_no_filtering_with_fixed_pathinput`
    — both use `subject=[], session=[]` (all `[]`), still take the
    `_discovered_combos = combos` path, unchanged.
  - `test_filtering_removes_nonexistent_combos`,
    `test_mixed_resolved_and_explicit` — DB-variable inputs, no
    PathInput, unaffected.
- `scihist-lib/tests/test_state_pathinput.py`,
  `test_state_matlab_pathinput.py` — both call `_persist_expected_combos`
  directly, bypassing for_each, unaffected.

## Logging / diagnostics added

Add an `Log.info` line on the explicit-values branch noting the
"explicit user values, skipping discovery filter" decision so the
behavior is visible in `scidb_run.log` when debugging.
