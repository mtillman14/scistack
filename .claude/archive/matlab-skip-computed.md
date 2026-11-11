# Plan: `skip_computed` for the MATLAB `for_each` path

## Problem

`scidb.for_each(..., skip_computed=true)` from MATLAB silently re-runs every
combo. Two root causes:

1. **Silently swallowed flag.** `+scidb/for_each.m` → `split_options` has a fixed
   switch of known options; `skip_computed` isn't one, so it falls through to
   `meta_args` and becomes a *phantom metadata iterable* (`skip_computed=[true]`),
   not a flag.
2. **Hook never built on the MATLAB seam.** The skip hook (`_build_skip_hook`)
   is constructed only in Python's `scidb.for_each` Step 1.6. The MATLAB path
   calls `scimatlab.bridge.for_each_prepare` → `_for_each_prepare` directly,
   bypassing Step 1.6, and the bridge hardcodes `_pre_combo_hook=None`.

The skip *logic* is sound and reusable; only the MATLAB plumbing is missing.

## Key constraint: function hash

On the MATLAB path `fn` is a no-op sentinel; the real source hash arrives as
`fn_hash` and is what the save path stores (`__fn_hash` → graph `function_hash`).
`_build_skip_hook` currently recomputes `compute_function_hash(fn)` internally —
on the sentinel that would never match the stored hash, forcing eternal
recompute. So the hook must accept the MATLAB `fn_hash` as an override.

## Changes

1. **`scidb/src/scidb/foreach.py` — `_build_skip_hook`**
   Add `fn_hash: str | None = None` param. Use it when provided instead of
   `compute_function_hash(fn, truncate=16)`. Python path passes nothing →
   behavior unchanged. (Solution lives in the scistack layer per CLAUDE.md.)

2. **`scimatlab/src/scimatlab/bridge.py` — `for_each_prepare`**
   - Add `skip_computed: bool = False` param + docstring.
   - When `skip_computed and not dry_run and outputs`: resolve a db (arg or
     global), build the hook via `_build_skip_hook(..., fn_hash=fn_hash)`, and
     pass it as `_pre_combo_hook` into the real `_for_each_prepare` call
     (replacing the hardcoded `None`). Log the decision; warn + no-op if no db.

3. **`scimatlab/.../+scidb/for_each.m`**
   - `split_options`: init `opts.skip_computed = false`, add `case "skip_computed"`.
   - Pass `'skip_computed', logical(opts.skip_computed)` through the
     `for_each_prepare` `pyargs(...)`.
   - **Typo guard:** after splitting, warn for any leftover metadata key whose
     normalized form (lowercased, underscores removed) collides with a reserved
     option name — catching future `skipComputed` / `dry_run` typos that would
     otherwise become silent phantom axes.

## Logging

- Bridge: `[bridge] skip_computed=True: built skip hook for <fn> (fn_hash=...)`,
  or a warn when no db is available.
- Existing `_build_skip_hook` per-combo `[skip]` / `[recompute]` lines now fire
  on the MATLAB path too (printed through the Python boundary to the MATLAB
  console).
- MATLAB: `split_options` warns on reserved-name-lookalike metadata keys.

## Tests

- **`scimatlab/tests/test_bridge_skip_computed.py`** (Python, no MATLAB):
  drive `for_each_prepare` against a real DB seeded with provenance from a prior
  `scidb.for_each` run; read the stored `function_hash` back from `_invocation`.
  - `skip_computed=False` → combo present in `full_combos`.
  - `skip_computed=True` + correct `fn_hash` → combo filtered out (skipped).
  - `skip_computed=True` + changed `fn_hash` → combo present (recompute).
  - `skip_computed=True` + no prior output → combo present (first run).
- **`scimatlab/tests/matlab/scidb/TestForEachSkipComputed.m`** (MATLAB e2e):
  global call-counter helper; run twice with `skip_computed=true`; assert the
  function executes once (second run skipped) and output is unchanged.
- **Helper** `scimatlab/tests/matlab/helpers/skip_counting_double.m`.

## Addendum: empty-combo short-circuit (found during MATLAB testing)

The skip hook fired correctly (`[skip] ...`, `skip_computed: 1/1 combos
skipped`) but the function still ran. Root cause: when *all* combos are
skipped, prepare returns an empty `full_combos`, and `+scidb/for_each.m`
passed it to `scifor.for_each` as `_all_combos`. scifor's `for_each.m`
treats an empty `_all_combos` as "unset" (`if ~isempty(opts.all_combos)`)
and rebuilds the full Cartesian product — re-running the very combos that
were filtered out.

Fix (scidb layer only, scifor untouched per the layer-ownership guidance):
`+scidb/for_each.m` short-circuits when `n_combos == 0` — it frees the
prepare-side cache via `for_each_save(handle, [], save=false)` and returns
an empty table instead of calling the scifor loop. `n_combos == 0` reliably
means "nothing to iterate" because a no-iterables single run yields
`full_combos = [{}]` (n=1), not 0. This also fixes the latent case where
non-existent-schema pruning removes every combo.

## Follow-up (offer)

Write a `docs/claude/matlab_foreach_bridge.md` mapping the MATLAB prepare/save
bridge onto Python's `for_each` steps — the gap that hid this.
