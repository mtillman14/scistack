# Plan: split `scidb.foreach._for_each_prepare` into named stages (F4)

Status: BUILT 2026-09-23 (all 9 stages, uncommitted; scidb + scimatlab pytest pending). Regression tests: scidb/tests/test_foreach_prepare_stages.py. Deviation: no `_PrepareContext` dataclass — stages take/return explicit values, which made each stage's inputs visible in its signature; `_RidIndex` owns the combo-location geometry instead.

## Why
`_for_each_prepare` is 1,518 lines (scidb/foreach.py ~1591–3109): the
biggest single function in the repo, in the #1 bug hotspot file (103 commits,
26 fix commits). Every block reads locals set by earlier blocks, so a fix
today means reading ~15 disjoint regions to know what a variable holds.

## Shape
One small dataclass, `_PrepareContext`, holds the values that flow between
stages (inputs, metadata_iterables, config_keys, call_id, loaded_inputs,
full_combos, bindings, …). Each stage is a module-level function
`_stage_name(ctx) -> None` (or returning an early `None` for dry run), named
by what it does — no step numbers (feedback: named steps, no numbering).
`_for_each_prepare` becomes a short, readable sequence of those calls ending
in the existing `_ForEachState(...)` construction. Behaviour, log lines and
the `_ForEachState` fields are unchanged; `scidb.external_loop` re-exports
stay valid (MATLAB calls `prepare` unchanged).

## Stages (current step markers -> function)
1. `_normalize_glue` — glue chains + constant-fed glue (≈1624–1655).
2. `_resolve_iterables` — user-explicit keys, empty-list DB resolution,
   PathInput discovery + its report, schema propagation, stringify
   (Steps 2–5, ≈1656–1936).
3. `_dry_run_preview` — Step 7; returns the sentinel for "stop".
4. `_build_call_identity` — output names + ForEachConfig keys / call_id
   (Steps 6, 8).
5. `_existing_combos` — pre-filter to existing schema combos + exclusions
   (Steps 9, 9.5).
6. `_load_all_inputs` — bulk load + glue fusion (Step 10 + fusion).
7. `_track_variants` — rid→branch-params map, inputs by kind (Step 11).
8. `_expand_combos` — full combos per schema location (Step 12), split
   further into `_expand_rid_combos`, `_log_multiplicity`, `_bind_inputs`
   (the Step-13 typed bindings) — this is ~780 lines today.
9. `_apply_pre_combo_hook` — skip_computed filter (Step 14).

## Procedure (safety)
- Start from a GREEN full suite (scidb + scimatlab + scistack-gui).
- One stage per commit-sized change, in order 1 -> 9; after each, the user
  runs `scidb/tests` (and `scimatlab/tests` after stages touching state the
  bridge reads). No behaviour change allowed: any test diff is a bug in the
  extraction, not a test to update.
- Variable flow per block is listed BEFORE moving it (reads / writes /
  used-after), so nothing silently stops being passed along.
- Verification per stage without Python: orphan check + `^#` comment diff vs
  HEAD (feedback_scripted_function_deletion), and `test_imports.py`.

## Not in scope
Changing what any stage does, the MATLAB mirror, or `for_each`'s own 480
lines (a later pass once prepare is legible).
