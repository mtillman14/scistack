# Plan: run-option (distribute/as_table) re-runs must not overplot

*Drafted 2026-09-14. Status: **implemented 2026-09-14** (all stages + Variant pin + popup dropdown; `as_table` included per user). Python tests written, not yet run. Doc: docs/claude/run-option-variants.md.*

## Symptom

Plot Studio shows two points per x-index per trial for `GAITRiteLoaded`;
identical tooltips, different y. One set is constant across all trials of a
subject/session/speed (the `distribute=false` run, which handed every trial the
whole file), the other varies per trial (the `distribute=true` run).

## Mechanism

- `distribute` is identity-bearing: it is folded into `invocation_id` and
  `call_id` (`docs/claude/gui-run-options-flow.md`). The two runs therefore
  wrote two distinct `GAITRiteLoaded` records at every
  `(subject, session, speed, trial)` location. Nothing was overwritten —
  correct, by design.
- `scistackplotdb.load.load_variable` reads **every** non-excluded record of a
  type (`WHERE r.type = ? AND r.excluded IS DISTINCT FROM TRUE`) and relies on
  `attach_variants` to tell coexisting records apart.
- `attach_variants` discriminates on two things only: branch params
  (`branch_params_batch`) and the upstream code chain
  (`code_versions_batch` → `Code:<fn>` axis + `is_latest`). Same constants,
  same function hash → no variant column, no latest column. The two records
  look like replicates and are drawn on top of each other.
- The `load()` path is **also** affected — corrected 2026-09-14 after
  `scidb show GAITRiteLoaded subject=SS01 session=BL speed=SSV trial=1` listed
  BOTH records (`52e30699…` 13:14, `c5262da4…` 11:48) under "latest per
  variant", with `_find_record` warning that locations "retain >1 record after
  collapse". The collapse key is `(fn_name, branch_params, output_num,
  consumed_input_schema_ids)`; a distributed run spreads one call across
  records whose `output_num` is the slice index, so at most one trial can share
  `output_num` with the non-distributed record and every other trial keeps
  both. Downstream steps loading `GAITRiteLoaded` per trial get a two-row
  table. (The WARN printed no examples because `groupby` dropped NULL-keyed
  groups — `cycle` is NULL on trial-level records; fixed with `dropna=False`,
  so the next run of the command names the surviving variant keys.)

This is the same shape as `variant-selection.md` §2 (records that differ in
something identity-bearing that the variant identity does not see), one axis
over: there it was an upstream code edit, here it is a run option.

## Fix (scidb layer, surfaced by scistackplotdb — no GUI logic)

**Stage 0 — scidb: load-path supersession.** In `_find_record`'s collapse,
records at one `(variable, schema_id)` whose producing invocations share
`fn_name`, `branch_params` and consumed locations but differ in identity-bearing
run options must collapse to the newest *invocation*, not survive side by side
on `output_num`. Concretely: when a location holds records from >1 invocation of
the same fn/bp/consumed key and those invocations differ in `(distribute,
as_table)`, keep only records of the newest such invocation, then apply the
existing per-`output_num` rule within it. Needs a test that the
distribute=false → distribute=true re-run leaves one row per trial, and that
genuine multi-output (`output_num`) records of ONE invocation are untouched.

**Stage 1 — scidb: run-option chain.** In `provenance_query`, alongside
`code_versions_batch`, walk the same chain collecting each hop's identity-bearing
run options `(distribute, as_table)` from `_invocation`. Fold them into the
per-location `is_latest` chain signature so a re-run with different options
supersedes the older record at that location (newest signature wins — the
`_order_versions` machinery unchanged). Two records that differ only in run
options are then `is_latest=True` / `False`, never both `True`.

**Stage 2 — scidb: run-option axis.** Emit, per function whose records of a
type hold more than one distinct option set, a `RunOptions:<fn>` entry in the
identity dict with levels like `distribute=false` / `distribute=true` — same
"omit the single-valued case" rule as `code_version_ordinals`, so an ordinary
project sees nothing new.

**Stage 3 — scistackplotdb: attach it.** `attach_variants` treats the new axis
exactly like `Code:<fn>`: a variant column (arms the pooling guard), the
`LATEST_COLUMN` attached whenever *either* a code axis or a run-option axis is
present (today it is `if code_keys:` only). Plot Studio then opens pinned to the
latest (existing default-variant-selection behaviour) and the older run is one
click away in the variant rows rather than silently overplotted.

**Stage 4 — logging + tests.**
- `variant_identity:` INFO line extended to name types with >1 run-option set.
- `scistackplotdb` test: two invocations of one fn at one location differing
  only in `distribute` → one variant column, exactly one `is_latest=True` row.
- `scidb` test: chain signature differs on `distribute`, equal otherwise.

## Not doing

- Deleting or auto-excluding the distribute=false records (project ethos:
  hide, never delete — and the older run may be wanted for comparison).
- A GUI-side dedupe: the GUI has no way to know which record is "right".

## Open question for the user

Whether `as_table` should join `distribute` in the signature. It is equally
identity-bearing (`gui-run-options-flow.md` table), so the plan includes it,
but it changes the *shape* of what a function receives rather than what it
writes, so a re-run toggling it is more likely a deliberate comparison than a
fix.
