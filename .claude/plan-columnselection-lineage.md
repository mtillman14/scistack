# ColumnSelection lineage + a latest-flag that does not depend on an axis

*2026-09-15. Trigger: `GAITRiteSymmetry` plotted values of 200 (the signature of
zeros in the input) although the function provably computed NaN-aware values —
confirmed by conditional breakpoint. Two independent defects, fixed separately.*

## What was actually wrong

`scidb.log`: `loaded GAITRiteSymmetry: 780 record(s) ... variants=none`.

780 = 390 superseded (computed from the zeroed input) + 390 current. `variants=none`
means no variant column was attached, so no `CodeIsLatest` flag, so no filter —
the plot drew both generations, and the old one carries the 200s.

Why no axis? `scidb.log`, same run:

```
WARN [batch_save] 'calculateSymmetryOneVector': NO variable input-binding source
  ... saved records will have NO _invocation_input edges, yet scidb-variable
  input(s) ['v'] WERE consumed.
```

The records have **no lineage**. The chain walk cannot reach `grSides` two hops
up, so no `Code:grSides` axis exists for this variable — although it exists and
works correctly for `GAITRiteLoaded_UA` one hop up.

The trigger was a GUI column selection on `v`, visible in the log as the run
changing from `converted input 'v' ... (table 390x59)` with `x__vsig_v` present,
to `converted input 'v' ... (scifor.ColumnSelection)` with no vsig and the WARN.

## Stage 1 — the rid wire carried two unrelated jobs

`ColumnSelection` is deliberately excluded from `rid_keys`, with a comment
saying why: coupling it in "perturbs Variant branch_param pinning and
for_columns aggregation". Correct — but `rid_keys` was ALSO the only feed for
the save path's input binding, so switching off **iteration** switched off
**lineage** with it.

`Fixed` inputs already show the separation: they bind lineage through
`fixed_rid_values` while taking no part in rid expansion. The save path names
three binding sources — `__rid_*` columns, `combo_to_rids`, fixed rids.

**Fix:** build the same per-combo rid mapping for ColumnSelection params into
its own dict (`colsel_rid_per_combo`) and merge it into `_combo_to_rids` at the
one point where that map is assembled. `rid_keys`, `vsig_cols` and
`rid_keys_for_schema` are untouched.

**Risk assessment (read, not assumed).** `combo_to_rids` has exactly two
readers:

| reader | effect |
|---|---|
| `foreach.py` save path | writes `_invocation_input` edges ← the point |
| the skip-computed gate | skips combos whose output already exists for these inputs |

It does not feed combo expansion, schema extension, `Variant` pinning or
`for_columns`. The second reader is a real behaviour change, chosen
deliberately: a re-run with a column-selected input now skips already-computed
work, as a plain input already does.

Full-iteration mode needed its own fix; see the section below.

## Stage 2 — the latest flag was gated on the wrong thing

`scistackplotdb.load.attach_variants` attached `CodeIsLatest` only
`if code_keys or run_keys`. An axis is how a superseded record is usually
*explained*; it is not what makes it superseded. Two cases the gate missed:

* a re-run over changed inputs — no code edit, no option flip;
* a record whose lineage was severed — no axis can be derived at all.

**Fix:** attach whenever any record is not latest
(`code_keys or run_keys or n_current < len(record_ids)`). Gated on "would this
exclude anything" rather than unconditional, so a single-generation variable
gains no column and no default pin, exactly as before. `default_selection` pins
on `if latest:` alone, so the pin follows automatically.

A WARN fires for the case the old gate missed — superseded records with no axis
to explain them — pointing at the lineage WARN as the usual cause.

## Tests

`scidb/tests/test_column_selection_lineage.py` (aggregation mode, 4 schema keys
with 3 iterated — the real pipeline's shape):
- edges exist at all; they point at the records actually consumed; a re-run over
  changed input binds a different record;
- **and** that iteration did not re-couple: one call per location (not expanded),
  the function still sees only the selected columns, a plain input still binds.

`scistackplotdb/tests/test_cache_content_validation.py::TestSupersededWithoutAnAxis`:
- a single-generation variable gains no flag (the ordinary case unchanged);
- a superseded record gets one;
- the flag is not offered as a plottable factor.

## Full-iteration mode — closed 2026-09-15

The gap noted below as "not done" was closed in the same session. Full iteration
binds lineage by a different route: plain inputs carry their rid in a `__rid_*`
column of the result table, and ColumnSelection has none. Three changes:

1. `rid_populated_idx` is now populated for ColumnSelection inputs too. It is
   read ONLY by `_rid_probe_key`, and the full-iteration expansion loop probes
   `rid_per_combo` alone, so no combo changes — it just lets a coarse input be
   looked up at a fine location.
2. The rid is injected into the combo as `__rid_{param}`, EXACTLY as a `Fixed`
   input already does, so the result table carries the column and the save
   path binds it with no new code at all.

**A wrong turn worth recording.** The first attempt routed this through
`combo_to_rids` in full-iteration mode. That map does not feed the lineage
edges — those come from `__graph_var_bindings`, built from the row's `__rid_*`
columns plus fixed rids — it feeds `__upstream`. Making it non-None in this
mode therefore bound nothing for the selected input AND diverted the
`elif rid_keys` branch that plain inputs use for `__upstream`, silently
dropping theirs. The merge test caught it; the two binding mechanisms look
interchangeable and are not.

Multi-variant edge: ColumnSelection pools variants rather than expanding them,
and one `__rid_*` combo key holds one id — so the first is bound and a DEBUG
line says so, rather than binding nothing.

Tests: `test_full_iteration_column_selection_binds_lineage`,
`test_full_iteration_binds_a_plain_and_a_selected_input_together` (the merge),
`test_full_iteration_without_column_selection_is_unchanged`.

## Not done

- The 390 already-orphaned `GAITRiteSymmetry` records cannot be repaired — a
  receipt is only writable when the work is done. Re-running after this fix
  writes correctly-linked records; the orphans should be marked **excluded**
  (never deleted), which the plot query already filters.
