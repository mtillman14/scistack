# Plan: Fix empty/duplicate combos in `for_each()` with column-selected inputs

## Symptom (actual repro — NO `where=` involved)
```matlab
scidb.for_each(@LRtoUA, ...
   struct('lr', GAITRiteLoadedCycle("StartFoot"), 'uaConfig', uaConfig), ...
   {UAStartFoot}, ...
   subject=[], session=[], speed=[], trial=[], cycle=[], as_table=true);
```
Iteration 1 → 2×6 table (SS02/BL/FV/1/1, StartFoot="R", two rows).
Iteration 2 → 0×6 empty table. Alternates thereafter.

> NOTE: the initial `where=` theory was WRONG — the repro has no `where=`. The
> `where=` schema_id-vs-row-granularity notes are unrelated to this bug (kept only
> as a possible future logging improvement).

## Root cause (confirmed)
`GAITRiteLoadedCycle("StartFoot")` is a `_scifor.ColumnSelection`. Both
`_scifor.Fixed` and `_scifor.ColumnSelection` store their frame under `.data`.

`_for_each_prepare` Step 11 (`scidb/src/scidb/foreach.py:1143-1182`) detects the
wrapper with a bare `hasattr(data, 'data')` and sets `is_fixed = True` for BOTH.
Only the non-fixed branch runs `rid_keys.append(rid_col)`. So a ColumnSelection
input:
- is mislabeled Fixed,
- `fixed_rid_values` not set (its `len(df)` is the whole spread, not 1),
- `rid_keys` stays empty → `rid_per_combo` empty.

In full-iteration mode (all schema keys iterated) the non-existent-combo skip at
`foreach.py:1357-1366` only fires when `rid_per_combo` is non-empty. With it
empty, EVERY base combo hits the unconditional `else` (line 1377). And
`base_combos` is the full Cartesian product of distinct schema values
(`foreach.py:1206-1209`, because `subject=[]…` → explicit iterables →
`all_combos is None`), which includes combos with no data. Hence:
- existing location → real rows (the "2 rows");
- non-existent combo → empty slice → "0×6".

The two identical rows are simply that location's loaded rows (2 records or a
2-row table value); without rid registration they are also not variant-separated.

## Fix (AS IMPLEMENTED — decoupled pruning from rid expansion)
First attempt coupled ColumnSelection into `rid_keys` (full rid tracking). That
regressed `Variant` pinning (wrong value) and `for_columns` (a `__rid_*` column
leaked into the as_table frame), because `rid_keys` drives THREE things: pruning,
rid **expansion**, and scifor **schema extension**. ColumnSelection only wants the
pruning.

Final design — `_for_each_prepare` Step 11/12:
- Distinguish wrapper types by `isinstance` (not `hasattr('.data')`):
  - `_scifor.Fixed` → fixed path (unchanged).
  - `_scifor.ColumnSelection` → recorded in new `colsel_params`; `__record_id`
    renamed to internal `__rid_{param}` (so column selection drops it); NOT added
    to `rid_keys` (no expansion, no schema change → Variant/for_columns semantics
    preserved). The ONLY net change vs the buggy code is pruning.
  - plain `pd.DataFrame` → full rid tracking (unchanged).
- Step 12 builds `colsel_existence` (schema locations each ColumnSelection has
  data for) + a coarse-level-safe `_colsel_combo_present(schema_vals)` probe, and
  the full-iteration combo loop prunes Cartesian combos absent from that coverage.
  Plain inputs still prune via the existing rid-validity check.
- scifor `as_table` assembly (`_prepare_input`, `_run_column_iteration`,
  `_extract_data`) now excludes internal `__`-prefixed schema columns — needed for
  PLAIN DataFrame as_table inputs, which legitimately extend the schema with
  `__rid_*`.
- aggregation-mode `__rid_*` strip also handles `ColumnSelection.data`.

## Logging improvements (make this class of bug obvious)
1. Step 11: per input, log detected wrapper type and whether rid tracking was
   enabled (`rid key added` vs `skipped — no __record_id` vs `fixed`).
   A ColumnSelection silently getting NO rid key is the exact signal that was
   missing.
2. Step 12/13: log base_combo count, full_combo count, and **combos pruned for
   missing rids**. WARN when `len(full_combos) == len(Cartesian product)` AND no
   rid pruning happened in full-iteration mode (i.e. existence filtering didn't
   run) — that is the smoking gun for this bug.
3. Per-combo run (scifor): when a combo's prepared input is empty, log
   `[empty-combo] <metadata>: input '<name>' had 0 rows` at INFO so empty
   function calls are never silent.

## Regression tests
- `for_each` with a `ColumnSelection` input over fully-iterated schema where the
  data is sparse (some Cartesian combos absent): assert NO empty-input combos
  reach fn, and combo count == number of populated locations (parity with the
  same call using a plain variable input, no column selection).
- Variant case: a location with 2 records + ColumnSelection → rid expansion
  splits into 2 single-record calls (same as without column selection).
- MATLAB bridge parity test mirroring the repro.

## Docs
Offer to add a `docs/claude/` note: "rid tracking does double duty (variant
separation + non-existent-combo pruning); any input wrapper must register its
rid key or the Cartesian product leaks empty combos."
