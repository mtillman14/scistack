# Plan: Fix re-run output-edge orphaning (cascading duplicate records)

## Confirmed root cause
`_invocation_output` PK is `(invocation_id, output_num)`. `invocation_id` is
deterministic (fn_hash + as_table + distribute + constant bindings; NOT inputs)
and the distribute fan-out reproduces the same `output_num` sequence each run.
When a for_each is re-run and produces NEW content (new content-addressed
`record_id`), `_commit_graph` inserts the output edges with
`conflict_cols=["invocation_id","output_num"]` → `ON CONFLICT DO NOTHING`
(`provenance_save.py:547`). The prior run's edge wins; the NEW record's edge is
silently dropped → the newest record is ORPHANED (no producing invocation).

At load, orphaned records have no invocation → collapse assigns them the
`"__raw__"` variant (`database.py:1473`), so each schema location keeps
1 linked + 1 (latest) orphan = 2 records → multi-row per-combo tables.

### Cascade (observed)
- `GAITRiteLoadedCycle`: 8040 linked, 24120 orphaned (4 runs; only run 1 linked).
- `GAITRiteLoaded` (its distribute=True input): 321 linked, 288 orphaned.
- Deterministic fn + 4 DISTINCT content_hashes per location ⇒ input changed each
  run ⇒ `GAITRiteLoaded`'s ambiguous load fed different data each run ⇒ new
  content ⇒ new orphans. Compounds per level and per re-run.

Identical re-runs are already idempotent (content-addressed `record_id`;
`generate_record_id` hashes class|schema|content|meta) — only `_record_save`
appends. The bug only bites when content legitimately changes.

## DEEPER ROOT CAUSE (found): input edges are essentially never recorded
DB has only **4 `_invocation_input` rows total** (3 on distribute invocations) for
60061 records. So consumed-input provenance is systemically missing — not a
distribute-only gap. This both breaks lineage AND is the precondition for the
orphan cascade (no recorded input ⇒ outputs can't be tied to the input version
they consumed ⇒ ambiguous loads ⇒ drifting content on re-run ⇒ new records ⇒
edge-collision orphaning).

Input edges are built in `_save_results` (`foreach.py:2962-3021`) ONLY from a saved
row's `__rid_*` columns (full iteration), `combo_to_rids` (aggregation), or
`lineage_fixed_rids` (Fixed). So one of those must be present at save. The MATLAB
save routes through `_for_each_save_resolved → _save_results` (bridge:843); the
bridge reverse-maps sanitized `x__rid_x → __rid_x` (bridge:802-818) IF
`rid_rename_map` was populated and MATLAB returned those columns.

### Bug #2 ROOT CAUSE FOUND + FIXED
Confirmed via logs: Step 11 DID register `__rid_gaitriteTable` (rid key present,
609 record_ids mapped), yet no input edges resulted. Cause: `rid_per_combo` (and
the ColumnSelection coverage set) grouped via
`df.groupby([k for k in _lookup_keys if k in df.columns])`, which INCLUDED the
all-NaN `cycle` column (GAITRiteLoaded is stored at subject/session/speed/trial,
so its spread carries `cycle` as all-NaN). pandas `groupby(dropna=True)` dropped
every NaN-key group → empty `rid_per_combo` → empty `combo_to_rids` → no
`__upstream` → NO `_invocation_input` edges. Systemic for any variable input
coarser than the full schema.

FIX (DONE, `foreach.py`): both groupbys now exclude all-NaN columns —
`[k for k in _lookup_keys if k in df.columns and not df[k].isna().all()]` — so a
coarser input groups by the keys it actually populates. Mapping keys are still
built over the full `_lookup_keys` (missing filled with ""), so downstream combo
matching is unchanged. Regression test: `tests/test_coarse_input_provenance.py`.

## Diagnostic logging (DONE)
- `_commit_graph` (`provenance_save.py`): `Log.warn`s when an output edge will be
  DROPPED because `(invocation_id, output_num)` already points to a different
  record_id (the orphaning event).
- `_save_results` (`foreach.py`): logs the input-provenance sources present
  (rid_keys / `__rid_*` columns / combo_to_rids / fixed rids) and `Log.warn`s when
  NONE is present (⇒ records saved with no `_invocation_input` edges).

## Fix (NEEDS APPROVAL — touches core provenance/version semantics)
At output-edge assignment, when `(inv_id, output_num)` already has a COMMITTED
edge to a different `record_id`, branch on consumed-input identity:
- **Same `consumed` input schema locations** → genuine re-computation:
  - update the edge to the new record_id (newest wins),
  - mark the superseded old record `excluded=TRUE` (kept for history, hidden from
    latest-version loads).
- **Different `consumed`** (legitimate cross-input variant, e.g. where=L/R):
  - assign the new record the next free `output_num` so BOTH keep edges.

This preserves where=-variant correctness (the reason the edge can't simply be
overwritten) while stopping the orphan cascade and restoring idempotency.

Open questions for implementation:
- `consumed=()` was observed for these records — confirm consumed-input edges are
  being recorded for distribute outputs at all (may be a second gap).
- Where to compute "next free output_num" across runs (needs a global probe, not
  just the per-run cursor).
- Transaction scope: the supersede UPDATE + `excluded` flip must be inside the
  same `_commit_graph` transaction.

## One-time cleanup of existing orphans (separate, reversible-ish)
Mark existing edge-less records `excluded=TRUE` (don't hard-delete) so current
pipelines load one record per location immediately:
```sql
UPDATE _record SET excluded = TRUE
WHERE type IN ('GAITRiteLoaded','GAITRiteLoadedCycle')
  AND record_id NOT IN (SELECT output_record_id FROM _invocation_output)
  AND <newer linked or orphan exists at same schema_id>;  -- refine: keep latest LINKED
```
Refine to keep the correct survivor per location (prefer the latest LINKED
record; if none linked, keep latest orphan and re-link). Validate counts before/
after. Do this AFTER the code fix so re-running can't re-orphan.

## Tests (regression)
- Re-run a deterministic distribute for_each with CHANGED content → latest record
  is linked; prior superseded record is excluded; load returns exactly one.
- where=-style: two runs, same fn+constants, DIFFERENT consumed inputs → both
  records keep edges (no supersede); load returns both variants.
- Identical re-run → still idempotent (no new record; only `_record_save` grows).

## Docs
`docs/claude/` note: output-edge PK `(invocation_id, output_num)` vs load-time
variant identity (which adds consumed inputs); why re-run collisions must branch
on consumed inputs; orphan = `"__raw__"` symptom.
