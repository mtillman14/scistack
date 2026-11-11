# Plan: Stage 1 — Variant auto-split in aggregation mode (D1)

Implements decision **D1** of
`docs/claude/endpoints-viz-and-stats-design.md`: aggregation-mode `for_each`
splits by upstream branch-param signature by default (one call per signature,
as-if the user wrote `EachOf(Variant(...), Variant(...))`), with an
`AcrossVariants(X)` opt-in that pools rows *with branch_params as columns*.
Ragged variant groups **warn** and proceed.

## Behavior change (breaking, intentional)

Today an unpinned aggregation pools every branch-param variant into one
multi-row DataFrame per combo (identity stripped, warning logged). After this
change the same call produces **one iteration per branch-param signature**,
each with its own output record carrying that group's (conflict-free) upstream
branch_params. Existing pipelines that aggregate over multi-variant inputs
will produce more (and differently-valued) outputs. Old pooled records remain
in the DB untouched (keep-everything).

Example: `for_each(aggregate_sum, {"signal": Filtered}, [Aggregated])` over
4 locations × 2 variants (`low_hz=20`, `low_hz=30`) currently yields 1 result
(pooled sum). After: 2 results — the `low_hz=20` aggregate and the
`low_hz=30` aggregate — distinguished downstream by their branch_params.

## Mechanism (reuse the rid-key seam)

Full-iteration mode already expands combos with `__rid_{param}` columns and
extends the scifor schema with them (`foreach.py` Step 15, ~line 1577;
restored Step 18, ~line 1629) so scifor's per-combo filter selects matching
rows. Aggregation mode currently bypasses this (`rid_keys_for_schema = []`,
`foreach.py:1460`). The split reuses the same seam at **signature**
granularity instead of record granularity:

1. **Signature = canonical form of a record's upstream branch_params dict**
   (`rid_to_bp[rid]`, built in Step 11 ~line 1088). Use the existing
   canonical-hash utility (`scidb/hashing.py` / scicanonicalhash) on the dict;
   empty dict → the "no-variants" signature.
2. In the aggregation branch (Step 12, `foreach.py:1383`), for each loaded
   DataFrame input with a `__rid_{param}` column: map each row's rid →
   signature and write it to a new **`__vsig_{param}`** column (then strip
   `__rid_*` as today).
3. **Expand base combos** over the observed signature combinations per combo
   (Cartesian across multi-variant inputs, mirroring full-iteration rid
   expansion; combos with a signature absent at that schema location are
   dropped, as rid expansion drops no-data combos). Single-signature inputs
   (the overwhelmingly common case) expand 1:1 — no behavior change when
   there are no variants.
4. **Extend the scifor schema** with the `__vsig_*` keys
   (`rid_keys_for_schema` equivalent) so scifor filters each call's rows to
   the signature group. Ensure `__vsig_*` columns are stripped from the
   DataFrame the user function receives (same guarantee `__rid_*` has today)
   and from the result table (`_apply_introspect`, `foreach.py:557`, already
   handles `__rid_*`; add `__vsig_*`).
5. **`_combo_to_rids`** (`foreach.py:1443`, feeds the save path's
   branch_params merge + provenance at ~line 2885): key it by
   (iterated-combo + signature values) so each output saves with only its
   group's contributing rids. Within a group the branch_params merge is
   conflict-free **by construction** — the existing
   "branch_params key ... overwritten" warning should no longer fire for
   split aggregations (it remains reachable via `AcrossVariants` /
   multi-input joint groups with genuinely conflicting namespaced keys).
6. **Output identity for free:** each group's output inherits its group's
   merged upstream branch_params via the existing save path, so two groups at
   the same schema location save as two variants — the same propagation that
   distinguishes variants in full-iteration mode. Verify with a test, not new
   code.

## `AcrossVariants(X)` opt-in wrapper

New `scidb/src/scidb/across_variants.py`, modeled on `variant.py` /
`fixed.py` wrapper conventions; exported from `__init__.py`.

- **Semantics:** for this input, skip signature-splitting (no `__vsig` column,
  no combo expansion contribution) and instead attach the rows' upstream
  branch_params as ordinary DataFrame columns — one column per namespaced key
  (e.g. `bandpass.low_hz`) — so the user function can group by specification
  (multiverse analysis). Values come from `rid_to_bp` per row; missing key for
  a row → NaN. If a bp column name collides with an existing data column,
  warn and skip that column.
- **Composition:** wraps a variable type, `ColumnSelection`, or `Fixed`
  (mirror `Variant`'s rules); `AcrossVariants(Merge(...))` and
  `AcrossVariants(EachOf(...))` are errors; `AcrossVariants(Variant(...))` is
  legal (pin some params, spread the rest as columns).
- **Identity:** `to_key()` string (e.g. `AcrossVariants(Filtered)`) written
  into the `__inputs` config key, same as `Variant.to_key()` — a pooled run
  and a split run of the same function must not collide.
- **Only meaningful in aggregation mode:** in full-iteration mode, warn and
  behave as the bare input (each combo already sees exactly one variant row).

## Ragged-group warning (decided: warn, not error)

After grouping, per input: compute each signature's set of schema locations
vs. the union across signatures. Any signature missing locations →
`warnings.warn` (so tests can catch it, consistent with the existing
branch_params warning) **and** `Log.warn`, naming the input, the signature's
branch_params, and the missing locations. Aggregation proceeds on the partial
group.

## Logging (NOTE 2)

- Aggregation split summary: per input, number of signatures, group sizes,
  and per-signature branch_params (Log.info).
- Ragged-group warning as above (Log.warn + warnings.warn).
- `AcrossVariants` pooling notice: input name, distinct signatures pooled,
  bp columns added (Log.info).
- Keep the Step 12 multi-record diagnostic but rephrase: with auto-split it
  now indicates *split groups created*, not pooling.

## Files

| File | Change |
|---|---|
| `scidb/src/scidb/foreach.py` | Step 12 aggregation branch: vsig columns, combo expansion, schema extension, `_combo_to_rids` keying, warnings; `_apply_introspect` strips `__vsig_*`; AcrossVariants handling in Step 11/12 (bp-column attach, skip split) |
| `scidb/src/scidb/across_variants.py` | new wrapper (+ `__init__.py` export) |
| `scidb/src/scidb/foreach_config.py` | serialize `AcrossVariants.to_key()` into `__inputs` (mirror Variant) |
| `scidb/tests/test_aggregation_with_variants.py` | rewrite pooled expectations → split expectations (see below) |
| `scidb/tests/test_aggregation.py` | should pass unchanged (no variants) — run to confirm |
| `docs/claude/scidb-for-each-internals.md` | replace "skips rid expansion / pools" description with split behavior |
| `docs/claude/variant-branch-param-pinning.md` | "Out of scope: auto-grouping" → implemented, link here |
| `docs/claude/endpoints-viz-and-stats-design.md` | mark D1 implemented |

## Tests

Rewrites in `test_aggregation_with_variants.py` (current file asserts pooling):

- `test_aggregates_all_upstream_variants`: 4 locations × 2 variants → now
  **2 iterations**, values 480 (`low_hz=20`) and 720 (`low_hz=30`), not one
  1200 pool. Analogous updates: partial aggregation per subject → 2×2
  iterations valued 120/180 per subject; uneven-locations test; 5-variant
  test → 5 iterations.
- branch_params tests: each output's `branch_params` now equals its group's
  (e.g. `bandpass.low_hz == 20`), no conflict warning fired;
  `test_aggregated_record_warns_on_conflicting_branch_params` repurposed to
  assert the conflict warning is **gone** for split aggregations.

New tests:

- **Split identity:** two groups at one schema location save as two loadable
  variants (`Variant(Aggregated, low_hz=20).load()`-style disambiguation or
  `load` returning both with distinct branch_params).
- **Provenance:** each split output's upstream rids are exactly its group's
  (via `get_upstream_provenance` / `_combo_to_rids` path).
- **Multi-input Cartesian:** two multi-variant inputs → signature product,
  matching full-iteration rid-expansion semantics.
- **Ragged warning:** variant present at only some locations → warning names
  the signature + missing locations; partial aggregate computed.
- **No-variant fast path:** single-signature inputs behave byte-identically to
  today (values, combo count, no new warnings).
- **`AcrossVariants`:** pooled single call; bp columns present with correct
  per-row values; identity differs from the split run; collision warning;
  full-iteration-mode warning; `AcrossVariants(Variant(...))` composition.
- **Grand aggregation regression** (design doc open item): zero iterated
  keys → single combo; with variants → one call per signature.
- **skip_computed cross-skip guard:** run split aggregation twice with
  `skip_computed=True` → second run skips both groups (not just one);
  then add a third variant upstream → only the new group computes. RISK: the
  skip gate (`_find_skip_gate_record`, `foreach.py:589`) matches on function +
  constants + schema combo — it may not discriminate groups that differ only
  in upstream branch_params. If the test exposes cross-skipping, extend the
  gate's signature match to include input record_ids / upstream bp (fix
  belongs in scidb).

Run: `pytest scidb/tests/test_aggregation_with_variants.py scidb/tests/test_aggregation.py scidb/tests/test_variant_pinning.py scidb/tests/test_plotting.py -x -q`
(user runs tests and reports back; no Python in assistant env).

## Explicitly out of scope (later stages)

- `finalized=True/False` draft/record mode (stage 3, D3).
- `stat_` leaves / csv-stats (stage 2, D5 — blocked on csv-stats API).
- Artifact metadata stamping (stage 3, D4).
- MATLAB parity work (stage 4, D7). NOTE: MATLAB `for_each` routes loading
  through the Python bridge, so the split *semantics* likely arrive for free;
  `AcrossVariants` needs a MATLAB builder + `describe_input_for_python`
  `'across_variants'` kind + bridge reconstruction — deferred to stage 4, but
  keep the Python wrapper bridge-friendly (plain dict of fields).

## Risks / watch items

1. **skip_computed cross-skip** (test above; likely needs a gate extension).
2. **Combinatorial growth** with several multi-variant inputs — same exposure
   as full-iteration rid expansion; log group counts so blowups are visible.
3. **Downstream consumers of split outputs**: multiple variants at one
   location now appear where one pooled record used to be — downstream
   *full-iteration* steps already handle this (rid expansion); downstream
   *aggregations* split again (consistent). Mention in migration note.
4. **`inspect`/CLI surfaces** that display aggregation provenance
   (`_record_id_*` / `_branch_params_*` introspection columns) — confirm they
   render per-group rows sensibly (read-only check, no code expected).
