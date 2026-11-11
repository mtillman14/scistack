# Implementation plan: B (output-edge collision) + multi-row load ordering

Self-contained so it can be executed from a fresh context. Diagnosis was done in
a long debugging session against a real MATLAB GAITRite pipeline.

## Background / causal chain (why)
A `distribute=true` for_each pipeline produced duplicate/orphaned records and
2-row per-cycle tables. Root causes found, in order of depth:
1. ColumnSelection input misclassified as Fixed → empty-combo leak. **FIXED.**
2. Input (`_invocation_input`) edges essentially never recorded (4 in a 60k-record
   DB) → ambiguous loads → re-run drift/orphan cascade. Root: `rid_per_combo` /
   ColumnSelection-coverage groupby included all-NaN finer schema columns (a
   coarser-level input carries them as NaN), and pandas `groupby(dropna=True)`
   dropped every row → empty `combo_to_rids` → no `__upstream` → no input edges.
   **FIXED.**
3. Re-run output-edge collision (`_invocation_output` PK `(invocation_id,
   output_num)` + `ON CONFLICT DO NOTHING`): a re-run/different-PathInput run with
   the same `invocation_id` collides on output slots → new records orphaned. **TODO = Fix B.**
4. Non-deterministic re-runs: identical per-cycle data produced DIFFERENT
   `content_hash`/`record_id` every run. NOT a row-order issue (records are
   single-row with a `DOUBLE[]` distributed to `DOUBLE`; the `__row_order` theory
   is DEAD/scrapped). Root: `canonical_hash` on a MIXED-TYPE DataFrame did
   `to_numpy()` → `object` dtype → `tobytes()` = Python POINTER bytes
   (non-deterministic); column order and the pandas index compounded it.
   **FIXED** (canonical-hash hardening).

PathInput is intentionally excluded from `invocation_id` (folder moves must not
recompute); content-addressing already makes folder changes idempotent, so a
`name=` kwarg is NOT needed — Fix B handles the collision structurally.

## ALREADY DONE (committed to working tree; tests green)
- `scidb/src/scidb/foreach.py`:
  - Step 11 wrapper detection by `isinstance` (Fixed vs ColumnSelection vs
    DataFrame); ColumnSelection drives PRUNING ONLY (`colsel_params` /
    `colsel_existence` / `_colsel_combo_present`), not rid expansion.
  - **#2 fix**: both `rid_per_combo` and `colsel_existence` groupbys now use
    `[k for k in _lookup_keys if k in df.columns and not df[k].isna().all()]`.
  - Logging: Step 11 per-input rid decision; Step 12 colsel-prune count + a
    record-multiplicity diagnostic (warns on >1 record_id per location); a WARN
    when full-iteration keeps the whole Cartesian product with no pruning.
  - `_save_results` input-provenance-source log + WARN when no binding source.
- `scifor/src/scifor/foreach.py`: `as_table` assembly excludes internal `__`
  columns (`_prepare_input`, `_run_column_iteration`, `_extract_data`);
  `[empty-combo]` log via `_input_is_empty`.
- `scidb/src/scidb/database.py`: `_find_record` latest-collapse diagnostic
  (classifies >1 survivor per location as distinct schema_id vs distinct variant).
- `scidb/src/scidb/provenance_save.py`: `_commit_graph` WARN when an output edge
  will be DROPPED on `(invocation_id, output_num)` collision.
- Tests: `tests/test_column_selection_combo_pruning.py`,
  `tests/test_coarse_input_provenance.py`. Existing suites pass.

## Fix B — cross-run output-edge collision (`provenance_save.py`)
Today `record_run`'s assembly loop assigns `output_num` from in-memory state only
(`output_edges`, `inv_cursor`, `rid_slot`, lines ~445-455); `_commit_graph`
inserts with `ON CONFLICT (invocation_id, output_num) DO NOTHING`.

### IMPLEMENTED — IMMUTABLE design (no upsert, no exclusion)
User pushed back on upsert/`excluded` mutation (immutable-records philosophy; no
upserts exist elsewhere). The actual bug (288 orphans) was DISJOINT locations, and
that needs only an immutable APPEND. Implemented:
- **B1 (seed committed slots, once per inv_id):** the first time a run touches an
  `invocation_id`, load its committed edges:
  `SELECT output_num, output_record_id FROM _invocation_output WHERE invocation_id=?`
  → seed `output_edges[(inv,onum)]=rid`, `rid_slot[(inv,rid)]=onum`,
  `inv_cursor[inv]=max(onum)+1`.
- **B2 (assignment — unchanged logic, now committed-aware):** the existing
  cursor/while-loop now probes past committed slots, so a NEW record gets the next
  FREE `output_num` (append). An identical re-save matches `rid_slot` and keeps its
  committed slot.
- **Commit:** insert ONLY this run's edges (`run_output_edges`), keeping
  `ON CONFLICT DO NOTHING` — which now only ever no-ops on an identical re-insert
  (new records never collide). Committed edges are never rewritten.
- The old "edge will be DROPPED" WARN in `_commit_graph` is repurposed as an
  INVARIANT check (a true collision after seeding ⇒ Fix-B regression).
- NOTHING is mutated or excluded.

### Deferred (immutable read-side, NOT implemented)
Same-location RE-COMPUTATION with genuinely-changed content (needs changed input,
since the content-hash fix makes deterministic re-runs idempotent): the new record
appends at a fresh `output_num`, so BOTH versions are retained (immutable). If a
plain `.load()` should return only the latest, that is a READ-side collapse change
(keep latest-by-timestamp among records at the same schema_id sharing
fn+bp+consumed, ignoring output_num WITHIN a schema_id) — watch the flatten case.
Not the user's situation; left as a follow-up.

### Fix B tests (`tests/test_rerun_output_edges.py`) — IMPLEMENTED
- Two runs sharing one `invocation_id` (constant-only fn) over DISJOINT locations
  → both records keep an `_invocation_output` edge (0 orphans), two distinct slots.
- Identical re-run → no new record, no new edge (idempotent), one linked record.

## ~~Fix — multi-row load ordering~~ (SCRAPPED — wrong theory)
Records are SINGLE-row with a `DOUBLE[]` column distributed to per-cycle `DOUBLE`;
there is no within-record row order to stabilize. `__row_order` is NOT needed.

## Fix — deterministic content hashing (`scicanonicalhash/hashing.py`) — DONE
Root cause of non-deterministic re-runs: `canonical_hash` on a mixed-type
DataFrame called `to_numpy()` → `object` dtype → `tobytes()` = Python pointer
bytes (non-deterministic across runs/processes). Column order and the pandas
index compounded it. Proven by: identical stored per-cycle values, different
`content_hash`.

Implemented in `_serialize_for_hash`:
- `object`-dtype ndarrays hashed BY VALUE (`tolist()`), never `tobytes()`.
- DataFrames hashed PER COLUMN with columns SORTED by name and the index IGNORED
  (column order + index are non-semantic for stored content).
- Series unchanged (already name+values, index-free; benefits from the object fix).
Save-time diagnostic added at `database.py` content_hash (`[content_hash]` logs
the first record's DataFrame columns/dtypes/`to_numpy` dtype/index → confirms the
`object` dtype culprit on a re-run).
Tests: `scicanonicalhash/tests/test_hashing.py` — object-array by value;
mixed-type DataFrame deterministic; column-order invariant; index invariant.

**MIGRATION / BREAKING:** this changes the `content_hash` (hence `record_id`) of
ALL DataFrame-backed records. Existing DBs must be rebuilt (or accept that the
next save of each such variable creates a new record_id). The user is rebuilding,
so acceptable. Mixed-type DataFrame hashes were already non-deterministic, so
nothing stable is lost there.

## Deferred: clean up existing orphans (user said skip for now)
Mark edge-less records `excluded=TRUE`, keeping the latest LINKED survivor per
location; or just rebuild. Do AFTER Fix B so re-running can't re-orphan.

## Verify
```
cd /workspace && python -m pytest scidb/tests scifor/tests scicanonicalhash/tests -q
```
Focused: `scicanonicalhash/tests/test_hashing.py` (content-hash determinism),
`test_rerun_output_edges.py` (Fix B), `test_coarse_input_provenance.py`,
`test_column_selection_combo_pruning.py`, `test_provenance_read.py`,
`test_aggregation.py`, `test_variant_pinning.py`, `test_for_columns.py`.
```
```
