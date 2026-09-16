# NULL list elements must round-trip as NaN, not 0

*2026-09-15. Trigger: `grSides` received `GAITRiteLoaded.L_StepLengths_GR` with a
leading `0` where `loadGaitRiteOneFile` produced `NaN`. Not a variant issue —
see the scidb.log analysis in the conversation: the 19:25 reload wrote 0 new
rows, so the bytes grSides loaded are the bytes the loader saved.*

## Diagnosis

| seam | layer | what happens |
|---|---|---|
| save | `sciduckdb._bulk_insert` (`sciduckdb.py:825-841`) | rows are registered as a pandas DataFrame; DuckDB's pandas scanner stores `NaN` as `NULL`, inside `LIST<DOUBLE>` too. `scidb sql` shows the element as `--` (masked). |
| load | `sciduckdb._storage_to_python` (`sciduckdb.py:254-260`) | DuckDB returns a list with NULLs as `numpy.ma.MaskedArray`; `np.asarray(value, dtype=…)` drops the mask and returns the fill buffer → **0**. Same in the `ndim >= 2` `np.stack` branch. |

Every load path funnels through `_storage_to_python`: `load()` per record
(`database.py:2527`), `load_all_as_df` (`database.py:3163` via
`_storage_to_python_column`), hence both MATLAB routes (`wrap_batch_bridge`,
`flatten_sequences`) — they only ever see deserialised arrays. One owner, one fix.

Scalar float columns are *already* fine: pandas turns a NULL DOUBLE into NaN on
fetch. So the rule "NULL in storage ≡ NaN in Python/MATLAB" is the existing
de-facto contract for scalars; this plan extends it to list elements.

## Decision

Keep NULL as the storage form of NaN (SQL aggregates skip it, which is what
`avg(list_element)`-style queries want; no migration of existing data). Fix the
load side to restore NaN. Content hashes are computed on the in-memory NaN
array before save, so after the fix a save → load → re-hash is stable (today
it is not: the reloaded zeros hash differently).

## Stages

### 1. sciduckdb — restore masked elements as NaN (the fix)

`_storage_to_python`, `ptype == "ndarray"`:
- if `value` (or any row in the `ndim >= 2` branch) is `np.ma.MaskedArray` and
  has any masked element: `np.ma.filled(value.astype(float64), np.nan)`. Integer
  and bool dtypes upcast to float64 — the only dtype that can carry NaN; log
  that upcast at DEBUG with the column's declared dtype.
- otherwise unchanged (`np.asarray(value, dtype=dtype)`).
- Helper `_masked_to_nan(arr)` so the 1-D and stacked branches share it.

Logging (NOTE 2): `_storage_to_python_column` counts restored elements per
column and emits one INFO line per load when non-zero:
`load: restored N NULL list element(s) as NaN in column(s) [...]`. Per-element
DEBUG is too chatty for 420×44 columns.

### 2. sciduckdb — save-side visibility

In `_bulk_df_to_storage_rows` (`ndarray` branch) count NaN elements per array
column; `save_batch` logs at INFO when non-zero:
`save_batch(T): M NaN element(s) in array column(s) [...] stored as NULL`.
This is the line that would have made today's diagnosis a one-grep job.

### 3. Tests (regression)

`sciduckdb/tests/test_null_list_roundtrip.py`:
- save a DataFrame variable with a float array column containing NaN at
  position 0 and mid-vector; `load()` → element is NaN, not 0.
- same via `load_all_as_df` (the for_each path).
- 2-D (`ndim=2`) array column with NaN → `np.stack` branch.
- int array column with an explicit NULL written by SQL → comes back float64
  with NaN (upcast documented).
- content-hash stability: `canonical_hash(loaded) == canonical_hash(saved)`.

`scimatlab` MATLAB test (user runs): `TestDataRoundTrip.test_nan_in_array_column`
— table with a NaN inside a cell vector, `for_each` save then `load(as_table=true)`,
assert `isnan(c(1))`.

### 4. `scidb sql` rendering (small)

`_cmd_sql` prints masked elements as `--`; render them as `NULL` so the CLI
says what the DB holds. Optional, cosmetic.

### 5. Docs

`docs/claude/null-nan-roundtrip.md`: the contract (NULL ≡ NaN), the two seams,
the masked-array trap (`np.asarray` silently drops the mask), the upcast rule,
and which log lines to grep.

## Out of scope / follow-ups noted during diagnosis

- `producing_invocation_batch` picks the **lowest** invocation_id when one
  record_id has several producers (`provenance_query.py:123`), which is why
  grSides saw a `{}` constants group beside the real one. Should prefer the
  newest / current-run-options producer. Separate plan.
- `function_sources_for(fn)` on the MATLAB path hashes the bridge wrapper —
  the same derived hash `4999059dff63` for every MATLAB function — so
  "source NOT captured … recipes have drifted" fires on every MATLAB run.
  Gate it like the `not units` branch (`foreach.py:5578`).
- Downstream consumers of `GAITRiteLoaded` array columns (symmetry, merge,
  plots) were fed zeros; the user should re-run them after Stage 1.
