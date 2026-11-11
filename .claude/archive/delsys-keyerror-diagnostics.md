# Plan: Diagnose & fix `KeyError: 'RHAM'` in `save_batch` (multi_column)

## Symptom
`scidb.for_each(@loadDelsysEMGOneFile, ...)` over 268 EMG files fails at save with:

```
[batch_save] Failed to save batch for DelsysLoaded: 'RHAM'
[error] subject=SS01, session=BL, speed=FV, trial=1/2/3: failed to save DelsysLoaded: 'RHAM'
```

0 records saved → later `DelsysLoaded().load()` finds nothing.

## Root cause (confirmed so far)
- `'RHAM'` is a `KeyError`. In `multi_column` mode the table columns are inferred
  from the **first record only** (`sciduckdb._infer_data_columns`, called at
  `database.py:905`). Row building then does `data_val[col]` for each first-record
  column:
  - Arrow fast path: `database.py:1063` `_arrow_col_arrays[col].append(data_val[col])`
  - Generic path: `sciduckdb.py:520` `flat[col]`
- If any of the 268 records' dict is missing `RHAM`, that lookup raises `KeyError('RHAM')`,
  and the atomic batch insert aborts → nothing saves.

## Key finding: the error log misidentifies the culprit
`foreach.py:3146-3154` `except` block logs `items[:3]` — the **first three batch rows**,
NOT the record that threw. So `SS01/BL/FV/1-3` are red herrings. User manually verified
those files + SS02/BL/FV1 all have the full 10-muscle fieldname set, which is consistent
with the real offender being a different (un-inspected) file among the 268.

Leading hypothesis: one file has an **empty** muscle field (`[]` in MATLAB). `fieldnames`
still lists it, but the struct→dict to_python round-trip may drop empty fields, yielding a
dict missing `RHAM` (or another muscle).

## Step 1 — Find the real culprit (user runs in MATLAB, no code change)
Scan all EMG files, report any whose fieldnames differ from the 10-muscle reference, and
flag empty fields (candidate for being dropped on to_python). (Snippet provided in chat.)

## Step 2 — Fix diagnostics in the save layer (code change)
Make `save_batch` name the **actual** failing record and the **actual** key delta:
- In `database.py` per-record loop (~1060-1069), wrap the column extraction so a missing
  key raises a precise error: record index, the record's schema metadata
  (`subject/session/speed/trial`), expected columns vs `sorted(data_val.keys())`, and the
  specific missing/extra keys.
- Optionally also fix `foreach.py:3149` to log the record tied to the raised error rather
  than `items[:3]`.

This is the diagnostics-first step required by CLAUDE.md NOTE 2 and is correct regardless
of what Step 1 finds.

## Step 3 — Decide handling (pending Step 1 result)
- If empty/missing fields are legitimate data: NULL-fill missing keys (union of keys across
  batch) + warn, so saves succeed and missing muscles load back as `None`. Also fixes the
  silent-drop bug for *extra* keys.
- If it's an upstream conversion bug (empty field dropped): fix in the struct→dict /
  to_python layer so empty arrays survive as empty arrays, not dropped keys.

## Step 4 — Regression test
Add a test in the scidb/sciduckdb layer: `save_batch` of `multi_column` records with a
ragged dict (one record missing a key, one with an extra key) — assert the chosen behavior
(clear error OR NULL-fill) and that the error message names the offending record.

## Step 5 — docs/claude
Offer to write `docs/claude/multi_column_ragged_schema.md` describing multi_column schema
inference (first-record), the ragged-key failure mode, and the chosen contract.

---

## IMPLEMENTED (2026-06-25)

Root cause confirmed: two empty EMG files (`SS33/MID24/SSV/2`, `SS33/POST24/SSV/3`)
returned empty structs → empty dict `{}` → `data_val['RHAM']` `KeyError` → atomic batch
abort → 0 of 268 saved.

Chosen contract: **skip-with-warning** for ANY record that can't fit the batch's storage
schema (empty dict, missing/extra keys, scalar-vs-vector shape mismatch). Persist the rest.

Changes:
- `sciduckdb/sciduckdb.py`: added `_storage_signature(ddb_type) -> (category, depth)` and
  `_record_schema_mismatch(ref_col_types, rec_col_types) -> reason|None`. Coarse category
  (numeric/string/json/temporal) so BIGINT↔DOUBLE coerce but shape/category changes are
  rejected. Exported from `sciduckdb/__init__.py`.
- `scidb/database.py::save_batch`:
  - Reference-schema selection: a leading degenerate (empty-dict) record no longer defines
    the table; fall back to first usable record. All-empty batch → return all `None`.
  - Per-record validation pass (non-dataframe modes): incompatible records skipped with a
    per-record `Log.warn` naming the record metadata + reason.
  - Returns `record_ids` aligned to ORIGINAL input order, `None` in each skipped slot.
- `scidb/foreach.py`: count actual saved (non-None); console+log skip summary; honest
  except-block (traceback + "first batch rows, NOT necessarily the culprit").
- `scimatlab/bridge.py`: `save_batch_bridge` join tolerates `None` slots (empty string,
  row-aligned).
- `scidb/tests/test_integration.py::TestSaveBatchSchemaValidation`: 5 regression tests
  (empty dict, partial keys, shape mismatch, leading-empty reference, all-empty).

Test: `cd scidb && pytest tests/test_integration.py::TestSaveBatchSchemaValidation -v`
Also run full `scidb` + `sciduckdb` suites for regressions.
