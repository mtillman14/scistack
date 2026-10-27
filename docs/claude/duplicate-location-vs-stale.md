# Duplicate location in one batch vs. a stale value across runs

Two different situations can leave a schema location (e.g. `subject=2455369`)
with more than one candidate record. They look alike to a reader, but they
have different causes and different owners.

Related: `docs/claude/multi-column-save-schema.md` (save_batch contract),
memory `project_same_invocation_latest` (the supersede rule),
plan `.claude/plan-duplicate-location-in-batch.md`.

---

## 1. How a DataFrame becomes many records

When a saved DataFrame has schema-key columns, it is **distributed**:
`BaseVariable.save` -> `save_from_dataframe` (`scidb/src/scidb/variable.py`)
turns each row into one `(data, metadata)` item, and all items go to
`DatabaseManager.save_batch` in one call. for_each outputs
(`foreach.py`, `_batch_save_outputs`) and MATLAB (`scimatlab/bridge.py`,
`save_batch_bridge`) end in the same `save_batch`.

A CSV loaded once (no iteration over schema keys) whose rows carry a `subject`
column is therefore saved per subject. If the CSV has **two rows for one
subject**, the batch holds two items at one location.

- Rows byte-identical -> same content hash -> same `record_id` -> stored once.
  Harmless.
- Rows differ -> two record_ids, one location, **same invocation, same save
  instant**.

## 2. Why the second case reads as "stale"

`provenance_query._supersede_same_invocation` decides "latest": of the records
one invocation wrote to a location, only the newest `_record_save` is latest,
**but records saved at the same instant stay latest together**, so a run's own
batch is never split. The two duplicate rows were saved at the same instant,
so both stay latest, and readers (Plot Studio, inventory, load) see two values
where one is expected.

## 3. Telling them apart

| | Duplicate location (data error) | Stale value (normal history) |
|---|---|---|
| Writers | one invocation, **one save batch** | two invocations / two batches |
| Save timestamps | identical | different |
| Detectable at | save time, exactly (the batch itself) | load time (supersede rule) |
| Right response | refuse the save; user fixes data or keys | newest wins; old record stays in history |

The key point is that **within one batch, a location must not hold two
different records of the same variant** (same version metadata incl.
`__invocation_id`, same `__branch_params`). Two records at one location with
different `__branch_params` are legitimate variants. This is a per-location
rule, NOT a content comparison across locations; the old cross-location
"IDENTICAL data" warning was removed on purpose and stays removed.

**Trap (found by the for_each test, 2026-10-27):** for_each's flatten mode
copies every scalar data column of a spread row into that record's
`__branch_params` ("dynamic discriminators", `foreach.py`). Two differing CSV
rows for one subject therefore carry different branch params and looked like
two variants, so nothing was refused. The rule now ignores a branch-params
entry that names one of the record's own data columns
(`_data_column_names`): a variant is told apart by its inputs and constants,
never by the value it stores.

Owner of the rule: `save_batch` (planned `DuplicateLocationError`, raised
before any write, as `ColumnSetChangedError` does).

## 4. What the user does about a duplicate

- The rows are genuinely different observations (two visits) -> add a finer
  schema key (e.g. `visit`/`session` column) so each row has its own location
  (a new project database: schema keys are fixed at creation).
- The rows should be combined -> aggregate in the function before returning.
- One row is a mistake -> remove it upstream (the CSV).

## 5. How it surfaces

Built 2026-10-27. `save_batch` → `_check_duplicate_locations` (rule:
`database._colliding_locations`, called after per-record validation and
before ANY write: table, schema ids, rows) → `DuplicateLocationError`.

The exception owns all wording: `str(exc)` for terminal/MATLAB/journal, and
`exc.remedy` for the GUI popup (title, explanation, duplicate locations with
the columns that differ, two steps). Through `OutputSaveError` it also gets
the usual red badge, FailuresPanel entry and toast. Plumbing:
`error-surfacing.md` §8.

The two steps:
- **Change the dataset schema** — explanation only. A database's schema keys
  are the columns of `_schema`, fixed when it is created (no add-key API;
  user decision 2026-10-27), so this means a new project database with the
  extra key (e.g. `visit`) and running again.
- **Change the data** — **Open <fn>** button: remove the extra row in the
  source file, or combine the rows in the function.

Diagnostics: `save_batch(X): N item(s) -> L location(s), V variant group(s),
… duplicate location(s)` (DEBUG, every batch) and the `[batch_save] …
refusing the save: …` WARN with every collision.
