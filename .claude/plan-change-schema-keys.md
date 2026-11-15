# Plan: Change the schema keys of an empty dataset (GitHub issue #10)

*Drafted 2026-11-15. Status: awaiting approval.*

## The issue

> When the schema keys are initialized using different keys than the first data
> loading step encounters (such that there is possibly more than one row per
> unique schema key combination), AND the dataset is 100% empty (because this is
> the first load) the popup assist window MUST offer the option to change the
> schema keys to the set found in the dataset at the PathInput's location.
> If the dataset is non-empty, the popup assist window MUST state that, and not
> allow the user to perform this action.

The popup is the existing `DuplicateLocationError.remedy` → `RemedyDialog`
(docs/claude/duplicate-location-vs-stale.md §5). Its "Change the dataset schema"
step has been explanation-only (`action: None`) since 2026-10-27, because schema
keys are the `_schema` columns, fixed when a database is created. This plan
makes that step actionable when, and only when, the dataset is empty.

## User decisions (2026-11-15)

- **Empty** = no variable holds a saved record. Failure outcomes, the run log,
  invocations and internal record types (constants, PathInput, glue,
  selection) are run history, not data. Declarations (exclusions, expectation
  marks, key types, canvas) are not data either; they are carried over.
- **One owner of "empty"**: a new `is_empty(db)`, used by `clear_data` AND the
  key change. Not a dry run of `clear_data`: `clear_plan` counts `_run_outcome`
  rows, which the failed first load itself writes, so the database would never
  look empty at the moment the popup appears.
- **The proposal**: the default new keys are the union of the PathInput's
  placeholder keys and the columns of the dataset being loaded. In a popup the
  user can add, delete, reorder and rename the proposed keys. The existing keys
  are shown in the same window.
- **Scripts with hard-coded keys**: `configure_database()` raises a descriptive
  error that names a schema key mismatch and shows the keys passed in beside
  the database's keys, so the user knows to change the keys they pass.

## One owner per concept

| Concept | Owner | Consumers |
|---|---|---|
| "does this dataset hold data" | NEW `scidb/store/emptiness.py`: `variable_record_counts(db)`, `is_empty(db)` | `clear_data.clear_plan`, `DuplicateLocationError` remedy, `change_schema_keys`, `scidb.is_empty` |
| which tables are built from the schema-key columns | `scidb/store/tables.py` registry (the `lambda keys:` DDL): NEW accessor `schema_keyed_tables()` | `change_schema_keys` |
| the default proposal (union and order) | NEW pure `scidb.store.schema_keys_change.propose_schema_keys(path_input_keys, data_columns)` | GUI RPC `schema_key_proposal` |
| a valid list of schema keys | NEW `validate_schema_keys(keys)` in the same module (non-empty, unique, not `schema_id`/`schema_level`, no `__` prefix) | `change_schema_keys`, GUI `create_db` |
| changing the keys | NEW `scidb.store.schema_keys_change.change_schema_keys(db, new_keys, *, renames)` | GUI RPC `change_schema_keys` |
| the dataset's columns at the failed save | `DuplicateLocationError.data_columns` (from `db_helpers._data_column_names` over the batch) | remedy payload |
| a PathInput's keys | `scifor.PathInput.placeholder_keys()` (existing) | GUI RPC `schema_key_proposal` |
| the popup's wording | `DuplicateLocationError.remedy` (existing owner) | `RemedyDialog` |
| the mismatch message | NEW `SchemaKeysMismatchError(SciStackError, ValueError)` raised by `SciDuck._validate_schema_columns` | `configure_database`, `DatabaseManager.reopen`, MATLAB |

## Steps

### is_empty_owner (scidb)

- `scidb/store/emptiness.py`:
  - `variable_record_counts(db) -> dict[str, int]`: the `SELECT type, COUNT(*) FROM _record GROUP BY type` that `clear_plan` does today, without `INTERNAL_RECORD_TYPES`. The query moves here.
  - `is_empty(db) -> bool`: an `EXISTS` over the same filter, so it never counts a large table. INFO log: `is_empty: True` or `is_empty: False (first variable found: X)`, with ms.
- `clear_data.clear_plan` takes `by_variable` from `variable_record_counts`. Its `table_rows` stay unchanged: they answer "what would a clear delete", which is a different question.
- Export `scidb.is_empty(db=None)` (the current db when `None`).
- Tests `scidb/tests/test_is_empty.py`:
  - a fresh DB is empty;
  - after a refused duplicate-row save it is **still empty** (the trap);
  - after one good save it is not empty;
  - after `clear_data` it is empty again;
  - parity: `clear_plan(db).total_records == 0` exactly when `is_empty(db)`.

### mismatch_error (scidb)

- `SchemaKeysMismatchError(SciStackError, ValueError)` in `model/exceptions.py`, with `passed` and `existing` attributes. Subclassing `ValueError` keeps existing `except ValueError` callers working.
- Message (draft):
  ```
  Schema key mismatch for <path>.
    Keys passed in:          ['subject']
    Keys in the database:    ['subject', 'visit']
  The database's schema keys are set when it is created (or changed in the
  GUI while it is empty). Pass the database's keys instead:
    configure_database(<path>, ['subject', 'visit'])
  ```
  When the two lists hold the same names in a different order, add one line saying the order differs (order is the hierarchy).
- Raised by `SciDuck._validate_schema_columns`. It reaches `configure_database`, `DatabaseManager.reopen` (another process whose keys were changed under it) and MATLAB (`scidb.configure_database` shows the message).
- Tests: wrong keys, a missing key, an extra key, the same keys in a different order. The message names both lists. MATLAB gets one test in `TestConfigureDatabase.m`, or the nearest existing file, checking that the message passes through.

### change_schema_keys (scidb)

`scidb/store/schema_keys_change.py`:

- `validate_schema_keys(keys)` raises `ValueError` naming the bad key.
- `propose_schema_keys(path_input_keys, data_columns) -> list[str]`: the PathInput keys in template order (path nesting = coarse to fine), then each data column not already listed, in column order. Pure; unit-tested.
- `change_schema_keys(db, new_keys, *, renames=None, reason) -> SchemaKeysChange`:
  - `renames` is `{old_key: new_key}`, for existing keys the user renamed in the popup.
  - Refuses (`SchemaKeysChangeRefused`, one line) when:
    - `not is_empty(db)`, re-checked inside the transaction because a check made when the popup opened can go stale;
    - the new keys fail `validate_schema_keys`;
    - an intent row (exclusion, expectation mark) has a non-NULL value under a key that is being deleted. That data would be dropped silently (memory `feedback_never_drop_data`), so the refusal lists the rows.
  - In one transaction, for each table in `schema_keyed_tables()`:
    - **history** (`_schema`, `_run_outcome`): empty them (no data references them), then DROP and CREATE them with the registry's DDL for the new keys.
    - **intent** (`__scidb_schema_overrides`, `_expectation_mark`): copy the rows into the new shape. A kept key keeps its column, a renamed key moves to its new name, and a new key is NULL (NULL = every value, the tables' existing meaning).
  - Declarations by key name: a renamed key's `_schema_key_type` and `_schema_key_expectation` rows carry over by appending rows (both tables are append-only): one for the new name, and a NULL row for the old name. A deleted key gets a NULL row and is reported in the result.
  - `[schema_keys]` order in the project config: find its writer while building this step (`dataset_schema_key_order` reads it), apply the renames, and report any deleted key it still names. Never silently edit the user's config beyond the renames.
  - After commit, close `db` so no caller keeps using stale keys in memory. The caller reopens through its normal open path. The docstring says so.
  - Logs: INFO `old -> new`, renames, and per-table rows kept or copied with ms (phases named by table, never numbered); WARN on every refusal.
- Tests `scidb/tests/test_schema_keys_change.py` (real DuckDB):
  - change, reopen with the new keys, save the CSV that collided before: the save succeeds;
  - non-empty → refused, nothing changed;
  - reopening with the old keys raises `SchemaKeysMismatchError`;
  - a rename carries the key type and the exclusion rows;
  - a deleted key that an exclusion names → refused;
  - reorder only;
  - invalid keys.

### remedy_offers_change (scidb)

- `DuplicateLocationError(variable, collisions, schema_keys, *, data_columns, dataset_empty)`. `_check_duplicate_locations` fills both: `data_columns` = the ordered union of `_data_column_names` over the batch items; `dataset_empty` = `is_empty(self)`. It is cheap (one EXISTS) and runs before any write.
- The "Change the dataset schema" step:
  - empty: `action: "change_schema_keys"`, text "This dataset has no saved data yet, so its schema keys can still be changed…", plus `data_columns` and `schema_keys` in the remedy payload;
  - not empty: `action: None`, text that says the dataset already holds data, so its keys can't be changed here, followed by the existing new-database explanation.
- The `str(exc)` message gains one clause for the terminal and MATLAB: the keys can still be changed (empty), or can't (not empty).
- Tests: extend `scidb/tests/test_duplicate_location_in_batch.py` for both branches, plus `data_columns` order.
- MATLAB: the remedy already rides `note_run_failure` → `run_failure_group_json`. Check that the new fields pass through (`scimatlab/tests`).

### gui_schema_keys_dialog (GUI)

- Backend, in the service that owns `set_distribute`:
  - `schema_key_proposal {node_id, data_columns}` → `{existing, proposed: [{key, source: "path_input"|"data_column"}], dataset_empty}`. It finds the PathInputs wired into the node (the existing input-binding owner; verify which while building), calls `placeholder_keys()` on each via `project.registry.get_path_input`, then `propose_schema_keys`. MATLAB PathInputs are in the same registry.
  - `change_schema_keys {keys, renames}`: calls scidb, then reopens through `runtime/db` (`close_db` + `init_db`, the startup path that reads the keys fresh), then refreshes the graph. It returns the scidb result (reported leftovers included) or the refusal text.
  - INFO logs on both.
- Frontend: NEW `SchemaKeysDialog.tsx`, opened from `RemedyDialog`'s new `change_schema_keys` button ("Change the schema keys…"):
  - left: the existing keys (read-only);
  - right: an editable list seeded with the proposal. Each row has its source tag, an inline rename, up/down buttons, and delete. "Add key" adds a row. Rows that came from an existing key remember it, so a rename is sent as `renames`.
  - one note: "Measure columns are listed too; delete every column that is not a key". A data column renamed here must still match a column your function returns, or rows get no location.
  - buttons: **Change keys and run again** (RPC, then `RERUN_NODE_EVENT` like `distribute_rows`) and **Cancel**. A refusal shows inline and the dialog stays open.
  - `runFailures.ts`: add `'change_schema_keys'` to `RemedyStep.action`; `console.debug('[remedy] action=change_schema_keys …')`.
- Rebuild the frontend bundle (memory `project_frontend_bundle_rebuild`).
- Tests (`scistack-gui/tests`): the proposal RPC (union and order, both sources tagged); apply → db reopened with the new keys, then the failed node re-runs cleanly; non-empty → refused.
- Manual check: add a section to `docs/gui-manual-testing-todo.md`.

### docs

- NEW `docs/claude/changing-schema-keys.md`: the rule, the owners, what each schema-keyed table does on a change, why "empty" ignores run history. Index it in `docs/claude/README.md`.
- `duplicate-location-vs-stale.md` §4/§5: the step is now actionable on an empty dataset.
- `docs/api/scidb.md`: `is_empty`, `change_schema_keys`, `SchemaKeysMismatchError`.
- Update memory `project_duplicate_location_remedy` ("explain only" no longer holds for an empty dataset).

## Out of scope

- Changing keys on a non-empty dataset (the issue forbids it). That still means a new project database.
- Migrating user scripts: the mismatch error tells the user what to change.

## Order and commits

One commit per step, and the user runs one package's pytest between commits:
`is_empty_owner` → `mismatch_error` → `change_schema_keys` → `remedy_offers_change`
(scidb, then scimatlab) → `gui_schema_keys_dialog` (scistack-gui) → `docs`.
