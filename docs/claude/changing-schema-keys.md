# Changing the schema keys of an empty dataset

GitHub issue #10; plan `.claude/plan-change-schema-keys.md`; built 2026-11-15.

## The rule

Schema keys are the columns of `_schema`, fixed when a database is created.
They can still change **while no variable holds a saved record**. The case
that needs it: the first load collides (two rows per `subject`) because the
keys were chosen too coarse. Once any data is saved the keys are fixed and the
way out is a new project database.

## "Empty" has one owner

`scidb.store.emptiness` (`is_empty(db)`, exported as `scidb.is_empty`;
`variable_record_counts(db)`). Empty = no variable has a `_record` row whose
type is not in `INTERNAL_RECORD_TYPES`. Everything else is **not data**:

- failure outcomes (`_run_outcome`: the failed first load itself writes them),
  the run log, invocations, internal record types (constants, PathInput, glue,
  selection) are run history;
- exclusions, expectation marks, key types, the canvas are declarations and
  are carried over.

That is why "empty" is not a dry run of `clear_plan`: its table counts include
`_run_outcome`, so the database would never look empty when the popup opens.
`clear_plan` takes its per-variable counts from `variable_record_counts`.

## The owners

| Concept | Owner |
|---|---|
| does the dataset hold data | `scidb.store.emptiness` |
| which tables are built from the keys | `scidb.store.tables.schema_keyed_tables()` |
| a valid key list | `schema_keys_change.validate_schema_keys` |
| the default proposal | `schema_keys_change.propose_schema_keys` (PathInput keys in template order, then data columns not already listed) |
| the change | `schema_keys_change.change_schema_keys(db, keys, renames=, reason=)` |
| the variable views' SQL | `db_save.variable_view_sql` (re-used to rebuild every view) |
| the popup wording | `DuplicateLocationError.remedy` (`_schema_step`) |
| the mismatch message | `SchemaKeysMismatchError` |
| the GUI half (find wired PathInputs, reopen) | `scistack_gui.services.schema_keys_service` |

## What a change does

One transaction. Refuses (`SchemaKeysChangeRefused`) when the dataset holds
data (re-checked inside the transaction: the popup's answer can be stale),
when the keys are invalid, or when an exclusion / expectation mark has a value
under a key that would be deleted (it would be dropped silently).

| Table kind | On a change |
|---|---|
| history (`_schema`, `_run_outcome`) | emptied, dropped, re-created for the new keys. Remaining `_record.schema_id` values are set NULL (the ids no longer exist). The id sequence is kept, so ids are never reused. |
| intent (`__scidb_schema_overrides`, `_expectation_mark`) | rows copied into the new shape. A kept key keeps its column, a renamed key moves, a new key is NULL (= every value). |
| `_schema_key_type`, `_schema_key_expectation` | append-only and keyed by name: a renamed key's newest row is appended under the new name, and the old name gets a NULL row. A deleted key gets a NULL row. |
| variable views | dropped first (they reference `_schema`'s columns), re-created after. |

`[schema_keys]` in the project config is the user's file: it is never edited.
A declaration naming a renamed or deleted key is reported in
`SchemaKeysChange.leftovers`.

The database is **closed afterwards**. The caller reopens through its normal
open path (the GUI: `runtime.db.close_db` + `init_db`).

## Reopening with the old keys

`SciDuck` now validates the `_schema` columns on every writable open (it used
to skip an empty `_schema`). A script that still passes the old keys gets
`SchemaKeysMismatchError` (a `ValueError`), whose message shows the keys
passed in beside the database's keys, and the call to use instead.

## The popup

`DuplicateLocationError(variable, collisions, schema_keys, data_columns=,
dataset_empty=)`. Empty: the first step has `action: "change_schema_keys"` and
the remedy carries `data_columns`, `schema_keys`, `dataset_empty`. Not empty:
`action: None` and the text says the dataset already holds data.

The GUI's `SchemaKeysDialog` lists the existing keys (read-only) beside an
editable list seeded from `schema_key_proposal` (add, delete, reorder,
rename). A rename of an existing key is sent as `renames`. Measure columns are
proposed too; the user deletes every column that is not a key. "Change keys
and run again" calls `change_schema_keys`, then fires `RERUN_NODE_EVENT`.

## Diagnostics

INFO `[emptiness] is_empty: ...`; INFO `[schema_keys_change] old -> new`, per
table rows carried / rebuilt with ms; WARN on every refusal; GUI INFO
`[schema_keys] proposal ...` / `change requested ...`; frontend
`console.debug('[schema-keys] ...')`.
