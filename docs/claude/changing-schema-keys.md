# Changing the schema keys of an empty dataset

GitHub issue #10; plan `.claude/plan-change-schema-keys.md`; built 2026-11-15.

> **Update 2026-11-15 (branched schema keys).** "Empty" is now the special case
> "every key is unused". A dataset that holds data can still ADD keys
> (`extend_schema_keys`) and change keys no record or run names; used keys stay
> fixed. See `branched-schema-keys.md`. The rebuild below is still what an empty
> dataset takes, and the popup's wording for a non-empty dataset is now "Add a
> schema key".

> **Update 2026-11-17 (entry point; references follow).** The GUI header has
> a **Schema keys…** button that opens the dialog with no failed node (the
> proposal is then the current keys). And a change now renames every
> REFERENCE to a renamed key, not just the key tables: see "References
> outside the key tables" below. Plan `.claude/plan-schema-keys-entry-point.md`.

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

## References outside the key tables

Many places name a key. All of them go through ONE map,
`scidb.project.schema_map.KeyMap`, whose methods each own one SHAPE a key can
appear in. A bundle import into another schema and an in-place change use the
same shapes, so there is no second list of places.

| Place | Shape (`KeyMap.…`) | Renamed by |
|---|---|---|
| scistack.toml `[schema_keys]` (value ORDER per key), `[aliases]`, `[colors]` | `config_tables` (the list is `SCHEMA_KEYED_CONFIG_TABLES`) | `scidb.project.key_rename` |
| entities file `[path_inputs]`: template, `aliases`, `key_regex` | `path_input_arm` | `key_rename` (comments kept, via `bundle._map_path_inputs`) |
| entities file `[library] schema_keys` | `key_list` | `key_rename` |
| node settings (`schema_location`: `schemaLevel`, `schemaSelection`, `whereFilters`) | `level`, `locations`, `where_filters` via `canvas_snapshot.remap_location_value` | GUI `GuiSection.rename_keys` → `canvas_snapshot.rename_schema_keys_in_place` |
| submodule bindings (`key_map` both sides, `iterate`) | `canvas_snapshot.remap_binding` | same |
| saved plots / plot presets / saved stats / stats presets, every version | `exact_strings` | `PlotsSection` / `StatsSection.rename_keys` → `VersionedStore.remap_in_place` |
| PathInputs declared in Python code | — | nobody: code is never edited; the GUI reports them (`registry.code_declared_path_inputs`) |

Rules:

- **A renamed key's references follow it.** `change_schema_keys(db, keys,
  renames=, root=, providers=)` calls `key_rename.rename_key_references` after
  the key tables commit. `root` defaults to `scifor.project.root.project_root()`;
  the GUI passes `resolve_project_root` and `headless.bundle_providers()`.
- **A deleted key is RETIRED, not dropped.** `KeyMap.renaming(renames, retired)`
  keeps every reference to a retired key as written and flags it. The flags
  become `SchemaKeysChange.leftovers` ("Still to check by hand" in the dialog).
  An import (`KeyMap.auto`) still drops unmapped references.
- **Adding keys touches nothing** (an identity map, every file byte for byte).
- **Library pipelines speak their own keys.** Their node statements are
  skipped. A placement's binding renames its project side always and its
  child side only for a non-library child.
- **Saved versions are rewritten in place**, not appended: a rename does not
  change what a version meant, and an old version must still open.
- **A failure renaming references does not undo the key change.** It is
  reported as a leftover.
- Fixed on the way: an import now also renames PathInput `key_regex` and node
  `whereFilters` (both used to be skipped). The config writer now forgets
  `aliases`/`colors` caches too.

The GUI re-reads the entities file (`registry.reload_entities_file`) when its
PathInputs changed, so a run uses the new placeholders without Refresh Code.
The dialog fires `scistack:schema-keys-changed` and the header re-fetches its
`schema: [...]` label.

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
`[schema_keys] proposal ...` / `change requested ... (project root ...)`;
INFO `[schema_map] in-place key map visit->session, trial (retired)`;
`[key_rename] section gui: {...} (N ms)` per section and one summary line;
WARN `[key_rename] <where>: <why>` per flagged reference;
`[canvas_snapshot] renamed keys in place`, `[saved_plot] remap_in_place`;
WARN `[schema_keys] leftover: ...`; frontend `console.debug('[schema-keys] ...')`.

Tests: `scidb/tests/test_key_rename.py` (every shape; both files; through
`change_schema_keys`), `scistack-gui/tests/test_schema_keys_entry_point.py`
(one project naming the key everywhere: rename / delete / add),
`frontend/src/components/schemaKeysResult.test.ts`.
