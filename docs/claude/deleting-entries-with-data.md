# Deleting entries that have data behind them

*Written 2026-11-19 for GitHub #26. Plan: `.claude/plan-delete-entries-with-data.md`.
Built 2026-11-19 (all six stages); pytest pending.*

## Why "delete" used to be impossible for anything that had run

The GUI shows an entry from **two sources**: what source declares now, and what
DB history says has run. Removing only the declaration leaves the history half
standing, so the entry stays on screen.

| Entry | Declared side | History side | What a source-only delete left behind |
|---|---|---|---|
| Parameter value | `scidb.Parameter(...)` args (`update_parameter`) | `_constant` records on `_invocation_input` edges, keyed by `parameter.parameter_node_name` | a grey `history` row with no × (`ParameterSettingsPanel.tsx`) |
| Variable / Parameter / PathInput entity | entities file (`remove_declaration`) | records of the type / bound inputs | palette Delete refused while on any canvas, and a node drawn only from history counts as placed (`entity_delete_service`) |
| Function | `.py` / `.m` / GUI-owned file | `_invocation` rows with `function_name` | a history node on the canvas; removing it from the canvas only hides it |
| Submodule | `_pipeline*` rows | none (records belong to functions) | the ✕ hid the pipeline (`delete_pipeline`) |
| Old code version / run-option set | — | variant cards | already deletable per card in the Variants popup |

Unchecking a Parameter value on the canvas is hiding, not deleting. Don't
confuse the two.

## The rule (user, 2026-11-19)

- **Where:** right-click a sidebar palette item, then pick the red **Delete…**.
  This is the only Delete for entities; the hover 🗑 and the Submodule ✕ are gone
  (`EditTab.tsx` `PaletteMenu`, `DeleteEntityDialog.tsx`, pure text in `deletePlan.ts`).
- **What:** a dry run, then a confirmation, then the deletion.
  - It deletes the entity's data **and everything downstream of it**, plus its
    declaration and its canvas nodes.
  - Being placed on a canvas never blocks it. Placements are listed, and their
    nodes are removed.
- **There's one behaviour:** no "keep the data" variant.
- **Parameter:** per-value checkboxes. All ticked deletes the whole Parameter;
  a subset deletes only those values.
- **Function:**
  - deletes its outputs plus everything downstream;
  - deletes its file only when the GUI created it (functions_dir, glue);
  - user `.py` / `.m` / library code is never touched, so the function stays in the palette.
- **Submodule:** removes its placements (`scope_service.remove_pipeline_use`)
  and hides the pipeline (`hide_pipeline`, the store's user-facing removal; it
  refuses the last visible pipeline, so the plan blocks that). **No data**,
  because records belong to functions.
- **Refusals come first.** A recording keeps nothing when its handler raises,
  and rows written before the raise stay. So the plan finds every refusal
  (stale entities file, library-locked canvas, last pipeline) and the execute
  re-plans and checks the fingerprint. After the data delete, a failing step
  goes into `warnings` and the call still returns, so Ctrl Z can undo it all.
- **Reason** defaults to `deleted from sidebar: <kind> <name>[=values]`, and
  can be edited in the dialog. It goes on the tombstone.

## Ownership (NOTE 4)

| Concept | Owner |
|---|---|
| Which records a target means, and the downstream closure | scidb `store/variant_delete._seed_records` + `_downstream` (`delete_plan`) |
| Executing the delete atomically, the fingerprint check, the tombstone | scidb `store/variant_delete.delete_variant` |
| Trash files, state, purge | scidb `store/trash` |
| Restore / redo and their preconditions | scidb `store/variant_delete` (`restore_deleted`, `delete_again`, `restore_conflicts`, `redo_conflicts`) |
| Which values a Parameter ran with | scidb `variant_delete.parameter_values` (the same edge rule as the target) |
| The sidebar Delete (plan, execute, order) | `scistack_gui/services/entity_delete_service` (RPCs `delete_entity_plan`, `delete_entity`) |
| Declaration edits | `target_file_service` (`remove_declaration`, `update_parameter`) |
| "Where is it placed" | `entity_delete_service.entity_placements` (the resolved graph) |
| Undo of GUI state and files | `scistack_gui/history` (row diff of `UNDOABLE_TABLES`, `note_write`) |
| Undo of data | `stores/history` `note_trash` + `ChangeRecord.trash`, which call scidb restore / redo |

The Variants popup (`variant_cards_service._delete_targets`) and the sidebar
Delete both go through the one scidb `delete_plan`.

## Undo: a trash, not a hard delete (user 2026-11-19)

This amends `undo-redo.md`'s "data is never undoable".

- **Files, not mirror tables.**
  - `delete_variant` saves each row it's about to delete as Parquet in
    `<db>.trash/<tombstone_id>/NN_CCC__<table>.parquet`.
  - This happens inside the delete's transaction, right before each
    `_delete_in`. Each step saves after the earlier steps have deleted, so
    every row lands in exactly one file.
  - Why files:
    - The table registry forbids undeclared tables.
    - A mirror per `<Type>_data` would be a second owner of each column list.
    - Parquet keeps the column types.
    - Purging frees disk space.
- **State:** one `_trash` row per tombstone (`trashed` / `restored` / `purged`).
  - It also holds the record / invocation / run ids, the released pins and the
    plan fingerprint.
  - `Tombstone.trash_state` reads it. `None` means a hard delete: a clear, a
    `:memory:` database, or a delete made before this change.
- **Restore** (`variant_delete.restore_deleted`) runs `INSERT ... BY NAME` from
  the files in one transaction, then re-pins the released pins.
  - It is refused (`trash.TrashConflict`, nothing written) when:
    - a file is missing or a table is gone;
    - a record, invocation or run id is live again (a re-run);
    - an edge would point at something deleted since.
- **Redo** (`delete_again`) re-plans with a `record_ids` target and requires the
  original fingerprint. A record computed from the restored data since then
  makes the redo refuse rather than delete something unplanned. It reuses the
  tombstone.
- **Purge:** `trash.purge_trash` (`scidb purge-trash --yes`).
  `trash.sweep_orphans` removes folders with no `trashed` row: left by a crash
  mid-delete, or after `clear_data` (which empties `_trash` and calls the sweep).
- **Failure:** a delete that rolls back removes its folder (`TrashWriter.discard`).
- **Ctrl Z.** One Delete is one undoable handler call, so one Ctrl Z step.
  - The service calls `history.note_trash(tombstone_id)` after the scidb delete;
    the change record carries the ids (persisted in `_canvas_change`).
  - Undo checks row, file AND `restore_conflicts` first (nothing written on any
    conflict), writes rows and files, then restores the data last; if that
    fails the rows and files are put back.
  - Redo is the mirror, with `redo_conflicts` / `delete_again`.
  - A record that ages out of the 500-entry window purges its trash
    (`history._purge_aged_out`).
  - The Variants popup's delete is undoable the same way (2026-11-19).

## Known gaps

- A Parameter target matches plain constant edges only. A value that reaches a
  function through constant-fed glue binds a `__constant_glue__` record and is
  not found.
- Trash files stay on this machine; a bundle does not carry them.
- `clear_data` is still a hard delete (its own `--backup` is the undo).
