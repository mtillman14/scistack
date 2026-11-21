# Plan: sidebar right-click Delete, data included (GitHub #26)

Status: all six stages implemented 2026-11-19; all pytest suites + gen_ts_types + tsc pass (user, 2026-11-20); manual GUI check pending. Deviations: trash is Parquet files beside the DB (state in one `_trash` table), not mirror tables; CLI is `scidb restore-deleted` / `scidb purge-trash`; a Submodule Delete HIDES the pipeline (the store's user-facing removal) after removing its placements; the Variants popup delete became undoable too.

## User direction (2026-11-19)

- Delete lives in a **right-click menu on sidebar palette items**, as a red "Delete…" entry.
- Delete does the full job:
  - a dry run;
  - finding the targets;
  - deleting the entity's data **and everything downstream of it**;
  - removing the declaration.
- **It works the same whether or not the entity is placed on any canvas.**
  Placements are reported in the confirmation and never block the delete.
- It must be crystal clear what will be deleted and what can't be undone.

This **reverses** two decisions in `.claude/plan-palette-order-and-delete.md` (2026-11-03):
"refused while placed" and "saved data is kept". Update that file and
`entity_delete_service`'s module docstring when this lands.

## Why (from rev 1 investigation)

- A Parameter value that has run stays listed as a `history` row with no ×
  (`ParameterSettingsPanel.tsx:224-247`). Only source edits exist; nothing deletes data.
- Palette Delete today (`entity_delete_service.delete_entity`) refuses any entity
  on a canvas, **including one that's there only because of run history**.
  So anything that has ever run can effectively never be deleted.

## Design

### One entry point (frontend)
- `EditTab.tsx` already has a Parameter-only context menu (`paramContextMenu`).
  Generalise it into one palette context menu for every deletable kind.
- Keep the existing items (Open source, …) and add a red **Delete…** at the bottom.
- The hover trash button (`PaletteItem.onDelete`, "Delete the declaration (saved
  data is kept)") is **removed**. It would be a second Delete with different
  semantics (NOTE 4).

### One dialog (frontend, `DeleteEntityDialog.tsx` rewritten)
It opens with the backend's dry run and shows four blocks:
1. **Data that will be deleted permanently.** For each variable: the record count
   and the locations it loses. This is the downstream closure, from scidb `delete_plan`.
   If there's no data, it says so.
2. **Declaration.**
   - Entities-file entry: "removed from `src/scistack_entities.toml`".
   - Declared in `.py` / `.m`: "declared in X:line — the declaration can't be
     removed from here; only its data will be deleted", plus Open source.
3. **Canvas placements**, per pipeline, with a link to each, and what will happen
   to those nodes (see Q2). This is information only and never blocks.
4. **Undo:**
   - restores the declaration (and the nodes);
   - does **not** restore data;
   - the deleted values and counts stay on record in `scidb tombstones`.

**Parameter only:** a checkbox per value (declared + history). All ticked means
the whole Parameter; a subset means only those values, and the declaration keeps
the rest. This one control covers the original issue (deleting a single value)
without a second surface.

The Delete button is red and enabled only once the plan has loaded. Execute sends
the plan's fingerprint, and a changed DB is refused.

### Data targets: scidb is the one owner (`store/variant_delete.py`)
| Kind | Target | Exists? |
|---|---|---|
| Parameter value | `{"parameter": N, "value": X}` | yes |
| Whole Parameter | `{"parameter": N}` (every value) | **new**: drop the value filter |
| Variable | `{"variable": V}`, every record of the type, including `exclude_variant`-hidden ones (cards miss those) | **new** |
| PathInput | `{"path_input": N}`, outputs of every invocation that bound it | **new**, mirrors the parameter branch |
| Function | `{"function": F}`, every output of F | **new**: today it needs `param` + `value` |

The downstream closure, atomic transaction, tombstone and pin release are unchanged.

### One GUI service (`entity_delete_service`)
- `delete_entity_plan(kind, name, values=None)` returns:
  - the scidb plan (with its fingerprint);
  - the declaration check (`remove_declaration(preview=True)`, or "declared in X");
  - the placements (`entity_placements`, kept as information only).
- `delete_entity(kind, name, values, fingerprint, reason)` (undoable handler):
  1. scidb `delete_variant(targets, reason, expect_fingerprint)`. Atomic: if it
     fails, nothing has changed.
  2. The source edit: remove the declaration, or for a Parameter subset,
     `update_parameter(remaining)`.
     - If this fails, the data is already gone and the declaration remains.
     - That outcome is reported, not hidden, and is harmless: retrying deletes
       the declaration only.
  3. Canvas nodes, per Q2.
- Drop the stale `hide_constant_value` rows for the deleted values (TBD: verify they linger).
- `variant_cards_service._delete_targets` keeps serving the Variants popup.
  Both routes go through scidb's one `delete_plan`.

### Undo (Ctrl Z): as much as possible (user, 2026-11-19)
The whole Delete is **one** `undoable` handler call, so it's **one** Ctrl Z step.

| Part | Undoable today? | How |
|---|---|---|
| Declaration (entities file, `update_parameter` subset) | yes | `history.note_write` file snapshot |
| Canvas nodes/edges removed, hides, pins, Parameter value groups, `_node_wiring` | yes | generic row diff of `UNDOABLE_TABLES` (no new code) |
| Variant pin released by the delete | no: it's in scidb's `_variant_pin` | covered by option B, below |
| **Data records + downstream** | **no**: "never undoable" (undo-redo.md) | options below |

**Option A: hard delete (today's scidb behaviour).**
- Ctrl Z restores everything except data. The nodes come back and show needs-run.
- The dialog says so in red. There's no new concept.

**Option B: trash, so Ctrl Z restores the data too.** This option is recommended,
and it amends "data never undoable".
- scidb `delete_variant` **moves** the rows instead of dropping them:
  - into `_trash_<table>` mirrors, keyed by `delete_id`, in the same transaction;
  - covering `<Type>_data`, `_record`, `_record_save`, `_invocation*`, `_run*`
    and the released `_variant_pin` rows.
  - Live readers don't change: the rows are gone from the live tables, so there's
    no `deleted` flag for every query to remember (NOTE 4; and `_record.excluded`
    already means "hidden" and must not be reused).
- New `scidb.restore_deleted(delete_id)` moves them back. It refuses with a
  conflict if any restored record id or location now exists live (for example
  after a re-run), and nothing is written.
- History gains **one** generic hook: a change record can carry
  `external = [{"owner": "scidb.trash", "id": delete_id}]`.
  - Undo calls `restore_deleted`; redo calls `delete_again(delete_id)`.
  - The precondition / conflict rules stay the same as for rows.
  - This is the only inverse-style code, and scidb owns it.
- Purge:
  - when the change record ages out of history's 500-entry window, or via
    `scidb trash purge`;
  - until then the DB file keeps the bytes, and the dialog shows that size.
- The tombstone gets `delete_id` and `state` (`trashed` | `restored` | `purged`).
- Cost: a new scidb concept (the trash) and the history hook; it's the bigger piece.

### Logging / diagnostics (NOTE 2)
- INFO:
  - the plan summary (kind, name, values, total records, variables touched, placements, ms);
  - the outcome of each execute step (records deleted, declaration edit, nodes removed);
  - the removed declaration text, which is already logged.
- WARN: data deleted but the source edit failed.

### Tests
- **scidb:** each new target kind deletes exactly its closure. A whole Parameter
  equals the union of its values. A Variable target includes excluded records.
  Surviving shared constants are untouched.
- **GUI integration:**
  - run → right-click delete of a placed Parameter value → the plan lists the
    downstream variables;
  - execute → the history row is gone, a tombstone exists, Undo restores the
    declaration and not the records;
  - the same for a Variable and a PathInput;
  - a `.py`-declared entity deletes data only;
  - a stale fingerprint is refused.
- **Frontend:** a unit test of the dialog's text model (data / declaration /
  placements / undo blocks).

## Decisions (user, 2026-11-19)
- **Undo: Option B (trash).** Ctrl Z restores the data too. This amends
  undo-redo.md's "data never undoable"; update that table row when it lands.
- **Every sidebar kind gets Delete:** Submodules, Functions, Variables, Parameters, PathInputs.
- **Placed nodes are removed** from every canvas, and the confirmation lists them.
  Ctrl Z restores them (generic row diff).
- **One behaviour:** there's no "keep data" choice.
- **Function:**
  - deletes every output plus everything downstream, and removes its nodes;
  - source code: a **GUI-owned file** (functions_dir via New function, or a glue
    file) is deleted, with a `note_write` snapshot so Ctrl Z restores it;
  - a function in the user's own `.py`/`.m` or a library keeps its code and stays
    in the palette, and the dialog says so.
- **Submodule:**
  - removes the pipeline and its placements on parent canvases;
  - replaces today's hide-only ✕ (`delete_pipeline`), and is undoable;
  - **no data is deleted**, because records belong to functions, not pipelines;
  - the dialog says this and points at the steps' Function / Variable Delete.
- Still open, low stakes: the reason text. Default to `"deleted from sidebar: <kind> <name>[=values]"`, editable in the dialog.

## Stages (each lands with its tests + logging)
1. **scidb trash** (`variant_delete.py`): move rows into `_trash_*`; `restore_deleted`
   / `delete_again` with a conflict check; `scidb trash list|purge`. Tombstone gets
   `delete_id` + `state`.
   Tests: delete → restore round-trips byte-equal rows; a re-run conflict refuses restore.
2. **scidb whole-entity targets:** `variable`, `parameter` (no value), `path_input`, `function`.
3. **History external hook:** a change record carries scidb trash ids, and
   undo/redo call restore/delete-again under the same precondition rules. Purge on
   age-out happens here.
4. **GUI service:** `delete_entity_plan` / `delete_entity` for all five kinds (one
   service, one undoable handler); node removal across canvases; GUI-owned
   function-file removal; Submodule = pipeline + placements.
5. **Frontend:** one palette context menu (red Delete…), one dialog (data /
   declaration-or-code / placements / undo blocks; Parameter value checkboxes);
   remove the hover trash and the Submodule ✕.
6. **Docs:** update `variant-pins-and-deletion.md` §3, `undo-redo.md`,
   `plan-palette-order-and-delete.md`, and the `entity_delete_service` docstring.
