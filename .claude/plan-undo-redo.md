# Plan: undo/redo for the SciStack GUI (DAG canvas + Plot Studio)

*Drafted 2026-10-03. Approved and implemented 2026-10-03 (all 5 stages). Python tests
pass (user, 2026-10-03, incl. `scistack-gui/tests/test_history.py`); frontend `npm test` passes
(538); both bundles rebuilt. Manual checks: `docs/gui-manual-testing-todo.md` §0zzy.*

**Decisions (2026-10-03):** (1) undo of a create may remove the row/file —
exempt from "never delete, mark hidden"; (2) saved plots are NOT undoable in v1;
(3) a conflict refuses and drops the entry, no "undo anyway".

**Deviation from the draft:** Plot Studio's preview mode and figure index are
NOT part of the local history (they are view state, like zoom); only the spec is.
Requests started in one event-loop task share one undo step by default
(`undo.ts taskChange`), so a multi-request gesture such as deleting a node with
its edges is one step without per-call-site grouping.

## Goal

Cmd/Ctrl+Z and Cmd/Ctrl+Shift+Z (and Ctrl+Y) plus ↶/↷ toolbar buttons that
undo and redo every *document edit* on the DAG canvas and in Plot Studio.
Undo and redo must be idempotent on both ends. A future feature should get
undo by setting one flag, with no per-feature inverse code.

## Core decision: record state diffs, not inverse commands

Two common approaches:

1. **Command pattern.** Every operation ships its own inverse. That means
   ~50 hand-written inverses, a new one for every future feature, and each
   one a second owner of what its operation does. Rejected.
2. **State diffs at one choke point (chosen).** Every GUI mutation already
   goes through `api/handlers.py:Handler.invoke` (one table, both
   transports). Wrap that call: read the GUI's document state before and
   after, keep the difference as a **change record**, and undo by writing
   the "before" rows back. New features get undo for free.

No Python library does row-level undo for DuckDB, and DuckDB has no
triggers or changefeed, so the backend part is about 300 lines of our own
code. On the frontend, no library supports a stack that mixes server-side
changes with local Plot Studio edits (`use-undo` and `zundo` only handle
local state). The stack logic is ~80 lines of pure functions, tested with
the existing `node --test` setup. **No new dependencies.**

## What counts as "document" (what gets recorded)

| State | Where | How it is captured |
|---|---|---|
| Pipeline structure, hypotheses, uses, hidden ports, node config, builtin fns, parameter groups, PathInput history/renames | `pipeline_store` tables | table row diff |
| Execution intent (wiring, hides, pins, constants, run options, column selections) | `_intent` | table row diff |
| Node positions | `<db>.layout.json` | file before/after |
| `scistack.toml`, entities TOML, glue sources, target source files (create_parameter / create_variable / path inputs) | files on disk | file before/after |
| Plot Studio panel document (spec + view, the same pair `savedPlots.modifiedKey` already treats as "the panel") | frontend state | local snapshot entry |

**Not undoable (facts, irreversible, or not document):** runs and cancels,
MATLAB dispatch, `_node_wiring` (run facts), `delete_variant` /
`delete_variant_plan` (deleting real data, already behind a confirmation),
exports and reports (`plot_export`, `export_pipeline_code`, `write_report`),
`refresh_*`, `plot_variant_sets_save` (written as a side effect of the spec
effect, so undoing the spec rewrites it; one owner), selection, viewport,
clipboard, and which tab is open.

## Backend: `scistack_gui/history.py` (new, the single owner)

```
ChangeRecord(change_id, label, method, state: "applied"|"undone",
             rows: {table: {pk: (before_row|None, after_row|None)}},
             files: {path: (before_bytes|None, after_bytes|None)},
             notify: {dag_updated: bool, ...}, created_at)
```

- **Tracked tables**: each store module lists its own undoable tables next
  to its `CREATE TABLE` (e.g. `pipeline_store.UNDOABLE_TABLES`). Primary
  keys are read from DuckDB (`duckdb_constraints()`), so keys are never
  declared a second time. A table with no PK (`_pipeline_path_input_renames`)
  is matched on the whole row.
  Guard test: every `CREATE TABLE` in `scistack_gui` must be in an
  `UNDOABLE_TABLES` list or a `NOT_UNDOABLE_TABLES` list with a reason.
- **`recording(change_id, label)`** context manager, held in a contextvar:
  - takes a process-wide `_history_lock`, so two undoable handlers can't
    end up in each other's diffs;
  - if the handler `needs_db`, does `SELECT *` on the tracked tables before
    and after, then diffs by PK (it opens its own `db_connection` for
    `holds_db_lock=False` handlers);
  - files are captured by **`history.write_text(path, text)`**, the only
    allowed way for GUI code to write a project file. It records the
    before-bytes on the first touch and the after-bytes on every touch.
    `layout._atomic_replace` reports through it too. AST guard: no
    `.write_text(` or `os.replace(` in `scistack_gui` outside `history.py`
    and an explicit export allowlist (`code_export_service`,
    `portability_service`, `plot_service` manifest).
  - **The same `change_id` seen again merges**: the first before-state is
    kept and the latest after-state wins. This is how one gesture made of
    several calls becomes one entry (a node drop sends 2 `put_layout`s
    ~1 ms apart), and why a retried request is harmless.
  - An empty diff still creates a record, flagged `empty`.
- **`undo(change_id)` / `redo(change_id)`**, both idempotent:
  - already in the target state: return `{status: "noop"}` and write nothing;
  - unknown id (backend restarted, or the record aged out of the 500-record
    ring buffer): return `{status: "unknown"}`;
  - **precondition check before any write**: every touched row and file
    must still equal the record's after-state (for undo) or before-state
    (for redo). Otherwise return `{status: "conflict", what: [...]}`
    naming each changed row or file (for example, "scistack.toml was
    edited since"). Nothing is written. No force option in v1;
  - apply: all rows in one DuckDB transaction (`BEGIN … COMMIT`), then
    files through atomic replace. If a file write fails, roll the DB back
    to the record's state and log an ERROR;
  - re-emit the notifications the original handler emitted
    (`dag_updated`, …), so every open view refreshes the same way.
- **In memory, bounded (500 records).** History does not survive a backend
  restart, which matches VS Code editors. Nothing is persisted, so there is
  nothing to migrate.
- **Logging (NOTE 2)**: INFO for each record (`history_record id=… method=…
  rows=N files=M snapshot_ms=…`), each undo/redo with its status, and every
  conflict with the diverged keys. `snapshot_ms` is the measurement that
  decides whether whole-table snapshots need optimizing later.

## Wiring into the handler table (`api/handlers.py`)

- New field `undoable: bool | None = None`. A test makes every non-GET
  handler state True or False explicitly, so each future feature has to
  decide.
- `Handler.parse` removes a reserved `_change: {id, label}` key before
  pydantic validation, in one place for both transports.
  `Handler.invoke` wraps the call in `history.recording` when
  `undoable and _change`.
- New handlers `history_undo`, `history_redo` (`{change_id}`), not
  undoable themselves.
- `get_info` returns `undoable_methods` from the Handler table. The
  backend owns that list and the frontend only reads it, so the existing
  route-map test needs no second copy.
- **Undoable**: put/delete_layout, put_node_config, set_note,
  parameter/path-input CRUD + rename + deep copy, put/delete/unhide_edge,
  create_builtin_function, glue create/save/delete, project paths,
  entities-file set/clear, pending constants, hide/unhide combo and
  parameter value, set_parameter_group_checked, pin_variant / release_pin
  / pin_newest_variant, create_variable, pipeline and hypothesis CRUD,
  hide/unhide_port, extract_to_submodule, duplicate_*, paste_nodes,
  import_pipeline, pipeline uses, plot_add_to_pipeline,
  plot_project_alias_set, plot_project_color_set.

## Frontend

- **`frontend/src/history.ts`** holds pure functions, so `node --test` can
  cover it:
  `Stack = {past: Entry[], future: Entry[]}`, with
  `Entry = {kind: 'server', changeId, label} | {kind: 'local', before, after, label}`.
  It provides push (clears `future` and dedupes the same `changeId` at the
  top), undo, redo, and merging local entries that arrive within 500 ms
  (typing in a field becomes one entry).
- **One stack per webview**: the DAG panel has one, and each Plot Studio
  panel has its own, the way each VS Code editor does. The frontend owns
  the order of entries; the backend owns what each server entry changed.
- **`callBackend`** (`api.ts`, already the single choke point): for a
  method in `undoable_methods`, attach `_change: {id, label}`, where `id`
  is the current group's id or a fresh UUID. On success, push it onto the
  stack. `withChange(label, async () => …)` makes several calls one entry
  (for example, a node drop).
- **Undo runner**: one call in flight at a time, and key presses during it
  are ignored. On `noop` or `empty`, pop and continue to the next entry. On
  `unknown` or `conflict`, drop the entry, show a notice saying why, and
  leave the rest of the stack intact. The stack moves only after the
  backend answers.
- **`useUndoShortcuts(stack)`**: Cmd/Ctrl+Z, Cmd/Ctrl+Shift+Z and Ctrl+Y,
  skipped while an INPUT, TEXTAREA, SELECT or contentEditable has focus,
  so native text undo still works there. This uses the same mechanism as
  the existing Cmd+C/V in `PipelineDAG.tsx:384`, which already works in
  the webview. ↶/↷ buttons go in the DAG toolbar and the Plot Studio
  figure toolbar, with the entry label as a tooltip ("Undo: delete edge").
- **Plot Studio**: replace `useState<Spec>` with `useHistoryState`, which
  returns `[spec, setSpec, replaceSpec]`. The ~40 user-edit `setSpec`
  call sites are unchanged. Loads and server normalization (≈ lines
  967/987) use `replaceSpec`, so they are not undo steps. View settings
  (`previewMode`, `figureIndex`) are part of the same snapshot. Alias and
  colour edits are server entries on the same panel stack.

## Stages (each ends with tests passing; you run them)

1. **`history.py` core**: record, diff, undo/redo, conflicts, idempotency,
   merge by id. Tests: create→undo→redo for rows and files; undo twice is
   a noop; conflict when a row or file changed since; merged gesture;
   unknown id.
2. **Handler wiring**: `undoable` flag, `_change` stripping, the two
   history handlers, `get_info.undoable_methods`, `write_text` routing,
   plus both AST/registry guards. Tests: one undo round trip per undoable
   handler family through `Handler.invoke` (parametrized), and the
   explicit-flag test.
3. **Frontend stack + DAG**: `history.ts` + tests, `callBackend`
   integration, shortcuts, toolbar buttons, and grouping for drop and paste.
   Rebuild **both** vite targets.
4. **Plot Studio**: `useHistoryState`, coalescing, alias/colour entries.
5. **Docs**: `docs/claude/undo-redo.md`, a `docs/gui-manual-testing-todo.md`
   section, a memory entry.

## Open questions

1. **Undo and the "never delete, mark hidden" rule.** Undoing "create node"
   removes the row the create inserted, and redo puts back the exact same
   row (same id). My recommendation is that undo is exempt, because it
   restores an exact earlier state rather than performing a user "remove".
2. **Saved plots** (`plot_saved_save`/`rename`/`hide`): leave them out of
   v1? Saving is an explicit act like saving a file, and undoing a save
   would delete a version row. Rename and hide would be easy to include.
3. **Conflict behaviour**: refuse and drop the entry (recommended for v1),
   or offer "undo anyway"?
