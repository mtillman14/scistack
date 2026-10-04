# Undo / redo in the GUI

*Written 2026-10-03. Plan: `.claude/plan-undo-redo.md`. Manual checks:
`docs/gui-manual-testing-todo.md` §0zzy.*

## The one idea

Undo records **what state changed**, not **how to reverse an action**.
There is no per-feature inverse code anywhere. Every GUI mutation already
passes through one choke point, `Handler.invoke` (`scistack_gui/api/handlers.py`).
For a handler declared `undoable=True`, that call is wrapped in
`history.recording(...)`, which:

1. snapshots every tracked DuckDB table before and after the call and
   keeps the per-row difference (keyed by the table's primary key, read
   from DuckDB itself); and
2. keeps the before/after bytes of every project file the call wrote
   (each writer calls `history.note_write(path)` before writing).

The result is a **change record**. Undo writes the record's *before* side
back; redo writes its *after* side.

## Where the GUI's editable state lives

| State | Storage | Owner module | Tracked as |
|---|---|---|---|
| Manual nodes, pipelines (tabs), uses, hypotheses, hidden ports, node config, builtin fns, parameter value groups, PathInput history/renames | DuckDB `_pipeline*`, `_node_config`, `_hypotheses` | `pipeline_store` | rows (`pipeline_store.UNDOABLE_TABLES`) |
| Execution intent: wiring/edges, hides, pins, pending constants, run options, column selections, schema location, variant selection | DuckDB `_intent` | `intent_store` | rows (`intent_store.UNDOABLE_TABLES`) |
| Canvas node ↔ wiring association (allocated function node ids) | DuckDB `_node_wiring` | `node_wiring` | rows (`node_wiring.UNDOABLE_TABLES`). Runs write it too: they are not undoable, and the conflict check below protects their writes |
| Node positions | `<db>.layout.json` | `layout` | file (`note_write(..., reload=False)`) |
| `scistack.toml` (paths, entities file, glue dir, aliases, colours) | file | `config` | file |
| Entities file (Parameters, PathInputs) | TOML / `.py` / `.m` | `services.target_file_service` | file |
| Glue sources | files in glue dir | `services.glue_service` | file |
| New variable classes | `.py` / `.m` | `services.variable_service` | file |
| Saved plots | DuckDB `_saved_plot` | `scistackplotdb.saved` | **not undoable** (decision 2026-10-03: saving is an explicit act, like saving a file) |
| Plot Studio panel spec | React state in `PlotStudio.tsx` | the panel | local history entries (frontend only) |
| Selection, viewport, open tab, clipboard, preview mode, figure index | frontend | — | not undoable (view state, not document) |
| Data, runs, variant deletion | DuckDB data tables | scidb | **never undoable** (facts; real Delete is behind its own confirmation) |

Two guard tests keep this table honest:
`test_every_gui_table_is_tracked` (any `CREATE TABLE` in `scistack_gui`
must appear in an `UNDOABLE_TABLES` list) and
`test_every_file_write_notes_history` (any function in `scistack_gui` that
writes a file must call `history.note_write`, or appear in the export
allowlist with a reason).

## Idempotency and safety

- A record is either `applied` or `undone`. Undoing an `undone` record
  returns `noop` and writes nothing. Redo works the same way.
- **The precondition comes before any write.** Every touched row and file
  must still equal the side being replaced: the after side for undo, the
  before side for redo. If anything differs, nothing is written and
  `{status: "conflict", conflicts: [...]}` names what changed. The
  frontend drops that entry and shows the reason (decision 2026-10-03: no
  "undo anyway").
- **The same `change_id` merges.** The earliest before state is kept and
  the latest after state wins. A gesture made of several requests (node
  drop = create + recentre) is one undo step, and a retried request is
  harmless.
- Undoable handlers run under one process-wide `RLock`, so two of them
  never see each other's writes in their diffs.
- Rows are written in one DuckDB transaction, then files are written via
  atomic replace. If a file write fails, the rows are put back.
- Undo of a "create" removes the created row or file. That is **exempt
  from the "never delete, mark hidden" rule** (decision 2026-10-03): undo
  restores an exact earlier state, and redo brings back the identical
  row (same id).
- Records live in memory, at most 500. A backend restart forgets them, and
  the frontend gets `unknown` and drops the entry.

## The frontend half

- `frontend/src/history.ts` holds pure stack logic (`node --test`):
  `past`/`future`, server entries (`changeId`) and local entries
  (`before`/`after` + `apply`). Local entries merge when they share a
  merge key within 600 ms.
- `frontend/src/undo.ts` holds runtime stacks. `createUndoStack()` gives
  one per surface: the DAG page has one, and each Plot Studio instance has
  its own. `useUndoShortcuts(stack)` binds Cmd/Ctrl+Z,
  Cmd/Ctrl+Shift+Z and Ctrl+Y. Only the most recently mounted surface
  answers, so a Plot Studio modal shadows the canvas underneath it. Keys
  are ignored inside text fields, which keep native text undo.
- `callBackend(method, params, { stack, change })` attaches
  `_change: {id, label}` for methods the backend lists as undoable
  (`history_status`), and pushes the entry when the call succeeds. Over
  HTTP the same object travels in the `X-SciStack-Change` header.
- **Grouping.** Every undoable request started in the same event-loop
  task shares one change id (`taskChange`), so a gesture that sends
  several requests at once is one step. Deleting a node with its edges is
  an example. A gesture that spans tasks passes
  `{ change: newChange(label) }` to each call: the canvas drop does this
  for the create and the re-centre sent once the node has been measured.
  A request that should not be a step at all (a view preference stored as
  a note) passes `{ stack: null }`.
- An undo or redo of a server entry calls `history_undo` /
  `history_redo`. The backend re-broadcasts `dag_updated` (and
  `history_applied`), so every view refetches exactly as it does after the
  original edit. If files changed, the registries are reloaded from disk
  first.

## Adding undo to a new feature

1. Declare the handler `undoable=True` (optionally with `undo_label="…"`).
2. If it writes a new GUI table, add the table to its module's
   `UNDOABLE_TABLES`. If it writes a project file, call
   `history.note_write(path)` before writing.
3. On the frontend, call it through `callBackend` with no extra
   arguments: it already lands on the page's stack. Inside Plot Studio,
   pass `{ stack }`.

Nothing else is needed. A local-only edit (React state) uses
`useHistoryState(stack, initial, label)` instead of `useState`.
