# Plan: Clear all data, keep the pipeline (CLI + GUI)

*Drafted 2026-10-21. Status: ON HOLD. The user asked whether the difficulty points at an architectural flaw; see `.claude/plan-canvas-as-document.md`. Its Stage 2 (adoption) would replace this plan's GUI half; the scidb half stands either way.*

## User decisions (2026-10-21)
- Backup: opt-in `--backup` flag / GUI checkbox (copies the .duckdb to
  `<name>.duckdb.bak-<timestamp>` after a CHECKPOINT, before the wipe).
- Pins: released (rows kept). Exclusions, saved plots/presets/stats: KEPT.
- Column sets must be free after a clear. No DROP needed: the existing owner
  `DatabaseManager._check_column_set` already rebuilds an EMPTY `<Type>_data`
  table (and its view) with the new save's columns. Emptying the rows is enough;
  a test pins "clear, then re-save with different columns succeeds".

## The ask

While debugging (GUI or code) the user needs to wipe every record from a
project database **without losing the pipeline**, then re-run from a clean
slate.

This is a real delete. It is the SECOND exception to "never delete"
(`feedback_never_delete_mark_hidden`; `inspect/mutate.py` docstring says "do
not add a second one without asking"). The user has now asked explicitly.

## The trap that shapes the design

On a project that has run, most of the canvas is **not stored**. Function
nodes, variable nodes, Parameter values and every edge are rebuilt from
provenance by `graph_builder.build_edges` / `aggregate_from_scidb` on each
graph build (`project_edge_based_inputs`: "an already-run pipeline has NO
manual edge rows"). Deleting `_record`/`_invocation*` naively would erase the
pipeline along with the data.

So the GUI side must **freeze the canvas before the wipe and write it back
after**. The GUI already has one owner for exactly that:
`services/canvas_snapshot` (`capture` reads the RESOLVED graph, history-derived
nodes included; `apply` writes manual nodes that "never depend on history the
target may not have"). Export-without-history + import into a fresh project
uses the same pair, so clearing in place equals "export the pipeline, import
it into an empty copy of this database", using the code path that export and
import already test.

## One owner per concept

| concept | owner (existing or new) |
|---|---|
| which scidb tables are history / data / intent | `scidb/bundle.py` `HISTORY_TABLES`, `INTENT_TABLES`, `_data_tables` (existing). Add `AUDIT_TABLES = ("_variant_pin", "_variant_tombstone")`: kept by a clear |
| which GUI tables are history / canvas / global | each GUI module's `PORTABILITY` via `canvas_snapshot.table_portability()` (existing) |
| deletion audit trail | `variant_delete` tombstone table (existing): one tombstone row per clear |
| "pin left pointing at nothing is released" | `variant_pins.release_pin` (existing rule from variant delete) |
| the scidb wipe | NEW `scidb/clear_data.py` |
| freezing and restoring the canvas | NEW `scistack_gui/services/clear_service.py`, a thin wrapper over `canvas_snapshot` |

## What a clear does

**scidb (`scidb/clear_data.py`)**
- `clear_plan(db) -> ClearPlan`: a dry run that writes nothing. It returns record
  counts per variable, row counts per table, run count and a `fingerprint`
  (same pattern as `delete_plan`).
- `clear_data(db, reason, *, expect_fingerprint, keepers=())`: a single
  transaction:
  1. re-plan, refuse on fingerprint mismatch, require reason;
  2. `keeper.capture(db)` for each keeper (the GUI canvas freeze), before anything is deleted;
  3. `DELETE` all rows of every `<Type>_data` table (tables, views and
     column sets are kept) and of `HISTORY_TABLES − INTENT_TABLES −
     AUDIT_TABLES` (`_schema`, `_record`, `_constant`, `_invocation`,
     `_function_source`, `_invocation_input/_output`, `_run`,
     `_run_invocation`) and `_record_save`;
  4. release every active pin (rows kept, `released_at` set);
  5. `keeper.restore(db, state)` (the GUI rewrites the canvas);
  6. tombstone `{targets: [{"all": true}], counts, record_ids: []}`;
  7. commit; any failure rolls everything back.
- **Kept:** variable registrations (`_variables`, `_registered_types`,
  `_variable_groups`), schema exclusions, schema types, pins (released),
  tombstones, saved plots / presets / stats (scistackplotdb, scistackstatsdb),
  entities TOML, all code, files on disk.
- INFO logging per table with timing (NOTE 2).
- `Mutator.clear_data` (`@_mutation`), and the `mutate.py` docstring updated to
  name the second exception.

**GUI (`clear_service.py`), the `CanvasKeeper` keeper**
- `capture`: `canvas_snapshot.capture(db, every non-library pipeline)` plus
  `portability_service.export_globals`. Then **Parameter values that exist only
  in history are materialised as pending constants**. Today `export_globals`
  bundles only `get_pending_constants`, so an export without history also
  loses them. The fix goes in `export_globals` (one owner), which also fixes
  that export gap.
- `restore`: empty the `canvas` + `history` class GUI tables of those
  pipelines (pipeline rows themselves kept, ids unchanged), then
  `canvas_snapshot.apply(db, snap, identity_map, include_hides=False)` +
  `apply_globals`. Layout positions are rewritten by `apply`. The `.layout.json`
  sidecar is copied first and restored if the transaction rolls back.
- Afterwards: reset undo/redo stacks (the old diffs reference vanished
  ids), broadcast graph refresh to every panel incl. PlotPanel
  (`project_plot_panel_notification_routing`).

## Entry points

**CLI**
- `scistack clear-data [--yes] [--reason R] [--json]` (owned by
  `scistack_gui/clear_cli.py`, mounted by `scistack` like `bundle_cli`): opens
  WITH discovery (capture needs the registry), prints the plan, then asks you to type
  the database name to confirm (`--yes` skips the prompt for scripts).
- `scidb clear-data` (scidb CLI, `_write_handler`): same flags, for
  script-only projects. **Refuses** when GUI canvas tables exist (it cannot
  freeze the canvas) and points at `scistack clear-data`.
- Lock contention (open GUI/MATLAB session) → `DatabaseLockedError`, one line.

**GUI**
- VS Code command **"SciStack: Clear All Data (keep pipeline)…"**. It also appears as
  an item in the canvas toolbar's overflow/project menu.
- Flow: RPC `clear_data_plan` → modal showing counts per variable +
  "pipeline is kept" → user types the database name → RPC `clear_data`
  (with fingerprint) → canvas refresh. The running session holds the
  DuckDB, so the GUI does this in-process (no lock issue).
- Refused while a run is in progress.
- Add §entry to `docs/gui-manual-testing-todo.md`.

## Tests (NOTE 2)

scidb (`scidb/tests/test_clear_data.py`):
- after clear: 0 records, 0 data rows, views still queryable, column set
  intact (re-save same columns works), variables still registered;
- exclusions + tombstones survive; pins released; one new tombstone;
- re-running the same `for_each` after clear reproduces the same record ids;
- fingerprint mismatch refuses, missing reason refuses, failure mid-way
  rolls back (injected keeper that raises in `restore` → nothing deleted);
- guard: every table in a fresh scidb DB is classified by `bundle.py`
  (history / intent / audit / data / registration), so a new history table
  cannot survive a clear unnoticed;
- CLI: refuses with GUI tables present; `--json` shape.

GUI (`scistack-gui/tests/test_clear_data.py`):
- a run project (history-only nodes/edges, history-only Parameter values):
  after clear the canvas graph has the same node labels/types, the same
  connections (by `connection_id`), the same Parameter values and the same
  positions; zero records;
- a run after clear executes the same wiring (edge-parity style check);
- hypothesis tab + placed submodule survive;
- undo stack reset.

## Out of scope / follow-ups
- MATLAB entry point (`scidb.clear_data` via bridge): not now.

## Status 2026-10-21 (branch `canvas-as-document`)

scidb half BUILT: `scidb/clear_data.py` (`clear_plan`, `clear_data`, `ClearRefused`),
`Mutator.clear_data`, `scidb clear-data [--yes] [--fingerprint] [--backup]`,
`variant_delete.write_tombstone` (shared), `variant_pins.forget_type_graph`.
Tests `scidb/tests/test_clear_data.py`, unrun. Changes from the plan above:
`_run` KEPT (user Q4), `_schema` KEPT (location interning; avoids re-using
schema ids under anything that cached one), no keeper hooks (the GUI clear
will be "adopt everything, then clear_data(allow_canvas=True)" at
plan-canvas-as-document Stage 2). A database with GUI tables is refused.

## Status 2026-10-22: GUI half BUILT (canvas-as-document Stage 2)

`scistack_gui/services/clear_service.py`: `clear_data_plan` builds every
canvas first (adoption stores what history would draw), then `clear_plan`;
`clear_data` adopts again, then `scidb.clear_data.clear_data(...,
allow_canvas=True)`. RPCs `clear_data_plan` / `clear_data` (api/project.py),
dialog `ClearDataDialog.tsx` ("Clear data…" in the canvas toolbar; type
`clear`; backup checkbox, default on). No snapshot/restore and no keeper hooks:
the canvas is stored, so a clear cannot touch it. Undo history is kept (a clear
touches no tracked table). The VS Code command palette entry was not added
(the toolbar button covers it). Tests: `tests/test_canvas_document.py::
TestClearDataKeepsTheCanvas`, pytest unrun.
