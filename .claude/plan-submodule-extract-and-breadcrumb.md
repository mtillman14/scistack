# Submodule extract and breadcrumb fixes

Revised after an adversarial review of the first proposal. Evidence:
`scistack-debug.log`, 2026-11-04 09:19-09:20. Built 2026-11-04.

## Issue 1: extract leaves nodes behind and strands their settings

**Causes**

- `extract_to_submodule` passes the selected id (`var__X::main`) to
  `move_node_scope`, but the row is stored under the bare id (`var__X`). The
  update matches nothing, so the node resolves on both canvases.
- Statements in `_intent` carry a scope column, and extract never moves them.
  `read_csv` had `run_options` and `schema_location` stated on `main`. After the
  move, `scope_of_node` says the submodule, whose overlay doesn't include them.
  Log: orphan WARN 0 times before the extract, 8 times after; config for 0 nodes
  on the submodule's scope.

**Fix**

Add one owner for "this node now lives on that canvas", in `stores/`, and have
`extract_to_submodule` call it for each selected node:

- Row: move the row under the bare id, only if its `pipeline_id` is the source
  canvas. A duplicated hypothesis can share the row.
- Position: keep the existing re-key to `{bare}::{new_scope}`.
- Edges: keep the existing endpoint rename.
- Statements: move rows for the bare subject from the source scope to the new
  scope. Leave other scopes' rows and `global` alone.

**Logging**

Log rows moved and statements moved per node. (A planned WARN for "stated settings
but none moved" was dropped: statements now move between the two scopes readers
resolve, so it could not fire.)

**Tests** (`tests/test_submodule_ports.py`)

Use the folded state: bare row, position under `::main`, after a real refresh.

- A folded variable and PathInput each appear on exactly one canvas.
- A function's `run_options` and `schema_location` read back the same inside the
  submodule, through both `get_node_config` and the canvas build.
- `TestNothingReruns` with stated settings: targets unchanged, nothing re-runs.
- A variable feeding an already-run function outside the selection: the recorded
  edge is drawn on neither canvas and isn't adopted back.
- Extract from a duplicated hypothesis leaves the original's node and settings alone.
- Undo restores rows, positions, edges and statements.

## Issue 2: wrong canvas under the breadcrumb, and repeated crumbs

**Causes**

- `fetchPipeline` applies a response even when the open scope has changed
  (refresh #13 drew main over the submodule at 09:19:45.680).
- After a scope switch the old canvas stays interactive until the new one loads
  (956-1539 ms in the log), while writes already use the new `currentScope`.
- `descend` appends without checking the path.

**Fix**

- **Refresh rule, in `CanvasRefreshLog` so it's unit-testable:** apply a response
  only if its scope is still open and no newer refresh has already been applied.
  Don't drop every superseded refresh: steady `db_watcher` traffic could starve
  the canvas, and `fetchPipeline` has no error path if the newest one fails.
- **Check before any effect:** after each await, before `setGraphWarnings`,
  `apply()` settling pending items, and the `loadedScope` write. Read the open
  scope from a ref, because the closure holds the old one.
- **Old canvas:** clear nodes and edges on a scope switch, so nothing stale can be
  dragged, deleted, double-clicked or opened from the sidebar. Check this against
  `docs/claude` notes on canvas flicker; it only applies to scope switches.
- **Breadcrumb, as a pure function beside `survivingBreadcrumb`:** a pipeline
  already on the path truncates back to it instead of adding a crumb. Truncating
  can't strand anyone. Refusing could, because `jumpTo` builds `[main, X]` even
  when X isn't under main.

**Logging**

- Report dropped refreshes to the backend with scope and reason, without settling
  pending items.
- Log each `descend` with its path, outside the state updater (`StrictMode`
  double-invokes updaters in dev).
- `superseded_by` only means a newer refresh started. A response older than what's
  on screen is now discarded, so the line says that instead of "an older picture
  applied".

**Tests** (`frontend`, `npm test`; pure functions only, there is no component harness)

- Refresh rule: other scope dropped; older than applied dropped; superseded but
  nothing newer applied still applied; A -> B -> A.
- A dropped refresh leaves pending items pending.
- Breadcrumb: same as last is unchanged; earlier on path truncates; a new pipeline
  appends; a repeated pipeline after `jumpTo`.

## Finishing

- Rebuild both bundles: `npm run build` and
  `VITE_BUILD_TARGET=webview npm run build`.
- The user runs pytest and checks the GUI.
- Update `docs/claude/submodule-ports.md`: row id vs placement id, and which
  statements move on extract.
- Update the GUI manual testing doc.
