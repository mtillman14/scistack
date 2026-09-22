# Multi-session GUI: one tab per database, many plot tabs

**Goal.** Open several databases at once in one VS Code window, each with its
own pipeline canvas, working independently. Open as many Plot Studio tabs as
you like, within one database and across databases.

**Status:** ALL STAGES IMPLEMENTED 2026-09-22 (uncommitted). Doc:
`docs/claude/gui-multi-session.md`. Manual-testing items 0j/0k/0l in
`docs/gui-manual-testing-todo.md`.

| Stage | State |
|---|---|
| 0 Diagnostics | done — prefixed Output Channel, session lifecycle logging, `SciStack: Show Open Sessions` |
| 1 Session core | done — `sessionCore.ts` + `session.ts`; new `node --test` cases |
| 2 N pipeline tabs | done — one tab per database, reveal-if-open, per-session watcher / project root / debug port, status bar rewritten |
| 3 Frontend awareness | done — injected `__SCISTACK_SESSION__`, header seeded from it + `db_changed` refetch, Plot Studio badges its database |
| 4 Plot tabs always new | done — `PlotPanel.current`, `retarget` and `open_plot_studio` deleted |
| 5 Shared-resource conflicts | done — per-session MATLAB script file, cross-session engine gate, debugpy ports, channel prefixes. One inherent limit, see below |
| 6 Database-less CSV session | done — `--plot-only`, `Handler.db_optional`, `tests/test_server_plot_only.py` |

**Correction to the plan's own premise (found 2026-09-22 after review):** the
plan said "one MATLAB engine can only be `configure_database`'d to one
database at a time" as if MATLAB were one process per window. It is not.
`matlab_sidecar._sidecar` is a PROCESS singleton, so every session already
has its own sidecar MATLAB — two databases running through sidecars are as
independent as two Python runs. Only the MathWorks terminal is shared (one
MATLAB per VS Code window, its design).

The first implementation of the gate read `MatlabRunTracker.isActive` and so
had it backwards: it blocked sidecar runs, which hold that mark for their
whole duration, while barely covering terminal runs, which clear it
milliseconds after dispatch. It now reads `sharedEngineActive`, set only by
the terminal and clipboard tiers.

**Still not closed, and cannot be here:** the shared-terminal gate covers the
dispatch window only, because nothing reports when a MATLAB terminal run
ends. Back-to-back dispatches are refused; a Run in database B during a long
terminal run in A is not. Real fix = Stage 2 of
`.claude/plan-matlab-terminal-run-tracking.md` (run markers written by
MATLAB). The temp-script race IS fixed unconditionally, and parallel MATLAB
work across databases already works on the sidecar path.

**Departures from the plan as written:**
* `api.ts` does NOT stamp a `sessionId` on every RPC (plan Stage 3). It
  would be dead weight: the host's message handler is the panel's own
  closure, so it already knows the session. The id is injected for DISPLAY
  and for the one place that crosses panels (`open_plot_panel`).
* `PanelRegistry.rebind` was deleted rather than scoped. Panels read
  `session.python` at call time, so there is no stored handle to refresh.

---|---|
| 0 Diagnostics | **done** — prefixed Output Channel, session lifecycle logging, `SciStack: Show Open Sessions` |
| 1 Session core | **done** — `sessionCore.ts` + `session.ts`; 13 new `node --test` cases |
| 2 N pipeline tabs | **done** — one tab per database, reveal-if-open, per-session watcher/project root/debug port, status bar rewritten |
| 3 Frontend awareness | **partial** — the canvas header is fixed (injected session + `db_changed`); the Plot Studio does not yet show its database |
| 4 Plot tabs always new | **not done** — reuse slot is now per session (`Session.currentPlot`) instead of global; deleting it is the remaining step |
| 5 Shared-resource conflicts | **partial** — debugpy ports and the Output Channel done; the MATLAB temp script and single-engine gate are NOT |
| 6 Database-less CSV session | **done** — `--plot-only`, `Handler.db_optional`, `tests/test_server_plot_only.py` |

---

## 1. Why this is a GUI-layer change and nothing else

The Python backend is pervasively **process-global**: `db._db` / `_db_path` /
`_db_refcount`, `registry._functions` / `_parameters` / `_path_inputs` /
`_config`, `config._project_root_hint`, plus everything the scistack layers
own below it (`scifor.set_schema`, `scidb.configure_database`, the
`scistacklog` sinks). Threading a session object through ~35k lines to make
two databases coexist *inside* one interpreter would be a rewrite with no
scientific payoff.

**Decision: one server process per open database.** That is already how the
server is invoked — `python -m scistack_gui.server --db <path>` — so
isolation comes for free and the layering rule (CLAUDE.md NOTE 3) is
respected: "which tab am I" is a genuinely GUI-only question and never
reaches scidb.

Costs, accepted: one interpreter + one DuckDB connection per open database
(~150-250 MB and one startup discovery pass each). Both are per-database
costs the user opts into by opening a second database.

Things that are already per-database and need no work:

| Resource | Keyed by | Verdict |
|---|---|---|
| `scidb.log` file | `log_path_for(db)` — beside the .duckdb | already isolated |
| `experiment.layout.json` | beside the .duckdb (`layout.py`) | already isolated |
| pipeline/intent stores | tables inside the .duckdb | already isolated |
| plot caches | inside the .duckdb | already isolated |
| push notifications | that process's own stdout | already isolated |
| React module state | per webview iframe | already isolated |

---

## 2. The bug this explains (reported 2026-09-22)

> Opening a second database showed the new canvas but kept the **old**
> database's filename at the top left.

Two independent causes, both fixed by construction below:

1. **`extension.ts` holds one `dagPanel` and one `pythonProcess` at module
   scope.** `startPipeline` kills the old server, spawns the new one, and
   then *reuses the existing panel* (`dagPanel.updatePythonProcess`,
   `reveal()`, `postMessage({method:'dag_updated'})`). So the second database
   never got a second tab — it replaced the first one's session.
2. **The header name is fetched once.** `App.tsx`'s `refreshInfo()` calls
   `get_info` in a mount effect only. `dag_updated` makes the canvas re-fetch
   the graph; nothing re-fetches `db_name`, and `retainContextWhenHidden`
   keeps the stale React state alive. Hence: new canvas, old name.

Separately, `startPipeline` creates a **new status bar item on every call and
never disposes the previous one** (`extension.ts`, end of `startPipeline`) —
a leak that also shows an out-of-date database name, at the bottom of the
window rather than the top left.

---

## 3. Decisions (user, 2026-09-22)

| Question | Decision |
|---|---|
| Open a database that is already open | **Reveal its existing tab.** One session per database file; DuckDB is single-writer and a second server would just lose the file-lock race. |
| Plot a variable when a plot tab is open | **Always open a new tab.** The current "reuse one tab" behaviour (`PlotPanel.current`) is removed, not merely scoped per session. |
| Status bar with several databases | One item that follows the focused tab; click to switch sessions. (Chosen by default — the user's reported symptom was the in-canvas header, not this.) |
| Plot a CSV with no database open | **Add a database-less plot session** — make `--db` optional so a CSV plot spawns its own lightweight server. |

---

## 4. Target architecture (Extension Host)

```
SessionManager
  sessions: Map<sessionId, Session>      // keyed by canonical db path
  active:   Session | undefined          // last focused DAG or plot tab
  plotOnly: Session | undefined          // the db-less CSV server (Stage 6)

Session
  id           stable string (canonical db path)
  dbPath
  projectRoot  the workspace folder CONTAINING dbPath (multi-root aware)
  python       PythonProcess
  dagPanel     DagPanel                  (one per session)
  plots        PanelRegistry<PythonProcess>   (N per session)
  dbWatcher    FileSystemWatcher
  dispose()
```

Everything currently at module scope in `extension.ts` — `pythonProcess`,
`dagPanel`, `dbWatcher`, `dbWatcherDebounce`, `lastStartArgs`, the status bar
item — becomes a field on `Session`. `outputChannel` stays global (§7c).

### Which session does a command belong to?

Three sources, most to least authoritative. The chosen source is **logged**
every time, because "the command went to the wrong database" is otherwise
indistinguishable from "the command did nothing".

1. **Explicit.** A command invoked *from* a webview (DAG right-click ▸ Plot,
   the sidebar Plot button, `restart_python`) carries the `sessionId` the
   host injected into that webview's HTML. Always correct.
2. **Focused.** Command palette / status bar → `manager.active`, tracked
   through `onDidChangeViewState` on every DAG and plot panel.
3. **None open** → warn with the same wording the Plot command uses today.

### Notification routing

`session.python.onNotification` must reach **only that session's** panels:
its `dagPanel` plus its own `plots` registry. Today `PlotPanel.broadcast` is
static and would deliver session A's `plot_save_complete` into session B's
tab — the exact failure `panelRegistry.ts` was written to prevent, one level
up. The registry class is reused unchanged; only its ownership moves from a
static field to a `Session` field.

---

## 5. Stages

### Stage 0 — Diagnostics first (CLAUDE.md NOTE 2)

Before any behaviour changes, make the current single-session state legible:

* Prefix every Output Channel line with `[<db basename>]` (see §7c).
* Log session lifecycle: created / ready / rebound / disposed, with the id.
* Extend the existing "delivered to N panels" counter to name the session.
* New command **SciStack: Show Sessions** — lists open sessions, their db
  path, project root, server PID and panel count. This is the thing to ask
  for when a tab misbehaves.

### Stage 1 — Session core, still capped at one session

Introduce `session.ts` (`Session`, `SessionManager`) and move every module
global in `extension.ts` onto it. No user-visible change; the cap stays at
one. This isolates the refactor from the feature.

*Design constraint, copied from `panelRegistry.ts`:* `SessionManager`'s
routing/lookup core imports **no `vscode`**, so it is testable under
`node --test`. The `vscode`-touching parts (panel creation, watchers, status
bar) live in thin wrappers.

Tests: `sessionManager.test.ts` — add/lookup/dispose, active tracking,
"already open → reveal", routing a notification to one session only.

### Stage 2 — N pipeline tabs

* Lift the cap. `openPipeline` looks the canonical db path up in the manager:
  found → `reveal()`; not found → new `Session`.
* `DagPanel` title becomes `SciStack — <db basename>` so the tab bar is
  readable with three open.
* Per-session `dbWatcher` (already path-keyed; just moves).
* Per-session project root: `vscode.workspace.getWorkspaceFolder(dbUri)
  ?? workspaceFolders[0]`. Today every server is handed `workspaceFolders[0]`
  unconditionally, so in a multi-root workspace the second database would
  read the *first* project's `scistack.toml` and discover the wrong code
  (`config.resolve_project_root` rule 2). This is a correctness fix that
  only multi-session exposes.
* `scistack.restartPython` acts on the **active** session, not on a global
  `lastStartArgs`.
* `deactivate()` disposes every session.
* Status bar: one item, retargeted on `onDidChangeViewState`, disposed with
  the last session. Clicking it runs a new `scistack.switchSession`
  quick-pick. Fixes the leak in §2.

### Stage 3 — Frontend session awareness

* The host injects `window.__SCISTACK_SESSION__ = {id, dbName, dbPath}` into
  both webview HTMLs, beside the existing `__SCISTACK_VIEW__`.
* `api.ts` stamps `sessionId` on every outgoing RPC; the host uses it for
  source (1) above.
* `App.tsx`: the header reads the injected name immediately (no flash of
  "loading…"), and `refreshInfo()` re-runs on a new `db_changed` notification
  so a panel can never display a name its server disagrees with. This is the
  belt to Stage 2's braces for the reported bug.
* Plot Studio header shows which database the tab is bound to — with N plot
  tabs across two databases, the tab title alone is not enough.

### Stage 4 — Plot tabs: always new, per session

* `PlotTarget` gains `sessionId`.
* Delete `PlotPanel.current` and the reuse branch in `PlotPanel.show`; every
  invocation constructs a panel. `retarget()` survives only for the
  `open_plot_studio` notification path.
* `PlotPanel.openPanels` (static) → `session.plots` (instance).
* Titles carry the database: `Plot — <var> · <db>`.
* Plot tabs open in their session's DAG panel's view column
  (`dagPanel.viewColumn`), which already holds.
* A session's dispose closes its plot tabs — they cannot outlive their
  server.

### Stage 5 — Shared-resource conflicts

These are the places where two sessions genuinely collide. Each gets a test.

a. **MATLAB temp script.** `matlabTerminal.ts` writes a fixed
   `os.tmpdir()/scistack_run.m` and sends `run('<path>')`. Two sessions
   dispatching at once race on that one file and MATLAB may `run` the other
   database's script. Fix: `scistack_run_<sessionId hash>.m`.

b. **The single MATLAB engine.** One MATLAB process can only be
   `configure_database`'d to one database at a time. Serialize: refuse a
   dispatch while another session has a MATLAB run in flight, naming the
   database that holds it. `MatlabRunTracker` already knows in-flight runs
   per session; the gate needs a manager-level view across sessions.

c. **debugpy port.** `scistack.debugPort` is a fixed 5678; the second
   session's `debugpy.listen` fails (currently a swallowed warning). Allocate
   `debugPort + index` per session and log the actual port.

d. **Output Channel.** One "SciStack" channel now interleaves two servers'
   stderr. Prefix each line with `[<db basename>]` (Stage 0) rather than
   creating one channel per session — cross-session ordering is exactly what
   you want when diagnosing a lock conflict.

### Stage 6 — Database-less plot session (CSV)

`plot_service` is already prepared for this: every entry point uses
`db_connection(..., needed=not csv_path)`, and `get_source(csv_path=...)`
builds a `CsvSource` that never touches DuckDB. What blocks it is startup:
`server.py`'s `--db` is `required=True` and the startup block unconditionally
does project init, registry load and `configure_database`.

* Make `--db` optional; add `--plot-only`.
* In that mode skip project-file init, registry load, MATLAB registry and
  `configure_database`; attach the log file beside the CSV instead of beside
  the database; send `ready` with `db_loaded: false`.
* Only the `plot_*` handlers are registered; everything else returns a clear
  "no database in this session" error rather than an AttributeError on
  `get_db()` returning None.
* The manager keeps at most one plot-only session, shared by all CSV tabs,
  started lazily on the first CSV plot and disposed with its last tab.

---

## 6. Explicitly out of scope

* **Two canvases on one database.** Rejected above; revisit only if viewing
  two hypotheses side by side becomes a real need.
* **The standalone FastAPI mode** (`app.py` / `__main__.py`, the
  `scistack-gui` CLI). It is one process per invocation already — run two if
  you want two. No change.
* **Cross-session operations** (copying a node from database A to B). The
  clipboard context is per webview; a cross-session paste is a separate
  feature with its own identity questions.

---

## 7. Testing and documentation

* `extension/src/sessionManager.test.ts` (new, `node --test`, no `vscode`
  import): session identity, reveal-if-open, per-session notification
  routing, active tracking, dispose cascade.
* `extension/src/serverArgs.test.ts` (extend): project root is the workspace
  folder containing the db, not `folders[0]`.
* `scistack-gui/tests/test_server_plot_only.py` (new): `--plot-only` starts
  with no database, serves `plot_describe` for a CSV, and refuses a
  non-plot method with a named error.
* Frontend: `App.tsx` re-fetches `get_info` on `db_changed`.
* `docs/claude/gui-multi-session.md` — the process-per-database decision, the
  session-resolution order, and the shared-resource table from §5.
* `docs/claude/gui-extension-startup-path.md` — update: startup is now
  per-session, and add the plot-only branch.
* `docs/gui-manual-testing-todo.md` — per the standing rule, one checklist
  entry per GUI-visible change in Stages 2, 3, 4 and 6.
