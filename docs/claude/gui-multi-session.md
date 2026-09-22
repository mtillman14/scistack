# Multi-session GUI: one tab per database

How the VS Code extension opens several SciStack databases at once, why each
one gets its own Python process, and which shared resources still have to be
kept apart by hand.

Implemented 2026-09-22 — all stages of `.claude/plan-multi-session-tabs.md`.
One limitation is inherent rather than unfinished; see
[Known limits](#8-known-limits).

---

## 1. One process per database

**The decision: a session is an operating-system process.**

The Python backend is pervasively process-global. `scistack_gui.db` holds
`_db`, `_db_path`, `_db_refcount` and `_connection_policy`;
`scistack_gui.registry` holds `_functions`, `_parameters`, `_path_inputs`
and `_config`; `scistack_gui.config` holds `_project_root_hint`; and below
all of it `scifor.set_schema` and `scidb.configure_database` are themselves
global. Teaching that stack to hold two databases at once would mean
threading a session object through layers that have no reason to know the
GUI has tabs at all.

`python -m scistack_gui.server --db <path>` already takes the database as an
argument, so spawning it twice buys the same isolation from the operating
system, for free.

The cost is one interpreter and one DuckDB connection per open database —
paid only by a user who opens a second one.

### What was already per-database

Nothing in the backend had to change to make two servers coexist, because
everything a server writes outside its own memory is already keyed to the
database file:

| Resource | Keyed by |
|---|---|
| `scidb.log` | `scidb.log.log_path_for(db)` — beside the `.duckdb` |
| `<name>.layout.json` | beside the `.duckdb` (`layout.py`) |
| pipeline / intent / plot-cache tables | inside the `.duckdb` |
| push notifications | that process's own stdout |
| React state | per webview iframe |

### What two sessions must never do

**Open the same file.** DuckDB is single-writer; a second server on one
database would lose the lock race and report it as a `DatabaseLockedError`
with no indication that the user caused it by opening the database twice.
`SessionManager.open` therefore looks the canonical path up and reveals the
existing tab instead.

"Canonical" is `sessionCore.sessionIdForDb`: `path.resolve`, lowercased on
Windows. Two spellings of one file are one session.

---

## 2. The shape in the extension host

```
SessionManager                       (session.ts — needs vscode)
  registry: SessionRegistry<Session> (sessionCore.ts — no vscode, tested)
  plotOnly: Session | undefined      (the database-less CSV server)

Session
  id           canonical database path
  dbPath       '' for the plot-only session
  label        basename — the tab title, the log prefix, the status bar
  projectRoot  the workspace folder CONTAINING this database
  python       PythonProcess          (replaced by restart)
  dagPanel     DagPanel               (undefined when plot-only)
  plots        PanelRegistry          (this session's plot tabs)
  debugPort    its own debugpy port
  watcher      its own .duckdb FileSystemWatcher
```

`sessionCore.ts` holds everything that is not about VS Code — identity,
lookup, project-root choice, the log prefix, and "which session does this
command mean" — for the same reason `panelRegistry.ts` does: it makes the
routing testable under `node --test` (`sessionCore.test.ts`).

### Panels read through their session

A panel never stores a `PythonProcess`. It calls `this.session.python`
at request time, so `Restart Python` swapping the process is invisible to
every open tab.

This retires a whole bug class by construction. `PanelRegistry` used to
carry a `rebind` half whose job was to hand every plot tab the replacement
process; a tab that missed it wrote to a destroyed stdin
(`ERR_STREAM_DESTROYED`, surfacing as "Could not open the plot panel",
2026-09-15). There is now no stored handle to go stale, and `rebind` is
gone.

### Which session does a command mean?

`SessionRegistry.resolve` answers in this order, and `resolveForCommand`
**logs which rule applied** — "the command went to the other database" and
"the command did nothing" are indistinguishable from the outside otherwise.

1. **explicit** — a command raised from inside a webview carries that
   panel's session id (`dagPanel.ts` stamps it on `open_plot_panel`). The
   host handler is the panel's own closure, so this is always right.
2. **only** — exactly one database is open. Not redundant with (3): a
   command can arrive before any panel has ever been focused, e.g. the
   Explorer context menu right after startup.
3. **active** — the last focused canvas or plot tab, tracked through
   `onDidChangeViewState`.

A stale explicit id (a tab outliving its session) falls through to (2)/(3)
rather than failing: the user's intent is still "plot something".

The **plot-only session is deliberately outside the registry**, so it can
never be the answer. In a window with only a CSV tab open it would otherwise
resolve "Plot Variable…" to the one server structurally incapable of serving
it.

### Notifications are per session

`Session.route` delivers a push notification to that session's canvas and
that session's plot tabs, and to nothing else. A `plot_save_complete` from
one database re-enabling the Save button of a tab plotting another is the
same failure `panelRegistry.ts` documents, one level up.

---

## 3. The project root is per session

`config.resolve_project_root` rule 2 takes `--project-root` at its word, and
the extension used to pass `workspaceFolders[0]` to every server. That is
right for exactly one session. With two databases open from a multi-root
workspace, the second server would read the **first** project's
`scistack.toml` and discover the wrong code — silently, because an empty or
wrong registry looks like a bare canvas, not an error.

`sessionCore.projectRootForDb` picks the innermost open folder that contains
the database, falling back to the first folder (correct for the common case
of a `.duckdb` under `datasets/` in the one open project) and then to
undefined, which makes the server fall back to its cwd and the extension
warn.

---

## 4. Diagnostics

One Output Channel, still, because when two databases fight over a file lock
the thing you need is the **interleaving** — which separate channels destroy.
Every line is prefixed with the database it came from
(`sessionCore.prefixedLog`), including every line of a multi-line stderr
chunk: a traceback whose first line alone is attributed tells you nothing.

**`SciStack: Show Open Sessions`** prints, for every live server: label,
database path, project root, debugpy port, whether its canvas is open, and
how many plot tabs it has. This is the first thing to ask for when a tab
misbehaves, because nothing in VS Code shows which server a webview talks to.

`SciStack: Switch Database` lists the open databases and reveals one; the
status bar item runs it.

### debugpy ports

`scistack.debugPort` is now a **base**. Each session gets `base + n` for the
first free `n`, because `debugpy.listen` on a port another server holds
fails — and the server treats that failure as a warning, so with one fixed
port the second session silently has no debugger at all.

Each canvas also names its debug session after its database
(`Attach to scistack-gui server (<db>)`), or `findExistingDebugSession`
would let one canvas adopt the other's debugger.

---

## 5. The database-less plot session

Explorer ▸ **Plot CSV** needs no project and no DuckDB:
`plot_service.get_source(csv_path=...)` builds a `scistackplot` `CsvSource`,
and every plot entry point is already written as
`db_connection(..., needed=not csv_path)`.

What made it need a pipeline anyway was *startup*, not plotting: `--db` was
required, so a CSV tab had to borrow an open project's server and died when
that project closed.

`python -m scistack_gui.server --plot-only` starts with no database, no
project-file init, no code discovery, no MATLAB registry and no
`configure_database`.

### How a method declares it can work without a database

`Handler.db_optional` — one more field on the row that already declares
everything else about a method (`api/handlers.py`). It means "this call
decides from its own request whether it needs the database", and the only
thing that decides is `csv_path`. `handlers.without_database()` derives the
served set from the table, so a new `plot_*` method cannot be forgotten in a
hand-kept list.

Two consequences, both tested in `tests/test_server_plot_only.py`:

* the dispatch hands a `db_optional` method `None` instead of calling
  `get_db()`, which would otherwise raise *before* the handler ever saw the
  `csv_path` saying it needed no database;
* `server._handle_request` refuses every other method **by name, up front**,
  so the user is told "this plot tab was opened on a file, not a project"
  rather than "Database not initialised. Call init_db() first." — which
  reads like a broken session instead of one that never had a database.

`plot_add_to_pipeline`, `plot_variant_sets_save` and `plot_invalidate` are
deliberately *not* marked: writing an endpoint into the project, saving
variant sets and dropping the cache all need somewhere to put them. The Plot
Studio hides "Add to pipeline" on a CSV tab to match. "Export code" stays —
`plot_export` is `db_optional` and works from a CSV.

### Where a plot-only server logs

It has no database, so `scidb.log`'s "beside the `.duckdb`" convention has no
anchor, and dropping a log into whichever folder the user's CSV happens to
live in would scatter logs through their data. `--log-file` names the path,
and the extension passes one inside its own storage (`context.logUri`).

---

## 6. Plot tabs

**Every plot opens its own tab.** There used to be one reused tab per window
(`PlotPanel.current`), so plotting a second variable retargeted it — which
destroyed the figure you were looking at and made "compare these two"
impossible without saving one to disk first. Comparing figures is the normal
reason to open two, so a new tab is the normal outcome and closing one is a
click.

Three things fell out of that once tabs were per session:

* `PlotPanel.retarget` and the `open_plot_studio` notification are gone —
  they existed only to re-point a reused tab. `PlotRoot` now reads its
  target once, from the injection, and never changes it.
* every tab's title names its database (`Plot — StepLength · gait.duckdb`),
  and so does the Plot Studio's own header. A figure you cannot attribute is
  a figure you cannot use, and with two databases open the variable name
  alone does not say which one you are looking at.
* a tab opens in its session's canvas group (`dagPanel.viewColumn`), so it
  is a sibling of the canvas it came from rather than a split nobody asked
  for; and closing a session closes its plot tabs, because they cannot
  outlive the server they send every `plot_*` RPC to.

---

## 7. MATLAB across databases

**MATLAB is not one process per window** — that was the first assumption
here and it was wrong. There are two dispatch tiers and only one of them is
shared:

| Tier | Process | Shared between databases? |
|---|---|---|
| sidecar (`matlab_sidecar.py`) | `_sidecar`, a **process** singleton | **No.** Every session has its own Python server, so every session already has its own MATLAB — as independent as two Python runs |
| MathWorks terminal | the extension's one MATLAB per VS Code window | **Yes**, and not by our choice |

So the parallelism you would expect from process-per-database already exists
on the sidecar path. Two problems remain, with unequal fixes.

### The temp script file — fixed outright

`matlabTerminal.ts` writes the generated script to a temp file and sends
MATLAB a one-line `run('<path>')`, because the MathWorks pseudoterminal
silently drops multi-line payloads. That filename was fixed, so two sessions
dispatching close together raced: the second write landed before the first
`run` read it, and one canvas ran the other's script. Each session now
writes `scistack_run_<slug>.m` (`sessionCore.sessionSlug`, an FNV-1a hash of
the canonical database path — two projects may both hold a `data.duckdb`,
and the full path is not a legal filename). No run tracking involved.

### The shared terminal's current database — gated

A generated script calls `configure_database` before anything else, so two
runs through the SAME MATLAB do not proceed in parallel: the second repoints
the engine mid-run and the first run's remaining `for_each` calls write into
the other project's database. Both writes are well-formed, so nothing
downstream can detect it.

`DagPanel.refuseIfMatlabBusyElsewhere` refuses it before the script is
generated, naming the database to wait for. The decision is
`matlabConnectionGate.matlabHolder` (pure, unit-tested); a session never
blocks itself, since several MATLAB runs against ONE database is the
existing supported case.

**It gates `sharedEngineActive`, not `isActive`.** Gating on "any MATLAB run
in flight" was exactly backwards: a sidecar run holds that mark for its whole
duration (Python pushes a real `run_done`) and would have blocked the other
database pointlessly, while a terminal run — the one that genuinely shares an
engine — clears it milliseconds after dispatch.
`MatlabRunTracker.noteSharedEngine` is called only by the terminal and
clipboard tiers.

---

## 8. Known limits

**None of the MATLAB ones, since 2026-09-22.** The shared-terminal gate used
to cover only the dispatch window, because nothing reported when a MATLAB
terminal run ended. That gap is closed: the run writes markers and the
watcher corroborates them against the DuckDB lock holder
(`docs/claude/matlab-run-completion.md`), so `MatlabRunTracker` —
`sharedEngineActive` included — now spans the real run and the gate refuses a
second database's Run for as long as MATLAB is genuinely busy.

What remains is not a limit on parallelism: the sidecar tier runs one MATLAB
process per session with no gate at all, and a second VS Code window has its
own MathWorks MATLAB. Only the *single shared terminal* is serialised, which
is what sharing one MATLAB means.
