# VS Code Extension: Server Startup Path and Failure Diagnostics

How the extension gets from "user ran **SciStack: Open Pipeline**" to a live
Python server, what can go wrong along the way, and how each failure is
reported. This is the *current* implementation; `gui-vscode-extension.md` is
the older pre-implementation migration plan and does not describe this path.

Everything here lives in `scistack-gui/extension/src/` (Extension Host,
TypeScript). Per CLAUDE.md NOTE 3 this is correctly a GUI-layer concern: the
question "which interpreter did VS Code pick, and does it have the packages"
has no meaning below the GUI layer.

---

## 1. The startup sequence

> **Since 2026-09-22 this path is per session.** The extension opens one
> server process per database (`gui-multi-session.md`), so what was one
> `startPipeline()` at module scope is now `SessionManager.open()` ->
> `spawnReady()` in `extension/src/session.ts`. The sequence below is
> unchanged; it just runs once per open database, and `--project-root` is
> now the workspace folder containing THAT database rather than
> `workspaceFolders[0]`. A `--plot-only` server takes the same path with no
> database at all.

`SessionManager.open()` is the single entry point — both the **Open
Pipeline** command and **Restart Python Process** go through `spawnReady`
(restart reuses the session's own dbPath and project root).

```
resolvePythonPath()            -> { path, source }
new PythonProcess(...)         -> spawn(python, ['-m','scistack_gui.server', ...])
pythonProcess.waitForReady(ms) -> resolves on the "ready" notification
DagPanel created / revealed
setupDbWatcher(dbPath)
status bar item
```

### Interpreter resolution (`resolvePythonPath`)

Returns both the path **and** where it came from, because "which interpreter"
and "where do I change it" are the two questions a failed start has to answer:

| Order | Source string | Origin |
|---|---|---|
| 1 | `scistack.pythonPath setting` | The extension setting, if non-empty |
| 2 | `active interpreter from the Python extension` | `ms-python.python` → `exports.environments.getActiveEnvironmentPath()` |
| 3 | `PATH fallback (no Python extension interpreter)` | Literal `python3` |

VS Code does **not** inherit a venv from the shell it was launched from, so
in practice step 2 decides, and step 3 is a near-guaranteed failure on
Windows (the `python3` App Execution Alias resolves to a Microsoft Store
advertisement, not an interpreter).

### The spawn (`pythonProcess.ts`)

```
python -m scistack_gui.server --db <path>
       [--project <path> | --module <path>]
       [--schema-keys k1,k2]
       [--project-root <workspace folder>]
```

`--project-root` is the first workspace folder. It exists because a `.duckdb`
usually lives in a `datasets/` subfolder, and without it a new
`scistack.toml` + entities file would be written next to the data instead of
at the project root (see `entities-toml-format.md`).

Env vars `SCISTACK_GUI_DEBUG` / `SCISTACK_GUI_DEBUG_PORT` are added when
`scistack.debug` is on, which makes `server.py` start a debugpy listener.

### The readiness handshake

The child speaks newline-delimited JSON-RPC on **stdout**; stderr is logging
only (`scidb.log`'s console sink, raised to DEBUG) and is forwarded to the
**SciStack** Output Channel.

Three startup-relevant notification methods:

| Method | Sent by `server.py` | Effect in `PythonProcess.handleLine` |
|---|---|---|
| `progress` | `_send_progress()` at each startup phase (loading project config, MATLAB registry, auto-discovery, opening database) | Logged; **resets the readiness timer** |
| `ready` | Once, after startup completes, with `db_name` + `schema_keys` | Resolves `waitForReady` |
| `error` | Startup failure the server could catch | Rejects `waitForReady` with the server's own message |

`waitForReady(timeoutMs)` is an **inactivity** timeout
(`scistack.startupTimeoutMs`, default 60000), not a total budget: every
`progress` notification restarts it, so a slow-but-progressing startup on a
network drive completes while a genuinely stuck server is still killed.

`waitForReady` can also reject from the `exit` handler (child died) or the
`error` handler (spawn failed).

---

## 2. Why the failure message needed diagnostics

Every one of the failure paths above used to surface as:

```
SciStack: Server failed to start — Error: Python process exited (code=1, signal=null)
```

which names neither the interpreter nor the cause. The dominant real-world
case is mundane: VS Code's active interpreter is simply a *different*
environment from the one where `dev-install.sh` was run, so
`python -m scistack_gui.server` prints `No module named scistack_gui` to
stderr and exits 1. That stderr went to the Output Channel and was then
discarded, and nothing in the message pointed at the Output Channel.

## 3. What happens now on failure

`startPipeline`'s catch block hands off to `reportStartupFailure`:

1. **Wait for the child to close.** `PythonProcess.whenClosed(2000)` — the
   ready promise can reject on `exit` (or on the inactivity timer) while the
   last stderr chunk is still queued, so quoting stderr before `close` can
   quote an empty traceback.
2. **Probe the interpreter.** `probeInterpreter(pythonPath)` runs
   `python -c <script>` and gets back `sys.executable`, `sys.version`,
   `sys.prefix`, and a `find_spec` result for each of `scistack_gui`,
   `scidb`, `scifor`, `duckdb`.
   - It uses `importlib.util.find_spec`, **not** `import`: no package code
     executes (fast, no side effects), and it can distinguish "scistack_gui
     is here but scidb is not" from "nothing is here".
   - It never throws. Interpreter unrunnable, probe timeout (10s), and
     non-JSON output are all reported as fields on the result.
3. **Classify.** `diagnoseStartupFailure(ctx)` is pure — probe + stderr tail
   + exit code + error message in, diagnosis out.
4. **Report.** Full detail block to the Output Channel; one-line message as a
   notification with action buttons.

### The five diagnoses

Checked in this order (`startupDiagnostics.ts`):

| Kind | Trigger | Actions offered |
|---|---|---|
| `interpreter_missing` | ENOENT-ish text in the error or the probe's spawn error / raw output — includes the Windows Microsoft Store alias | Select Interpreter, Open Settings, Show Details |
| `package_missing` | Probe says `scistack_gui` not found, or stderr says `No module named scistack_gui` | Copy Install Command, Select Interpreter, Show Details |
| `dependency_missing` | `scistack_gui` present but `scidb`/`scifor`/`duckdb` missing, or stderr names some other missing module | Copy Install Command, Select Interpreter, Show Details |
| `startup_timeout` | Error message matches `did not become ready` | Show Details |
| `server_error` | An exception line (incl. dotted names like `duckdb.IOException`) in the stderr tail | Show Details |
| `unknown` | Anything else — still names the interpreter and quotes the error | Show Details |

Ordering matters: the probe is consulted before stderr, because a missing
`scidb` produces a traceback whose *last* line is the `ModuleNotFoundError`
but whose first frames are inside `scistack_gui` — the probe settles which
package is actually absent rather than inferring it from text.

### What the message says

The notification always names the interpreter, and shows the resolved
`sys.executable` too when it differs from the configured path (e.g. a
`python3` shim):

```
SciStack: the Python environment "/Users/x/.venvs/analysis/bin/python" does not
have scistack_gui installed. Install it there, or switch to the environment that
has it.
```

The Output Channel gets the full report:

```
=== SciStack startup failure (package_missing) ===
Interpreter (configured): /Users/x/.venvs/analysis/bin/python
Interpreter source: active interpreter from the Python extension
Interpreter (sys.executable): /Users/x/.venvs/analysis/bin/python
Environment (sys.prefix): /Users/x/.venvs/analysis
Python version: 3.11.8
Command: <python> -m scistack_gui.server --db ... --project-root ...
Exit code: 1
Packages:
  - scistack_gui: NOT FOUND
  - scidb: found at /.../scidb/__init__.py
  ...
Failure: Python process exited (code=1, signal=null)
Server stderr (tail):
<python>: No module named scistack_gui
Install with: /Users/x/.venvs/analysis/bin/python -m pip install scistack-gui
=== end of startup failure report ===
```

Note the install hint is the published name; for a source checkout the real
fix is `dev-install.sh`, which installs every layer in dependency order.

### Supporting state on `PythonProcess`

Added for the report, all cheap and always-on:

- `stderrTail` — ring buffer of the last 200 stderr lines (`getStderr()`).
  Previously stderr was written to the Output Channel and dropped.
- `exitCode` — captured in the `exit` handler (`getExitCode()`).
- `pythonPath`, `args` — public readonly, so the report can print the exact
  command.
- `closed` — a promise resolved on `close`, exposed via `whenClosed(ms)`.

---

## 4. Tests

`extension/src/startupDiagnostics.test.ts`, run with `npm test` in
`extension/` (`tsc -p tsconfig.test.json && node --test dist/test`). This is
the extension's only test suite and the reason `tsconfig.test.json` exists:
it compiles **only** `vscode`-free modules, so node's built-in runner can
execute them with no VS Code host and no extra dependencies. `dist/test/` is
gitignored; the rest of `dist/` is the committed bundle.

`startupDiagnostics.ts` is deliberately split so this is possible:
`diagnoseStartupFailure` is pure, `probeInterpreter` touches only
`child_process`, and nothing in the file imports `vscode`. All VS Code
interaction (notifications, buttons, clipboard, `python.setInterpreter`)
stays in `extension.ts`.

Covered: each diagnosis kind, stderr-only classification when no probe ran,
configured-path vs `sys.executable` both appearing, Windows Store alias,
quoting of interpreter paths containing spaces, and the negative assertion
that `code=1` is no longer the user-facing cause.

---

## 5. Gotchas

- **`npm run build` uses esbuild, which does not typecheck.** Type errors sit
  in `src/` unnoticed; run `npx tsc --noEmit -p tsconfig.json` explicitly.
  (This is how `dagPanel.ts:201`'s `vscode.TextEditorRevealKind` — no such
  export; it is `TextEditorRevealType` — survived.)
- **`dist/extension.js` is committed.** A source change is not live for the
  user until the bundle is rebuilt.
- **The probe runs only on the failure path**, so the happy path pays nothing
  for it. Resist the urge to make it a preflight check on every start.
- **Do not classify from stderr alone** when a probe is available — see the
  ordering note above.
- **stdout is protocol, stderr is logging.** Anything written to the child's
  stdout that is not JSON-RPC breaks the handshake; `server.py` routes all
  logging to stderr for exactly this reason.
