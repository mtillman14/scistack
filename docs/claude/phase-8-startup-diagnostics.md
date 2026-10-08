# Phase 8 — GUI Startup Diagnostics

> **2026-10-08:** the uv lockfile check this doc was written for is gone
> (SciStack no longer manages environments; `.claude/plan-portability.md`
> Stage 1b). The pattern below still holds for the remaining check,
> `check_windows_config_paths`, and for any new one.

Design rationale for `scistack_gui/startup.py` and the startup-error
delivery flow. Read this before adding any new project-open check (lockfile
staleness is the first but won't be the last) so new checks follow the same
pattern and the frontend behaviour stays consistent.

The concrete task this doc was written for is Phase 8 of
`.claude/project-library-structure.md` — "on project open, detect a stale
`uv.lock` and run `uv sync`, surfacing errors visibly". The pattern
generalises to any diagnostic that needs to run before the user starts
interacting with a project.

---

## The problem

When the GUI opens a project, we need to perform checks that:

1. **Run before the user sees the DAG.** A stale venv means every function
   call the user makes could behave unexpectedly. We need to fix it (or
   refuse to proceed) before the user clicks anything.
2. **Have a GUI-visible failure mode.** Logging to stderr is invisible to
   most users. If `uv sync` fails, the dialog has to be impossible to miss.
3. **Work in both server modes.** The GUI runs two different entry points:
   - `scistack_gui/__main__.py` — FastAPI + uvicorn, used by `scistack-gui
     some.duckdb` on the CLI
   - `scistack_gui/server.py` — JSON-RPC over stdin/stdout, used by the
     VS Code extension
   Both need the same startup checks and the same user-visible failure
   mode. Implementing the logic twice would bit-rot.
4. **Be reliably delivered to the frontend.** The webview (or browser) is
   not yet mounted at the moment the backend starts. Any "push at startup"
   scheme races against frontend readiness.

## Why not just emit a JSON-RPC notification?

The obvious approach — and the one Phase 8 originally shipped with — was
to call `_send({"jsonrpc": "2.0", "method": "lockfile_sync_error", ...})`
directly from the server's startup sequence. That was removed because:

- In the VS Code extension, notifications from Python are forwarded to the
  webview via `dagPanel.postMessage`. If the DAG panel isn't open yet, or
  the React app hasn't finished mounting, the message is **dropped**.
  There's no replay.
- In the FastAPI mode, startup runs before uvicorn accepts connections,
  which means the WebSocket queue has no consumer — `ws.push_message`
  silently drops the message when `_loop is None`.
- Adding a queue/replay layer to either path is code we'd have to maintain
  forever for a feature that only needs to deliver once, on mount.

A **pull model** sidesteps both races: the frontend asks the backend
"what happened during startup?" when it's ready to handle the answer. The
natural place to put that is the `/api/info` endpoint the App component
already calls on mount.

## The shape of the solution

```
┌─────────────────────────────────────────────────────────────────────┐
│                      scistack_gui.startup                           │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  _startup_errors: list[StartupError]   (module-level state)   │  │
│  └───────────────────────────────────────────────────────────────┘  │
│       ▲                                              ▲              │
│       │ _record()                                    │ get_startup_ │
│       │                                              │ errors()     │
│       │                                              │              │
│  ┌────┴──────────────────────────┐          ┌────────┴──────────┐   │
│  │ check_lockfile_staleness()    │          │ api/schema.py     │   │
│  │                               │          │ GET /api/info     │   │
│  │   (one check per diagnostic)  │          │ _h_get_info       │   │
│  └───────────────────────────────┘          └───────────────────┘   │
│       ▲                                              ▲              │
└───────┼──────────────────────────────────────────────┼──────────────┘
        │                                              │
        │ called at startup                            │ polled on mount
        │                                              │
┌───────┴───────────────┐  ┌───────────────────────┐   │
│  __main__.py          │  │  server.py            │   │
│  (FastAPI mode)       │  │  (JSON-RPC mode)      │   │
│                       │  │                       │   │
│  startup.check_...(   │  │  startup.check_...(   │   │
│      db_path.parent)  │  │      db_path.parent)  │   │
│  for e in startup.... │  │                       │   │
│      print to stderr  │  │                       │   │
└───────────────────────┘  └───────────────────────┘   │
                                                       │
                                      ┌────────────────┴──────────────┐
                                      │  frontend/src/App.tsx         │
                                      │                               │
                                      │  useEffect(() => {            │
                                      │    get_info → startupErrors   │
                                      │  })                           │
                                      │                               │
                                      │  {blocking.length > 0 && (    │
                                      │    <StartupErrorDialog />     │
                                      │  )}                           │
                                      └───────────────────────────────┘
```

### `scistack_gui/startup.py` — the module

Holds three things:

1. **`StartupError` dataclass** — `kind`, `message`, `details`, `blocking`.
   `kind` is a stable string the frontend can switch on
   (`"lockfile_sync_failed"`, `"uv_not_installed"`, …). `blocking=True`
   tells the frontend to render a modal overlay that blocks all other
   interaction. `blocking=False` is available for future warnings that
   should show but not halt the user.
2. **Module-level `_startup_errors: list[StartupError]`** — the single
   source of truth for "what went wrong at startup". Persists across the
   lifetime of the process; does not get cleared when the user restarts
   the project since the process itself is respawned.
3. **One function per check** — today that's
   `check_lockfile_staleness(project_root)`. Each check is responsible
   for its own logging, its own error kinds, and for calling `_record()`
   on failure. They return the recorded error (or `None`) so callers can
   make their own decisions (`__main__.py` prints to stderr as a CLI
   convenience).

### `_record()` dedupes by kind

```python
def _record(err: StartupError) -> None:
    for i, existing in enumerate(_startup_errors):
        if existing.kind == err.kind:
            _startup_errors[i] = err
            return
    _startup_errors.append(err)
```

Why dedupe? Because the same check may run twice in a process (e.g. after
a manual retry hook we haven't written yet) and we don't want two copies
of the same sync-failure message stacking up. Using `kind` as the dedup
key lets distinct failures (e.g. `lockfile_sync_failed` and
`uv_not_installed`) coexist while replacing the prior record of the same
kind with the most recent version.

### `get_startup_errors()` returns a copy

```python
def get_startup_errors() -> list[StartupError]:
    return list(_startup_errors)
```

Callers including API handlers may mutate the returned list without
poisoning module state. Tests rely on this.

### Delivery via `/api/info`

```python
@router.get("/info")
def get_info():
    return {
        "db_name": get_db_path().name,
        "startup_errors": [e.to_dict() for e in startup.get_startup_errors()],
    }
```

The frontend already calls `get_info` once on App mount to populate the
header. Piggybacking startup errors onto that response costs nothing, has
no race conditions, and means the frontend always sees the current state
even on page refresh or webview reload.

The JSON-RPC server has an identical `_h_get_info` handler that returns
the same shape. In both transports, the response is a normal result to a
normal request, which both transports deliver reliably.

### The blocking dialog

`App.tsx` filters `startup_errors` by `blocking: true` and renders a full
screen overlay `<StartupErrorDialog>` when any blocking error is present.
The dialog has:

- **No dismiss button.** Per the Phase 8 plan: "don't proceed until
  resolved". Dismissing would create a path where the user interacts with
  a broken venv.
- **`role="alertdialog" aria-modal="true"`.** Proper ARIA semantics for
  screen readers and keyboard focus management.
- **Expandable details.** `err.details` (uv's stderr, tracebacks, etc.)
  is rendered inside a `<pre>` so line breaks and indentation survive.
- **Footer guidance.** "Fix the problem above, then restart the SciStack
  project to continue." — tells the user what the expected recovery flow
  is (edit `pyproject.toml`, restart the extension).

---

## Adding a new startup check

To add another project-open diagnostic (e.g. "verify the database schema
matches the code", or "warn if `.duckdb` is on a network mount"):

1. **Add a function to `scistack_gui/startup.py`** with the same shape as
   `check_lockfile_staleness`:
   ```python
   def check_schema_drift(project_root: Path) -> Optional[StartupError]:
       # ... do the check ...
       # on failure:
       err = StartupError(
           kind="schema_drift",
           message="Database schema doesn't match code.",
           details=diff_output,
           blocking=True,  # or False for a warning toast (future)
       )
       _record(err)
       return err
   ```
2. **Call it from both entry points** next to the existing
   `check_lockfile_staleness` call — one line in `__main__.py`, one line
   in `server.py`.
3. **No frontend changes needed** if the new check uses `blocking=True`
   and the `StartupError` shape. The `StartupErrorDialog` will render it
   automatically.
4. **Add tests** following the `test_startup.py` pattern: unit tests for
   the check function (patch its dependencies), plus an `/api/info`
   integration test asserting the error is surfaced.

## Testing patterns

Two things worth noting in `tests/test_startup.py`:

1. **`clear_startup_errors()` in an autouse fixture.** Module-level state
   means tests can poison each other. The autouse fixture clears errors
   before and after every test:
   ```python
   @pytest.fixture(autouse=True)
   def _reset_startup_state():
       clear_startup_errors()
       yield
       clear_startup_errors()
   ```
2. **Monkeypatching `scistack.uv_wrapper` works because the import is
   inside the function.** `check_lockfile_staleness` does
   `from scistack.uv_wrapper import is_lockfile_stale, sync` each time it
   runs. That resolves against `sys.modules["scistack.uv_wrapper"]` at
   call time, so `monkeypatch.setattr("scistack.uv_wrapper.sync", fake)`
   is picked up on the next call. If the imports were at module level
   this wouldn't work without `monkeypatch.setattr` against the
   `scistack_gui.startup` module itself.

## What this pattern deliberately doesn't do

- **No retry UI.** If sync fails, the user has to fix their
  `pyproject.toml` and restart the project. A retry button is tempting
  but adds complexity (what if it fails again? what if uv is fixed but
  the server is in a weird state?) that the current usage doesn't
  justify.
- **No persistent storage.** Errors live in memory for the process
  lifetime. Restarting the server starts with an empty list. This is
  intentional — the errors describe the *current* state of the project,
  not a history.
- **No severity beyond `blocking: bool`.** We could add info/warn/error
  levels, but the current design only has one semantic ("the user must
  fix this before proceeding") and one unused flag for future "the user
  should know but can keep working". YAGNI applies until we have a real
  warning-level check.

## Related code

- `scistack/src/scistack/uv_wrapper.py` — the `is_lockfile_stale` and
  `sync` primitives this pattern orchestrates. Lives in the scistack layer
  per CLAUDE.md's "solutions should live in the corresponding scistack
  layer" rule.
- `scistack-gui/scistack_gui/startup.py` — this module.
- `scistack-gui/scistack_gui/__main__.py` — FastAPI CLI, calls
  `check_lockfile_staleness` before `uvicorn.run`.
- `scistack-gui/scistack_gui/server.py` — JSON-RPC server, calls
  `check_lockfile_staleness` before the readiness notification.
- `scistack-gui/scistack_gui/api/schema.py` — hosts the `/api/info`
  endpoint that surfaces errors.
- `scistack-gui/frontend/src/App.tsx` — reads `startup_errors` from
  `get_info` and renders `StartupErrorDialog`.
- `scistack-gui/tests/test_startup.py` — unit + API tests.
