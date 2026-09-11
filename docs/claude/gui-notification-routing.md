# GUI notification routing: which channel reaches which webview

*Written 2026-09-11, after the stuck-"Saving…" bug (backend finished the save,
the Plot tab never heard about it). Concerns `scistack-gui` only — the
`extension/`, `frontend/`, and `scistack_gui/api` layers.*

## The question this answers

The GUI has **two ways** for the backend to tell the frontend something, and
**three webview-ish surfaces** that might need to hear it. Get the pairing
wrong and there is no error anywhere: the backend logs a clean send, the
frontend's handler is registered and correct, and the message simply evaporates
in between. Every bug of this shape looks like "the UI is stuck".

If you are adding a feature that reports progress, finishes asynchronously, or
otherwise says something the user didn't just ask for in an RPC — read this
first.

## Two channels

### 1. Request/response (RPC)

The webview calls `callBackend(method, params)` (`frontend/src/api.ts`), which
`postMessage`s `{method, params, id}` to the extension host. The host forwards
it to Python over JSON-RPC on stdin and posts the reply back with the same `id`.
`api.ts` matches `id` against its `pending` map and resolves the promise.

**This is per-panel and it always works.** Each panel class owns its own
`onDidReceiveMessage` handler (`dagPanel.ts`, `plotPanel.ts`) and replies into
its own webview. A panel can never miss its own RPC response.

**It has two independent timeouts**, and in VS Code mode both are armed on
every call:

| Where | Value | Note |
|---|---|---|
| Webview (`api.ts::callVSCode`) | **30s, hardcoded** | The binding one. Rejects the promise. |
| Extension host (`pythonProcess.ts::request`) | `scistack.rpcTimeoutMs`, default 300s | "A backstop, not a work limit." |

The webview's 30s fires first and is the one any long operation actually hits —
raising `scistack.rpcTimeoutMs` does nothing for it. That gap is the whole
reason anything ever becomes a job; see "When to use which" below.

### 2. Push notification

Python emits a message nobody asked for. Two transports, chosen at runtime:

| Mode | Entry point | Wire |
|---|---|---|
| VS Code extension (JSON-RPC) | `scistack_gui/notify.py` | newline-delimited JSON on **stdout** |
| Standalone (FastAPI) | `scistack_gui/api/ws.py` | WebSocket `/ws`, per-client outbox |

Call sites do **not** choose. They call `ws.push_message({"type": ..., ...})`,
and `ws.push_message` checks `notify._enabled` and delegates to
`notify.push_message` in JSON-RPC mode. `notify.push_message` pops `type` and
re-emits it as the JSON-RPC `method`, with the remaining keys as `params`.
Async handlers in `server.py` may call `notify(method, params)` directly.

That rename is why every frontend consumer starts with the same normalization:

```ts
const kind = (msg.type ?? msg.method) as string
const params = (msg.params ?? msg) as Record<string, unknown>
```

WebSocket delivers the dict as sent (`type` at top level); JSON-RPC delivers it
wrapped (`method` + `params`). Same message, two shapes.

Current notification types: `dag_updated`, `run_start`, `run_output`,
`run_progress`, `run_done`, `show_rendered`, `plot_save_progress`,
`plot_save_complete`, `plot_save_failed`.

## Three surfaces

1. **The DAG webview** (`dagPanel.ts` → `App.tsx`) — the pipeline canvas.
   A singleton the extension holds in a module variable.
2. **The Plot tab** (`plotPanel.ts` → `PlotRoot.tsx` → `PlotStudio`) — its own
   editor tab. **Not a child of the DAG panel.** There can be more than one
   (`newTab`), and each is created long after startup.
3. **Embedded PlotStudio** — the *same component* mounted inside the DAG
   webview (`VariableSettingsPanel.tsx`, `PipelineDAG.tsx`).

Surfaces 2 and 3 run identical React code. **They do not have identical
delivery.** That asymmetry is the trap: a notification-driven plot feature can
work perfectly in the sidebar and be dead in the Plot tab, which reads as
"it works sometimes" rather than as a routing bug.

One bundle serves all three; `plotPanel.ts` injects
`window.__SCISTACK_VIEW__` and `main.tsx` mounts `PlotRoot` instead of `App`
(see `main.tsx` — "one bundle, two roots").

## The routing table

`extension.ts` registers exactly one `pythonProcess.onNotification` handler,
and that handler is the *only* thing standing between Python's stdout and any
webview. It must fan out to **both** panel sinks:

```ts
pythonProcess.onNotification((method, params) => {
  PlotPanel.broadcast({ method, params });   // all open plot tabs
  if (dagPanel) dagPanel.postMessage({ method, params });
});
```

- **DAG panel**: one `if`, because it is a singleton.
- **Plot tabs**: `PlotPanel.broadcast` over a `PanelRegistry`
  (`extension/src/panelRegistry.ts`). Panels register in their constructor and
  unregister in `dispose`. A registry rather than a variable because plot tabs
  are plural, are created after the handler was registered, and close
  independently.

`PanelRegistry` is deliberately free of any `vscode` import — a panel is only
"something you can `postMessage` to" — so the routing is covered by
`panelRegistry.test.ts` under `node --test` (`npm test` in `extension/`).
`send()` returns a delivery count and swallows a single dead webview's throw so
it cannot block delivery to the rest.

In **standalone mode** none of this applies: `ws.py` fans out to every
connected client, and a browser page is one client regardless of which
component is mounted. This entire section is a VS Code–only concern.

## A third source: host-originated notifications

Not every notification comes from Python. The extension host synthesizes some
itself, posting directly into one panel:

- `open_plot_studio` — `PlotPanel.retarget`, when plotting a second variable
  reuses the open tab (`PlotRoot` listens for it and remounts `PlotStudio`).
- `run_output` / `run_done` — `dagPanel.ts` fabricates these for the MATLAB
  terminal and clipboard tiers, which finish inside the extension and never
  produce a Python notification at all.

These are point-to-point by design and correctly bypass the routing table.
Mentioned so you don't "fix" them into broadcasts.

## When to use which

Use an **RPC** when the answer is bounded and the panel can wait. Use a **job +
notifications** when it cannot — and note that "cannot" is measured, not
guessed:

> A full-resolution single figure of a 1-D measure took ~12 minutes against a
> 30s client timeout. The single-figure save had been an RPC on the theory that
> one figure is cheap next to a fan-out. There is no timeout value that fixes
> that; the shape has to change.
> — `services/plot_service.py::start_save_job`

The practical threshold is the webview's hardcoded 30s, not the configurable
300s — anything that can plausibly exceed half a minute wants a job.

The job contract, worth copying for the next one:

- Return `{"job_id": ...}` from the RPC **immediately**.
- The frontend **generates the id itself** and adopts it *before* the request
  leaves (`PlotStudio.tsx`). A handler that learned the id from the response
  would drop messages from a job that finished while the promise was still
  settling.
- Filter on that id (`if (params.job_id !== saveJob.current) return`) — two
  panels can run jobs at once.
- Emit **exactly one** terminal message (`*_complete` / `*_failed`) on every
  path including an unhandled exception. A panel that disables its buttons for
  the duration has no other way to learn it is over.
- Release the UI in the request's `catch` too: a request that never reached the
  backend has no job to report.

## Diagnosing "the UI is stuck"

Work outward. Each hop logs, and the hop that stops logging is the broken one.

1. **Did the backend finish?** `scidb.log` — the service's own completion line
   (`[plot] save job ps-… complete: 1 file(s) in 2.8s`).
2. **Did it try to send?** `scidb.log` — `[notify] Emitting <method> to
   extension host`. Note this line is logged **unconditionally**; it says the
   frame was written to stdout, *not* that anyone received it. In WebSocket
   mode the equivalent lines are `[ws]`, which do report drops
   (`[ws] DROPPED … no clients connected`).
3. **Did the host route it?** SciStack output channel — `[notify] <method>
   (job=…) → N plot panel(s)`. **`N = 0` is the signature of this bug class.**
4. **Did the webview dispatch it?** `api.ts` routes `id`-bearing frames to
   `pending` and `method`-bearing frames to `_notificationHandlers`. A message
   with neither is silently dropped.
5. **Did the component filter it out?** The `job_id` guard, or the
   `kind.startsWith(...)` prefix check.

Step 3 exists *because* step 2 lies by omission. Before it, a notification with
no listener was indistinguishable from one that was never sent.

## The bug this document exists for

`plot_save_start` was converted from an RPC into a background job. As an RPC
the result came back through the panel's own per-panel response channel, which
always works. As a job it came back as a notification — and `extension.ts`
forwarded notifications to `dagPanel` only. The canvas received
`plot_save_complete` and ignored it; the Plot tab that started the save never
ran `setSaving(false)` and its Save button stayed disabled for the rest of the
session.

Nothing errored. The backend log was clean. The frontend handler was correct.

**Checklist when adding a notification-driven feature:** does it reach the DAG
panel *and* every plot tab; is the terminal message guaranteed on all paths;
does the frontend normalize `type`/`method`; and is there a log line on the
*receiving* side that would show a count of zero.

## See also

- `docs/claude/gui-vscode-extension.md` — panel/process lifecycle
- `docs/claude/scistack-gui-backend-internals.md` — the JSON-RPC server loop
- `docs/claude/logging-architecture.md` — where `scidb.log` comes from
- `docs/claude/matlab-run-database-ownership.md` — the one-response-per-RPC rule
