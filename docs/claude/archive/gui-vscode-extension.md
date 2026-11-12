# Migrating scistack-gui to a VS Code Extension

> **Archived 2026-11-10** (grug follow-up, one live doc per concept). Superseded by
> gui-extension-startup-path.md (how the shipped extension starts). Kept for history; do not treat as current.

> **Status:** this is the pre-implementation migration plan, kept for its
> architecture rationale. For how the shipped extension actually starts the
> Python server (interpreter resolution, the readiness handshake, and
> startup-failure diagnostics), see `gui-extension-startup-path.md`.

## Why Migrate?

The standalone GUI (React + FastAPI) needs a code editor and debugger to be a self-contained solution. Rather than reimplementing what VS Code already provides, we can embed the scistack DAG UI inside VS Code and get editor, debugger, terminal, git, and file management for free.

---

## 1. VS Code Extension Development 101

### What Is a VS Code Extension?

A VS Code extension is a TypeScript/JavaScript package that runs inside VS Code's **Extension Host** -- a separate Node.js process. Extensions can add commands, views, panels, language features, and custom editors. They are packaged as `.vsix` files and distributed through the VS Code Marketplace or installed locally.

### The Three Execution Contexts

Understanding where code runs is the single most important concept:

**Extension Host (Node.js):** Your TypeScript code (`extension.ts`) runs here. It has access to all VS Code APIs (file system, workspace, commands, tree views, status bar, terminals) but NO DOM or browser environment. Think of it as the orchestrator.

**Webview (sandboxed iframe):** An isolated Chromium iframe that VS Code renders inside a panel. It runs your HTML/CSS/JavaScript -- in our case, the React DAG app. It has NO direct access to VS Code APIs or the file system. It communicates with the Extension Host solely through `postMessage()` / `onDidReceiveMessage()`.

**Python process (child process):** The Python backend runs as a child process spawned by the Extension Host. It communicates over stdin/stdout using JSON-RPC.

Data flow:

```
Webview (React DAG UI)
    |  postMessage / onDidReceiveMessage
Extension Host (Node.js, TypeScript)
    |  stdin/stdout JSON-RPC
Python Process (scistack backend)
    |  DuckDB, user .py module, scidb
Local filesystem
```

### Project Structure

```
scistack-gui-vscode/
  package.json          # Extension manifest (commands, views, activation events)
  tsconfig.json         # TypeScript config for extension host
  src/
    extension.ts        # Entry point: activate() and deactivate()
    pythonProcess.ts    # Manages the child Python process
    dagPanel.ts         # Creates and manages the Webview panel
    sidebarProvider.ts  # TreeDataProvider for sidebar tree views
  webview/              # Separate sub-project for the React Webview UI
    package.json
    tsconfig.json
    vite.config.ts
    src/
      App.tsx           # Existing React app, adapted
      components/       # Existing components, mostly reused
      hooks/
        useExtensionMessage.ts  # Replaces useWebSocket.ts
  python/               # Python backend, adapted to JSON-RPC over stdio
    scistack_gui/
      server.py         # Replaces __main__.py + app.py
      ...               # All other modules reused
  .vscode/
    launch.json         # Debug configurations for F5
  .vscodeignore         # Files to exclude from .vsix
```

### Development Workflow

1. Open the extension project in VS Code
2. Press **F5** -- this launches a new VS Code window (the "Extension Development Host") with your extension loaded
3. The extension host code is debuggable with breakpoints directly in VS Code
4. For Webview debugging: Command Palette > "Developer: Toggle Developer Tools" opens Chrome DevTools for the iframe
5. For Python: attach a debugpy debugger or read stdout logs

### Packaging and Distribution

```bash
npm install -g @vscode/vsce    # Install packaging tool
vsce package                    # Produces scistack-gui-x.y.z.vsix
vsce publish                    # Publish to Marketplace (needs publisher account)
# Users install with:
code --install-extension scistack-gui-x.y.z.vsix
```

### Key VS Code APIs

| API | Purpose |
|-----|---------|
| `window.createWebviewPanel()` | Creates the DAG canvas panel |
| `WebviewPanel.webview.postMessage()` | Send data to Webview |
| `WebviewPanel.webview.onDidReceiveMessage` | Receive requests from Webview |
| `window.registerTreeDataProvider()` | Sidebar tree views |
| `commands.registerCommand()` | Commands (Open Pipeline, Refresh, Run) |
| `window.createStatusBarItem()` | Status bar (DB name, connection status) |
| `child_process.spawn()` | Launch Python backend |

---

## 2. Architecture Mapping

### What Becomes a Webview Panel

The **DAG canvas** must remain a Webview. There is no VS Code native equivalent of a zoomable/pannable graph canvas. This includes:

- `PipelineDAG.tsx` (React Flow canvas)
- `FunctionNode.tsx`, `VariableNode.tsx`, `ConstantNode.tsx`, `PathInputNode.tsx`
- `layout.ts` (dagre auto-layout)
- Context providers (`RunLogContext`, `SelectedNodeContext`)
- Settings panels (`FunctionSettingsPanel`, `ConstantSettingsPanel`, `VariableSettingsPanel`, `PathInputSettingsPanel`) -- these are interactive forms that TreeViews can't handle

### What Becomes Native VS Code UI

| Current Component | VS Code Equivalent |
|---|---|
| Header (DB name, schema keys) | **Status bar items** |
| Refresh button | VS Code **command** (Command Palette + optional toolbar button) |
| EditTab (palette) | **TreeView** in sidebar, or keep in Webview (see risks) |
| RunsTab (execution logs) | **Output Channel** or **TreeView**, or keep in Webview |

### Python Backend: JSON-RPC over stdio (Recommended)

The extension host spawns `python -m scistack_gui.server` as a child process. Communication uses JSON-RPC over stdin/stdout. This eliminates FastAPI, uvicorn, HTTP, CORS, and WebSocket entirely.

Why this approach:
- No port conflicts or network setup
- Process lifecycle managed by the extension (start on activate, kill on deactivate)
- This is the established pattern (how the Python Language Server works)

Message flow example:

```
Webview                        Extension Host                  Python
-------                        --------------                  ------
postMessage({                  onDidReceiveMessage ->
  method: "get_pipeline",        child.stdin.write() ->        reads JSON-RPC request
  id: 1                                                        processes, writes response
})                               <- child.stdout.on('data')    {"id":1, "result":{...}}
                               webview.postMessage({
<- onMessage({                   id: 1, data: {...}
     id: 1, data: {...}       })
   })

                               --- Push notifications ---
                               <- child.stdout notification    {"method":"run_output",...}
                               webview.postMessage({
<- onMessage({                   type: "notification",
     method: "run_output"        method: "run_output"
   })                          })
```

---

## 3. Reuse Assessment

### Frontend React (~2500 lines) -- ~80% reusable

| Component | Reusable? | Changes |
|---|---|---|
| `PipelineDAG.tsx` | Yes | Replace `fetch()` with `postMessage()` |
| `FunctionNode.tsx` | Yes | Same fetch-to-postMessage swap |
| `VariableNode.tsx`, `ConstantNode.tsx`, `PathInputNode.tsx` | Yes | Minimal changes |
| `layout.ts` (dagre) | Yes | Zero changes |
| `RunLogContext.tsx`, `SelectedNodeContext.tsx` | Yes | Zero changes |
| `useWebSocket.ts` | **Replaced** | New `useExtensionMessage.ts` (~15 lines) wrapping `window.addEventListener('message')` |
| Settings panels | Yes | Same fetch-to-postMessage swap |
| `App.tsx` | Partially | Remove header (moves to status bar) |
| `Sidebar.tsx`, `EditTab.tsx`, `RunsTab.tsx` | Decision point | Keep in Webview (easy, ~0 changes) or migrate to native (moderate effort, more idiomatic) |

**The key pattern change:** Every `fetch('/api/...')` becomes `vscode.postMessage({method: '...', params: {...}})`. A small utility (~30 lines) wraps this into `callBackend(method, params): Promise<Result>` so component code reads almost identically.

### Backend Python (~2300 lines) -- ~85% reusable

| Module | Reusable? | Changes |
|---|---|---|
| `db.py` | Yes | Unchanged |
| `registry.py` | Yes | Unchanged |
| `pipeline_store.py` | Yes | Unchanged |
| `layout.py` | Yes | Unchanged |
| `api/pipeline.py` | Yes | Strip `@router` decorators; `_build_graph()` is pure logic |
| `api/run.py` | Yes | Replace `push_message()` with stdout notification |
| `api/variables.py` | Yes | Strip FastAPI decorators |
| `api/layout.py` | Yes | Strip FastAPI decorators |
| `api/schema.py`, `api/registry.py` | Yes | Strip FastAPI wrappers |
| `api/ws.py` | **Replaced** | New `notify(method, params)` that writes to stdout (~20 lines) |
| `__main__.py` | **Replaced** | New `server.py` with JSON-RPC stdin/stdout loop |
| `app.py` | **Replaced** | No longer needed |

### New Code Required (~400-600 lines TypeScript)

- `extension.ts`: Extension entry point, command registration (~100 lines)
- `pythonProcess.ts`: Spawn Python, JSON-RPC message routing (~150 lines)
- `dagPanel.ts`: Webview panel creation, HTML generation, message forwarding (~150 lines)
- `sidebarProvider.ts`: Optional TreeView for edit palette (~100-200 lines)

---

## 4. Infrastructure Changes

### Build System

**Webview build (Vite, modified):**
- Output goes to `dist/webview/` inside the extension project
- Must produce a single JS bundle (no code splitting -- Webview CSP restrictions)
- No `index.html` served directly; the extension host constructs the HTML wrapper with CSP nonces and `webview.asWebviewUri()` for asset paths

**Extension host build (esbuild):**
- Compiles TypeScript to a single `dist/extension.js`
- The `yo code` generator scaffolds this

**Python packaging:**
- Require users to `pip install scistack-gui` separately
- Extension discovers the Python interpreter via the VS Code Python extension API
- Alternative: bundle Python files in the `.vsix` directly

### Dependency Changes

**Added:**
- `@types/vscode`, `esbuild`, `@vscode/vsce`, `@vscode/test-electron`

**Removed:**
- `fastapi`, `uvicorn` (Python side)
- Vite proxy configuration (no longer needed)

**Unchanged:**
- `react`, `react-dom`, `@xyflow/react`, `@dagrejs/dagre` (Webview)
- `scidb`, `scihist`, `duckdb`, `jupyter_client`, `ipykernel` (Python)

### Dev Workflow Comparison

| Before | After |
|---|---|
| `cd frontend && npm run dev` | `cd webview && npm run watch` (builds to disk) |
| `scistack-gui my.duckdb --module pipe.py` | Press F5 in VS Code; Python starts automatically |
| Browser at `localhost:8765` | DAG in a VS Code Webview panel |
| Browser DevTools | "Developer: Toggle Developer Tools" in Extension Dev Host |

---

## 5. Difficulty Assessment

### Per-Component Ratings

| Work Item | Difficulty | Effort | Notes |
|---|---|---|---|
| Scaffold extension (`yo code`) | Easy | 1-2 hours | Generator does the work |
| Spawn Python + manage lifecycle | Moderate | 1-2 days | Startup, shutdown, error recovery, interpreter discovery |
| JSON-RPC message router (TS) | Moderate | 1-2 days | ~200 lines. Request/response correlation, notification forwarding |
| Python `server.py` (JSON-RPC stdio) | Moderate | 1-2 days | ~150 lines for stdin/stdout loop + method dispatch |
| Strip FastAPI decorators | Easy | 2-4 hours | Mechanical transformation |
| Replace `push_message` with stdout | Easy | 1-2 hours | Change one function |
| Adapt React for Webview | Moderate | 2-3 days | Replace fetch with postMessage, new hook, CSP-compatible assets |
| Vite config for Webview | Easy-Moderate | 2-4 hours | Single-bundle output |
| Webview panel creation + HTML | Moderate | 1 day | CSP nonces, asset URIs |
| Native sidebar TreeViews | Moderate-Hard | 2-4 days | Optional; TreeView API is verbose |
| Status bar + commands | Easy | 2-4 hours | Simple API |
| Packaging (.vsix) | Easy-Moderate | 0.5-1 day | Configuration and testing |
| End-to-end testing | Moderate | 2-3 days | New test infrastructure |

### Risks

1. **Drag-and-drop TreeView to Webview** -- bridging native drag into a Webview iframe is fragile or unsupported. **Mitigation:** Keep EditTab inside the Webview initially.

2. **Webview CSP** -- VS Code enforces strict Content Security Policy. Inline styles via React work fine, but external fonts or `eval()` would be blocked. Our codebase uses no external resources, so this should be fine.

3. **Python interpreter discovery** -- depends on VS Code Python extension. Must handle: no Python configured, package not installed.

4. **Stdout isolation** -- JSON-RPC protocol messages vs user print statements from `for_each`. The existing `redirect_stdout` in `run.py` already captures user output separately, so this should work but needs careful testing.

### Phasing

**Phase 1 -- MVP (~2 weeks):**
- Extension scaffold with `package.json` manifest
- Python JSON-RPC server
- Extension host spawns Python, routes messages
- Webview with full current UI (EditTab, RunsTab, sidebar all stay in Webview)
- "Open Pipeline" command (prompts for .duckdb + .py module)
- Status bar: DB name

**Phase 2 -- Polish (~1 week):**
- Native TreeView for edit palette
- Output Channel for run logs
- Error handling (Python crash recovery, missing package notifications)
- Settings: default Python interpreter, auto-detect installation

**Phase 3 -- Distribution (~0.5 week):**
- `.vscodeignore`, `vsce package`
- README, marketplace listing
- CI for automated builds

---

## 6. What You Get For Free

| Capability | Current Cost | With VS Code |
|---|---|---|
| Code editing | Would need Monaco integration (weeks) | Free -- already world-class |
| Python debugging | Would need DAP client (weeks) | Free -- Python extension + debugpy |
| Terminal | Not available | Free -- integrated terminal |
| Git integration | Not available | Free -- source control panel |
| File management | Not available | Free -- file explorer |
| Light/dark themes | Hardcoded dark theme | Free -- CSS custom properties (`--vscode-*`) |
| Keybindings | Would need custom implementation | Free -- Command Palette + customizable shortcuts |
| Multi-project support | Not available | Free -- multi-root workspaces |
| Settings per project | Not available | Free -- `.vscode/settings.json` |
| IntelliSense for pipeline .py | Not available | Free -- Python extension |

### Theme Integration Detail

The Webview can use VS Code's CSS custom properties:
```css
background: var(--vscode-editor-background);
color: var(--vscode-editor-foreground);
border: 1px solid var(--vscode-panel-border);
```
This means the DAG canvas automatically matches the user's chosen theme (light, dark, high contrast) without maintaining separate color schemes.

---

## Critical Files for Implementation

Files that would need the most significant changes:

- `scistack_gui/api/ws.py` -- replaced entirely with stdout notifications
- `scistack_gui/__main__.py` -- replaced with `server.py` (JSON-RPC loop)
- `scistack_gui/app.py` -- eliminated (no FastAPI)
- `frontend/src/hooks/useWebSocket.ts` -- replaced with `useExtensionMessage.ts`
- `frontend/src/components/DAG/PipelineDAG.tsx` -- largest concentration of `fetch()` calls to convert
- `frontend/src/App.tsx` -- header elements move to VS Code native UI
