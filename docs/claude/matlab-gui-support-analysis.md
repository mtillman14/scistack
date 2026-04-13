# MATLAB Support in the SciStack VS Code Extension

Analysis of what's involved in supporting MATLAB code in the SciStack GUI VS Code extension, which currently supports Python only.

---

## Table of Contents

1. [Current Architecture (Python Only)](#1-current-architecture-python-only)
2. [What MATLAB Support Requires](#2-what-matlab-support-requires)
3. [Function Execution: The Core Challenge](#3-function-execution-the-core-challenge)
4. [Execution Options](#4-execution-options)
5. [Debugging Analysis](#5-debugging-analysis)
6. [DAG Refresh for Out-of-Process Execution](#6-dag-refresh-for-out-of-process-execution)
7. [Recommended Architecture](#7-recommended-architecture)
8. [Summary of Trade-offs](#8-summary-of-trade-offs)

---

## 1. Current Architecture (Python Only)

```
VS Code Extension (TypeScript)
   ├── extension.ts          — Entry point, spawns Python child process
   ├── pythonProcess.ts      — JSON-RPC client over stdin/stdout
   └── dagPanel.ts           — Webview panel, auto-attaches debugpy for breakpoints
         │
         │  JSON-RPC (stdin/stdout)
         ▼
Python Backend (scistack_gui/server.py)
   ├── config.py             — Parses [tool.scistack] from pyproject.toml
   ├── registry.py           — Discovers Functions, Variables, Constants via importlib
   ├── api/run.py            — _run_in_thread() → for_each(fn, inputs, outputs)
   └── api/pipeline.py       — Builds DAG graph from DB + manual edges
         │
         ▼
     DuckDB (.duckdb file)
```

**Key points:**
- Extension spawns `python -m scistack_gui.server --db <path> --project <path>`
- Python discovers code by importing `.py` files (`importlib.util.spec_from_file_location`)
- Execution: `registry.get_function(name)` returns a Python callable → `for_each(fn, ...)`
- Debugging: Python server starts `debugpy` listener; extension auto-attaches VS Code debugger before runs
- All communication is JSON-RPC over the Python process's stdin/stdout

---

## 2. What MATLAB Support Requires

### 2.1 Code Discovery

The registry (`registry.py`) discovers code by calling `importlib` to import `.py` files and scanning for `BaseVariable` subclasses, `LineageFcn` instances, and callables. MATLAB `.m` files can't be imported this way.

**MATLAB Variables** are `.m` classdef files:
```matlab
classdef RawSignal < scidb.BaseVariable
end
```

**MATLAB Functions** are `.m` function files like `bandpass_filter.m`.

Two approaches:
- **Static parsing**: Regex/text-parse `.m` files for `classdef Foo < scidb.BaseVariable` and function declarations. Lightweight, no MATLAB license needed, but can't validate code.
- **MATLAB Engine API** (`matlab.engine`): Start a MATLAB process from Python and introspect live. Accurate but requires MATLAB license and is heavy.

### 2.2 Configuration

Currently everything lives in `pyproject.toml` → `[tool.scistack]`. Options:
- Add `matlab_modules` or `matlab_path` fields alongside `modules`
- MATLAB projects may not have a `pyproject.toml`, so a standalone `scistack.toml` or auto-detection of `.m` files may be needed

### 2.3 Variable Creation

`_h_create_variable` in `server.py` appends Python `class Foo(BaseVariable): pass` to a `.py` file. For MATLAB, it would write a new `.m` classdef file:
```matlab
classdef Foo < scidb.BaseVariable
end
```

### 2.4 Extension UI

`extension.ts` offers three choices (project/single-file/none), all Python-centric. Would need:
- A new option like "Select MATLAB source folder"
- File picker filters for `.m` files
- New CLI arguments (`--matlab-path`)

### 2.5 Function Source / Refresh

- `get_function_source` uses Python `inspect`. For MATLAB, track `.m` file paths directly.
- Refresh: re-scan `.m` files on disk rather than re-importing Python modules.

### 2.6 Function Execution

This is the hardest part. See section 3.

---

## 3. Function Execution: The Core Challenge

### Current call directions

**Python GUI flow** (what works today):
```
Extension (TS) → Python server (JSON-RPC)
                 → _run_in_thread()
                   → registry.get_function(name)   # lookup Python callable
                   → for_each(fn, inputs, outputs)  # Python orchestrates everything
```

**MATLAB standalone flow** (outside the GUI):
```
MATLAB user code
  → scihist.for_each(@my_fn, inputs, outputs, ...)
    → scidb.for_each(...)         # MATLAB orchestrates iteration (~1700 lines)
      → py.sci_matlab.bridge.load_and_extract()   # Python does DB I/O
      → feval(fn, args{:})        # MATLAB executes the user function
      → py.sci_matlab.bridge.for_each_batch_save() # Python does DB I/O
```

**The asymmetry:** The bridge in `sci_matlab/bridge.py` is one-directional — MATLAB calls *into* Python for DB operations, but Python never calls into MATLAB. The GUI's `_run_in_thread` (in `run.py:413`) needs a Python callable:

```python
for_each(fn, inputs=inputs, outputs=[OutputCls], ...)
```

For MATLAB functions, `fn` doesn't exist in Python.

---

## 4. Execution Options

### Option A: `matlab.engine` (Python → MATLAB Engine API)

Python starts an embedded MATLAB instance and calls functions through it.

| Aspect | Details |
|--------|---------|
| **How** | `matlab.engine.start_matlab()` in Python; call MATLAB functions through the engine |
| **Pros** | MATLAB handles its own data marshaling and `for_each` orchestration |
| **Cons** | Requires MATLAB license; 10-30s engine startup; ~1-2 GB RAM; not pip-installable; finicky API |
| **Verdict** | **Avoid.** Worst developer experience of all options. |

### Option B: MATLAB Sidecar Process (persistent)

Run MATLAB as a second child process alongside Python, communicating via JSON-RPC.

| Aspect | Details |
|--------|---------|
| **How** | Extension spawns `matlab -batch "scistack_matlab_server(port)"`; MATLAB listens for JSON-RPC commands; uses existing `scihist.for_each` to execute |
| **Pros** | Clean separation; reuses MATLAB `for_each` infrastructure; persistent process avoids cold starts; can stream progress |
| **Cons** | Need to write a MATLAB JSON-RPC server (~200-400 lines); manage two child processes; requires MATLAB license; debugging is hard (see section 5) |
| **Verdict** | Best architecture if we need full control and structured progress, but debugging support is limited. |

### Option C: `matlab -batch` Subprocess Per Run

Spawn a fresh MATLAB subprocess for each execution.

| Aspect | Details |
|--------|---------|
| **How** | `matlab -batch "scihist.for_each(@fn, ...)"` per run |
| **Pros** | Simplest implementation; reuses existing MATLAB `for_each` completely |
| **Cons** | **30-60s cold start per run** (MATLAB startup + path setup + DB config); no state sharing; fragile command string construction |
| **Verdict** | Fine for a prototype, impractical for real use. |

### Option D: View-Only for MATLAB

GUI shows the DAG but doesn't support execution. Users run from MATLAB directly.

| Aspect | Details |
|--------|---------|
| **How** | DAG built from DB history + static `.m` file parsing; no "Run" button for MATLAB functions |
| **Pros** | Zero execution complexity; no MATLAB license requirement for GUI; most GUI value is visualization |
| **Cons** | Users lose "click to run"; no progress streaming |
| **Verdict** | Gets 80% of value for 20% of work. Good fallback. |

---

## 5. Debugging Analysis

### How Python debugging works today

1. User enables `scistack.debug = true` in VS Code settings
2. Extension spawns Python with `SCISTACK_GUI_DEBUG=1` env var
3. Python server starts a `debugpy` listener on port 5678 (`server.py:36-47`)
4. When user clicks "Run", `ensureDebugAttached()` in `dagPanel.ts:142-174` auto-attaches VS Code's debugger via DAP
5. Breakpoints in `.py` user functions get hit — user can inspect variables, step through, etc.
6. On `run_done`, the debug session auto-detaches

The key enabler is **`debugpy`** — a lightweight library that turns any Python process into a DAP-compatible debug server. VS Code attaches over a TCP socket, and breakpoints "just work."

### Why MATLAB debugging is fundamentally different

MATLAB has no equivalent of `debugpy`. There's no lightweight library you can drop into a running MATLAB process to expose DAP over a socket. MATLAB's debugging options:

1. **Built-in debugger** (`dbstop`, `dbcont`): Only works inside an interactive MATLAB session. If a sidecar hits `dbstop`, it blocks with no VS Code UI.
2. **MathWorks VS Code Extension** (R2023b+): Added DAP support via debug adapter type `"matlab"`. But designed for the workflow where the MathWorks extension owns the MATLAB process.
3. **MATLAB Engine API**: Can start MATLAB but doesn't expose debugging.

### Debugging tiers

#### Tier 1: "Debug in MATLAB" button (easy, works with any MATLAB version)

When user right-clicks a MATLAB function node:
- Generate the full `scihist.for_each(...)` call with inputs/outputs/schema keys pre-filled
- Copy to clipboard or open MATLAB with the command ready
- User sets breakpoints in MATLAB's own debugger and runs there

**Trade-off:** Zero integration effort, works on all MATLAB versions. But execution is outside VS Code — no DAG refresh unless combined with DuckDB file-watching (see section 6).

#### Tier 2: Coordinate with MathWorks VS Code extension (medium difficulty)

Use the MATLAB terminal managed by the MathWorks extension to execute code. Since it's the same MATLAB instance that handles breakpoints, debugging "just works."

**Integration path — `terminal.sendText()`:**

The MathWorks extension exposes no public API (`activate()` returns `Promise<void>`), but VS Code's terminal API lets any extension send text to any open terminal:

1. Open the MATLAB terminal: `vscode.commands.executeCommand('matlab.openCommandWindow')`
2. Find it: `vscode.window.terminals.find(t => t.name === "MATLAB")`
3. Send command: `terminal.sendText('scihist.for_each(@bandpass_filter, ...)')`

**How debugging works with Tier 2:**
1. User sets breakpoints in `.m` files in VS Code (standard breakpoint UI)
2. Our extension sends the `scihist.for_each(...)` command to the MATLAB terminal
3. MATLAB hits the breakpoint → VS Code debug UI activates (call stack, variables, stepping)
4. User steps through, inspects, continues
5. Execution finishes → results saved to DuckDB → file watcher triggers DAG refresh

**Limitations:**
- **No structured progress feedback.** `terminal.sendText()` is fire-and-forget; VS Code's terminal API intentionally doesn't expose stdout reading (https://github.com/microsoft/vscode/issues/59384). MATLAB's `for_each` prints progress to the terminal, but our GUI won't see it.
- **DAG refresh is indirect.** Relies on DuckDB file watcher (section 6) — no clean "run_done" event.
- **Can't differentiate "our" runs from manual commands.** The terminal is shared.
- **Command construction is fragile.** Must construct valid MATLAB syntax from the GUI's internal representation, with correct quoting/escaping.
- **Hard dependency on MathWorks extension.** If user doesn't have it installed, integration breaks.

#### Tier 3: Start MATLAB sidecar with DAP (hard, R2023b+ only)

When spawning our own MATLAB sidecar, start it with flags that enable DAP. Attach VS Code's debugger.

**Status:** Underexplored. MATLAB's DAP server is designed for the MathWorks extension workflow. Unknown whether it can be started in a headless sidecar or coexist with our JSON-RPC communication.

---

## 6. DAG Refresh for Out-of-Process Execution

When MATLAB execution happens outside the Python backend (Tier 1 or Tier 2), the GUI has no direct notification that the database changed.

### DuckDB File Watcher + Debounce (recommended)

Use `vscode.workspace.createFileSystemWatcher()` on the `.duckdb` file. When MATLAB writes to it, the extension sends a `dag_updated` notification to the webview.

**Implementation notes:**
- Watch both `*.duckdb` and `*.duckdb.wal` (DuckDB uses write-ahead logging, so the main file's mtime may not change on every write)
- Debounce: during a long `for_each`, there may be rapid-fire writes; use a 1-2 second debounce window
- On change: Python backend re-queries the DB and sends updated pipeline graph to the webview
- This also benefits any scenario where the database is modified externally (scripts, notebooks, etc.)

**Estimated effort:** Small — a handful of lines in `extension.ts`.

---

## 7. Recommended Architecture

**Hybrid: Tier 2 execution + Tier 1 file-watch refresh + Option D fallback**

```
VS Code Extension (TypeScript)
   ├── extension.ts
   │   ├── Python process (existing) — DAG, registry, layout, DB queries
   │   ├── MathWorks terminal integration — execution via sendText()
   │   └── DuckDB file watcher — triggers DAG refresh on external writes
   ├── pythonProcess.ts (existing)
   └── dagPanel.ts
         │
         │  JSON-RPC (existing, for DAG/registry/layout)
         ▼
Python Backend (existing, unchanged for MATLAB execution)
   ├── MATLAB .m file parser (new) — static discovery of Variables/Functions
   ├── config.py (extended) — new matlab_modules / matlab_path fields
   └── registry.py (extended) — register MATLAB-discovered items
```

### Execution flow for MATLAB functions:

1. User clicks "Run" on a MATLAB function node in the DAG
2. Extension constructs the `scihist.for_each(...)` command string
3. Extension calls `vscode.commands.executeCommand('matlab.openCommandWindow')`
4. Extension finds the MATLAB terminal and calls `terminal.sendText(command)`
5. MATLAB executes in its terminal (with breakpoint support)
6. DuckDB file watcher detects database change
7. Extension sends `dag_updated` → webview re-fetches pipeline graph
8. Node colors/states update

### Graceful degradation:

- **MathWorks extension installed:** Full Tier 2 experience (execute + debug)
- **MathWorks extension NOT installed:** Fall back to Option D (view-only) + Tier 1 ("Copy command to clipboard")
- **No MATLAB at all:** View-only DAG built from DB history (the DB was populated by earlier MATLAB runs)

---

## 8. Summary of Trade-offs

| Capability | Python (today) | MATLAB (recommended) |
|------------|----------------|----------------------|
| Code discovery | `importlib` | Static `.m` file parsing |
| DAG visualization | Full | Full (from DB history + static parse) |
| Click-to-run | In-process, structured progress | Via MathWorks terminal, progress in terminal only |
| Breakpoint debugging | `debugpy` auto-attach | Native via MathWorks extension's debug adapter |
| Progress in GUI | Real-time structured events | Not available (terminal only) |
| DAG auto-refresh | Immediate (`run_done` event) | Delayed ~1-2s (DuckDB file watcher) |
| Variable creation | Append to `.py` file | Write new `.m` classdef file |
| Dependencies | Python + scistack | MATLAB + MathWorks VS Code extension (recommended) |

### Key references:

- [MathWorks MATLAB Extension GitHub](https://github.com/mathworks/MATLAB-extension-for-vscode)
- [MathWorks MATLAB Extension package.json](https://github.com/mathworks/MATLAB-extension-for-vscode/blob/main/package.json) — 13 commands, debug adapter type `"matlab"`, no public API
- [VS Code Terminal API](https://code.visualstudio.com/api/references/vscode-api) — `terminal.sendText()` for cross-extension communication
- [VS Code issue #59384](https://github.com/microsoft/vscode/issues/59384) — terminal stdout not exposed (why structured progress isn't possible via terminal)
- `scistack-gui/extension/src/dagPanel.ts:142-174` — current Python debug auto-attach implementation
- `scistack-gui/scistack_gui/api/run.py:55-429` — current Python execution flow
- `sci-matlab/src/sci_matlab/bridge.py` — MATLAB→Python bridge (one-directional)
- `sci-matlab/src/sci_matlab/matlab/+scidb/for_each.m` — MATLAB-side `for_each` (~1700 lines)
