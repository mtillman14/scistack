# MATLAB GUI Support — Implementation Plan

## Context

The SciStack GUI VS Code extension currently supports Python-only pipelines: Python code discovery via `importlib`, Python function execution via `for_each()`, and Python debugging via `debugpy`. Users who write MATLAB pipeline functions (using the `sci-matlab` package) cannot visualize or execute their pipelines from the GUI.

The goal is to add MATLAB support incrementally: first get the DAG displaying MATLAB functions with a copy-paste-to-MATLAB execution model (MVP), then integrate with the MathWorks VS Code extension for in-editor execution and debugging.

**User priorities:** (a) Function execution with debugging essential, (b) Simplicity — get something working reliably even if UX isn't perfect, (c) MVP first (copy-paste), then VS Code integration.

---

## Phase 1: MATLAB Code Discovery & DAG Display

**Goal:** MATLAB functions and variables appear in the DAG alongside Python ones.

### 1a. Configuration — extend `[tool.scistack]` for MATLAB

**File:** `scistack-gui/scistack_gui/config.py`

Add new fields to `SciStackConfig`:
```python
matlab_functions: list[Path]    # .m function files
matlab_variables: list[Path]    # .m classdef files (BaseVariable subclasses)
matlab_addpath: list[Path]      # extra MATLAB path entries
matlab_variable_dir: Path | None  # where create_variable writes .m files
```

Parse from `pyproject.toml`:
```toml
[tool.scistack.matlab]
functions = ["matlab/bandpass_filter.m", "matlab/compute_vo2.m"]
variables = ["matlab/types/*.m"]  # support globs
addpath = ["matlab/lib"]
variable_dir = "matlab/types"
```

Also support standalone `scistack.toml` for MATLAB-only projects (no `pyproject.toml`).

### 1b. Static `.m` file parser

**New file:** `scistack-gui/scistack_gui/matlab_parser.py`

Two parsing functions:
- `parse_matlab_function(path) -> MatlabFunctionInfo | None` — parse first line for `function [out] = name(in1, in2, ...)`, extract function name and parameter names
- `parse_matlab_variable(path) -> str | None` — parse for `classdef Foo < scidb.BaseVariable`, return class name

Data class:
```python
@dataclass
class MatlabFunctionInfo:
    name: str               # function name
    file_path: Path         # absolute .m file path
    params: list[str]       # parameter names
    source_hash: str        # SHA-256 of file contents (for lineage)
    language: str = "matlab"
```

### 1c. MATLAB registry

**New file:** `scistack-gui/scistack_gui/matlab_registry.py`

Module-level state:
```python
_matlab_functions: dict[str, MatlabFunctionInfo] = {}
_matlab_variables: dict[str, Path] = {}  # name -> .m file path
```

Functions:
- `load_from_config(config: SciStackConfig)` — scan configured paths, parse .m files, populate registries
- `refresh_all()` — re-scan all paths
- `get_matlab_function(name) -> MatlabFunctionInfo`
- `is_matlab_function(name) -> bool`
- `get_all_function_names() -> list[str]`
- `get_all_variable_names() -> list[str]`

On load, also call `bridge.register_matlab_variable(name)` for each discovered MATLAB variable to create Python surrogates (needed for DAG graph building from DB history).

### 1d. Integrate into server handlers

**File:** `scistack-gui/scistack_gui/server.py`

Modify `main()`:
- After loading Python registry, also call `matlab_registry.load_from_config(config)` if config has MATLAB paths

Modify handlers:
- `_h_get_registry`: merge MATLAB function/variable names into response, with a `language` field:
  ```python
  {
    "functions": [...],  # Python function names
    "variables": [...],  # all variable names
    "matlab_functions": [...]  # MATLAB function names (or add language tag to each)
  }
  ```
- `_h_get_function_params`: check `matlab_registry` if not found in Python registry, return parsed params
- `_h_get_function_source`: check `matlab_registry`, return `.m` file path and line 1
- `_h_refresh_module`: also call `matlab_registry.refresh_all()`

### 1e. Frontend: distinguish MATLAB nodes

The frontend already renders `functionNode` type — add a `language: "matlab"` data field to MATLAB function nodes so the frontend can style them differently (e.g., different color/icon) and show appropriate context menu options.

**File:** `scistack-gui/scistack_gui/api/pipeline.py` — when building graph nodes, tag MATLAB-sourced functions.

---

## Phase 2: DuckDB File Watcher

**Goal:** DAG auto-refreshes when the database changes from external processes (MATLAB, scripts, notebooks).

**File:** `scistack-gui/extension/src/extension.ts`

After `startPipeline()` successfully starts:
1. Create a `FileSystemWatcher` on the `.duckdb` and `.duckdb.wal` files:
   ```typescript
   const watcher = vscode.workspace.createFileSystemWatcher(
     new vscode.RelativePattern(path.dirname(dbPath), path.basename(dbPath) + '*')
   );
   ```
2. On change, debounce with a 2-second window, then send a `dag_updated` notification to the webview via `dagPanel.postMessage({ method: 'dag_updated', params: {} })`.
3. Dispose watcher on panel close or Python process restart.

This is ~20-30 lines of TypeScript and benefits ALL users (Python too), not just MATLAB.

---

## Phase 3: MVP Execution — "Run in MATLAB" (Copy Command)

**Goal:** User clicks "Run" on a MATLAB function node, gets a ready-to-paste MATLAB command.

### 3a. MATLAB command generation

**New file:** `scistack-gui/scistack_gui/api/matlab_command.py`

Function: `generate_matlab_command(function_name, db_path, schema_keys, variants, schema_filter, ...) -> str`

Generates a complete, self-contained MATLAB script:
```matlab
%% SciStack: Run bandpass_filter
% Generated by SciStack GUI — paste into MATLAB Command Window

% Configure database (skip if already configured)
addpath('/path/to/user/matlab/lib');
db = scihist.configure_database('/path/to/experiment.duckdb', ["subject", "session"]);

% Register variable types
scidb.register_variable('RawSignal');
scidb.register_variable('FilteredSignal');

% Run
scihist.for_each(@bandpass_filter, ...
    struct('raw_signal', RawSignal(), 'cutoff_freq', 10), ...
    {FilteredSignal()}, ...
    subject=[1 2 3], session=["A" "B"]);
```

The command builder reuses the same variant/input resolution logic from `run.py` — it resolves inputs/outputs/constants from DAG edges and DB history, then formats as MATLAB syntax instead of calling Python `for_each`.

Helper functions:
- `_format_matlab_value(val)` — format Python values as MATLAB literals
- `_format_matlab_struct(inputs_dict)` — build `struct(...)` syntax
- `_format_matlab_cell(outputs)` — build `{Type1(), Type2()}` syntax
- `_format_schema_kwargs(kwargs)` — build `key=[val1, val2], ...` syntax

### 3b. New server handler

**File:** `scistack-gui/scistack_gui/server.py`

New handler `_h_generate_matlab_command`:
```python
def _h_generate_matlab_command(params):
    from scistack_gui.api.matlab_command import generate_matlab_command
    from scistack_gui.db import get_db, get_db_path
    db = get_db()
    return {
        "command": generate_matlab_command(
            function_name=params["function_name"],
            db_path=str(get_db_path()),
            schema_keys=db.dataset_schema_keys,
            variants=params.get("variants", []),
            schema_filter=params.get("schema_filter"),
            schema_level=params.get("schema_level"),
        )
    }
```

Register in `METHODS` dict.

### 3c. Extension: copy-to-clipboard action

**File:** `scistack-gui/extension/src/dagPanel.ts`

New host-side method `run_matlab` (or intercept `start_run` for MATLAB functions):

1. When the webview sends `start_run` for a MATLAB function:
   - Call Python `generate_matlab_command` RPC
   - Copy result to clipboard via `vscode.env.clipboard.writeText(command)`
   - Show info notification: "MATLAB command copied to clipboard. Paste into MATLAB to run."
   - Optionally open a read-only document with the command for review

2. The webview's "Run" button for MATLAB functions changes label to "Run in MATLAB" (or shows a different icon).

---

## Phase 4: MATLAB Variable Creation

**Goal:** "Create Variable" dialog works for MATLAB projects.

**File:** `scistack-gui/scistack_gui/server.py` — modify `_h_create_variable`

When the project has MATLAB config (`matlab_variable_dir` is set):
1. Write a `.m` classdef file:
   ```matlab
   classdef RawSignal < scidb.BaseVariable
       % Documentation string here
   end
   ```
2. Call `bridge.register_matlab_variable(name)` to create the Python surrogate
3. Call `matlab_registry.refresh_all()` to pick up the new file
4. Notify `dag_updated`

Detection: check `registry._config` for MATLAB config, or add a param to the create_variable request indicating the target language.

---

## Phase 5: MathWorks Terminal Integration (Post-MVP)

**Goal:** Execute MATLAB functions directly in VS Code via the MathWorks extension's terminal.

### 5a. Detect MathWorks extension

**File:** `scistack-gui/extension/src/extension.ts`

```typescript
function isMatlabExtensionAvailable(): boolean {
  return vscode.extensions.getExtension('MathWorks.language-matlab') !== undefined;
}
```

### 5b. New `matlabTerminal.ts` module

**New file:** `scistack-gui/extension/src/matlabTerminal.ts`

```typescript
export async function runInMatlabTerminal(command: string): Promise<void> {
  // 1. Open MATLAB command window
  await vscode.commands.executeCommand('matlab.openCommandWindow');

  // 2. Find the MATLAB terminal
  const terminal = vscode.window.terminals.find(t => t.name === 'MATLAB');
  if (!terminal) throw new Error('MATLAB terminal not found');

  // 3. Send command
  terminal.sendText(command);
  terminal.show();
}
```

### 5c. Execution flow

**File:** `scistack-gui/extension/src/dagPanel.ts`

When `start_run` arrives for a MATLAB function:
1. Generate command via Python RPC (same as Phase 3)
2. If MathWorks extension available -> call `runInMatlabTerminal(command)`
3. Else -> fall back to clipboard copy (Phase 3)
4. Show notification: "Running in MATLAB terminal..."
5. DuckDB file watcher (Phase 2) handles DAG refresh when execution completes

**Limitations to document:**
- No structured progress (terminal.sendText is fire-and-forget)
- No "run_done" signal — rely on file watcher
- Can't distinguish our runs from manual commands in MATLAB terminal

---

## Phase 6: MATLAB Debugging in VS Code (Post-MVP)

**Goal:** Breakpoints in `.m` files work when running from the GUI.

This tier works almost "for free" with the MathWorks terminal approach:
1. User sets breakpoints in `.m` files using VS Code's standard breakpoint UI
2. Our extension sends `scihist.for_each(...)` to the MATLAB terminal (Phase 5)
3. When MATLAB hits the breakpoint, the MathWorks extension's debug adapter activates
4. User sees call stack, variables, stepping controls in VS Code
5. After continuing past all breakpoints, execution completes -> DuckDB watcher refreshes DAG

**No additional code needed** beyond Phase 5, because the MathWorks extension handles debugging for any code run in its terminal. However, we should:
- Document this workflow
- Ensure the generated command doesn't use `-batch` mode (which disables breakpoints)
- Consider adding a "Debug in MATLAB" vs "Run in MATLAB" distinction (debugging shows terminal, run could hide it)

---

## Files Modified / Created

### New files:
| File | Phase | Purpose |
|------|-------|---------|
| `scistack_gui/matlab_parser.py` | 1 | Static .m file parsing |
| `scistack_gui/matlab_registry.py` | 1 | MATLAB function/variable registry |
| `scistack_gui/api/matlab_command.py` | 3 | MATLAB command string generation |
| `extension/src/matlabTerminal.ts` | 5 | MathWorks terminal integration |

### Modified files:
| File | Phase | Changes |
|------|-------|---------|
| `scistack_gui/config.py` | 1 | Add MATLAB config fields |
| `scistack_gui/server.py` | 1,3 | MATLAB registry init, new handlers |
| `scistack_gui/api/pipeline.py` | 1 | Tag MATLAB nodes with language |
| `extension/src/extension.ts` | 2,5 | File watcher, MathWorks detection |
| `extension/src/dagPanel.ts` | 3,5 | MATLAB run/copy actions |

### Existing code reused (no changes):
| File | Usage |
|------|-------|
| `sci-matlab/src/sci_matlab/bridge.py` | `register_matlab_variable()` for Python surrogates |
| `scistack_gui/api/run.py` | Variant resolution logic (referenced, not modified) |

---

## Verification Plan

### Phase 1 Testing:
- Create a test project with `.m` function files and variable classdefs
- Configure `[tool.scistack.matlab]` in pyproject.toml
- Start the extension -> verify MATLAB functions/variables appear in DAG
- Verify `get_function_source` opens `.m` files
- Verify `get_function_params` returns correct MATLAB function params

### Phase 2 Testing:
- Open a pipeline in the GUI
- From an external process, write to the .duckdb file
- Verify DAG refreshes within ~2 seconds
- Verify rapid writes don't cause excessive refreshes (debounce works)

### Phase 3 Testing:
- Click "Run in MATLAB" on a MATLAB function node
- Verify clipboard contains valid, runnable MATLAB command
- Paste into MATLAB, verify it runs successfully
- Verify DAG refreshes after MATLAB execution completes

### Phase 5 Testing:
- Install MathWorks VS Code extension
- Click "Run in MATLAB" -> verify command appears in MATLAB terminal
- Verify execution runs in terminal
- Verify DAG refreshes after completion
- Test fallback when MathWorks extension is not installed

### Phase 6 Testing:
- Set breakpoint in a `.m` function file
- Run via GUI -> verify breakpoint is hit
- Step through, inspect variables
- Continue -> verify DAG refreshes
