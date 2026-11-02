# MATLAB Support in the SciStack VS Code Extension — Implementation Reference

This document describes the implementation of MATLAB support in the SciStack GUI VS Code extension. It covers configuration, code discovery, DAG integration, execution, variable creation, and debugging. For the pre-implementation analysis that motivated these design decisions, see `matlab-gui-support-analysis.md`.

---

## Table of Contents

1. [Configuration](#1-configuration)
2. [MATLAB .m File Parser](#2-matlab-m-file-parser)
3. [MATLAB Registry](#3-matlab-registry)
4. [DAG Integration](#4-dag-integration)
5. [DuckDB File Watcher](#5-duckdb-file-watcher)
6. [MATLAB Command Generation](#6-matlab-command-generation)
7. [Execution Flow](#7-execution-flow)
8. [MATLAB Variable Creation](#8-matlab-variable-creation)
9. [MathWorks Terminal Integration](#9-mathworks-terminal-integration)
10. [Debugging](#10-debugging)
11. [File Inventory](#11-file-inventory)

---

## 1. Configuration

### pyproject.toml (mixed Python + MATLAB projects)

MATLAB settings live under `[tool.scistack.matlab]`:

```toml
[tool.scistack]
modules = ["pipeline.py"]
entities_file = "src/scistack_entities.toml"

[tool.scistack.matlab]
functions = ["matlab/bandpass_filter.m", "matlab/compute_vo2.m"]
variables = ["matlab/types/*.m"]   # glob patterns supported
addpath = ["matlab/lib"]
variable_dir = "matlab/types"
```

| Key | Type | Description |
|-----|------|-------------|
| `functions` | list of paths | `.m` function files to register (supports globs) |
| `variables` | list of paths | `.m` classdef files extending `scidb.BaseVariable` (supports globs) |
| `addpath` | list of paths | Directories added to MATLAB path in generated commands |
| `variable_dir` | path | Where `create_variable` writes new `.m` classdef files |

### scistack.toml (MATLAB-only projects)

Projects with no Python code can use a standalone `scistack.toml` instead of `pyproject.toml`. The schema is the same but without the `[tool.scistack]` wrapper — the entire file IS the scistack section:

```toml
# scistack.toml — equivalent to [tool.scistack] in pyproject.toml
modules = []

[matlab]
functions = ["bandpass_filter.m", "compute_vo2.m"]
variables = ["types/*.m"]
addpath = ["lib"]
variable_dir = "types"
```

When searching for a config file, the system checks for `pyproject.toml` first, then falls back to `scistack.toml`. Both are searched upward from the database file's directory.

### Config class

`SciStackConfig` (in `scistack_gui/project/config.py`) carries the parsed MATLAB fields:

```python
@dataclass
class SciStackConfig:
    # ... existing fields ...
    matlab_functions: list[Path]       # resolved absolute paths
    matlab_variables: list[Path]       # resolved absolute paths
    matlab_addpath: list[Path]         # resolved absolute paths
    matlab_variable_dir: Path | None   # where create_variable writes .m files
```

### Glob resolution

Paths in `functions` and `variables` support glob patterns (`*`, `?`, `[...]`). The helper `_resolve_glob_paths()` expands them relative to the project root:

```toml
variables = ["matlab/types/*.m"]   # matches all .m files in matlab/types/
```

Non-glob paths that don't exist emit a warning but are still included (the file may be created later).

---

## 2. MATLAB .m File Parser

**File:** `scistack_gui/matlab/parser.py`

Static parser that extracts function signatures and classdef declarations from `.m` files without running MATLAB. No MATLAB license or installation required.

### Function parsing

`parse_matlab_function(path) -> MatlabFunctionInfo | None`

Handles all standard MATLAB function declaration forms:

```matlab
% Multiple outputs:
function [filtered, residual] = bandpass_filter(signal, low_hz, high_hz)

% Single output:
function result = compute_vo2(breath_data)

% No output:
function plot_results(data, title_str)

% No parameters:
function setup()
```

Returns a `MatlabFunctionInfo` dataclass:

```python
@dataclass
class MatlabFunctionInfo:
    name: str           # "bandpass_filter"
    file_path: Path     # absolute path to the .m file
    params: list[str]   # ["signal", "low_hz", "high_hz"]
    source_hash: str    # SHA-256 of file contents (for lineage)
    language: str       # always "matlab"
```

Returns `None` if the file can't be read or doesn't contain a valid function declaration (e.g., it's a script).

### Variable classdef parsing

`parse_matlab_variable(path) -> str | None`

Looks for `classdef ClassName < *.BaseVariable`:

```matlab
classdef RawSignal < scidb.BaseVariable
    % Raw EMG signal data
end
```

Returns `"RawSignal"` (the class name), or `None` if the file isn't a `BaseVariable` subclass. Accepts any parent ending in `BaseVariable` (e.g., `scidb.BaseVariable`, `mylib.BaseVariable`).

---

## 3. MATLAB Registry

**File:** `scistack_gui/matlab/registry.py`

Mirrors `scistack_gui/project/registry.py` for Python code. Module-level state tracks discovered MATLAB functions and variables.

### Loading

```python
from scistack_gui.matlab import registry as matlab_registry

# Called once at startup if MATLAB config is present:
result = matlab_registry.load_from_config(config)
# result = {"matlab_functions": ["bandpass_filter", ...], "matlab_variables": ["RawSignal", ...]}
```

During loading, each discovered MATLAB variable automatically gets a **Python surrogate class** created via `sci_matlab.bridge.register_matlab_variable()`. This ensures the type appears in `BaseVariable._all_subclasses` and can participate in DAG graph building (which is driven by DB history that references these type names).

### Lookup API

```python
matlab_registry.is_matlab_function("bandpass_filter")   # True
matlab_registry.get_matlab_function("bandpass_filter")   # MatlabFunctionInfo(...)
matlab_registry.get_all_function_names()                 # ["bandpass_filter", "compute_vo2"]
matlab_registry.get_all_variable_names()                 # ["FilteredSignal", "RawSignal"]
matlab_registry.has_matlab_config()                      # True if any MATLAB paths configured
```

### Refresh

`matlab_registry.refresh_all()` re-scans all configured `.m` file paths. Called by the `refresh_module` JSON-RPC handler (alongside the Python registry refresh) and after MATLAB variable creation.

---

## 4. DAG Integration

### Function nodes tagged with language

In `api/pipeline.py`, function nodes sourced from the MATLAB registry get an extra `language` field in their data:

```python
# pipeline.py:_build_graph() output for a MATLAB function node:
{
    "id": "fn__bandpass_filter",
    "type": "functionNode",
    "data": {
        "label": "bandpass_filter",
        "language": "matlab",           # <-- new field
        "input_params": {"signal": "RawSignal", "low_hz": ""},
        "output_types": ["FilteredSignal"],
        "run_state": "red",
        ...
    }
}
```

Python function nodes don't have this field (or it's absent), so the frontend can distinguish them for styling (different color/icon) and behavior (different run action).

### Parameter resolution fallback

`_fn_params_from_registry()` — used when filling in unknown function parameters for DAG display — falls back to the MATLAB registry when a function isn't found in the Python registry:

```python
def _fn_params_from_registry(fn_name: str) -> list[str]:
    fn = registry._functions.get(fn_name)
    if fn is not None:
        # Python path: use inspect.signature
        ...
    # MATLAB fallback: use parsed .m file params
    from scistack_gui.matlab import registry as matlab_registry
    if matlab_registry.is_matlab_function(fn_name):
        return list(matlab_registry.get_matlab_function(fn_name).params)
    return []
```

### Server handler updates

| Handler | MATLAB change |
|---------|--------------|
| `get_registry` | Returns `matlab_functions` list alongside `functions` and `variables` |
| `get_function_params` | Checks MATLAB registry first, returns `.m` file params |
| `get_function_source` | Returns `.m` file path and line 1 for MATLAB functions |
| `refresh_module` | Also calls `matlab_registry.refresh_all()` |

---

## 5. DuckDB File Watcher

**File:** `extension/src/extension.ts` — `setupDbWatcher()`

Watches the `.duckdb` and `.duckdb.wal` files for changes from external processes (MATLAB, scripts, notebooks). Benefits **all** users, not just MATLAB.

```typescript
// Watch pattern matches both experiment.duckdb and experiment.duckdb.wal
const pattern = new vscode.RelativePattern(dbDir, dbBase + '*');
dbWatcher = vscode.workspace.createFileSystemWatcher(pattern);
```

### Debouncing

Rapid writes during a long `for_each` don't flood the UI. Changes are debounced with a 2-second window:

```
MATLAB writes row 1   → timer starts (2s)
MATLAB writes row 2   → timer resets (2s)
MATLAB writes row 3   → timer resets (2s)
...silence for 2s...   → dag_updated notification fires → DAG refreshes
```

### Lifecycle

- Created after `startPipeline()` succeeds
- Disposed on `deactivate()` or when a new pipeline is opened (creates a fresh watcher)
- The debounce timer is also cleared on dispose

---

## 6. MATLAB Command Generation

**File:** `scistack_gui/services/matlab_command_format.py`

Generates complete, self-contained MATLAB scripts ready to paste into a MATLAB command window.

### Example output

For a function `bandpass_filter` with one input (`RawSignal`), one output (`FilteredSignal`), and a constant (`low_hz = 20`):

```matlab
%% SciStack: Run bandpass_filter
% Generated by SciStack GUI — paste into MATLAB Command Window

addpath('/home/user/matlab/lib');

% Configure database (skip if already configured)
db = scihist.configure_database('/data/experiment.duckdb', ["subject", "session"]);

% Register variable types
scidb.register_variable('FilteredSignal');
scidb.register_variable('RawSignal');

% Run
scihist.for_each(@bandpass_filter, ...
    struct('signal', RawSignal(), 'low_hz', 20), ...
    {FilteredSignal()}, ...
    'subject', [1 2 3], 'session', ["pre" "post"]);
```

### What it includes

1. **addpath** entries from `[tool.scistack.matlab] addpath`
2. **configure_database** with the exact DB path and schema keys
3. **register_variable** for every variable type referenced in the function's variants
4. **for_each** call with:
   - Function handle (`@bandpass_filter`)
   - Inputs as a MATLAB `struct(...)` (variable types instantiated, constants as literals)
   - Outputs as a MATLAB cell array (`{FilteredSignal()}`)
   - Schema filter kwargs (when the user selected specific subjects/sessions in the GUI)

### Template mode

When no variant history exists (function was just added, never run), a template is generated:

```matlab
%% SciStack: Run my_new_function
% Generated by SciStack GUI — paste into MATLAB Command Window

% Configure database (skip if already configured)
db = scihist.configure_database('/data/experiment.duckdb', ["subject", "session"]);

% Run (fill in inputs/outputs)
scihist.for_each(@my_new_function, ...
    struct(), ...
    {});
```

### Variant deduplication

If a function has multiple output types with the same constants (common when a function produces multiple outputs), the command generator deduplicates to one `for_each` call per unique constants combination.

### JSON-RPC handler

The `generate_matlab_command` handler is registered in the method dispatch table:

```python
# server.py
def _h_generate_matlab_command(params):
    # params: {"function_name": "bandpass_filter", "schema_filter": {...}, ...}
    # Returns: {"command": "% SciStack: Run bandpass_filter\n..."}
```

---

## 7. Execution Flow

### End-to-end sequence

```
User clicks "Run" on a MATLAB function node
  ↓
Webview sends: {method: "start_run", params: {function_name: "bandpass_filter", language: "matlab"}}
  ↓
DagPanel.handleMatlabRun() intercepts (language === "matlab")
  ↓
Python RPC: generate_matlab_command → returns MATLAB script string
  ↓
Extension checks: MathWorks extension installed?
  ├── YES → runInMatlabTerminal(command)
  │         → matlab.openCommandWindow
  │         → terminal.sendText(command)
  │         → "Running in MATLAB terminal..."
  └── NO  → vscode.env.clipboard.writeText(command)
            → "MATLAB command copied to clipboard. Paste into MATLAB to run."
  ↓
User runs in MATLAB → for_each writes to DuckDB
  ↓
FileSystemWatcher detects .duckdb change (debounced 2s)
  ↓
dag_updated notification → Webview re-fetches pipeline → DAG refreshes
```

### How `start_run` dispatches by language

In `dagPanel.ts`, the `start_run` handler checks for the `language` field:

```typescript
if (method === 'start_run') {
  const params = (msg.params ?? {}) as Record<string, unknown>;
  const language = params.language as string | undefined;
  if (language === 'matlab') {
    await this.handleMatlabRun(msg.id as number, params);
    return;
  }
  // Python function — existing path with debugpy auto-attach
  await this.ensureDebugAttached();
}
```

The frontend is responsible for passing `language: "matlab"` when the function node has `data.language === "matlab"`.

---

## 8. MATLAB Variable Creation

The `create_variable` JSON-RPC handler now accepts an optional `language` parameter:

```json
{"method": "create_variable", "params": {"name": "StepLength", "language": "matlab"}}
```

### Python variable (default, unchanged)

Appends `class StepLength(BaseVariable): pass` to the configured `.py` variable file.

### MATLAB variable (language: "matlab")

1. Checks that `matlab_variable_dir` is configured
2. Creates `{matlab_variable_dir}/StepLength.m`:

```matlab
classdef StepLength < scidb.BaseVariable
    % Optional docstring here
end
```

3. Calls `bridge.register_matlab_variable("StepLength")` to create the Python surrogate
4. Calls `matlab_registry.refresh_all()` to pick up the new file
5. Sends `dag_updated` notification

### Error handling

- Returns error if `matlab_variable_dir` is not configured
- Returns error if the `.m` file already exists
- Returns error if the name fails Python identifier validation (since surrogate class names must be valid Python identifiers too)

---

## 9. MathWorks Terminal Integration

**File:** `extension/src/matlabTerminal.ts`

### Detection

```typescript
import { isMatlabExtensionAvailable } from './matlabTerminal';

if (isMatlabExtensionAvailable()) {
  // Extension ID: 'MathWorks.language-matlab'
}
```

### Sending commands

```typescript
import { runInMatlabTerminal } from './matlabTerminal';

const sent = await runInMatlabTerminal(command, outputChannel);
if (!sent) {
  // Fallback to clipboard
}
```

The function:
1. Returns `false` immediately if the MathWorks extension isn't installed
2. Executes `matlab.openCommandWindow` to ensure the MATLAB terminal exists
3. Finds the terminal named `"MATLAB"` in `vscode.window.terminals`
4. Calls `terminal.sendText(command)` and `terminal.show()`
5. Returns `true` on success, `false` on any failure (with logging)

### Limitations

- **Fire-and-forget**: `terminal.sendText()` has no return value or completion callback
- **No structured progress**: VS Code's terminal API doesn't expose stdout reading
- **No "run_done" signal**: The DuckDB file watcher is the only way to detect completion
- **Shared terminal**: Can't distinguish SciStack runs from manual MATLAB commands

---

## 10. Debugging

### Python functions (unchanged)

1. `scistack.debug = true` in VS Code settings
2. Extension sets `SCISTACK_GUI_DEBUG=1` env var
3. Python server starts `debugpy` listener
4. `ensureDebugAttached()` auto-attaches before runs
5. Breakpoints in `.py` files hit normally
6. Auto-detach on `run_done`

### MATLAB functions via MathWorks terminal

Debugging MATLAB functions works "for free" with the MathWorks terminal approach:

1. User sets breakpoints in `.m` files using VS Code's standard breakpoint UI
2. User clicks "Run" on a MATLAB function in the DAG
3. Extension sends `scihist.for_each(...)` to the MATLAB terminal
4. When MATLAB hits the breakpoint, the MathWorks extension's debug adapter activates
5. User sees call stack, variables, stepping controls in VS Code
6. After continuing past all breakpoints, execution completes
7. DuckDB watcher refreshes the DAG

**Important**: The generated command does NOT use `-batch` mode (which disables breakpoints and interactive debugging). The command is sent to an interactive MATLAB session.

### MATLAB functions without MathWorks extension

Falls back to clipboard copy. User can:
1. Open MATLAB
2. Set breakpoints using MATLAB's own `dbstop` or IDE breakpoints
3. Paste the generated command
4. Debug interactively in MATLAB

---

## 11. File Inventory

### New files

| File | Lines | Purpose |
|------|-------|---------|
| `scistack_gui/matlab/parser.py` | ~115 | Static `.m` file parser |
| `scistack_gui/matlab/registry.py` | ~140 | MATLAB function/variable registry |
| `scistack_gui/services/matlab_command_format.py` | ~190 | MATLAB command string generation |
| `extension/src/matlabTerminal.ts` | ~55 | MathWorks extension detection + terminal integration |
| `tests/test_matlab.py` | ~270 | Tests for parser, command generation, config |

### Modified files

| File | Key changes |
|------|-------------|
| `scistack_gui/project/config.py` | 4 new `SciStackConfig` fields, `scistack.toml` support, `_resolve_glob_paths()`, `_extract_scistack_section()` |
| `scistack_gui/server.py` | MATLAB registry init in `main()`, 5 updated handlers, 2 new handlers (`generate_matlab_command`, MATLAB `create_variable`) |
| `scistack_gui/api/pipeline.py` | `language: "matlab"` tag on function nodes, MATLAB fallback in `_fn_params_from_registry()` |
| `extension/src/extension.ts` | DuckDB `FileSystemWatcher` with 2s debounce, cleanup in `deactivate()` |
| `extension/src/dagPanel.ts` | MATLAB run interception, `handleMatlabRun()`, import of `matlabTerminal` |
