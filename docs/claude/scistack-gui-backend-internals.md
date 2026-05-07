# SciStack-GUI Backend Internals

## Overview

The scistack-gui Python backend is a dual-protocol server that provides:
1. **FastAPI HTTP + WebSocket** (standalone GUI mode)
2. **JSON-RPC 2.0 over stdin/stdout** (VS Code extension mode)

Both protocols share the same service layer, allowing the GUI to run either as a standalone web application or integrated into VS Code.

**Key responsibilities:**
- Build pipeline DAG from database + manual nodes
- Execute scihist.for_each() in background threads
- Stream stdout to frontend in real-time
- Manage node positions, edges, and pending constants
- Hot-reload user code without server restart
- Coordinate database access with MATLAB

---

## Architecture Layers

```
┌─────────────────────────────────────────────────────┐
│  Protocol Adapters (FastAPI routes, JSON-RPC)      │
├─────────────────────────────────────────────────────┤
│  Service Layer (shared business logic)             │
├─────────────────────────────────────────────────────┤
│  Domain Logic (pure, testable graph/state logic)   │
├─────────────────────────────────────────────────────┤
│  Persistence (DuckDB + JSON layout files)          │
└─────────────────────────────────────────────────────┘
```

### Design Principles

1. **Service layer abstraction**: Both protocols call the same service methods (no code duplication)
2. **Pure domain logic**: `domain/` modules have zero I/O dependencies (easy to test)
3. **Singleton database**: One `DatabaseManager` per process, reference-counted connections
4. **Per-call-site state**: Function nodes keyed by `(fn_name, call_id)` for lineage tracking
5. **DuckDB-backed persistence**: Manual nodes/edges in `_pipeline_*` tables, positions in JSON
6. **Background threading**: Long-running for_each calls execute in worker threads

---

## Entry Points

### FastAPI Mode (Standalone GUI)

**Command**: `scistack-gui experiment.duckdb`

**Entry file**: `scistack_gui/__main__.py`

**Flow**:
1. Parse CLI args: `--db`, `--module`/`--project`, `--port`, `--no-browser`
2. Import user code → populates `BaseVariable._all_subclasses` via metaclass
3. Load project config (if `--project`) → `config.py` parses `[tool.scistack]`
4. Load function registry → `registry.py` discovers functions
5. Initialize database → `db.py` creates singleton `DatabaseManager`
6. Bridge Python logging → `scidb.log`
7. Check lockfile → `startup.py` validates/cleans stale locks
8. Start uvicorn → `app.py` creates FastAPI app on `localhost:{port}`
9. Open browser → `webbrowser.open()`

**Key modules loaded**:
- `registry.py`: Function discovery from user code
- `config.py`: `pyproject.toml` parsing
- `db.py`: Database singleton initialization
- `app.py`: FastAPI application factory

---

### JSON-RPC Mode (VS Code Extension)

**Command**: Called by VS Code extension (extension/extension.ts)

**Entry file**: `scistack_gui/server.py`

**Flow**:
1. Read CLI args from stdin (JSON-RPC initialize request)
2. Send `progress` notifications during startup phases 1-9
3. Import user code → same registry/config flow as FastAPI mode
4. Initialize database → same `DatabaseManager` singleton
5. Release DB connection → `close_initial_connection()` for MATLAB access
6. Send `ready` notification → extension opens webview panel
7. Enter stdin read loop → process JSON-RPC requests/responses
8. Write responses to stdout → newline-delimited JSON

**Protocol differences**:
- No HTTP server (stdin/stdout instead)
- No WebSocket (uses `notify.py` for push messages)
- Thread-safe stdout with mutex (`_lock`)
- Supports debugpy attachment for breakpoints

**Method dispatch table** (`server.py` lines 120-140):
```python
METHODS = {
    "get_pipeline": pipeline_service.get_pipeline_graph,
    "get_info": pipeline_service.get_info,
    "start_run": run_service.start_run_json_rpc,
    "cancel_run": run_service.cancel_run,
    "force_cancel_run": run_service.force_cancel_run,
    # ... (28 methods total)
}
```

---

## Database Connection Management (db.py)

### DatabaseManager Singleton

**Purpose**: Single shared database instance with reference-counted connections.

**Why reference counting?** MATLAB needs exclusive access to the database between GUI requests. The backend acquires the connection for each request, then releases it to allow MATLAB to run.

**Key methods**:

| Method | Purpose |
|--------|---------|
| `init_db(path)` | Open existing database, verify `_schema` table |
| `create_db(path, schema_keys)` | Create new database with schema |
| `acquire_db_connection()` | Increment refcount, reopen if closed |
| `release_db_connection()` | Decrement refcount, close if idle |
| `close_initial_connection()` | Release lock after startup (JSON-RPC mode) |

**Connection lifecycle**:
```python
# Per-request pattern
db = acquire_db_connection()
try:
    # Use db
    results = db.list_variants(...)
finally:
    release_db_connection()
```

**Refcounting logic** (lines 45-75):
- `_refcount = 0` initially
- `acquire_db_connection()`: `_refcount += 1`, reopen if `_db is None`
- `release_db_connection()`: `_refcount -= 1`, close if `_refcount == 0`
- Thread-safe with `_lock: threading.Lock()`

**Why this design?**
- GUI needs database during request handling
- MATLAB needs database between requests (exclusive write access)
- Closing after each request allows MATLAB to acquire the lock

---

## Code Registry (registry.py)

### Purpose

Discover and store user-defined functions and variable classes.

### Three Loading Modes

| Mode | CLI Flag | Source |
|------|----------|--------|
| **Single-file** | `--module pipeline.py` | Legacy, single Python file |
| **Project** | `--project pyproject.toml` | Recommended, loads from `[tool.scistack]` |
| **Hybrid** | `--project` + explicit files | Project + additional `.py` files |

### Project Mode Discovery (`config.py`)

**Config format** (`pyproject.toml`):
```toml
[tool.scistack]
files = ["pipeline/**/*.py"]  # Glob patterns
packages = ["my_lib"]          # Installed pip packages
plugins = true                 # Entry-point discovery
matlab.enabled = true
matlab.src_dir = "matlab_code"
```

**Discovery process** (`config.py` lines 30-100):
1. Parse `[tool.scistack]` section
2. Resolve glob patterns relative to project root
3. Find installed packages in sys.modules
4. Discover entry-point plugins (if enabled)
5. Return `SciStackConfig` dataclass with absolute paths

**MATLAB config** (`config.py` lines 150-180):
- `[tool.scistack.matlab]` section
- `enabled`, `src_dir`, `exclude_patterns`
- Used by `matlab_registry.py` to discover MATLAB functions

### Function Registry

**Storage**: `_functions: dict[str, Callable]` (module-level global)

**Population** (`registry.py` lines 200-250):
1. Import user modules → `importlib.import_module()`
2. Scan module namespace → `inspect.getmembers(module)`
3. Filter callables → `inspect.isfunction()` or `inspect.isclass()`
4. Store in `_functions[name] = callable`

**Special handling**:
- Decorated functions (scilineage.LineageFcn) → unwrap to get original
- MATLAB functions → separate registry in `matlab_registry.py`
- Duplicate names → last-imported wins (no error)

### Variable Class Discovery

**Automatic via metaclass** (`scidb.BaseVariable`):
```python
class BaseVariable(metaclass=BaseVariableMeta):
    # All subclasses auto-register in:
    # BaseVariable._all_subclasses = {}
```

**Discovery** (`registry.py` lines 300-320):
1. User imports define classes: `class EMGData(BaseVariable): ...`
2. Metaclass `__init__` adds to `_all_subclasses`
3. No explicit registration needed

**Why this works**: User modules imported during startup → classes defined → metaclass populates registry.

### Hot Reload

**Methods**:
- `refresh_module(module_path)` — reload single module
- `refresh_all()` — reload all discovered modules

**Process** (`registry.py` lines 400-450):
1. Clear `_functions` dict
2. `importlib.reload()` each module
3. Re-scan for callables
4. Notify frontend: `dag_updated` message

**Limitations**:
- Doesn't reload imports (only top-level user modules)
- Class redefinition creates new types (incompatible with old instances)
- Best for adding/modifying functions, not changing variable schemas

---

## Pipeline Persistence

### Two Storage Systems

| System | File/Table | Purpose | Example |
|--------|------------|---------|---------|
| **Structure** | DuckDB `_pipeline_*` | Nodes, edges, constants | Manual function node, user-drawn edge |
| **Positions** | `experiment.layout.json` | x/y coordinates | Node at (250, 100) |

**Why separate?** Structural changes trigger DAG rebuild; position changes don't. Keeping positions in JSON avoids DB writes on every drag operation.

### Structure Tables (`pipeline_store.py`)

**Tables**:
```sql
CREATE TABLE _pipeline_nodes (
    node_id TEXT PRIMARY KEY,
    node_type TEXT,         -- 'function', 'variable', 'constant', 'pathInput'
    label TEXT,             -- Function name or variable type
    config TEXT             -- JSON: {inputs, outputs, constants, etc.}
);

CREATE TABLE _pipeline_edges (
    edge_id TEXT PRIMARY KEY,
    source TEXT,            -- Source node_id
    target TEXT,            -- Target node_id
    source_handle TEXT,     -- MATLAB multi-output: 'output_1', 'output_2'
    target_handle TEXT      -- Input parameter name
);

CREATE TABLE _pipeline_pending_constants (
    node_id TEXT,
    param_name TEXT,
    value TEXT,             -- JSON-encoded constant value
    PRIMARY KEY (node_id, param_name)
);

CREATE TABLE _pipeline_hidden_nodes (
    node_id TEXT PRIMARY KEY,
    hidden_at TIMESTAMP
);
```

**Key operations** (`pipeline_store.py` lines 100-400):
- `write_manual_node()` — insert/update `_pipeline_nodes`
- `delete_manual_node()` — remove from `_pipeline_nodes`, add to `_pipeline_hidden_nodes`
- `write_edge()` — insert `_pipeline_edges`
- `delete_edge()` — remove from `_pipeline_edges`
- `set_pending_constant()` — upsert `_pipeline_pending_constants`
- `clear_pending_constant()` — delete constant after execution

**Hidden nodes**: When user deletes a DB-derived node (e.g., a variable that exists in the database), it's added to `_pipeline_hidden_nodes` to prevent automatic recreation on next graph build.

### Position Storage (`layout.py`)

**File**: `experiment.layout.json` (same directory as `experiment.duckdb`)

**Format**:
```json
{
  "nodes": {
    "fn:process_emg:abc123": {"x": 250, "y": 100},
    "var:EMGData:456def": {"x": 50, "y": 100}
  }
}
```

**Operations** (`layout.py` lines 50-150):
- `write_node_position(node_id, x, y)` — update JSON file
- `read_node_positions()` — load all positions
- `delete_node_position(node_id)` — remove from JSON

**Atomicity**: Write to temp file, then `os.replace()` for atomic update.

**Migration**: Legacy format (structured hierarchy) auto-converted to flat dict on first access.

---

## Pipeline Graph Building (domain/graph_builder.py)

### Overview

The graph builder merges three sources:
1. **DB-derived nodes**: Variables and for_each call sites in the database
2. **Manual nodes**: User-placed function/constant/pathInput nodes
3. **Inferred edges**: Connections based on for_each call metadata

### Build Process

**Entry point**: `api/pipeline.py` → `_build_graph(db)`

**Steps**:

#### 1. Fetch Data from Database (lines 50-100)

```python
# Get all variants (for_each outputs)
variants = db.list_pipeline_variants()
# [{'fn_name': 'process_emg', 'call_id': 'abc123', 'inputs': {...}, ...}, ...]

# Get all variable types and their records
variables = db.list_variable_types()
# [{'var_type': 'EMGData', 'record_count': 42, ...}, ...]
```

#### 2. Aggregate Variants (lines 120-200)

**Purpose**: Merge multiple variants of the same `(fn_name, call_id)` into one node.

**Logic** (`aggregate_variants()` in `domain/variant_resolver.py`):
- Group by `FnKey = (fn_name, call_id)`
- Collect all input/output specs across variants
- Merge constant values (deduplicate)
- Track variant count

**Result**: `AggregatedData` dataclass
```python
@dataclass
class AggregatedData:
    fn_name: str
    call_id: str
    inputs: dict[str, set[str]]      # param → {var_types}
    outputs: list[str]                # [var_type1, var_type2]
    constants: dict[str, set[Any]]    # param → {values}
    variant_count: int
```

#### 3. Build Nodes (lines 250-450)

Three node builders:

**a) Variable Nodes** (`build_variable_nodes()`)
```python
{
    'id': 'var:EMGData',
    'type': 'variable',
    'data': {
        'label': 'EMGData',
        'recordCount': 42,
        'schemaKeys': ['subject', 'session'],
        'dataColumns': ['emg', 'time']
    },
    'position': {'x': 50, 'y': 100}  # from layout.json
}
```

**b) Function Nodes** (`build_function_nodes()`)
```python
{
    'id': 'fn:process_emg:abc123',
    'type': 'function',
    'data': {
        'label': 'process_emg',
        'callId': 'abc123',
        'inputs': {'signal': ['EMGData'], 'threshold': ['constant']},
        'outputs': ['ProcessedEMG'],
        'constants': {'threshold': [0.5, 1.0]},
        'variantCount': 2,
        'state': 'green'  # computed by run_state.py
    },
    'position': {'x': 250, 'y': 100}
}
```

**c) Manual Nodes** (`merge_manual_nodes()`)

Loaded from `_pipeline_nodes` table:
- Function nodes: user-placed, not yet executed
- Constant nodes: user-defined constant values
- PathInput nodes: filesystem-based data discovery

**Merge logic** (lines 500-550):
- Manual function nodes replace DB-derived (if same `fn_name + call_id`)
- Manual constant/pathInput nodes added to graph
- Hidden nodes (in `_pipeline_hidden_nodes`) excluded

#### 4. Build Edges (lines 600-750)

Two edge sources:

**a) Inferred from for_each metadata**
```python
# For each function node's inputs:
for param, var_types in fn_node['inputs'].items():
    if var_types[0] != 'constant':
        edge = {
            'id': f"{var_node['id']}-{fn_node['id']}",
            'source': var_node['id'],
            'target': fn_node['id'],
            'targetHandle': param
        }
```

**b) Manual edges from `_pipeline_edges` table**

**MATLAB multi-output handling** (lines 700-750):
- MATLAB functions can return multiple outputs
- Each output gets a `sourceHandle`: `'output_1'`, `'output_2'`
- Frontend uses handles to draw connections from specific outputs

**Example edge**:
```python
{
    'id': 'edge_123',
    'source': 'fn:matlab_process',
    'sourceHandle': 'output_2',      # Second MATLAB output
    'target': 'fn:downstream',
    'targetHandle': 'signal'          # Parameter name
}
```

#### 5. Compute Run States (lines 800-850)

**Entry**: `domain/run_state.py` → `propagate_run_states(nodes, edges, db)`

**States**:
- **Green**: All dependencies executed, outputs exist in DB
- **Grey**: Dependencies OK, but node not executed yet
- **Red**: Missing dependencies or pending constants

**Algorithm** (pure DAG traversal):
1. Mark all variable nodes as green (data source)
2. For each function node:
   - If any input edge's source is grey/red → mark grey
   - If node has pending constants → mark grey
   - If all inputs green but no outputs in DB → mark grey
   - If outputs exist in DB → mark green
   - If any required input missing → mark red

**Per-call-site state** (lines 50-100):
- Same function reused in pipeline → each call site gets independent state
- Keyed by `FnKey = (fn_name, call_id)`

**Multiple producers** (lines 150-200):
- If a variable has multiple functions producing it, take worst state
- Example: `EMGData` produced by `load_data` (green) and `simulate_data` (grey) → `EMGData` node is grey

---

## Function Execution (api/run.py, services/run_service.py)

### Run Request Flow

**Frontend request**:
```json
POST /api/run
{
  "functionName": "process_emg",
  "callId": "abc123",
  "constants": {"threshold": 0.75},
  "where": "subject == 1",
  "outputs": ["ProcessedEMG"]
}
```

**Backend processing**:

#### Step 1: Validate Request (lines 50-100)

- Check function exists in registry
- Validate output variable types exist
- Parse `where` filter (SQL string)

#### Step 2: Resolve Variants (lines 150-250)

**Entry**: `domain/variant_resolver.py` → `filter_variants()`

**Process**:
1. Get all variants for `(fn_name, call_id)` from DB
2. Filter by schema keys from `where` clause
3. Merge with pending constants from `_pipeline_pending_constants`
4. Generate cross-product if multiple constant values exist

**Example**:
```python
# DB has: threshold=[0.5, 1.0] (2 variants)
# User provides: threshold=0.75
# Result: Override with 0.75, 1 variant

# DB has: subject=[1, 2] (2 variants)
# User provides: where="subject == 1"
# Result: Filter to subject=1, 1 variant
```

#### Step 3: Spawn Worker Thread (lines 300-400)

**Why threading?** for_each() can run for minutes/hours. Background thread allows:
- Non-blocking API response
- Stdout streaming during execution
- Cooperative cancellation

**Thread creation** (`_run_in_thread()`):
```python
run_id = str(uuid.uuid4())
cancel_event = threading.Event()
thread = threading.Thread(
    target=_execute_run,
    args=(run_id, fn, inputs, outputs, metadata_iterables, cancel_event)
)
_active_runs[run_id] = {
    "event": cancel_event,
    "thread": thread,
    "cancelled": False,
    "force_cancelled": False
}
thread.start()
return run_id  # Return immediately
```

#### Step 4: Execute for_each (lines 450-550)

**Context**: Worker thread

**Process**:
1. Acquire database connection (hold for duration)
2. Redirect stdout → custom stream that pushes to WebSocket/JSON-RPC
3. Call `scihist.for_each(fn, inputs, outputs, ...)`
4. Clear pending constants on success
5. Release database connection
6. Send `run_done` notification

**Stdout streaming** (lines 500-520):
```python
class StreamToWebSocket(io.StringIO):
    def write(self, text):
        for line in text.splitlines():
            push_message({
                "type": "run_output",
                "runId": run_id,
                "line": line
            })

with redirect_stdout(StreamToWebSocket()):
    scihist.for_each(...)
```

**Error handling** (lines 580-620):
- Exceptions caught and sent as `run_done` with error message
- Traceback logged to server console
- Database connection always released (finally block)

#### Step 5: Notify Completion (lines 650-700)

**Success**:
```json
{
  "type": "run_done",
  "runId": "abc-123",
  "success": true
}
```

**Failure**:
```json
{
  "type": "run_done",
  "runId": "abc-123",
  "success": false,
  "error": "ValueError: threshold must be > 0"
}
```

**Frontend action**: Refresh graph to show new green states.

### Run Cancellation

**Two modes**:

| Method | Signal | Force | Use Case |
|--------|--------|-------|----------|
| `cancel_run()` | `Event.set()` | No | User clicks "Cancel" |
| `force_cancel_run()` | Kill thread | Yes | Hung process |

**Cooperative cancellation** (`cancel_run()`):
- Sets `cancel_event` → scifor checks via `_cancel_check` callback
- Clean shutdown: database connection released, partial results saved
- Preferred method

**Force cancellation** (`force_cancel_run()`):
- Not implemented (Python doesn't support forced thread termination)
- Placeholder for future improvement (subprocess-based execution)

---

## Edge Type Resolution (domain/edge_resolver.py)

### Purpose

Infer parameter types from manually-drawn edges when user hasn't executed the function yet.

### Problem

User drags edge from `EMGData` → `process_emg.signal`. We need to tell the backend:
```python
# Before execution, we know:
inputs = {"signal": ???}

# After edge drawn, we infer:
inputs = {"signal": EMGData}
```

### Algorithm (lines 50-150)

**Input**: `manual_edges`, `variable_nodes`, `function_nodes`

**Process**:
1. Build reverse edge map: `param_name → source_node_id`
2. For each function node's input parameter:
   - Check if manual edge exists for this parameter
   - Follow edge to source node
   - If source is variable node → use `var_type`
   - If source is function node → check function's output signature
   - If source is constant node → skip (handled separately)
3. Update function node's `inputs` dict

**MATLAB multi-output handling** (lines 100-130):
- Manual edge has `sourceHandle = 'output_2'`
- Parse output index from handle
- Look up MATLAB function's output signature: `outputs = ['out1', 'out2', 'out3']`
- Resolve `outputs[1]` → variable type

**Example**:
```python
# MATLAB function: [a, b, c] = matlab_func(...)
# Frontend draws edge from output_2 (b) → downstream.input
# Edge: {source: 'fn:matlab_func', sourceHandle: 'output_2', target: 'fn:downstream', targetHandle: 'input'}
# Resolution: outputs[1] = 'ProcessedData' → inputs['input'] = ProcessedData
```

---

## Variant Resolution (domain/variant_resolver.py)

### Responsibilities

1. Filter variants by schema keys
2. Merge constants from pending table
3. Deduplicate identical variants
4. Generate cross-product for multi-valued constants

### Filtering (lines 50-150)

**Input**: `where` filter (SQL string)

**Process**:
1. Parse `where` clause: `"subject == 1 AND session == 'A'"`
2. Extract schema key filters: `{'subject': '1', 'session': 'A'}`
3. Filter variants: keep only those matching all conditions
4. Return filtered list

**Example**:
```python
# DB variants:
[
    {'subject': '1', 'session': 'A', 'threshold': 0.5},
    {'subject': '1', 'session': 'B', 'threshold': 0.5},
    {'subject': '2', 'session': 'A', 'threshold': 0.5}
]

# Filter: "subject == 1"
# Result:
[
    {'subject': '1', 'session': 'A', 'threshold': 0.5},
    {'subject': '1', 'session': 'B', 'threshold': 0.5}
]
```

### Constant Merging (lines 200-300)

**Input**: DB variants + pending constants from `_pipeline_pending_constants`

**Process**:
1. Load pending constants: `{'threshold': 0.75}`
2. For each variant:
   - Override constant values with pending values
   - Keep schema keys unchanged
3. Deduplicate: remove identical variants

**Example**:
```python
# DB variants (2 threshold values):
[
    {'subject': '1', 'threshold': 0.5},
    {'subject': '1', 'threshold': 1.0}
]

# Pending constant: {'threshold': 0.75}
# After merge:
[
    {'subject': '1', 'threshold': 0.75}  # Only 1 variant now
]
```

### Cross-Product Generation (lines 350-450)

**When**: Multiple values for a constant parameter, no pending override.

**Process**:
1. Identify constants with multiple values: `{'threshold': [0.5, 1.0]}`
2. Generate Cartesian product with schema keys
3. Expand variants

**Example**:
```python
# Input:
schema_keys = {'subject': ['1', '2']}
constants = {'threshold': [0.5, 1.0]}

# Output (2 subjects × 2 thresholds = 4 variants):
[
    {'subject': '1', 'threshold': 0.5},
    {'subject': '1', 'threshold': 1.0},
    {'subject': '2', 'threshold': 0.5},
    {'subject': '2', 'threshold': 1.0}
]
```

---

## WebSocket & JSON-RPC Notification System

### Dual Protocol Support

| Feature | FastAPI Mode | JSON-RPC Mode |
|---------|--------------|---------------|
| Protocol | WebSocket | stdout stream |
| Connection | `/ws` endpoint | stdin/stdout |
| Message queue | `ws.py` | `notify.py` |
| Concurrency | Async (FastAPI) | Mutex-locked stdout |

### Message Types

#### 1. DAG Updated
```json
{
  "type": "dag_updated"
}
```
**Trigger**: Manual node/edge change, function execution completion, hot reload
**Action**: Frontend refetches `/api/pipeline`

#### 2. Run Output
```json
{
  "type": "run_output",
  "runId": "abc-123",
  "line": "[info] Processing subject=1, session=A"
}
```
**Trigger**: Stdout during for_each execution
**Action**: Frontend appends to run log panel

#### 3. Run Done
```json
{
  "type": "run_done",
  "runId": "abc-123",
  "success": true,
  "error": null
}
```
**Trigger**: for_each completion or error
**Action**: Frontend shows success/failure state, refreshes graph

#### 4. Progress (JSON-RPC only)
```json
{
  "method": "progress",
  "params": {
    "phase": 3,
    "message": "Loading user code..."
  }
}
```
**Trigger**: Server startup phases 1-9
**Action**: Extension shows progress in status bar

#### 5. Ready (JSON-RPC only)
```json
{
  "method": "ready"
}
```
**Trigger**: Server initialization complete
**Action**: Extension opens webview panel

### WebSocket Implementation (api/ws.py)

**Connection handling** (lines 30-80):
```python
_active_connections: list[WebSocket] = []

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    _active_connections.append(websocket)
    try:
        while True:
            # Keep connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        _active_connections.remove(websocket)
```

**Broadcasting** (lines 100-130):
```python
def push_message(message: dict):
    """Send message to all connected WebSocket clients."""
    for ws in _active_connections:
        asyncio.create_task(ws.send_json(message))
```

**Thread safety**: Called from worker threads → uses `asyncio.create_task()` to schedule on event loop.

### JSON-RPC Implementation (notify.py)

**Notification sending** (lines 20-60):
```python
import sys
import json
import threading

_lock = threading.Lock()

def notify(method: str, params: dict):
    """Send JSON-RPC notification to stdout."""
    with _lock:
        msg = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        }
        print(json.dumps(msg), flush=True)
```

**Thread safety**: Multiple worker threads can call `notify()` concurrently → mutex ensures atomic writes.

---

## MATLAB Integration

### Discovery (matlab_registry.py)

**Process**:
1. Load config: `[tool.scistack.matlab]` from `pyproject.toml`
2. Scan `src_dir` for `.m` files (lines 50-100)
3. Parse function signatures via regex (lines 120-180)
4. Build registry: `{'func_name': {'outputs': [...], 'params': [...]}}`

**Function signature parsing** (lines 150-180):
```matlab
function [out1, out2] = my_func(param1, param2)
```
↓
```python
{
    'name': 'my_func',
    'outputs': ['out1', 'out2'],
    'params': ['param1', 'param2']
}
```

### Command Generation (services/matlab_command_service.py)

**Purpose**: Generate MATLAB code to execute a function with specific inputs/outputs.

**Example** (lines 100-200):
```python
# Input:
fn_name = "process_signal"
inputs = {"signal": "EMGData", "threshold": 0.5}
outputs = ["ProcessedEMG"]
metadata = {"subject": "1", "session": "A"}

# Output (MATLAB code):
"""
signal = EMGData.load(subject="1", session="A");
[out1] = process_signal(signal, 0.5);
ProcessedEMG.save(out1, subject="1", session="A");
"""
```

**Variable loading** (lines 150-200):
- Python variable classes have MATLAB equivalents
- `EMGData.load(...)` → calls Python `EMGData.load()` via MATLAB Python engine
- Schema keys passed as name-value pairs

**Multi-output handling** (lines 250-300):
```matlab
[out1, out2, out3] = matlab_func(inputs...);
VarType1.save(out1, ...);
VarType2.save(out2, ...);
VarType3.save(out3, ...);
```

### Execution Flow

1. **Frontend**: User clicks "Run" on MATLAB node
2. **Backend**: `GET /api/matlab_command` → generate MATLAB code
3. **Frontend**: Receives code, displays in modal
4. **User**: Copies code, pastes into MATLAB console
5. **MATLAB**: Executes → calls Python `load()`/`save()` via engine
6. **Database**: MATLAB writes outputs via Python API
7. **Frontend**: User clicks "Refresh" → graph updates with new data

**Why not auto-execute?** MATLAB engine integration is complex and platform-dependent. Manual copy-paste is simple and reliable.

---

## Project Configuration (config.py)

### pyproject.toml Format

```toml
[tool.scistack]
# Python sources (at least one required)
files = ["pipeline/**/*.py", "analysis.py"]
packages = ["my_analysis_lib"]
plugins = true  # Enable entry-point discovery

# MATLAB (optional)
[tool.scistack.matlab]
enabled = true
src_dir = "matlab_code"
exclude_patterns = ["**/test_*.m", "**/*_old.m"]
```

### Parsing Logic (lines 30-150)

#### Step 1: Load TOML
```python
with open(project_path, "rb") as f:
    data = tomllib.load(f)
    config_data = data.get("tool", {}).get("scistack", {})
```

#### Step 2: Resolve Files (lines 80-120)
```python
project_root = project_path.parent
file_patterns = config_data.get("files", [])
resolved_files = []

for pattern in file_patterns:
    # Expand glob patterns
    matches = project_root.glob(pattern)
    resolved_files.extend(matches)
```

#### Step 3: Resolve Packages (lines 130-160)
```python
packages = config_data.get("packages", [])
for pkg in packages:
    # Verify package is installed
    if pkg not in sys.modules:
        raise ValueError(f"Package {pkg} not found")
```

#### Step 4: Discover Plugins (lines 170-200)
```python
if config_data.get("plugins", False):
    # Use entry_points() to find plugins
    for ep in entry_points(group="scistack.plugins"):
        plugin_files.append(ep.load())
```

#### Step 5: Parse MATLAB Config (lines 220-280)
```python
matlab_config = config_data.get("matlab", {})
if matlab_config.get("enabled", False):
    matlab_src = project_root / matlab_config["src_dir"]
    exclude = matlab_config.get("exclude_patterns", [])
    # Return MATLABConfig dataclass
```

### Configuration Dataclasses

```python
@dataclass
class SciStackConfig:
    files: list[Path]          # Absolute paths to .py files
    packages: list[str]        # Package names
    matlab: MATLABConfig | None

@dataclass
class MATLABConfig:
    src_dir: Path              # Absolute path
    exclude_patterns: list[str]
```

---

## Startup & Lockfile Management (startup.py)

### Purpose

Prevent multiple GUI instances from opening the same database simultaneously.

### Lockfile Format

**File**: `experiment.duckdb.lock`

**Content**:
```json
{
  "pid": 12345,
  "timestamp": "2024-01-15T10:30:00",
  "mode": "fastapi"
}
```

### Startup Sequence (lines 50-200)

#### Phase 1: Check for Existing Lock
```python
lock_path = db_path.with_suffix(db_path.suffix + ".lock")
if lock_path.exists():
    # Read lock file
    with open(lock_path) as f:
        lock_data = json.load(f)

    # Check if process still running
    if _is_process_alive(lock_data["pid"]):
        raise RuntimeError(f"Database locked by PID {lock_data['pid']}")
```

#### Phase 2: Validate Stale Lock (lines 100-150)
```python
def _is_process_alive(pid: int) -> bool:
    """Check if process is still running."""
    try:
        os.kill(pid, 0)  # Signal 0 = check existence
        return True
    except OSError:
        return False
```

#### Phase 3: Clean Stale Lock (lines 160-180)
```python
# Process not running → remove stale lock
if not _is_process_alive(lock_data["pid"]):
    lock_path.unlink()
    print(f"[warn] Removed stale lock from PID {lock_data['pid']}")
```

#### Phase 4: Create New Lock (lines 190-220)
```python
lock_data = {
    "pid": os.getpid(),
    "timestamp": datetime.now().isoformat(),
    "mode": "fastapi"  # or "jsonrpc"
}
with open(lock_path, "w") as f:
    json.dump(lock_data, f)
```

#### Phase 5: Register Cleanup (lines 230-260)
```python
import atexit

def _cleanup_lock():
    if lock_path.exists():
        lock_path.unlink()

atexit.register(_cleanup_lock)
```

### Concurrent Access Protection

**Problem**: MATLAB needs database between GUI requests.

**Solution**: Connection refcounting in `db.py`:
1. GUI acquires → refcount = 1
2. GUI request completes → refcount = 0 → connection closed
3. MATLAB acquires → exclusive access
4. MATLAB releases → GUI can reopen

**Lockfile role**: Prevents *simultaneous GUI instances*, not GUI-MATLAB contention.

---

## Key Data Structures

### FnKey (domain/graph_builder.py)
```python
FnKey = tuple[str, str]  # (fn_name, call_id)
```
**Purpose**: Unique identifier for a for_each call site. Same function reused in pipeline gets different `call_id` values.

### AggregatedData (domain/graph_builder.py)
```python
@dataclass
class AggregatedData:
    fn_name: str
    call_id: str
    inputs: dict[str, set[str]]      # param → {var_types}
    outputs: list[str]                # [var_type1, ...]
    constants: dict[str, set[Any]]    # param → {values}
    variant_count: int
    has_pending_constants: bool
```
**Purpose**: Aggregated view of all variants for one call site.

### Node Structure (React Flow)

**Variable Node**:
```python
{
    "id": "var:EMGData",
    "type": "variable",
    "data": {
        "label": "EMGData",
        "recordCount": 42,
        "schemaKeys": ["subject", "session"],
        "dataColumns": ["emg", "time"]
    },
    "position": {"x": 50, "y": 100}
}
```

**Function Node**:
```python
{
    "id": "fn:process_emg:abc123",
    "type": "function",
    "data": {
        "label": "process_emg",
        "callId": "abc123",
        "inputs": {"signal": ["EMGData"]},
        "outputs": ["ProcessedEMG"],
        "constants": {"threshold": [0.5]},
        "state": "green"
    },
    "position": {"x": 250, "y": 100}
}
```

---

## Common Workflows

### 1. Initial Graph Load

```
Browser → GET /api/pipeline
  → pipeline_service.get_pipeline_graph(db)
    → Fetch variants from DB
    → Fetch variables from DB
    → Build nodes + edges
    → Load manual nodes from _pipeline_nodes
    → Load positions from layout.json
    → Compute run states
    → Return {nodes, edges}
```

### 2. Manual Node Creation

```
Browser → PUT /api/layout/{node_id}
  → layout_service.put_layout(node_id, x, y, node_type, label)
    → Write position to layout.json
    → Write structure to _pipeline_nodes
    → Unhide canonical DB nodes (if variable type)
    → Notify: dag_updated
```

### 3. Function Execution

```
Browser → POST /api/run
  → run_service.start_run(fn_name, call_id, constants, where, outputs)
    → Spawn worker thread
    → Thread: scihist.for_each(...)
    → Stream stdout → WebSocket → Browser
    → On success: clear pending constants
    → Notify: run_done
Browser → Refresh graph
```

### 4. Hot Reload

```
Browser → POST /api/project/refresh
  → registry.refresh_all()
    → Clear _functions dict
    → Reload all user modules
    → Rescan for callables
    → Notify: dag_updated
```

---

## Threading Model

### Main Event Loop
- **FastAPI**: Uvicorn async event loop
- **JSON-RPC**: Blocking stdin read loop

### Worker Threads
- One per active run
- Executes `scihist.for_each()`
- Holds database connection for duration
- Communicates via push messages

### Concurrency Primitives

| Primitive | Location | Purpose |
|-----------|----------|---------|
| `threading.Lock` | `db.py` | Protect refcount & connection state |
| `threading.Event` | `run.py` | Cooperative cancellation signal |
| `threading.Thread` | `run.py` | Background execution |
| `asyncio.create_task` | `ws.py` | Schedule WebSocket sends |

### Database Access Pattern

```
Request thread:
  acquire_db_connection()  # _refcount++, reopen if needed
  try:
    # Use db
  finally:
    release_db_connection()  # _refcount--, close if idle

Worker thread:
  acquire_db_connection()  # Hold for entire run
  try:
    scihist.for_each(...)
  finally:
    release_db_connection()
```

**Critical invariant**: Only one thread holds connection at a time (enforced by DuckDB's exclusive write lock).

---

## Error Handling Patterns

### 1. Graceful Degradation
```python
# MATLAB registry optional
try:
    from .matlab_registry import get_matlab_functions
    matlab_funcs = get_matlab_functions(config)
except Exception as e:
    logger.warning(f"MATLAB discovery failed: {e}")
    matlab_funcs = {}
```

### 2. User-Facing Errors
```python
# Invalid function name
if fn_name not in registry.get_functions():
    raise HTTPException(
        status_code=404,
        detail=f"Function '{fn_name}' not found in registry"
    )
```

### 3. Background Thread Errors
```python
# Execution error
try:
    scihist.for_each(...)
except Exception as e:
    push_message({
        "type": "run_done",
        "runId": run_id,
        "success": False,
        "error": str(e)
    })
    logger.exception("Run failed")  # Full traceback to server log
```

---

## Performance Considerations

### 1. Lazy Loading
- Database connection opened per-request, closed when idle
- Layout file only read on first access
- Registry only rebuilt on explicit refresh

### 2. Caching
- Function source code cached in registry (no re-inspection)
- Variable metadata cached in `BaseVariable._all_subclasses`

### 3. Efficient Persistence
- Positions in JSON (avoid DB write on every drag)
- Manual structure in DuckDB (queryable, transactional)

### 4. Streaming
- Stdout line-by-line → WebSocket (no buffering)
- JSON-RPC responses newline-delimited (no framing overhead)

---

## Debugging & Observability

### Logging

**Python logging**:
```python
import logging
logger = logging.getLogger(__name__)
logger.info("Message")
```

**scidb.log bridge** (`app.py` lines 50-80):
```python
# Redirect Python logging → scidb.log
from scidb.log import Log
logging.basicConfig(handlers=[LogHandler()])
```

### JSON-RPC Debugging

**Enable debugpy** (`server.py` lines 20-40):
```python
import debugpy
debugpy.listen(("localhost", 5678))
# Attach VS Code debugger
```

### FastAPI Debugging

**Run with `--reload`**:
```bash
uvicorn scistack_gui.app:create_app --reload --port 8765
```

### Database Inspection

```python
# CLI mode
python -m scistack_gui experiment.duckdb

# Inside GUI, open browser console:
fetch('/api/pipeline').then(r => r.json()).then(console.log)
```

---

## Testing Strategy

### Unit Tests (domain/)
- Pure functions, no I/O
- Test graph building, state propagation, variant resolution
- Example: `test_run_state.py`, `test_variant_resolver.py`

### Integration Tests (services/)
- Mock database with in-memory DuckDB
- Test service layer methods
- Example: `test_pipeline_service.py`

### End-to-End Tests
- Start server in test mode
- HTTP client requests
- Validate responses

---

## Summary

The scistack-gui backend is a sophisticated dual-protocol server that:

1. **Builds pipeline graphs** from database records + manual nodes
2. **Executes functions** in background threads with real-time stdout streaming
3. **Manages persistence** via DuckDB (structure) + JSON (positions)
4. **Supports hot reload** without server restart
5. **Integrates MATLAB** via command generation
6. **Runs standalone or in VS Code** via protocol abstraction

**Key design decisions**:
- **Service layer**: Shared logic for both protocols
- **Pure domain logic**: Testable graph/state algorithms
- **Connection refcounting**: Allows MATLAB concurrent access
- **Per-call-site state**: Lineage-aware pipeline visualization
- **Background threading**: Non-blocking execution with streaming

The architecture cleanly separates concerns and provides a solid foundation for building scientific data processing pipelines.
