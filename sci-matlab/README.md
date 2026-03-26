# sci-matlab

MATLAB wrapper for the SciStack scientific data versioning framework.

Provides `scidb.BaseVariable` and `scidb.Thunk` for MATLAB, with full lineage tracking and caching. All hashing, lineage computation, and database operations are delegated to Python via MATLAB's `py.` interface — the MATLAB layer is a thin wrapper.

## Requirements

- MATLAB R2021b or later (for name=value argument syntax)
- Python 3.10+ with the `scidb` and `sci-matlab` packages installed
- MATLAB's Python environment configured (`pyenv`)

## Setup

```matlab
% One-time: configure MATLAB's Python environment
pyenv('Version', '/path/to/python');

% Add the MATLAB package to the path
addpath('/path/to/sci-matlab/matlab');
```

## Quick Start

```matlab
%% Define variable types (just a classdef line — no boilerplate)
% In RawSignal.m:
%   classdef RawSignal < scidb.BaseVariable
%   end

%% Configure the database
scidb.configure_database("experiment.duckdb", ["subject", "session"], "pipeline.db");

%% Save raw data
RawSignal().save(randn(100, 3), subject=1, session="A");

%% Load data
raw = RawSignal().load(subject=1, session="A");
disp(raw.data);       % 100x3 double
disp(raw.record_id);  % "a3f8c2e1b9d04710"

%% Thunked computation with lineage tracking
filter_fn = scidb.Thunk(@bandpass_filter);
result = filter_fn(raw, 10, 200);

%% Save result (lineage is stored automatically)
FilteredSignal().save(result, subject=1, session="A");

%% Second run — cache hit, no computation
raw = RawSignal().load(subject=1, session="A");
result = filter_fn(raw, 10, 200);  % Returns cached result instantly

%% Inspect provenance
p = FilteredSignal().provenance(subject=1, session="A");
fprintf("Computed by: %s\n", p.function_name);
```

## Architecture

```
MATLAB (user code)
   │
   ├── scidb.BaseVariable   ← instance methods: save, load, load_all, list_versions, provenance
   ├── scidb.Thunk           ← wraps function handle, orchestrates cache check / execute
   │
   └── py. interface ──────────────────────────────┐
                                                    │
Python (in-process)                                 │
   ├── sci_matlab.bridge                          │
   │     ├── MatlabThunk          ← proxy for Thunk duck-typing contract
   │     ├── MatlabPipelineThunk  ← reuses classify_inputs() from thunk-lib
   │     └── make_thunk_output    ← creates real ThunkOutput instances
   │                                                │
   ├── thunk-lib (unchanged)                        │
   │     ├── classify_inputs()                      │
   │     ├── compute_lineage_hash()                 │
   │     └── extract_lineage()                      │
   │                                                │
   └── scidb (unchanged)                            │
         ├── DatabaseManager.save_variable()        │
         ├── DatabaseManager.find_by_lineage()      │
         └── configure_database()                   │
                    │                    │
                 DuckDB             SQLite
                 (data)            (lineage)
```

The key insight: Python proxy classes satisfy the duck-typing contracts of thunk-lib, so all existing Python code (lineage hashing, input classification, cache lookup, lineage extraction) works unchanged. No existing Python packages are modified.

## Defining Variable Types

Variable types are plain classdefs with zero boilerplate:

```matlab
% RawSignal.m
classdef RawSignal < scidb.BaseVariable
end

% FilteredSignal.m
classdef FilteredSignal < scidb.BaseVariable
end
```

The class name becomes the database table name — no properties or methods needed. Types are auto-registered with Python on first use (save, load, etc.).

## API Reference

### Database Configuration

| Function | Description |
|---|---|
| `scidb.configure_database(db, keys, pipeline)` | Set up database connection |
| `scidb.register_variable(Type(), schema_version=N)` | Pre-register with custom schema version (optional) |

### Data Storage (instance methods on BaseVariable)

All methods are called on instances of BaseVariable subclasses:

| Method | Description |
|---|---|
| `Type().save(data, name=val, ...)` | Save data with metadata |
| `Type().load(name=val, ...)` | Load latest matching data |
| `Type().load_all(name=val, ...)` | Load all matching data |
| `Type().list_versions(name=val, ...)` | List all versions |
| `Type().provenance(name=val, ...)` | Get lineage information |

### Thunk System

| Class/Function | Description |
|---|---|
| `scidb.Thunk(@func)` | Wrap a named function for lineage + caching |
| `t(args...)` | Call thunk: check cache, execute on miss, return ThunkOutput |

### Return Types

- `Type().load(...)` returns `scidb.BaseVariable` with `.data`, `.record_id`, `.metadata`
- Thunk calls return `scidb.ThunkOutput` with `.data` (pass to `Type().save(...)`)

## Cross-Language Interop

Data saved from Python can be loaded in MATLAB and vice versa. Lineage chains are continuous across languages — a MATLAB thunk can consume a Python-produced variable, and the provenance graph records the full history.

MATLAB thunks cache against other MATLAB thunks (not Python thunks), since function identity is computed differently (source file hash vs bytecode hash).
