# scimatlab

MATLAB wrapper for the SciStack scientific data versioning framework.

Provides `scidb.BaseVariable` and `scidb.for_each` for MATLAB, with full provenance tracking. All hashing, provenance recording, and database operations are delegated to Python via MATLAB's `py.` interface — the MATLAB layer is a thin wrapper. Lineage is recorded automatically by `scidb.for_each` into scidb's bipartite provenance graph; there is no per-call lineage wrapper.

## Requirements

- MATLAB R2021b or later (for name=value argument syntax)
- Python 3.10+ with the `scidb` and `scimatlab` packages installed
- MATLAB's Python environment configured (`pyenv`)

## Setup

```matlab
% One-time: configure MATLAB's Python environment
pyenv('Version', '/path/to/python');

% Add the MATLAB package to the path
addpath('/path/to/scimatlab/matlab');
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

%% Provenance-tracked computation (lineage recorded automatically)
scidb.for_each(@bandpass_filter, ...
    struct('signal', RawSignal(), 'low_hz', 10, 'high_hz', 200), ...
    {FilteredSignal()}, ...
    subject=1, session="A");

%% Inspect provenance (read from the bipartite graph)
p = FilteredSignal().provenance(subject=1, session="A");
fprintf("Computed by: %s\n", p.function_name);
```

## Architecture

```
MATLAB (user code)
   │
   ├── scidb.BaseVariable   ← instance methods: save, load, list_versions, provenance
   ├── scidb.for_each        ← batch execution; provenance recorded on save
   │
   └── py. interface ──────────────────────────────┐
                                                    │
Python (in-process)                                 │
   ├── scimatlab.bridge                          │
   │     ├── for_each_prepare / for_each_save  ← run scidb.for_each's prepare +
   │     │                                        save phases (MATLAB runs the loop)
   │     ├── save_batch_bridge / load_and_extract ← bulk save/load
   │     └── MatlabLineageFcn                  ← lightweight identity proxy used
   │                                              only for node-state coloring
   │                                                │
   └── scidb (the single source of truth)           │
         ├── DatabaseManager.save_batch / load      │
         ├── for_each (records the bipartite        │
         │   provenance graph from save metadata)   │
         ├── get_provenance / provenance_query      │
         └── configure_database()                   │
                    │
                 DuckDB
          (data + bipartite provenance graph)
```

The key insight: the MATLAB layer hands its inputs/results to the Python bridge, which drives scidb's real prepare/save phases. All correctness-sensitive logic — variant tracking, provenance recording, identity hashing, where= semantics — lives in scidb, so MATLAB-driven and Python-driven pipelines stay in sync. (`scilineage` is reduced to function-source hashing; the former `@lineage_fcn` / `LineageFcnResult` / rerun-cache system was removed in favor of the bipartite graph.)

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

A variable declared in the project's TOML entities file (`variables = [...]`, e.g. one created in the GUI) does not need a hand-written file: `scidb.entities()` checks every declared name with `exist(name, 'class')` and writes a stub classdef for the ones MATLAB cannot resolve, then adds that directory to the path. Stubs go to `scistack.toml`'s `[matlab] variable_dir` when configured, otherwise to `scistack_variables/` beside the entities file (`scimatlab.stubs`). A name that already resolves is never touched, so a hand-written classdef is never shadowed by a generated one.

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
| `Type().load(name=val, ...)` | Load latest matching data (single or array) |
| `Type().load(name=val, 'version', 'all', ...)` | Load all stored versions |
| `Type().load(name=val, 'version', id, ...)` | Load a specific record by record_id |
| `Type().list_versions(name=val, ...)` | List all versions |
| `Type().provenance(name=val, ...)` | Get lineage information |
| `Type().to_csv(filename, name=val, ...)` | Export to a flat CSV (one row per schema_id). Works for scalars, single-row tables, `Type("col")` column selection, and `scidb.Merge(A(), B()).to_csv(...)` |

### Batch Execution & Provenance

| Class/Function | Description |
|---|---|
| `scidb.for_each(@func, inputs, outputs, Name, Value, ...)` | Run `func` once per combo; loads inputs, runs the loop, saves outputs, and records the provenance graph automatically |
| `Type().provenance(Name, Value, ...)` | Read provenance from the graph: `function_name`, `function_hash`, `inputs` (cell of structs), `constants` (struct) |

`inputs` is a struct mapping parameter names to `scidb.BaseVariable` instances, `scidb.Fixed` / `scidb.Variant` / `scidb.Merge` wrappers, `scifor.PathInput` instances, or constant values. `outputs` is a cell array of output type instances.

### Return Types

- `Type().load(...)` returns `scidb.BaseVariable` with `.data`, `.record_id`, `.metadata`, `.content_hash`, `.branch_params`

## Cross-Language Interop

Data saved from Python can be loaded in MATLAB and vice versa. Provenance chains are continuous across languages — a MATLAB `scidb.for_each` step can consume a Python-produced variable, and the bipartite graph records the full history.

Function identity is content-addressed by source hash, computed differently per language (MATLAB source-file hash vs Python bytecode/AST hash), so a re-run reproduces records within a language.
