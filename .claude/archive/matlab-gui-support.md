# MATLAB Support in SciStack VS Code Extension — Implementation

## Files Created
| File | Purpose |
|------|---------|
| `scistack_gui/matlab_parser.py` | Static .m file parsing (function signatures, classdef) |
| `scistack_gui/matlab_registry.py` | MATLAB function/variable registry (mirrors Python registry) |
| `scistack_gui/api/matlab_command.py` | MATLAB command string generation for copy-paste execution |
| `extension/src/matlabTerminal.ts` | MathWorks VS Code extension terminal integration |
| `tests/test_matlab.py` | Tests for parser, command generation, and config parsing |

## Files Modified
| File | Changes |
|------|---------|
| `scistack_gui/config.py` | Added MATLAB fields to SciStackConfig, scistack.toml support, glob path resolution |
| `scistack_gui/server.py` | MATLAB registry init, updated handlers (registry, params, source, refresh, create_variable), new generate_matlab_command handler |
| `scistack_gui/api/pipeline.py` | Tag MATLAB function nodes with `language: "matlab"`, fallback to MATLAB params in `_fn_params_from_registry` |
| `extension/src/extension.ts` | DuckDB file watcher with 2s debounce |
| `extension/src/dagPanel.ts` | MATLAB run interception, clipboard copy, MathWorks terminal integration |

## Architecture Decisions
1. **MATLAB functions use copy-paste execution (MVP)**: When user clicks "Run" on a MATLAB function, the extension generates a complete MATLAB script and either sends it to the MathWorks terminal or copies to clipboard.
2. **Python surrogates for MATLAB variables**: Each MATLAB `classdef Foo < scidb.BaseVariable` gets a Python surrogate via `bridge.register_matlab_variable()` so it appears in the DAG.
3. **DuckDB file watcher benefits all users**: Not just MATLAB — any external process writing to the DB triggers a DAG refresh.
4. **MathWorks terminal is best-effort**: Falls back gracefully to clipboard if the extension isn't installed.
5. **scistack.toml support**: MATLAB-only projects (no Python, no pyproject.toml) can use a standalone `scistack.toml`.

## Key Data Flow
```
User clicks "Run" on MATLAB node
  → Webview sends start_run with language: "matlab"
  → DagPanel.handleMatlabRun()
  → Python RPC: generate_matlab_command
  → Extension: try MathWorks terminal → fallback to clipboard
  → User runs in MATLAB → writes to DuckDB
  → FileSystemWatcher detects change
  → dag_updated notification → DAG refreshes
```
