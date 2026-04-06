# Multi-Source Pipeline Discovery

## Overview

SciStack GUI supports discovering pipeline Functions and Variable classes from multiple sources beyond a single `.py` file. This enables real-world project structures where code is split across files, shared via pip packages, and composed from reusable libraries.

## Two Modes

### Single-File Mode (Legacy)

Pass `--module pipeline.py` at startup. All Functions and Variables must be defined in (or imported by) that one file. This mode is unchanged and fully backward-compatible.

### Project Mode

Pass `--project path/to/pyproject.toml` (or a directory containing one). The server reads `[tool.scistack]` configuration and loads code from three source types:

1. **Explicit module files** — local `.py` files listed by path
2. **Explicit packages** — pip-installed packages listed by name
3. **Auto-discovered plugins** — any installed package declaring a `scistack.plugins` entry point

## Configuration Format

In the user's `pyproject.toml`:

```toml
[tool.scistack]
# Local .py files to load (paths relative to pyproject.toml)
modules = [
    "pipeline/variables.py",
    "pipeline/preprocessing.py",
    "pipeline/analysis.py",
]

# Where create_variable writes new BaseVariable subclasses
variable_file = "pipeline/variables.py"

# Pip-installed packages to scan for Functions/Variables
packages = ["lab_shared_utils", "eeg_preprocessing"]

# Auto-discover packages with scistack.plugins entry points (default: true)
auto_discover = true
```

All fields are optional. An empty `[tool.scistack]` section is valid (useful if you only want auto-discovery).

## Entry Points for Shared Packages

A pip-installable package can make its Functions and Variables discoverable by adding to its `pyproject.toml`:

```toml
[project.entry-points."scistack.plugins"]
my_plugin = "my_package.pipeline"
```

The value is a dotted module path. When `auto_discover = true`, SciStack imports that module at startup, which:
- Registers any `BaseVariable` subclasses via `__init_subclass__`
- Makes any top-level callable functions (not starting with `_`) available in the registry

Multiple entry points per package are supported:

```toml
[project.entry-points."scistack.plugins"]
variables = "my_package.variables"
filters = "my_package.filters"
analysis = "my_package.analysis"
```

## Discovery Order and Name Collisions

Sources are loaded in this order:
1. Explicit modules (in listed order)
2. Explicit packages (in listed order, submodules walked recursively)
3. Auto-discovered entry points

If two sources define a function with the same name, the later one wins and a warning is logged. BaseVariable subclasses follow the same last-write-wins behavior (keyed by class name via `__init_subclass__`).

## Refresh Behavior

In project mode, `refresh_module` (RPC) clears all registered functions and re-runs the full discovery pipeline. All three source types are reloaded. In the VS Code extension, "SciStack: Restart Python Process" does a full subprocess restart which achieves the same effect.

## Key Implementation Files

- `scistack_gui/config.py` — `SciStackConfig` dataclass and `load_config()` parser
- `scistack_gui/registry.py` — `load_from_config()`, `refresh_all()`, sub-loaders for each source type
- `scistack_gui/server.py` — `--project` CLI arg, startup dispatch between modes
- `scistack_gui/__main__.py` — Same `--project` arg for standalone FastAPI mode
- `extension/src/extension.ts` — UI flow with "Select a project" option
- `extension/src/pythonProcess.ts` — Passes `--project` to the Python child process

## Config Location

If `--project` is not passed explicitly, the server searches upward from the database file's directory for a `pyproject.toml` containing a `[tool.scistack]` section. This means placing your `pyproject.toml` at the project root and your `.duckdb` file anywhere within the project tree will work automatically.

## Migration from Single-File Mode

A user with an existing `pipeline.py` can either:
1. Continue using `--module pipeline.py` (nothing changes)
2. Create a `pyproject.toml` alongside it:

```toml
[tool.scistack]
modules = ["pipeline.py"]
variable_file = "pipeline.py"
```

To split code later, just add more entries to `modules` and move definitions between files.
