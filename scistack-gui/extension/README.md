# SciStack Pipeline GUI — VS Code Extension

Visual pipeline builder for SciStack scientific data processing.

## Install

The extension is the front end; the work happens in the `scistack-gui` Python package, which you install into your project's environment **at the same version as the extension** (both are released together from one tag):

```
pip install scistack-gui==<extension version>
```

The extension version is shown on its page in the Extensions view. If the two drift apart (the Marketplace auto-updates the extension; pip never updates your venv), the extension warns on startup with the exact `pip install` command, and logs both versions to the **SciStack** Output Channel.

## Usage

Run **SciStack: Open Pipeline** from the Command Palette and select a `.duckdb` file (and optionally a pipeline `.py` module).

## Python Interpreter

The extension spawns a Python child process that must have `scistack-gui` installed. It picks an interpreter in this order:

1. The `scistack.pythonPath` setting, if set.
2. The active interpreter reported by the VS Code Python extension (`ms-python.python`).
3. `python3` on `PATH` as a last resort.

Note that VS Code does **not** inherit a venv from the shell it was launched from. If you rely on a virtual environment, either set `scistack.pythonPath` to that venv's python, or install the Python extension and select your interpreter via "Python: Select Interpreter".

### When the server fails to start

If the child process never reaches "ready", the extension probes the interpreter it used (`importlib.util.find_spec` for `scistack_gui`, `scidb`, `scifor`, `duckdb` — no imports are executed) and reports which of these went wrong:

| Diagnosis | Meaning |
| --- | --- |
| `interpreter_missing` | The interpreter path could not be executed at all (bad `scistack.pythonPath`, or the Windows `python3` Store alias). |
| `package_missing` | The interpreter runs, but that environment has no `scistack_gui`. |
| `dependency_missing` | `scistack_gui` is installed, but something it imports at startup (e.g. `scidb`) is not. |
| `startup_timeout` | The server started but never signalled ready within `scistack.startupTimeoutMs` of silence. |
| `server_error` | The server crashed; the exception line is quoted in the message. |

The notification always names the interpreter that was used and where that path came from. The full report — configured path, `sys.executable`, `sys.prefix`, version, per-package locations, spawn command, exit code, and the tail of the server's stderr — is written to the **SciStack** Output Channel ("Show Details").

## Tests

```
npm test     # tsc -p tsconfig.test.json && node --test dist/test scripts
```

Covers the startup-failure diagnostics (`src/startupDiagnostics.test.ts`). Only `vscode`-free modules are compiled into `dist/test`.

## MATLAB

### Debugging MATLAB code in VS Code

To hit breakpoints in your MATLAB pipeline functions when they are called via the SciStack-generated run command, the MathWorks MATLAB extension must be configured to attach its debugger automatically:

1. Open **Settings** (`Cmd+,` / `Ctrl+,`)
2. Search for **MATLAB: Start Debugger Automatically**
3. **Check** the checkbox

Without this setting enabled, breakpoints set in `.m` files will not be hit when code runs in the MATLAB Command Window.
