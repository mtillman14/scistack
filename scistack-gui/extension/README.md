# SciStack Pipeline GUI — VS Code Extension

Visual pipeline builder for SciStack scientific data processing.

## Install

The extension is the front end; the work happens in the Python backend, which you install into your project's environment **always at the same version as the extension**:

```
pip install scistack==<extension version>
```

The extension version is shown on its page in the Extensions view. If the two drift apart (the Marketplace auto-updates the extension; pip never updates your venv), the extension warns on startup with the exact `pip install` command, and logs both versions to the **SciStack** Output Channel.

## Usage

Run **SciStack: Open Pipeline** from the Command Palette and select or create a `.duckdb` file

## Python Interpreter

The extension spawns a Python child process that must have `scistack-gui` installed. It picks an interpreter in this order:

1. The `scistack.pythonPath` setting, if set.
2. The active interpreter reported by the VS Code Python extension (`ms-python.python`).
3. `python3` on `PATH` as a last resort.

## MATLAB

### Debugging MATLAB code in VS Code

To hit breakpoints in your MATLAB pipeline functions when they are called via the SciStack-generated run command, the MathWorks MATLAB extension must be configured to attach its debugger automatically:

1. Open **Settings** (`Cmd+,` / `Ctrl+,`)
2. Search for **MATLAB: Start Debugger Automatically**
3. **Check** the checkbox

Without this setting enabled, breakpoints set in `.m` files will not be hit when code runs in the MATLAB Command Window.
