# SciStack Pipeline GUI — VS Code Extension

Visual pipeline builder for SciStack scientific data processing.

## Usage

Run **SciStack: Open Pipeline** from the Command Palette and select a `.duckdb` file (and optionally a pipeline `.py` module).

## Python Interpreter

The extension spawns a Python child process that must have `scistack-gui` installed. It picks an interpreter in this order:

1. The `scistack.pythonPath` setting, if set.
2. The active interpreter reported by the VS Code Python extension (`ms-python.python`).
3. `python3` on `PATH` as a last resort.

Note that VS Code does **not** inherit a venv from the shell it was launched from. If you rely on a virtual environment, either set `scistack.pythonPath` to that venv's python, or install the Python extension and select your interpreter via "Python: Select Interpreter".

## MATLAB

### Debugging MATLAB code in VS Code

To hit breakpoints in your MATLAB pipeline functions when they are called via the SciStack-generated run command, the MathWorks MATLAB extension must be configured to attach its debugger automatically:

1. Open **Settings** (`Cmd+,` / `Ctrl+,`)
2. Search for **MATLAB: Start Debugger Automatically**
3. **Check** the checkbox

Without this setting enabled, breakpoints set in `.m` files will not be hit when code runs in the MATLAB Command Window.
