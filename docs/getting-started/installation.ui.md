# Installation

The instructions below assume that you are using VS Code and have Python 3.10 or newer installed.

/// tip | New to coding, or on a new computer?
Start with [New to Coding?](new-to-coding.md): it covers installing VS Code and Python, creating a virtual environment, and the extensions to install.
///

### 1. Install the "[SciStack Pipeline GUI](https://marketplace.visualstudio.com/items?itemName=scistack.scistack-gui)" extension

### 2. `Python: Select Interpreter` in the Command Palette

<div class="step-body" markdown>

Specifies the path to the interpreter that VS Code (and therefore SciStack) should use.

Create a new virtual environment with `python -m venv path/to/venv` (if needed)

</div>

### 3. Activate the virtual environment

<div class="step-body" markdown>

Automatically activate by opening a new Terminal tab in VS Code

Manually activate the virtual environment (called `.venv`):
on Mac/Linux

```bash
source .venv/bin/activate
```

or on Windows

```bash
# PowerShell
.venv\Scripts\Activate.ps1

# Command Prompt (cmd.exe)
.venv\Scripts\activate.bat

# Git Bash
source .venv/Scripts/activate
```

</div>

### 4. Install SciStack into your selected virtual environment

```bash
pip install scistack
```
