# scimatlab Setup Guide

## 1. Install Python

MATLAB requires a compatible CPython installation. Check the [MATLAB-Python compatibility table](https://www.mathworks.com/support/requirements/python-compatibility.html) for your MATLAB release. scidb requires Python 3.9+.

Install Python if you don't already have it. On macOS/Linux, using a virtual environment is recommended:

```bash
python3 -m venv ~/scidb-env
source ~/scidb-env/bin/activate
```

On Windows:

```cmd
python -m venv %USERPROFILE%\scidb-env
%USERPROFILE%\scidb-env\Scripts\activate
```

## 2. Install Python Packages

From the root of this repository, install every package in editable mode, in
dependency order (the script is the one owner of that order):

```bash
bash dev-install.sh
```

## 3. Point MATLAB at Your Python

Open MATLAB and configure it to use the Python where you installed the packages:

```matlab
% Check current Python config
pyenv

% Set the Python executable (do this once per MATLAB session, before any py. calls)
pyenv('Version', '~/scidb-env/bin/python')   % macOS/Linux
pyenv('Version', 'C:\Users\you\scidb-env\Scripts\python.exe')  % Windows
```

To make this persistent across MATLAB sessions, add the `pyenv(...)` call to your `startup.m` file (typically at `~/Documents/MATLAB/startup.m`).

## 4. Add the MATLAB Package to the Path

```matlab
addpath('/path/to/scimatlab/matlab')
```

Add this to your `startup.m` as well to make it persistent.

## 5. Verify the Setup

Run the following in MATLAB to confirm everything is connected:

```matlab
% Test that Python imports work
py.importlib.import_module('scidb');
py.importlib.import_module('scimatlab');
disp('Python packages loaded successfully.')

% Test database configuration
scidb.configure_database("test.duckdb", ["subject", "session"], "test_pipeline.db");
disp('Database configured successfully.')

% Clean up test files
delete test.duckdb test_pipeline.db
```

## 6. Define Your Variable Types

Create a `.m` file for each variable type on the MATLAB path:

```matlab
% RawSignal.m
classdef RawSignal < scidb.BaseVariable
end
```

## 7. Run a Pipeline

```matlab
scidb.configure_database("experiment.duckdb", ["subject", "session"], "pipeline.db");

% Save data
RawSignal().save(randn(100, 3), subject=1, session="A");

% Provenance-tracked computation (lineage recorded automatically)
scidb.for_each(@bandpass_filter, ...
    struct('signal', RawSignal(), 'low_hz', 10, 'high_hz', 200), ...
    {FilteredSignal()}, ...
    subject=1, session="A");
```

## Troubleshooting

**"Python Error: ModuleNotFoundError: No module named 'scidb'"**
The `pyenv` in MATLAB is not pointing at the Python environment where the packages are installed. Run `pyenv` in MATLAB to check the executable path, then verify that `python -c "import scidb"` works from a terminal using that same Python.

**"Python Error: ModuleNotFoundError: No module named 'scimatlab'"**
You need to `pip install -e scimatlab/` in the same Python environment that MATLAB is using.

**"Python is not loaded" or "Cannot change Python version after it has been loaded"**
`pyenv('Version', ...)` must be called before any `py.` call in the MATLAB session. Restart MATLAB and set `pyenv` before doing anything else. Put it in `startup.m` so it happens automatically.

**"MATLAB does not support this version of Python"**
Check the [compatibility table](https://www.mathworks.com/support/requirements/python-compatibility.html). You may need a different Python version. Create a separate virtual environment with a compatible version.
