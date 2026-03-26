# MATLAB Setup

SciStack works natively in MATLAB via a thin wrapper around the Python package. All hashing, database operations, and lineage tracking are performed by Python under the hood — MATLAB is the user-facing layer.

## Requirements

- MATLAB R2021b or later (required for `name=value` argument syntax)
- Python 3.10 or later
- The `scidb` Python package and its dependencies installed
- MATLAB's Python environment configured to point at your Python installation

## Installation

### 1. Install the Python Package

```bash
pip install git+https://github.com/mtillman14/general-sqlite-database
```

### 2. Configure MATLAB's Python Environment

MATLAB must be pointed at the same Python environment where `scidb` is installed. Do this once per MATLAB session (or add it to your `startup.m`):

```matlab
% Point MATLAB at your Python interpreter
pyenv('Version', '/path/to/python');

% Verify Python can find the scidb package
py.importlib.import_module('scidb');
```

To find your Python path:

```bash
# In your terminal / Anaconda prompt
which python        # macOS/Linux
where python        # Windows
```

If you use a conda environment:

```bash
conda activate my_env
which python
```

### 3. Add the MATLAB Package to Your Path

```matlab
% Add the scidb-matlab MATLAB package to your MATLAB path
addpath('/path/to/sci-matlab/src/sci_matlab/matlab');

% Verify it works
help scidb.BaseVariable
```

Add both lines to your `startup.m` so they run automatically when MATLAB starts.

## Quick Verification

```matlab
% This should complete without errors
scidb.configure_database("test.duckdb", ["subject", "session"], "pipeline.db");

% Define a variable type
% (In a real project, this goes in its own .m file — see below)

% Should print: "SciStack is ready"
fprintf("SciStack is ready\n");
```

## Defining Variable Types

In MATLAB, each variable type is a one-line classdef in its own `.m` file:

```matlab
% In RawSignal.m:
classdef RawSignal < scidb.BaseVariable
end

% In FilteredSignal.m:
classdef FilteredSignal < scidb.BaseVariable
end
```

The class name becomes the database table name automatically. No additional properties or methods are needed for the common case.

Place these files somewhere on your MATLAB path. A common pattern is a `vars/` folder in your project:

```
my_project/
  startup.m
  vars/
    RawSignal.m
    FilteredSignal.m
    StepLength.m
  pipeline/
    load_data.m
    compute_steps.m
```

```matlab
% In startup.m
addpath('/path/to/sci-matlab/src/sci_matlab/matlab');
addpath(fullfile(pwd, 'vars'));
```

## Cross-Language Interoperability

Data saved from Python can be loaded in MATLAB and vice versa. The database format is identical — both languages write to the same DuckDB and SQLite files.

```matlab
% Load data that was saved from Python
raw = RawSignal().load(subject=1, session="A");
disp(raw.data);  % Your Python-saved array, now in MATLAB
```

!!! note "Lineage caching is language-specific"
    Python thunks and MATLAB thunks have separate cache namespaces. A MATLAB thunk does not find cache entries written by a Python thunk for the same function, and vice versa. This is because function identity is computed differently (bytecode hash in Python, source file hash in MATLAB).

## Troubleshooting

**`py.importlib.import_module('scidb')` fails:**
Make sure the Python environment has `scidb` installed:
```bash
pip show scidb
```

**MATLAB cannot find `scidb.BaseVariable`:**
Check that the MATLAB package path is on your path:
```matlab
which scidb.BaseVariable
```

**Type errors when passing data to Python:**
MATLAB and Python have different numeric defaults. MATLAB integers may need explicit casting. SciStack handles most common cases automatically, but if you see conversion errors, try:
```matlab
data = double(data);  % Ensure float64 for numpy compatibility
```
