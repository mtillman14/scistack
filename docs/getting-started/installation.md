# Installation

<!-- Ground truth (source win over prose). Verified against:
     on-disk package folders each with pyproject.toml: scicanonicalhash, path-gen (pkg
     scipathgen), scifor, sciduckdb, scilineage, scidb, scimatlab, scihist, scidb-net;
     declared [dependencies] giving the layer order; dev-install.sh (editable install in
     dependency order — but it has stale paths: "sciscicanonicalhash" typo and a "thunk/scirun"
     comment); run_tests.sh (per-package pytest; scimatlab skipped unless `matlab` on PATH);
     docs/requirements.txt (mkdocs docs build); .readthedocs.yaml.
     NOTE: folder `path-gen` ships package `scipathgen`; folder `scicanonicalhash` ships
     `scicanonicalhash`. Install editable in dependency order. -->

/// tip | New to coding, or on a new computer?
Start with [New to Coding?](new-to-coding.md): it covers installing VS Code and Python, creating a virtual environment, and the extensions to install.
///

```bash
pip install scistack
```

## To Update Versions

```bash
pip install -U scistack
```

## MATLAB One-Time Setup

1. Make sure that you have created and activated a Python virtual environment, located at `path/to/venv`

2. Install SciStack into the virtual environment with `pip install scistack`.

3. In MATLAB's Command Window, run `pyenv('path/to/venv')` to register the virtual environment in MATLAB. For frequent use, I recommend adding this line to your `startup.m` file, located by typing `userpath` in the Command Window.

4. Add the `scimatlab` package to your path with `scimatlab.addpath()` or adding to the path manually `addpath(genpath('path/to/venv/lib/scimatlab'))`.

/// warning | Python and MATLAB Versions
Check [here](https://www.mathworks.com/support/requirements/python-compatibility.html) to make sure that the Python interpreter is the right version for your version of MATLAB.
///
