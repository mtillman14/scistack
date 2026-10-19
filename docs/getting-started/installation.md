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

## Keep the project on a local disk

Keep two things on a disk inside your own computer, not on a network drive:

- **The database file** (`.duckdb`). SciStack stores everything in a DuckDB database, and DuckDB is built for a file on a local disk. On a network share it reads and writes in many small pieces, each one a round trip over the network, so runs and the GUI slow down sharply. DuckDB also relies on file locks to stop two programs writing at once, and network file systems don't always honour them, which risks a corrupted database.
- **The virtual environment** (`.venv`). Starting Python imports hundreds of small files from it. Over a network each file is a separate request, so every script, test and GUI start becomes slow.

That rules out:

- mapped network drives (`Z:\`, `\\server\share`) and SMB/NFS shares mounted on macOS or Linux (often under `/Volumes` or `/mnt`);
- cloud-synced folders such as OneDrive, Dropbox, Google Drive and iCloud Drive. They also sync the database while it is half-written.

/// warning | Your Documents folder may not be local
Many organizations redirect `Documents` and `Desktop` to a network share or to OneDrive. Check before creating a project there. On Windows, right-click the folder → **Properties**: a **Location** tab or a path starting with `\\` means it is redirected. Somewhere like `C:\Users\<you>\projects\my-study` (Windows) or `~/projects/my-study` (macOS/Linux) is a safe choice.
///

Raw data files can stay on the network. Only your loader functions read them, through [`PathInput`](writing-functions.md#5-read-raw-files-only-in-loader-functions-through-pathinput), one whole file at a time, which network drives handle well.

### Back up the database

Files on your own disk are often not backed up automatically, so **back up the `.duckdb` file yourself**. It holds every result and its whole history. Copy it while nothing has it open (close the GUI and any running scripts first), so it isn't copied mid-write. Copy the `.layout.json` file with the same name next to it too, since it holds the GUI's canvas layout. A copy on a network drive, or one picked up by your organization's backup, is a good destination: a backup is only read when you need it.

You don't need to back up `.venv`. It can be recreated at any time (see [New to Coding?](new-to-coding.md#step-4-create-a-project-folder-and-a-virtual-environment)), as long as your project lists the packages it needs: see [Export & Import Projects](../sharing/export-import.md#list-your-dependencies).
