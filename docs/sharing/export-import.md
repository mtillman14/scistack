# Export & Import Projects

<!-- Ground truth: scistack-gui/scistack_gui/bundle_cli.py (export / import /
     bundle-info / install; EXPORT_FLAGS), scidb/project/bundle.py (ExportOptions:
     history on, data off, wheelhouse off, logs on; check_environment reads pyproject
     [project] dependencies), scidb/project/environment.py (install_project_requirements:
     only declared deps that are missing, at the exporter's versions),
     scidb/project/init.py (_PYPROJECT template), design docs/claude/portability.md.
     Hidden folders (verified 2026-11-21): scidb/tests/test_path_redaction.py,
     scistack-gui/tests/test_bundle_hide_locations.py. -->


A whole project (its code, settings, pipelines and run history) can be exported as one `.scistack` file and imported on another computer. Use it to hand a project to a labmate, move to a new machine, or publish an analysis so others can reproduce it.

To share less than a whole project, see [Share Part of a Project](pipelines.md). For a step-by-step example with three typical hand-offs, see the [Export & Import Walkthrough](export-import-walkthrough.md).

## What travels, and what doesn't

| Part | In the export |
| --- | --- |
| Your code (`src/`, `.m` files, glue functions) and `pyproject.toml` | always |
| `scistack.toml` and the entities file | always |
| Pipelines, canvas layout, saved plots and presets | always |
| The exact version of every installed package, your Python version and platform | always |
| Run history: what was run, on which inputs, with which code | on by default |
| Saved results (the data itself) | off by default; can be large |
| Wheelhouse: an installable copy of each [library](libraries.md) the project uses | off by default; add it when a library isn't published anywhere the recipient can install it from |
| The last 5,000 lines of `scistack.log` and `scistack-debug.log` (the run journal and the debug log) | on by default; kept in the importer's `.scistack/imported/logs/`, never mixed into their own logs. With hidden folders, folders in the logs are replaced too |
| Raw data files read through `PathInput` | **never** |
| `.venv` | never; the recipient makes their own |

Raw data files are never copied. The person importing says where their own copy is.

A `.scistack` file is an ordinary zip of JSON, TOML and Parquet files, so it can be opened without SciStack.

## List your dependencies

For an import to work on another computer, the project has to say which packages it needs. Those go in `pyproject.toml` in the project root, which `scistack init` creates:

```toml
[project]
name = "my_study"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "scidb",
    "numpy",
    "pandas",
    "scipy",
]
```

**Add every package your code imports**: anything you installed with `pip install` yourself. Only packages listed here are installed on import. A package you forgot makes the code fail with `ModuleNotFoundError` on the recipient's computer.

You don't need to pin versions here. The export records the exact version of every installed package, and an import installs the missing ones at those versions. Pin a version (`"scipy==1.13.1"`) only when your code really depends on it.

The same list lets you rebuild your own `.venv` at any time: in a fresh environment, `python -m pip install -e .` from the project root installs the project and everything it lists.

/// note | MATLAB
MATLAB itself and its toolboxes aren't installed by an import. The export records the MATLAB release and toolboxes when MATLAB was running, so the recipient can check. List them in your project's README too.
///

## Export

/// tab | GUI

Click **⇩ export project** in the bar above the canvas. Tick whether to include the run history and the saved results, then choose where to save the file.

///

/// tab | Terminal

From the project root, with the project's `.venv` active:

```bash
scistack export                          # writes <database name>.scistack here
scistack export my_study.scistack --data # also include every saved result
scistack export --no-history             # code, settings and pipelines only
scistack export --wheelhouse             # also a wheel of each library the project uses
scistack export --no-logs                # leave out the end of the logs
scistack export --hide-path-locations    # leave out where your raw files are (see below)
```

`--db path/to/study.duckdb` picks the database when it isn't the one in `scistack.toml` or the only `.duckdb` in the folder.

///

Close any running scripts first, so the export doesn't catch a run halfway through.

### Hide where your raw files are

Folder names can say more than you mean to publish: a lab member's name, a drive layout, a participant's ID in a folder name. For a public release, tick **Hide PathInput folders (for a public release)** in the export popover, or pass `--hide-path-locations`. It's off by default.

With it on, no folder of yours appears anywhere in the bundle:

- Each `PathInput` keeps its name and the part of its template that says *which* files to read, but not *where* they are. `D:/study/raw/{subject}/{session}.csv` travels as `{subject}/{session}.csv`, and `root_folder` is dropped. Aliases and `key_regex` travel unchanged.
- The run history and logs travel with each folder replaced by the name of its `PathInput`, such as `<PathInput RAW>`.
- The bundle doesn't record your project's own folder either.

Your own project and database are not changed. Only the bundle is.

If a folder is still written somewhere SciStack can't rewrite, such as a path in your code (`DATA = "D:/study/raw"`) or in a canvas note, the export stops and writes nothing. The message names the file and the `PathInput` whose folder it found, but never the folder itself. Remove the path there, or read it through the `PathInput`, and export again.

`scistack bundle-info` on a bundle made this way says its folders were hidden. Whoever imports it gives a folder for each `PathInput` (below).

## Import

An import always makes a **new** project folder. It runs none of the bundle's code until you say you trust it, because running it (or installing its packages) runs code someone else wrote.

Before importing, create and activate a virtual environment for the new project, on a [local disk](../getting-started/installation.md#keep-the-project-on-a-local-disk), and install SciStack into it ([New to Coding?](../getting-started/new-to-coding.md) steps 4 and 7).

/// tab | GUI (VS Code)

Run **SciStack: Import Project Bundle…** from the Command Palette (**View → Command Palette…**). Pick the `.scistack` file and answer the questions: the new project's folder, your schema (and, for each of the exporter's keys that isn't in it, which of yours it is, or drop it), where your copy of each raw data folder is, and whether to import the history. A report opens when it's done. **Trust, Install and Open** installs the missing packages and opens the project; **Trust and Open** opens it without installing anything.

///

/// tab | Terminal

```bash
scistack bundle-info my_study.scistack   # what's inside: schema, PathInputs, sections, environment

scistack import my_study.scistack --into ~/projects/my_study \
    --path-root emg_file=/data/my_study/raw \
    --trust --check-code
```

- `--path-root NAME=FOLDER` gives your copy of one `PathInput`'s raw files. Repeat it for each one. A bundle exported with hidden folders has no folder to fall back on, so give one for every `PathInput`. The import report lists any you left out, and you can set them later in the entities file.
- `--schema subject session` uses your own schema keys instead of the exporter's. `--map OLD=NEW` renames one key. If your schema differs, the history is kept as files in `.scistack/imported/run-history/`, not loaded.
- `--trust` installs what the project lists in `pyproject.toml` and your environment lacks, at the exporter's versions. Add `--no-install` to skip that and run `scistack install` later.
- `--check-code` loads the project's code after importing and lists any pipeline step whose code wasn't found.
- `--no-history` leaves the exporter's run history out.
- `--no-value-rules` says the project will run on **other data** than the exporter's. The history still comes in (kept as files in `.scistack/imported/run-history/`, for `scistack verify`), and so do the rules about keys, such as schema key types. The rules that name the exporter's own values (exclusions, Expected / Not expected marks at a particular subject, declared key values) are left out: they are kept in the imported run history, unapplied, and the import prints a warning listing them. The default, `--value-rules`, applies them all, which is right when you'll run on the exporter's data.

///

## Check that the results reproduce

After running the imported pipelines on your copy of the raw data, compare your results with the ones the exporter recorded:

```bash
scistack verify --against my_study.scistack
```

It runs no code. It reports, step by step, which results match, which differ (and whether the difference starts at that step or upstream), and which steps' code changed or haven't been run.
