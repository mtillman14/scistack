# Export & Import Projects

<!-- Ground truth: scistack-gui/scistack_gui/bundle_cli.py (export / import /
     bundle-info / install; EXPORT_FLAGS), scidb/bundle.py (ExportOptions:
     history on, data off, wheelhouse off; check_environment reads pyproject
     [project] dependencies), scidb/environment.py (install_project_requirements:
     only declared deps that are missing, at the exporter's versions),
     scidb/project.py (_PYPROJECT template), design docs/claude/portability.md. -->

A whole project (its code, settings, pipelines and run history) can be exported as one `.scistack` file and imported on another computer. Use it to hand a project to a labmate, move to a new machine, or publish an analysis so others can reproduce it.

## What travels, and what doesn't

| Part | In the export |
| --- | --- |
| Your code (`src/`, `.m` files, glue functions) and `pyproject.toml` | always |
| `scistack.toml` and the entities file | always |
| Pipelines, canvas layout, saved plots and presets | always |
| The exact version of every installed package, your Python version and platform | always |
| Run history: what was run, on which inputs, with which code | on by default |
| Saved results (the data itself) | off by default; can be large |
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
```

`--db path/to/study.duckdb` picks the database when it isn't the one in `scistack.toml` or the only `.duckdb` in the folder.

///

Close any running scripts first, so the export doesn't catch a run halfway through.

## Import

An import always makes a **new** project folder. It runs none of the bundle's code until you say you trust it, because running it (or installing its packages) runs code someone else wrote.

Before importing, create and activate a virtual environment for the new project, on a [local disk](../getting-started/installation.md#keep-the-project-on-a-local-disk), and install SciStack into it ([New to Coding?](../getting-started/new-to-coding.md) steps 4 and 7).

/// tab | GUI (VS Code)

Run **SciStack: Import Project Bundle…** from the Command Palette (**View → Command Palette…**). Pick the `.scistack` file and answer the questions: the new project's folder, your schema, and where your copy of each raw data folder is. A report opens when it's done. **Trust and Open** installs the missing packages and opens the project.

///

/// tab | Terminal

```bash
scistack bundle-info my_study.scistack   # what's inside: schema, PathInputs, sections, environment

scistack import my_study.scistack --into ~/projects/my_study \
    --path-root emg_file=/data/my_study/raw \
    --trust --check-code
```

- `--path-root NAME=FOLDER` gives your copy of one `PathInput`'s raw files. Repeat it for each one.
- `--schema subject session` uses your own schema keys instead of the exporter's. `--map OLD=NEW` renames one key. If your schema differs, the history is archived, not loaded.
- `--trust` installs what the project lists in `pyproject.toml` and your environment lacks, at the exporter's versions. Add `--no-install` to skip that and run `scistack install` later.
- `--check-code` loads the project's code after importing and lists any pipeline step whose code wasn't found.
- `--no-history` leaves the exporter's run history out.

///

## Check that the results reproduce

After running the imported pipelines on your copy of the raw data, compare your results with the ones the exporter recorded:

```bash
scistack verify --against my_study.scistack
```

It runs no code. It reports, step by step, which results match, which differ (and whether the difference starts at that step or upstream), and which steps' code changed or haven't been run.
