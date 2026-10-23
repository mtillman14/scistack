# Export & Import Projects

<!-- Ground truth: scistack-gui ProjectExportButton.tsx (⇩ export project;
     choices from bundle_cli.export_choices / EXPORT_FLAGS, defaults from
     scidb.bundle.ExportOptions), extension command scistack.importProjectBundle
     (runs bundle_cli import in its own process; "Trust and Open"),
     VerifyButton.tsx (✓ verify, compares only), scidb/bundle.py,
     scidb/environment.py, design docs/claude/portability.md. -->

A whole project (its code, settings, every hypothesis, saved plots and run history) can be exported as one `.scistack` file and imported on another computer. Use it to hand a project to a labmate, move to a new machine, or publish an analysis so others can reproduce it.

To share less than a whole project, see [Share a Hypothesis or Submodule](pipelines.md).

## What travels, and what doesn't

| Part | In the export |
| --- | --- |
| Your code (`src/`, `.m` files, glue functions) and `pyproject.toml` | always |
| `scistack.toml` and the entities file | always |
| Every hypothesis and submodule, the canvas layout, saved plots and presets | always |
| The exact version of every installed package, your Python version and platform | always |
| Run history: what was run, on which inputs, with which code | on by default |
| Saved results (the data itself) | off by default; can be large |
| Wheelhouse: an installable copy of each [library](libraries.md) the project uses | off by default; tick it when a library isn't published anywhere the recipient can install it from |
| Raw data files read through path inputs | **never** |
| `.venv` | never; the recipient makes their own |

Raw data files are never copied. The person importing says where their own copy is.

A `.scistack` file is an ordinary zip of JSON, TOML and Parquet files, so it can be opened without SciStack.

## Before you export: list your dependencies

For an import to work on another computer, the project has to say which packages it needs. Those go in `pyproject.toml` in the project folder, which SciStack created with the project. Open it in VS Code and add **every package your code imports** that you installed yourself:

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

Only packages listed here are installed on import. A package you forgot makes the code fail with `ModuleNotFoundError` on the recipient's computer.

You don't need version numbers. The export records the exact version of every installed package, and an import installs the missing ones at those versions.

/// note | MATLAB
MATLAB itself and its toolboxes aren't installed by an import. The export records the MATLAB release and toolboxes when MATLAB was running, so the recipient can check. List them in your project's README too.
///

## Export

1. Let any run finish first, so the export doesn't catch a run halfway through.
2. Click **⇩ export project** at the right end of the hypothesis tab strip.
3. Tick what to include besides the code, settings and canvases: **History**, **Data**, **Wheelhouse**.
4. Click **Export…** and choose where to save the file.

/// info | 📷 Screenshot
**Show:** the **Export project** popover open under the **⇩ export project** button, with the History, Data and Wheelhouse checkboxes (History ticked) and the **Export…** button.
///

### Which boxes to tick

| You're… | History | Data |
| --- | --- | --- |
| Giving someone your pipeline to run on **their own** data | off | off |
| Handing off a finished study they'll re-run to check | on | off |
| Handing off a finished study they should open with every result already there | on | on |

With **Data** on, the canvas opens on the other end exactly as you left it, every step green. With **History** but no data, the history is kept for checking (see [Check that the results reproduce](#check-that-the-results-reproduce)) and the steps start as not run.

## Import

An import always makes a **new** project folder. It runs none of the bundle's code until you say you trust it, because running it (or installing its packages) runs code someone else wrote.

1. Create and activate a virtual environment for the new project, on a [local disk](../getting-started/installation.md#keep-the-project-on-a-local-disk), and install SciStack into it ([New to Coding?](../getting-started/new-to-coding.md) steps 4 and 7).
2. In VS Code, open the Command Palette (**View → Command Palette…**) and run **SciStack: Import Project Bundle…**.
3. Pick the `.scistack` file and answer the questions:
    - **The new project's folder.**
    - **Your schema.** Keep the exporter's schema, or enter your own keys. With your own schema the exporter's history isn't loaded (it describes a dataset of a different shape), and every node starts as not run.
    - **Where your copy of each raw data folder is**, one question per path input.
4. A report opens when it's done: what was imported, anything that needs a look (a filter or plot setting that named a schema key you dropped, a path input you left pointing at the exporter's folder).
5. Click **Trust and Open**. SciStack installs the packages the project lists that you don't have yet, at the exporter's versions, and opens the project.

/// info | 📷 Screenshot
**Show:** the Command Palette with **SciStack: Import Project Bundle…** highlighted.
///

/// info | 📷 Screenshot
**Show:** the import report document next to the **Trust and Open** prompt.
///

When the project opens, any canvas step whose code wasn't found is listed. That usually means a package was missing from the exporter's `pyproject.toml`: install it into the project's `.venv` and click **Refresh**.

/// tip | An imported project is yours
The imported project is a copy. Nothing links it back to the exporter's project: their later changes don't reach you, and yours don't reach them. To share an updated version, export again and import into a new folder.
///

## Check that the results reproduce

After running the imported pipelines on your copy of the raw data, click **✓ verify** in the hypothesis tab strip. It compares your results with the ones the exporter recorded, using the history that came in the bundle (or a `.scistack` file you name).

It runs no code. It reports, step by step, which results match, which differ (and whether the difference starts at that step or upstream), and which steps' code changed or haven't been run yet.

/// info | 📷 Screenshot
**Show:** the **Verify reproduction** popover after a check: counts of reproduced / differs / not run, and the first difference listed.
///

## From a terminal

Every step above also works without the GUI: `scistack export`, `scistack import` and `scistack verify`. See the Code version of this page (the **Code** toggle in the header).
