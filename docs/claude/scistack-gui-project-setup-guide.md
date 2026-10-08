# SciStack Project Setup Guide

How a SciStack project is laid out on disk, and how to create one. Rewritten
2026-10-08 (portability plan Stage 1); the previous version described uv,
library taps and `[tool.scistack]`, none of which apply any more.

Reference docs this guide points to rather than repeats:
`config-file-formats.md` (every `scistack.toml` key), `entities-toml-format.md`
(the entities file), `code-discovery-categories.md` (what counts as a
function/variable), `portability.md` (export/import).

## Creating a project

There is one owner of "make this folder a SciStack project":
`scidb.project.init_project`. Both entry points call it.

- **Terminal:** `scistack init [PATH] [--name NAME] [--schema-keys subject session ...]`.
  `PATH` defaults to the current directory. `--schema-keys` also creates
  `<name>.duckdb`.
- **GUI:** creating a database (VS Code "new database", or the browser
  wizard's *Create*) runs the same function before anything else. Opening an
  existing database does not: it never turns a folder of scripts into a
  package unasked.

`init` **only creates what is missing.** An existing file is never rewritten,
so it is safe in a folder that already has code, and safe to run twice. It
reports each file as `created` or `kept`.

The package name is `pyproject.toml`'s `[project].name` if there is one,
otherwise the folder name made into a valid identifier (`"Gait Study 2"` ->
`gait_study_2`). It must be lowercase letters, digits and underscores, and
start with a letter.

## Layout

```
my_study/
├── pyproject.toml                  # packaging only: name, dependencies, build
├── scistack.toml                   # THE project config
├── .gitignore
├── src/my_study/
│   ├── __init__.py
│   ├── <your modules>.py           # functions, hand-written declarations
│   └── scistack_entities.toml      # Variables/Parameters/PathInputs (GUI-written)
├── my_study.duckdb                 # data + history (not committed)
└── my_study.layout.json            # canvas positions (beside the database)
```

- **`pyproject.toml`** is packaging metadata. SciStack reads only
  `[project].name` from it (to find `src/<name>/`); a `[tool.scistack]` table
  there is ignored, and the GUI never edits this file. It exists so the
  project can be built into a wheel and shared.
- **`scistack.toml`** is the only config file. The GUI writes it (Paths
  popup, entities file, glue dir, aliases, colors) through
  `scidb.config_file`, which keeps every key, including ones it does not
  know. `init` seeds `modules = ["."]` and `[matlab] sources = ["."]` (the
  project root, as a relative path so the file is portable).
- **The entities file lives inside the package** so it is package data and
  ships with the code that uses it. Its default location has one owner,
  `scidb.entities.default_entities_relpath`.

## How code is discovered

The GUI's loader is `scistack_gui.config.load_config`.

1. **The project's own package** (`src/<name>/`, named by `pyproject.toml`)
   is always loaded as a package, with or without a `scistack.toml`. Its
   files are never also imported as loose modules, even though `"."` covers
   them (`_without_own_package_files`).
2. **`modules`**: files, directories (walked recursively) and globs,
   relative to the project root.
3. **`packages`**: installed packages to scan, by import name. This is how
   a project uses a shared library: `pip install` it, then list it here.
4. **Entry points**: installed packages declaring `scistack.plugins`, unless
   `auto_discover = false`.
5. **No `scistack.toml` at all**: the project root is folder-scanned for
   `.py`/`.m` files (the own package is still loaded as a package).

`scidb.discover.scan_project` (headless) uses the same two sources: the
project's own package and the `packages` list. It never reads a lockfile.

## MATLAB

`.m` code is found through `[matlab] sources` (seeded with the project root)
or the explicit `functions`/`variables` lists. Variables declared in the
entities file get classdef stubs materialized next to it. See
`matlab-path-resolution.md` and `matlab-gui-implementation.md`.

## Environments

SciStack does not create or sync Python environments (no uv, 2026-10-08).
Install the project and its dependencies with any tool (`pip install -e .`
works, since `init` writes a buildable `pyproject.toml`) and point the GUI at
that interpreter.

## Troubleshooting

- **Something does not appear in the GUI.** `scidb.log` (beside the
  database) records which config file was loaded, which files and packages
  were scanned, and each module's import errors. Lines tagged `[config]` show
  the resolved project root and every `modules`/`packages` entry.
- **"No scistack.toml at ..."** when saving an alias or colour: the project
  has no config yet. Add a path in the Paths popup, or run `scistack init`.
- **A `[tool.scistack]` table is ignored.** Move its keys into
  `scistack.toml`; it is the only config file.
