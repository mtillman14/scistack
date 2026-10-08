# Project & Library Structure

> **Partly superseded (2026-10-08).** Still current: the project/library wall
> and "one project = one .duckdb". Superseded: uv and `uv.lock` (removed), the
> `.scistack/` snapshot folder (see `portability.md`), and the layout below
> (see `scidb.project.init_project` and `config-file-formats.md`: config lives
> only in `scistack.toml`, the entities file inside `src/<pkg>/`).

How scistack organizes user code for the GUI's Functions/Variables/Constants sidebar and for reproducible scientific workflows. This doc captures the design agreed on during the 2026-04-09 brainstorming session.

## Core concepts

**Project** — a single scientific research effort. Contains code, raw data, pipeline results, plots, and one `.duckdb` file. Intended to be used *once* for one study, tagged at publication time, and then possibly extended.

**Library** — reusable code shared across projects. Plain Python package. Distributed via a Homebrew-tap-style custom index (a GitHub repo of package metadata the user "taps" per-machine).

## Guiding principles

### Hard wall between projects and libraries

- **Projects depend only on libraries, never on other projects.**
- Reuse across projects happens by **extracting shared code into a library**, not by one project importing another.
- This forces the extraction discipline that keeps the dependency graph a clean DAG. A future tool will help promote parts of a project into a library; for now, it's a manual refactor.

### One project = one `.duckdb`

- Each project owns exactly one DuckDB file. Data and results are not shared across projects.
- Wanting to "share data across projects" is a signal that either (a) the two projects are really one, or (b) the shared thing should live in a data-library.
- The GUI action is **"New Project"**, not "New Database" — creating a `.duckdb` scaffolds a full project folder around it.

### Reproducibility via snapshots

A **project snapshot** (the publication state) is three things pinned together:

1. **Code SHA** — git tag on the project repo at publication time
2. **Library lockfile** — `uv.lock` pinning exact versions of every library dependency
3. **Record manifest** — the set of `record_id`s (or equivalently the `branch_params`) in the `.duckdb` that were the blessed outputs for the paper

Stored as `.scistack/snapshots/{name}.toml`. Extensions happen in-place in the same project and same db — scistack's existing variant tracking (`branch_params`) means new code produces new records without touching the snapshotted ones.

## Project folder layout

```
my-study/
├── .scistack/
│   ├── project.toml           # project metadata, scistack version
│   └── snapshots/
│       └── 2025-nature.toml   # publication snapshots
├── pyproject.toml             # library deps + tapped index URLs
├── uv.lock                    # pinned library versions
├── src/my_study/              # project's own Variables/Functions/Constants
├── data/                      # raw inputs (gitignored or DVC-tracked)
├── plots/
├── my_study.duckdb
└── README.md
```

- `src/my_study/` is **always in scope** — not something the user "adds." The GUI scans it automatically.
- `pyproject.toml` pins index URLs directly (not by reference to user-level config) so the project is portable: another machine only needs `uv sync`, not an identical index config.
- `uv.lock` is the source of truth for which libraries are in scope.

## Library layout

A library is a **plain Python package** — nothing scistack-specific at the filesystem level.

```
mylab-preprocessing/
├── pyproject.toml
├── src/mylab_preprocessing/
│   ├── __init__.py
│   ├── filters.py
│   └── constants.py
└── README.md
```

It becomes a scistack library by virtue of what's in the code — specifically, containing at least one `BaseVariable` subclass, `LineageFcn` instance, or `Constant` instance. Detected at import time, not declared in metadata.

## Discovery mechanism

The GUI sidebar is populated by walking every module in `src/{project}/` plus every package in `uv.lock` and running:

```python
from scidb import BaseVariable, Constant
from scilineage.core import LineageFcn

def discover(module):
    variables = [c for c in vars(module).values()
                 if isinstance(c, type) and issubclass(c, BaseVariable) and c is not BaseVariable]
    functions = [f for f in vars(module).values()
                 if isinstance(f, LineageFcn)]
    constants = [(name, c) for name, c in vars(module).items()
                 if isinstance(c, Constant)]
    return variables, functions, constants
```

### Why reuse `LineageFcn` instead of a new `@function` decorator

`@lineage_fcn` (scilineage/src/scilineage/core.py:394) already wraps the function in a `LineageFcn` instance. Any function that belongs in the GUI sidebar is already going to be `@lineage_fcn`-decorated because that's how it participates in scistack at all — lineage tracking isn't optional for pipeline functions. Helper/utility functions that shouldn't appear in the sidebar are naturally excluded because they won't be wrapped.

### `Constant` — the one new primitive

The only genuinely new piece of machinery introduced by this design. A lightweight wrapper so the scanner has something to `isinstance`-check against, and so values can carry a description and source location:

```python
from scidb import constant

SAMPLING_RATE_HZ = constant(
    1000,
    description="Default sampling rate for all recordings",
)
DEFAULT_BANDPASS = constant((1.0, 40.0), description="Standard LFP bandpass")
```

**Transparent value semantics:** constants act as their underlying value via operator overloading / `__getattr__` passthrough, so `freq = SAMPLING_RATE_HZ` just works. Decided to start transparent (ergonomic); may revisit if leakage causes problems.

### Filtering

Packages in `uv.lock` that contain zero scistack exports (e.g. `numpy`, `scipy`) are **hidden from the Libraries panel entirely**. The panel only shows packages where `discover()` returned something.

## Packaging & distribution

- **Tool:** `uv`. Handles lockfiles, custom indexes, and is the direction the Python ecosystem is converging on.
- **Index model:** Homebrew-tap-style — the user configures one or more GitHub-hosted package indexes in `~/.scistack/config.toml`. Each project's `pyproject.toml` references indexes by URL (not by tap-name) so projects are portable.
- **Language scope:** Python-only for now. Polyglot (e.g., MATLAB functions discoverable alongside Python) is a future extension; the discovery mechanism will need a separate path for non-Python code.

## Publication → extension workflow

The scenarios that motivated the hard wall:

| Scenario | What happens |
|---|---|
| Publish a paper | Create snapshot: git tag + record current `uv.lock` hash + record manifest of blessed `record_id`s. Commit `.scistack/snapshots/{name}.toml`. |
| Reproduce 3 years later | Clone repo → `git checkout {tag}` → `uv sync` (restores pinned libs) → re-run pipeline → compare against the record manifest. |
| Fix a bug after publication | Continue in same project, same db. New code produces new variants with new `branch_params`; old publication records untouched. Snapshot still resolves. |
| Add new subjects | Same project, same db, same code. New `schema_id` values → new records. Snapshot unaffected. |
| Start a new study reusing preprocessing | **Signal to extract preprocessing into a library.** Both the old project (retroactively, via a minor bump) and the new project depend on the library. Old snapshot unaffected because its lockfile pins the original inlined version. |
| Fundamentally divergent analytical fork | Rare. File-level copy: `scistack project fork` → new folder, new git repo, copy or re-ingest the db. Treated as "git clone," not "git submodule." |

## GUI workflow

### New Project

The creation action. User picks parent folder and project name → GUI scaffolds the folder layout above, pre-fills `pyproject.toml` with scistack baseline deps, runs `uv sync` to create `uv.lock` and the project venv. Sidebar starts empty.

### Project config panel

A project-level panel (not a node sidebar) with two sections:

**Project code**
- Lists modules under `src/{project}/` that were scanned
- Per module: name, import status (ok / error + traceback), counts of Variables/Functions/Constants
- Refresh via the existing Refresh button (no file watcher for now)

**Libraries**
- Lists entries from `uv.lock` where `discover()` found at least one export
- Per library: name, pinned version, source index, export counts
- "Add Library" button → add-library dialog
- Per-row remove and "view exports" actions

### Add-library dialog

- Index selector (from tapped indexes)
- Search against the selected index
- Version picker (default latest; explicit pin allowed)
- "Install" runs `uv add {name}=={version}` → relocks → re-imports env → re-runs discovery → updates sidebar
- Error states: network failure, version conflict (show uv's error verbatim), import failure after install (show traceback, offer remove)

### Index management

Separate settings panel for user-global config (`~/.scistack/config.toml`). Tapped indexes are GitHub repos containing package metadata. Adding a tap pulls the repo; the GUI queries its package list when populating the add-library dialog. Index URLs that a project uses are copied into its `pyproject.toml` so collaborators only need `uv sync`, not matching taps.

### Stale lockfile handling on project open

Current plan: **silent `uv sync` on project open, with a visible error if resolution fails.** Revisit after hands-on uv experience — the alternative is to prompt before syncing.

## Open items reserved for later

- **Polyglot discovery** (MATLAB and beyond) — not designed yet; Python-only for now.
- **Project fork tooling** (`scistack project fork`) — deferred; rare case.
- **Snapshot creation UI** — format is reserved (`{git_sha, lockfile_hash, record_ids}`) but the GUI action isn't built yet.
- **Library extraction tooling** — promote parts of a project into a library. Deferred until real usage shows which patterns of reuse are common.
