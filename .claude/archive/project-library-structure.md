# Project & Library Structure — Implementation Plan

Implementation plan for the project/library structure designed on 2026-04-09. Full design context: `docs/claude/project-library-structure.md`. This plan orders the work into buildable units and flags dependencies.

## Summary of what gets built

1. `Constant` primitive in a new `scistack` package (or wherever the discovery module lives)
2. Discovery scanner that walks project src + installed libraries
3. Project scaffolder for "New Project"
4. `uv` integration for sync, lockfile reading, add/remove
5. GUI project config panel (Project Code + Libraries sections)
6. Add-library dialog driven by a tapped index
7. User-global index config (`~/.scistack/config.toml`)
8. Stale lockfile detection on project open

Phases are ordered so that each phase produces something testable without requiring later phases.

---

## Phase 1 — `Constant` primitive

**Goal:** a `Constant` wrapper type that behaves transparently as its value, detectable via `isinstance`.

**Where it lives:** inside `scidb` — `scidb/src/scidb/constant.py`, re-exported from `scidb/__init__.py`. User imports as `from scidb import constant, Constant`.

**Deliverables:**
- `Constant` class with transparent value semantics (operator overloading, `__getattr__` passthrough for attribute access on the underlying value)
- `constant(value, description=...)` factory function
- Carries `description: str`, `source_file: str`, `source_line: int` for the sidebar (captured via `inspect` at construction time)
- Unit tests covering: scalar transparency (`SAMPLING_RATE_HZ + 1 == 1001`), container transparency (`DEFAULT_BANDPASS[0]`), `isinstance(x, Constant)` detection, description/location capture

**Not doing yet:** GUI integration. This phase is just the primitive + tests.

---

## Phase 2 — Discovery scanner

**Goal:** a pure Python function that, given a project folder, returns the full list of Variables / Functions / Constants in scope.

**Where it lives:** `scidb/src/scidb/discover.py`, re-exported from `scidb/__init__.py`.

**Deliverables:**
- `scidb.discover.scan_project(project_root: Path) -> DiscoveryResult` function
- Walks `src/{project_name}/` by importing modules (not AST parsing — we need the real runtime objects)
- Walks every package in `uv.lock` that's importable in the current environment
- Runs `discover(module)` on each (see design doc for the algorithm)
- Returns a structured result: `{project_code: {variables, functions, constants}, libraries: {lib_name: {variables, functions, constants}}}`
- Skips libraries with zero exports (filter at the panel layer, but the scanner can still return them so the panel can choose)
- Records import errors per module without aborting the whole scan — each module result is either `Ok(exports)` or `Err(traceback)`

**Tests:**
- Fixture project with known variables/functions/constants under `src/`
- Fixture library (installed in the test env) with known exports
- Test module with deliberate `ImportError` — verify the error is captured, not thrown
- Test library with zero scistack exports — verify it's returned empty (panel filtering happens later)

**Depends on:** Phase 1 (needs `Constant` to detect constants).

---

## Phase 3 — Project scaffolder

**Goal:** `scistack project new {name}` CLI command (and library function) that creates the full project folder layout.

**Project name rule:** 1:1 project name → package name. The project name must be a valid Python package identifier (lowercase letters, digits, underscores; must start with a letter). Scaffolder validates this up front and rejects invalid names with a clear error. No auto-conversion (no snake_case munging of `My Study` → `my_study`) — force the user to pick a valid name to avoid later confusion about which name is which.

**Deliverables:**
- Function `scaffold_project(parent_dir: Path, name: str, schema_keys: list[str]) -> Path`
- `validate_project_name(name: str) -> None` helper that raises a clear error for invalid names
- Creates:
  - `.scistack/project.toml` with scistack version, creation date
  - `.scistack/snapshots/` (empty directory)
  - `pyproject.toml` pre-filled with the baseline below
  - `src/{name}/__init__.py` (name is used verbatim, no conversion)
  - `.gitignore` (excludes `data/`, `*.duckdb`, `*.duckdb.wal`, `.venv/`)
  - `README.md` stub
  - `{name}.duckdb` configured with the provided `schema_keys` via scidb's `configure_database`
- Runs `uv sync` to create `uv.lock` and the venv
- CLI wrapper: `scistack project new`

**Baseline `pyproject.toml` contents:**
```toml
[project]
name = "{project_name}"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "scidb",
]

[dependency-groups]
dev = [
    "scistack-gui",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/{project_name}"]
```

Rationale: `scidb` transitively pulls in `thunk` (scilineage), `sciduckdb`, `scipathgen`, `canonicalhash`, `scirun`, so the user gets `BaseVariable`, `@lineage_fcn`, and the db layer for free. `scistack-gui` is a dev dep because it's a tool for working with the project, not a runtime dep of pipeline code. No numpy/scipy/pandas baked in — those come from user libraries.

**Tests:**
- Scaffold a fresh project in a tmpdir; verify all files exist
- Verify `pyproject.toml` parses and lists the baseline deps
- Verify invalid project names are rejected (`"My Study"`, `"1study"`, `"my-study"`, `""`)
- Verify the `.duckdb` has the correct schema keys (load it back and check)
- Verify `uv sync` actually ran (check for `uv.lock`)

**Depends on:** `uv` being available on the system (document as a prerequisite).

---

## Phase 4 — `uv` integration layer

**Goal:** a thin Python wrapper around `uv` CLI operations that the GUI and scaffolder can share.

**Deliverables:**
- `scistack.uv_wrapper` module with:
  - `sync(project_root: Path) -> SyncResult` — runs `uv sync`, captures stdout/stderr, returns success/error
  - `add(project_root: Path, package: str, version: str | None, index: str | None) -> AddResult`
  - `remove(project_root: Path, package: str) -> RemoveResult`
  - `read_lockfile(project_root: Path) -> list[LockedPackage]` — parses `uv.lock` and returns installed packages
  - `is_lockfile_stale(project_root: Path) -> bool` — compares `pyproject.toml` mtime or hash vs `uv.lock`
- All functions shell out to `uv`; no direct interaction with uv's internals
- Errors from uv are captured and surfaced, not thrown

**Tests:**
- Integration tests that actually invoke `uv` in a tmpdir (mark as requiring uv)
- Mock-based tests for error handling (uv returns non-zero, parses error output correctly)

**Depends on:** nothing. Can be developed in parallel with Phase 1–3.

---

## Phase 5 — User-global index config

**Goal:** tapped indexes live in `~/.scistack/config.toml`, readable/writable by the GUI.

**Deliverables:**
- `scistack.user_config` module:
  - `load_config() -> UserConfig` — reads `~/.scistack/config.toml`, creates empty if missing
  - `add_tap(url: str, name: str | None = None)` — adds a GitHub repo URL as a tapped index
  - `remove_tap(name_or_url: str)`
  - `list_taps() -> list[Tap]`
- Each tap records `{name, url, local_clone_path}` — local clones live under `~/.scistack/taps/{name}/`
- `refresh_tap(name)` — git pulls the tap to get latest package metadata

**Not doing yet:** the package metadata format inside a tap. Defer until Phase 7 (add-library dialog) needs it — for now, a tap is just a directory with a `packages.toml` listing available packages and their index URLs.

**Tests:**
- Create/read config roundtrip
- Add/remove tap operations
- Git clone/pull handling (mark as requiring network)

**Depends on:** nothing.

---

## Phase 6 — GUI project config panel

**Goal:** the two-section panel (Project Code + Libraries) visible in the GUI.

**Deliverables:**
- New FastAPI endpoints in `scistack-gui/scistack_gui/api/`:
  - `GET /project/code` — runs the discovery scanner on `src/{project}/`, returns per-module exports + errors
  - `GET /project/libraries` — reads `uv.lock`, runs discovery per installed package, filters out zero-export packages, returns the rest
  - `POST /project/refresh` — re-runs both scans
- New React component: `ProjectConfigPanel` with Project Code and Libraries sections
- Wire into the existing Refresh button (per design decision — no file watcher)
- Import errors shown inline with a "view traceback" expander

**Tests:**
- Backend: endpoint tests with a fixture project
- Frontend: component renders with mocked API responses; error state renders

**Depends on:** Phase 2 (scanner), Phase 4 (lockfile reading).

---

## Phase 7 — Add-library dialog

**Goal:** users can browse a tapped index and install a library into the current project.

**Deliverables:**
- Backend endpoints:
  - `GET /indexes` — lists user's tapped indexes
  - `GET /indexes/{name}/packages?q={query}` — searches a tap's package list
  - `POST /project/libraries` — body `{name, version, index}` → calls `uv add` via the wrapper → returns success or uv's error
- Frontend `AddLibraryDialog` component with index selector, search, version picker, Install button
- On success: trigger `/project/refresh` so the sidebar picks up new exports
- On failure: show uv's error verbatim (per design)

**Depends on:** Phase 4 (uv wrapper), Phase 5 (index config), Phase 6 (panel to add the button to).

---

## Phase 8 — Stale lockfile handling

**Goal:** on project open, detect a stale `uv.lock` and run `uv sync` silently, surfacing errors visibly.

**Deliverables:**
- On project-open flow (GUI startup): call `is_lockfile_stale`; if stale, call `sync`
- On sync failure: show a blocking error dialog with uv's output; don't proceed until resolved
- On sync success: proceed silently, scanner picks up any changes

**Depends on:** Phase 4 (uv wrapper), Phase 6 (GUI infrastructure to surface errors).

---

## Deliberately deferred

- **Snapshot creation UI** — format reserved, implementation left for a follow-up plan
- **Project fork tooling** — rare case, manual for now
- **Library extraction tooling** — defer until real usage patterns are visible
- **Polyglot discovery** — Python-only scope for this plan
- **File watcher for auto-rescan** — existing Refresh button is enough for now

---

## Decisions settled (2026-04-09)

1. **`Constant` lives inside `scidb`** — `scidb/src/scidb/constant.py`, re-exported from `scidb/__init__.py`. Same for the discovery scanner (`scidb/discover.py`).
2. **Baseline `pyproject.toml` deps** — `scidb` only as a runtime dep; `scistack-gui` as a dev dep; `requires-python = ">=3.12"`. See Phase 3 for the full template.
3. **Project-name → package-name rule** — 1:1, no conversion. Project names must be valid Python identifiers (lowercase letters, digits, underscores, starting with a letter); invalid names are rejected at scaffold time.

## Still to settle

- **Tap package metadata format** — the file inside a tapped GitHub repo listing available packages. Defer until Phase 7 starts; no blocker for earlier phases.
