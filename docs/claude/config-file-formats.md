# Config File Formats: `pyproject.toml` vs `scistack.toml`

SciStack GUI reads its project configuration from **one** of two TOML files. They are functionally equivalent — the same fields, same defaults, same behavior. The only difference is where the keys live in the file.

## `pyproject.toml` — nested under `[tool.scistack]`

The scistack section is nested inside the standard Python project file:

```toml
[project]
name = "my_study"
version = "0.1.0"
dependencies = ["scidb"]

[tool.scistack]
modules = ["src/my_study/pipeline.py"]
entities_file = "src/scistack_entities.toml"
packages = ["lab_shared_utils"]
auto_discover = true

[tool.scistack.matlab]
functions = ["matlab/functions/*.m"]
variables = ["matlab/types/*.m"]
variable_dir = "matlab/types"
```

Use this when your project already has a `pyproject.toml` (the common case for Python projects managed with uv, pip, or hatch).

## `scistack.toml` — top-level keys

The entire file IS the scistack config. No `[tool.scistack]` nesting needed:

```toml
modules = ["src/my_study/pipeline.py"]
entities_file = "src/scistack_entities.toml"
packages = ["lab_shared_utils"]
auto_discover = true

[matlab]
functions = ["matlab/functions/*.m"]
variables = ["matlab/types/*.m"]
variable_dir = "matlab/types"
```

Use this when:
- The project doesn't have a `pyproject.toml` (e.g. a pure MATLAB project)
- You want scistack config in a separate file for clarity

## Mapping between formats

| `pyproject.toml` key | `scistack.toml` key |
|---|---|
| `[tool.scistack].modules` | `modules` |
| `[tool.scistack].entities_file` | `entities_file` |
| `[tool.scistack].variable_file` | `variable_file` |
| `[tool.scistack].packages` | `packages` |
| `[tool.scistack].auto_discover` | `auto_discover` |
| `[tool.scistack.matlab].functions` | `[matlab].functions` |
| `[tool.scistack.matlab].variables` | `[matlab].variables` |
| `[tool.scistack.matlab].variable_dir` | `[matlab].variable_dir` |
| `[tool.scistack.matlab].entities_file` | `[matlab].entities_file` |

## The three entity-declaration keys

Easy to confuse, so stated once. Full detail in `entities-toml-format.md`.

| Key | Format | Written by the GUI? |
|---|---|---|
| `entities_file` | TOML | **Yes — the only one.** Default `src/scistack_entities.toml` |
| `variable_file` | `.py` | No. Read-only legacy; folded into `modules` so its declarations stay discovered |
| `[matlab] entities_file` | `.m` script | Read-only in principle; still writable pending a decision |

A top-level `entities_file` (TOML, language-neutral) and a `[matlab]
entities_file` (a MATLAB script) are different keys in different tables.

## Which directory is "the project"?

**One resolver answers this for readers and writers alike:**
`config.resolve_project_root(project_path, db_path)`. Everything — locating
the config file, folder-scan discovery, where a new `scistack.toml` and
`src/scistack_entities.toml` get written — hangs off its answer.

**The project root is the folder the user opened.** Most to least
authoritative, logging which rule fired:

1. An explicit `--project`/`--module` argument — the user naming a project
   outright. A file means its containing directory.
2. `--project-root`, i.e. the VS Code workspace folder (passed by
   `extension/src/pythonProcess.ts`).
3. The current working directory (browser/CLI). The extension also spawns
   the server with `cwd` set to the workspace folder, so rules 2 and 3
   agree there.
4. The database's own directory, as a last resort, with a WARNING.

**The database's location does not decide the project** (except as rule 4).
A `.duckdb` usually lives in a datasets folder that has nothing to do with
the code.

Then `config.locate_config_at(root)` looks for `pyproject.toml`, then
`scistack.toml`, **in that directory only**.

**Key rule**: `pyproject.toml` always wins over `scistack.toml` in the same
directory.

### Why there is no upward walk any more

Until 2026-09-01 there were *two* resolvers: writing used
`infer_project_root` (which consulted `--project-root`), while reading used
`_locate_pyproject`, which walked upward from the **database** directory.
With a database on `C:\Users\...\Datasets` and a project on
`Y:\LabMembers\...`, that walk could never reach the project — so the GUI
wrote a `scistack.toml` it was structurally incapable of reading back, then
folder-scanned the datasets directory and reported `0 .py, 0 .m` forever.
`add_path` used the same db-rooted lookup to decide "is this the first
write?", so it also re-seeded from scratch every time, silently discarding
previously added paths.

Searching upward from the *project* root was considered and rejected: a
stray `pyproject.toml` in a parent directory — entirely plausible on a
shared network drive — would capture every project beneath it. The cost is
that opening a **subfolder** of a packaged repo no longer finds the config
above it; open the repo root instead. The resolved root is logged on every
load and returned by `describe_managed_paths` (`project_root`,
`config_path`), so the Paths popup can show which folder it settled on.

`scifor.discovery.find_project_config` (the upward walk) still exists and is
still used by `scidb.entities`, which walks from `cwd` — a correct anchor.

### First write

The first `add_path`/`set_entities_file` on a project with no config seeds
`modules` with **both** the database's directory and the project root.
Creating a config file switches discovery from folder-scan to config-driven,
so seeding only the new root would silently stop discovering code the folder
scan was already finding.

Under pytest, rule 3 would make the repo itself the project root — the GUI
test suite pins the hint to `tmp_path` in an autouse fixture
(`scistack-gui/tests/conftest.py::_pin_project_root`). That pin now governs
config *reading* and folder-scan discovery too, not just where files land.

## `[schema_keys]` — declared level order

The one table in this file that scidb reads and the GUI never writes.

```toml
# scistack.toml
[schema_keys]
session = ["BL", "POST", "FU"]
speed   = ["SSV", "FAST"]

# pyproject.toml
[tool.scistack.schema_keys]
session = ["BL", "POST", "FU"]
```

A schema key's levels have no inherent order: `session` is chronological to the
person who ran the study and alphabetical to everything else, so without this
every axis and every table reads `BL, FU, POST`. Declared levels come first, in
the declared order; anything not named is appended in whatever order sorted it
before, so a level collected after the file was written shows up at the end
rather than disappearing. An undeclared KEY is untouched entirely.

Values are matched as text, so `"01"` stays `"01"` — which spelling is identity
is `schema_key_types`' decision (`docs/claude/schema-key-types.md`), never this
table's.

One reader owns it, `scidb.schema_order`, and every display surface asks it:
DataFrame row order, plot factor levels, the GUI's level lists, the location
picker's tree. A key named here that is not a schema key of the dataset is
WARNED about when the database opens — otherwise a typo is completely silent.

**The GUI round-trips this table without understanding it.** `add_path` and
friends rewrite the whole file from the fields they know, so the table is
carried across verbatim and emitted last (a TOML table swallows every key after
it). Tests pin both halves.

## All fields are optional

Every config field has a sensible default. An empty `scistack.toml` (or a `pyproject.toml` with an empty `[tool.scistack]` section, or even a `pyproject.toml` with no `[tool.scistack]` at all) produces a valid config:

| Field | Default | Effect when omitted |
|---|---|---|
| `modules` | `[]` | No local `.py` files loaded explicitly (project source scanner still walks `src/{name}/`). Entries can be files, directories (recursive), or glob patterns. |
| `entities_file` | not set | Auto-created as `src/scistack_entities.toml` in the project root the first time an entity is created from the GUI (or eagerly at project creation) |
| `variable_file` | not set | No legacy `.py` entities file; nothing to fold into `modules` |
| `packages` | `[]` | No extra pip-installed packages scanned |
| `auto_discover` | `true` | `scistack.plugins` entry points are scanned |
| `matlab.functions` | `[]` | No MATLAB `.m` function files loaded |
| `matlab.variables` | `[]` | No MATLAB `.m` classdef files loaded |
| `matlab.variable_dir` | not set | "Create Variable" cannot generate MATLAB classdef files |

**Note**: The MATLAB path (`addpath` directories) is auto-derived from the parent directories of `matlab.functions`, `matlab.variables`, and `matlab.variable_dir`. There is no explicit `matlab.addpath` config field.

## `modules` accepts files, directories, and globs

Each entry in `modules` can be:

- **A `.py` file path**: loaded directly.
- **A directory**: all `.py` files under it are discovered recursively.
- **A glob pattern** (contains `*`, `?`, or `[`): expanded, but only `.py` matches are kept.

These can be mixed freely:

```toml
modules = [
    "pipeline.py",           # single file
    "lib/",                   # all .py files under lib/, recursively
    "extras/**/*.py",         # glob pattern
]
```

An empty directory or a glob that matches no `.py` files logs a warning but is not an error.

## `auto_discover` and `scistack.plugins` entry points

When `auto_discover = true` (the default), SciStack scans all installed Python packages for `scistack.plugins` entry points at startup. This is how shared library packages make their pipeline code visible without every project needing to list them explicitly.

### What makes a package auto-discoverable

A pip-installed package must declare a `scistack.plugins` entry point in its own `pyproject.toml`. This uses the standard Python entry point syntax (PEP 621) — it looks verbose, but it's the same mechanism used by pytest plugins, Flask extensions, etc., and only needs to be written once per library:

```toml
[project.entry-points."scistack.plugins"]
my_filters = "my_package.filters"
```

- **Left side** (`my_filters`): an arbitrary name used for logging. Multiple entry points per package are allowed by using different names.
- **Right side** (`"my_package.filters"`): a dotted Python module path. SciStack imports this module (via `importlib.metadata.entry_points(group="scistack.plugins")`) and scans it for `BaseVariable` subclasses, `@lineage_fcn`-decorated functions, `Constant` instances, and other top-level callables.

A package that does **not** declare this entry point will never be auto-discovered, even if it contains scistack-compatible code. To load such a package, list it explicitly in `packages`.

### When to set `auto_discover = false`

Auto-discovery is almost always what you want. Set it to `false` only if:
- You need full control over which packages are loaded (e.g. to avoid name collisions from a library you installed but don't want active)
- Startup time is a concern and you have many installed packages with entry points

### `packages` vs `auto_discover`

| Mechanism | Who declares it | Where it's configured |
|---|---|---|
| `packages = [...]` | The **project** author lists packages to scan | Project's `scistack.toml` or `pyproject.toml` |
| `auto_discover = true` | The **library** author declares an entry point | Library's own `pyproject.toml` |

Both can be used together. If the same package appears via both `packages` and auto-discovery, it is only scanned once.

## Edge cases

- **Empty `scistack.toml`**: Valid. Parses as `{}`, uses all defaults.
- **`pyproject.toml` without `[tool.scistack]`**: Valid. Uses all defaults. (This was previously a `ValueError` but was fixed to be more forgiving — many projects have a `pyproject.toml` for packaging but haven't added `[tool.scistack]` yet.)
- **No config file at all**: not an error. `load_config` falls back to folder-scan discovery rooted at the project root (`_folder_scan_config`), and `services.project_init_service.ensure_project_files` creates a `scistack.toml` + entities file at open/create time. This is server-side for both front ends; the VS Code extension used to pre-check and prompt for it in `projectInit.ts`, which was removed along with the "How should SciStack discover your pipeline code?" picker.
- **Both files in the same directory**: `pyproject.toml` is used; `scistack.toml` is ignored.

## Paths are cross-platform, in both directions

A config file is committed and shared: the same project gets opened on
Windows and on macOS. Both halves of that have to be handled, and both used
to be wrong (fixed 2026-09-10).

**Reading** — `scifor.discovery.resolve_config_path(root, raw)` is the single
place a raw config string becomes a `Path`. Backslash is treated as a
separator whenever the host is not Windows, because on POSIX `\` is a legal
*filename* character: `entities_file = "src\scistack_entities.toml"` written
by a Windows session does not error on macOS, it names a file called
`src\scistack_entities.toml` in the **project root**. The GUI created and
read exactly that file, and everything derived from its location moved with
it — MATLAB classdef stubs sit beside the entities file, so they went to
`<root>/scistack_matlab_variables` instead of
`<root>/src/scistack_matlab_variables`. That was the visible symptom.

`scistack_gui.config` routes every read through `_config_path` (Path form) or
`_config_pattern` (glob patterns, expanded as text); `scidb.entities`
resolution goes through the same scifor function. Nothing joins a raw string
onto the project root directly.

**Writing** — relative keys (`entities_file`, `glue_dir`) point *inside* the
project, so they must round-trip on another machine. They are recorded with
forward slashes (`config._portable_relpath`), which Windows accepts natively.
`str(WindowsPath("src/x.toml"))` is `src\x.toml`, which is how the broken
values got written in the first place.

**Windows-absolute values** (`Y:\LabMembers\...`, `\\server\share`) are a
different problem: there is no such drive on a Mac and nothing to guess.
`resolve_config_path` returns them unchanged rather than gluing them onto the
project root (which produced `/Users/me/project/y:\LabMembers\...` in every
error message), and `startup.check_windows_config_paths` records a
non-blocking `windows_config_paths` startup notice listing them so the user
can re-add them in 📁 Paths.

Absolute `modules`/`matlab.sources` entries are machine-specific by nature —
this makes them *fail legibly*, it does not make them portable.

## Implementation

- **Parser**: `scistack_gui/config.py` — `load_config()`, `resolve_project_root()`, `locate_config_at()`, `_extract_scistack_section()`, `_config_path()`
- **Cross-platform path reading**: `scifor/discovery.py` — `resolve_config_path()`, `normalize_config_separators()`, `is_windows_absolute()`
- **Config/entities-file creation**: `scistack_gui/services/project_init_service.py` — `ensure_project_files()`, called from `bootstrap.open_or_create_project()`
- **VS Code server argv**: `extension/src/serverArgs.ts` — `buildServerArgs()`; never passes `--module`/`--project`, so the project root is always `--project-root` (the workspace folder)
