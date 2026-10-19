# Project Files

A SciStack project has two configuration files, both plain text in [TOML](https://toml.io/) format:

| File | Where | What it holds |
|---|---|---|
| **`scistack.toml`** | The project root (the folder you open in VS Code) | *How the project is set up*: where its code is, which libraries it uses, how levels are ordered and labelled in figures. |
| **`scistack_entities.toml`** | `src/<project>/scistack_entities.toml` (or `src/scistack_entities.toml`) | *What the project declares*: its variables, parameters and input file paths. |

Both are created for you by `scistack init` or when the GUI creates a database (see [Create New Project](create-new-project.md)). Both are read by Python, MATLAB and the GUI alike, and both are safe to edit by hand: SciStack re-reads them on every scan, with no restart needed. Commit them to version control along with your code.

<!-- Ground truth (verified 2026-10-18):
     scistack.toml: name/location scifor.discovery.CONFIG_FILENAME/config_path_at;
     the ONE writer scidb/config_file.py (TOP_LEVEL_ORDER, MATLAB_ORDER, TABLE_ORDER);
     GUI reader scistack_gui/config.py load_config; db -> scidb/inspect/cli.py
     _config_db; packages -> scidb/discover.py _configured_packages + scidb/names.py
     library_packages; copied_libraries -> scidb.names.COPIED_LIBRARIES_KEY, written by
     config.mark_library_copied; glue_dir default src/scistack_glue (config.set_glue_dir);
     [schema_keys] scidb.schema_order; [aliases] scidb.aliases; [colors] scidb.colors;
     [matlab] variable_dir also scimatlab/stubs.py (default scistack_matlab_variables
     beside the entities file). Entities: scidb/entities.py (grammar, [library] table,
     empty-array parameter, list of path_inputs = alternatives), resolution
     scidb.entities.resolve_entities_path, default location default_entities_relpath.
     Internal notes: docs/claude/config-file-formats.md, entities-toml-format.md. -->

## TOML in two minutes

```toml
# A comment runs to the end of the line.
name = "text"                 # a string, always in quotes
count = 3                     # a number
enabled = true                # true / false
items = ["a", "b", "c"]       # an array (list)
point = { x = 1, y = 2 }      # an inline table (a small dictionary)

[section]                     # a table header: every key below it,
key = "value"                 # up to the next header, belongs to [section]
```

The rule that trips people up: **a key written below a `[section]` header belongs to that section.** Top-level keys must come *before* the first header.

Paths in either file are relative to the project root unless they're absolute. Write them with forward slashes (`src/my_study/pipeline.py`), which work on Windows, macOS and Linux. SciStack also reads backslashes correctly on any system, but an absolute Windows path such as `D:\data` cannot work on a Mac, and SciStack warns about these when it opens the project.

---

## `scistack.toml`

A complete example, with every setting:

```toml
modules = ["."]
entities_file = "src/my_study/scistack_entities.toml"
glue_dir = "src/scistack_glue"
packages = ["lab_emg_tools"]
copied_libraries = []
auto_discover = true
db = "data/my_study.duckdb"

[matlab]
sources = ["."]
functions = []
variables = []
variable_dir = "src/scistack_matlab_variables"

[schema_keys]
session = ["BL", "POST", "FU"]

[aliases.session]
name = "Session"
levels = { "BL" = "Baseline", "POST" = "Post-training", "FU" = "Follow-up" }

[colors]
default = "#333333"

[colors.session]
"BL" = "#0072b2"
"POST" = "#d55e00"
```

**Every setting is optional.** An empty `scistack.toml` is valid. With no `scistack.toml` at all, SciStack scans the whole project folder for code.

/// warning | The GUI rewrites this file
When you change a setting in the GUI (adding a path, a library, an alias or a colour), SciStack writes the whole file again. Every setting is kept, including ones it doesn't recognise, but **comments are not**. Keep notes about your configuration somewhere else, such as a README.
///

/// note | Only this file
SciStack reads its configuration only from `scistack.toml` in the project root. It doesn't look in parent folders, and a `[tool.scistack]` table in `pyproject.toml` is ignored. `pyproject.toml` is only packaging information; SciStack reads just its `[project] name`, to find your project's own package under `src/<name>/`.
///

### Where your code is

These settings tell SciStack which files to scan for variables, parameters and functions. Scanning reads the files without running your pipelines.

#### `modules`

*Array of paths. Default: `[]`.*

The Python code to scan. Each entry can be:

- a `.py` file: `"src/pipeline.py"`;
- a folder, scanned recursively: `"analysis/"`, or `"."` for the whole project;
- a glob pattern: `"extras/**/*.py"`.

Your project's own package (`src/<name>/`, when `pyproject.toml` names one) is always loaded as a package, whether or not it's listed. Test files (`test_*.py`, `tests/` folders) are never scanned. A folder or pattern that matches nothing is logged as a warning, not treated as an error.

**Used by:** GUI discovery (what appears on the canvas and in the function list). **Written by:** `scistack init` (`["."]`), and the GUI's **📁 Paths** popup when you add or remove a folder.

#### `entities_file`

*Path. Default: see below.*

Which [entities file](#scistack_entitiestoml) this project uses. How SciStack decides:

1. If `entities_file` names a file, that file is used.
2. If it's set to `""` (empty), the project has **no** entities file, even if one exists at the default location.
3. Otherwise, `src/scistack_entities.toml` is used if it exists.

**Used by:** `scidb.entities` in Python, `scidb.entities()` in MATLAB, and the GUI. **Written by:** `scistack init`, and the GUI when it creates the file (the first time you create a variable, parameter or file input on the canvas). New files go inside your project's package (`src/<name>/scistack_entities.toml`) so they ship with it; without a package, at `src/scistack_entities.toml`.

#### `glue_dir`

*Folder path. Default: not set (the GUI creates `src/scistack_glue/` when needed).*

Where the GUI keeps **glue** functions, the small reshaping steps you write in the code panel between two other steps. One file per glue function. The folder is scanned for `.py` and `.m` files.

**Used by:** GUI discovery. **Written by:** the GUI, the first time you create a glue function.

#### `variable_file`

*Path to a `.py` file. Default: not set.*

A Python module holding variable and parameter declarations (`class X(BaseVariable)`, `X = Parameter(...)`). It's scanned like any module in `modules`, even if no `modules` entry covers it. The GUI never writes to it: new declarations go in the entities file.

#### `[matlab]`

The MATLAB counterparts. Only needed for projects with MATLAB code.

| Key | Type | Purpose |
|---|---|---|
| `sources` | Array of paths | `.m` files and folders to scan. Each file is classified automatically: a `classdef` of `scidb.BaseVariable` is a variable; a `function` file is a function. `scistack init` writes `["."]`; the 📁 Paths popup adds to it. |
| `functions` | Array of paths | `.m` files or patterns that are always treated as functions. |
| `variables` | Array of paths | `.m` files or patterns that are always treated as variable `classdef`s. |
| `variable_dir` | Folder path | Where MATLAB `classdef` files are generated for variables declared in the entities file. MATLAB needs a real `classdef` file for each variable, so SciStack writes a short one per variable, once, and never deletes it. Default: a `scistack_matlab_variables` folder next to the entities file. |
| `entities_file` | Path to a `.m` script | A MATLAB script that declares parameters and file inputs. This is a different key from the top-level `entities_file` (which is TOML). |

The folders of all these entries are added to the MATLAB path when SciStack runs MATLAB code; there's no separate `addpath` setting.

**Used by:** the GUI's MATLAB discovery and the commands it generates to run MATLAB steps; `scimatlab` (for `variable_dir`).

### Libraries

A **library** is an installed Python package of reusable SciStack functions (and possibly shared pipelines), such as your lab's analysis toolkit. See [Writing Functions](writing-functions.md#reusable-code-and-project-specific-code).

#### `packages`

*Array of import names. Default: `[]`.*

Installed packages to scan, by their import name (`"lab_emg_tools"`, not a path). Their functions appear in the GUI, and their functions are recorded in history under the library's name (`lab_emg_tools.lowpass_envelope`), so a function with the same name in your own code can't be confused with it.

The package must be installed in the project's virtual environment (`pip install ...`). **Used by:** GUI discovery, `scidb.scan_project`, and function naming in history. **Written by:** the GUI's library panel.

#### `copied_libraries`

*Array of import names. Default: not set.*

Libraries whose source you've copied into your own project's package (the GUI's **Make my own copy**). Their functions keep their library names in history, so results computed before the copy stay current. **Written by:** the GUI, which also removes the library from `packages` at the same time.

#### `auto_discover`

*`true` or `false`. Default: `true`.*

Also scan any installed package that advertises itself to SciStack. A library opts in with an entry point in its own `pyproject.toml`:

```toml
# In the LIBRARY's pyproject.toml, not your project's
[project.entry-points."scistack.plugins"]
emg_tools = "lab_emg_tools.filters"
```

Set `auto_discover = false` to load only the libraries listed in `packages`, for example if an installed library's names clash with yours. A package found both ways is scanned once.

### The database

#### `db`

*Path to a `.duckdb` file. Default: not set.*

The default database for the [`scidb` command-line tool](../api/scidb.md#command-line), so `scidb status` works without `--db`. A relative path is relative to the project root. The tool looks for a database in this order: the `--db` flag, the `SCIDB_DATABASE` environment variable, this setting, then a single `.duckdb` file in the current folder.

**Used by:** the `scidb` CLI only. Your scripts still name their database in `configure_database(...)`, and the GUI asks which database to open.

Wherever the database is named, keep the file on a local disk and back it up: see [Keep the project on a local disk](installation.md#keep-the-project-on-a-local-disk).

### How figures and tables show your data

These three tables affect only how data is **displayed**: the order of levels, their names, and their colours. They never change what is stored or computed. Edits take effect on the next table or figure, without a restart.

#### `[schema_keys]`: level order

```toml
[schema_keys]
session = ["BL", "POST", "FU"]
speed   = ["SLOW", "SSV", "FAST"]
```

By default a schema key's values are sorted alphabetically, so sessions come out as `BL, FU, POST`. List a key here to fix its order. Values you list come first, in your order; any others follow in the default order, so a session added later still appears. Keys you don't list are unaffected.

Values are matched as text, so `"01"` matches `"01"` and not `1`. A key that isn't one of the database's schema keys is warned about in `scidb.log`.

**Used by:** loaded DataFrames (`load(as_df=True)`, CSV exports), `for_each` iteration order, the GUI's location picker, plot axes, facets and legends, and code exported from Plot Studio (`order=`, `hue_order=`, ...). **Written by:** you; the GUI keeps it but never edits it.

#### `[aliases]`: display names

```toml
[aliases.session]                 # one table per thing being renamed
name = "Session"                  # how the thing itself is labelled

[aliases.session.levels]          # how each of its values is labelled
"BL" = "Baseline"
"POST" = "Post-training"

[aliases."Demographics.Sex"]      # a variable's column: quote names with dots
name = "Sex"
levels = { "F" = "Female", "M" = "Male" }
```

A "thing" can be a schema key, a variable, a variable's column (`"Var.Column"`), or a figure factor such as `ColName` or `Variant`. Level keys are the values as text, always in quotes. Figures show the aliases in titles, axes and legends; each saved plot can override them. Unknown names and two levels with the same alias are warned about in `scidb.log`; a malformed entry is skipped.

**Used by:** Plot Studio and Stat Studio, and their exported code. **Written by:** you, or the GUI's label editor in Plot Studio.

#### `[colors]`: mark colours

```toml
[colors]
default = "#333333"               # the colour of a figure with nothing to colour by

[colors.session]                  # one table per thing, keyed by level text
"BL" = "#0072b2"
"POST" = "#d55e00"
```

Fixes the colour of each level wherever that thing colours a figure, so Baseline is the same blue in every plot. A string directly under `[colors]` is a setting (only `default` exists); a table is a thing, named as in `[aliases]`. Colours are hex codes (`#rrggbb` or `#rgb`), `rgb(r, g, b)`, or named colours such as `red` or `tab:blue`. Transparency isn't accepted here. A plot's own colour choices take precedence over these.

**Used by:** Plot Studio and exported plot code. **Written by:** you, or the GUI's colour picker.

### Finding out what SciStack read

Every read is logged to `scidb.log` in your database's folder. Search it for:

| Search for | Shows |
|---|---|
| `[config]` | Which `scistack.toml` was loaded and what each setting contributed. |
| `[schema_order]` | Which config supplied level order, and for which keys. |
| `[aliases]`, `[colors]` | What was read, and any warnings. |
| `[config_file] wrote` | Each time SciStack rewrote the file, and which keys it held. |

In the GUI, the 📁 **Paths** popup shows the project root SciStack settled on and the `scistack.toml` it is using.

---

## `scistack_entities.toml`

The entities file declares your project's **variables**, **parameters** and **file inputs** by name, in one place that Python, MATLAB and the GUI all read. When you create one of these on the GUI canvas, this is the file it's written to. It's also the easiest place to declare them by hand.

```toml
# Variables come first: a top-level key below a [section] header
# would belong to that section.
variables = ["RawEMG", "EmgEnvelope", "PeakEMG"]

[parameters]
FS_HZ        = 1000                       # one value
CUTOFF_HZ    = [10, 20, 30]               # three values: three variants
SUBJECTS     = ["01", "02"]               # strings stay strings: "01", not 1
FILTER_ORDER = []                         # declared, no value yet
CONFIG       = { order = 4, kind = "low" }  # a dictionary is one value
WINDOW       = [[0, 100]]                 # one value that is itself a list

[path_inputs]
EMG_FILE = "{subject}/{session}_emg.csv"
RAW_FILE = { template = "{subject}/raw.csv", root_folder = "D:/study/raw" }
```

### `variables`

*Top-level array of names.*

Each name declares a variable, exactly like `class RawEMG(BaseVariable): pass` in Python or a one-line `classdef` in MATLAB. The name is the variable's database table.

`variables` must be **above** the first `[section]` header. If it ends up below `[parameters]`, TOML would read it as a parameter named `variables`; SciStack detects that and reports it rather than silently losing your variables.

For MATLAB, SciStack generates a small `classdef` file for each name in `[matlab] variable_dir` (see [above](#matlab)), because MATLAB needs one per class.

### `[parameters]`

*One entry per parameter: `NAME = value`.*

Each entry declares a `Parameter`: a named setting you pass to functions as an input. What the value means:

| Value | Meaning |
|---|---|
| `1000`, `"low"`, `true` | One value. |
| `[10, 20, 30]` | **Three alternative values.** A step using it runs once per value, and each result is kept as its own variant. An array always means alternatives. |
| `[[0, 100]]` | One value that is a list. Wrap it in a second array. |
| `{ order = 4, kind = "low" }` | One value that is a dictionary (a MATLAB struct). |
| `[]` | Declared, but with no value yet. This is what the GUI writes when you create a parameter. A step that uses it refuses to run until you give it a value. |

Values are taken exactly as written: `"01"` is the text `"01"`, never the number `1`.

### `[path_inputs]`

*One entry per file input: `NAME = template` or `NAME = { template, root_folder }`.*

Each entry declares a [`PathInput`](../api/scifor.md#input-wrappers): a template that builds a file path for each location. `{subject}` and `{session}` are filled in with the current location's values.

| Form | Meaning |
|---|---|
| `"{subject}/{session}_emg.csv"` | A template relative to the project root. |
| `{ template = "...", root_folder = "D:/study/raw" }` | A template inside another folder, such as a data drive. These are the only two fields allowed. |
| `["{subject}/a.csv", "{subject}/b.csv"]` | Alternatives: a step using it runs once per template. |

### Using the declarations

/// tab | Python
```python
from scidb import entities

entities.CUTOFF_HZ      # Parameter(10, 20, 30)
entities.EMG_FILE       # PathInput('{subject}/{session}_emg.csv')

for_each(lowpass_envelope,
         inputs={"signal": RawEMG, "fs_hz": entities.FS_HZ, "cutoff_hz": entities.CUTOFF_HZ},
         outputs=[EmgEnvelope], subject=[], session=[])
```
///

/// tab | MATLAB
```matlab
e = scidb.entities();          % a struct of every declaration
scidb.for_each(@load_emg, struct("path", e.EMG_FILE), {RawEMG()}, ...
    subject=["S01", "S02"], session=["BL", "POST"]);

scidb.entities();              % or, with no output: define each name in the workspace
```
///

The file is re-read whenever it changes, so an edit in the GUI is visible to the next call.

### What the entities file can't express

Some declarations need code. These stay in a Python (or MATLAB) file listed in `modules`. They're still discovered and shown in the GUI, but the GUI won't edit them:

| Needs code | Example |
|---|---|
| A parameter description | `Parameter(30, description="window, seconds")` |
| A computed value | `Parameter(*range(10, 60, 10))` |
| A variable with [custom storage](writing-functions.md#saving-your-own-classes) or a docstring | `class X(BaseVariable)` with `to_db`/`from_db`, or a MATLAB `classdef X < scidb.BaseVariable` with both methods |
| PathInput matching options | `aliases=`, `key_regex=`, `regex=` |

If the same name is declared both in code and in the entities file, the GUI shows the entities file's version.

### When something is wrong

A bad entry (an invalid name, an unknown field in a `[path_inputs]` table, the same name declared twice) is skipped and reported with its name and line number. Every other entry still loads. The GUI lists these in its load-errors panel; in MATLAB, `scidb.entities()` raises a warning for each.

### In a library: `[library]`

A library package can have its own entities file. If it includes shared pipelines, a `[library]` table names the schema keys those pipelines are written for:

```toml
[library]
schema_keys = ["subject", "session"]
```

When you add the library's pipeline to a project with different keys (say `participant` and `visit`), the GUI uses this to suggest which key maps to which. Project entities files don't need it.
