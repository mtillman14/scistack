# Code Discovery: Functions, Submodules, Variables, Parameters, PathInputs

## Overview

The GUI's pipeline graph is built from **five** kinds of "things":
functions, variables, parameters, PathInputs, and submodules. All five are
either scanned from source directly, or have a working source-code
translation. This doc is the map of which mechanism applies to which, in
both Python and MATLAB.

**Constants and Sweeps were merged into one Parameter on 2026-08-25** (§3):
one class, one registry, one canvas node type, one sidebar tab. See
`docs/claude/entity-editability-model.md` (D6).

| Category | How it's found | Python source | MATLAB source |
|---|---|---|---|
| **Functions** | Scan source files/packages | `registry.py` / `scidb/discover.py` | `matlab_registry.py` + `matlab_parser.py` (regex) |
| **Variables** | Scan source files/packages | `BaseVariable` subclass (metaclass auto-register) | `classdef ... < ...BaseVariable` (regex) |
| **Parameters** | Scan source files/packages | top-level `scidb.Parameter(...)` binding (one or many values; IS an `EachOf`) | `scidb.Parameter(...)` in the entities script (regex) |
| **PathInputs** | Scan source files/packages | top-level `scidb.PathInput(...)` binding | top-level binding in the entities script (regex) |
| **Submodules** | GUI composition (`pipelineNode`) OR source (`scidb.Pipeline`) | `scidb.Pipeline`/`.use()`/`.bind()`, bidirectional (import + export) | export only (flattened into the script) |

**Discovered is not placed (2026-09-23).** A declared Parameter or PathInput
is always BUILT as a graph node (so a sidebar drop can graduate onto it),
but one with no run history and no GUI-added value carries
`data["declared_only"]` (`scope_filter.DECLARED_ONLY`, set in
`build_parameter_nodes` / `build_path_input_nodes`). `resolve_scope_view`
denies such a node the "unplaced → root canvas" default: it appears only
where it has a position, or when an edge already touches it. History-backed
nodes keep the default. Before this, a brand-new database opened with every
declared Parameter and PathInput strewn across the root canvas.

---

## 1. Functions — scanned from source

### Python

Two separate scanners exist, used in different contexts:

- **`scistack_gui/registry.py`** — powers the live GUI/VS Code server. For
  each configured source (see "Where Python looks," below), it imports the
  module and calls `inspect.getmembers(module, callable)`, keeping anything
  that:
  - does **not** start with `_`
  - has `obj.__module__ == module.__name__` (i.e. actually *defined* there,
    not merely imported/re-exported — so `from scidb import for_each` inside
    a pipeline file doesn't leak `for_each` in as a discovered function)

  No `@scistack` decorator is required in this path — any bare top-level
  callable qualifies.

- **`scidb/src/scidb/discover.py`** (`scan_project`) — used for packaged
  projects (walks `src/{project}/` + everything in `uv.lock`). Here a
  function must be explicitly tagged `@scistack` (`is_scistack_function`,
  `scidb/src/scidb/pipeline.py`) to be picked up.

### MATLAB

`matlab_parser.py` never imports/runs the `.m` file — it's a pure regex
parse over the file's text (comments/line-continuations stripped first):

```
_FUNCTION_RE = r"^\s*function\s+(?:\[out1,out2\]\s*=\s*|out\s*=\s*)?name\s*\(params\)"
```

Rules:
- A file containing **any** `classdef` is never scanned for a function — a
  `function` match inside it is always a class method, not a pipeline step
  (prevents test-suite setup helpers from being mis-registered).
- A function that happens to construct a `PathInput`/`Parameter` is just a
  function. (Until 2026-08-25 such a zero-arg function was a *value getter*,
  checked before the plain-function branch; that convention is gone — see
  §4.)
- The function name comes from the regex's name group; params/output names
  come from the bracket/parenthesis groups.
- Files can be explicitly declared (`[tool.scistack.matlab] functions =
  [...]`) or auto-classified during folder-scan (see below).

---

## 2. Variables — scanned from source

### Python

`BaseVariable` uses a metaclass (`BaseVariableMeta`) — **any** subclass
defined anywhere that gets imported registers itself into
`BaseVariable._all_subclasses` automatically, with no explicit call needed.
Discovery just needs to *import* the file; the metaclass does the rest.

### MATLAB

`parse_matlab_variable` regex-matches:

```
_CLASSDEF_RE = r"^\s*classdef\s+(\w+)\s*<\s*([\w.]+)"
```

and accepts it as a variable class if the parent name **ends with**
`BaseVariable` (covers both `BaseVariable` and `scidb.BaseVariable`). On
match, `matlab_registry._register_matlab_variable` creates a Python
*surrogate* class via `scimatlab.bridge.register_matlab_variable` so the DAG
builder can treat it like a real Python `BaseVariable` subclass.

---

## 3. Parameters — scanned from source

A **Parameter** is a named configuration value holding **one or more**
values, declared with `scidb.Parameter(...)`. It replaced the former
`scidb.constant()` (one value) and `scidb.Sweep()` (many) on 2026-08-25:
two constructs for one idea, which forced an entity to change *kind* the
moment a second value was added. See
`docs/claude/entity-editability-model.md` (D6).

```python
SAMPLING_RATE_HZ = scidb.Parameter(1000, description="Recording rate")
WINDOW_SECONDS   = scidb.Parameter(10, 20, 30)
THRESHOLDS       = scidb.Parameter(*range(10, 60, 10))   # plain varargs
```

`Parameter` **is** a `scifor.EachOf`, so `for_each` fans it out with no
special handling — one call per value. Crucially, a one-value Parameter is
**not** a special case: `EachOf` expansion has no branch for it, so
`Parameter(30)` records byte-identical `version_keys`/`call_id` to a bare
`30`. That is what makes "adding a value" purely additive — no change of
form, id, node, or history.

A single-valued Parameter keeps the transparent-proxy behaviour the old
`Constant` had (`5 * SAMPLING_RATE_HZ` works). A multi-valued one
deliberately does not: arithmetic on "10, 20 or 30" has no meaning, so it
raises. Use `.values` for the list, `.value` for the single one.

`__hash__` follows the same rule — a single-valued Parameter hashes AS its
value, so it is interchangeable with the raw value as a dict key or set
member. Hashing the 1-tuple instead would make `Parameter(42) == 42` true
while their hashes differed, silently breaking every hash-based lookup.

### Python

Discovery (`_scan_module_parameters` in `registry.py`, mirrored by
`discover.is_parameter`) walks `vars(module).items()` and keeps any
non-`_`-prefixed name bound to a `Parameter`. Unlike functions/variables,
this is **not** filtered by `__module__` — `Parameter` doesn't reliably
expose one (unknown attribute lookups proxy to the wrapped value), so a
Parameter imported into two scanned modules can legitimately show up
attributed to both.

A bare `EachOf` is deliberately NOT discovered — only a *named*, top-level
`Parameter` binding is GUI-visible. `_scan_module_path_inputs` also skips
Parameters explicitly: a Parameter is an `EachOf`, and `is_path_input`
accepts an `EachOf` whose alternatives are all PathInputs, so without that
guard a Parameter wrapping PathInputs would register as both.

**`__getattr__` must raise `AttributeError`, never `TypeError`**, for a
multi-valued Parameter: `hasattr()` only swallows `AttributeError`, and
`foreach._is_loadable` probes with `hasattr(var_spec, "load")`. A
`TypeError` there crashes every `for_each` carrying a multi-valued
Parameter.

### MATLAB

`+scidb/Parameter.m` — `classdef Parameter < scifor.EachOf` — declared in
the entities script (§4) exactly like PathInput:

```matlab
window_seconds = scidb.Parameter(10, 20, 30, description='Analysis window');
```

`matlab_registry._register_matlab_parameter_object` constructs a **real**
Python `scidb.Parameter` and registers it through the same
`registry._register_parameter` that Python discovery uses, so
`build_parameter_nodes` and every other consumer stay language-agnostic.

Because a Parameter IS an `EachOf`, `+scidb/for_each.m`'s existing Step 0
expansion handles it with **no** unwrapping step — one reason the class
beat the earlier `scidb.Constant` value holder, which needed an explicit
unwrap at the top of `for_each`.

The constructor peels a trailing `description=...` off its varargs before
calling the superclass; `scifor.EachOf` treats every argument as an
alternative, so the description would otherwise silently become a value.

A MATLAB `for_each` call passing a **bare literal** in its inputs struct
still has no named identity; `pipeline_discovery.py`'s source→GUI import
(§6) surfaces those as GUI Parameter nodes with a staged pending value.

---

## 4. PathInputs — scanned from source (clean break, 2026-08-20)

`PathInput` (`scifor/src/scifor/pathinput.py`) is a path-template object.
As of this migration, a PathInput is only GUI-visible when bound to a
top-level module name:

```python
RAW_EMG = scidb.PathInput("{subject}/{trial}.mat", root_folder=DATA_DIR)
```

### Python

`registry._scan_module_path_inputs` (mirrors `_scan_module_parameters`
exactly) walks `vars(module).items()` for a `PathInput` instance, OR an
`EachOf` whose every alternative is a `PathInput` (this is how "alternate
templates" express themselves now — `EachOf(PathInput(t1), PathInput(t2))`
bound to one name — no separate GUI concept). No `__module__` filtering,
same reasoning as Constants.

There is **no DB-history fallback anymore** — the old
`overlay_saved_path_inputs`/layout.json-authored path was deleted. A
function's *historically recorded* PathInput usage (from a run before this
migration, or a still-inline/unnamed `PathInput(...)` at a call site) is
resolved back to a source-declared name by **content-matching**
(`graph_builder.resolve_path_input_name`: template+root_folder equality
against the registry) since the DB only ever recorded the value, never a
name. No match → a `__unresolved__:{template}` synthetic key, logged at
WARN — the node still renders (best-effort) but isn't wired to any current
source declaration.

**A PathInput's `root_folder` is part of its identity, so nothing may
rewrite it on the way to a run.** `PathInput.to_key()` serializes
`(template, root_folder)` and that pair is the *only* link between a run and
a node. Generated MATLAB commands used to substitute the project root for a
rootless declaration — to stop MATLAB's cwd (a temp script dir) from
deciding resolution — and the first successful run then grew an
`__unresolved__` ghost node beside the very declaration that produced it. The
resolution base is now pinned separately, without touching identity:
`scifor.set_project_root()`, stated by the generated script's preamble and by
`scidb.entities(PROJECT_ROOT)` over the bridge. `resolve_path_input_name`
additionally treats a recorded `root_folder` equal to the project root as
equivalent to no root, because the already-recorded rows are permanent. See
`.claude/plan-pathinput-unresolved-after-run.md`.

### MATLAB

> **Superseded 2026-09-01**: both languages now declare GUI-written
> entities in one TOML `entities_file` (`entities-toml-format.md`). The
> `.m` script below and Python's `variable_file` remain **discovered and
> read-only**, so everything about how they are *found* still applies.

MATLAB declares entities in an **entities script** — a plain `.m` file (no
`function`, no `classdef`) of top-level bindings, configured as
`[matlab] entities_file` and structurally identical to Python's
`variable_file`:

```matlab
% scistack_entities.m
raw_emg = scidb.PathInput('{subject}/{trial}.mat');
window  = scidb.Parameter(10, 20, 30);
```

Pipeline code runs `scistack_entities;` and the names are in scope.
`+scidb/PathInput.m` is a one-line subclass shim over the `+scifor/`
original, and `+scidb/Parameter.m` subclasses `scifor.EachOf`, so both
languages' entities files read the same.

> **The `value getter` convention (a zero-arg function returning a
> constructed PathInput/Sweep) was REMOVED on 2026-08-25**, along with the
> `[matlab] path_inputs`/`sweeps` config lists. One declaration form per
> language. A function that happens to construct a Parameter is now just a
> function. See `docs/claude/entity-editability-model.md`.

`matlab_parser.parse_matlab_entities_script` regex-parses each `NAME = ` at
the start of a line (rejecting `==` so a comparison isn't read as a
binding), matches the RHS against the known constructors, and returns a
`MatlabBinding` carrying the span of the whole RHS **and** of just the
argument text. Static-only — never runs MATLAB.

`matlab_registry.load_entities_script` then attempts best-effort literal
extraction: split the argument text on top-level commas (respecting quoted
strings and doubled-quote escaping) and parse each as a MATLAB
string/number literal. `root_folder` is accepted in **both** MATLAB
syntaxes — `root_folder='/d'` (name=value, R2021b+) and `'root_folder', '/d'`
(name-value pair). When every needed argument parses, a **real**
`scifor.PathInput`/`scidb.Parameter` object is constructed and registered into
the same shared `scistack_gui.registry` Python-discovered entities use
(`registry._register_path_input(name, pi, source=path)`) — so the GUI
canvas's `pathInput__` nodes, `execution_service.build_run_inputs`'s
content-matching resolution, and `matlab_command_service`'s command
generation all pick it up with no MATLAB-specific branching downstream.

If any argument is not a literal (a variable reference, a function call,
string concatenation), extraction returns `None` and the declaration falls
back to name-only tracking — `registry.get_path_input(...)` returns `None`
and a load error is recorded (`_record_load_error`). A MATLAB pipeline
using it still resolves at MATLAB run time; it just isn't visible or
wireable on the canvas.

Refreshing (`load_from_config`) deregisters any previously-registered entry
whose recorded source no longer declares it
(`_deregister_stale_matlab_path_inputs_and_sweeps`, exact-source-path match
only, so it never touches a same-named Python-discovered definition).

### Portability (cross-user export/import)

`portability_service.py` still bundles a referenced PathInput's resolved
value into the export document (`export_pipeline`, reading
`registry.get_path_inputs_registry()`) — but the semantics shifted from
"the only copy" to an **import-time fallback on a local name miss**:
`import_pipeline_document` reuses the LOCAL definition untouched if the
name already exists there; only on a miss does it call
`path_input_service.create_path_input` to *materialize* the bundled value
into the importer's own configured source file (never a phantom GUI-only
value). A materialization failure (no entities file configured) surfaces
in the import result's `materialization_errors`, not a silent drop.

### GUI-side creation

`create_path_input`/`layout_service.py` write the declaration into
`config.entities_file` (TOML — `NAME = "template"`, or a
`{template, root_folder}` table) and refresh the registry
(`path_input_service.py`, mirrors `variable_service.create_variable`
exactly). In legacy single-file mode (`--module`) the same call still
appends `NAME = PathInput(...)` to the module, dispatched by suffix — see
`entities-toml-format.md`.
**No update/alternates endpoints anymore** — editing an existing
PathInput's template means editing the source file directly and hitting
Refresh Code. "Delete" (`delete_path_input`) hides the `pathInput__{name}`
node only (`pipeline_store.hide_node`) — the source declaration is never
touched (never delete, mark hidden).

---

## 5. Submodules — GUI composition, with a working source-code translation

What appears in the graph as `pipelineNode` is a **nested pipeline**
(another whole pipeline placed as a node inside a parent one) — the
"Hypothesis tabs & submodules" feature, backed by `_pipeline_uses` DB rows
(`parent_pipeline_id` / `child_pipeline_id` / `use_id` / `binding_json =
{key_map, params, iterate}`). `scidb.Pipeline`/`.use()`/`.bind()`
(`scidb/src/scidb/pipeline.py`) already implements the identical shape in
source code — `binding_json`'s three keys are a direct, field-for-field
match with `PipelineBinding.key_map`/`.params`/`.iterate`.

### Source → GUI import (`scistack_gui/pipeline_discovery.py`)

A user's file can define a pipeline in source:

```python
pipe = Pipeline("gait_analysis")   # deliberately no db= -- see module docstring
pipe.activate()
for_each(bandpass_filter, {"signal": RawSignal, "low_hz": 20}, [Filtered], ...)
```

`Pipeline` registration is side-effect-free until `run_*` is called — the
same property that lets functions/variables/constants be discovered by
*importing* a file. `discover_and_seed_pipelines(db)` reads
`scidb.pipeline._all_pipelines` (filtered to `db=None` — the convention that
tells a genuine user-authored pipeline apart from
`execution_service.build_backend_pipeline`'s own per-request COMPILED
Pipelines, which always set `db=`), recursively seeds each into GUI state
(one manual `functionNode` + manual `variableNode`/`parameterNode`/edges per
`StepSpec`, one `_pipeline_uses` row per `PipelineBinding` in `.uses`), then
discards them from scidb's own bookkeeping (avoids scidb's "pipeline
registered but never run" atexit warning; also how the function knows
what's new next time — nothing is left behind to re-process).

**"Create once"**: a discovered pipeline whose name already exists locally
is skipped entirely, never overwritten — same precedent as
`create_variable`/`create_path_input`. Re-editing the source file and
hitting Refresh Code does not resync hand-edited GUI state.

Manual `variableNode`/`parameterNode` creation (not just edges) is required
because `build_variable_nodes`/`build_parameter_nodes` only render from DB
run history — a genuinely never-run type/constant needs a manual node to
show up at all (`merge_manual_nodes` is what makes a manual node appear
regardless of history). PathInput/Parameter nodes are the one exception —
always registry-derived (§3/§4), never manual.

Each manual `variableNode`/`parameterNode` created by discovery gets an
**arbitrary** id (`discovered_{var,const}_{uuid}`), never the bare
canonical form (`var__RawA`) — `_get_or_create_node` mints one fresh id per
`(node_type, label)` the first time it's seen within one pipeline's seed
pass (cached in a `node_cache` scoped to that single
`_seed_pipeline_recursive` call), and reuses it for every other step in
*that* pipeline referencing the same variable/constant. This matters
because `_pipeline_nodes.node_id` is a global PRIMARY KEY, not scoped by
`pipeline_id` — two different discovered pipelines that happen to share a
variable/constant name (e.g. both use a `RawA` input) must NOT write to the
same row, or the second pipeline's write silently clobbers the first's
`pipeline_id` (last-write-wins via `ON CONFLICT DO UPDATE`), making the
node vanish from the first pipeline's graph. `merge_manual_nodes` matches a
manual node to its DB-derived counterpart by `(type, label)` only, never by
id (see `.claude/plan-placement-qualified-node-ids.md`), which is exactly
why an arbitrary id works correctly here — function nodes already followed
this pattern; var/const nodes previously didn't, and that gap is fixed.

Wired into `bootstrap.open_or_create_project` (initial load, all project
modes) and `api/project.py._refresh_registries` (loose-script/single-file
"Refresh Code" path). **Known gap**: not wired into the packaged-project
(`pyproject.toml` present) `scan_project` refresh branch — see the comment
at that call site in `api/project.py`.

### GUI → source export (`scistack_gui/services/code_export_service.py`)

Already existed before this migration (contradicts
`docs/claude/gui-export-to-plain-python.md`'s "Status: not yet built" —
that doc is stale). Reuses `build_backend_pipeline`'s compiled
`scidb.Pipeline` directly (`Pipeline._composed_steps()`/`_topo_order()`) for
Python; a parallel manual closure walk + type-level topological sort for
MATLAB (registry.get_function only resolves Python callables, so it can't
reuse the same `Pipeline` object).

**Submodule composition is FLATTENED**: `_composed_steps()` already resolves
the full `.uses` closure with bindings pre-applied (key_map/params/iterate
rewritten into each `StepSpec`) and returns one flat, topologically-ordered
step list — the exported script has no explicit `child.use(parent.bind(...))`
calls, just one linear sequence of `for_each(...)` calls. PathInput/Sweep
values are inlined as reconstructed literals via `repr()`/duck-typing
(`getattr(value, "alternatives", None)`), not name references — needed zero
changes for the new `Sweep` class.

---

## Where Python looks for source files (config.py)

Governs where the **function/variable/constant/PathInput/Sweep** scanners
above look for `.py`/`.m` files in the first place — three tiers, in
priority order:

1. **`pyproject.toml` / `scistack.toml`**, `[tool.scistack]`:
   - `modules = [...]` — explicit files, directories (recursively walked),
     or glob patterns
   - `packages = [...]` — installed pip packages, walked recursively via
     `pkgutil.walk_packages`
   - `auto_discover = true` — scans installed packages' `scistack.plugins`
     entry points
   - `[tool.scistack.matlab]` — `functions = [...]`, `variables = [...]`,
     `sources = [...]` (unclassified, auto-classified per-file),
     `variable_dir`, `entities_file` (§4). The `path_inputs`/`sweeps` lists
     were removed with the value-getter convention on 2026-08-25.
2. **Folder-scan fallback** (no config file found at all): recursively walks
   the project root for `.py`/`.m` files, pruning noise dirs (`.git`,
   `__pycache__`, `.venv`, `node_modules`, `build`, `dist`) and, for MATLAB,
   `private/`, `@ClassName/`, `+package/` directories (never swept in — see
   `_is_matlab_skip_dir`).
3. **GUI "Paths" popup** (`add_path`/`remove_path` in `config.py`) — writes
   directories into `scistack.toml`'s `modules` + `[matlab] sources` lists;
   only available for loose-script projects (no `pyproject.toml`). Fully
   round-trips every `[tool.scistack]`/`[matlab]` field it knows about
   (including `variable_dir`/`entities_file`) via `_render_scistack_toml` —
   earlier versions of this function silently dropped any field it wasn't
   explicitly passed on every popup save. **`_render_scistack_toml` has four
   call sites**; a new field must be threaded through all of them or it is
   dropped on the next save.

Later sources win on name collisions (functions/constants: last-loaded
wins, with a warning logged; see `_register_function`/`_register_constant`).

## 6. Test-file exclusion (2026-08-22)

Anything found *exclusively* inside a MATLAB or Python test is excluded from
final discovery results, for all six kinds above. This is enforced by
filtering at the **file-path/module-name level, before a file is ever
imported** — since every kind above is only ever discovered as a side effect
of importing/scanning a module (functions/constants/PathInputs/Sweeps via
`inspect.getmembers`/`vars()`; variables via `BaseVariable`'s metaclass;
submodules via `scidb.pipeline._all_pipelines`), preventing the import means
nothing from that file is ever discovered — one choke point per source tier
covers all six kinds with no per-category logic needed.

A file/module counts as "test" if EITHER:
- any path directory component (case-insensitive), or any dotted
  module-name component, is `test`/`tests`, OR
- the filename matches a test naming convention — Python `test_*.py` /
  `*_test.py`; MATLAB `Test*.m` (PascalCase prefix) / `*Test.m` (suffix).

There is no override: even an explicit single-file config entry that names
a test file (e.g. `modules = ["tests/test_foo.py"]`) is still excluded.

**Shared predicate lives in `scidb`** (the core layer scistack-gui already
depends on), not duplicated per package:
- `scidb.discover.is_test_path(path)` / `is_test_modname(modname)` — the
  directory + Python-filename-convention check. Applied in
  `discover.py:scan_package`'s `pkgutil.walk_packages` loop (skips a test
  submodule before import) and reused by `scistack_gui/registry.py`'s
  `_load_packages` (same `pkgutil.walk_packages` shape, for pip-installed
  `packages = [...]` config entries).
- `scistack_gui/config.py:_is_test_file(path)` — wraps `is_test_path` and
  adds the MATLAB-only `_MATLAB_TEST_FILE_RE` check. Applied in
  `_walk_source_files` (prunes `test`/`tests` dirnames during any walk, and
  filters the final per-file result list), `_resolve_glob_paths` (MATLAB
  `functions`/`variables`/`sources` — covers glob matches, directory-walk
  results, and explicit single-`.m`-file entries),
  and `load_config`'s Python `modules` handling (glob branch and explicit
  single-file branch; the directory branch already routes through the
  patched `_walk_source_files`).

**No changes needed** in `matlab_parser.py`, `matlab_registry.py`, or
`pipeline_discovery.py` — they only ever see paths that already passed
through the upstream filtering above, so a test file's contents (including
a `scidb.Pipeline`/submodule constructed inside it) simply never get a
chance to register anywhere.

Confirmed real-world leak this closes: `scimatlab/tests/matlab/helpers/*.m`
contains plain functions (`sum_all.m`, `col_max.m`, ...) and `BaseVariable`
classdefs (`RawSignal.m`, `BaselineSignal.m`, ...) that exist solely to
support the MATLAB test suite — previously fully discoverable if a project
config pointed at that directory; used as a regression-test fixture in
`scistack-gui/tests/test_config.py`.

## 7. Import-time side-effect screen (updated 2026-09-25)

Discovery reads a `.py` file's definitions by **importing** it, and importing
runs every module-level statement. A script with its analysis at the top level
therefore does that analysis during discovery. It writes files relative to the
server's cwd, takes time, and can exhaust the extension's startup window.

**Owner:** `scifor.discovery.find_top_level_side_effects`. The GUI's
`registry._screen_for_side_effects` is only a consumer: a file with any
finding is refused (recorded as a load error, shown in Paths → Discovered
Code) and never imported.

Rules:
- **Form 1:** a bare call whose result is discarded (`df.to_csv(...)`),
  unless it is benign configuration/output (`BENIGN_TOPLEVEL_CALLS`: `print`,
  `matplotlib.use`, `pd.set_option`, any `*.info/debug/...` logger call).
- **Form 2:** an assignment calling a function `def`'d in the same file
  (`data = plot_gait(...)`). Imported callees (`Parameter(...)`,
  `Path(...)`, `logging.getLogger`) never count.
- Both forms apply **recursively inside top-level `for`/`while`/`with`/`if`/
  `try` blocks**, and a local-function call in a loop iterable, `while`/`if`
  test or `with` context expression counts too. `def`/`class` bodies and the
  `if __name__ == "__main__":` guard (either operand order) are never entered;
  the guard's `else` is.
- The reason in the refusal names the enclosing block and its line, e.g.
  `top-level call df.to_csv() at line 5 (its result is discarded, inside a
  top-level for loop at line 3)`.

Why the recursion exists: before 2026-09-25 only direct module-body children
were inspected. Stroke-R01-Aim1's `src/stats/create_*_df.py` scripts looped
over conditions writing CSVs, passed the screen, ran for 13 s and 8 s on
import, and the next file used up the rest of the 60 s startup window.

Remaining gaps (for a low false-positive rate): an assignment calling an
*imported* heavy function (`df = pd.read_csv("huge.csv")`) and instantiation
of a locally-defined class are not flagged. The backstop is timing: the
registry logs each file's import duration on its "Loaded module file" line
and WARNs "Slow import" above `registry.SLOW_IMPORT_WARN_S` (5 s).

## See also

- `docs/claude/multi-source-discovery.md` — the `[tool.scistack]` config
  format in more detail (Python side).
- `docs/claude/input-markers-colname-pathinput-pathoutput.md` — full
  PathInput/PathOutput/ColName resolution semantics (PathInput's own
  template-resolution mechanics are unchanged by this migration — only
  *discovery* changed).
- `docs/claude/each-of-variant-expansion.md` — `EachOf`'s own design; `Sweep`
  is documented there as sugar, not a separate mechanism.
- `docs/claude/matlab-gui-implementation.md`,
  `docs/claude/matlab-gui-support-analysis.md` — broader MATLAB-GUI design.
- `docs/claude/pipeline-import-identity.md` — pipeline/submodule identity
  (`pipeline_id`) across export/import (unaffected by this migration —
  works identically whether a submodule originated from hand-drawn GUI
  wiring or a source-code import).
- `docs/claude/gui-export-to-plain-python.md` — **stale**, says "not yet
  built"; see §6 above for what's actually there.
- `.claude/plan-pathinput-sweep-submodule-source-of-truth.md` — the plan
  this whole migration executed.
- `docs/claude/entity-editability-model.md` — the **write** half: how the
  GUI edits these declarations back into source, the Constant+Sweep →
  "Parameters" presentation merge, and MATLAB's entities *script* (which
  replaced the value-getter convention in §4/§5).
  **Design only — not yet implemented**; check its status header and
  `.claude/plan-gui-entity-editing-26-08-24.md` before relying on it.
- `docs/claude/function-input-resolution.md` — the **execution** half: how a
  discovered Parameter/PathInput actually reaches `for_each(inputs=)`. Note
  that the declared name a category here carries is NOT what binds it to a
  function parameter — the canvas edge is (as of 2026-08-25).
