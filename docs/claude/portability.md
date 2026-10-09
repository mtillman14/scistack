# Portability: exporting and importing whole projects

Status: **design, 2026-10-08** (revised the same day with the user's
decisions). Implementation plan: `.claude/plan-portability.md`. Supersedes the
uv-based snapshot/scaffold parts of `project-library-structure.md` (the
project/library wall in that doc still stands).

## Goal

Someone clicks **Export** (or runs `scistack export`) on a ready-to-share
project. Someone else clicks **Import** (or runs `scistack import`) and the
project appears in front of them exactly as it was.

Two use cases, in priority order:

1. **Reuse a pipeline on new data.** Structure, layout, settings and code come
   across. The recipient may use a completely different schema. Nodes show as
   not run.
2. **Hand off a finished study.** The same, with the exporter's schema kept,
   plus run history and (opt-in) the derived data, so nodes that have run
   reappear in their run state.

Live collaboration (two people merging back and forth) is **out of scope**.

## Ground rules

- **The GUI is optional at both ends.** Every step works from the CLI with no
  GUI process, and entirely from the GUI. Both are thin wrappers over **one**
  `export_project(options)` / `import_project(bundle, options)` pair in scidb.
- **Raw input files are never touched.** The files PathInputs read are never
  exported, imported, copied, moved or hashed into a bundle. They're valuable
  scientific data and are never managed automatically. An import only asks
  where the recipient's own copy lives.
- **Derived data is opt-in.** The default export holds no variable data. Run
  history is included by default.
- **One owner for the export defaults.** `ExportOptions` in `scidb` sets the
  defaults; the GUI checkboxes and the CLI flags both read from it.
- **No user identity anywhere.** SciStack records what ran and when, never
  who (decision 2026-10-08).
- **Schemas don't have to match.** Import suggests the exporter's schema but
  lets the user enter their own (see "Schema on import").
- **Standard Python packaging.** Code travels as a wheel built from
  `pyproject.toml`, which is packaging only. Project configuration lives
  only in `scistack.toml`, which the GUI owns (decision 2026-10-08, see
  `config-file-formats.md`). Neither file is in the wheel, so installing one
  project into another never brings a second configuration. No uv.
- **Layering.** The bundle format, options, manifest, code packaging, schema
  choice, history/data sections and verification live in **scidb**. The GUI
  state section lives in **scistack-gui** and plugs into scidb's bundle as a
  section provider. scidb never imports the GUI.
- **Beta rules apply.** A format-version mismatch refuses with a clear
  message. No migration code.

## What a project is made of

| Layer | Lives in | In the export |
|---|---|---|
| Code | `src/<pkg>/`, glue functions, `.m` files | always, as SOURCE (a bundle import is a copy into the new project; see "Reusing code"). Wheels are for libraries (Stage 10) |
| Entity declarations (Variables, Parameters, PathInputs) | `scistack_entities.toml` | always. **Moves into the package** (`src/<pkg>/scistack_entities.toml`) so it is package data and travels with installed code |
| Dependencies | `pyproject.toml` `[project]` | always (the file travels with the code) + record of exact resolved versions |
| Environment | not recorded today | always: Python version, platform, MATLAB release and toolboxes |
| Project config | `scistack.toml` (the only config file) | always |
| GUI state | `_pipeline_*`, `_node_config`, `_intent`, `_node_wiring`, `_hypotheses`, `<db>.layout.json`, `_variant_pin`, scistackplotdb saved plots and presets | always, when the project has any |
| Run history | `_record`, `_invocation*`, `_run`, `_run_invocation`, `_function_source`, `_constant`, `_schema` | default on |
| Derived variable data | the variable tables + `_record_save` | opt-in |
| Raw input files | wherever the user keeps them | **never** |

### Why entities stay a separate file inside the package

`scistack.toml` holds **project** configuration: it should *not* travel when
the project is installed into another project, and it isn't in the wheel. Entity
declarations are part of the **code**: an installed project's Variables and
Parameters must travel with its functions, or its functions refer to things
the importer doesn't know about. So the two have opposite needs. Entities stay
a separate TOML file, but inside the package so they're package data.

PathInput `root_folder` values are machine-specific, so they're the one part
of the entities file that isn't really "code". Import rewrites them (see
below); that's enough, and doesn't justify a separate file.

## Bundle

A `.scistack` file. It is an ordinary zip with a different extension.

```
manifest.json   format_version, scistack version, export options used,
                sections present, content hashes, exporter's schema
env/            resolved-versions record, Python/platform/MATLAB info
code/           the project's source tree (pyproject.toml, src/<pkg>/, other
                code files under the root that discovery loads)
                wheelhouse/                    (opt-in, Stage 10)
config/         scistack.toml
gui/gui.json    every visible pipeline's canvas + global GUI state (GuiSection)
plots/          saved_plots.json, presets.json (PlotsSection), when any exist
history/        provenance tables (Parquet)    (default on)
data/           variable tables (Parquet)      (opt-in)
```

Open formats (Parquet, JSON, TOML) throughout, so a bundle can be read
without SciStack.

Built in Stage 4 (`scidb/bundle.py`): manifest, config, gui, plots. Sections
are providers the front end passes in (`headless.bundle_providers()`); the
manifest lists only the sections actually written. env, code, history and
data come in Stages 5 and 7.

## Schema on import

Import proposes the exporter's schema. The user can accept it, or decline and
enter their own, in which case the exporter's schema is ignored.

Code is never schema-bound: functions don't see the schema (the scifor
principle). Everything else that names a schema key gets handled through
**one key map** (exporter key → recipient key, or none). Names that exist in
both schemas are matched automatically; the user edits the rest.

| Thing in the bundle | Refers to schema by | Schema kept | Recipient's own schema |
|---|---|---|---|
| Run history, derived data | `schema_id`; iteration level is part of the invocation ID | imported verbatim | **not imported**: it describes a dataset with a different shape |
| Node iteration level, schema selection | key names | as-is | remapped; unmapped keys cleared, falling back to the schema-level default rule (node > last run > inputs union > all keys) |
| Where filters | key names + values | as-is | key remapped; values are dataset-specific, so each filter is kept and flagged for review in the import report |
| PathInput templates (`{subject}/{trial}.csv`) | placeholder names | as-is | placeholders renamed through the map; unmapped placeholders flagged |
| Saved plots / presets (grouping, facets, collapse by key) | key names | as-is | remapped; roles on unmapped keys removed and flagged |
| `[schema_keys]` types | key names | as-is | the recipient's own |

Each store's portable declaration (plan Stage 2) carries its own schema-key
remap function next to its ID remap, so one walker applies the map and there
is no second list of "places that mention schema keys".

With the recipient's own schema, no history comes across. Function nodes start
as never-run nodes, and the recipient's first run gives them new identities.
That's correct: it's a different dataset.

## PathInput roots on import

Raw files stay where they are. For each PathInput, import shows the
exporter's `root_folder` and asks for the recipient's location (CLI:
`--path-root NAME=/path`). The answer is written to the recipient's entities
file. PathInput identity is the **name only** (`to_key`); `root_folder` is
never hashed. So changing the root doesn't fork anything, and the imported
history (which records the exporter's spec on the `__pathinput__` record, an
accurate fact of what was read) stays valid.

## History import

History and data are imported **only into a new, empty project with the
exporter's schema kept**. That makes the import a verbatim copy: every record,
invocation and run row arrives exactly as exported, and nothing has to be
deduplicated or skipped. Importing history into an existing project would be
merging, which is live collaboration and out of scope. Into an existing
project, only code, config and GUI state are imported.

History is also a choice at import time (default on when present). For reuse
on new data with the same schema, the recipient can decline it so their
database holds only their own results.

## Reusing code: copies, libraries, and names

Decided with the user 2026-10-08.

### What people reuse is a submodule

A scientist reuses lab A's `preprocessing` submodule in projects B, C and D,
not all of project A. Depending on a whole project would bring its other
pipelines, its data layout (PathInputs), its config and its history, and
every later change to A would ripple into B, C and D. So projects still
depend only on libraries (`project-library-structure.md`), and SciStack makes
a library out of a submodule in one click instead of asking scientists to
restructure code by hand.

### Two ways code arrives, chosen at the natural moment

| | How | When | Names |
|---|---|---|---|
| **Copy** | Source written into the project: a whole-project bundle into `src/<pkg>/`; a library submodule the user chose to edit into `src/<pkg>/<lib>/` | Importing a `.scistack` (handoff, reuse on new data): always a copy. "Make my own copy" on a library submodule. | The project's own top-level code is bare; a copied subpackage's functions are qualified by it (`preprocessing.filter_emg`), so two copies' `filter` cannot clash. |
| **Use** | An installed library; its submodule is placed read-only | Placing a library submodule (the default) | Functions qualified by the library (`preprocessing.filter_emg`), like library functions (`pandas.read_csv`, `library-function-name-identity.md`). |

Nobody is asked "dependency or copy?" in the abstract: they get **use** by
default and **copy** when they try to change something.

### "Share as library" (portability Stage 10)

On a submodule tab: generate a package from it — the functions it uses,
their declarations, and the submodule itself as a source-defined pipeline
(`scidb.Pipeline` + `for_each`, which `pipeline_discovery.py` already seeds
onto a canvas as a submodule, written by the existing code export). The
source project can then switch to the library, so there is one copy.

### Names when code from several places meets

- **Functions** from an installed library or a copied subpackage are
  qualified by it. Only the project's own top-level code is bare.
- **Variables are not namespaced.** The name is the database table, the
  record `type` and the MATLAB class name, and `.` already means
  `Variable.Column`. Instead: the same name declared the same way is ONE
  type (a project's `variables = ["RawEMG"]` and a library's `RawEMG` merge
  silently); only genuinely different definitions (a different
  `schema_version`, a custom `to_db`/`from_db`) are an error naming both
  sources and which to remove.
- **A library's Parameters and PathInputs stay out of the project's names.**
  They are the library's internals (defaults its own functions use); a
  PathInput is one project's data layout. A project that wants one on its
  canvas declares its own.
- **Chaining submodules from several libraries:** matching variable names
  connect by themselves; differing names are wired with the placed
  submodule's port bindings (`_pipeline_uses.binding`) or a glue node.
- **Different schemas:** placing a library submodule into a project with
  other schema keys goes through the same key map as a bundle import
  ("Schema on import").

## Export steps

Key: ✅ exists · 🔧 exists but needs fixing or has more than one owner · ❌ missing.
Status as of 2026-10-08.

| # | Step | Status | Notes |
|---|---|---|---|
| E0 | Open the project without the GUI | ✅ | `headless.open_for_export`: bootstrap WITH discovery (the canvas needs the registry; the code is the exporter's own) (Stage 3). |
| E1 | GUI button + CLI command | 🔧 | The GUI handler `export_pipeline` exists for one pipeline plus the ones it uses. No whole-project export, no CLI. |
| E2 | Pre-export checks | ❌ | Nothing running; disconnected edges settled at export time (`gui-export-to-plain-python.md`). |
| E3 | One list of what is exported | ✅ | Every GUI table classified canvas / global / history in its owner's `PORTABILITY`; guard `test_every_gui_table_is_classified` (Stage 2). |
| E4 | Nodes, edges, sub-pipelines, hidden ports, hypothesis | ✅ | `services/canvas_snapshot.capture`, shared with duplicate/paste (Stage 2). |
| E5 | Node settings | ✅ | Every intent statement, resolved in the source scope, for run and never-run nodes alike (Stage 2). |
| E6 | Other GUI state | 🔧 | Notes, built-in function references, parameter value groups: ✅ (Stage 2). `_node_wiring`, PathInput rename history: history-only (Stage 7). `_variant_pin`, saved plots/presets: Stage 4. |
| E7 | Hidden nodes/edges/combos | 🔧 | Captured (shared with duplicate) but not applied on a canvas-only import; applied with history (Stage 7). |
| E8 | Positions | ✅ | In the snapshot. |
| E9 | Parameters / Sweeps / PathInput values | 🔧 | Bundled by registry lookup. Replace with the entities file travelling in the wheel; check the `layout.json` constants palette as an owner. |
| E10 | Project config | 🔧 | `scistack.toml` is the bundle's `config` section (scidb). Copied verbatim; absolute first-write seeds still need Stage 6. |
| E11 | Code + entities file | ✅ | `bundle_section.CodeSection`: the files discovery loads (the config loader's list), `pyproject.toml`, the own package with its data; code from outside the root is listed, not copied (Stage 5). |
| E12 | Wheel + resolved versions + environment | 🔧 | Environment ✅ (`scidb.bundle` env section: Python, platform, every distribution's version, MATLAB if already loaded; Stage 5). Wheels: Stage 10 (libraries only). |
| E13 | Run history | ❌ | Default on. Requires user ID removed first. |
| E14 | Derived variable data | ❌ | Opt-in. Only `scidb/csv_export.py` exists. |
| E15 | Subset / anonymize subjects | ❌ | Later. |
| E16 | Manifest, format version, `.scistack` archive | ✅ | `scidb/bundle.py`: manifest with per-file SHA-256, plain zip, `ExportOptions` as the one owner of defaults (Stage 4). |
| E17 | Plain-script export | ✅ | `code_export_service.py`. |

## Import steps

| # | Step | Status | Notes |
|---|---|---|---|
| I0 | Run without the GUI | ✅ | `headless.open_for_import`: bootstrap with `discover=False`; nothing imports user code (Stage 3). |
| I1 | GUI button + CLI; new project or current project | 🔧 | Whole project: `headless.import_project_bundle` into a NEW project (Stage 4). Single pipeline into the current project: the GUI's JSON import. CLI/GUI front ends: Stage 8. |
| I2 | Check the format version | ✅ | Bundle (`read_bundle`, also hashes and unlisted files) and the single-pipeline document. |
| I3 | Ask the user to trust the bundle's code | ❌ | Must come before anything imports the bundle's code. |
| I4 | Create the project folder (`init`) | ✅ | `scidb.project.init_project`, the one owner (Stage 1); `import_project` calls it after writing the bundle's config. |
| I5 | Install the wheel + dependencies | 🔧 | Code arrives as SOURCE (a copy, before init). Declared dependencies are compared with the exporter's versions and the `pip install` command is reported; nothing is installed automatically (Stage 5). |
| I6 | MATLAB paths for the installed `.m` files | ❌ | `scimatlab`. |
| I7 | PathInput roots | ✅ | `import_project(path_roots={name: folder})` rewrites each PathInput's `root_folder` in the new project's entities TOML; a root left as the exporter's, or given for an unknown name, is reported. PathInputs declared in Python source are not rewritten (Stage 6). |
| I8 | Schema choice + key map | ✅ | `scidb/schema_map.KeyMap` (auto by name + overrides), applied by each section to its own data: config tables, PathInput templates, node levels and location selections, submodule bindings, saved plots; dropped/flagged references reported (Stage 6). |
| I9 | Recreate pipelines (reuse an identical one, fork one that differs) | ✅ | `_resolve_pipeline`, keyed on `pipeline_id`. |
| I10 | Mint node IDs | ✅ | `ids.new_manual_node_id` (every Python site; the frontend mints its own). |
| I11 | Mint edge IDs | ✅ | `ids.new_manual_edge_id` (a drawn edge's stored id is allocated; `connection_id` derives the connection's id for matching, a different concept). |
| I12 | Restore the rest of the GUI state, with schema keys remapped | ✅ | `canvas_snapshot.apply` + `remap_schema_keys` (Stages 2, 6). |
| I13 | Add missing PathInputs/Sweeps to the recipient's source | ✅ | Becomes unnecessary for new projects (the entities file arrives in the wheel); still needed when importing into an existing project. |
| I14 | Restore history / data | ❌ | New empty project + schema kept only; a verbatim copy. |
| I15 | Check that every canvas node's code is found | 🔧 | Needs discovery. A headless import (`discovered=False`) reports it unchecked (`unresolved_labels=None`) and defers PathInput/Sweep materialisation (`deferred`); the GUI checks on open. |
| I16 | Same function identity from a wheel and from loose files | ✅ | Moot for a bundle import: the code stays source in `src/<pkg>/`, the exporter's own layout. Revisit for libraries (Stage 10). |
| I17 | Reproduction check | ❌ | Later: `scistack verify` re-runs against the recipient's own copy of the raw files (read, never copied) and compares content hashes with the imported history. |
| I18 | Import report | 🔧 | A summary dict is returned; needs a report view, including everything the schema remap flagged. |

## Open questions

1. Installed packages' entities files: not read today (only the project's own
   file is loaded). Stage 5 adds it, under the merge rule in "Reusing code".
2. Constants path (E9): is the `layout.json` palette still a real owner, or
   leftover from before Parameters moved into the entities TOML?

## Related docs

- `project-library-structure.md`: project vs library wall (uv parts superseded)
- `gui-export-to-plain-python.md`: disconnected wiring at export time
- `database-model.md`: provenance tables
- `edge-model.md`, `node-identity.md`, `iteration-level-identity.md`
- `config-file-formats.md`, `entities-toml-format.md`
- `undo-redo.md`: the per-store table declaration pattern reused for portable tables
