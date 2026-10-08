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
  `pyproject.toml`. Project configuration lives in `[tool.scistack]` there.
  When one project installs another, the installed one's `pyproject.toml` is
  not part of the wheel, so there's only ever one project configuration in
  play. No uv.
- **Layering.** The bundle format, options, manifest, code packaging, schema
  choice, history/data sections and verification live in **scidb**. The GUI
  state section lives in **scistack-gui** and plugs into scidb's bundle as a
  section provider. scidb never imports the GUI.
- **Beta rules apply.** A format-version mismatch refuses with a clear
  message. No migration code.

## What a project is made of

| Layer | Lives in | In the export |
|---|---|---|
| Code | `src/<pkg>/`, glue functions, `.m` files | always (wheel; `.m` files as package data) |
| Entity declarations (Variables, Parameters, PathInputs) | `scistack_entities.toml` | always. **Moves into the package** (`src/<pkg>/scistack_entities.toml`) so it is package data and travels with installed code |
| Dependencies | `pyproject.toml` `[project]` | always (in the wheel) + record of exact resolved versions |
| Environment | not recorded today | always: Python version, platform, MATLAB release and toolboxes |
| Project config | `pyproject.toml` `[tool.scistack]` (`scistack.toml` is retired as the default) | always |
| GUI state | `_pipeline_*`, `_node_config`, `_intent`, `_node_wiring`, `_hypotheses`, `<db>.layout.json`, `_variant_pin`, scistackplotdb saved plots and presets | always, when the project has any |
| Run history | `_record`, `_invocation*`, `_run`, `_run_invocation`, `_function_source`, `_constant`, `_schema` | default on |
| Derived variable data | the variable tables + `_record_save` | opt-in |
| Raw input files | wherever the user keeps them | **never** |

### Why entities stay a separate file and don't move into `[tool.scistack]`

`[tool.scistack]` holds **project** configuration: it should *not* travel when
the project is installed into another project, and the wheel drops it. Entity
declarations are part of the **code**: an installed project's Variables and
Parameters must travel with its functions, or its functions refer to things
the importer doesn't know about. So the two have opposite needs. Entities stay
a separate TOML file, but inside the package so they're package data.

There's a second reason: the GUI writes the entities file constantly, using
scidb's span-based editor (`scidb.entities.upsert_entry`, etc.). That editor
assumes `variables`, `[parameters]` and `[path_inputs]` sit at the top level.
Keeping GUI writes out of `pyproject.toml` means the GUI never edits the
file that controls the user's packaging.

PathInput `root_folder` values are machine-specific, so they're the one part
of the entities file that isn't really "code". Import rewrites them (see
below); that's enough, and doesn't justify a separate file.

## Bundle

A `.scistack` file. It is an ordinary zip with a different extension.

```
manifest.json   format_version, scistack version, export options used,
                sections present, content hashes, exporter's schema
env/            resolved-versions record, Python/platform/MATLAB info
code/           <project>-<version>-py3-none-any.whl
                wheelhouse/                    (opt-in)
config/         [tool.scistack] table
gui/            GUI state section (omitted for script-only projects)
history/        provenance tables (Parquet)    (default on)
data/           variable tables (Parquet)      (opt-in)
```

Open formats (Parquet, JSON, TOML) throughout, so a bundle can be read
without SciStack.

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

## Export steps

Key: ✅ exists · 🔧 exists but needs fixing or has more than one owner · ❌ missing.
Status as of 2026-10-08.

| # | Step | Status | Notes |
|---|---|---|---|
| E0 | Open the project without the GUI | 🔧 | No discovery needed (see plan Stage 3). Remove `portability_service`'s dependence on GUI process state (`scistack_gui.db.get_db_path()`, registry lookups). |
| E1 | GUI button + CLI command | 🔧 | The GUI handler `export_pipeline` exists for one pipeline plus the ones it uses. No whole-project export, no CLI. |
| E2 | Pre-export checks | ❌ | Nothing running; disconnected edges settled at export time (`gui-export-to-plain-python.md`). |
| E3 | One list of what is exported | ❌ | Portable-tables declaration in each owner, modelled on `history.py`'s undoable tables. |
| E4 | Nodes, edges, sub-pipelines, hidden ports, hypothesis | ✅ | `portability_service.export_pipeline` |
| E5 | Node settings | 🔧 | Only for nodes that have never run; settings on run nodes are dropped. |
| E6 | Other GUI state | ❌ | `_node_wiring`, intent statements other than edges, notes, parameter value groups, built-in function choices, PathInput rename history, `_variant_pin`, saved plots and presets. |
| E7 | Hidden nodes/edges/combos | 🔧 | Left out on purpose; must be included when history is included. |
| E8 | Positions | 🔧 | From `<db>.layout.json`; a store in the list. |
| E9 | Parameters / Sweeps / PathInput values | 🔧 | Bundled by registry lookup. Replace with the entities file travelling in the wheel; check the `layout.json` constants palette as an owner. |
| E10 | Project config | ❌ | `[tool.scistack]`. |
| E11 | Code + entities file | ❌ | |
| E12 | Wheel + resolved versions + environment | ❌ | |
| E13 | Run history | ❌ | Default on. Requires user ID removed first. |
| E14 | Derived variable data | ❌ | Opt-in. Only `scidb/csv_export.py` exists. |
| E15 | Subset / anonymize subjects | ❌ | Later. |
| E16 | Manifest, format version, `.scistack` archive | 🔧 | JSON with `FORMAT_VERSION = 1` written to `exports/`. |
| E17 | Plain-script export | ✅ | `code_export_service.py`. |

## Import steps

| # | Step | Status | Notes |
|---|---|---|---|
| I0 | Run without the GUI | 🔧 | Same as E0. |
| I1 | GUI button + CLI; new project or current project | 🔧 | The GUI imports into the open database only. |
| I2 | Check the format version | ✅ | |
| I3 | Ask the user to trust the bundle's code | ❌ | Must come before anything imports the bundle's code. |
| I4 | Create the project folder (`init`) | 🔧 | Two owners today: `scistack/project.py::scaffold_project` vs `project_init_service.ensure_project_files`. |
| I5 | Install the wheel + dependencies | ❌ | Into the interpreter the project runs under. |
| I6 | MATLAB paths for the installed `.m` files | ❌ | `scimatlab`. |
| I7 | PathInput roots | ❌ | Ask, write to the entities file. |
| I8 | Schema choice + key map | ❌ | See "Schema on import". |
| I9 | Recreate pipelines (reuse an identical one, fork one that differs) | ✅ | `_resolve_pipeline`, keyed on `pipeline_id`. |
| I10 | Mint node IDs | 🔧 | Import writes its own; `ids.py` is the owner. |
| I11 | Mint edge IDs | 🔧 | Import writes its own `edge_{uuid}`; `graph_builder.connection_id` is the owner. |
| I12 | Restore the rest of the GUI state, with schema keys remapped | ❌ | Follows from E3/E6 and I8. |
| I13 | Add missing PathInputs/Sweeps to the recipient's source | ✅ | Becomes unnecessary for new projects (the entities file arrives in the wheel); still needed when importing into an existing project. |
| I14 | Restore history / data | ❌ | New empty project + schema kept only; a verbatim copy. |
| I15 | Check that every canvas node's code is found | 🔧 | `unresolved_labels` reports but doesn't block. Needs discovery; runs when the GUI opens the project, or on request from the CLI. |
| I16 | Same function identity from a wheel and from loose files | ❌ | Unverified; test it. |
| I17 | Reproduction check | ❌ | Later: `scistack verify` re-runs against the recipient's own copy of the raw files (read, never copied) and compares content hashes with the imported history. |
| I18 | Import report | 🔧 | A summary dict is returned; needs a report view, including everything the schema remap flagged. |

## Open questions

1. Installed projects' entities files: does discovery of `packages = [...]`
   already read an entities file shipped inside an installed package? If not,
   Stage 1 adds it.
2. Constants path (E9): is the `layout.json` palette still a real owner, or
   leftover from before Parameters moved into the entities TOML?

## Related docs

- `project-library-structure.md`: project vs library wall (uv parts superseded)
- `gui-export-to-plain-python.md`: disconnected wiring at export time
- `database-model.md`: provenance tables
- `edge-model.md`, `node-identity.md`, `iteration-level-identity.md`
- `config-file-formats.md`, `entities-toml-format.md`
- `undo-redo.md`: the per-store table declaration pattern reused for portable tables
