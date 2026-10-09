# Plan: project portability (export / import)

Design: `docs/claude/portability.md` (step IDs E*/I* refer to its tables).
Drafted 2026-10-08, revised the same day with the user's decisions.

Every stage: INFO logs at each named operation (snake_case op names, no
step numbers), tests that would have caught the problem, pytest commands
handed to the user (one package at a time).

## Stage 0: remove user identity entirely — DONE, committed adba4cc9 (2026-10-08), tests pass

Clean break (beta: no deprecation, no migration). Old databases keep their
now-unused nullable columns; nothing reads or writes them.

- scidb: delete `database.get_user_id` and its re-export in
  `scidb/__init__.py` (`SCIDB_USER_ID` stops being read).
- Remove the columns from `CREATE TABLE` and every INSERT/SELECT:
  `_run.user_id` (`provenance.py`, `provenance_save.record_run`),
  `_record_save.user_id` (`database.py` save paths, the batch path ~L2064,
  the reader ~L2385), `_variant_pin.pinned_by` (`variant_pins.py`), the
  tombstone table's `deleted_by` (`variant_delete.py`),
  `_exclusions.changed_by` (`exclusions.py`).
- `foreach.py` (~L5900, ~L5983): drop the `user_id` arguments.
- Readers: `provenance_query.py` (~L176, ~L2036), `database.py` ~L4826,
  `inspect/api.py` (3 dataclass fields + queries), `inspect/render.py`
  ("by X" text, `run_user_fmt`), `inspect/cli.py` (pins/tombstones tables),
  `inspect/variant_cards.py`, `scistack-gui/domain/variant_resolver.py`.
- Frontend: `variantCards.ts` (`user_id` field), `VariantsPopup.tsx`
  ("by …"), `variantCards.test.ts`. Rebuild both vite targets.
- Tests: `scidb/tests/test_provenance_graph.py`, `test_provenance_read.py`,
  `test_provenance_identity.py`, plus any `pinned_by`/`deleted_by` fixtures
  (incl. `scistackplotdb/tests/test_presets.py`, check).
- Guard: a test in scidb that fails if `user_id`, `SCIDB_USER`, `getpass`
  or `getuser` appear under any package's `src/`.
- Docs: ADR entry in `docs/claude/decisions.md`; fix
  `scidb-identity-and-data-flow.md` and other docs naming `user_id`.
- GUI manual testing entry: Variants popup shows no "by …".

## Stage 1: `scistack.toml` is the only config; one owner for `init` — DONE 2026-10-08, tests pass

User decision 2026-10-08 (option A): config lives ONLY in `scistack.toml`,
which the GUI owns and writes. `pyproject.toml` is packaging only and never
a config source; the GUI never edits it. Neither file ships in the wheel.

### 1a. `scistack.toml` is the only config file
- scifor.discovery owns the file name and its location:
  `CONFIG_FILENAME = "scistack.toml"`, `config_path_at(root)`.
  `project_config_at` and the GUI's `config.locate_config_at` both use it
  (they disagreed before: scifor took a pyproject only with a
  `[tool.scistack]` section, the GUI took any pyproject). `[tool.scistack]`
  is never read again (beta clean break).
- `read_project_name` stays: it reads packaging metadata (`[project].name`)
  so a project's own `src/<name>/` package is discovered. That is not config.
- GUI: remove `_reject_packaged_project` and the "packaged = read-only"
  mode (`describe_managed_paths.packaged`, PathsPopup read-only branch,
  glue/target-file/project-init refusals). Every project's config is
  GUI-editable.
- scidb CLI: `db = "..."` read from `scistack.toml`, not
  `[tool.scistack]`.
- Tests + `config-file-formats.md` rewritten; user docs updated.

### 1b. Remove uv
- `scistack/uv_wrapper.py`, `test_uv_wrapper.py`, the scaffold's `uv sync`,
  the GUI's lockfile-staleness check (`startup.py`, `server.py`).

### 1c. One owner for creating a project
- `scidb.project.init_project(root, *, name=None, schema_keys=None,
  scaffold_package=False)`. Every step creates only if absent; an existing
  file is never rewritten.
  - Always: `scistack.toml` (with `entities_file`), the entities TOML.
  - `scaffold_package=True` (CLI `scistack init`, GUI "New Project"):
    `pyproject.toml` (`[project]`, `[build-system]`, the entities TOML as
    package data), `src/<pkg>/__init__.py`, entities at
    `src/<pkg>/scistack_entities.toml`, `.gitignore`.
  - Opening an existing project only ensures config + entities, as today: it
    never turns someone's folder of scripts into a package unasked.
- The `scistack.toml` writer (`_render_scistack_toml`) moves from the GUI
  into scidb so `init` and the GUI write through one renderer.
- `scistack/project.py::scaffold_project` and
  `project_init_service.ensure_project_files` become thin callers.
  `scistack init` replaces `scistack project new`.
- Entities conventional fallback (`src/scistack_entities.toml`) unchanged;
  new projects get an explicit `entities_file` key inside the package.
- Tests: init twice is a no-op; existing files byte-for-byte unchanged;
  CLI and GUI produce identical trees.

Moved to Stage 5: reading entities files shipped inside installed packages
(today only the project's own file is ever loaded).

## Stage 2: one owner for "a canvas as data" (capture/apply) + import fixes — DONE 2026-10-08, tests pass

Refined 2026-10-08 after reading the code. `scope_service._clone_nodes`
(duplicate, paste) already copies canvas state correctly (resolved graph,
every intent statement via `intent_store.copy_subject`, hidden edges,
placements); `portability_service` re-implemented a weaker copy of it
(settings of run nodes dropped, no statements, no hides, its own id
minting). So the fix is ONE owner, not a per-table remap mechanism:

- `scistack_gui/canvas_snapshot.py`:
  - `capture(db, pipeline_ids, node_ids=None) -> CanvasSnapshot`: plain data
    read from the RESOLVED graph (what the canvas shows, history-derived
    nodes included), so a snapshot never depends on run history: every
    node is written as a manual node on apply ("de-graduation", the same
    thing `_clone_nodes` already does).
  - `apply(db, snapshot, pipeline_map, translation) -> old_to_new`: fresh
    ids, nodes, statements (as resolved in the source scope, written at the
    target scope), edges, uses, hides, hidden ports, positions.
  - `to_dict` / `from_dict`: the JSON form inside the bundle.
  - The snapshot is DATA, so Stage 6's schema-key remap is a pure function
    over it, applied before `apply`.
- `intent_store.copy_subject` splits into `resolved_statements` (capture)
  and `put_statements` (apply).
- `_clone_nodes` = capture + apply (+ its source-side placement
  solidifying). `portability_service` export = capture(closure) + globals;
  import = pipeline resolution (unchanged: reuse/fork by `pipeline_id`) +
  apply. FORMAT_VERSION 2 (v1 refused, beta).
- ID minting: `ids.new_manual_node_id(prefix, label)`,
  `ids.new_manual_edge_id()`; every Python minting site uses them (the
  frontend mints its own, a different language).
- Declaration + guard: every GUI table is classified in its owner module
  (`PORTABILITY = {table: "canvas" | "global" | "history" | "none"}`);
  a test fails when a table is unclassified (like
  `test_every_gui_table_is_tracked`). `canvas` tables are covered by the
  snapshot; `global` ones (pending constants, parameter value groups,
  built-in functions, notes, palette) form a `globals` section; `history`
  ones (`_node_wiring`, path-input renames/history) travel only with
  history (Stage 7).
- scistackplotdb saved plots/presets: own section, Stage 4 (separate
  package, keyed by variable name, no canvas ids).
- Tests: capture->apply round trip equals duplicate; export->import keeps
  run-node settings, statements, hides, notes; existing duplicate/paste and
  portability tests still pass; guard test.

## Stage 3: open the project without the GUI — DONE 2026-10-08, tests pass

**Corrected while building it.** The original claim "export needs no
discovery" was wrong: the canvas is built with the code registry (function
signatures give nodes their ports, PathInput history is matched to
declarations by template, Parameters come from source), so a capture
without it is not the canvas the user sees. Export runs only the exporter's
OWN code, so discovering is safe. The safety rule is about IMPORT, which
must never run a bundle's code before it is trusted, and applying a canvas
needs no registry.

- `bootstrap.open_or_create_project(discover=False)`: no registry load, no
  stubs, no built-in replay, no pipeline seeding; the project root is still
  resolved and pinned. One "open a project" sequence for the GUI's two entry
  points and the terminal.
- `scistack_gui/headless.py`: `open_for_export` (discover) and
  `open_for_import` (no discovery; may create the database).
- `portability_service.import_pipeline_document(discovered=False)`: applies
  the canvas, constants, notes, built-in references, value groups; DEFERS
  the two registry steps (materialising PathInputs/Sweeps into source,
  checking labels) and reports them (`deferred`,
  `unresolved_labels=None`). The explicit flag replaces reading registry
  globals, which test fixtures do not reset.
- Tests: `tests/test_headless.py` (a tripwire module proves import-mode
  open runs no code; export-mode open does; a canvas imports without
  discovery; HTTP and direct export give the same canvas).

Found for Stage 4: importing a pipeline whose id is `main` into a project
that already has an (empty) `main` compares contents and FORKS to
"main (imported)". A whole-project import into a new project must apply
`main` onto `main` instead.

## Stage 4: bundle format + options (scidb) — DONE 2026-10-08, tests pass

- `scidb/bundle.py`: `ExportOptions` (the one owner of the defaults:
  history on, data off, wheelhouse off), the manifest (format, versions,
  package, database file name, schema keys, options, per-file SHA-256),
  `.scistack` = plain zip; `read_bundle` refuses a changed, missing or
  unlisted file and another format version. `export_project(root, db, out,
  options, providers)`; `import_project(bundle, target, providers,
  schema_keys, open_db)` makes a NEW project only (config written, then
  `init_project`, then the front end's `open_db`, then each provider); a
  section with no importer is reported. scidb owns the `config` section.
- Providers are passed explicitly; scidb imports neither the GUI nor the
  plotting packages:
  - `scistack_gui/bundle_section.GuiSection`: every visible pipeline's
    canvas (`canvas_snapshot`), hypotheses, and the global GUI state via
    `portability_service.export_globals`/`apply_globals` (factored out of
    the pipeline export/import, one owner). Every pipeline keeps its id;
    `main` is filled, never forked. Hidden edges applied only when the
    bundle carries history.
  - `scistackplotdb/bundle_section.PlotsSection`: saved plots and presets,
    row for row (`VersionedStore.dump_rows`/`load_rows`).
- `headless.bundle_providers()`, `export_project_bundle`,
  `import_project_bundle`: the compositions Stage 8's CLI calls.
- Tests: `scidb/tests/test_bundle.py`, `scistack-gui/tests/
  test_bundle_project.py`, a plots round trip in `scistackplotdb/tests/
  test_saved_plots.py`.

Known gaps until later stages: no code/env section (Stage 5), so an
imported project has its canvas but not its functions; no history/data
(Stage 7); the config section is copied verbatim, including the GUI's
absolute first-write seeds (Stage 6). The single-pipeline JSON export (GUI
Export button) remains a separate document format.

## Stage 5: names across packages + code and environment sections — DONE 2026-10-08, tests pass

Revised 2026-10-08 with the user ("Reusing code" in `docs/claude/
portability.md`): a bundle import is a COPY (source), libraries are
installed and namespaced, identical declarations merge.

### 5a. Names when code comes from several places
- Entities files shipped inside installed SciStack packages (listed in
  `scistack.toml` `packages`) are read; today only the project's own file is.
- Merge rule (one owner, in scidb): the same Variable name declared the same
  way is one type; a different definition (schema_version, custom
  to_db/from_db) is an error naming both sources and which to remove.
- Functions from an installed package are qualified `package.fn`, through the
  same mechanism as library functions (`library_functions.
  with_qualified_name`), so the recorded name equals the canvas label.
- An installed package's Parameters and PathInputs are not registered into
  the project's names.
- Tests: two packages with the same function name both load, qualified;
  identical Variable declarations merge; conflicting ones fail with both
  sources named; a library PathInput is not on the project's palette.

### 5b. Code and environment sections
- `code` section: the project's source tree, as files (pyproject.toml,
  `src/<pkg>/`, other code files under the root that discovery loads, glue
  files, `.m` files, the entities file). A file discovery loads from OUTSIDE
  the root is listed in the report, not copied (Stage 6 roots).
- `env` section (scidb): Python version, platform, installed distributions
  with versions (`importlib.metadata`), MATLAB release if a MATLAB engine
  is already running in-process (never started for this).
- Import: the code section is written before `init_project` (a new
  "files" phase in `scidb.bundle.import_project`, before the database
  opens), so the new project's `src/<pkg>/` IS the exporter's code (copy).
  Dependencies are compared with the env record; missing or different
  distributions are reported with the `pip install` command. Nothing is
  installed automatically and nothing is imported (trust: Stage 8).
- Tests: bundle round trip reproduces the source tree byte for byte; files
  outside the root are reported; env section lists the running Python; a
  missing distribution is reported.

Moved to Stage 10: building wheels, the optional wheelhouse.

As built: `scidb/names.py` (one owner of the recorded function name; switched
foreach, CallSite, StepSpec, node state, the GUI registry and code export);
`variable.same_definition` + `BaseVariable.definition_conflicts`;
`registry._load_library_entities`; env section and `check_environment` in
`scidb/bundle.py` with a "files" import phase and unsafe-path refusal;
`bundle_section.CodeSection` (in the GUI package: the config loader owns
which files are code). Tests: `scidb/tests/test_names.py`, `test_bundle.py`,
`scistack-gui/tests/test_library_names.py`, `test_bundle_project.py`.

## Stage 6: schema choice + PathInput roots on import — DONE 2026-10-08, tests pass

As built (D-2026-10-08-7):
- `scidb/schema_map.py`: `KeyMap.auto(exporter, recipient, overrides)` and
  one renaming per shape (`level`, `locations`, `template`, `table`,
  `exact_strings`), reporting into a `MapReport` (dropped / flagged).
  `scifor.locations.LocationFilter.without_keys` added beside `renamed`.
- `scidb.bundle.import_project(schema_keys, key_map, path_roots)`: config
  tables remapped and the exporter's absolute in-project paths made
  relative (manifest now records the exporter's root); PathInput templates
  and `root_folder`s rewritten in the new project's entities TOML; a
  `schema` and a `path_inputs` entry in the report.
- GUI section: `canvas_snapshot.remap_schema_keys` (node location
  statements, submodule bindings) before apply. Plots section: exact-key
  renaming of each envelope, flagged.
- `headless.import_project_bundle(schema_keys, key_map, path_roots)`.
- Tests: `scidb/tests/test_schema_map.py`, Stage 6 block in
  `test_bundle.py`, `scifor/tests/test_locations.py`
  (`without_keys`), `scistack-gui/tests/test_bundle_project.py`,
  `scistackplotdb/tests/test_saved_plots.py`.

Not done here: the GUI's first-write seeding still writes absolute paths
(import now relativises them, which is what portability needed); PathInputs
declared in Python source are not rewritten; refusing history/data into a
changed schema is Stage 7; the dialogs that collect the schema, key map and
roots are Stage 8.

## Stage 7: history and data sections — DONE 2026-10-08, tests pass

As built (D-2026-10-08-8, user decision: history without data is an archive):
- `scidb/table_copy.py`: dump/load tables between databases (DDL from
  `duckdb_tables()`, rows as Parquet, views from `duckdb_views()`); load
  empties an existing table and inserts the shared columns.
- `scidb.bundle`: `history` section (`HISTORY_TABLES`: provenance + `_schema`
  + exclusions, variant pins, tombstones; default on) and `data` section
  (variable tables, `_record_save`, variable metadata, views; opt-in, refused
  without history). Import: history + data + same schema -> live verbatim
  (`ImportContext.history_live`); otherwise archived under
  `.scistack/archive/<export time>/`, exclusions (`INTENT_TABLES`) live when
  the schema was kept; `import_history=False` declines it.
- GUI section verbatim mode (with data): every GUI table
  (`table_portability()`) and the layout file copied exactly; the snapshot
  path otherwise.
- Tests: Stage 7 block in `scidb/tests/test_bundle.py`; two tests at the end
  of `scistack-gui/tests/test_bundle_project.py`.

## Stage 8: CLI + GUI front ends — DONE 2026-10-08, tests pass

As built (D-2026-10-08-9):
- `scistack_gui/bundle_cli.py` owns the commands `export [OUT] [--db]
  [--history/--no-history] [--data/--no-data]`, `import BUNDLE [--into]
  [--schema ...] [--map OLD=NEW] [--path-root NAME=DIR]
  [--history/--no-history] [--check-code] [--trust]` and `bundle-info`,
  each with `--json` (one line, last). `scistack/__main__.py` mounts the
  same parsers. `EXPORT_FLAGS` + `export_choices()` are the one list of
  offered options; defaults read from `ExportOptions` and
  `import_project`'s signature. `--wheelhouse` is not offered (Stage 10).
- `scidb.bundle.preview` (what an import dialog needs: schema, sections,
  PathInputs, environment; config + files-phase sections unpacked into a
  temp folder) with `_declared_path_inputs` shared with the import rewrite;
  `ImportReport.to_dict`.
- `headless`: `export_open_project` (the one export composition, used by
  the GUI handler and `export_project_bundle`), `preview_bundle`,
  `import_project_bundle(import_history=)`, `check_code` (I15, discovery
  on, after trust). `portability_service.unresolved_labels` made public.
- GUI: `api/bundles.py` (`get_export_options`, `export_project_bundle`);
  `ProjectExportButton.tsx` in the tab strip. Import is the extension
  command `scistack.importProjectBundle` (`bundleImport.ts` +
  vscode-free `bundleImportCore.ts`), which runs `python -m
  scistack_gui.bundle_cli` in its own process, shows the report as
  Markdown and asks for trust before opening. The browser build has no
  import (the terminal covers it).
- Tests: `scistack-gui/tests/test_bundle_cli.py` (GUI handler and headless
  export write identical bundles; CLI and headless import make the same
  project), `scidb/tests/test_bundle.py` (preview, JSON report),
  `extension/src/bundleImportCore.test.ts`.

Originally planned:

- CLI: `scistack export [--with-data] [--no-history] [--wheelhouse]`,
  `scistack import <bundle> [--into <project>] [--trust] [--schema ...]
  [--map old=new] [--path-root NAME=/path] [--no-history] [--check-code]`.
  Flag defaults read from `ExportOptions`.
- GUI: Export dialog (checkbox defaults from an RPC that returns
  `ExportOptions`); Import as a new project or into the current one; trust
  prompt; schema choice + key map; PathInput roots; report view.
- `docs/gui-manual-testing-todo.md` entry.
- Test: a headless round trip and a GUI-handler round trip of the same
  project produce identical results.

## Stage 9: verify (+ tolerance); subset deferred — 9a DONE 2026-10-09, tests pass

As built:
- **scidb:** `scidb/verify.py`; `.scistack/import.json` on every import
  (`bundle._write_import_record`); the `scidb verify` command
  (`inspect/cli._cmd_verify`; dispatch honours a handler's exit code);
  the `scistack verify` alias.
- **GUI:** the `verify_reproduction` handler (`api/bundles.py`);
  `VerifyButton.tsx`.
- **Tests:** `scidb/tests/test_verify.py`. Docs: D-2026-10-09-2;
  GUI §0zzzl.

Built now (user, 2026-10-09): 9a verify, with a numeric tolerance when the
exporter's data is available. 9b subset stays deferred (no concrete need).

**Tolerance (added to 9a):**
- The exporter's data exists only in a bundle file: archives hold history
  only, and a bundle imported with its data goes live. So the tolerance
  workflow is: export WITH data; import into a fresh project
  `--no-history`; run the pipeline on your own raw files; then
  `verify --against the.scistack`.
- A record whose content hash differs, and whose data rows both sides have,
  is compared value by value (`rtol` / `atol`, defaults 1e-6 / 1e-12,
  flags `--rtol` / `--atol`). Rows go in stored order; numbers and arrays
  element-wise; anything else must be equal. Within tolerance → class
  `within_tolerance`.
- Without exporter data the comparison is exact, and the report says so.

**Recorded at import (always, not only with an archive):**
`.scistack/import.json` holds the exporter's schema keys, the key map, the
manifest's options and `exported_at`. Verify reads the key map from it.

User decisions 2026-10-08:
- `verify` COMPARES only. The user runs the pipeline as usual (GUI Run or
  their scripts) on their own raw files; verify never executes code.
- Anonymize DROPPED. Subject codes are normally pseudonyms already, real
  identifiers belong upstream of the raw files, values inside the data would
  not be covered, and pseudonyms would defeat `verify`. Revisit only for a
  concrete requirement (a journal or a data-use agreement). Subset stays.

### 9a. `verify`: the exporter's history vs your re-run

**Owner: `scidb/verify.py`.**
- Inputs: the live database, and the exporter's history from either:
  - an archive folder (default: the newest under `.scistack/archive/`), or
  - `--against <archive folder | bundle.scistack>`.
- History imported LIVE (bundle with data) is the exporter's own, so there
  is nothing to compare in place. Verify a fresh project against the bundle
  file instead; the error message says so.
- The archive side is read from its Parquet files into a separate in-memory
  DuckDB connection, never into the live database.

**Structural key** (one owner, `structural_key`). It is content-free, so
records line up across the two histories even where upstream content differs:
- record = (type, schema_version, location, key of the producing invocation).
  The location is the non-null schema columns of its `_schema` row.
- invocation = (function_name, function_hash, as_table, distribute,
  across_variants, sorted bindings).
- binding = (param, selector, then one of: constant content_hash, PathInput
  name, or the input record's KEY).
- `__save__` (direct saves): only its constant kwargs.
- Exporter locations are renamed through the import's key map. A dropped key
  makes those records "not comparable" (reported, not guessed).

**Classes, for each non-excluded exporter variable record:**
- reproduced: same key, same content_hash;
- differs at source: same key, different content; every variable input reproduced;
- differs downstream: same key, different content, and an input differs;
  names the first divergence upstream;
- code changed: the key matches except function_hash;
- not run: no live record with the key;
- new: live records with no exporter key, listed separately.

When the exporter's bundle was a subset (9b), live records outside it are
ignored. When one side has several records for a key, the newest
(`created_at`) is compared and the count of older ones is reported.

**Recorded at import (prerequisite):** `import.json` in the archive folder,
holding the exporter's schema keys, the key map, the manifest's options
(including any subset) and `exported_at`.

**Report:** counts per (function, variable) and class; first divergences
with location and both content hashes; `--json`. Exit codes: 0 all
reproduced, 2 differences, 1 error.

**Logging:** INFO counts per class; timings for load / keys / compare
(diagnostics for big histories).

**Front ends:**
- `scidb verify [--db] [--against] [--json]`, a read-only inspect command in
  `scidb.inspect.cli`. `scistack verify` mounts the same handler.
- GUI: an RPC `verify_reproduction` that runs in the session process (a
  separate process cannot open the locked database) and a "✓ verify"
  popover in the tab strip. It lets you choose an archive and shows the
  summary and the first divergences.
- `docs/gui-manual-testing-todo.md` entry.

**Tests (scidb), on a small pipeline exported without data, imported (so
the history is archived), then re-run:**
- all reproduced;
- a changed raw value → differs at source at that location, downstream below it;
- an edited function → code changed;
- a location not re-run → not run;
- an extra subject → new;
- a schema renamed via the key map → still matched;
- a dropped key → not comparable;
- duplicate keys → the newest is compared;
- the archive is never loaded into the live database.

### 9b. Subset export

**Owner: `scidb/subset.py: Subset`,** built from a `scifor.LocationFilter`
(the one meaning of a location selection). It has one method per shape,
like `KeyMap`; each section applies it to its own data and reports what
was removed.

**`ExportOptions.locations`:** the `LocationFilter` dict, or None for
everything. It is recorded in the manifest.

**History + data rule:**
- A variable record is kept iff its location matches AND all its variable
  inputs are kept (transitive). So an aggregate computed over removed
  locations is dropped and reported; its value describes data the recipient
  will not have.
- Constant and PathInput records are kept when a kept invocation uses them.
- An invocation is kept iff its inputs are kept and at least one output is.
- A run is kept iff it keeps at least one invocation.
- `__scidb_schema_overrides` rows are filtered by location.
- Variant pins and tombstones are kept (they are per variable, not per
  location). Checked in implementation.
- Data tables and `_record_save` are filtered by kept record_id.
- Mechanism: `table_copy.dump(..., keep=...)` filters rows through TEMP
  tables of kept ids in the exporter's own connection, dropped afterwards.

**Config, canvas and plots:** values outside the subset are removed from
the `[schema_keys]` level lists, node location selections and per-level
plot settings (colours, reference level). It is reported.

**Front ends:**
- CLI: `scistack export --only KEY=V1,V2` (repeatable; ANDed across keys;
  becomes `exclude_levels` of the complement, using the database's values)
  and `--exclude KEY=V1,V2`.
- GUI: the existing schema-location picker in the export popover, which
  yields the `LocationFilter` dict directly.

**Tests:**
- an aggregate over a removed subject is dropped;
- constants are kept;
- data rows are filtered;
- config levels and node selections are filtered;
- a subset bundle with data imports live and is consistent (every kept
  invocation's inputs exist);
- verify against a subset bundle ignores live records outside it.

### Order
9a core (`import.json`, `scidb.verify`, tests) → 9a CLI → 9a GUI →
9b core (Subset, table_copy keep, history/data) → 9b config/canvas/plots →
9b CLI/GUI → docs (portability.md E15 + I17, decisions D-2026-10-08-10,
manual testing doc).


## Stage 10: share a submodule as a library; use or copy it — PLANNED 2026-10-08, awaiting approval

Taken BEFORE Stage 9 (user, 2026-10-08).

User decisions 2026-10-08:
- **Use = seed, lock, re-sync.** A library's pipeline is seeded into the
  canvas tables like today's source pipelines, but owned by the library:
  - every structural edit is refused, and "Make my own copy" is offered;
  - when the installed library's definition changes, it is re-seeded in
    place with stable node ids.
  - Rejected: rendering live from source, which would need a second,
    virtual kind of pipeline in the graph builder, the run compiler and
    node state.
- **Copy replaces the library.** "Make my own copy" copies the library's
  source to `src/<pkg>/<lib>/`, and EVERY placement of that library in
  this project switches to the copy:
  - function names stay `lib.fn`;
  - the library leaves `packages`;
  - one source per name; copying is all-or-nothing per library.

Scope: Python, MATLAB and mixed submodules (user, 2026-10-08: MATLAB is an
important part). MATLAB fits the naming rule natively:
- `func2str(@lib.fn)` is `lib.fn`, so a MATLAB library is a `+<lib>/`
  folder and its functions are recorded as `lib.fn` without help;
- the GUI's run commands already emit `@<name>`.
SciStack still never installs anything itself: it shows (or, in VS Code,
offers to run in a terminal) the `pip install` command.

**Library pipeline format = canvas snapshot documents** (revised
2026-10-08, replacing `pipelines.py`):
- Each shared submodule ships as `src/<lib>/pipelines/<name>.json`, a
  `canvas_snapshot` document (the Stage 2 owner of copying a canvas). It is
  language-neutral, so Python, MATLAB and mixed submodules all work.
- Re-sync hashes the document.
- Script users generate a `.py` / `.m` from it with the existing code
  export.
- Hand-written Python `scidb.Pipeline`s inside a library are still
  discovered (today's path). Both kinds become library-owned pipelines
  through the SAME seed/lock owner.

### 10a. Library format, loading, placing (use) — DONE 2026-10-08, tests pass

As built:
- **scidb:** `scidb/library.py` (document format, `read_library`,
  `matlab_dir`); `entities.parse_library_table`.
- **MATLAB:** `matlab_parser.matlab_package_prefix` / `matlab_path_entry`;
  `config._library_matlab_sources`.
- **Store:** `pipeline_store._library_pipelines`, `GUARDED_WRITES`,
  `library_owner` / `set_library_owner` / `release_library_owner` /
  `clear_pipeline_content`.
- **Ids and lock:** `ids.library_*_id`; `library_lock`, marked as a user
  edit in `Handler.invoke`; `intent_store` guards plus `delete_scope`.
- **Seeding:** `canvas_snapshot.apply(node_id_for, use_id_for)`;
  `services/library_service` (sync, list, `suggest_key_map`), called from
  `pipeline_discovery.discover_and_seed_pipelines`.
- **Front ends:** `api/libraries.py`; `config.add_package` /
  `remove_package`; `library_cli` mounted by `scistack`; `bundle_section`
  carries library pipelines by reference; frontend `LibrariesSection.tsx`
  and the key-map step in the drop handler.
- **Tests:** `scidb/tests/test_library.py`,
  `scistack-gui/tests/test_libraries.py`; `test_config` package test.
- **Docs:** D-2026-10-08-10/11, GUI §0zzzh.

Known gaps: hand-written Python `scidb.Pipeline`s inside a library are
still seeded the old way (editable, create-once), not as library-owned.
Libraries found only through entry points are not scanned for MATLAB. The
bundle key map renames the child side of a library placement's `key_map`
as if it were the exporter's vocabulary.

**What a library is on disk:** an ordinary package, which can live in its
own git repo:
- `pyproject.toml` (name, version, dependencies);
- `src/<lib>/` holding the copied Python function modules, `pipelines/`
  and `scistack_entities.toml`;
- `src/<lib>/matlab/+<lib>/` for MATLAB functions (package data, like
  scimatlab's own `+scidb/`);
- in `pipelines/`, one snapshot document `<name>.json` per shared submodule;
- in `scistack_entities.toml`, the Variables the pipelines use, plus a new
  `[library]` table with `schema_keys = [...]`, the library's native
  vocabulary for the key map. The grammar owner is `scidb.entities`.

**Library pipelines are iteration-free.** A snapshot carries no location
selections (the generator strips them), and generated scripts use `key=[]`
("every value"). The project's concrete subject lists never enter a
library. The project chooses values through the placement binding's
`iterate`, and its schema through `key_map`.

**MATLAB discovery and path:**
- `+package` folders are discovered, no longer skipped
  (`config._is_matlab_skip_dir`). A function in `+a/+b/f.m` is named
  `a.b.f`, which is what `func2str` records.
- Each installed library's `matlab/` folder is added to the MATLAB path the
  GUI builds (`matlab_addpath`), and its functions are registered (I6).

**Loading:**
- For each library in `packages`, `registry` reads
  `<lib>/pipelines/*.json` through `importlib.resources`, and notes which
  hand-written `Pipeline`s the package registered.
- `pipeline_discovery` seeds both as LIBRARY-OWNED pipelines; a snapshot
  through `canvas_snapshot.apply`.
- A new table `_library_pipelines(pipeline_id, library, pipeline_name,
  definition_hash)` (CREATE IF NOT EXISTS: no migration), classified in
  `PORTABILITY`.
- Library-owned pipelines are not tabs. They appear in the sidebar's
  Libraries list, which is new, to place into a scope.

**Re-sync:** `definition_hash` is the SHA-256 of the snapshot document
(or the hand-written `Pipeline`'s step signatures). When it changes at
load:
- nodes and edges are replaced, with deterministic ids derived from
  (library, pipeline, the node's id in the document);
- placements and their bindings are kept;
- logged at INFO with the old and new hash.

**Lock (one owner):** `pipeline_store.assert_editable(db, pipeline_id)`.
- Every store-level write of nodes, edges, node config or hidden state
  calls it, guarded by a test like the existing table-classification guard.
- It raises `LibraryPipelineReadOnly`; handlers turn that into a reply the
  frontend shows as "This submodule comes from library X. [Make my own copy]".
- What the project still sets: the placement binding (`key_map`, `params`,
  `iterate`) and dataset exclusions.

**Placing:**
- Placing a library pipeline is `add_pipeline_use` (the existing machinery).
- When the library's `schema_keys` differ from the project's, the placement
  dialog suggests a key map with `scidb.schema_map.KeyMap.auto` (Stage 6)
  and stores it as the binding's `key_map`.
- Chaining across libraries works by matching variable names, or by port
  bindings / glue (existing).

**Adding a library:**
- The Paths popup gains a Libraries list: add or remove a package name in
  `scistack.toml` `packages`, through `scidb.config_file`.
- `scistack library add NAME` is the CLI twin.
- An uninstalled name is reported with the pip command to install it.

**Bundles:** the GUI section marks library-owned pipelines. Import
re-seeds them from the installed library. A missing library is reported by
the existing env check.

### 10b. Share as library (generator) — DONE 2026-10-08, tests pass

As built:
- scidb: `library.create_library` / `entities_text`; `read_library`
  gained `parameter_defaults` / `path_input_defaults`.
- GUI: `services/library_share.py`; `library_service.placement_check` /
  `declare_requirements`.
- API: `library_placement_check` (replaces `suggest_library_key_map`),
  `declare_library_requirements`, `share_library_defaults`,
  `share_as_library`.
- Front ends: `ShareLibraryButton.tsx`; the drop handler asks to declare;
  `scistack library create`.
- Tests: `scistack-gui/tests/test_library_share.py`, a requirements test in
  `test_libraries.py`, `create_library` tests in
  `scidb/tests/test_library.py`. GUI §0zzzi.
- Not done: switching the source project over to the library; glue nodes
  are refused.

User decision 2026-10-08, **Parameters and PathInputs = requirements +
defaults:**
- The library's entities file declares the Parameters and PathInputs its
  pipeline uses, with the exporter's values / templates as DEFAULTS.
  PathInputs carry no `root_folder`: that is the project's data layout.
- They are never registered into a project (Stage 5 rule).
- Runs resolve those nodes by name in the PROJECT's registry. So placing a
  library pipeline lists the ones the project lacks, and offers "Declare
  from library defaults", which writes them into the project's own entities
  file (`parameter_service.create_parameter` /
  `path_input_service.create_path_input`). Nothing new at run time.

**Owner of the package files:** `scidb.library.create_library(dest, name, *,
files, documents, variables, parameters, path_inputs, schema_keys,
dependencies)`. It is a pure writer:
- writes `pyproject.toml` (hatchling, like `scidb.project`), the copied
  code, the `pipelines/*.json` documents and the entities file with its
  `[library]` table;
- refuses a non-empty destination or an invalid name.

**GUI composition:** `services/library_share.share_as_library(db,
pipeline_id, dest, name)`.
- **Closure:** the closure (`_closure_pipeline_ids`) is captured
  (`canvas_snapshot.capture`). A library-owned pipeline inside it is
  refused: another library's pipeline cannot be re-shipped.
- **Python functions:**
  - Each function label resolves through the registry.
  - Another library's function, or a built-in library reference like
    `pandas.read_csv`, keeps its label and becomes a dependency.
  - The project's own function is copied with its module. The project-local
    modules it imports are copied too (transitively, by AST). Own package
    `pkg.a.b` → `<lib>/a/b.py`; a loose module `m` → `<lib>/m.py`.
  - Imports are rewritten (`pkg.` → `<lib>.`; `import m` →
    `from <lib> import m`).
  - An unresolvable function is refused.
- **MATLAB functions:**
  - Each `.m` file goes to `matlab/+<lib>/` (a `+a/+b/` package keeps its
    nesting under `+<lib>`), with the project-local helpers it calls
    (found by name, transitively).
  - Calls and `@handles` to copied functions are qualified as `<lib>.fn`.
    Lines that are comments are left alone; anything unparseable is
    reported.
- **The document:**
  - Function labels become `lib.fn`.
  - Location selections (`schemaSelection`) are stripped; levels are kept.
  - Variables come from the variable nodes. Parameters and PathInputs
    (from the parameter and PathInput nodes) are written with their
    registry values / templates as defaults.
- **Schema keys:** the project's.
- **Dependencies:** the distributions of the third-party modules the copied
  Python imports (`importlib.metadata.packages_distributions`, stdlib
  excluded), plus other libraries used.
- **Report:** files, rewritten imports and calls, requirements, anything
  refused or unresolved, and the install command
  (`<python> -m pip install -e <dest>`).

**Placing requirements:** `library_service.placement_check(db,
pipeline_id)` = the key map suggestion plus the missing Parameters /
PathInputs with their library defaults. `declare_library_requirements`
writes the chosen ones. The drop handler asks before placing.

**Front ends:**
- CLI: `scistack library create --from-submodule NAME --into DIR [--name
  LIB] [--db]` opens the project WITH discovery, like export.
- GUI: a ⇪ button on a submodule row → a popover (library name, folder;
  defaults from the `share_library_defaults` RPC) → the report.

**Deferred to the end of 10b:** "switch this project to the library".


### 10c. Make my own copy — DONE 2026-10-09, tests pass

As built:
- **Naming:** `scidb.names.copied_libraries` / `library_of` (the
  `copied_libraries` key).
- **Rewriting:** `scistack_gui/code_rewrite.rewrite_imports` (the one
  owner; `library_share` now uses it).
- **Service and config:** `services/library_copy.make_own_copy`;
  `config.mark_library_copied`.
- **Front ends:** the `make_library_copy` handler; ✎ in
  `LibrariesSection`; `scistack library copy`. The lock message names
  them.
- **Tests:** `tests/test_library_share.py` (10c part),
  `tests/test_code_rewrite.py`, and `scidb/tests/test_names.py`. GUI
  §0zzzj.
- **Not undoable:** it writes a whole source tree; the report lists every
  file.

Originally planned:

- Offered when an edit hits `LibraryPipelineReadOnly`, and from the
  Libraries list.
- The installed library's source is copied into `src/<pkg>/<lib>/`
  (`importlib.resources` / the distribution's files). There is a refusal if
  that folder exists.
- `scistack.toml` gets `copied_libraries = ["<lib>"]` and loses `<lib>`
  from `packages`.
- **Names:** `scidb.names` (the one owner) qualifies a function from
  `<pkg>.<lib>.*` as `<lib>.fn` when `<lib>` is in `copied_libraries`. So
  history and canvas labels do not change, and nothing re-runs that was
  current.
- Every library-owned pipeline of `<lib>` becomes an ordinary editable
  pipeline: its `_library_pipelines` rows are removed (this is a status
  change, not deleted data); ids, placements and bindings are kept.
- The import paths inside the copied modules are rewritten from `<lib>.` to
  `<pkg>.<lib>.`, with the same rewriter as 10b.
- MATLAB: `matlab/+<lib>/` is copied into the project's MATLAB sources.
  Names stay `lib.fn` natively, and no rewrite is needed.

### 10d. Wheels and the wheelhouse; install after trust — DONE 2026-10-09, tests pass

As built:
- **scidb:** `scidb/environment.py` (`install_missing`, `plan_install`,
  `install_project_requirements`, `build_wheel`, `library_wheel`, `_pip`).
- **Bundle:** the env section records `libraries`
  (import name → distribution, version); `check_environment` gains
  `missing_requirements` / `missing_libraries`; the `wheelhouse` section
  (`wheelhouse.json` index); import keeps `.scistack/environment.json` and
  `.scistack/wheelhouse/`.
- **CLI:** `--wheelhouse` in `EXPORT_FLAGS` (so also a GUI checkbox);
  `scistack import --trust [--no-install]`, `scistack install [PROJECT]
  [--yes]`, `scistack library build`.
- **Extension:** "Trust, Install and Open" (`buildInstallArgs`,
  `describeInstall`).
- **Tests:** `scidb/tests/test_environment.py`, the wheelhouse tests in
  `scidb/tests/test_bundle.py`, and `bundleImportCore.test.ts`.
- **Docs:** D-2026-10-09-1; GUI §0zzzk.


- `scistack library build DIR` runs `pip wheel --no-deps -w DIR/dist DIR`
  (`sys.executable -m pip`).
- `ExportOptions.include_wheelhouse` gets its flag `--wheelhouse` (added to
  `EXPORT_FLAGS`, so it is also a GUI checkbox). The bundle's `wheelhouse/`
  section holds a wheel per library in `packages`, built from its installed
  distribution (an editable install builds from its source path).
- Import writes them to `.scistack/wheelhouse/`, and the report gives
  `pip install --no-index --find-links .scistack/wheelhouse <libs>`.
- **Install after trust (user, 2026-10-09; supersedes "nothing is installed
  automatically").** Importing installs what the project needs, but only
  after the user trusts the bundle:
  - GUI: "Trust, install and open"; CLI: `--trust` or a yes at the prompt.
  - Never the project's own package, which is discovered as source.
- **What is installed:** the declared dependencies plus the listed
  libraries, only those MISSING. A version that differs from the
  exporter's is reported and never changed.
- **The install, one owner** (`scidb.environment.install_missing`):
  1. **Check first.** `pip install --dry-run --report` resolves the whole
     tree. If ANY resolved package would upgrade, downgrade or replace an
     installed one, or touch SciStack's own distributions, nothing is
     installed and the conflicts are listed.
  2. **Install exactly that.** The resolved list goes in pinned, with
     `--no-deps`, so pip cannot re-resolve differently between check and
     install. Dependencies like pytorch ARE installed: they are in the list.
  3. **Roll back on failure.** Pip is not transactional, but everything
     installed was new, so it is uninstalled again and the environment is as
     it was.
  4. **Only in a virtual or conda environment.** A system Python is never
     installed into; the report says why.
  5. **The source:** the bundle's wheelhouse when it has one (`--no-index
     --find-links`), else the package index.
- **The import never depends on the install.** A stopped or failed install
  is reported with the command to finish by hand, and the import goes on.

### Logging and diagnostics
- `[library]` INFO lines: what was copied, rewritten and refused; seed and
  re-sync with hashes.
- Every `LibraryPipelineReadOnly` refusal names the handler and the library.

### Tests
- **10a:**
  - an entities `[library]` table parses;
  - a library package's snapshot document (and a hand-written `Pipeline`)
    seeds as library-owned and is not a tab;
  - MATLAB `+lib/f.m` is discovered as `lib.f`, an installed library's
    `matlab/` lands in the addpath list, and a run command emits
    `@lib.f`;
  - every store write path refuses inside it (guard test);
  - a binding edit is allowed;
  - a changed definition re-seeds with stable ids and keeps placements;
  - placing it into another schema suggests and stores a key map, and a run
    compiles with it;
  - the bundle round trip re-seeds from the installed library.
- **10b:**
  - the generated package installs from source in a temp dir (importable
    via `sys.path`, never pip in tests);
  - its pipeline seeds back into the same canvas shape (labels, edges,
    ports) as the source submodule;
  - iterables are `[]`;
  - a project-internal import is rewritten;
  - a MATLAB function is copied into `matlab/+<lib>/` and its call to a
    copied helper is qualified;
  - a mixed Python + MATLAB submodule round-trips;
  - a non-empty destination is refused.
- **10c:**
  - the copy lands in `src/<pkg>/<lib>/`;
  - names stay `lib.fn` (`scidb.names` tests);
  - pipelines become editable with the same ids;
  - `packages` / `copied_libraries` are updated;
  - an existing folder is refused.
- **10d:**
  - `--wheelhouse` default from `ExportOptions`;
  - the bundle carries the wheels;
  - the import report gives the install command (pip is mocked in tests).

### Order
10a (format + loading + lock + placing) → 10b (generator, CLI, GUI) →
10c (copy) → 10d (wheels). Each sub-stage gets its decision record
(D-2026-10-08-10 use = seed/lock/re-sync, -11 copy replaces), its
`docs/gui-manual-testing-todo.md` entry and a `docs/claude/portability.md`
update.

## Order and dependencies

0 → 1 → 2 → 3 → 4 → {5, 6, 7} → 8 → 10 → 9 (user, 2026-10-08: Stage 10 before Stage 9).
Stages 5–7 are independent of each other once 4 lands; 6 needs Stage 2's
schema-key remap hooks.

## Notes from Stage 1 (2026-10-08)

- Extra owners created: `scidb.config_file` (the only writer of
  scistack.toml; keeps unknown keys), `scifor.discovery.own_package_dir`,
  `scidb.entities.default_entities_relpath(root, package=None)`.
- GUI first-write seeding (`config._first_write_seed_roots`) still writes the
  project root and database directory as ABSOLUTE paths into `modules` /
  `[matlab] sources`. That is a portability bug for Stage 4/6 (a config
  exported from one machine names another machine's folders). `init` seeds
  `"."` instead.
- `scidb.discover.scan_project` lost `skip_dists`/`library_filter` (they only
  filtered uv.lock entries).
- Browser wizard: creation can no longer opt out of an entities file.
