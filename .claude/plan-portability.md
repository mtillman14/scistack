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

## Stage 6: schema choice + PathInput roots on import

- Schema choice (I8): propose the exporter's schema; accept, or enter your
  own. One key map (exporter key → recipient key | none), auto-matched by
  name. Applied by the Stage 2 walker through each store's schema-key remap.
  Unmapped keys are cleared and flagged in the report. Where filters are
  kept and flagged for review. PathInput template placeholders are renamed
  through the map.
- With the recipient's own schema: history and data sections are not
  imported (enforced in `import_project`, logged, reported).
- PathInput roots (I7): show each PathInput's exported `root_folder`, ask
  for the recipient's location, and write it to the entities file. Never
  read, copy or hash raw files. CLI: `--path-root NAME=/path`.
- Tests: kept schema imports untouched; renamed key remaps node level,
  template placeholders and plot roles; dropped key clears and flags; new
  schema refuses the history section; root change doesn't change any
  record ID.

## Stage 7: history and data sections

- Export: the provenance tables + `_schema` + `_function_source` to Parquet;
  data opt-in (the variable tables + `_record_save`).
- Import: only into a new, empty project with the exporter's schema kept.
  Verbatim copy, no deduplication. Refuse otherwise with a clear message.
  Import-time opt-out for history (default on when present).
- Tests: export → import reproduces every table row-for-row; refusing a
  non-empty target; refusing a changed schema.

## Stage 8: CLI + GUI front ends

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

## Stage 9 (later): verify, subset/anonymize

- `scistack verify`: re-run against the recipient's own raw files, compare
  content hashes with the imported history, report records that differ.
- Subset/anonymize exports (E15).


## Stage 10: share a submodule as a library; use or copy it

- "Share as library" on a submodule tab: generate a package from it (the
  functions it uses, their declarations, the submodule as a source-defined
  `scidb.Pipeline`, written by the existing code export), optionally built
  as a wheel. The source project can switch to the library.
- "Add library" + place its submodule: read-only on the canvas, functions
  qualified (5a), placed through the Stage 6 key map when schemas differ,
  chained to other submodules by name or by port bindings.
- "Make my own copy" when the user tries to edit a library submodule: copy
  into `src/<pkg>/<lib>/` (functions qualified by the subpackage, 5a), like
  Duplicate.
- Optional wheelhouse for archives (`pip wheel -w`).

## Order and dependencies

0 → 1 → 2 → 3 → 4 → {5, 6, 7} → 8 → 9; 10 after 5, 6 and 8.
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
