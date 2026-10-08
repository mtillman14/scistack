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

## Stage 2: portable-state declaration + import fixes (GUI section)

- Each GUI store declares the tables or files it contributes to an export,
  beside its `CREATE TABLE`, with two functions: an ID remap and a
  schema-key remap. This is the same pattern as
  `UNDOABLE_TABLES`/`history.py`. Stores: `pipeline_store`, `intent_store`,
  `node_wiring`, `layout` (positions, palette, notes), `_variant_pin`
  (scidb), scistackplotdb saved plots and presets.
- `portability_service` becomes a walker over that declaration (E3).
- Fix E5: export settings for run nodes too (`get_node_configs`).
- Fix I10/I11: mint node IDs through `ids.py` and edge IDs through
  `graph_builder.connection_id`. Guard test: portability code builds no ID
  strings itself.
- E7: hidden nodes/edges/combos exported when history is in the bundle.
- E9: stop resolving Parameter/PathInput values through the registry for
  export (the entities file travels whole); resolve doc open Q2 (palette).
- Positions missing → automatic layout on import.
- Tests: round trip of every declared store; a newly added table with no
  portable/not-portable declaration fails a test; extend
  `tests/test_portability.py`.

## Stage 3: open the project without the GUI, without discovery

Discovery exists because the GUI can't see what a script imports. Export
and import don't need it:

- Export reads stored state (tables, `layout.json`), builds the wheel from
  `pyproject.toml`, and copies the entities file. None of that needs to know
  which functions exist.
- Import writes stored state, installs the wheel and writes files. Again,
  no function lookup.
- Skipping discovery also means **import never runs the bundle's code**.
  Discovery imports user modules, and that would run code before (or
  without) the trust prompt.

The one step that does need discovery is I15 ("does every canvas node's
function exist?"). It runs when the GUI opens the imported project, which it
does anyway, or from the CLI on request (`scistack import --check-code`, after
the trust prompt).

- `open_project_headless(root)`: opens the db and config only, no discovery.
- Give `portability_service` the opened project explicitly instead of
  reading GUI process state (`scistack_gui.db.get_db_path()`,
  `registry.*`).
- Test: export from the headless open equals export from a live GUI
  session; an import test asserts no user module was imported.

## Stage 4: bundle format + options (scidb)

- `scidb.bundle`: `ExportOptions` (the one owner of defaults:
  `include_history=True`, `include_data=False`, `include_wheelhouse=False`),
  `Manifest`, `FORMAT_VERSION`, `.scistack` writer/reader (a plain zip) with
  content hashes.
- Section-provider interface: scidb owns `code`, `env`, `config`, `history`,
  `data`. scistack-gui registers `gui`.
- `export_project(root, options) -> Path`,
  `import_project(bundle, target, options) -> ImportReport`.
- Tests: manifest round trip; refuses an unknown format version; a bundle
  with no `gui/` section imports cleanly.

## Stage 5: code + environment

- Read entities files shipped inside installed SciStack packages (moved
  from Stage 1): today only the project's own file is loaded
  (`scidb.entities.load_for_project`). Name collisions between the project
  and an installed package are a load error, as for duplicate PathInputs.
- Build the project wheel (PEP 517 build of the project), with `.m` files
  and the entities file as package data. Record resolved versions
  (`pylock.toml` if the installed pip supports it, else a `pip freeze`-style
  list), plus Python version, platform and, if available, MATLAB `ver`.
- Optional wheelhouse (`pip wheel -w`).
- Loose-file projects: refuse with "run `scistack init`", which makes them
  packages.
- I16: test that a function's identity is the same loaded as a loose file
  and installed from the wheel. If it differs, fix in the identity owner.
- Install on import (I5), after the trust prompt, into the running
  interpreter; MATLAB path setup in scimatlab (I6).

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

## Order and dependencies

0 (independent, do first) → 1 → 2 → 3 → 4 → {5, 6, 7} → 8 → 9.
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
