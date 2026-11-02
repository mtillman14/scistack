# Plan P3 — one home for project runtime; one owner of the project database schema

*Index: `.claude/plan-tidy-00-index.md` (global rules apply). Themes T1 and T2
in `docs/design-review/09-synthesis.md`. Decisions D2 and D3. Refactor IDs:
R3′, R4 (doc 02); B3 and its answer (doc 04a); G3, G4 (doc 07); U1 (doc 08);
D4 (doc 03); PD2 (doc 06). P2's import-layer tests must be in place: every
move below updates those layer maps.*

## Target

```
scifor/project/      model-free; imports only stdlib, tomllib/tomli, scistacklog,
                     and other scifor.project modules — NEVER the scifor engine
  root.py            project root (out of pathinput.py)
  machine.py         (moved from scifor/machine.py)
  discovery.py       (moved from scifor/discovery.py)
  config.py          scistack.toml: locate, read, the meaning of every section
  config_file.py     the one writer/renderer (from scidb)
  aliases.py  colors.py  schema_order.py  names.py  source_edit.py   (from scidb)
  entities_file.py   TOML grammar/parse half of scidb/entities.py

scidb/project/       model-bound: Variables, Parameters, the database
  entities.py        materialize classes from entities_file
  discover.py  init.py (was project.py)  library.py  bundle.py  environment.py
  verify.py  table_copy.py  schema_map.py
  cli.py             bundle + library commands (from scistack_gui/*_cli.py)
```

`schema_map` stays model-bound because it imports `scifor.locations` (the
engine), which `scifor.project` must not import. Before moving each module,
re-check its imports with `grep -n "^from\|^import\|^\s*from \." <file>`.
The review found `names.py` and `schema_order.py` importing
`scifor.pathinput`, probably only for `project_root`. After stage 1 that
import is `scifor.project.root`. If anything else from the engine is used,
the module stays in scidb.

## Stage 1 — `scifor.project`: root, machine, discovery (R3′, R4 prep)

1. Create `scifor/src/scifor/project/__init__.py`.
2. Move `discovery.py` and `machine.py` into it. Move `set_project_root`,
   `get_project_root`, `enter_project_root`, `clear_project_root`,
   `project_root` and `_project_root_override` from `pathinput.py` into
   `project/root.py`. `PathInput` imports them from there.
3. Update `scifor/__init__.py` exports. The beta rule applies: no re-export
   aliases from the old locations. Rewrite all importers. The GUI has 23
   files (`grep -rn "scifor\.\(discovery\|machine\|pathinput import.*project_root\|pathinput import.*set_project_root\)" .`),
   plus scidb (10+ modules), scimatlab (`stubs.py`, `bridge.py`) and tests.
4. Layer rule in `scifor/tests/test_import_layers.py`: `scifor.project.*`
   must not import any other `scifor` module.
5. User runs `scifor`, `scidb`, `scimatlab`, `scistack-gui` and `scistack`,
   one at a time.

## Stage 2 — model-free scidb modules → `scifor.project`

1. Move `config_file`, `aliases`, `colors`, `schema_order`, `names` and
   `source_edit` (after the import re-check above). Replace their
   `from .log import Log` with `from scistacklog import Log`.
2. Rewrite importers: scidb internal; the GUI (`aliases` 4, `colors` 3,
   `names` 8, `config_file` 1); scistackplotdb (`schema_order` 1); and tests.
   Rewrite docstring mentions in scistackplot (`spec.py`, `aliases.py`,
   `table.py` say "`scidb.aliases` owns that grammar"); those become
   `scifor.project.aliases`.
3. Move their tests to `scifor/tests/project/`, or keep them flat with a
   `test_project_` prefix. Mind that conftest fixtures may come from scidb's
   conftest. Re-home the fixtures they need.
4. Each module's "ONE owner" docstring stays true; only the import path
   changes. Update `docs/claude` files that name them (grep).
5. User runs `scifor`, `scidb`, `scistackplotdb` and `scistack-gui`.

## Stage 3 — split entities (the one design step)

`scidb/src/scidb/entities.py` (1,472 lines) imports `variable`, `parameter`
and `source_edit`.

1. Read the whole file first. Classify every function as **parse/validate**
   or **materialize**.
   - Parse/validate covers TOML load and decode errors, the `[variables]`,
     `[parameters]`, `[path_inputs]` and `[library]` tables, aliases,
     `key_regex`, name errors, path resolution (`resolve_entities_path`,
     `entities_path`, `default_entities_relpath`), and the cache.
   - Materialize covers `_make_variable_class`, `Parameter` construction, and
     anything returning scidb objects.
2. `scifor/project/entities_file.py` returns plain frozen dataclasses:
   `DeclaredVariable(name, …)`, `DeclaredParameter(name, values, …)`,
   `DeclaredPathInput(name, template, arms, aliases, key_regex, …)`,
   `EntitiesFile(…, errors)`. `PathInput` is a scifor engine class, so
   `entities_file` stores specs and does **not** construct `PathInput`.
3. `scidb/project/entities.py` takes an `EntitiesFile` and builds classes and
   objects. It keeps the public functions the GUI and scimatlab call
   (`load_for_project`, …), with the same names, delegating parse to scifor.
4. Tests:
   - parse tests move to scifor;
   - materialization tests stay in scidb;
   - add one end-to-end test (TOML text → materialized `BaseVariable`
     subclass), so the seam is covered.
5. Logging: keep `[entities]` log lines in both halves, with the same
   prefix.
6. User runs `scifor`, `scidb`, `scimatlab` and `scistack-gui`.

## Stage 4 — GUI `config.py`: section meaning moves down (G3)

`scistack_gui/project/config.py` (1,912 lines, at its P2 location) opens
`scistack.toml` itself (two `tomllib.load` sites, ~417 and ~1206) and decides
what `modules`, `matlab.sources`, packages and plugin discovery mean.

1. Create `scifor/project/config.py`, the ONE reader. It does
   locate (`config_path_at`, `project_config_at`) → read
   (`read_scistack_section`) → **typed sections**
   (`ProjectConfig(modules, packages, matlab_sources, …)`, with path
   resolution via `resolve_config_path` and machine roots).
2. The GUI's `config.py` keeps only GUI concerns: building the code registry
   from those sections, and hints. Its two direct `tomllib.load` calls go.
3. Test in the GUI: `grep`-based, asserting no `tomllib.load` / `tomli.load`
   outside `scifor/project/`. Put the same assertion in a scifor test over the
   whole repo, excluding csv-stats and `scidb/project/bundle.py`'s
   bundle-manifest parsing. Bundle manifests are a different file; whitelist
   it with a reason.
4. User runs `scifor` and `scistack-gui`. Start the GUI on the aim2 example
   project (`project_aim2_example_dataset`) to confirm discovery is
   unchanged, and add a manual-test entry.

## Stage 5 — `scidb.project` + CLIs out of the GUI (B3 rest, U1)

1. Create `scidb/src/scidb/project/`. Move `entities` (the materialize half),
   `discover`, `project` → `init.py`, `library`, `bundle`, `environment`,
   `verify`, `table_copy` and `schema_map`. Move `csv_export` next to the
   store, and `artifact_stamp` / `run_markers` next to `foreach`, into
   whatever subpackage P4 creates. Until then, leave those three flat.
2. Move `scistack_gui/bundle_cli.py` and `library_cli.py` into
   `scidb/project/cli.py`. `scistack/__main__.py` registers them.
   - The GUI-specific bundle sections (`scistack_gui/bundle_section.py`
     `CodeSection`, `GuiSection`) stay in the GUI and **register themselves**,
     the way `scistackplotdb.bundle_section.PlotsSection` and
     `scistackstatsdb` do.
   - Read `scistack_gui/headless.py` (~90–93) to see how sections are
     assembled today. If the CLI needs the GUI's sections at runtime, make
     registration go through an entry-point group (`scistack.bundle_sections`)
     declared in each package's `pyproject.toml`, so scidb never imports the
     GUI.
3. Update the P2 layer maps and the scidb `SUPPORTED_MODULES` tuple.
4. User runs `scidb`, `scistack`, `scistack-gui`, `scistackplotdb` and
   `scistackstatsdb`. Then run `scistack export` / `scistack import` once on
   a small project, giving the user the exact commands.

## Stage 6 — scidb owns the project database's DDL (D3; D4, G4, PD2)

Today five packages create tables: sciduckdb 3, scidb ~20, scistackplotdb 3,
scistackstatsdb 2, GUI 18. scidb also reads the GUI-created `_intent`.

1. Create `scidb/src/scidb/tables.py` (a store-cluster module), the ONE
   registry:
   ```python
   @dataclass(frozen=True)
   class TableDecl:
       name: str; ddl: str; owner: str          # owner = declaring package, for logs/bundles
       copy_in_bundle: bool; cleared_by_clear_data: bool
   def declare(decl: TableDecl) -> None         # idempotent; conflicting re-declare raises
   def ensure_all(duck) -> None                 # CREATE IF NOT EXISTS, in declaration order
   def declared() -> list[TableDecl]
   ```
   - `configure_database` calls `ensure_all` after connect.
   - Log one DEBUG line per table created, and an INFO summary (`[tables]
     ensured N tables from M owners`).
2. Move `VersionedStore` from `scistackplotdb/_versioned.py` to
   `scidb/versioned_store.py`. It declares its table through `tables.declare`.
   scistackplotdb and scistackstatsdb import it from scidb. That removes the
   private cross-package import `scistackplotdb._versioned` and
   `bundle_section._remap_row`; move `_remap_row` too if it is generic.
3. Convert every `CREATE TABLE` site to a `TableDecl`:
   - scidb: `provenance.py` `ensure_provenance_tables`, `database.py`
     (`_registered_types`, `_record_save`), `exclusions`, `variant_pins`,
     `variant_delete`, `schema_types`;
   - **`<Type>_data` tables stay dynamic** (created per variable in
     `database.py`). Exempt them: the registry handles only fixed tables.
   - scistackplotdb (3) and scistackstatsdb (2), via `VersionedStore`;
   - GUI (18), in the `stores/` modules after P2. They declare with
     `owner="scistack_gui"`.
4. **`_intent` moves to scidb.** Its DDL goes in `scidb/intent.py`, next to
   `INTENT_TABLE`. The GUI's `intent_store` stops creating it (G4).
5. **sciduckdb creates none** (D4 option 2). Move `_schema`, `_variables`,
   `_variable_groups` and the `_schema_id_seq` DDL from
   `SciDuck._init_metadata_tables` into scidb `TableDecl`s.
   - SciDuck still *validates* `_schema` columns against `dataset_schema`
     (`_validate_schema_columns`) and allocates ids.
   - The construction order must be connect → `tables.ensure_all` → validate.
   - Check sciduckdb's own tests. They construct `SciDuck` standalone and
     expect tables. Give `SciDuck` an `ensure_metadata_tables()` that tests
     call explicitly, or have those tests go through a tiny helper. Decide by
     reading `sciduckdb/tests` first.
6. **Guard test** (`scidb/tests/test_table_registry.py`): grep every
   `CREATE TABLE` in `scidb/src`, `sciduckdb/src`, `scistackplotdb/src`,
   `scistackstatsdb/src` and `scistack-gui/scistack_gui`. Assert each is
   either inside `scidb/tables.py`'s callers through `TableDecl`, or is the
   dynamic `<Type>_data` creation. List the allowed files explicitly.
7. Optional follow-up, only if trivial: drive `table_copy` / bundle export and
   `clear_data` from `declared()` flags instead of their own table lists.
   Otherwise note it in Progress.
8. User runs every DB-touching package, one at a time: `sciduckdb`, `scidb`,
   `scistackplotdb`, `scistackstatsdb`, `scistack-gui`, `scimatlab`. Then
   open an **existing** project database in the GUI. Tables already exist, and
   `CREATE IF NOT EXISTS` must be a no-op. Add a manual-test entry.

## Stage 7 — explicit project root for PathInput (R4)

1. `PathInput` resolution takes `root: Path | None`. The global from
   `scifor.project.root` is the default only at entry points: `for_each`,
   the GUI server start, and the MATLAB bridge entry.
2. Thread the root from `scifor.for_each(…, project_root=None)`, and from
   scidb via `_ForEachState`, down to `_resolve_path_inputs`.
3. Test: two `PathInput`s resolved against two different roots in one
   process, with no global mutation.
4. User runs `scifor`, `scidb` and `scimatlab`.

## Progress
- 2026-11-01: stages 1-7 written in one session, awaiting the user's test run.
  Nothing committed. Python was not run; pyright (repo sources on
  extraPaths) reports no unresolved import in any package's src or tests.
  - Stage 1: `scifor/project/{root,machine,discovery}.py`. Log prefix of the
    root functions kept as `[pathinput]` (the manual-test doc greps it).
  - Stage 2: config_file, aliases, colors, schema_order, names, source_edit
    moved; their pure tests are `scifor/tests/test_project_{config_file,
    source_edit,colors}.py`. They still log on the `scidb` layer (Log's
    default), so existing caplog filters hold. scistackplotdb now depends on
    scifor directly (pyproject + override-versions-of).
  - Stage 3: `scifor.project.entities_file` returns `DeclaredEntities`
    (deviation: not named `EntitiesFile`, which stays scidb's materialized
    result). The mtime cache stays in scidb: it caches the materialized
    classes, which must be one class per process. A declaration only PathInput
    refuses is rejected at materialize, appended after the parse errors.
    New: `scifor/tests/test_project_entities_file.py`,
    `scidb/tests/test_entities_seam.py`.
  - Stage 4: `scifor.project.config` (locate/read moved out of discovery;
    `read_project_config` -> `ProjectConfig`, `load_config_file` strict read).
    Deviation: expanding a `modules` dir/glob into files stays in the GUI
    (code discovery, with its test-file and noise-dir rules). Guards:
    `scifor/tests/test_toml_reader_owner.py`, GUI
    `tests/test_config_reader_owner.py`.
  - Stage 5: `scidb/project/` = entities, discover, init (was project.py),
    library, bundle, environment, verify, table_copy, schema_map.
    `from scidb import entities` still works (facade). CLIs NOT moved (user
    decision): every command needs the GUI runtime; recorded under U1 in 08.
  - Stage 6 (full D3/D4, user decision): `scidb/tables.py` (TableDecl,
    declare, declared, ensure, ensure_core). Deviations: scidb's own 21
    tables are declared in tables.py itself (deterministic, one list) and
    `ensure_core` creates only owner "scidb" tables on open; other owners'
    tables keep "created on first write, never by a read" via
    `tables.ensure`. No copy_in_bundle / cleared_by_clear_data flags: step 7
    (driving bundle/clear_data from the registry) not done; bundle and
    clear_data keep their own lists. `SciDuck` moved to `scidb.sciduck`
    (uses `schema_values.schema_str`, dropping sciduckdb's `_schema_str`
    use); `test_sciduck.py` split into sciduckdb `test_codec_storage.py`
    and scidb `test_sciduck.py`. `VersionedStore` -> `scidb.versioned_store`
    (`owner` field, `remap_row` method).
  - Stage 7: `PathInput.bound_to(root)` + `scifor.foreach.bind_path_inputs`;
    `project_root=` on both `for_each`s, threaded through scidb's prepare
    state and the dry-run preview, recorded in Pipeline step options.
  - User to run (one package per invocation): scifor, scidb, sciduckdb,
    scimatlab, scistack-gui, scistackplotdb, scistackstatsdb, scistack,
    tools. Reinstall first (`./dev-install.sh`): packages and modules moved.
- 2026-11-01, first test run: 8 failures, all fixed (not yet re-run).
  - MATLAB parity: `project_root=` added to `+scifor/for_each.m` (binds
    PathInput / Fixed(PathInput) inputs via new `scifor.PathInput.bound_to`),
    `+scidb/for_each.m` (forwards to Python prepare and the MATLAB loop) and
    `bridge.for_each_prepare`.
  - scimatlab api-surface tests read `api/matlab_command.py`, moved to
    `services/matlab_command_format.py` in P2 (pre-existing, from P2).
  - `clear_data.KEPT_TABLES` gains `_intent` (now created by scidb on open).
  - test_inspect_phase5 patched `sciduckdb.schema_keys_from_db`; now
    `scidb.sciduck.schema_keys_from_db`.
  - GUI test_history / test_portability grepped GUI source for
    `CREATE TABLE`; they now read the registry (owner `scistack_gui` +
    `_intent`).
  - GUI lazy imports 1380 > 1377: the stage 2-4 splits turned 3 lazy
    imports into 6; hoisted 4 scifor.project imports to module top
    (bundle_section, project/registry, services/library_authoring), so the
    count should be about 1376.
