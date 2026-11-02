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
- (none yet)
