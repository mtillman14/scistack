# Plan P2 — enforce layers and declare public APIs

*Index: `.claude/plan-tidy-00-index.md` (global rules apply). Themes T3 and T6
in `docs/design-review/09-synthesis.md`. Refactor IDs: B1 (test half), B4,
D1, D2, G1, G2, P1. Decision D8 applies: lazy imports are allowed only from
an allow-list that may only shrink.*

**Principle.** First make the current structure *visible and frozen*: every
existing upward import goes on an allow-list, and new ones fail a test. Then
move code. That way every move in P3/P4 is checked automatically. This plan
moves **files**, not logic, except G1, which moves functions between GUI
layers unchanged.

## Stage 1 — one import-layer checker (shared tool)

1. Create `tools/import_layers.py`, the ONE owner of "which module may import
   which". It reads source with `ast`, with no import side effects, so tests
   stay fast. API:
   ```python
   def module_imports(root: Path, package: str) -> list[Edge]
       # every import in every module: (src_module, dst_module, lineno, lazy: bool)
       # lazy = inside a def/class body; resolves relative imports
   def check(edges, layer_of: Callable[[str], str | None], order: list[str],
             allow: set[tuple[str, str]]) -> list[str]
       # violations: an edge from layer i to layer j > i (upward), not in allow
   def lazy_imports(edges) -> set[tuple[str, str]]
   ```
   - Lower layers come first in `order`. Same-layer edges are fine.
   - `allow` holds `(src_module, dst_module)` pairs, read from a
     `tests/import_allowlist.txt` next to each package's test. One pair per
     line, plus a `# reason` comment.
2. `tools/tests/test_import_layers.py` covers the checker itself on a tiny
   fixture tree: relative imports, lazy detection, an allow-list hit, an
   upward violation.
3. Each package test adds the repo root to `sys.path` and imports
   `tools.import_layers`. Check `tools/` for an existing `__init__.py`; add
   one if needed.
4. User runs `python -m pytest tools/tests -x -q`.

## Stage 2 — scidb layer test (B1, test half)

1. `scidb/tests/test_import_layers.py`, with the layer map from
   `docs/design-review/04a-scidb-structure.md` "The six clusters". Copy the
   module lists exactly. Order, lowest first:
   `api` (model types) → `store` → `provenance` → `run` → `inspect`, with
   `project` beside them.

   For now, let `project` import `api`, `store` and `run`, and let nothing
   import `project` except `inspect` and `run` (until P3 moves it).
2. Generate the initial allow-list from the checker's violations: about
   Storage→Provenance, Run→Provenance, Storage→API reverse edges, and the
   core→`inspect` edges (`database.py:5989`, `variant_pins.py:161,221`,
   `variant_delete.py:206`, `locations.py:519,675`). Give each line a
   `# reason`: "cycle via BaseVariable", "core uses inspect.variant_cards",
   and so on.
3. A second test asserts the allow-list holds **no stale entries** (every
   listed pair still exists). The list can then only shrink, by deleting
   lines.
4. Also count function-level imports (the review counted 569 in scidb) and
   pin the number: `assert n <= BASELINE`. Lower the baseline when it drops.
5. User runs `python -m pytest scidb/tests/test_import_layers.py -x -q`.

## Stage 3 — scidb's declared public surface (B4)

1. In `scidb/src/scidb/__init__.py` docstring, and in a new module-level tuple
   `scidb.SUPPORTED_MODULES`, list the modules other packages may import:
   - `scidb` (top level);
   - `scidb.inspect`;
   - the internals the GUI genuinely needs today. Start from the 40 measured
     in 04a "Contract". Get the current list with
     `grep -rhoE '^\s*from scidb(\.[a-z_.]+)? import' --include='*.py' scistack-gui/scistack_gui scistackplotdb/src scimatlab/src scistackstatsdb/src scistack/src | sed -E 's/^\s*from //; s/ import//' | sort -u`.
2. A test in **each consumer package** fails on an import outside the tuple:
   - `scistack-gui/tests/test_scidb_boundary.py`;
   - `scistackplotdb/tests/…`;
   - `scimatlab/tests/…`;
   - `scistackstatsdb/tests/…`.

   It uses `tools.import_layers.module_imports`. `db._duck` attribute access
   is not an import. Add a second check in the GUI test that counts `._duck`
   uses with a pinned baseline (26 today) that only goes down.
3. Shrinking the list is P3/P4 work (G6, Q3). This stage only fences.
4. User runs each consumer package's suite, one at a time.

## Stage 4 — sciduckdb: split the module, make the used interface public (D1, D2)

1. Split `sciduckdb/src/sciduckdb/sciduckdb.py` (2,383 lines), keeping
   `__init__.py`'s export list as the facade:
   - `codec.py`: the module-level conversion functions: `_infer_duckdb_type`,
     `infer_data_columns`, `_python_to_storage`, `storage_to_python*`,
     `dataframe_to_storage_rows`, `bulk_df_to_storage_rows`,
     `value_to_storage_row`, `column_type_change`, `record_schema_mismatch`,
     `array_from_storage`, `_flatten_dict` / `unflatten_dict`, and the
     `count_*` helpers;
   - `connection.py`: `SciDuck`'s lock, transaction (P0's `transaction()`),
     execute/fetch and bulk methods;
   - `schema.py`: `_schema` id allocation, `distinct_schema_*`, groups,
     `schema_keys_from_db`.

   `SciDuck` can stay as the class name.
2. **Public names for the used private methods.** These are the 153 call
   sites across scidb, scistackplotdb and the GUI. Rename, with no aliases
   (beta rule):

   | Old | New |
   |---|---|
   | `_execute` | `execute` |
   | `_fetchall` | `fetchall` (a public wrapper already exists; delete the private one) |
   | `_fetchone` | `fetchone` (same) |
   | `_fetchdf` | `fetchdf` |
   | `_fetchdf_if_open` | `fetchdf_if_open` |
   | `_table_exists` | `table_exists` |
   | `_bulk_insert` | `bulk_insert` |
   | `_bulk_update` | `bulk_update` |
   | `_get_or_create_schema_id` | `get_or_create_schema_id` |
   | `_begin` / `_commit` / `_rollback` | remove from callers in favour of `transaction()` (P0); keep private |
   | `_fetch_table` | `fetch_table` |
   | `_restore_types` | `restore_types` |

   Do it with `perl -pi -e 's/\b_duck\._fetchall\b/_duck.fetchall/g'` and
   similar, one name at a time, then `grep -rn "_duck\._" .` to confirm zero.
   Mind `feedback_perl_at_interpolation`: single-quote perl expressions.
   Plain-`duck` variables exist too (`provenance_query` functions take
   `duck`), so also sweep `\bduck\._(fetchall|…)`.
3. Docstrings on each public method. `sciduckdb/README.md` lists them as the
   supported interface.
4. User runs, one package each: `sciduckdb`, `scidb`, `scistackplotdb`,
   `scistackstatsdb`, `scistack-gui`, `scimatlab`.

## Stage 5 — GUI: api modules hold only Handler tables (G1) + layer test

1. Move logic out of `api/` into `services/`, unchanged:
   - `api/pipeline.py`: `_build_graph` (~681) and `_fn_params_from_registry`
     → `services/graph_service.py`. Services import them 4 + 3 times today.
   - `api/matlab_command.py` (2,475 lines): the generation logic →
     `services/matlab_command_service.py` (which already exists), or a new
     `matlab/command_gen.py` after stage 6.
   - `api/run.py` (2,197): anything that isn't request parsing or the Handler
     table → `services/execution_service.py` or a new `services/run_service.py`.
   - `api/ws.py` `push_message` → `notify.py` or `services/notify_service.py`.
   - Then fix the 30 services→api imports and 24 top→api imports.
2. `scistack-gui/tests/test_import_layers.py`, with order
   `domain` → `services` → `api` (api on top), and the top-level modules
   classified by stage 6's homes. Use an allow-list as in stage 2.
3. User runs `python -m pytest scistack-gui/tests -x -q`. Also run the
   frontend tests (`cd scistack-gui/frontend && npm test`). No frontend change
   is expected, but the handler-table test reads `api.ts`.

## Stage 6 — GUI: give the top-level modules homes (G2)

Move files, fix imports, and change nothing else:

| New subpackage | Modules |
|---|---|
| `scistack_gui/stores/` | `pipeline_store`, `intent_store`, `layout`, `node_wiring`, `history`, `library_lock` |
| `scistack_gui/matlab/` | `matlab_parser`, `matlab_registry`, `matlab_rewrite`, `matlab_run_watch`, `matlab_sidecar` (drop the `matlab_` prefix inside the package) |
| `scistack_gui/project/` | `config`, `registry`, `pipeline_discovery`, `library_functions`, `code_rewrite`, `step_names`, `ids` |
| `scistack_gui/runtime/` | `db`, `startup`, `bootstrap`, `headless`, `stdin_reader`, `notify`, `app` |
| stays top-level | `server.py` and `__main__.py`, because the extension spawns `python -m scistack_gui.server` (see `extension/src/serverArgs.ts`); the CLIs `bundle_cli`, `library_cli`, `canvas_shape_cli`, `bundle_section` (they move out in P3) |

1. Find string references too: `"scistack_gui.<module>"` in
   `scistack/src/scistack/__main__.py`, `extension/src/*.ts`, `pyproject.toml`
   scripts, and `importlib` calls. Use
   `grep -rn "scistack_gui\.\(config\|db\|registry\|…\)" --include='*.py' --include='*.ts' --include='*.toml' .`.
2. Update stage 5's layer map. Order: `runtime` (bottom: db connection) →
   `stores` → `project` / `matlab` → `domain` → `services` → `api`.
   `server.py` is top.
3. User runs `python -m pytest scistack-gui/tests -x -q`, then starts the GUI
   once (`run` skill or VS Code) to confirm the server boots. Add a manual-test
   line: "GUI opens a project after the module reorg".

## Stage 7 — scistackplot subpackages (P1) — optional in P2, required before P4's codegen work

1. Group into `model/`, `reduce/`, `text/`, `render/` (exists), `export/`,
   following `docs/design-review/06-plot-and-stats.md` "Shape".
   `scistackplot/__init__.py` keeps exporting the public names, so outside
   importers (`from scistackplot import PlotSpec`) are unchanged. Deep imports
   (`from scistackplot.roles import …`, used by scistackstats and the GUI)
   need rewriting. Grep for them.
2. Layer test with order `model` → `reduce` → `text` → `render` / `export`.
   scistackstats may import only `scistackplot.model` and the top level. Add
   that test in scistackstats.
3. User runs scistackplot, scistackplotdb, scistackstats, scistackstatsdb and
   scistack-gui, one at a time.

## Progress
- (none yet)
