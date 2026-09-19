# Plan: declared `[schema_keys]` level order everywhere

Owner: `scidb.schema_order` (reader) — every consumer asks it; no layer re-reads the TOML.

## Audit (2026-09-19)

Already correct:
- `DatabaseManager._sort_by_schema_keys` (every `_find_record` / `load(as_df=True)` frame, hence CSV export).
- `ScidbSource._ordered` -> `LongTable.level_order` -> `x_order`, `color_order`, facet groups, nested-x plan (plotly + mpl preview).
- GUI `get_schema` level lists; `locations._build_tree` (location picker).

Gaps:
1. **Snapshot at open.** `DatabaseManager.__init__` stores `dataset_schema_key_order` once. Editing
   scistack.toml while the GUI/session is open changes nothing until restart, although
   `declared_level_order` itself is mtime-cached and would pick the edit up.
2. **Found via `Path.cwd()` only.** A script/MATLAB session whose cwd is outside the project reads `{}` silently.
3. **Plot table cache** (`ScidbSource`) is fingerprinted on data content only, so even with (1) fixed a cached
   LongTable keeps its old `level_order`.
4. **Exported seaborn code** (`scistackplot.codegen`): flat single-factor x gets no `order=`, colour gets no
   `hue_order=`, facets get `col_order=` only when layout rules exist, second facet never gets `row_order=`.
   seaborn then uses order of appearance -> saved/exported figures can differ from the preview.
5. **for_each iteration + result frames**: `[]` resolves through `sciduckdb.distinct_schema_values`
   (`ORDER BY key`, alphabetical), so iteration order and `_results_to_output_dataframe` rows are alphabetical.
6. `mpl.py:351` fallback `sorted(..., key=str)` when `x_order` is None (minor; only reached without a factor order).

## Stages
1. scidb: make `dataset_schema_key_order` a property that re-asks `declared_level_order(project_root)`
   (mtime-cached, so one stat per access); resolve start from the db's project root, falling back to cwd.
   Log at INFO which config was used / "no config found from <start>". Tests: edit TOML mid-session -> next load re-sorted.
2. scidb: `DatabaseManager.distinct_schema_values` applies `order_levels` (fallback = existing DuckDB order), so
   for_each iteration and GUI lists share it. Test: for_each result frame rows follow declared order.
3. scistackplotdb: include the declared order in the table-cache key (or invalidate on change). Test.
4. scistackplot codegen: emit `order=`, `hue_order=`, `col_order=`, `row_order=` from `table.factor(..).levels`
   whenever the factor is on that channel. Tests assert emitted kwargs and fan-out parity with the preview.
5. mpl fallback -> natural sort (same as `_level_rank`).
6. Docs: update docs/claude/config-file-formats.md `[schema_keys]` section with the full consumer list.

## Status (2026-09-19)
All 6 stages implemented, uncommitted; all tests pass (user-run 2026-09-19).
- New tests: scidb/tests/test_schema_order.py (TestLiveReading, TestLocatingTheProject, TestForEachOrder),
  scistackplotdb/tests/test_declared_level_order.py, scistackplot/tests/test_codegen_level_order.py,
  scistackplot/tests/test_mpl_x_levels.py.
