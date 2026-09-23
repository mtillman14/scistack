# Single-owner audit — 2026-09-23 (IMPLEMENTED, tests not yet run)

The user approved items 1–4 and 6; item 5 (`value_spellings`) was left alone
on purpose. Their ruling on item 1: "project root" means the working directory
whatever `pyproject.toml` says, and a `scistack.toml` location is the better
indicator of a config.

## Done
1. **Project root** → `scifor.project_root()`: the pinned root, else the cwd, with no walking. Config is looked up AT the root (`scifor.discovery.project_config_at`), and the database-folder fallback is gone.
   - `find_project_config`, `_find_project_root` and `scidb.entities.project_root` are removed.
   - The GUI pins its root in `registry.load_from_config`, and `registry.get_project_root()` reads the pin.
   - `api/project.py._project_root` reads it too.
   - The MATLAB bridge pins the folder it is given.
   - Recorded as D-2026-09-23-1.
2. **Id/handle spelling** → `ids.py` owns node ids and handles: `var_node_id`, `param_node_id`, `path_input_node_id`, `fn_nodes_prefix`, `legacy_fn_node_id`, `in_handle`/`out_handle`/`param_handle`, `handle_name` and `NODE_TYPE_PREFIXES`.
   - About 75 inline sites are rewritten.
   - The var-label rule is owned by `edge_resolver.node_id_to_var_label`, and `scope_filter` delegates to it.
   - Guard: `scistack-gui/tests/test_id_spelling.py`. It is an AST guard, and also pins the frontend prefix map and `ROOT_SCOPE`.
3. **Load errors** → `registry.all_load_errors()`. Guard: `tests/test_load_errors_owner.py`.
4. **Series grouping** → `scistackplot.render.base.series_groups`. Test: `scistackplot/tests/test_series_groups_owner.py`.
6. **Small items:**
   - `node_identity.identity_token` is deleted.
   - `scifor.merge` imports `_display_name` from `column_selection`.
   - `"main"` → `ROOT_SCOPE`.
   - `scihist.configure_database` is now a re-export of scidb's, so it no longer drops `**kwargs`.

## Still open
- The GUI's `config.locate_config_at` counts a `pyproject.toml` with no scistack section as the config, and scifor's `project_config_at` does not. The "packaged project" rule depends on the GUI's version.
- The frontend still mints its own prefixes. The test pins them, but they are still two copies.
- `scihist` as a whole is a deprecation shim, which conflicts with the beta no-deprecation rule.
