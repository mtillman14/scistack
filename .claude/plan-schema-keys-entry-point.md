# Plan: a "Schema keys…" entry point in the GUI — BUILT 2026-11-17

Today `SchemaKeysDialog` opens only from a failed run's duplicate-location popup
(RemedyDialog). Add a direct entry point, and keep everything that names a key
consistent after a change.

## Decisions (user, 2026-11-17)

- A. Config entries for a RENAMED key: rewritten automatically. DELETED key:
  kept and reported, never removed.
- B. Node settings naming a renamed key: REWRITTEN too, "exhaustively tested,
  across toml files, the database, various node types".

## Design: one map, one list of places

The bundle import already renamed keys section by section through
`scidb.project.schema_map.KeyMap` (one method per SHAPE). An in-place change
reuses it rather than growing a second list:

- `KeyMap.renaming(renames, retired)`: in-place map; retired keys are kept and
  flagged by every shape. New shapes: `config_tables`
  (`SCHEMA_KEYED_CONFIG_TABLES`), `path_input_arm` (template + aliases +
  key_regex), `key_list`, `where_filters`.
- `scidb.project.key_rename.rename_key_references(db, root, renames, retired,
  providers=)`: config + entities file itself; each bundle section's optional
  `rename_keys(ctx)` for what it owns.
- `change_schema_keys(..., root=, providers=)` calls it after the key tables
  commit (replaces `_config_leftovers`); flags become `leftovers`.
- Sections: `GuiSection.rename_keys` → `canvas_snapshot.rename_schema_keys_in_place`
  (shared `remap_location_value` / `remap_binding` with the import; library
  pipelines skipped); `PlotsSection`/`StatsSection.rename_keys` →
  `VersionedStore.remap_in_place` (every version rewritten in place).
- GUI service: no-node proposal; passes `resolve_project_root` +
  `bundle_providers()`; reloads the entities file; reports code-declared
  PathInputs (`registry.code_declared_path_inputs`).
- Frontend: `SchemaKeysButton` in the header; dialog `nodeId` optional, result
  view (`schemaKeysResult.ts`); `scistack:schema-keys-changed` re-fetches the
  header's `schema: [...]` (stale before, remedy route included).

Fixed on the way: import skipped PathInput `key_regex` and node `whereFilters`;
`config_file.write` did not forget `aliases`/`colors` caches.

## Tests (user runs; one package at a time)

- scidb: `tests/test_key_rename.py`
- scistack-gui: `tests/test_schema_keys_entry_point.py` (+ existing
  `test_schema_keys_dialog.py` must still pass)
- frontend: `npm test` (passes, 771)
- also re-run: scidb `test_schema_keys_change.py`, `test_extend_schema_keys.py`,
  bundle/portability tests (shared shapes); scistackplotdb + scistackstatsdb.

Docs: `docs/claude/changing-schema-keys.md`, `config-file-formats.md`,
`docs/gui-manual-testing-todo.md`.
