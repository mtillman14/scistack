# PathInput rename control + hint fix (2026-09-25)

## Trigger

A user made a PathInput in the GUI and then wanted to rename it. They looked
for it in `scistack.toml`, but it lives in the entities file
(`src/scistack_entities.toml`). The panel hint also sent them the wrong way:
it still said "rewrites the `scidb.PathInput(...)` declaration in source",
which was true before the 2026-09-01 move to TOML. The panel also had no
rename control.

## What a rename has to move

A PathInput's name is used in four places. A rename that only rewrites the
TOML key leaves the other three stale:

1. **The declaration.** The key under `[path_inputs]` in the entities file.
2. **Canvas node ids.** `pathInput__NAME[::scope]`
   (`ids.path_input_node_id`). The name is part of the id, and
   `edge_resolver.resolve_source_binding` reads the binding back out of the
   id. Everything keyed by node id is therefore keyed by the name:
   - layout.json positions
   - `_pipeline_nodes` rows (and their label)
   - `_node_config`
   - intent statements (hidden, and so on)
   - `_node_wiring`
   - manual edge endpoints
   - hidden DB-derived edges, whose id is `e__NAME__param__fn__wid` from
     `graph_builder.candidate_edge_id`
3. **Run history.** Since F38, each run records `declared_name` on the
   PathInput edge, and `convert_scidb_path_inputs` prefers that recorded name.
   After a rename, the recorded OLD name is no longer declared, so without an
   alias the canvas would draw a ghost `pathInput__OLD` node beside the new
   one. Runs recorded without a name still content-match, because the
   template is unchanged.
4. **Notes.** The key is `pathInput:NAME` (the frontend `noteKey`).

## Design

- **Grammar (scidb).** `scidb.entities.rename_entry(text, section, old, new)`
  renames the key in place. The value, comments and neighbours stay the same
  byte for byte. It raises `ValueError` if `old` is missing or `new` is
  already declared as any kind. This follows the same one-owner rule as
  `find_entry_span` and `upsert_entry`.
- **Write policy (GUI).** `target_file_service.rename_declaration(kind, old,
  new)`: editability, stale-file guard, atomic write, reload, and a check
  that NEW resolves and OLD does not, with rollback if that check fails. It
  works on TOML only. A `.py` or `.m` entities file is refused with a clear
  message.
- **Rename record (GUI store).** A new table
  `_pipeline_path_input_renames(old_name, new_name, renamed_at)` with
  `record_path_input_rename` and `path_input_rename_index(db)`. The index
  maps `old -> current`. The resolving is done in one place,
  `graph_builder.resolve_renamed_path_input`, which follows chains, guards
  against cycles, and only applies when the recorded name is not declared
  now. It is used by `convert_scidb_path_inputs`. That function has two
  callers, `api/pipeline` via `aggregate_from_scidb` and `execution_service`,
  and both pass the index. This is the same kind of record as D7. It is not
  a migration or backfill (see `feedback_beta_no_deprecation`): nothing
  existing is rewritten, and the table only records a rename that happens
  from now on.
- **Moving node-keyed state.** Extract
  `pipeline_store.rekey_node_state(db, old, new)` from
  `graduate_manual_node`. It moves config, intent subjects, wiring and edge
  endpoints, and graduation now calls it too, so there is one owner for
  moving node-keyed state. A rename also rekeys the `_pipeline_nodes` row
  (id and label), the layout positions (`layout.rekey_node_positions`), the
  hidden DB-derived edges (`intent_store.repoint_hidden_edges`, which
  computes new ids with `candidate_edge_id`), and the note key.
- **Orchestration.** `layout_service.rename_path_input(old, new)`, exposed
  as the RPC/REST handler `rename_path_input`
  (`POST /path-inputs/{name}/rename`). It writes the declaration first; if
  that fails, nothing else changes. Then it records the rename and rekeys
  every placement, logging each step with a count.
- **Frontend.** A Name field at the top of `PathInputSettingsPanel`, saved on
  Enter or blur and cancelled with Escape, using `useSourceEdit`. It is
  disabled when the PathInput is locked. The hint names the entities file
  and no longer says `scidb.PathInput(...)`.

## Known limits (stated, not fixed)

- Scripts that refer to the PathInput by name (`entities.OLD`, or a
  hand-written `for_each`) are not rewritten. The GUI does not own them.
- `scidb graph` (CLI) still groups old runs under OLD. The rename record is
  GUI state, like D7.

## Tests (for the user to run)

- `scidb/tests/test_entities_toml.py::TestRenameEntry`
- `scistack-gui/tests/test_path_input_rename.py`
