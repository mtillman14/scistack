# Sidebar: palette order, delete, search, shared header, non-default badges

**Status:** implemented 2026-11-03, uncommitted; pytest + GUI check pending (user). Deviations: `remove_declaration` lives in target_file_service beside rename_declaration; variables joined `entity_editability` (`_SOURCE_KIND_REGISTRIES`); a shared `_stale_refusal` now backs edit/rename/remove; "placed" = on the RESOLVED graph of any pipeline, hidden pipelines included. Revised after an adversarial review: the archive/restore idea is dropped in favour of the existing undo, and the sort modes are cut down. Extended the same day with search, a shared panel header and non-default badges.

Problems with the right sidebar (`scistack-gui/frontend/src/components/Sidebar/`):

- **Palette (`EditTab.tsx`):**
  - Function entries aren't organised. Many Python functions share one file, but the list is one flat A→Z run.
  - Entries can't be deleted.
  - Variables can't be ordered by when they were last saved.
  - Nothing can be found by name. The only filter is the Functions role dropdown.
- **Node panels:**
  - Each panel builds its own title: `FunctionSettingsPanel` has `styles.fnName`, `PathInputSettingsPanel` has an editable name field, and Variable/Parameter/Glue/Pipeline each style their own. There's no common place for the node kind, where it's declared, or actions like "Open source". Only GUI-created functions and glue get "Open in editor" today.
  - A non-default setting is invisible unless you scroll to it. Dry run on, active filters or hidden combos give no sign at the top.
  - The run-option defaults have **two owners** that can drift: `Sidebar.tsx:202` hard-codes `{ dry_run: false, save: true, distribute: false, as_table: false }`, and `run_service.py` hard-codes its own `.get(…, default)` values. Badges need one definition of "default", so this gets fixed first.

## Decisions (user, 2026-11-03)

- **Delete is real, scoped to the declaration.** It removes the entry from `src/scistack_entities.toml` and keeps every DB record. This is a deliberate exception to "never delete, mark hidden", and it applies to entities-file declarations only.
- **Delete applies to Variables, Parameters and Path Inputs only.** Functions get no delete.
- **Delete is refused while the entity is placed on any canvas.** The refusal lists each placement as a clickable link that opens that pipeline tab.
- Entities declared outside the entities file (a `BaseVariable` subclass in a `.py`, a `scidb.Parameter` in Python or MATLAB) get no delete button, only "declared in X — open source".
- **Undo is the existing undo/redo system (`scistack_gui/history.py`), with no second mechanism.** `delete_entity` is `undoable=True` and calls `history.note_write(entities_file)` first. Undo restores the file's exact bytes, refuses if the file changed since, and still works after a restart (the newest 500 changes are kept, and the `_canvas_change` audit log is never pruned).
  - Beyond that window, recovery is through git, or through the removed text written to the INFO log. This gap is accepted.
  - Rejected alternative: a deleted-declarations table plus a "Recently deleted" list. It would be a second owner of undo, and a copy of each declaration that can drift from the TOML (NOTE 4). It wouldn't travel with a project bundle either.

## Owners (one per concept, NOTE 4)

| Concept | Owner | Consumers |
|---|---|---|
| Removing an entry from entities-file text | `scifor.project.entities_file`, with new `remove_entry(text, section, name)` and `remove_variable(text, name)` next to `upsert_entry` / `rename_entry` / `add_variable` | GUI delete service |
| "Is it placed, and where" | Existing GUI placement lookup. Confirm which one in `entity_placements`; `library_service.placement_check` and `scope_filter._placed_in` are candidates. No new copy. | delete service, refusal links |
| Undo of the delete | `scistack_gui/history.py` (`undoable=True` + `note_write`) | page undo stack |
| Last save time per variable type | **scidb**, new `latest_save_by_type()`: `MAX(_record_save.timestamp)` joined to `_record.type`, one batched query | `get_registry` |
| A function's source file | Backend registry (Python: the discovered function object; MATLAB: the registry file path; library references: a "Library" group) | `get_registry` → palette grouping |
| A→Z order | `domain/display_order.display_sorted` (unchanged, still the only owner) | every list |
| Selected sort mode | Frontend, saved per tab through `set_note` with `{ stack: null }` (same as `ui:function_role`; not an undo step) | — |
| Run-option defaults | **scidb `for_each`'s signature** (`scidb/src/scidb/run/foreach.py`): that is where `save` / `distribute` / `as_table` / `on_error` / `on_duplicate` get their defaults. `dry_run` too, if `for_each` has it; if it's GUI-only, its default is declared once next to the others in the backend. | backend `run_option_defaults()` → `run_service` (replaces its literal defaults) and the frontend (replaces the `Sidebar.tsx` literal; badges compare against it) |
| "Is this setting non-default" | Frontend pure module `sidebarBadges.ts`. It compares node data with the served defaults and invents none of its own. | shared header summary, section-title badges |
| Panel header | `Sidebar/PanelHeader.tsx`, one component | every node panel |
| Where a node is declared (file, line) | Existing data, not a new lookup: `function_sources` (from `registry_entries`), the variable registry entry, and the `source_file`/`source_line` already on Parameter and PathInput node data | `PanelHeader`'s "declared in" line and Open source |
| Palette search match | Frontend pure function `paletteSearch.ts` (case-insensitive substring on the name, plus the file basename for functions) | every palette tab |

The frontend never sorts alphabetically. Its only sort is the stable sort by timestamp, where ties keep the server's A→Z order.

## Steps

### `entities_file_remove` (scifor)
- `remove_entry(text, section, name)` finds the entry with `find_entry_span` and deletes its line(s), including a comment on the same line. Other entries and comments are untouched.
- `remove_variable(text, name)` drops one element from `variables = [...]`. It handles: only element, last element (trailing comma), multi-line array.
- Both go through `checked_text`, so a broken result is never written. If the name isn't there, the text comes back unchanged with a not-found signal.
- **Tests:** each case above.

### `latest_save_query` (scidb)
- `latest_save_by_type()` returns `{type_name: timestamp}` from one SQL statement.
- **Tests:** two types saved at different times; a re-save moves the timestamp (it uses the newest save, not `created_at`); a type that was never saved is absent.

### `registry_entries` (GUI backend, `pipeline_service.get_registry`)
- Add `function_sources: {name: {file, line, language}}` and `variable_last_saved: {name: timestamp}`. If no DB is open, the latter is `{}`, because `get_registry` runs before a database exists. The existing name lists don't change.
- Logging: INFO with the count of functions per source file and how many have no file. DEBUG with the full map.
- **Test:** a two-file project groups correctly, and a library reference lands in "Library".

### `entity_placements` (GUI backend)
- Choose the existing placement lookup (see Owners) and expose `entity_placements(kind, name) -> [{pipeline_id, pipeline_name, node_id}]`.
- **Test:** an entity placed in two pipelines returns both.

### `delete_entity` (GUI service + RPC, `undoable=True`, `undo_label="Delete <name>"`)
- The order of checks:
  1. Is it declared in the entities file? If not, refuse with "declared in X".
  2. Is it placed anywhere? If so, refuse and return the placements.
  3. Call `note_write`, remove the entry in scifor, write the file, record the source hash, refresh the registries.
- Palette notes are kept.
- Logging, at INFO: kind, name, file, number of placements, outcome, and **the exact removed entry text**.
- **Tests:**
  - refused when placed (file unchanged)
  - refused when declared in `.py`
  - success removes only that entry, and the DB records are still there
  - `history.undo` restores byte-identical file contents
  - undo is refused if the file was edited after the delete

### `palette_sort_and_group` (frontend, EditTab)
- **Functions:** By file (default) · A→Z. "By file" shows collapsible file headers: the basename, the full path as a tooltip, and a count. The role filter applies inside the groups.
- **Variables:** A→Z (default) · Last saved. Never-saved variables go last. The relative time ("2 d ago") is shown only in this mode.
- **Parameters / Path Inputs:** A→Z only. No dropdown.

### `palette_delete_ui` (frontend)
- A trash button appears on row hover, only when `declared_in_entities_file` is true. Variables need this flag in the registry too: add it to `registry_entries` if it isn't there already.
- The confirm dialog shows the exact entry text being removed and says "Saved data is kept. Undo with Ctrl/Cmd+Z."
- A refusal is shown inline and is never swallowed. Placement refusals list clickable links that open each pipeline tab.
- The call goes through `callBackend` with no extra arguments, so it lands on the page's undo stack.
- Rebuild the frontend bundle (the bundle rebuild trap).

### `palette_search` (frontend, EditTab)
- One search box above the tab strip. `/` focuses it while the canvas isn't taking keystrokes, and Esc clears it.
- It filters the active tab through `paletteSearch.ts`. Functions also match on the file basename, so typing `emg_utils` shows that file's functions.
- It works together with the role filter and with "By file" grouping: a file header stays only if one of its functions matches, and file headers expand while a search is active.
- Every tab label shows its match count while searching ("Variables (3)"), so a match in another tab is never hidden.
- An empty result shows "No matches for 'x'" plus a "clear" link, never a blank list.
- The query is not saved; it clears when the palette is remounted.
- **Tests (`node --test`):** case-insensitivity, basename match, no match, and an empty query returns everything in the server's order.

### `run_option_defaults` (scidb → GUI backend → frontend)
- The backend exposes `run_option_defaults()`, read from the `for_each` signature with `inspect.signature`, so there's no literal copy. It's served in the registry or config payload the sidebar already fetches.
- `run_service` uses it in place of its `.get(…, literal)` defaults. `on_error_from_config` / `on_duplicate_from_config` stay the owners of reading those two options.
- The frontend uses it in place of the literal in `Sidebar.tsx:202`.
- Logging: DEBUG with the resolved defaults at startup.
- **Tests:** a guard test that `run_option_defaults()` equals the `for_each` signature defaults; an empty `runOptions` gives the same effective options as before.

### `panel_header` (frontend)
- `PanelHeader` props: `kind` (icon + label: Function / Plot / Stat / Glue / Variable / Parameter / Path Input / Submodule), `name` (or a `nameSlot` for PathInput's editable rename field, so rename stays where it is), `source` (`{file, line} | null`), `readOnly` (which renders the existing `ReadOnlyDeclarationBanner`, moved inside the header with no change to its logic), and `badges`.
- Actions:
  - **Open source:** `reveal_in_editor`, shown whenever `source` is known. This extends it from GUI-made functions to all of them. The existing "Open in editor" buttons in `FunctionCodeSection` / `GlueSettingsPanel` are removed in favour of this, leaving one action.
  - **← Palette:** clears the selection, the same as clicking the canvas background.
- The "declared in `file:line`" line sits under the name, with the full path as a tooltip.
- Adopted by Function, Glue, Variable, Parameter, PathInput and Pipeline panels; `EndpointPanel` renders under the function's header. Each panel's own title element is removed.
- Kept compact: one row plus at most one sub-row.

### `non_default_badges` (frontend)
- `sidebarBadges.ts` turns a function node's data and `run_option_defaults` into a list of `{section, label, tone}`:
  - **Run options:** one badge per option that differs from the default, e.g. "Dry run", "Not saving", "Stop on error".
  - **Filters:** "2 filters" when any are set.
  - **Variants:** "3 hidden" for hidden combos.
  - **Schema keys / level:** "Subset" when the selection is narrower than everything.
  - **Inputs:** "Columns picked" / "Per column" when a column selection or `for_columns` is set.
- They're shown in two places from the same list: as small chips next to the matching section title, and as a summary in `PanelHeader`. Clicking a summary chip scrolls to its section.
- Warning tone (amber) for settings that change what gets saved: dry run, save off, stop on error. Neutral tone for everything else.
- Parameter / PathInput / Variable panels pass no badges in v1.
- **Tests (`node --test`):** defaults give no badges; each option flipped gives exactly one badge in the right section; an empty `runOptions` gives no badges, so the payload defaults and the badge defaults can't disagree.
- Update the GUI manual testing doc for header, badges and search.

## Deferred (add only if missed)
- Z→A order; "Last run" for functions; "By file" for Variables/Parameters/Path Inputs.
- Function delete or hide.
- Searching docstrings or notes; one search across all tabs at once.
- Collapsible sections (reuse `Studio/RailGroup.tsx` when wanted).
- Badges for non-function panels.
