# Saved plots (per-variable, named, restorable)

*Drafted 2026-09-24. Status: ALL 5 STAGES BUILT 2026-09-24. Stages 1-4 tests
pass (user). Stage 5 is docs only: `docs/claude/saved-plots.md`, README
sections in both plotting packages, and ADR D-2026-09-24-1. Not yet visually
checked (manual-test doc §0zi). Uncommitted.*

## Request, restated

Plot Studio loses everything when its tab closes. The only thing that survives
today is the Variants rows (`variant_selection` statements in the intent
store, restored by `plot_service._with_stored_variant_sets`). Everything else
(roles, groups, kind, facets, y-axis, style, filters, location filter,
show-sample, figure size and so on) has to be rebuilt by hand.

Wanted:

1. **Named saved plots, many per variable.** One saved plot holds one complete
   set of settings. A spec that fans out into *separate figures* (ITERATE)
   still counts as one saved plot.
2. **Save button** that asks for a name and stores the current settings under
   the variable.
3. **"Saved Plots" section**, collapsible, on the right of Plot Studio. It
   lists this variable's saved plots. Clicking one loads it into the current
   tab and restores every setting, so the figure looks the same as when it was
   saved.
4. Right-clicking a variable still opens Plot Studio as it does now.
5. **The settings will keep changing shape, possibly a lot.** A saved plot
   written today has to keep opening after PlotSpec changes.

## What the code tells us

- `PlotSpec` is already the serializable description (`to_dict`/`from_dict`,
  `scistackplot/src/scistackplot/spec.py:1004`), so most of the state is one
  dict.
- **`from_dict` is brittle against drift:** `StyleOptions(**raw["style"])` and
  `Filter(**f)` raise on any renamed or removed key, unknown enum values raise,
  and `_refuse_legacy_spec` raises outright. Today a saved plot would stop
  opening the first time a field is renamed. That makes requirement 5 the core
  of this design.
- A few settings live only in the panel, not in the spec. `previewMode`
  (export/pane), `aspectChoice` (figure-size dropdown) and `figureIndex`
  (which figure of the fan-out is showing) all affect how the plot "looks
  identical".
- Saved plots are **display intent**. By `docs/claude/intent-and-fact.md` §1
  they are out of scope for the intent store, because they change nothing a
  run computes. So they get their own table, not an aspect.

## Ownership (NOTE 3 / NOTE 4)

| concept | owner | consumers |
|---|---|---|
| Reading a stored spec dict that may have drifted | `scistackplot.spec` (`PlotSpec.restore(raw) -> (spec, notes)`) | scistackplotdb, GUI, codegen docstring reopen |
| Checking a restored spec against today's data (factors gone, pins matching nothing) | `scistackplot` (`restore.reconcile(spec, table)`) | scistackplotdb |
| Saved-plot storage (table, save/list/load/rename/hide) | `scistackplotdb.saved` | GUI service, user scripts (`load_saved_plot(db, "StepLength", "Fig 3")` then `render`) |
| Panel-only view state (preview mode, aspect choice, figure index) | frontend: one `viewState()` / `applyViewState()` pair in `PlotStudio/savedPlots.ts` | the save and open paths |

The GUI never parses a spec. It sends the dict out and receives
`{spec, view, notes}` back.

## Drift policy: salvage, don't migrate

In keeping with [beta: no migrations], there is **no per-version upgrader
code**. Instead:

- Each save stores an **envelope**:
  `{format: 1, spec: spec.to_dict(), view: {...}, saved_with: <scistackplot version>}`.
  `format` is logged but nothing branches on it.
- **The stored blob is never rewritten on open.** Only an explicit re-save
  writes it. A newer build can therefore still salvage fields an older build
  dropped.
- `PlotSpec.restore(raw)` starts from the defaults and reads field by field.
  Each field that parses is kept. A field that doesn't parse falls back to its
  default and adds a note. A key the spec no longer has adds the note "no
  longer a setting: …". A legacy vocabulary (today's `LegacySpecError`) turns
  into notes rather than an error.
- `reconcile` drops references to factors that no longer exist (roles,
  groups, color, show_sample, sample_color, filters) and adds a note for each.
- The notes show up in a banner on the restored plot ("Restored 'Fig 3' — 2
  settings no longer apply: …") and are logged at WARNING.

### What happens to settings in an old format
- **A re-save writes only the current format.** The envelope is rebuilt from
  `spec.to_dict()` of the spec in the panel. Keys the spec no longer has are
  NOT carried forward. After one re-save, the newest row is entirely in the
  current format, and a renamed setting's new value (whether left at its
  default or edited) round-trips like any other field.
- **Old keys survive only in history rows**, because older versions are
  hidden rather than deleted. Nothing ever reads them except
  `plot_history`, where they are inert.
- **A plot that restored with notes opens as "modified".** Until it is
  re-saved, every open salvages the same row and shows the same notes. The
  banner says "Save to update this plot to the current settings format".
- Test: restore a drifted envelope, then save and restore again. The second
  restore produces zero notes, and the newest row contains no unknown keys.

## Storage (decided: DuckDB)

Recommended: a `_saved_plot` table in the project DuckDB, created by
`scistackplotdb.saved.ensure_table`:

```
variable VARCHAR, name VARCHAR, saved_at TIMESTAMP, hidden BOOLEAN,
envelope_json VARCHAR, PRIMARY KEY (variable, name, saved_at)
```

- The key is the variable the panel was opened on (`spec.measures[0]`).
- Re-saving under an existing name adds a row. The newest non-hidden row is
  "the" saved plot, and older rows remain as history. Nothing is ever deleted
  ([never delete, mark hidden]).
- "Remove" sets `hidden = true`.
- Reads go through `_fetchall`/`_fetchone` only ([DuckDB fetch locking]).

## Stages

### Stage 1: tolerant restore (scistackplot)
- `PlotSpec.restore(raw) -> (PlotSpec, list[RestoreNote])`. Strict `from_dict`
  stays for callers that want a hard failure.
- `restore.reconcile(spec, table) -> (spec, notes)`.
- Logging: DEBUG one line per salvaged or defaulted field, INFO summary
  (`[restore] 27 fields kept, 2 defaulted, 1 unknown key`).
- Tests:
  - **Field coverage:** every `dataclasses.fields(PlotSpec)` (recursively
    through the nested dataclasses) round-trips through
    `to_dict → restore` unchanged. A new field that was forgotten in
    `to_dict`/`from_dict` fails here.
  - **Drift fixtures:** golden envelopes with unknown keys, missing keys, bad
    enum values, an unknown style key and a legacy role vocabulary. All of
    them restore without raising, and each yields the expected notes.
  - **Reconcile:** removed factor → role dropped + note; the rest is kept.

**BUILT 2026-09-24 (pytest unrun):**
- `scistackplot/restore.py`: `restore_spec(raw, fallback_measure=)` →
  `Restored(spec, notes)`, `reconcile(spec, table)`, `RestoreNote`,
  `NoteKind`, `RestoreError`. All are exported from `scistackplot`. The name
  changed from `PlotSpec.restore`: one module-level owner, no alias.
- The walk is driven by the type hints of each dataclass (`get_type_hints`),
  so it holds no second copy of the spec's shape. After salvage, a class
  with its own `from_dict` re-normalises the result, so the class keeps
  ownership of its wire format.
- A dropped entry reports ONE note. Notes about its inner fields are
  discarded with it.
- Reconcile deviates from the bullet above: a missing factor's **role is
  reported but kept**. `strip_answered_roles` already drops it at every
  resolve, and keeping it means the role returns if the factor does. Only
  `show_sample` / `sample_color` are removed, because `validate` refuses
  them and the figure would not draw.
- The stale-role predicate was extracted as `variants.stale_role_names`
  (one owner), which both `strip_answered_roles` and `reconcile` read.
- Logging: WARN per note, INFO summary per restore, DEBUG for settings
  absent from the stored copy (i.e. new fields defaulted).
- Tests: `scistackplot/tests/test_restore.py`.

### Stage 2: store (scistackplotdb)
- `saved.py`: `save_plot`, `list_saved_plots(variable)`,
  `load_saved_plot(variable, name) -> SavedPlot(spec, view, notes, saved_at)`,
  `rename_saved_plot`, `hide_saved_plot`, `plot_history(variable, name)`.
- Logging: INFO on save (variable, name, bytes, format) and on load (notes
  count), WARNING for each note.
- Tests: round trip, same-name re-save keeps history, hide excludes from
  list, rename, name validation (non-empty, trimmed, unique per variable).

**BUILT 2026-09-24 (pytest unrun):** `scistackplotdb/saved.py`.
- The table is keyed by `plot_id` (a uuid) + `version`, not by name. A name
  is a label, so rename is a plain UPDATE (no update of an identity column)
  and a new plot that reuses a removed plot's name does not inherit its
  history.
- The next version number is computed inside the INSERT (`MAX + 1`), so two
  racing saves cannot claim the same version.
- `save_plot` reads a dict spec STRICTLY, so the stored copy is always in the
  current format. `load_saved_plot(plot_id, version=, table=)` restores the
  spec and, when given a table, reconciles it with today's data. It never
  writes. A damaged envelope opens on defaults for its variable.
- Reads never create the table. Writes call `ensure_table`.
- `hide_saved_plot(hidden=False)` un-hides, refused if another visible plot
  now has that name.
- `check_name` is the single definition of a valid name (trimmed,
  whitespace collapsed, ≤120 chars).
- `scistackplot` and `scistackplotdb` were added to the repo-wide AST
  fetch-locking guard (`sciduckdb/tests/test_fetch_locking.py`).
- Tests: `scistackplotdb/tests/test_saved_plots.py`.

### Stage 3: GUI backend
- Handlers in `api/plot.py` (the single handler table): `plot_saved_list`,
  `plot_saved_save`, `plot_saved_open`, `plot_saved_rename`,
  `plot_saved_hide`.
- `plot_saved_open` loads the source table so that `reconcile` runs against
  today's data, and returns `{spec, view, notes, capabilities}` in one round
  trip.
- Loading a saved plot also rewrites the intent-store variant pins for the
  variable, because the user's latest statement wins. As a result, the next
  plain right-click opens on the pins the user last looked at, which matches
  current behaviour.
- Tests in `scistack-gui/tests/test_plot_saved.py`.

**BUILT 2026-09-24 (pytest unrun):**
- `services/saved_plot_service.py` is an adapter only. The six handlers
  (`plot_saved_list/save/open/rename/hide/history`) are in `api/plot.py`,
  and matching routes are in `frontend/src/api.ts`.
- `open_plot` holds the database only while the row and the data frames
  load, via `load_saved_plot(table_for=...)` and the panel's cached source.
  The capability report is computed after release. It is the only
  self-managed (`holds_db_lock=False`) handler of the six.
- **Deviation from the plan:** opening does NOT write the intent-store
  Variants pins. The panel already persists pins whenever `variant_sets`
  changes (`PlotStudio.tsx`, the `plot_variant_sets_save` effect), so
  loading a saved plot into the panel updates them. A backend writer would
  be a second owner. A test pins that opening writes nothing.
- A Stage 2 addition, `load_saved_plot(table_for=callable)`: the loader
  receives the RESTORED spec. A loader that raises opens the plot
  unreconciled, with a note.
- None of the six is `db_optional`, so a plot-only (CSV) server refuses
  them.
- Tests: `scistack-gui/tests/test_plot_saved.py`, plus 2 new tests in
  `scistackplotdb/tests/test_saved_plots.py`.

### Stage 4: frontend
- `SavedPlotsPanel.tsx`: a collapsible rail on the RIGHT (the mirror image of
  the controls rail, same collapse idiom). It holds the list (name, saved
  date), click to open, and a ⋯ menu with Rename / Remove (hide).
- Header: "Save plot" button with a name prompt, prefilled with the loaded
  plot's name. Saving under an existing name asks for confirmation.
- Header shows the loaded plot's name plus a "modified" dot once the spec
  diverges from what was loaded. Opening another plot while modified asks
  first.
- `savedPlots.ts`: `viewState()` / `applyViewState()` with unit tests.
- Restore banner for notes.
- Rebuild both vite targets ([frontend bundle rebuild trap]). Add steps to
  `docs/gui-manual-testing-todo.md`.

**BUILT 2026-09-24 (tsc clean, npm test 344/344, both bundles rebuilt;
pytest for the backend additions unrun; never visually checked):**
- `SavedPlotsRail.tsx`: right-hand rail with collapse strip, list, and ✎/✕
  per row. All questions are asked INLINE, because webviews block
  prompt/confirm: name box, "another plot has that name", discard unsaved
  changes, remove.
- `savedPlots.ts` (in the node test list): `readView` (lenient view
  restore), `modifiedKey`/`isModified` (figure index excluded from
  "modified"), `iterateSignature` (moved out of PlotStudio, single owner),
  and `notesSummary`.
- In PlotStudio, `captureBaseline` takes the baseline from the NEXT
  render's key, so the comparison sees applied state. `baseline` is an
  effect dependency, so saving unchanged settings still captures.
  `restoredSignature` skips the one fan-out reset a load triggers.
- Notes are shown in the rail (yellow box, auto-expands), not on the figure.
- Backend addition: `save_plot(overwrite=False, current_plot_id=)` raises
  `SavedPlotExists` for ANOTHER plot's name. The GUI service returns
  `{ok: False, exists}`. Name normalisation stays in the backend.
- Manual test steps: `docs/gui-manual-testing-todo.md` §0zi.
- Risk to watch: if something adjusts the spec automatically after open, a
  fresh or just-loaded plot shows "modified" immediately. §0zi asks the user
  to check.

### Stage 5: docs
- `docs/claude/saved-plots.md` covering the envelope, the drift policy, the
  ownership table, and why this is not an intent-store aspect.
- README sections in scistackplot (`restore`) and scistackplotdb (saved
  plots from scripts).

### Optional stage 6
- Canvas right-click on a variable gets an "Open saved plot ▸" submenu.

## Caveat to state up front
A saved plot restores **settings, not data**. If the data or the "latest"
variant has changed since the save, the figure is today's data drawn the saved
way. The existing empty-figure and span banners explain any mismatch.

## Decisions (user, 2026-09-24)
1. Storage: project DuckDB `_saved_plot` table.
2. Drift: salvage-with-notes, no upgraders.
3. CSV plots: not supported for now; Saved Plots rail hidden in CSV mode.
4. Re-save same name: append a row, older versions kept as history (never deleted).
5. Old-format keys are not carried forward on re-save. A restore that
   produced notes opens as "modified", so the user is prompted to save it in
   the current format.
6. Stage 6 (canvas submenu) dropped.
