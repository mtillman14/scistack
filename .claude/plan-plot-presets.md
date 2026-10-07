# Plan: Plot presets (user-defined plot types)

*Drafted 2026-10-06. Decisions D1–D3: recommended options chosen (user, 2026-10-06). All 5 stages built 2026-10-06: tsc clean, npm test passes, both bundles rebuilt; pytest NOT yet run (user runs it); GUI §0zzz unchecked. Uncommitted.*

## The idea

A **saved plot** = settings + data, attached to ONE variable.
A **preset** = the settings with the data taken out, attached to NO variable.
Applying a preset to a new variable = "draw THIS variable the way I drew that one".

Example: on `StepLength` the user builds "subjects collapsed, box per session,
coloured by limb, faceted by condition, journal text sizes, custom colours,
difference bars session 1 vs 2". Saves it as preset **"Session box"**. Opens
`StepWidth`, picks "Session box", and gets the same figure for StepWidth.

## What is "the data"? — one owner: `scistackplot/presets.py`

Every `PlotSpec` field gets exactly one classification, in ONE table in
`presets.py`. That table is used both when saving (strip) and when applying
(take from the target), so the two can never disagree (NOTE 4).

| Class | Fields | On save | On apply |
|---|---|---|---|
| **DATA** | `measures`, `variant_sets` | dropped | taken from the target panel (its default/pinned variant via `default_spec`) |
| **VARIABLE TEXT** (see D2) | `style.title`, `style.y_label`, `y_axis.minimum/maximum` | dropped | target's (auto from variable name / autoscale) |
| **REBASED** | `factor_variables` entries where `is_own_column(old measure)` | stored with the old measure marked | rewritten to the new measure if it has that column, else dropped with a note |
| **TEMPLATE** | everything else: `kind`, `roles`, `groups`, `color`, `aggregate`, `cell_statistic`, `show_sample`, `join_sample`, `sample_color`, `facet`, `y_axis.scope`, rest of `style`, `aliases`, `colors`, `panel_overrides`, `difference_bars`, `comparison`, `filters`, `location_filter`, other `factor_variables`, `level_groups`, `x_measure`, `index_column` | stored | applied, then checked against the new data |

Guard test (same pattern as `test_fixture_sets_every_field_to_a_non_default_value`):
**every PlotSpec field (and nested StyleOptions/YAxis field) must be classified**;
adding a field without classifying it fails the test.

### Applying = the existing salvage path, not new logic

`apply_preset(template: dict, target: PlotSpec, table) -> Restored`:

1. `restore_spec(template + target's DATA fields)` — drifted presets salvage
   exactly like drifted saved plots (no migration code).
2. Rebase own-column factor variables (note if dropped).
3. `reconcile(spec, table)` — roles/show_sample/colours naming factors the new
   variable lacks get the existing `not_in_data` notes (roles kept-but-inert,
   show_sample removed, etc.).
4. **Kind check:** if `kind` is not in `available_plots(new shape, roles)`
   (e.g. a scalar "box" preset applied to a 1-D variable), fall back to
   `default_plot(...)` with a new `NoteKind.KIND_UNAVAILABLE` note.

Every step logs; `_log_notes("apply_preset", notes)` + one INFO line
`[preset] applied "Session box" to StepWidth: 2 note(s), kind box→line`.

The envelope records `made_on: {variable, shape}` so the rail can say
"made on StepLength (scalar)" and flag presets whose shape differs from the
current variable (still applicable, just warned).

## Storage — `scistackplotdb/presets.py`

Table `_plot_preset (preset_id, name, version, saved_at, hidden, envelope_json,
PRIMARY KEY (preset_id, version))` — same rules as `_saved_plot`: id is identity,
name is a label, re-save appends a version, remove = hide, reads never create
the table, `_fetchall/_fetchone` only (add to the fetch-locking AST guard).

To avoid two copies of "named, versioned, hideable rows" (NOTE 4), extract the
generic machinery from `saved.py` into `scistackplotdb/_versioned.py`
(parameterised by table + optional scope column), and have both `saved.py` and
`presets.py` consume it. Existing `scistackplotdb/tests/test_saved*.py` guard
the refactor. (No schema change to `_saved_plot`, so no migration.)

Save is strict (`PlotSpec.from_dict` on the full spec before stripping), open
salvages — identical policy to saved plots.

## GUI

- **Backend:** `services/plot_preset_service.py` (adapter only) + handlers
  `plot_preset_list/save/apply/rename/hide/history` in `api/plot.py`.
  `plot_preset_apply(preset_id, variable, current_spec)` loads the table with the
  panel's cached source, calls `apply_preset`, returns spec + notes + capabilities
  in one round trip (same shape as `plot_saved_open`).
- **Frontend:** a 6th collapsible rail group **Presets** (next to Saved plots):
  "Save settings as preset…" (inline name prompt), list with **Apply**,
  rename, remove, history; "made on X (shape)" subtitle; notes shown with the
  existing restore-notes UI.
- Applying does NOT touch the variable's Variants pins (frontend effect stays
  the single owner — same rule as saved plots). The panel becomes "modified";
  if a saved plot is loaded it stays loaded, so "apply preset, then Save"
  updates that saved plot.
- `docs/gui-manual-testing-todo.md`: new section for presets.

## Script use

```python
from scistackplotdb import list_presets, apply_preset_to
spec, notes = apply_preset_to(db, "Session box", "StepWidth")
```

## Stages

1. `scistackplot/presets.py`: partition table, `strip_to_template`,
   `apply_preset`, `KIND_UNAVAILABLE` note. Tests: partition guard, round trip,
   apply onto a different variable, own-column rebase, kind fallback,
   reconcile notes, target variant_sets kept.
2. `scistackplotdb`: extract `_versioned.py`, add `presets.py`. Tests: storage,
   versions, hide/unhide, name clash, saved-plot tests still pass.
3. GUI backend service + handlers. Test `tests/test_plot_presets.py`.
4. Frontend rail group + `presets.ts` + tests; rebuild BOTH vite targets.
5. Docs: `docs/claude/plot-presets.md`, ADR entry, manual-test section.

## Open decisions

- **D1 Scope:** project DB only (recommended, matches saved plots) vs.
  user-global (shared across projects) vs. both.
- **D2 Variable text:** drop title / y-label / y-limits from presets
  (recommended) vs. keep them.
- **D3 Apply semantics:** replace all template settings (recommended) vs.
  merge only the fields the preset changed from defaults.

## Out of scope (follow-ups)

- "Default preset" per variable/shape (open new plots on it automatically).
- Partial presets ("style only").
- Export/import preset JSON between projects (moot if D1 = global).
