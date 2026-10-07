# Plot presets

*Written 2026-10-06. Plan: `.claude/plan-plot-presets.md`. Manual checks:
`docs/gui-manual-testing-todo.md` §0zzz. Sibling feature: `saved-plots.md`.*

## What it is

A **saved plot** is settings + data for one variable. A **preset** is the
same settings with the data taken out, kept for the whole project. Apply it
to any variable's plot and that variable is drawn the same way: same kind,
roles, grouping, facets, text sizes, colours, difference bars, and so on.

**Text sizes and the automatic size** (2026-10-07). `style.text` is template,
so a preset carries what the user SET: a fixed Font, or auto (`base` unset,
dropped by `to_dict`) plus its Print/Slide `target`. The sizes auto chose are
never stored. They belong to one figure's labels at one size
(`ResolvedPlot.auto_text`), so an applied auto preset is sized again for the
new variable. Presets saved before 2026-10-06 stored `base: 14` and apply it
fixed. Tests: `test_presets.py`, "Automatic text size".

Decisions (user, 2026-10-06):

- **D1** Presets live in the project database only (not per user, not
  across projects).
- **D2** The title, y label and y limits stay with the variable: a preset
  never stores them, and applying one keeps the panel's own.
- **D3** Applying replaces every setting the preset owns. It is not a merge
  of "the fields the preset changed from defaults".

## What a preset owns: one table

`scistackplot/presets.py: FIELD_CLASSES` classifies every `PlotSpec` setting.
It is read on save (`make_template` strips) and on apply
(`apply_preset` takes those settings from the target), so the two cannot
disagree.

| Class | Settings | Saved? | On apply |
|---|---|---|---|
| DATA | `measures`, `variant_sets` | no | the target's |
| VARIABLE_TEXT | `style.title`, `style.y_label`, `y_axis.minimum/maximum`, the measure's own `aliases` entry, `panel_overrides[].y_minimum/y_maximum/y_label` | no | the target's (per-panel ones matched by the panel's `match`) |
| REBASED | `factor_variables` | yes; own-column groupings (`Gait.Side` on a Gait plot) are stored as `<measure>` | `<measure>` becomes the target variable; if it has no such column, the grouping is dropped with a note |
| TEMPLATE | everything else | yes | the preset's |

`test_every_spec_field_is_classified` fails when a new `PlotSpec` field (or a
new field of `StyleOptions`, `YAxis`, `PanelOverride`) is not placed. **Adding
a spec field means deciding here whether it travels with a preset.**

## Applying is the saved-plot salvage path

`apply_preset(template, target, table=|table_for=)`:

1. Strip the stored template again (a setting reclassified since it was
   saved still stays with the variable), then fill in the target's DATA and
   VARIABLE_TEXT settings.
2. `restore_spec`, so a preset saved by an older build still applies. Same
   policy as saved plots: salvage with notes, never migrate.
3. Load the target's table with `table_for(spec)`. If that fails and the spec
   has own-column groupings, retry without them and note each dropped
   grouping. (`ScidbSource._own_column_groups` raises for a missing column,
   and it is the only judge of that.)
4. `reconcile(spec, table)`: roles, shown sample keys, comparison layer and
   aliases the target's data lacks become `not_in_data` notes. This is the
   same function and the same rules as for saved plots.
5. `_fit_kind`: if `capabilities(spec, table)["available"]` lacks the
   preset's kind, use `capabilities()["default"]` and add a
   `kind_unavailable` note. `capabilities` is the one judge, so the panel
   never shows a kind it then greys out. A scalar kind (box) on a 1-D
   variable is available (cells are collapsed), so it is kept.

Nothing is written on apply. The stored preset is never rewritten.

## Storage

`_plot_preset (preset_id, name, version, saved_at, hidden, envelope_json)`
in the project DuckDB, owned by `scistackplotdb/presets.py`. The envelope:

```json
{"format": 1, "template": {...}, "made_on": {"variable": "StepLength", "shape": "scalar"},
 "saved_with": {"scistackplot": "0.1.0"}}
```

`made_on` is display only ("made on StepLength (scalar)", and the shape
warning). Nothing about applying depends on it.

The rules (the id is the identity and the name a label; re-saving a name
appends a version; remove = hide; reads never create the table) are the
**same object** as for saved plots: `scistackplotdb/_versioned.py:
VersionedStore`, with `scope_column=None` (names are unique across the
project) where saved plots use `scope_column="variable"`.

## The GUI

- **Backend:** `services/plot_preset_service.py` (an adapter) and six
  handlers `plot_preset_list/save/apply/rename/hide/history` in
  `api/plot.py`. `plot_preset_apply` is self-managed: it holds the
  connection only while the preset row and the target's frames load, the
  same split as `plot_saved_open`. The list takes the panel's `shape` and
  returns `shape_warning` per preset (`scistackplot.shape_warning` owns the
  wording).
- **Frontend:** `PresetsSection.tsx`, rendered under the saved plots in the
  same right-hand column (`SavedPlotsRail` `children`). `presets.ts` holds
  the React-free wording (`madeOnLabel`, `appliedSummary`).
- **Applying is `setSpec`**: one undo step on the panel's own stack, so
  Ctrl+Z restores the settings from before. There is no confirmation
  question for that reason.
- Applying never writes the Variants pins. They are DATA and are unchanged,
  and the panel's own effect is their one writer. Covered by
  `test_applying_writes_no_variant_pins`.
- A saved plot that is open stays open. "Apply preset, then Save" updates
  that saved plot.

## Using it from a script

```python
from scistackplot import render
from scistackplotdb import ScidbSource, apply_preset_to, find_preset

source = ScidbSource(db)
table_for = lambda s: source.get_table(
    s.variant_variables(), x_measure=s.x_measure, factor_variables=list(s.factor_variables)
)
preset = find_preset(db, "Session box")
applied = apply_preset_to(db, preset.preset_id, "StepWidth", table_for=table_for)
for note in applied.notes:
    print(note.path, note.kind, note.message)
figure = render(table_for(applied.spec), applied.spec)
```

A bare variable name with no `table` starts from `PlotSpec(measures=[name])`
with no variant pins. Pass `table=` (it then starts from `default_spec`) or a
full target spec when the variable has several variants.

## Traps

- **New spec field:** classify it in `FIELD_CLASSES` (the guard test tells
  you). If it is variable-specific text or limits, it is VARIABLE_TEXT, not
  TEMPLATE.
- **Don't make apply merge "changed" fields** (D3). A partial "style only"
  preset would be a new feature with its own classification, not a change
  to this one.
- **The GUI table loader** is `plot_service._table_for`. Scripts must pass
  `factor_variables` too, or own-column groupings are never validated and
  the rebase is never checked.
- **"Strict" on save means `PlotSpec.from_dict`.** It refuses legacy keys
  and unknown `style` keys, but it IGNORES an unknown top-level key. That is
  `from_dict`'s rule, and `save_plot` follows the same one. Don't write a
  test that expects an unknown top-level key to be refused (2026-10-06).
