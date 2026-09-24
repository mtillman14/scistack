# Saved plots

*Written 2026-09-24. Plan and stage-by-stage build notes:
`.claude/plan-saved-plots.md`. Manual checks: `docs/gui-manual-testing-todo.md` §0zi.*

## What it is

A **saved plot** is a named Plot Studio figure attached to a variable. The
user saves the settings in the panel under a name. Later, in a new tab or a
new session, they pick the name from the **Saved plots** rail and get the
same figure back: spec, preview mode, figure-size choice, and which figure
of a Separate-figures set was showing.

One saved plot = one `PlotSpec` + the panel's view settings. A spec that fans
out into several figures (ITERATE) is still one saved plot.

## Where each part lives

| Concept | Owner | Consumers |
|---|---|---|
| Reading a stored spec whose shape may have drifted | `scistackplot/restore.py`: `restore_spec` | `scistackplotdb.saved` |
| Checking a restored spec against today's data | `scistackplot/restore.py`: `reconcile` (roles through `variants.stale_role_names`) | `scistackplotdb.saved` |
| "Which roles no longer name a factor?" | `scistackplot/variants.py`: `stale_role_names` | `strip_answered_roles` (acts at resolve), `reconcile` (reports) |
| Storage, versions, names, hiding | `scistackplotdb/saved.py` | GUI service, scripts |
| What a valid name is, and whether it clashes | `scistackplotdb.saved.check_name`, `SavedPlotExists` | GUI (displays the answer) |
| JSON adapter + loading the table with the panel's cached source | `scistack_gui/services/saved_plot_service.py` | the six `plot_saved_*` handlers in `api/plot.py` |
| View settings (preview mode, aspect choice, figure index): meaning + lenient read | `frontend/.../PlotStudio/savedPlots.ts` | `PlotStudio.tsx` |
| "Has the panel changed since open/save?" | `savedPlots.ts`: `modifiedKey`/`isModified` | `PlotStudio.tsx`, `SavedPlotsRail.tsx` |
| When the figure cursor resets | `savedPlots.ts`: `iterateSignature` | `PlotStudio.tsx` |
| The Variants pins for a variable (intent store) | the panel's `plot_variant_sets_save` effect | **not** the saved-plot code |

The last row is deliberate. Loading a saved plot changes the panel's
`variant_sets`, and the existing effect persists that like any other edit. A
backend write on open would be a second owner. `test_opening_writes_no_variant_pins`
pins it.

**Why not the intent store:** a saved plot is *display intent*. Changing
it changes nothing a run computes, so by `intent-and-fact.md` §1 it is out
of that store's scope.

## Storage

One append-only table, `_saved_plot`, in the project DuckDB:

```
plot_id VARCHAR, variable VARCHAR, name VARCHAR, version INTEGER,
saved_at VARCHAR, hidden BOOLEAN, envelope_json VARCHAR,
PRIMARY KEY (plot_id, version)
```

- **`plot_id` is the identity; the name is a label.** Rename is a plain
  UPDATE of `name`. A new plot that reuses a removed plot's name gets a new
  `plot_id` and does not inherit the old history.
- **Re-saving a visible name appends a version.** The newest version is
  what the list shows and what opens. Older versions open by number
  (`load_saved_plot(version=)`, `plot_saved_history`).
- The version number is computed inside the INSERT (`MAX(version) + 1`), so
  two racing saves cannot claim the same one.
- **Nothing is ever deleted.** "Remove" sets `hidden` on every version.
  `hide_saved_plot(hidden=False)` brings a plot back, and is refused if
  another visible plot has taken its name in the meantime.
- **Reads never create the table.** Listing an untouched database does not
  write to it. Writes call `ensure_table`.
- All reads go through `_fetchall`/`_fetchone`. `scistackplot` and
  `scistackplotdb` are in the AST guard `sciduckdb/tests/test_fetch_locking.py`.

The envelope:

```json
{"format": 1, "spec": <PlotSpec.to_dict()>, "view": {...}, "saved_with": {"scistackplot": "0.1.0"}}
```

`format` is logged on every open. **Nothing branches on it.**

## Drift: salvage, never migrate

The spec's shape will keep changing, sometimes drastically. The project rule
is no migration code (feedback_beta_no_deprecation), so saved plots are
handled by one policy:

1. **Every save writes the current format.** `save_plot` reads an incoming
   dict with the STRICT `PlotSpec.from_dict`, and a spec that does not parse
   is refused (`SavedPlotError`). The stored copy is always exactly what
   this build writes.
2. **Every open salvages.** `restore_spec` walks the stored dict field by
   field, driven by each dataclass's own type hints (`typing.get_type_hints`),
   so there is no second copy of the spec's shape to fall out of date.
   - A key the spec no longer has → `unknown_setting` note, ignored.
   - A value that no longer parses (bad enum, wrong type) →
     `invalid_value` note, field default.
   - A list or table entry that cannot be built (a filter with no column,
     an old role word) → one `dropped_entry` note. Notes about its inner
     fields are discarded with it.
   - Unusable `measures` → the variable the plot was saved under
     (`measure_replaced`).
   - A class with its own `from_dict` re-normalises the salvaged object, so
     its wire format has one owner (LocationFilter pair filtering,
     FactorVariable selection).
   - It never raises except for "no measure and no fallback".
3. **Opening never writes.** The stored copy stays as it was, so a later
   build may salvage more of it.
4. **A plot restored with notes opens as modified** (`baseline = null`), and
   the rail says "Save to keep these settings in the current format". One
   re-save writes only current-format keys. Old keys survive only in the
   hidden older versions, where nothing reads them.

A renamed setting therefore comes back at its default, with a note naming
the old key. That trade-off was chosen explicitly by the user over writing
upgraders.

The panel's view settings drift too. `readView` applies the same policy on
the frontend: known keys that still make sense are kept, and everything else
takes the panel default.

## Reconciling with today's data

A spec can parse and still name things the data no longer has.
`reconcile(spec, table)`, run when a table (or a `table_for` loader) is
given:

| Stale reference | Action | Why |
|---|---|---|
| role for a factor in neither the raw nor the derived table | **note, keep** | `strip_answered_roles` drops it at every resolve. Keeping it means the role returns if the factor does |
| `show_sample` key, `sample_color` | **note, remove** | `roles.validate` refuses them, so the figure would not draw |
| the measure | note | nothing to substitute |

"Derived" means after variant sets and level groups are applied, so `Phase`
and `Variant` count as present. The GUI passes `table_for`, so the table is
loaded from the RESTORED spec with the panel's cached source. The data a plot
needs depends on its spec. If the loader raises, the plot still opens,
unreconciled, with a note.

## The GUI

- Six handlers, one row each in `api/plot.py`: list, save, open, rename,
  hide, history. None is `db_optional` (a CSV plot has no rail).
  `plot_saved_open` is the only self-managed one: it holds the connection
  while the row and the data frames load, then computes the capability
  report after release.
- `save(overwrite=False, current_plot_id=…)`: saving under the name of the
  plot you have open appends silently. Saving under ANOTHER plot's name
  returns `{ok: false, exists}` and the rail asks. The name comparison is
  never repeated in TypeScript.
- Every question is asked inline in the rail, because a VS Code webview
  blocks `window.prompt`/`confirm`.
- **Modified** compares `modifiedKey(spec, view)`, where the figure index
  is excluded because browsing is not editing. The baseline is captured from
  the render AFTER settings are applied (`captureBaseline`), with `baseline`
  as an effect dependency so that saving unchanged settings still captures.
- **Figure index:** changing ITERATE roles normally resets the cursor to 0.
  On a load, `restoredSignature` records the incoming signature and skips
  exactly that one reset.

## Using it from a script

```python
from scistackplot import render
from scistackplotdb import ScidbSource, list_saved_plots, load_saved_plot

source = ScidbSource(db)
plot = list_saved_plots(db, "StepLength")[0]
saved = load_saved_plot(db, plot.plot_id,
                        table_for=lambda s: source.get_table(s.variant_variables()))
for note in saved.notes:
    print(note.path, note.message)
figure = render(source.get_table(saved.spec.variant_variables()), saved.spec)
```

## Traps

- **Don't make `restore_spec` stricter to "catch bugs".** Its job is to
  never lose a saved plot. Strictness belongs in `save_plot`, which is where
  it is.
- **Don't add upgrade code for a renamed field.** Put a note in the
  changelog instead. The notes already tell the user what reverted.
- **Adding a PlotSpec field:** `test_fixture_sets_every_field_to_a_non_default_value`
  fails until `full_spec()` in `scistackplot/tests/test_restore.py` sets it.
  That is the round-trip guard doing its job.
- **Adding a view setting:** add it to `PlotView`, `viewState`, `readView`,
  and, if it changes how the figure looks, `modifiedKey`.
- If a fresh or just-loaded plot shows "● modified" without any edit,
  something is adjusting the spec after it is applied. Find that, don't
  weaken `modifiedKey`.
