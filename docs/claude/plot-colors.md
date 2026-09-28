# Plot colours: pinned mark colours per level

*Written 2026-09-27, after the build. Plan: `.claude/plan-custom-mark-colors.md`.*

**Status:** all 7 stages built 2026-09-27. npm tests pass (490/490) and both
vite bundles are rebuilt. Pytest has not been run. Not checked by eye in the
GUI (manual tests §0zzl).

This covers one question: **what colour is a summary mark painted in**. That
means bars, box fills, violin bodies, strip points, line-plot lines, spaghetti
lines, and the "Show sample" points and lines. It is display only, like the
aliases (`plot-text-and-labels.md`): no row, order, position or number changes.

## Three layers, first match wins

For each level of the factor that paints a mark:

1. **The plot:** `PlotSpec.colors` = `{thing: {level text: colour text}}`,
   stored with the saved plot.
2. **The project:** `[colors]` in `scistack.toml`
   (`[tool.scistack.colors]` in `pyproject.toml`).
3. **The palette:** `render.base.mark_palette` (`style.palette`, else
   Okabe-Ito), at the level's **declared** position. Pinning one level
   therefore never moves another level's colour.

A figure with **no colour layer** paints every mark one colour. The order
there is plot `style.mark_color`, then the project's `[colors] default`, then
the palette's first colour.

A plot value of `""` means "use the palette here, even though the project
pins it", in the same way as a plot alias of `""`. There is no GUI control
for it yet.

```toml
[colors]
default = "#333333"           # a STRING under [colors] is a setting (only `default`)

[colors.session]              # a TABLE is a thing: schema key, variable,
"BL" = "#0072b2"              # "Var.Column", ColName, Variant
"01" = "#d55e00"              # level keys are TEXT, quoted: "01" stays "01"

[colors."Demographics.Sex"]
"F" = "#cc79a7"
```

The string-vs-table type rule means a variable called `default` is a thing,
never the setting. The normalised form (`scidb.colors.ColorTable`) has the
same flat shape as the TOML. `default_of` / `things_of` split it, and so does
`LongTable.project_colors` on the scistackplot side.

**Why this is separate from `[aliases]`** (user decision): an alias is what
a thing READS as, and a colour is how it is PAINTED. They are two concepts,
so they have two owners (CLAUDE.md NOTE 4). They share only the lookup keys
(below) and the TOML string escaping (`scidb.aliases._toml_str/_toml_key`).

## Owners

| Concept | Owner |
|---|---|
| `[colors]` TOML grammar (read, shape-check, render, one-edit) | `scidb.colors` (`normalize`, `colors_in`, `project_colors`, `render_colors_table`, `with_color`, `validate`) |
| What colour TEXT may be (`#rgb`, `#rrggbb`, `rrggbb`, `rgb(r,g,b)`, matplotlib names) → canonical `#rrggbb` | `scistackplot.colors.parse_color` |
| Plot-over-project merge, with origins | `scistackplot.colors.merge` / `mark_colors` → `MarkColors` |
| Factor → lookup keys (a grouping column `Sex` finds `Demographics.Sex` first) | `scistackplot.aliases.lookup_keys` (shared with the aliases) |
| The colour a mark is DRAWN in | `render.base.palette_for` / `sample_palette_for`, which read `resolved.colors` first |
| What the GUI's Colours section lists | `render.base.colorable` → `layout.meta.colorable` |
| Writing the project layer | `scistack_gui.config.set_project_color`: it canonicalises through `parse_color`, then calls `scidb.colors.with_color` and the whole-file writer |

scidb cannot import scistackplot, so the project layer arrives as TEXT and is
parsed at merge time. A bad value gets a WARN and is dropped, and the figure
still draws. The GUI writer refuses a bad value before anything is written
(`ColorError`, a `ValueError`, returned as `{ok: false, error}`). So the file
only ever holds `#rrggbb` written by the GUI, plus whatever someone typed by
hand.

Refused colour text:
- alpha channels (`#rrggbbaa`), because fill opacity belongs to
  `render.base.fill_alpha`;
- matplotlib cycle references (`C0`), because they depend on the rc in force;
- any colour that is not opaque.

## How pins reach a figure

- `reduce.resolve` / `resolve_one` merge once per resolve, as they do for
  aliases. They call `mark_colors(plan.spec, plan.table)` and log it via
  `colors.log_summary`.
- `_build_figure` puts the result on `ResolvedPlot.colors`, told the figure's
  roles with `.for_figure(color=…, sample_color=…)`.
- `MarkColors.mark(level)` returns the colour layer's pin, or the single
  colour when there is no colour layer. `.sample(level)` returns the
  overlay key's pin.
- **The overlay:** a pin on the key that colours the "Show sample" points
  paints them too (user decision). A subject's colour is its colour
  everywhere. In "Mark's colour" mode the points take their mark's colour,
  pinned or not.
- **Nothing is rebuilt.** `colors` is in `_PLAN_IRRELEVANT_FIELDS`
  (`mark_color` lives in `style`, which already was). The table carries a
  live reader, `LongTable.colors_source`, set by
  `sources.base._attach_project_sources` together with `aliases_source`.
  `ScidbSource._colors_source` reads `DatabaseManager.dataset_colors`, which
  is live, mtime-cached, and validated when the file content changes. None
  of this is in `_cache_generation`.

## Exported code

`codegen` bakes the merged pins in as literals, as it does for aliases (a
project edit needs a re-export). With no pins, it emits exactly what it did
before. `test_no_pins_emit_no_pin_code` checks this.

- **With a hue:** `_hue_palette_lines` emits
  `_hue_pins = {...}` and
  `_hue_palette = [_hue_pins.get(str(v), c) for v, c in zip(_hue_order, sns.color_palette(<palette>, len(_hue_order)).as_hex())]`.
  The call gets `palette=_hue_palette`, and the overlay's `_palette` is
  `dict(zip(_hue_levels, _hue_palette))`.
- **Without a hue:** `_mark_color(spec, table)` emits `color=` with the
  merged single colour.
- **The overlay's own key:** `_sample_palette.update({...})`.

## GUI: Appearance > Colours

`ColorsSection.tsx` sits under Labels. Its edit logic is in `colorEdit.ts`,
which is node-tested.

- It shows one block per `layout.meta.colorable` entry:
  - the colour layer (`colour`);
  - the overlay key (`sample colour`);
  - or one `marks` row (key `null` → `style.mark_color` /
    `[colors] default`).
  
  Heatmaps get none: a colormap is continuous, and it is out of scope.
- Each level row has:
  - a **swatch** (`<input type="color">`) showing the DRAWN hex. A drag
    updates the swatch locally, and only the last value of a burst is
    committed (`debounced`, 250 ms, flushed on blur and on a project button).
    Without this, every picker step was a resolve.
    
    The debouncer reads `onChange` through a ref and is built once. If it
    were rebuilt every render, its cleanup would cancel the pending commit.
  - a **text box**, committed on Enter or blur, never per keystroke. `#ff`
    on the way to a full colour is not a colour, and each keystroke would
    resolve and WARN.
  - **↑ project / ✕ project**, which call `plot_project_color_set`.
- The panel never parses a colour. Typed text goes into the spec as written,
  and Python canonicalises it.
- The help text about formats lives in `colorEdit.COLOR_FORMATS`, because
  `plotTheme.test.ts` forbids colour literals in `.tsx` files.
- Cleared values delete their key, so a reopened saved plot does not read as
  "● modified".

## Diagnosing

- **"My colour doesn't show":**
  - `grep "\[colors\]" scidb.log` shows which config was read.
  - The per-resolve INFO line
    `colours: levels session 2/3 pinned (project 2, plot 0); single mark colour palette; …`
    lists pins that name no level (`1` vs `"01"`) and entries dropped as not
    colours.
  - A WARN `colours: … is ignored — <reason>` names the bad value.
- **"Two levels look the same":** `colors.warn_duplicates` WARNs when a pin
  repeats another level's colour (it runs from `colorable`, once per preview
  render).
- **"The export has the old colour":** re-export the step after the project
  edit.
- **"The project write vanished":** every `_render_scistack_toml` caller
  passes `colors=section.get("colors")`. A new caller that forgets it would
  silently drop `[colors]`. `test_colours_survive_every_other_write` covers
  one caller (the alias write).

## Tests

- `scistackplot/tests/test_parse_color.py`
- `scistackplot/tests/test_mark_colors.py`
- `scidb/tests/test_colors_config.py`
- `scistack-gui/tests/test_config.py` (`set_project_color` block)
- `scistack-gui/tests/test_plot_service.py` (last two tests)
- `scistack-gui/frontend/src/components/PlotStudio/colorEdit.test.ts`
