# Plan: custom mark colours (project `[colors]` + per-plot), with a GUI colour picker

Drafted 2026-09-27. Status: ALL 7 STAGES BUILT 2026-09-27. npm tests pass and both bundles are rebuilt. Pytest unrun (the user runs it). GUI §0zzl unchecked. Doc: docs/claude/plot-colors.md.

## Goal

Users pin the colour of summary marks (bars, violins, box fills, line-plot
lines, spaghetti lines, points) per LEVEL of the factor that colours them,
either for one plot or for the whole project (written to `scistack.toml`).
In the GUI, each colour level gets a swatch that opens the native colour
picker plus a text box that accepts hex / `rgb()`.

This mirrors display aliases (`docs/claude/plot-text-and-labels.md`):
plot layer > project layer > palette, it is display-only, and identity never
changes.

## What exists (the seams)

- `render.base.palette_for(resolved, level, fallback)` is already the ONE
  owner of "which colour does a mark level get". It is read by mpl, plotly
  and (through `mark_palette`) codegen. `sample_palette_for` does the same
  for the "Show sample" overlay.
- `palette_for(resolved, None, 0)` is the single colour used when the figure
  has no colour layer.
- `codegen._mark_palette` / `_mark_color` emit `palette=` / `color=`.
- The alias stack gives the templates to copy:
  - `scidb.aliases` owns the TOML grammar (normalize, read on mtime,
    render, with_…).
  - `LongTable` carries a live project reader.
  - `scistackplot.aliases` owns the merge (`DisplayText`, including the
    `keys` lookup so a grouping column `Sex` also finds `Demographics.Sex`).
  - The GUI has `LabelsSection.tsx` + `aliasEdit.ts` +
    `plot_project_alias_set` → `config.set_project_alias`.

## Design

### TOML grammar (project layer). Proposed: a separate `[colors]` table

```toml
[colors.session]             # thing = schema key / variable / "Var.Column" / ColName / Variant
"BL" = "#0072B2"             # level TEXT (quoted; "01" stays "01") = colour
"FU" = "#D55E00"

[colors."Demographics.Sex"]
"F" = "#CC79A7"
```

It is kept separate from `[aliases]` because an alias is what a thing READS
AS, and a colour is a different concept. That calls for a separate owner
module, `scidb.colors`, per NOTE 4. The things it keys on and their lookup
rules are the same as for aliases.

### Colour value grammar: ONE owner, `scistackplot.colors.parse_color`

- Accepts `#rgb`, `#rrggbb`, `rgb(r, g, b)` (0-255), and matplotlib named
  colours when matplotlib is importable.
- Returns canonical lowercase `#rrggbb`, or raises `ColorError` with a
  readable message.
- scidb cannot import scistackplot, so scidb only normalises STRUCTURE and
  keeps value strings as text. scistackplot parses them at merge time and
  WARNs and drops a bad one. The GUI writer (scistack_gui depends on both)
  canonicalises through `parse_color` BEFORE writing, so the file only ever
  holds canonical hex written by the GUI.
- The frontend never parses colours. It sends the raw text and shows what
  Python resolved.

### Merge + application: `scistackplot.colors`

- `PlotSpec.colors: dict[str, dict[str, str]]` (the plot layer) is
  plan-irrelevant (`_PLAN_IRRELEVANT_FIELDS`), so an edit re-renders and
  never re-reduces.
- `MarkColors` (frozen) is built once in `reduce._build_figure` and rides on
  `ResolvedPlot`:
  `{factor: {level: (hex, origin)}}` with origin plot/project/palette.
  It reuses `DisplayText.keys` for factor → lookup keys (one owner of the
  key resolution; extract it to a shared helper if needed, not a copy).
- `palette_for` / `sample_palette_for` consult the pinned colour first,
  then fall back to the palette at the level's DECLARED position. Pinning
  one level therefore never shifts the others.
- The single-colour case (no colour layer): `StyleOptions.mark_color:
  str | None`, plot-level only. This is an open question below.
- A pinned colour equal to another level's (pinned or palette) colour gets
  a WARN, not a refusal.
- Fill alpha is unchanged: `fill_alpha` still applies on top of the pinned
  hue.

### Export (codegen)

- With a hue: emit `palette={level: hex, …}` over the full `hue_order`,
  merged, as a literal. seaborn accepts dict palettes. The overlay's
  `_palette` / `_sample_palette` use the same dicts.
- Without a hue: `color=<mark_color or default>`.
- Colours are baked in, as aliases are: after a project edit, re-export.
  With no pins, the emitted code is byte-identical to today's (tested).

### GUI: a "Colours" section (under Labels)

- It is driven by `layout.meta.colorable` (Python's list), one block per
  painted factor (the mark colour layer, the sample-colour key):
  `{factor, key, levels: [{level, label, hex, origin}]}`. With no colour
  layer, there is one "Marks" row.
- Each level row has:
  - a swatch `<input type="color">` (the native picker works in VS Code
    webviews), which on change writes the plot layer;
  - a text box for hex / `rgb()`;
  - "↑ project" and "✕ project" buttons, as in Labels;
  - "reset", which clears the plot pin.
- An unpinned row shows the resolved colour greyed with its origin
  ("palette" / "project").
- Edits live in a small node-tested `colorEdit.ts` (delete cleared keys, drop
  empty entries, so a reopened saved plot does not read as modified).
- A picker drag fires many `input` events, so the spec write is debounced
  (commit on `change`, preview on `input` throttled) to avoid an RPC storm.
- Write path: `plot_project_color_set` → `plot_service` →
  `config.set_project_color` (whole-file writer + `scidb.colors.render_colors_table`).
- Both vite bundles get rebuilt, and a `docs/gui-manual-testing-todo.md`
  entry is added.

## Logging (NOTE 2)

- `scidb.colors`: an INFO line when the config is read
  (`[colors] <path>: session (2 levels) …`), plus WARNs for malformed entries.
- `scistackplot.colors`: a per-resolve INFO line,
  `colours: session 2/3 pinned (project 2, plot 0); 1 dropped: FU='#zz'`,
  plus a WARN on duplicate colours.
- `config.set_project_color`: an INFO line with thing/level/value.

## Tests

- `scidb/tests/test_colors_config.py`: normalize (bad entries dropped, "01"
  kept), round trip render → read, `with_color`, mtime cache.
- `scistackplot/tests/test_parse_color.py`: every accepted form, and refusals.
- `scistackplot/tests/test_mark_colors.py`:
  - plot beats project beats palette;
  - pinning one level leaves the others' colours unchanged;
  - `Var.Column` key lookup;
  - pins are the same across facets;
  - using `plot_geometry.py`, the drawn colours in mpl AND plotly equal the
    pins (bar, box, violin, line, spaghetti, sample overlay);
  - codegen emits a dict palette, and with no pins the output is unchanged;
  - the exported figure's colours match the preview's
    (feedback: export matches preview).
- `scistack-gui` pytest for `set_project_color` (pyproject refused, no-config
  refused, rest of the file preserved).
- `colorEdit.test.ts`.

## Stages

1. `scistackplot.colors.parse_color` + tests.
2. `scidb.colors` grammar (read/normalize/render/with_color) + `LongTable`
   live reader + tests.
3. `PlotSpec.colors` + `StyleOptions.mark_color` + `MarkColors` merge;
   `palette_for`/`sample_palette_for` consult it; renderer tests.
4. codegen dict palette + parity tests.
5. `layout.meta.colorable` + RPCs + `config.set_project_color`.
6. GUI Colours section + `colorEdit.ts` + bundles + manual-test doc entry.
7. Docs: `docs/claude/plot-colors.md`, README section, ADR in decisions.md.

## Decisions (user, 2026-09-27)

1. Project colours go in a SEPARATE `[colors.<thing>]` table, owned by
   `scidb.colors`.
2. Pins also paint the "Show sample" overlay when it is coloured by the same
   key.
3. The single mark colour (no colour layer) can be set per plot
   (`StyleOptions.mark_color`) AND as a project default.
   - Grammar: `[colors] default = "#rrggbb"`. A STRING directly under
     `[colors]` is a setting and a TABLE is a thing, so a variable named
     `default` cannot collide.
   - Order: plot `mark_color` > project `default` > `DEFAULT_PALETTE[0]`.
   - The GUI "Marks" row gets its own "↑ project" / "✕ project" buttons.
4. Heatmap colormaps stay out of scope (they are continuous).
