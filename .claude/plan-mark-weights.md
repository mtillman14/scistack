# Plan: mark weights for "Show sample" and spaghetti

User request (2026-09-24): one "boldness" setting that scales BOTH the point
size and the connecting-line thickness, for the Show sample overlay and for
spaghetti plots.

## User decisions (2026-09-24)

- **Two knobs.** `StyleOptions.sample_weight` scales the overlay's points +
  lines; `StyleOptions.line_weight` scales a spaghetti's own points + lines.
  On a spaghetti with a sample the two are independent (faint trials, bold
  subject lines).
- **Multipliers**, default `1.0` = today's look exactly. One factor scales the
  marker diameter and the line width together, so they stay in proportion.

## Design

### One owner: `scistackplot/weights.py`

- `ResolvedWeight(weight, marker_pt, line_pt)` — marker DIAMETER in pt
  (matplotlib `markersize`), `marker_area` (scatter `s`, pt²), `marker_px`
  (plotly), `line_pt` (both backends, 1 pt = 1 px as today).
- `sample_weight(style)` / `spaghetti_weight(style)` resolve the style's
  multiplier against today's constants. `SAMPLE_MARKER_FRACTION` and
  `SAMPLE_LINE_WIDTH` MOVE here from `render.base` (clean break) along with
  the spaghetti line width (1.2) that mpl and plotly each hard-coded.
- Plotly markers: today's preview draws markers at 8 px where matplotlib draws
  6 pt, i.e. px = pt x 4/3 (the 96/72 pt->px ratio). Pinned as
  `PLOTLY_PX_PER_PT` in the owner, so 1x reproduces the preview exactly.
- `mark_weights_meta(resolved)` -> `layout.meta.mark_weights` for the GUI:
  `{sample: {applies, weight, marker_pt, line_pt}, lines: {...}}`. Python
  decides which knob applies (spaghetti kind / an overlay present); the GUI
  never tests `kind === 'spaghetti'`.
- Edge width of overlay markers (0.5) stays fixed.

### Consumers

- `render/mpl.py`: `_draw_spaghetti`, `_draw_sample`.
- `render/plotly_.py`: `_spaghetti_traces`, `_sample_traces`, + meta.
- `codegen.py`: the overlay block reads the owner; the spaghetti `relplot`
  now passes explicit `linewidth=` and `markersize=` (it passed neither, so
  the export drew seaborn's defaults, not the preview's 1.2 pt — an existing
  export != preview difference, fixed here).

### Validation

`roles.validate` refuses a non-positive / non-finite weight. `restore_spec`
needs nothing: new fields default, old saved plots restore at 1x.
`style` is already plan-irrelevant, so a weight change never re-reduces.

### Logging

The INFO `figure size ...` line in `render_mpl` gains the weights when one
applies (`marks: lines 1.5x (marker 9.0 pt, line 1.8 pt); sample 0.5x ...`).

### GUI

- "Show sample" section: **Weight** number box (0.25-4, step 0.25), shown
  once a key is shown, bound to `style.sample_weight`.
- "Plot type" section: **Line weight** shown when
  `layout.meta.mark_weights.lines.applies`.
- 1x deletes the key (like the text sizes) so a reopened saved plot does not
  read as "modified".
- Manual test entry in `docs/gui-manual-testing-todo.md`.

### Tests (`scistackplot/tests/test_mark_weights.py`)

- owner: 1x reproduces today's constants; 2x doubles diameter and line width.
- mpl: overlay Line2D / PathCollection sizes and spaghetti lines scale.
- plotly: trace marker size / line width scale; meta applies flags.
- codegen parity: generated spaghetti + overlay line widths / marker sizes
  equal the preview's (exec the export, compare Line2D).
- validate refuses 0 / negative.

### Docs

`docs/claude/show-sample-overlay.md` + `spaghetti-plot.md` owner rows; README
style section if it lists style fields.
