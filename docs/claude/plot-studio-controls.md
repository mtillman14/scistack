# Plot Studio controls: where each one lives and what it writes

The left rail of Plot Studio (`scistack-gui/frontend/src/components/PlotStudio/PlotStudio.tsx`)
and the toolbar over the figure. This file answers "where does control X go,
and which field does it write?" when a control is added or moved.
The design history is in `.claude/plot-studio-sidebar-redesign.md`.

## Principles

1. **Workflow order.** The rail has five collapsible groups, in the order a figure
   is built: Data, Chart, Structure, Statistics, Appearance. A new control goes
   in the group that matches the question it answers, not wherever there is room.
2. **What is drawn vs. how it looks.** Data, Chart, Structure and Statistics
   change *what* the figure shows (`PlotSpec` meaning fields). Appearance only
   changes *how it looks* (`PlotSpec.style` = `StyleOptions`, plus aliases).
   View-only settings (not part of the saved figure) and actions that take the
   figure out of the panel go in the **figure toolbar**, never in the rail.
3. **The panel displays, the backend decides.** Availability, reasons, roles,
   resolved sizes and limits come from Python (`capabilities`, `layout.meta`).
   Group summaries (`sidebarGroups.ts`) only restate the spec; they never
   decide anything.
4. **Explanations behind ⓘ, facts always visible.** A fixed explanation goes
   in `Section`'s `hint` prop (hidden until ⓘ is clicked). A note about the
   *current* figure (a refusal, a layout note, the applied y-limits, the
   label-fit verdict, the sample note) is a child and is always shown.
5. **Width comes from measurement, never from an assumed number.** The rail is
   280 px today and may become user-resizable. `controlsRef` measures it, and
   `railWidthClass` turns the width into `narrow` (<300 px), `normal` (<420 px)
   or `wide`. A row that cannot fit at 256 px has to lay itself out from that
   class (as the text sizes do through `textSizeColumns`).

## Map

| Group | Section | Control | Writes |
|---|---|---|---|
| **Data** | Variants | variant rows, + Add | `variant_sets` |
| | Schema keys | location picker button | `location_filter` |
| | Filters | level pickers, range | `filters` |
| **Chart** | Plot type | kind grid | `kind` |
| | | Per-record value (1-D only) | `cell_statistic` |
| **Structure** | Combine | combine rows (one line collapsed, one open at a time; bucket-first editor), + Combine levels of… | `level_groups` (`mapping`, `unmatched`, `active`) |
| | Grouping | Group by… picker | `factor_variables` |
| | | grouping list: include, order, colour; name = slot dropdown when combines exist | `roles[f]='group'`, `groups`, `color`; dropdown → `combine.ts switchSlot` |
| | Factors | role per non-grouped factor; name = slot dropdown | `roles` (facet / iterate / collapse) |
| | Layout (only when a factor has the subplot role) | N rows/cols, row/col slots (one panel: locked 1 × 1, slots hidden) | `facet` |
| **Statistics** (only when summarising or a sample overlay exists) | Summary | Centre, Spread, Weight by N | `aggregate.{statistic,error,pooled}` |
| | Show sample | key ticks, Join, Colour points by, Show in legend | `show_sample`, `join_sample`, `sample_color`, `sample_in_legend` |
| **Appearance** (starts closed) | Size | Aspect | `style.{width,height}` via `commitSize` (aspect is DERIVED from the size; Custom is a disabled readout; the toolbar's Lock aspect checkbox → `figureSize.resizeFigure`) |
| | Y axis | scope ticks, Min/Max | `y_axis.{scope,minimum,maximum}` |
| | X tick labels | Rotation, Show, Hide labels the legend repeats | `style.{tick_rotation,tick_every,hide_legend_ticks}` |
| | Marks | Line weight, Sample weight | `style.{line_weight,sample_weight}` |
| | Text | Font, per-element sizes, Reset | `style.text` (`base` = Font) |
| | Labels | titles, aliases, ↑/✕ project | `style.{title,x_label,y_label}`, `aliases`, project `[aliases]` |
| **Figure toolbar** | — | Preview at (default Fit pane), W, H, unit (in/mm/px) | view state `previewMode`; W/H write `style.{width,height}` via `commitSize`; unit is per-viewer localStorage `scistack.plotStudio.sizeUnit` |
| | — | Format, Save image / Save all, Save data (CSV) + depth chooser, Export code, Add to pipeline | actions, nothing in the spec |

Right rail: Saved plots (`SavedPlotsRail.tsx`), unchanged.

## Why each control sits where it does

- **Plot type comes first.** The kind decides what Grouping, Statistics and the
  mark weights offer, so it is the first decision about the figure.
- **Roles belong to Structure; how collapsed data is summarised belongs to
  Statistics.** The collapse *role* stays with the other roles. Centre, spread
  and the sample overlay describe what happens to collapsed data, so they sit
  together.
- **Layout goes directly under Factors**, because the subplot role is what
  creates the grid.
- **Both mark weights sit in Appearance > Marks.** They are styling and were
  previously split between two sections.
- **Font moved from Size to Text.** It is the base of the text sizes, and
  "Reset" returns every size to it.
- **Preview at and the output actions moved to the toolbar.** They are not
  part of the figure, and they used to be a long scroll away.

## Mechanics

- `Group` (PlotStudio.tsx) hides its body with `display: none` instead of
  unmounting it, so an input the user is part-way through editing keeps its
  text. Open state is kept per viewer in localStorage
  (`scistack.plotStudio.openGroups`). Every read and write is wrapped in
  try/catch; if storage is missing or unreadable, `parseOpenGroups` returns
  the defaults.
- The figure toolbar sits outside `canvasRef`, so its height is never part of
  the canvas measurement that sizes the figure and the pane preview. The CSV
  data chooser floats (absolute position) for the same reason: pushing the
  canvas down would re-request a pane preview.
- Colours only through `var(--ps-*)` tokens. `plotTheme.test.ts` fails on a
  literal colour.
- Pure logic lives in `sidebarGroups.ts` and is tested in
  `sidebarGroups.test.ts` (registered in `tsconfig.test.json`).
- After any `.tsx` change, rebuild **both** vite targets
  (`npm run build`, then `VITE_BUILD_TARGET=webview npm run build`).

## Figure size and steppers (2026-09-26)

- **One owner for the written size:** `preview.figureOutputSize(mode, meta,
  specSize)`. Export size → the spec's `style.width/height`; Fit pane → the
  size the pane view was decided at (`layout.meta.preview`). The toolbar's W/H
  show it, and `outputSpec` (the spec with that size) is what Save image,
  Export code and Add to pipeline send. Saved plots store `spec` + the view,
  so a Fit-pane saved plot refits on reopen. Save CSV ignores size.
- **Editing the size in Fit pane** (W, H, or Aspect) goes through
  `commitSize`: the pane size becomes the spec's, the edit applies, and
  `previewMode` switches to `export` (logged `[Plot Studio] size_fixed_from_pane`).
- **Units** (`figureUnits.ts`) are display only; the spec is always inches,
  stored to 4 dp so a typed px/mm reads back as itself. px = saved raster at
  `SAVE_DPI`.
- **▲▼ steppers** (`StepperInput`, rule in `stepper.stepValue`): one click goes
  to the next multiple of the step; an empty box steps from its drawn value
  (`fallback`), never from 0. Steps: Font and text sizes 1 pt, weights 0.1,
  W/H 0.1 in / 1 mm / 10 px. Not `type="number"`, because the native spinner
  starts an empty box from 0. N rows/cols keep the native spinner.
- **One panel is a 1 × 1 grid**, owned by `scistackplot.reduce.plan_layout`
  (pins and slots ignored, kept in the spec). The panel reads
  `layout.meta.panels` and locks N rows/cols to 1.
