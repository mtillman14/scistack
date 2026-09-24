# Preview/export paper parity (and the Plot Studio's light mode)

Added 2026-09-24. Plan: `.claude/plan-preview-paper-parity.md`.

## The concept

A figure's **paper** is everything that is not data: the background, the text
colour, the frame round each panel, the ticks, the grid, and the error-bar ink.
The export (matplotlib, `render/mpl.py`) and the preview (plotly.js,
`render/plotly_.py`) are two renderers. Marks, sizes, labels and legends have
long been matched between them (see `plot-text-and-labels.md`,
`figure-size.md`). The paper never was:

| | matplotlib default (the export) | plotly.js default (the old preview) |
|---|---|---|
| background | white | white, but the GUI made it transparent |
| frame | black box, four sides, 0.8 pt | none |
| ticks | outward, 3.5 pt, bottom/left | none |
| grid | none | light grey |
| zero line | none | dark |
| error bars | black, 1.5 pt, caps 3 pt | `#444`, 2 px, caps 4 px |

## One owner: `scistackplot/paper.py`

`PAPER` (a frozen `PaperStyle`) holds the values, which ARE matplotlib's
defaults. `test_paper_is_matplotlibs_own_default_look` pins that, so
introducing the owner changed no exported file. Its consumers:

- **`render_matplotlib`** opens `figure_rc_params(sizes)` (paper + text sizes),
  then calls `apply_paper_axes(ax)` on every panel. The explicit pass is
  needed because matplotlib creates **tick objects lazily**. A save outside
  the `rc_context` (`write_figure`, `plot_service`) builds them from whatever
  rc is in force at that moment, including a user's `matplotlibrc`.
  `tick_params` persists on the axis, so later ticks follow it.
- **`write_figure`** passes `facecolor=PAPER.background`, because `savefig`
  also reads its facecolor at save time.
- **codegen** bar plots pass `err_kws=seaborn_err_kws()`. seaborn 0.13
  (now the floor) draws each error bar as one two-point line with no caps.
  `_` end markers, sized `2 x error_cap`, are exactly `Axes.errorbar`'s caps.
  seaborn's own `capsize` is a fraction of the category spacing, so it was
  not used.
- **codegen** emits the same rc dict and `paper_axes_code("g.axes.flat")`. The
  latter also undoes seaborn's default despine, which exported an open frame.
  A heatmap uses `[ax]`: a colourbar keeps its own outline, as in
  `render_matplotlib`.
- **plotly preview**: `plotly_layout_style()` (backgrounds), `layout.font.color`,
  `plotly_axis_style()` on every x/y axis (`showline`, `mirror: True` = the
  frame's far sides without ticks, `ticks: outside`, `showgrid`/`zeroline`
  off), and `plotly_error_style()` on `error_y`. Lengths are pt read as px
  (the preview is drawn at 1 pt = 1 px).

Logging: the export's `figure size …` info line ends with `describe_paper()`,
and the preview logs the same phrase at debug level.

## The GUI side

The Plot Studio's light/dark toggle (extension `globalState`, shared by every
plot tab; `frontend/.../PlotStudio/plotTheme.ts`, `usePlotTheme.ts`,
`extension/src/plotTheme.ts`) decides how the figure is shown, via
`plotTheme.screenFigure`:

- **Light** returns the renderer's figure **untouched**. That is the saved
  file. Never add a GUI override here; fix the renderer instead.
- **Dark** is a screen view: a transparent background, with text, frame,
  ticks and error bars recoloured to `PLOT_THEMES.dark.plotText`. Data
  colours are never touched.

The studio's chrome colours are separate: `var(--ps-*)` tokens, all owned by
`plotTheme.ts` and source-guarded by `plotTheme.test.ts`.

## Known remaining differences

- plotly has one `thickness` for an error bar and its caps, so the preview's
  caps are 1.5 px where matplotlib's are 1.0 pt (`PAPER.error_cap_width`).
  This half-pixel is the only error-bar difference left.
- matplotlib draws **minor ticks** on a log axis and plotly does not.
- The bracket rules (`#888888`) and the sample edge (`#333333`) are still
  literals in both renderers. They agree with each other, but they are not
  yet in PAPER.
