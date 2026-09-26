# Figure size and aspect ratio

Written 2026-09-16, when the Plot Studio "Figure size" section was added.

## The one fact that explains the design

`StyleOptions.width` / `height` are **inches** and they size the **saved**
figure — `render_matplotlib` (`figsize=`), the generated seaborn code
(`set_size_inches`), and therefore every `plot_` pipeline step. The interactive
plotly preview does **not** use them: it fills whatever pane it is in
(`PlotStudio.tsx` measures the canvas with a `ResizeObserver`, and `figureHeight`
is a floor of `max(320, rows * 240, available)`).

That split is deliberate and was confirmed by the user: the setting exists to
control the file, "not so much the size of the figure in the GUI". A preview
locked to 16:9 in a tall sidebar-plus-canvas layout would waste most of the
pane; a preview that fills the pane is more useful for exploring, and the
exported size is stated beside it rather than shown.

Consequences worth remembering:

- A change to `style` never re-reduces: `style` is in
  `reduce._PLAN_IRRELEVANT_FIELDS`, so editing the size costs one plotly
  re-render, not a reduce over millions of rows.
- The plotly payload carries `layout.meta.figure_size = {width, height, aspect}`
  next to `rows`/`cols`. That is how the export size is *stated* to the GUI
  without the preview honouring it.
- `render_matplotlib` logs `figure size W x H in (aspect), R x C panel grid`
  at INFO. A figure that "came out squashed" is diagnosed from `scidb.log`,
  not from the panel.

## One vocabulary: `scistackplot.figsize`

Ratios, not sizes. In practice the width is dictated by the destination — a
journal single column is 3.5 in (89 mm), 1.5 column 5.5 in, double column
7.2 in (183 mm), a slide is whatever the deck is — and the ratio is the free
choice. So the contract is *pick a ratio, name a width, the height follows*:

| Function | Direction |
|---|---|
| `height_for(width, name, height=…)` | preset → stored size (rounds to 2 dp) |
| `aspect_name(width, height)` | stored size → preset, else `"custom"` |
| `describe_size(width, height)` | what the plotly renderer puts in `meta` |
| `presets_payload()` | the dropdown, in order, as JSON |

`RATIO_TOLERANCE = 0.01` is what makes the round trip work: a height rounded
to two decimals is off the exact ratio by ~0.001 at 8 in wide, and the nearest
pair of presets (4:3 = 1.333, 3:2 = 1.5) are 0.17 apart.
`test_presets_do_not_collide_within_tolerance` guards that gap, so adding a
preset that sits within 0.02 of an existing one fails a test rather than
making the dropdown ambiguous.

The presets: `4:3` (the 8 x 6 default — so a fresh spec never opens as
"custom"), `16:9`, `3:2`, `golden` (1.618:1), `2:1`, `1:1`, `3:4`, `9:16`,
`custom` (ratio `None`, always last).

## Where the list lives, and what is duplicated anyway

The list is owned by Python. `plot_service._describe` ships it as
`figure_presets` beside `image_formats`, for the same reason: a dropdown built
from a list in the frontend is a second copy that drifts. The GUI has a
fallback of four entries only for a describe that predates the field.

What **is** duplicated is the arithmetic. `figureSize.ts` re-implements
`heightFor` and `aspectName` because they must run on every keystroke — a
round trip through a resolve is 180 ms of debounce plus a render, and a
dropdown that snapped back after the fact would be worse than none. The
parity rule is enforced the same way as `xLayers.ts` ↔ `ordered_x_layers`:
`figureSize.test.ts` and `test_figsize.py` pin the **same cases** (8 x 6 is
4:3; 7.2 in at 16:9 is 4.05; 8 x 5 is custom; every preset round-trips at
3.5 / 7.2 / 8 / 13.333 in). If one side changes the rounding or the tolerance,
the other side's test is the one that tells you.

## The dropdown reads the size; the Lock checkbox decides how W and H move

Changed 2026-09-26 (user). Before, the dropdown was view state (`aspectChoice`)
and "Custom" was a MODE: W and H independent. Now:

- The Aspect dropdown is derived: `aspectName(width, height)`. "Custom" is a
  disabled option: it shows when the size matches no preset and cannot be
  picked. There is no `aspectChoice` any more (not in the saved view either).
- **Picking a ratio** always keeps the width and sets the height
  (`heightFor`), whether locked or not, including from a custom size.
- **Lock aspect** is a checkbox in the figure toolbar after W/H. Locked: typing or
  stepping W moves H, and typing H moves W, at the CURRENT ratio (a preset's
  exact ratio when the size is one, so steps cannot drift off it through
  two-decimal rounding; otherwise the current w / h). Unlocked: only the
  edited side moves, and the dropdown then reads whatever results.
- One owner of that rule: `figureSize.resizeFigure`. The lock is per viewer
  (localStorage `scistack.plotStudio.aspectLocked`, default locked), like the
  size unit: it is a way of editing, not a property of the figure.
- Logs: `[Plot Studio] figure_resized` (why, locked, from, to) and
  `[Plot Studio] aspect_picked` in the webview console.

`InchInput` follows `LimitInput`'s discipline (text held while typing, commit
only a finite positive number) with one difference: blank means nothing.
`Number('') === 0`, and a 0-inch figure is a matplotlib error, not a size.

## The file is exactly the size (since 2026-09-23)

The GUI used to save with `bbox_inches="tight"`, which cropped or grew the
PNG/SVG around the drawn content, so a 7.2 x 4.05 in figure was only *roughly*
1440 x 810 px, and long labels made it grow (spec/images/graph1.png). Decision
2026-09-23 (reversing 2026-09-16): no trim. `scistackplot.write_figure` is the
one owner of render-and-write; it refuses `bbox_inches`, and the renderer fits
the x labels and the legend INSIDE the canvas (`.claude/plan-tick-label-legibility.md`,
stages 2 and 2b). Content that still reaches past the edge is measured
(`canvas_overflow`) and WARNed, never fixed by resizing the file. The readout
under the inputs now says "exactly".

The readout's dpi is `SAVE_DPI = 200` in `PlotStudio.tsx`, mirroring the
`SaveRequest.dpi` default in `api/plot.py`. The GUI does not send a dpi; if a
dpi control is added, the readout must read from it.

## Files

- `scistackplot/src/scistackplot/figsize.py`, `tests/test_figsize.py`
- `scistackplot/src/scistackplot/render/mpl.py` (INFO log),
  `render/plotly_.py` (`meta.figure_size`)
- `scistack-gui/scistack_gui/services/plot_service.py` (`figure_presets`),
  `tests/test_plot_service.py`
- `scistack-gui/frontend/src/components/PlotStudio/figureSize.ts`,
  `figureSize.test.ts` (registered in `tsconfig.test.json`), `PlotStudio.tsx`
  ("Figure size" section, `InchInput`, `setStyle`)
- Plan: `.claude/figure-size-setting.md`

## Font size (added the same day; per element since 2026-09-24)

*Superseded in detail by `plot-text-and-labels.md`, which covers the
per-element sizes (`StyleOptions.text`). The notes below are what still holds.*

`TextSizes.base` (points, default **14**; matplotlib's 10 was "far too small"
at 8 x 6 in) is matplotlib's `font.size`. Every other element is either fixed
or derived from it with matplotlib's own ratios, by the one owner
`scistackplot.textsize`. The single `font_size` / `tick_font_size` fields were
replaced as a clean break.

How it is applied:

- **`rc_context`, never `rcParams`.** `render_matplotlib` runs inside the GUI
  server and inside `for_each`; a global rc would resize the next figure
  anyone draws. The generated function is wrapped the same way, with
  `with plt.rc_context({...textsize.rc_params...}):`. That is why the body of a
  generated `plot_` function is indented one level deeper than the imports.
- **Text reads the size at creation.** The context must be open while every
  label, tick and legend entry is created, and it is: the whole timer body is
  under it. Ticks that matplotlib creates *later* (at `savefig`, outside the
  context) copy their properties from tick 0, which `tight_layout` forced into
  existence inside the context. `test_fontsize.py` saves to a `BytesIO` first
  and then inspects the sizes, precisely to cover that path.
- **No renderer derives its own size.** Every size comes from
  `resolve_sizes`. The plotly bracket labels were `0.8 x` while matplotlib's
  were `small` (0.833 x): two owners of one number, now one.

The plotly preview gets `layout.font.size = text.base` and the per-element
fonts from the renderer; the GUI adds only the colour
(`{...layout.font, color: '#ccc'}`). Points and px are not the same unit, but
one number in both places means the text box does something visible before a
save. The sidebar thumbnail (`VariablePlot.tsx`) keeps its own fixed 10 px,
because it is not a preview of the export.

The GUI box is "Font (pt)" in the Figure size section (writes `text.base`), a
`PositiveNumberInput` like width and height (0 pt is a matplotlib error, not
a size).
