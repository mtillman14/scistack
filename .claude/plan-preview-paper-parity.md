# Plan: light mode shows the exported figure exactly ("paper" preview)

## Problem
Light mode (2026-09-24) themed only the Plot Studio chrome. The preview figure
is still NOT the export:

| | Export (matplotlib, `render/mpl.py`) | Preview (plotly.js, `render/plotly_.py` + GUI) |
|---|---|---|
| Background | white figure + white axes | transparent, over the studio's `#f7f7fb` (GUI override) |
| Text | black | `#444` (GUI override) |
| Axes frame | black box on all four sides | no axis lines |
| Ticks | outward, black | none (labels only) |
| Grid | none | light grey grid (plotly.js default) |
| Zero line | none | dark zero line (plotly.js default) |

mpl draws matplotlib defaults, apart from text sizes set via `textsize.rc_params`.
The plotly renderer never sets axis styling, so plotly.js's own defaults show through.

## Fix (the renderer layer owns it: NOTE 3, NOTE 4)
1. **One owner of the "paper" look** in scistackplot, e.g. `render/base.py`
   `PAPER` (background, text colour, axis-line colour/width, tick direction/length,
   grid off). `mpl.py` sets these explicitly through `rc_params`/rc_context instead
   of relying on rc defaults. That also protects the export from a user's
   `matplotlibrc`. `plotly_.py` writes the same values into every x/y axis
   (`showline`, `mirror`, `linecolor`, `ticks="outside"`, `ticklen`,
   `showgrid=False`, `zeroline=False`) and into `paper_bgcolor`/`plot_bgcolor`/`font.color`.
2. **GUI:** light mode stops overriding the figure: no transparent backgrounds,
   no font colour, so it draws exactly what the renderer sent. Dark mode keeps
   today's recolouring as a "screen" view, labelled as not what gets exported.
   Maybe rename the toggle to "Paper preview".
3. **Tests:** a parity test that renders one spec both ways and checks that the
   plotly layout's colours, lines, ticks and grid equal the mpl figure's
   (facecolor, spine colour and visibility, tick direction, grid on/off, text
   colour), plus a guard that every axis in the plotly layout carries the paper
   style (facets included).
4. **Logging:** the preview log line states `style=paper`.
5. Afterwards, compare one figure side by side (preview vs saved PNG) for leftovers:
   legend frame (mpl draws a framed legend), bar edges, error-bar caps.
   Fix each in the same owner.
