# Plot panel spacing: margins, cell gaps and bracket rows

How the export (matplotlib) and the preview (plotly) decide where text around
a panel goes, and why they agree. Written 2026-09-27.

## The rule: text has a size in points, so its room must too

Tick labels, axis titles and bracket rows have a fixed size in points,
whatever the figure's size. A space sized as a fraction of the figure is too
small on a narrow or short figure. That was the cause of two bugs:

- brackets drawn on top of 45°/90° tick labels (a rotated label is as deep as
  it is long, not as tall as its font);
- the right panel's y title running into the left panel (6% of a 4 in figure
  is about 17 px, and a y title plus tick numbers needs about 50).

## One owner: the export measures, the preview draws

Only matplotlib can measure text. The plotly preview never estimates text when
it has decisions. It reads what `mpl.layout_decisions()` measured at the same
size, with 1 pt = 1 px (`plotly_.PX_PER_IN = 72`).

| Concept | Owner | Measured in | Read by |
|---|---|---|---|
| Bracket rows below the ticks | `render/base.py::BracketGeometry` (tick_depth_pt, row_height_pt → rule_pt / label_pt / title_pad_pt / bottom_pt) | `mpl._draw_x_groups` (`_tick_label_depth_pt` per axes; the deepest one is kept) | mpl placement + `labelpad`; plotly `_add_x_groups` (`yshift`, shapes `ysizemode: "pixel"`, title `standoff`) |
| Text outside each axes | `render/base.py::GridReach` (left/below × outer/inner) | `mpl._grid_reach`: axes box vs `get_tightbbox`, on the FINAL layout | plotly `_frame` |
| Margins + cell gaps | `plotly_._frame` → `_Frame(margin, gaps)` | — | `layout.margin`, `_cell(..., gaps)` for every axis domain and bracket anchor |

`layout_decisions()` returns `bracket_geometry` and `grid_reach` alongside
`ticks`, `brackets` and `legend`. The figure attributes are `LABEL_FIT_ATTR`
and `GRID_REACH_ATTR`.

In `GridReach`, "outer" means text in the figure margin (column 0's left side,
the bottom row's underside). "Inner" means text drawn into a gap between two
cells. The margins are `max(default, outer + REACH_PAD_PX)`. The gaps are
`(inner + REACH_PAD_PX) / plot px`, floored at `MIN_CELL_GAP_PX`, and capped at
half the plot across all gaps (it WARNs when capped).

The legend's room also lives in `_frame`, not in `_apply_decisions`. That way
the plot-area size used to turn pixels into fractions already accounts for it.

## Undecided fallback

`render_plotly(resolved)` with no decisions (library callers, or when the
decisions fail) keeps the old fractions: `_gaps` (`X_GAP`, `Y_GAP +
X_GROUP_ROW * depth`). The brackets use an upright estimate
(`_bracket_geometry`, `UPRIGHT_LINE_HEIGHT`). The GUI always passes decisions.

## Diagnostics

- DEBUG `panel text reach: …` (export) and `preview frame from the export's
  text reach … cell gaps N x M px` (preview). The line `preview frame
  (undecided)` means the fractions were used.
- WARN `panel (r,c): its y labels run Npt into the panel on its left` means
  the export itself overlaps (tight_layout could not fit it).
- WARN `preview: the column gap needs Npx but only Mpx fits` means the figure
  is too small for its labels.

## Known gaps

- Plotly's automatic y tick values are not exactly matplotlib's, so their
  widths differ slightly. `REACH_PAD_PX` absorbs that.
- For more than one labelled column, the export draws one shared `supxlabel`
  while the preview titles each bottom axis. The x axes keep plotly
  `automargin`, which reserves that title.

Tests: `scistackplot/tests/test_grid_gaps.py`,
`test_preview_decisions.py::test_rotated_ticks_push_the_preview_brackets_down`,
`test_mpl_label_fit.py::test_rotated_ticks_push_the_brackets_down`.
