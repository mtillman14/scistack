# Plan: custom plot kinds in Plot Studio

Status: ALL STAGES WRITTEN 2026-10-14 (uncommitted; all pytest suites pass; frontend tsc + both vite builds + extension build pass). Decisions: D1 (a) + parity test, D2 both levels by `ax`, D3 refuse non-numeric/2-D collapse; kind grid locked to the function on a `plot_` node. Doc: docs/claude/custom-plot-kinds.md.

## Goal

A user's own Python `plot_` function appears in Plot Studio's kind grid next to
the built-in kinds (scatter … heatmap). It reacts to the same settings — variant
rows, location filter, filters, roles (iterate / facet / group / collapse),
size, text, colours, aliases — and saves and loads like any other plot.

Out of scope for now: code export / Add to pipeline (user: "forget about code
export for a second"), function versioning (later: the `Code:<fn>` axis, like
processing functions), and MATLAB.

## What a custom kind is

**It is an ordinary `plot_` endpoint function**, written and discovered exactly
like a processing function. `scidb.roles.function_role` already classifies it as
`"plot"`, so no new decorator or registry is needed:

```python
def plot_speed_vs_mass(StepLength, Mass, filename, ax=None, context=None):
    ...
```

**Plot Studio is opened from the `plot_` node on the canvas** (user,
2026-10-14), not from a variable. A `plot_` node has no outputs: it is a leaf.

- **Inputs:** the node's own incoming edges bind every input. The edges are the
  ground truth (edge-based inputs, `edge_view`) and there is no name matching.
  Each bound variable gets one row in the Data section, with its own variant
  pin. An unwired input makes the studio open with a refusal naming the port,
  not an empty figure.
- **Session identity:** the node's placement-qualified id, looked up after
  `resolve_scope_view` (see the placement-id lookup trap). Saved plots for this
  figure key on the `plot_` function, not on a variable.
- **The variable-launched Plot Studio is unchanged:** it still offers only the
  built-in kinds.
- **Verify in Stage 0:** whether the canvas currently draws output handles or
  `output_types` for `plot_` nodes. If it does, removing them belongs in the
  canvas's one owner of node ports, keyed on `function_role == "plot"`.
- **Open question for later (code export, deferred):** `for_each(...,
  finalized=True)` currently saves the figure's path as an output record
  (`outputs=[PlotFigure]`). "No outputs" on the canvas has to agree with that
  record when export comes back.

The `PathOutput` parameter is not passed in Plot Studio. Plot Studio writes the
file itself through Save image.

## Two levels of integration, chosen by the function's signature

The precedent is `share_limits`: the `{input}_limits` kwarg is passed only when
the function's signature accepts it.

| Signature | Level | Who draws what |
|---|---|---|
| accepts `ax` | **panel-level** | scistackplot builds the figure, the facet grid, ticks, brackets, legend, y-limits and text. The function draws only the marks for one panel onto `ax`. Every Plot Studio setting applies. |
| no `ax` | **figure-level** | The function returns a whole Figure (matplotlib or plotly). Only the data settings (variants, location, filters, ITERATE) and the size apply. Facet, group and Appearance controls are greyed out, each with its reason. |

An optional `context` kwarg (`scistackplot.PlotContext`) hands over what
scistackplot decided, so the function never re-derives it. It holds:

- the figure and panel keys
- the GROUP order (innermost first) and the colour factor
- `color_of(level)`, which reads `palette_for` and so includes pins and project colours
- `x(level…)`, the categorical x position, owned by `x_positions`. Placing marks with it is what makes ticks, brackets, Show sample and difference bars line up.
- `text`, the `DisplayText` aliases
- the resolved style: mark weights and text sizes

## What the function receives (user's Q1)

The data is set by the inputs' schema levels plus the roles:

| Role of a schema key | Effect on the call |
|---|---|
| ITERATE | one figure per level, which is one call (figure-level) or one set of calls (panel-level) |
| FACET | panel-level only: one call per panel |
| COLLAPSE | averaged away before the call, using the same nested, unweighted chain as `reduce`. The function receives the sample. |
| GROUP | kept as a column. `context` gives the order and colours. The function is expected to draw one mark per level, but scistackplot cannot enforce that. |

`as_table` (a Plot Studio field, defaulted from the function node's recorded run
option):

- **True or a list of params:** each such input arrives as a DataFrame with its
  schema-key columns. It is per input, as in `for_each`. Inputs are not joined.
- **False:** the input arrives as the bare value. This requires one value per
  call, so every key at or above that input's level must be ITERATE, FACET or
  COLLAPSE. GROUP is refused with a reason, because a group needs a column.

Inputs at different schema levels follow `for_each`'s coarse-input broadcast:
the shallower input is passed whole to each call. The Structure section offers
the union of the inputs' keys.

## Preview and export (user's Q2)

- **matplotlib result:** the preview is a PNG rendered at the pane's pixel size
  on the `paper.PAPER` rc and shown as an `<img>`. It matches the export
  exactly. Hover and click-picking are not available, so `capabilities` greys
  out difference bars with a reason.
- **plotly result:** the Figure's JSON is sent straight to the existing plotly
  preview, so it stays interactive.
- **Exporting a plotly figure to SVG/PNG/PDF: possible, through kaleido
  (`fig.write_image`).** matplotlib cannot read a plotly figure. kaleido is not
  installed today. It goes in an optional extra (`scistackplot[plotly-export]`).
  kaleido ≥1 also needs a Chrome binary, which `plotly_get_chrome` /
  `kaleido.get_chrome_sync()` installs. A missing kaleido or Chrome must fail
  the save with that fix in the message.
- One-owner issue found: figures are written in two places,
  `scistackplot.figure_file.write_figure` and `scidb.foreach._make_plot_wrapper`
  (`savefig`). The plotly branch must not be added to both. Proposal: one
  stdlib-free `write_any_figure(fig, path)` owner in scidb, which
  scistackplot's writer calls for the final write.

## Decisions for the user

- **D1. Where the data comes from.**
  - (a) Recommended: Plot Studio's own path (ScidbSource → LongTable → reduce),
    so variant rows, location, filters and collapse keep one owner each. A
    parity test (like `test_fanout_parity.py`) asserts the function gets the
    same arguments `for_each(..., as_table=…)` would give it.
  - (b) Call `scidb.for_each` directly in draft mode. This gives one owner for
    loading, but collapse, facets and `ax` don't exist there.
- **D2. Both integration levels, chosen by `ax`?** Or only panel-level, with
  plotly functions refused until there is a plotly-panel contract
  (`fig, row, col`)?
- **D3. Collapse for array or non-numeric inputs.** Use the `cell_statistic`
  rule (reduce first) or refuse? Proposal: numeric scalars and 1-D are averaged
  as today; anything else refuses COLLAPSE with a reason.

## Persistence (Q5b)

- `PlotKind.CUSTOM` plus `PlotSpec.custom: CustomPlot | None`, holding
  `function` (name), `as_table` and `inputs` (param → variable, as last bound,
  for display and drift notes only; the edges stay the authority).
- The function's identity is its name, as with `Code:<fn>`.
- Saved plots and presets serialize it through the usual `to_dict` /
  `from_dict`.
- `restore_spec` behaviour:
  - A function that no longer exists opens with the spec intact and a refusal
    naming it. The plot is never silently swapped to scatter.
  - A rebinding is reported in `saved.notes`.
- Presets: `custom` is a FIELD_CLASSES "data" field, so it is not carried by a
  preset. To be confirmed when Stage 1 starts.

## Stages

1. **Spec:** `PlotKind.CUSTOM`, `CustomPlot`, round-trip, `restore_spec`
   leniency, presets classification. Tests: round trip, missing-function notes.
2. **Owner `scistackplot/custom.py`:**
   - signature inspection (`ax`, `context`, `{input}_limits`, PathOutput skipped)
   - `custom_requirement(...)` beside `roles.kind_requirement`, which is the one
     statement of what the custom kind needs and is read by both `validate` and
     `capabilities`
   - per-input shaping (collapse, then slice per figure/panel, then a table or
     a value)
   - `PlotContext`

   Tests: shaping per role, as_table on/off, broadcast, refusals.
3. **Render:**
   - `render/mpl.py` `_draw_panel` dispatches CUSTOM to the user function
   - figure-level adoption: resize to `style.width × height`, paper rc
   - plotly passthrough
   - `write_figure` + kaleido path

   Tests: panel-level grid, figure-level size, a plotly figure written as SVG
   (importorskip kaleido).
4. **GUI backend:**
   - a new `describe_function(node_id)` entry point: the node's edge bindings
     come from the scidb edge owner (no second copy of the edge rules), plus
     each input's table, shape and schema level
   - `capabilities` reports the custom kind's requirements and reasons
   - the user function's exceptions are caught and returned as the figure's
     error, with the traceback logged in scidb.log
   - matplotlib runs on Agg

   Logging (NOTE 2): `[plot] custom fn=<name> level=panel|figure calls=N
   rows_in=… duration=…`, one INFO line per resolve, and a DEBUG line per call.
5. **Frontend:**
   - an "Open in Plot Studio" item in the `plot_` node's context menu
   - the kind grid hidden (or locked to the function) when opened from a
     `plot_` node
   - one Data row per bound input
   - an as_table toggle in Chart
   - `<img>` preview mode
   - rebuild both vite targets
   - update docs/gui-manual-testing-todo.md
6. **Docs:** `docs/claude/custom-plot-kinds.md` and the scistackplot README
   section.

## Diagnostics if something misbehaves

Each call logs the argument shapes and types it passed (DataFrame columns and
row count, or the value's type and shape), and the time spent in the user's
code versus in scistackplot. That keeps "my function got the wrong data"
separate from "my function is slow".
