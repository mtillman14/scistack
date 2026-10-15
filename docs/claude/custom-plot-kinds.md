# Custom plot kinds: a `plot_` function drawing in Plot Studio

*Written 2026-10-14 with the feature (plan `.claude/plan-custom-plot-kinds.md`).
Read this before touching `scistackplot/custom.py`,
`scistack_gui/services/custom_plot_service.py`, or anything that dispatches on
`PlotKind.CUSTOM`.*

## What it is

Plot Studio has a small set of built-in kinds (scatter … heatmap). A **custom
kind** is the user's own `plot_` function. It is an ordinary pipeline function,
written and discovered like any processing function (Python only for now),
which draws the figure while Plot Studio's settings still apply.

- **Plot Studio opens on the `plot_` node.** Right-click it and choose
  **Open in Plot Studio**. A `plot_` node has no outputs. A variable's Plot
  Studio never offers custom kinds.
- **The node's edges bind the inputs**, read through the same owners a run
  uses: `edge_view.effective_edges` and `edge_resolver.resolve_function_edges`.
  A node with no drawn input edges that has run with `finalized=True` takes
  its wiring from history (`derive_target_for_node`). Edges are read again on
  every request, because `CustomPlot.node` names the node rather than its
  inputs.
- **Spec:** `kind="custom"` and `PlotSpec.custom = CustomPlot(function,
  as_table, inputs, node)`. `roles.custom_kind_problem` is the one pairing
  rule: CUSTOM needs a function, and a built-in kind with a function is
  refused.

## Two levels, chosen by the signature

The precedent is `share_limits`: an argument the call can supply is passed
only when the function names it.

| Signature | Level | What applies |
|---|---|---|
| has `ax` | panel | Everything. scistackplot builds the figure, facet grid, ticks, brackets, legend, y-limits and text, and the function draws one panel's marks onto `ax`. |
| no `ax` | figure | Data settings (variants, location, filters, ITERATE) and size. FACET is refused with the fix. The function returns a matplotlib or plotly figure. |

An optional `context` parameter receives a `PlotContext`:

- `x(levels)`: the categorical x position, from `xaxis.leaf_key` and
  `render.base.x_positions`, the same keys `reduce` composes.
- `color_of(level)`: from `palette_for`, so pins and project colours apply.
- `label` / `level_label`: the display aliases.
- `groups`, `x_layers`, `color`, `sample`, `style`.

Marks placed with `context.x` line up with the ticks.

## What one call receives

**Each variable input gets its own table**, as in `for_each`. It is not joined
to the other inputs.

1. Its variant rows are applied (`VariantSet.variable`; a row with none
   belongs to the layout input), then combines and filters.
2. Fully-null schema columns are dropped, which is the coarse-input broadcast
   (`coarse-level-inputs.md`).
3. Rows are selected by the figure's ITERATE values and the panel's FACET
   values, **on the keys that input carries only**. A subject-level input is
   passed whole to every trial-level call.
4. `steps.pre` (the collapse chain's inner keys, `roles.collapse_steps`) is
   averaged away, nested and unweighted. Scalars average as numbers. 1-D cells
   average sample by sample and must have equal lengths. The sample key and
   the GROUP keys stay as columns.
5. **`as_table`**: True gives the table (schema keys plus a data column named
   after the variable). False gives the bare value, which needs one row per
   call, so every key the input carries must be ITERATE (or FACET at panel
   level).

Parameters that are not variables:
- **Parameter edges** pass their value. If a sweep has several values, the
  first is used and a note says so.
- **PathInput edges** and **unwired parameters** are passed `None`, with a
  note. The GUI has no PathOutput, so a `plot_` endpoint's `filename` arrives
  this way.

## The layout input

The **deepest** input (most schema keys; ties go to signature order) is the
one the figure is laid out from. Its factors are the ones roles can name.
`custom.prepare` resolves it as the built-in kind for its shape
(`layout_kind`):

| Shape | Layout kind | Why |
|---|---|---|
| scalar | scatter | categorical ticks from the GROUP layers |
| 1-D | line | numeric x; no cell collapse, so the function gets arrays |
| 2-D | heatmap | cannot be grouped |

Mark-only settings are removed from the layout spec: Show sample, Compare,
Difference bars, x measure. Each figure carries a `CustomDraw` on
`ResolvedPlot.custom`, and `render.mpl.render` calls
`custom.draw_panel(ax, panel, resolved)` instead of `_draw_panel`. The layout
input's **computed** y range is dropped (`_attach`). Only pinned limits stay,
and a single pinned end is applied after the call, so the axis fits what the
function drew.

**Autosize** lays a figure out several times to measure it.
`render.mpl.layout_report` runs under `custom.measuring()`, so trial layouts
never call the user's function. Only the final render does.

## Preview and files

`custom.preview_payload` (exported as `custom_preview`) returns one of two
payloads:
- `{"kind": "image"}`: a PNG at 144 dpi of exactly what Save writes, shown
  1 pt = 1 px. It is not interactive and always uses the light paper.
- `{"kind": "plotly"}`: the plotly figure a figure-level function returned,
  drawn interactively. The panel's bundle is cartesian-only.

The GUI puts it in `figures[i].custom_preview`, with `figure: null`.

`figure_file.write_figure` handles both. A plotly figure goes through
`custom.write_plotly` (kaleido, `scistackplot[plotly-export]`; kaleido 1.x
also needs Chrome via `plotly_get_chrome`), sized at 96 px per inch with
`scale = dpi / 96`. A missing kaleido or Chrome fails with that fix.

## Where the GUI routes

`custom_plot_service.is_custom(payload)` (`kind == "custom"`) is checked by:
- `plot_service.resolve_figures`
- `plot_service.capabilities_for`
- `plot_service.save_figure` (through `resolve_for_save`)
- `export_code` (refuses)
- `save_plot_data` (refuses)

`custom_capabilities` is the layout input's normal report with three changes:
- the kind list replaced by the function alone, so the grid is locked;
- the built-in-only sections set to unavailable with reasons;
- a `custom` block added: level, inputs, as_table, reason.

Saved plots are keyed by the **function name** (the rail's "variable" is a
label). Presets treat `custom` as DATA, and `presets._kind_follows_function`
lets the target's function decide the kind.

## Not done (deliberately)

- Code export / Add to pipeline (the function already is the code).
- Function versioning (later, through the `Code:` axis like processing
  functions).
- MATLAB.
- Save data.
- Variant pins for secondary inputs are not persisted in the intent store,
  because the panel's `variant_sets` save is keyed by variable.
- Two figure writers still exist: `figure_file.write_figure` and scidb's
  `_make_plot_wrapper` `savefig`. A pipeline run of a figure-level `plot_`
  that returns a plotly figure is not handled by scidb's wrapper yet.
- `for_each` parity is tested for the no-collapse case only
  (`scistackplotdb/tests/test_custom_parity.py`: subject iterated, a coarse
  input broadcast, `as_table=True`). Collapse has no `for_each` counterpart:
  a pipeline function averages its own data.
