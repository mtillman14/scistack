# `where=` in the GUI (issue #13)

*Written 2026-11-16. Plan and decisions: `.claude/plan-where-in-gui.md`.*

## What a node's filters are

A function node's **Filters** section (sidebar) holds rows `variable / [column] / op / value`, stored in the
node's config as `whereFilters`. **Several rows mean AND**: one run every filter must pass
(`where=[a, b]`). Separate runs, one per filter, are `where=EachOf(a, b)`, which has no GUI yet.

**One route.** Every way to run a node reads the STORED list through `scistack_gui/services/where_service.py`
(per-node run, pipeline run, generated MATLAB command, both code exports). Nothing reads the live canvas
data and the request carries no filters, so the sources cannot disagree
(`gui-run-options-flow.md`, "these two can disagree"). Log line: `[where] node=… route=… filters=N -> where=…`.

| Concept | Owner |
|---|---|
| Filter as data, list = AND, Python text | `scidb.model.filters`: `filter_from_dict`, `all_of`, `where_source` |
| Text a person reads (`Age > 80`) | `scidb.model.filters.filter_label` (a comparison's own `to_key`) |
| Pass / fail / no value per location | `scidb.model.filters.filter_report` |
| The node's stored filters, MATLAB spelling | `where_service` |
| Names and levels of the check factors | `scistackplot.model.where_check` |
| The Yes/No columns on the rows | `ScidbSource._attach_where_check` |
| How a check figure opens | `scistack_gui/services/where_check_service.py` |

## The node shows its filters

`graph_builder.where_chips(rows)` puts `whereChips` (`index, text, variable, column`) on the node's data;
`FunctionNode` draws one chip each (`components/DAG/WhereChips.tsx`) and never words a filter.

* Click a chip: histogram of the filter's variable, coloured by Matching.
* Right-click: Histogram; "Plot *input* against *variable*" (one per variable input); Edit (select the node);
  Remove (`set_where_filters`).
* Opening is a window event (`OPEN_WHERE_CHECK_EVENT`) caught by `PipelineDAG`, then `open_plot_panel` with
  `whereCheck` (entry point `where-chip`). The extension carries `whereCheck` opaquely to the webview,
  which sends it with the opening `plot_describe(where_check=…)`.

## The check figure

A check is **an ordinary Plot Studio figure plus Yes/No factors**: no second screen, no renderer code.
`PlotSpec.where_check = {node_id, function_name, filters:[{variable, column, op, value, enabled}]}`.

* The source asks `filter_report` for the plotted variable (`y_measure`) and joins the verdicts on schema
  keys (as text). It adds `Matching` (Yes when every ENABLED filter passes, No when one fails, "No value"
  when none failed but one could not be answered) and, for two or more filters, `Filter 1`, `Filter 2`…
  (by position, not text, so editing a comparison never renames a factor that roles and colours name).
  A location with no value for a filter is labelled, never dropped.
* The factors exist at the source, so `reduce`, `validate` and the renderers see ordinary categorical
  columns (`synthetic-factors.md` invariant 1). The cost is invariant 4: **codegen does not mirror them**, so
  Export code and Add to pipeline are unavailable in a check (`capabilities.code_export`, and
  `generate_plot_function` refuses).
* Opening spec (`opening_check_spec`): histogram of the filter's variable (a text variable opens as its
  ordinary Count bar); "against", numeric filter: input on y, `x_measure` = the filter variable, Scatter;
  "against", categorical filter: the filter variable joined as the grouping (x ticks), Box, Show sample on
  the keys. Every schema key collapsed, `Matching` holds the colour (green / red / grey,
  `where_check.CHECK_COLORS`).
* Numeric or categorical is read from the comparison value with `shape.classify_value`.
* **Limit.** The verdict is computed at the *plotted* variable's locations. A filter coarser than the
  plotted variable applies to every location under it; a filter FINER than it is not applicable (passes
  everywhere), exactly as `resolve` treats it.

## Editing and going back

`WhereCheckSection.tsx` (top of the Data group, only in a check): a tick per filter (counts toward
Matching), the comparison and value editable in place (the figure redraws; the value commits on blur),
**Apply to node** (writes the TICKED rows with `set_where_filters`, then closes the tab) and **Discard**.
An unticked filter was only being explored and is not written back.

From an ordinary figure, Data gains "Check a where= filter…" (`WhereCheckEntry`): the root-canvas function
nodes that take the plotted variable as input and state a filter (`where_service.check_targets`,
`where_check_nodes`), opening the variable against the node's first filter. VS Code only.

## Not built (said no / later)

* "x axis" radio in the section (the opening click already chooses the axis).
* Greying a "Plot against" input that cannot be joined, with the backend's reason: the table build's error
  shows instead.
* The filter variable's value in the hover; every-point transport (`max_points`) off in a check.
* Elevated filters, filter sweeps (`EachOf`), `isin` in the dict form, OR/NOT in the GUI.
* Nodes inside nested pipelines are not listed by "Check a where= filter…".

## Tests

`scidb/tests/test_where_list.py`, `test_filter_report.py`; `scistackplotdb/tests/test_where_check.py` (Yes
rows = the rows `load(where=…)` returns); `scistack-gui/tests/test_where_every_route.py`,
`test_where_chips.py`, `test_where_check_open.py`; `frontend/.../whereCheck.test.ts`.
GUI manual: chips on a node, click / right-click, edit a value in the check and watch the figure, Apply,
re-run the node.
