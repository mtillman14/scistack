# Plan: `where=` in the GUI (issue #13)

Status: approved 2026-11-16. ALL stages BUILT (uncommitted; pytest, MATLAB and GUI
unchecked): filters reach every run, node chips, the check figure, editing in
the check, docs. See "As built" at the end.

## Decisions (user, 2026-11-16)

1. `where=[a, b]` is real syntax and means AND: one run that every filter
   must pass. `where=EachOf(a, b)` is separate runs, one per filter.
2. Elevated filters and filter sweeps are deferred to their own plans.
3. Entering a check from an ordinary Plot Studio figure: my call. Chosen: a
   "Check a where= filter…" entry listing the function nodes that take the
   plotted variable as input, so Apply always has a node to write to.
4. Tick boxes in the check explore the effect of each filter (they change
   what counts toward Matching). Saving a filter as switched off on the node
   is a possible later addition, not in this build.
5. A location where the filter variable has no value is drawn as a third
   state, "no value".
6. Docs stay in the Docs stage.

## What exists today (read before building)

- The function sidebar has a **Filters** section (`FunctionSettingsPanel.tsx`,
  `WhereFilterRow`): rows of `variable / op / value`, saved as node config
  `whereFilters`.
- Only the **single-node Python run** reads it (`run_service._build_where`),
  from the live canvas data. Pipeline runs, MATLAB generation and both code
  exporters ignore it (`MATLAB_OPTION_EMISSION["where"] = "the GUI has no
  where= filter"`). So a filter the panel shows is silently absent from three
  of four ways to run. This is the "two sources can disagree" failure in
  `docs/claude/gui-run-options-flow.md`.
- Several rows currently mean `EachOf(f1, f2)` (one variant per filter), not
  "all of these". Only whole-variable filters (`VariableFilter`); no column
  of a table, no `isin`.
- scidb owns the truth of a filter: `Filter.resolve(db, target, table,
  validate_coverage=)` returns the target's passing schema ids. `__where` is
  display only since the where= redesign; variants are told apart by consumed
  locations. (`docs/claude/where-filter-system.md` and
  `where-provenance-and-merge.md` still describe Strategy 1 string matching
  and are stale there.)
- Plot Studio already has every drawing piece the issue asks for:
  - **X variable** (`PlotSpec.x_measure`, wide join, `joinable_with`) = Age on
    x, Distance on y, scatter.
  - **Group by a variable** (`PlotSpec.factor_variables`) = Sex as x ticks,
    with bar / box / violin and every Appearance control.
  - **Histogram** with all keys collapsed = pooled histogram; a text variable
    opens as a Count bar.
  - `shape.classify_value` = the one rule for numeric vs. categorical.
  - **Show sample**, `colors`, sample hover (`render.base.sample_hover`).

What is missing: the pass/fail mask as something a figure can colour by, the
node showing its filters, the way in and back out, and the run paths.

## The one idea

A where= check is **an ordinary Plot Studio figure plus one new kind of
factor**. Each filter becomes a Yes/No factor on the plotted rows, and
"Matching" is the Yes/No of all ticked filters together. Factors already take
roles (colour, panels, separate figures), already reach StatStudio through
the shared design, and already have colours and legends. No new renderer
code, no new plot kind, no second plotting screen.

## Owners (NOTE 4)

| Concept | Owner | Consumers |
|---|---|---|
| Filter as data (`{variable, column?, op, value}`), list = AND | `scidb.model.filters`: `filter_from_dict`, `filter_to_dict`, `all_of(filters)` | node config, GUI run (all four paths), exports, the plot source |
| Which target locations pass which filter, and why not | `scidb.model.filters.filter_report(db, target, filters)` built on `Filter.resolve(validate_coverage=False)` | scistackplotdb source |
| Filter text shown to a person (`Age > 80`) | `Filter.to_key()` (exists) | node chips, legend titles, hover, run report |
| Python / MATLAB source text of a filter | one emitter each, next to `filter_from_dict` (Python) and in `matlab_command_format` (MATLAB), both from the dict | code export, MATLAB command |
| The where factors on a figure | `scistackplot` `PlotSpec.where_check` + `reduce/where_check.py` (names, levels, Matching rule) | reduce, capability, GUI |
| Node's filters | node config `whereFilters`, stored config only (single route, like `columnSelections`) | every run path, node chips, the check |
| Numeric or categorical | `shape.classify_value` (exists) | which x options a filter offers |

`filter_report` returns one row per target location: the schema keys, and
per filter `verdict` (`pass` / `fail` / `no value`) and the filter variable's
value there. "No value" replaces the coverage error a run would raise, so
the check can show the gap instead of refusing to draw.

## Stages (one commit each; user runs the suites between)

### Filters reach every run
scidb + scistack-gui. No plotting.

- `filter_from_dict` / `filter_to_dict` / `all_of` in scidb. Supports
  whole-variable and `Var["col"]` filters and the six comparisons.
  `_build_where` in `run_service.py` is replaced by a call to it (hot spot
  gets smaller, not bigger).
- Several rows mean AND. The "Each filter runs as a separate variant" hint
  goes. (EachOf of filters returns in the sweep stage, as its own control.)
- `whereFilters` is read from stored config on every path:
  `execution_service` (per-node and pipeline), MATLAB command
  (`MATLAB_OPTION_EMISSION["where"] = BINDING`), Python and MATLAB code
  export. The `where_filters` field of `start_run` is dropped.
- Sidebar row gains an optional column picker (reuse `input_columns`).
- Logging: `[where] node=… filters=… → <to_key>` once per run path; WARN
  when a filter names an unknown variable or column.
- Tests: scidb round trip dict ↔ Filter ↔ `to_key`; GUI integration: the
  same node config produces the same `_run.where_clause` from per-node run,
  pipeline run, exported Python, generated MATLAB text (parity table).

### The node shows its filters
scistack-gui frontend.

- `FunctionNode` draws one chip per filter under the inputs (`Age > 80`),
  text from the backend (`to_key`), never worded in the frontend.
- The variable name in a chip is a link. Click = pooled histogram of that
  variable (next stage). Right-click = menu: Histogram · Plot against ▸ one
  entry per variable input of the node · Edit · Remove. An input that
  cannot be joined is greyed with the backend's reason.
- New entry point `where-chip` in `STUDIO_ENTRY_POINTS`.

### The check figure
scidb → scistackplotdb → scistackplot → GUI.

- `PlotSpec.where_check`: `{node_id, filters: [{…dict, enabled}], }`. Empty =
  an ordinary figure; nothing else changes.
- Source: when `where_check` is set, `ScidbSource` asks `filter_report` and
  joins the verdict columns on schema keys, the way `factor_variables` are
  joined.
- `reduce/where_check.py` turns them into factors before `validate`:
  `Matching` (Yes/No over the enabled filters) always; one factor per filter,
  named by its `to_key`, when there are two or more. Declared level order
  Yes, No. Wired into the three call sites named in
  `docs/claude/synthetic-factors.md`.
- Opening spec (one function, `plot_service.where_check_spec`):
  - click on a chip: measure = the filter variable, Histogram, every key
    collapsed, coloured by Matching;
  - Plot against, numeric filter: `x_measure` = filter variable, Scatter;
  - Plot against, categorical filter: filter variable as the grouping layer
    (x ticks), Box, Show sample on for every collapsed key;
  - Matching holds the colour layer, `colors = {Yes: green, No: red}`.
- Every point shown: transport striding (`max_points`) is off in a check,
  with the count in the layout note.
- Hover: `sample_hover` already names shown keys. In a check it names the
  full schema key combination, the value, and one line per filter
  (`Age > 80: no (72)`), from the report's columns.
- Export code and Add to pipeline are unavailable in a check, with a reason
  from `capabilities` (a check is for looking; its factors are not mirrored
  in codegen). Save image and Save data work.
- Logging: `[where_check] target=… filters=… pass=… fail=… no_value=…`,
  timing for the report under the existing `[timing]` lines.
- Tests: scidb `filter_report` cases (same level, coarser filter, missing
  value, AND of two); scistackplotdb integration on real DuckDB: a check
  figure's Yes rows are exactly the rows `for_each(where=…)` hands the
  function.

### Editing in the check, and going back
GUI. New file `PlotStudio/WhereCheckSection.tsx`; state in the existing
`plotSpecReducer`. `PlotStudio.tsx` only mounts it.

- Top of the Data group, only in a check: one row per filter with a
  tick box (counts toward Matching or not), the op and value editable in
  place (the figure redraws), a numeric / categorical badge, and an
  "x axis" radio. Choosing a numeric filter for x moves to Scatter with
  `x_measure`; a categorical one moves it to the grouping layer. The other
  filters stay available to colour or to separate panels by, in the normal
  Structure group.
- Banner: "Checking where= of <function>" with **Apply to node** (writes
  `whereFilters` through `put_node_config`, then closes the tab) and
  **Discard**.
- From an ordinary Plot Studio figure: Data > Filters gains "Check a where=
  filter…", listing the function nodes that take this variable as an input.

### Docs
`docs/claude/where-in-gui.md` (new), refresh the stale parts of
`where-filter-system.md` and `where-provenance-and-merge.md`, GUI manual
testing doc, `plot-studio-controls.md` map row, README mentions.

### Later, separate plans (not in this build)
- **Elevated filters.** A named filter (or a set of cut points, e.g. gait
  speed <0.4 / 0.4–0.8 / >0.8) declared once in the project, offered as a
  factor on any figure or table of a variable it covers. The check's
  per-filter factor is the same object, so this stage is mostly a place to
  store and name them.
- **Filter sweeps.** `EachOf` over filters or over a threshold Parameter.

## Said no to, for now
- Marker shape by a filter. Plot Studio has one painted layer and no shape
  channel; adding one touches both renderers and codegen. Colour and panels
  cover it.
- `where=[a, b]` as a new `for_each` argument form. `all_of` composes with
  `&`, so nothing changes in the five kwarg-parity places.
- OR / NOT trees in the GUI. Rows are ANDed; `isin` covers the common OR.

## As built: Filters reach every run (2026-11-16)

- scidb `model/filters.py`: `all_of`, `as_filter`, `and_terms`,
  `filter_value`, `filter_parts`, `is_blank_filter_dict`,
  `filter_from_dict`, `filters_from_dicts`, `filter_to_dict`,
  `where_source`. `scidb.all_of` is exported.
- A list is read as AND in `for_each` (at the public entry, before a
  deferred step is registered, and in `_normalize_where`) and in
  `BaseVariable.load`. MATLAB: `+scidb/all_of.m`, used by `for_each.m`
  and `BaseVariable.load` (a cell means AND).
- GUI `services/where_service.py`: `stated_filters`, `where_for_node`,
  `matlab_where_source`, `matlab_where_for_node`. Read by the per-node run
  (`run_service._prepare_run`, phase `prepare.where`), the compiled pipeline
  (`build_backend_pipeline`), Python export (`where_source` of the step's
  filter), the MATLAB command (three emission sites, `_format_where_pair`)
  and the MATLAB export.
- `start_run` no longer takes `where_filters`; `WhereFilterSpec` and
  `_build_where` are gone. Wire schema and TS types regenerated.
- Sidebar: column picker on a table variable, `IN` removed from the
  operator list, hint says AND. Both bundles rebuilt.
- Not done here, said no: `isin` in the dict form. MATLAB has no membership
  filter on a variable, so a row could not be written for a MATLAB node.
- Log: `[where] node=… route=… filters=N -> where=…`.
- Tests: `scidb/tests/test_where_list.py`,
  `scistack-gui/tests/test_where_every_route.py`. No MATLAB test yet for
  `where={A, B}`.

## As built: chips, check figure, editing, docs (2026-11-16)

Docs: `docs/claude/where-in-gui.md`. Deviations from the plan above, all deliberate:

- No `reduce/where_check.py`: the factors are attached at the SOURCE (like a struct's `ColName`), so
  `reduce`/`validate`/renderers see ordinary columns and the three apply-derived-table call sites are
  untouched. Names/levels/colours live in `scistackplot/model/where_check.py`.
- Per-filter factors are `Filter 1`, `Filter 2` (by position), not the filter text, so editing a
  comparison never leaves a role or colour naming a vanished factor.
- The verdict is computed at the plotted variable's locations (see doc, "Limit").
- Chip text is `scidb.filter_label`, put on node data by `graph_builder.where_chips`; no variable
  class needed.
- New RPCs: `set_where_filters` (chip Remove, Apply; merges into stored config),
  `where_check_nodes` (the "Check a where= filter…" list). `plot_describe` takes `where_check`.
- Apply writes only the TICKED filters.
- Not built: "x axis" radio; greying un-joinable inputs; filter value in hover; transport striding off;
  nested-pipeline nodes in the figure's list. `capabilities.code_export` blocks Export/Add to pipeline.
- Wire schema + TS types were regenerated with a node script (no Python here): run
  `python scistack-gui/tools/gen_ts_types.py` once to confirm it produces the same files.
- Both vite bundles and `extension/dist/extension.js` were rebuilt.
- Tests written, NOT run: `scidb/tests/test_filter_report.py`, `scistackplotdb/tests/test_where_check.py`,
  `scistack-gui/tests/test_where_chips.py`, `test_where_check_open.py`; `whereCheck.test.ts` passes.
