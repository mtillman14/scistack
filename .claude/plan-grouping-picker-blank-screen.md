# Grouping picker blank screen — diagnosis and fix plan

Date: 2026-09-15. Prompted by: "the variant selector for Grouping in the plotting
GUI resulted in just a blank screen".

## What the log shows

`scidb.log` 17:39:05 — last successful `bar` render (14 rows).
`scidb.log` 17:39:19–17:39:25 — `get_pipeline` (5.75 s) + `get_layout`: that is
`usePipelineCanvas()` in `DagPicker.tsx`, i.e. the Grouping popup opening.
After that: **nothing**. No `RPC << … FAILED`, no `plot_*` line, no
`plot_grouping_columns` (which would have logged a DuckDB hold). Python never
saw a failing request. The crash happened inside the webview's React render,
which nothing reports to `scidb.log` today.

## Root cause (frontend, `scistack-gui/frontend`)

`GroupingDagPopup.tsx` step one ("which variable?") passes
`canvasWrapper={undefined}` — no `VariantSelectionProvider` around the canvas.
Its `pickNodeTypes` still maps `functionNode: FunctionNode` and
`parameterNode: ParameterNode` from `components/DAG/`. Both branch on
`useVariantSelection()`: with a provider they render the inert popup bodies;
**without one they render `PipelineFunctionNode` / `PipelineParameterNode`**.
`PipelineFunctionNode` calls `useScope()` and `useRunLog()`, which throw
(`useScope must be used within ScopeProvider`) because `PlotRoot` — the Plot
Studio tab — mounts no providers. React unmounts the whole tree → blank tab.

`VariantDagPopup` never hits this because it always wraps (line 466). The
Grouping picker was never visually checked (memory: grouping-nesting-and-picker).

Second, unrelated-but-adjacent gap: `api.ts` has no routes for
`plot_grouping_graph`, `plot_grouping_columns`, `plot_grouping_default_variant`,
so the standalone (FastAPI) build would throw `Unknown method` on step two.

## Fix

1. **Make the invariant structural** — `PickerDialog` owns the rule "a picker
   canvas never mounts execution state": it takes `selection: VariantSelectionValue`
   and always wraps the canvas in `VariantSelectionProvider`. Remove
   `canvasWrapper`. `GroupingDagPopup` step one passes an inert selection (no
   axes, everything selected, `latest`) built by a new
   `inertVariantSelection()` in `VariantSelectionContext.tsx`.
2. **Observability** (NOTE 2): a `PlotErrorBoundary` in `PlotRoot` that renders
   the error + component stack in the tab instead of blank, and forwards it to
   Python via a new `report_client_error` RPC (server.py handler → `Log.error`,
   FastAPI route for parity, api.ts route). Next time a render crash lands in
   `scidb.log`.
3. **Regression test** (`npm test`, node --test, React-free): a source guard
   `pickerCanvas.test.ts` — every `<ReactFlow` under `components/PlotStudio/`
   is in `DagPicker.tsx` and sits inside `<VariantSelectionProvider`; no
   PlotStudio file mentions `canvasWrapper`. Same spirit as the Python AST
   guard tests.
4. Add the three missing `plot_grouping_*` routes to `api.ts`.
5. Rebuild BOTH vite targets (memory: frontend-bundle-rebuild trap).
