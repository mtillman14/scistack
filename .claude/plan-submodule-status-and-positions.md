# Plan: one node status from every canvas, submodule status roll-up, canvas position shifts

**Status 2026-11-19: built (C1, C2, roll-up, position diagnostics); full scistack-gui pytest and frontend tests pass; manual GUI check pending. Item 2 fix waits on the diagnostic log line.**

Evidence: scistack-debug.log 2026-11-19 13:49–14:02 (old code, before d1705e3f / 5947392a).

## Item 1: steps inside submodules red, output green

The 12 run states differ by which canvas asks: `main` builds give 12 green
(13:50:02, 13:56:54, 14:02:37); the EMG submodule 6 red (13:55:25); XSENS 8 red
(13:49:57, 14:02:27). Data is up to date; the red is the wiring check.

Causes:

- **C1 (still present after d1705e3f): a removed inner line kills the live port
  route.** Inside `load_filter_emg`, the old direct line
  `pathInput__emgPath_6MWT::pipe_b4d1… -> loadDelsysEMGOneFile.emgFilePath` was
  removed (hidden on the EMG canvas). A connection id names what the ends are, not
  which occurrence, so it equals the live route
  `pathInput__emgPath_6MWT::main -> use -> port -> loadDelsysEMGOneFile.emgFilePath`.
  `Visibility.connection_hidden` drops a traced connection whose id is hidden on
  either end's canvas, so the route goes, and all three EMG steps are
  "disconnected". This is now true from `main` too.
- **C2: the disconnected check uses the asking canvas's hides for every node.**
  `graph_builder.hidden_wirings` gets one set (the asking canvas plus severed)
  and applies it to every call site. A node's colour depends on who looks.
- **C3 (fixed by d1705e3f): stale inner EMG lines bound from other canvases.**
  From the XSENS canvas, `renameFields.dataStruct = [RawEMG, FilteredEMG]` and
  `filterDelsys.loaded_data = [RenamedEMG, RawEMG]` (removed lines judged by the
  wrong canvas's hides) formed a cycle: "DAG propagation stalled".

Fixes:

- **C1:** a whole-connection hide applies to a traced connection only when the
  hide names the same occurrences: the hide's recorded source and target are on
  the same canvases as the traced wire's (adoption's `retire_rewired_edges`
  records exactly the traced ends). A hide with no recorded ends keeps today's
  match. `Visibility` keeps each edge hide's ends.
- **C2:** `hidden_wirings` takes `hidden_edge_ids_of(node_id)`: each node is
  judged by its own canvas's hides plus severed connections. `EdgeView` exposes
  `severed`.
- Logging: DEBUG when a whole-connection hide is not applied because it names
  other occurrences; INFO per node the disconnected check names, with the canvas
  it was judged on.
- Tests: visibility cases (removed inner line vs live port route; retired route
  still hidden); integration: extract with a removed inner line, run states
  identical from `main` and from the submodule, no node red.

## Item 1b: submodule node status

`pipelineNode` gets `data.run_state`: red if any function node inside (recursively
through nested placements) is red, else pending if any is pending, else green.
No function nodes with a state: no status. One owner:
`domain/run_state.placement_state(states, members)`; `scope_service.build_pipeline_nodes`
takes the build's run states and the child's member nodes. Frontend
`PipelineNode.tsx` draws the border/background like FunctionNode, sharing
`STATE_STYLES` (moved to one module both import). Logged once per build: each
placement's state and the red members that decided it.

## Item 2: main canvas nodes shift after leaving a submodule

Facts: no position writes during navigation (every `put_layout` to main carries
a user `_change`); `get_layout(main)` returns 17 positions for 17 nodes each time.
On a scope switch `PipelineDAG.fetchPipeline` uses saved positions and lays out
with dagre any node whose id has no saved key, so a key/id mismatch shows as a
node that moves depending on the rest of the graph. The log doesn't record keys,
so the cause is not yet proven.

Diagnostics first (NOTE: diagnose before fixing):
- Frontend: on each refresh, count nodes placed from saved / on-screen / dagre and
  list the dagre-placed ids and the saved keys that match no node; send both in
  `report_canvas_refresh` so they land in the log line.
- Backend: `get_layout` logs, per scope, saved keys with no stored or derived node.
Then reproduce, read the line, fix the id/key mismatch at its owner, and add a test.
