# Plan: GitHub #27 — "Disconnected" inside and on submodules

## User decisions (2026-11-19)

1. A step inside a submodule shows 🔌 disconnected only if an input cannot be
   traced back to an input port. An input that reaches an input port counts as
   connected, even if nothing is connected to that port on the parent.
2. The submodule node on the parent carries two badges:
   - **Port not connected** (error): an input or output port of this
     placement has no line on the parent canvas.
   - **Steps disconnected inside** (warning): one or more steps inside,
     nested submodules included, are disconnected. It's a warning as long as
     something inside can still run end to end.

## Cause today

`graph_builder.hidden_wirings` counts a hidden recorded input as covered only
when `EdgeView.wiring_list` holds a fully traced wire onto that handle. An open
input port traces to nothing (`ports.resolve_wiring` records it in
`Wiring.open_inputs` instead), so the step reads as disconnected.

Related: the PathInput branch of `hidden_wirings` still checks the shared
`hidden_edge_ids` and not `hidden_edge_ids_of(node)`, the per-canvas rule from
fa55b45b. That's a second owner of the same rule.

## Changes

### A. Inner steps (scistack_gui/domain/graph_builder.py, graph_service.py)
- `hidden_wirings(..., port_fed=None)`: a handle in `port_fed` (the
  `(bare node id, argument)` pairs of `Wiring.open_inputs`, exposed as
  `EdgeView.open_inputs`) counts as covered. There's one owner of "which
  handles a port feeds", read by the badge and by `_wiring_blocker`.
- The PathInput branch uses `hidden_here` (each node's own canvas's hides).
- The run is unchanged: `_wiring_blocker` already refuses an open port first,
  with its own message.
- Logging: each uncovered handle is named (`... in__X: hidden, no line, no
  port`), plus an INFO line for every handle cleared by an input port.

### B. Placement badges (scope_service.build_pipeline_nodes)
- `graph_service` collects `fn_disconnected` (function node ids with
  `data.disconnected`) on every canvas, beside `fn_states`, before the view
  narrows, and passes it and the canvas's final `edges` in.
- `data.open_ports`: `{inputs: [...], outputs: [...]}`, the ports of this
  placement with no drawn edge on `in__<name>` / `out__<name>`. Owner:
  `ports.open_ports_of_placement(use_id, iface, edges)`.
- `data.disconnected_members`: labels of the disconnected steps inside, from
  `_steps_inside`.
- `data.disconnected_severity`: `"warning"` while `ports.runnable_routes`
  finds a route, else `"error"`.
- The existing placement INFO line also logs both badges.

### C. Frontend (PipelineNode.tsx)
- A pure helper `placementBadges(data)` (unit-tested like `mergePorts.test.ts`)
  returns the badges to draw. `PipelineNode` draws them with the same style as
  FunctionNode's `disconnectedBadge`: red for an open port, amber for the
  warning. The tooltips name the ports or steps.
- Rebuild the static and webview bundles.

### D. Tests (tests/test_submodule_ports.py)
- An open input port: the step inside is not `disconnected`, and
  `no_target_reason` names the open port.
- An inner line removed with no port behind it: still `disconnected`.
- A placement with an open input: `open_ports.inputs == [name]`. It's empty
  once the outer line is drawn. The same for an output.
- A disconnected step inside: `disconnected_members` and severity
  (warning/error).
- The PathInput branch is judged by the node's own canvas
  (`hidden_wirings` unit test with `hidden_edge_ids_of`).

### E. Docs
- `docs/claude/submodule-ports.md` (Behaviour: open input; Owners table).
- `docs/claude/function-node-colour.md` (disconnected override).

## Decided (2026-11-19)

"Something can run end to end" is a real path trace: input port (or an inner
PathInput) → steps that are not disconnected → output port, through nested
placements (`ports.runnable_routes`). Not the 80/20 "one step isn't
disconnected".

## Built differently from the plan

- The pipeline run's skip report (`disconnected_report_entries`) does NOT
  pass `open_arguments`. It asks "can it run?", so an open-port step stays
  listed, and now names the port as the reason. Only the canvas badge
  changed.
