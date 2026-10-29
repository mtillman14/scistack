# Plan: Python output ports, declared on the node (2026-10-28)

User decision: inputs and outputs should look and behave as alike as
possible. Inputs come from the signature in both languages; MATLAB outputs
come from the signature; Python outputs cannot be read from the function, so
the user declares them on the node. Reorder/rename/add/remove UI exists only
on Python function nodes. Follows `.claude/plan-output-order-owner.md`
(output order has ONE owner, `domain.output_order`).

## Model (mirrors inputs and MATLAB outputs)

- **A port is a named return value**, like a MATLAB output name or an input
  argument. Port *i* is return value *i*; the port list's order IS the
  `outputs=` order of a canvas run.
- **A connection (edge from the port to a variable node) decides the
  variable**, exactly as for MATLAB (`sourceHandle = out__{port}`). The edge
  is the only owner of that fact; the port list stores names only.
- **Stored per node** as `outputPorts: [name, ...]` in the node config, like
  `shownArguments` (one RPC `set_output_ports`, merged into the config).
  Absent = automatic (today's behaviour): one port per output the node
  already has (named after its variable, recorded order), or, for a new node,
  a single port shown as **output** (the existing unnamed handle).
- **Editing seeds from what is shown**: the first rename/reorder/add/remove
  saves the full list, starting from the current ports (variable names), so
  existing edges (`out__{Variable}`) keep matching with no migration.
- **Rename re-points the node's manual output edges** (`out__{old}` →
  `out__{new}`) in the same call, as a PathInput rename does.
- **Precedence (user 3a):** the declared list wins for canvas runs; when the
  newest script run listed a different order, the Outputs section says so
  (like an unused column pick).

## Stages

### Stage 1: backend storage + drawing
- `outputPorts` in `graph_builder._SAVED_CONFIG_KEYS`; RPC
  `set_output_ports(node_id, ports, renames)` (layout_service, like
  `set_shown_argument`), validating names (non-empty, unique, identifier-ish)
  and re-pointing manual edges on rename; undoable.
- Graph: a Python function with declared ports is drawn like a MATLAB one:
  `declared order = ports`; `port -> class` from the node's edges
  (`infer_manual_fn_param_to_class`), then recorded port names, then by
  position from the newest recorded order for ports still unmapped. One
  helper, `domain/output_order.declared_python_ports(...)`, feeds the same
  dicts `build_function_nodes` / `build_edges` already read; their MATLAB-only
  condition becomes "has a declared output order".
- Never-run manual Python nodes with declared ports draw them (the MATLAB
  branch's equivalent).

### Stage 2: run path
- Canvas run: `outputs=` = the declared ports mapped to their variables, in
  port order. A declared port with no connection refuses the run with a
  message naming it (positional: an unconnected return value has nowhere to
  go). The run records the port names (`_run_outputs.output_params`) via a
  scidb context (`scidb.intent`-style `run_output_params`), so history maps
  ports by name later.
- Picker/tooltips: unchanged (they read the node's handles), but split
  pairing pairs groups with ports in port order.

### Stage 3: frontend
- Python function nodes: an **Outputs** section in the settings panel (rows:
  name box, ↑ ↓, ✕, **+ output**), mirroring the Inputs section. Output
  handles get visible labels like input handles. The new-node single port is
  labelled **output**.
- The 3a note when the newest script run used another order.
- Bundles rebuilt; manual-test entry.

### Stage 4: docs + memory

## Tests (user runs)
`scistack-gui/tests/test_python_output_ports.py` (storage, rename re-points
edges, drawing, mapping precedence, run refuses an unconnected port, run
order = port order, script-run order note), frontend node tests for the
row ↔ list helpers.

## Status (2026-10-28): Stages 1-4 built, tests pending user run
- Found while testing the design: with only script history a port's
  variable is inferred by POSITION, which a reorder would swap. Fix: every
  `set_output_ports` maps the ports BEFORE the edit (`port_variables`) and
  writes an edge for each mapped port without one (the edge owns it from the
  first edit), deletes removed ports' edges, re-points on rename (default-
  handle edges matched by target), and re-points back on clear. Mapping rule
  gained "a variable of the same name" (automatic ports are named so).
- Frontend: `OutputPortsSection` + `outputPorts.ts` (+ tests, 659 pass);
  default handle tooltip **output**; bundles rebuilt.
- Tests: `scistack-gui/tests/test_python_output_ports.py`,
  `scidb/tests/test_output_order_recorded.py::test_a_gui_run_records_its_declared_python_port_names`.
