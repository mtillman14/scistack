# Function-node input port order: one owner

## Problem
`load_xsens_one_file(xsens_file_path, col_names, sheet_name)` drew `col_names`
third. The left-side dots come from two lists that are each sorted by
signature order but then concatenated:

- backend `build_function_nodes`: `input_params` (variable / PathInput / unwired)
  and `constant_params` (Parameter-fed), sorted separately; unrecorded
  signature args appended AFTER the sort; the manual-override overlay appends
  further keys.
- frontend `FunctionNode.tsx` (twice: canvas node + variant-graph node):
  `[...input_params, ...constant_params]`.

## Fix
1. `graph_builder.input_port_order(signature, names)` — THE rule: signature
   order, then names outside the signature alphabetically.
   `build_function_nodes` uses it for its sorts (and re-sorts after the
   fill-in).
2. `graph_builder.assign_input_ports(nodes, signatures)` — one final pass in
   `api/pipeline._build_graph` after every overlay/manual node: writes
   `data.input_ports` (ordered arg names, the union of signature,
   `input_params`, `constant_params`) and re-orders `input_params` with the
   same rule. INFO log per node whose port order differs from the
   concatenation the old frontend would have drawn; DEBUG for every node.
3. Frontend: one helper `inputPortNames(data)` used by both handle builders;
   draws `input_ports` as-is. Freshly dropped node (`PipelineDAG.tsx`) sets
   `input_ports = info.params`.
4. Tests: `tests/test_graph_builder.py` — Parameter-fed arg in the middle stays
   in the middle; unrecorded arg keeps its signature slot; overlay-added arg
   keeps its slot; frontend unit test for `inputPortNames`.
5. Docs: `docs/claude/function-node-input-params.md` §4,
   `docs/gui-manual-testing-todo.md`.
