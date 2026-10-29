# Output order: one owner (2026-10-28)

**Output order** is the order of `outputs=` in a for_each call: a function's
return order, or a for_columns split's group pairing (group *i* → output *i*,
`docs/claude/for-columns-iteration.md` "Column groups"). Plan and decision
log: `.claude/plan-output-order-owner.md`.

## The fact and its owner

| Role | Where |
|---|---|
| The fact, written by every run (Python and MATLAB) | scidb `_run_outputs (run_id, outputs VARCHAR[])`, written by `provenance_save.record_run` → `_commit_graph` beside the `_run` row |
| Reading it | `scidb.provenance_query.recorded_output_orders(duck, fns)` → `{fn: [order, ...]}`, distinct, newest run first |
| The node's handle order | `scistack_gui.domain.output_order.node_output_order(wired, recorded, edge_order, declared)` |
| Whether one call writes them all | `output_order.writes_together` |

`node_output_order`: the newest recorded order naming any wired output
(restricted to the wired ones), then unrecorded outputs in `declared` order
(a MATLAB signature mapped to classes), then in `edge_order` (the order the
output edges were drawn), then alphabetically with an INFO line.

## Why not something already stored

- `_invocation_output.output_num` is a SLOT, not a position: a re-run of
  the same invocation appends to the next free slot (`provenance_save`
  "Fix B"), and a distributed run's slot is the slice index.
- `_invocation` is per call and its insert is DO NOTHING; outputs are not in
  `invocation_id`, so a re-run in another order would keep the first order.
  Hence a per-RUN table, like `_run_step`. Not identity.
- Manual edges had no read order (`intent_store.manual_edges`, no ORDER BY);
  they are now sorted by their statement's `stated_at` (drawn order;
  re-drawing an edge moves it last).

## Consumers (read the handles, never re-derive)

- Graph build: `api/pipeline._build_graph` queries `recorded_orders` once;
  `graph_builder.build_function_nodes(recorded_output_orders=...)` orders
  Python handles (was `sorted(actual_outputs)`); never-run manual nodes go
  through the owner with their drawn edge order.
- MATLAB nodes keep signature-param handles, except a function whose newest
  run wrote MORE outputs than it declares (a split):
  `pipeline.matlab_class_handle_functions` drops its param mapping so it draws
  class handles in recorded order. `_matlab_param_to_class_from_db` maps
  signature output *i* to recorded class *i* when the counts match
  (`output_num` only as the fallback for older runs).
- Python canvas run: `execution_service.run_output_types(inputs, node
  handles, output_type, recorded)`: every node output in one call, in handle
  order, for a splitting selection or when `writes_together`. Fixed bug: a
  canvas run used to write ONE output per call and de-duplication kept one
  target, so a two-output function's second output was never written.
  Still open: functions whose runs wrote outputs SEPARATELY keep one output
  per call, and de-duplication still keeps one target for them.
- MATLAB canvas run: `matlab_command_service.generate_matlab_command`
  passes the owner's class order as `output_types`; with history
  `api.matlab_command._order_group_outputs` sorts each group by it (was
  variant-row order); without history the signature order is the
  `declared` input, not a re-sort of its own.
- Picker and output tooltips: node handle order (`plan_groups`).

Logs: `run … of <fn> wrote outputs in order [...]` (scidb),
`[run_thread] target N writes output(s) [...]`, `generate_matlab_command:
output order [...]`, `[pipeline] matlab fn=… wrote … class-named handles`,
`[output_order] no recorded, declared or drawn order …`.

Tests: `scidb/tests/test_output_order_recorded.py`,
`scistack-gui/tests/test_output_order.py`.
