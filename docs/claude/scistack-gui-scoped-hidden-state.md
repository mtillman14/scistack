# SciStack-GUI: canonical-id sharing across pipeline scopes, and scoped hidden state

## The core fact: canonical ids are scope-independent by design

A DB-derived canvas node/edge id (`var__{Type}`, `fn__{fn_name}__{wiring_id}`,
`const__{name}`, `pathInput__{name}`, and the `e__...` edge ids built from
those) is computed from real, shared backend data — NOT from which pipeline
scope (hypothesis tab / submodule) is asking. In particular
`domain.graph_builder.wiring_id()` hashes only `fn_name + input/output
shape`; it has no pipeline_id in its input at all.

This is intentional: `scope_service.duplicate_pipeline`'s whole point is
that a duplicated hypothesis keeps computing IDENTICALLY to the original
until the user actually changes something — "Function/variable-type
identity stays shared for free (a fresh node with the same label just
resolves against the same real function/DB table)". Two hypothesis
pipelines with unedited, identical wiring will therefore always produce the
exact same bare canonical id for that wiring.

A node/edge only gets a scope suffix (`{canonical}::{pipeline_id}` — see
`graph_builder.placement_id`/`parse_placement_id`) once it's independently
**placed** in a non-root scope (graduation, or an explicit position write).
The root scope (`main`) itself stays bare UNLESS something forces an
explicit placement there too (e.g. `duplicate_pipeline`'s "solidify" step,
which affirms the source's own placement so it doesn't go ambiguous once
the copy independently graduates elsewhere).

`domain.scope_filter.node_scope(node_id, manual_nodes, positions_by_scope)`
is the one shared "what scope is this node in" answer — it checks (in
order): the manual node's own `pipeline_id` column, a placement-qualified
suffix, a bare-id position-scan fallback, and finally defaults to root.
Every part of the codebase that needs to answer this question should call
it rather than re-deriving the logic.

## The bug this created (fixed 2026-08-11, see .claude/plan-scope-hidden-nodes-edges.md)

Because canonical ids are shared, "hidden" state for them (a user deleting
a node/edge from the canvas — never a real delete, see
`pipeline_store.hide_node`/`hide_edge`, project ethos: never delete, mark
hidden) used to be recorded and read back **globally**, keyed only by the
bare id, with no scope column at all in `_pipeline_hidden_nodes`/
`_pipeline_hidden_edges`. Concretely:

- Deleting a node from a duplicated hypothesis pipeline hid it in the
  ORIGINAL pipeline too (and everywhere else sharing that wiring) — because
  `api.pipeline._build_graph(db, pipeline_id)`, despite being called once
  per scope, fetched `get_hidden_node_ids(db)` / `get_hidden_edge_ids(db)`
  with no scope filter.
- Symmetrically, `layout.write_manual_node`'s unhide-on-recreate
  (re-adding a deleted node to the canvas) unhid the bare canonical id
  globally, resurrecting it — already wired — in every OTHER scope sharing
  that wiring too.

## The fix

`_pipeline_hidden_nodes`/`_pipeline_hidden_edges` now key on
`(pipeline_id, node_id|edge_id)`. `hide_node`/`unhide_node`/`hide_edge`/
`unhide_edge` take an explicit `pipeline_id`; `get_hidden_node_ids`/
`get_hidden_edge_ids`/`list_hidden_edges` take `pipeline_id: str | None`
(`None` = every scope unioned — kept for callers not yet updated, see
below). The scope for a delete/reconnect action is derived from the
already-resolved id(s) the frontend sends (via `scope_filter.node_scope`
for a node id, or an edge's `source`/`target` endpoints) — no new required
frontend fields were needed for the canvas delete/re-add paths.

`_pipeline_hidden_combos` (pending-constant combo hiding) was deliberately
**left global** — `get_hidden_node_ids` always unions its ids in regardless
of the requested `pipeline_id`. It has the same scope-independence risk in
principle but wasn't part of the reported bug; scoping it is a documented
follow-up.

## Known remaining gap: the execution/run-readiness path is still global

`services/execution_service.py` (`derive_fn_targets`, `disconnected_reason`,
target derivation used by both per-node and per-pipeline runs) calls
`get_hidden_node_ids(db)` / `get_hidden_edge_ids(db)` with **no**
`pipeline_id` — none of those functions currently take a scope parameter at
all. This means: canvas rendering can now correctly show a function as
"connected" for scope X after the fix above, while the actual RUN path
still treats it as disconnected because of a hide recorded in a different
scope Y sharing the same wiring (or vice versa). This is unchanged
pre-existing behavior, not a regression from the fix — scoping the
execution path is a materially bigger change (it would need `pipeline_id`
threaded through `derive_fn_targets`/`derive_target_for_node`/
`disconnected_reason`, `api/run.py`, and the `server.py` JSON-RPC handlers)
and wasn't part of the reported bug. Worth revisiting if a user reports a
canvas-vs-run-time disconnected-state mismatch across hypothesis pipelines.

## Where to look

- `scistack_gui/domain/graph_builder.py`: `wiring_id`, `placement_id`/
  `parse_placement_id`/`strip_placement`.
- `scistack_gui/domain/scope_filter.py`: `node_scope`, `_resolve_in_scope`,
  `resolve_scope_view`.
- `scistack_gui/stores/pipeline_store.py`: `_pipeline_hidden_nodes`/
  `_pipeline_hidden_edges` schema + migration, `hide_node`/`unhide_node`/
  `hide_edge`/`unhide_edge`/`get_hidden_node_ids`/`get_hidden_edge_ids`.
- `scistack_gui/stores/layout.py`: `delete_node`, `write_manual_node`.
- `scistack_gui/services/layout_service.py`: `delete_edge`, `put_edge`
  (reconnect-unhide), `unhide_edge`/`get_hidden_edges` (no canvas UI since 2026-11-03; redraw or undo restores).
- `scistack_gui/api/pipeline.py::_build_graph`: where scope gets threaded
  into the hidden-id fetch for canvas rendering.
- Tests: `tests/test_pipeline_store.py::TestScopedHiding`,
  `tests/test_pipeline_scopes.py::TestScopedNodeEdgeHiding`.
