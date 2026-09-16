# SciStack-GUI: per-handle manual-edge coverage clears "disconnected" wiring

## The bug this fixed (2026-08-11, see .claude/plan-manual-reconnect-disconnected-fix.md)

Deleting a function node's required inbound edge (e.g. `RawVO2 -> signal`)
is a GUI-only "hide" — never a real delete (project ethos: never delete,
mark hidden; see `pipeline_store.hide_edge`/`get_hidden_edge_ids`). It
correctly marks the node's wiring "disconnected" (🔌 badge, forced red,
Run blocked). But manually wiring a *different* variable onto that same
`in__signal` handle (drag `RawHeartRate -> signal`) used to do nothing: the
badge/red state never cleared and Run kept failing with `"input 'signal' is
disconnected"`, even though the canvas visibly showed a new edge feeding
that input.

Root cause: every "is this input disconnected" check —
`graph_builder.hidden_wirings()` (badge + forced-red run state) and
`variant_resolver.reconcile_manual_inputs()` (actual execution
eligibility, via `execution_service.derive_fn_targets`/
`derive_target_for_node`) — decided purely from the function's **DB-recorded**
wiring (`db.list_pipeline_variants()`) crossed against the hidden-edge-id
set. None of them looked at whether a manual edge now supplied that same
`target_handle` with a different variable. Hiding one inbound edge
therefore poisoned that input forever, unless the user reconnected the
*exact same* source (which `layout_service.put_edge`'s `candidate_edge_id`
auto-unhide special-case already handled correctly, and still does — this
fix is specifically about reconnecting a DIFFERENT source).

## The core concept: hidden-handle coverage

A hidden inbound edge id (`e__{var_type_or_const}__{fn}__{wiring_id}`) does
not by itself say *which* input param it feeds — that requires reconstructing
the id alongside the param it came from. Two new `graph_builder.py` helpers
make that association explicit and reusable:

- `inbound_edge_candidates_by_handle(fn, wid, input_params, const_names, path_names)
  -> {candidate_edge_id: target_handle}` — same id shape as the older
  `inbound_edge_candidates` (flat list), but keeps the `target_handle`
  (`in__{param}` / `const__{name}`) each candidate maps to.
- `manual_edge_handle_index(manual_edges) -> {(fn_name, wiring_id, target_handle): edge}`
  — indexes every manual edge by the exact call site + handle it currently
  feeds, via `parse_fn_node_id(edge["target"])` (which already strips any
  placement suffix, so a bare, wiring-grouped, or scope-placed target id
  all resolve to the same key — no new id-parsing logic).

With that, "is this wiring disconnected" becomes: for each hidden handle on
the wiring, is there a manual edge currently covering `(fn, wid, handle)`?
If **every** hidden handle is covered, the wiring is reconnected. If even
one is not, it stays disconnected — a partial reconnection (e.g. only the
variable input, not a hidden constant) never becomes runnable.

This check now happens in two independent places that both needed it:

1. **Display/run-state** — `graph_builder.hidden_wirings()` takes an
   optional `manual_edges` param and excludes covered handles before
   deciding a wiring is disconnected. `api/pipeline.py` fetches
   `manual_edges_for_fn_lookup` earlier than before (right before the
   "Disconnected wirings" block) so it's available at this point, and
   passes it through. This is what clears `node["data"]["disconnected"]`
   and the forced-red `run_state`.
2. **Execution** — `variant_resolver.reconcile_manual_inputs()` takes
   `manual_edges`/`manual_nodes` params. When a target's hidden handles are
   all covered, it does **not** just re-admit the stale DB target as-is
   (that historical call was never run with the new variable) — it
   synthesizes a fresh target dict with the covered handle's `input_types`
   substituted for the manually-wired variable's type (resolved via
   `edge_resolver.node_id_to_var_label`, the same manual-id-to-label
   resolver the never-run-fallback path already used), and drops any stale
   `call_id` so `compute_call_id` recomputes it fresh. `execution_service.
   derive_fn_targets`/`derive_target_for_node` pass their already-fetched
   `all_edges`/`manual_nodes` through; no other change was needed there —
   both already return the target list as soon as it's non-empty, so a
   substituted result now flows straight through instead of hitting the
   "nothing safe to run" fallback that used to fire for an already-executed
   (graduated) node whose only DB-history target got excluded.
   `disconnected_reason()`/`disconnected_report_entries()` got the same
   coverage check so their human-readable messages stay consistent for the
   still-blocked (partial reconnection) case.

## Why substitution, not just "un-exclude"

A DB variant's `input_types` records what a specific historical call
actually ran with. If the hidden handle's edge is now covered by a manual
edge to a *different* variable, re-admitting the old target unchanged would
silently queue a re-run with the wrong (old) input. Substituting the new
variable's type in produces a target shaped exactly like the existing
"never-run" fallback (`resolve_function_edges`'s output) — a fresh,
not-yet-executed wiring that will register a genuinely new call_id in scidb
when it actually runs.

## Known pre-existing limitation, unchanged by this fix

`get_hidden_edge_ids(db)` in `execution_service.py` is still called with no
`pipeline_id` (global union across scopes) — see
`scistack-gui-scoped-hidden-state.md`'s "Known remaining gap" section. This
fix doesn't touch that; a manual edge in one scope can in principle "cover"
a hidden handle for a same-wiring placement in a different scope. Not a new
regression, just not addressed here.

## Where to look

- `scistack_gui/domain/graph_builder.py`: `inbound_edge_candidates_by_handle`,
  `manual_edge_handle_index`, `hidden_wirings`.
- `scistack_gui/domain/variant_resolver.py`: `reconcile_manual_inputs`.
- `scistack_gui/domain/edge_resolver.py`: `node_id_to_var_label` (reused,
  not new).
- `scistack_gui/services/execution_service.py`: `derive_fn_targets`,
  `derive_target_for_node`, `disconnected_reason`, `disconnected_report_entries`.
- `scistack_gui/api/pipeline.py`: where `manual_edges_for_fn_lookup` gets
  fetched early and threaded into `hidden_wirings`.
- Tests: `tests/test_graph_builder.py::TestHiddenWirings` (manual-reconnect
  cases), `tests/test_variant_resolver.py::TestFilterDisconnectedTargets`
  (substitution cases), `tests/test_api.py::TestDisconnectedEdges`
  (end-to-end reconnect/Run cases), `tests/test_pipeline_scopes.py::
  TestDeriveTargetForNode` (graduated-node reconnect case).
