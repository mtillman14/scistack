# Plan: manual input edges onto history (DB-derived) function nodes

Date: 2026-09-15. Triggered by a real GUI session: `grSides` had run before;
the user wired `Demographics → in__side` (a parameter its history never
bound) and the Inputs section showed `n/a`. Dragging a fresh `grSides` node
did not help — it graduated straight into the history node.

## Root cause (one sentence)

Wiring a variable onto a parameter that history leaves unbound is treated
as "no information" in three places, when it is actually a wiring change.

| Where | What happens today | Consequence |
|---|---|---|
| Display (`api/pipeline.py` → `build_function_nodes`) | a history node's `input_params` come only from `get_aggregated_variants`; manual edges are drawn by `build_edges` but never folded back | fill-in writes `side: ""` → panel `n/a` |
| Execution (`derive_target_for_node` / `derive_fn_targets`) | targets are the recorded variants; only manual **output** edges override history; `filter_disconnected_targets` substitutes manual edges only on **hidden** handles | run ignores the new edge, `side` unbound |
| Graduation (`_wiring_conflicts_with_candidate`) | `if real_type and real_type != var_type` — a param the candidate has nothing for is "compatible" | fresh node merges into the history node; escape hatch closes |

The existing **reconnect** precedent (hide a history edge, wire a different
variable onto the same handle) is the same idea for a handle that *had* a
history edge: `manual_edge_handle_index` keyed by `(fn, wiring_id, handle)`,
substitution in `variant_resolver.filter_disconnected_targets`, display
state cleared in `graph_builder.hidden_wirings`. This plan generalises that
owner to the "handle never had a history edge" case rather than adding a
parallel mechanism.

## The rule (revised 2026-09-15 after review)

**The edges visible on the DAG are the ground truth — for display and for
execution — whether they came from history or were drawn by the user.**
Equivalently: a history node's effective inputs are resolved from its
visible edges exactly as a fresh node's are (`resolve_function_edges`):
per `in__<param>` handle, the variable sources of every visible edge —
history-derived edges that are not hidden, plus manual edges. One source →
bare binding; several → `EachOf`; none → unbound (a hidden history edge
with no manual cover keeps the wiring disconnected, as today). History is
only the source of the *default* edges; it never outranks what is drawn.

The first draft of this plan ignored (with a WARN) a manual edge onto a
handle whose history edge was still visible. That was a scope shortcut,
not a principle: it left a wire the user could see doing nothing, which is
the exact class of surprise this plan exists to remove. Under the rule
that picture means `EachOf [history, manual]`, the same as on a fresh
node. Replacing a variable is what hiding its edge is for (the existing
reconnect flow).

Node identity (`fn__{fn}__{wiring_id}`) does **not** change when an edge
is drawn: saved position, scope placement and node config all key off it
(see `project_placement_id_lookup_trap`). The overlay is display + execution
only, exactly like the reconnect precedent.

## Stages

### 1. One owner for the rule — `graph_builder.manual_input_overrides`

```python
def manual_input_overrides(fn, wid, input_params, const_names, manual_index,
                           manual_nodes, hidden_edge_ids=()) -> dict[str, str | list[str]]
```
For every `in__` handle at `(fn, wid)` that a manual variable edge lands
on: the handle's full VISIBLE source list — the history types whose edge
ids are not in `hidden_edge_ids`, plus the manual edge's variable — in DB
shape (bare string for one source, list for several = `EachOf`). A handle
with no manual edge is not in the result (history stands). Uses
`inbound_edge_candidates_by_handle` + `node_id_to_var_label` (local import
— `edge_resolver` imports `graph_builder`). DEBUG for the result.

### 2. Execution — `variant_resolver.reconcile_manual_inputs`

Renames `filter_disconnected_targets` (clean break, 2 callers + tests) and
extends it: per target, using the target's **history** wiring id once,
(a) drop when a hidden handle is not covered by a manual edge (unchanged),
(b) substitute bindings for every param `manual_input_overrides` returns —
hidden-covered *and* unbound, (c) when anything changed: refresh
`input_types`, pop `call_id` (recomputed by `compute_call_id`), INFO log.
Both derive paths call it whenever hidden edges **or** manual edges exist
(today: only when hidden edges exist). `_attach_column_selections` then
finds the binding and stamps the column selection onto it.

The single-pass shape matters: substituting first changes the wiring id, so
a second pass keyed on the new id misses both remaining hidden edges
(partial-reconnection regression) and further manual edges on the same node.

### 3. Display — `graph_builder.overlay_manual_inputs`

Called in `_build_graph` right after `build_function_nodes`, before
`build_edges`. For each function node: parse `(fn, wid)`, compute overrides,
set `data.input_params[param] = var_label` and `data.manual_inputs =
{param: var_label}` (observability; frontend needs no change — the panel
keys off `input_params`). DEBUG line per node listing every param with its
origin (`history` / `manual` / `constant` / `unbound`). Returns
`{node_id: overrides}` for stage 5.

### 4. Graduation — third conflict case

`_wiring_conflicts_with_candidate`: a manual node that actively binds a
variable to a param the candidate has no variable for is a **different
wiring** → no graduation. A bare (unwired) node still graduates (existing
UX, `test_graduation_preserves_sub_scope_membership`). The existing WARN on
rejection already names both wirings.

### 5. Supersession once the new wiring has run

After a run through the overlay, history gains records under the effective
wiring `W2`, so a second node `fn__{fn}__W2` appears with a DB-derived
edge. Without this stage node A keeps the overlay + manual edge forever
(two runnable copies of the same thing) and the column selection saved on A
never reaches B — B's next run would silently load whole tables.

`graph_builder.superseded_manual_input_overrides(overrides_by_node,
fn_input_params, fn_outputs, path_inputs, manual_index)` → for each node A
with overrides, `W2 = wiring_id(fn, history ∪ overrides, outputs, pis)`; if
`fn__{fn}__W2` exists in this graph, return edge rewrites `A → B`
(placement suffix preserved) and the `(A, B)` pair. `_build_graph` then:
persists the rewrites (`pipeline_store.write_manual_edge`, same as
`legacy_edge_rewrites`), patches the in-memory edge list, drops A's overlay
(A reverts to its true history), and copies `columnSelections` from any
config keyed on A to the matching key on B when B has none. The rewritten
edge is an exact duplicate of B's DB-derived edge, so `build_edges`'
endpoint dedup drops it — the row stays (hide, never delete).

### 6. Docs, memory, tests

- `docs/claude/manual-edges-on-history-nodes.md` — fresh vs history nodes,
  where `input_params` come from, graduation, the rule, the supersession
  lifecycle, the log lines to look for.
- Cross-refs in `docs/claude/column-selection.md` (§From the GUI) and
  `docs/claude/function-node-input-params.md`.
- Update memory `project_input_params_empty_type` / `project_column_selection_ui`.

Tests (scistack-gui, user-run):
- `test_graph_builder.py`: `manual_input_overrides` (unbound / hidden /
  live handle → EachOf / non-variable source ignored); `overlay_manual_inputs`;
  `superseded_manual_input_overrides`.
- `test_variant_resolver.py`: existing `filter_disconnected_targets` tests
  renamed; new: unbound param substituted, call_id dropped; hidden +
  unbound on one node in one pass; partial reconnection still dropped.
- `test_api.py` / `test_pipeline_scopes.py`: manual edge onto history
  node's unbound param → `GET /api/pipeline` shows the type; `POST /api/run`
  captures `inputs[param] is Var`; column selection saved on that node
  reaches the binding; fresh node wired to an unbound param does NOT
  graduate; after a run under `W2`, the edge is rewritten to B and B
  carries the `columnSelections`.

Test vehicle: `_registry._functions["bandpass_filter"]` re-registered with a
wider signature `(signal, low_hz, side=None)` — this mirrors the real case
exactly (grSides' log line 455: "source has changed since it last ran";
`side` was added to the signature after the recorded runs).

## Out of scope (noted, not done)

- A manual variable edge onto a param history bound as a **constant**
  (`param__` handle) — different handle kind, not touched.
- The Inputs column picker is per parameter, so on an EachOf handle one column set applies to every source (already documented in column-selection.md).
- Run-state of a node with an overlay still reflects history (green) even
  though the effective wiring never ran; a "needs run" badge is a follow-up.
- Node config is not migrated on **graduation** (`graduate_manual_node`
  deletes the `_pipeline_nodes` row; `_node_config` rows under the manual id
  are orphaned — that is the `fn__grSides__5c9r0r` warning). Separate fix.
