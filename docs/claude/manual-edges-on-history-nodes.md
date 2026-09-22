# Manual edges onto history nodes

**Rule: the edges visible on the DAG are the ground truth — for display and
for execution — whether they came from history or were drawn by the user.**

Written 2026-09-15 after a real session: `grSides` had run before, `side`
was added to its signature afterwards, the user wired `Demographics →
in__side`, and the Inputs section said `n/a`. Dragging a fresh `grSides`
node did not help either — it graduated straight into the history node.

## Two kinds of function node, two sources of truth (before)

| | Fresh (manual) node | History (DB-derived) node |
|---|---|---|
| Exists because | user dragged it in; row in `_pipeline_nodes` | provenance has records for `(fn, wiring)` |
| Node id | random `fn__{fn}__{6 chars}` | `fn__{fn}__{wiring_id}` — hash of fn + variable inputs + outputs (+ PathInputs) |
| Edges | manual rows in `_pipeline_edges` | **re-synthesised from provenance on every build** (`build_edges`); manual rows drawn on top |
| `input_params` | resolved from its manual edges (`resolve_function_edges`) | copied from `get_aggregated_variants` — recorded `input_types` only |
| Run targets | never-run fallback: bindings from edges | the recorded variants (`list_pipeline_variants`) |

The consequence, before this change: a manual edge drawn onto a history
node was drawn and nothing else. `input_params` did not show it (fill-in
wrote `side: ""` → the panel's only signal for `n/a`), the run did not use
it, and the only precedent for honouring a manual edge on a history node
was the **reconnect** flow — hide a history edge, wire a different variable
onto the same handle — which keyed on the *hidden* edge and so never fired
for a handle that never had one.

## The rule, concretely

A history node's effective inputs are resolved from its visible edges
exactly as a fresh node's are: per `in__<param>` handle, the variable
sources of every visible edge — history-derived edges that are not hidden,
plus manual edges.

| What is visible on `in__side` | Effective binding |
|---|---|
| no history edge, manual edge from Y | `Y` |
| history edge hidden, manual edge from Y | `Y` (the reconnect case) |
| history edge from X visible, manual edge from Y | `EachOf [X, Y]` — same as on a fresh node |
| history edge hidden, nothing manual | unbound → wiring stays *disconnected* (target dropped) |
| history edge from X visible, nothing manual | `X` |

History is only the source of the *default* edges; it never outranks what
is drawn. Replacing a variable is what hiding its edge is for.

**Node identity does not change** when an edge is drawn. Saved position,
scope placement and node config all key off `fn__{fn}__{wiring_id}` (see
`placement-id lookup trap`), so the overlay lives on the built node data
and the derived run targets only.

## One owner, four consumers

`graph_builder.manual_input_overrides(fn, wid, input_params, const_names,
manual_index, manual_nodes, hidden_edge_ids)` → `{param: type | [types]}`.
Keyed by the node's **history** wiring id, because that is what a manual
edge's `target` names (`manual_edge_handle_index`). A handle with no manual
edge is not in the result; a list value is `EachOf`.

1. **Display** — `graph_builder.collect_manual_input_overrides` +
   `overlay_manual_inputs`, called in `api/pipeline._build_graph` right
   after `build_function_nodes`. Sets `data.input_params[param]` (string —
   every frontend consumer types it that way; an `EachOf` shows its first
   source, the same choice the fresh-node path makes with `ts[0]`) and
   `data.manual_inputs` (the full override, for tooling and the log). The
   Inputs column picker keys off `input_params`, so it lights up with no
   frontend change.
2. **Execution** — `variant_resolver.reconcile_manual_inputs` (renamed from
   `filter_disconnected_targets`, clean break). Per target, in ONE pass on
   the history wiring id: drop when a hidden handle has no manual cover
   (unchanged), then substitute bindings for every override, refresh
   `input_types`, pop the stale `call_id` so `compute_call_id` rebuilds it.
   Both `derive_fn_targets` and `derive_target_for_node` call it whenever
   hidden edges **or** manual edges exist. `_attach_column_selections` runs
   after it and finds the new binding.
3. **Graduation** — `api/pipeline._wiring_conflicts_with_candidate`: a fresh
   node that actively binds a variable to a param the candidate has no
   variable on is a *different wiring* and stays its own node. A bare
   (unwired) fresh node still graduates into its single candidate.
4. **Colour** (added 2026-09-22) —
   `graph_builder.input_params_with_manual_edges` returns a COPY of
   `fn_input_params` with the overrides folded in, and that copy goes to
   `run_state.propagate_run_states` and nowhere else. See §Colour below for
   why it was missing and why it is a copy.

Why one pass in (2): substituting changes the wiring id, so a second pass
keyed on the new id misses both the remaining hidden edges (re-admitting a
partially reconnected wiring) and any further manual edge on the same node.

## Lifecycle: what happens after the run

Provenance records the run under the **effective** wiring, so a second
node `fn__{fn}__W2` appears with a DB-derived `in__side` edge. On the next
build, `graph_builder.superseded_manual_input_overrides` notices that the
old node's effective wiring already exists and:

- rewrites the manual edge's `target` from the old node to the new one
  (`pipeline_store.write_manual_edge`, placement suffix preserved). The
  edge is now an endpoint-duplicate of the DB-derived one, so `build_edges`'
  dedup draws it once; the row stays (hide, never delete — if the DB edge
  ever disappears, the manual one renders again);
- drops the old node's overlay — it reverts to its true history;
- copies `columnSelections` from any config keyed on the old node to the
  same key on the new one, if the new one has none
  (`api/pipeline._migrate_column_selections`). That is the one setting the
  run actually used; without it the new node's next run would silently load
  whole tables.

For an `EachOf` overlay the run splits into one wiring per source; the
wiring looked for is history with the *manual edge's* variable (the
history source's wiring is the old node itself).

## Reading scidb.log

| Line | Meaning |
|---|---|
| `[graph_builder] manual_input_overrides(fn, wid): {...} (recorded=..., hidden handles=...)` (DEBUG) | the rule fired for this node |
| `[graph_builder] <node> input_params after manual overlay: {...} (origins={param: history/manual/unbound}, manual_inputs=..., constants=...)` (DEBUG) | what each handle says and why |
| `[pipeline] manual input overlay applied to N history node(s): {...}` (INFO) | per build summary |
| `[variant_resolver] target for 'fn' (wiring W): manual edge(s) override {...} — substituting bindings=... (call_id recomputed)` (INFO) | the run will use the drawn edge |
| `[graph_builder] manual input overlay on <A> (...) is superseded by <B> — that wiring has run` (INFO) | lifecycle step above |
| `[pipeline] manual edge <id> moved onto <B>` / `column selection {...} migrated from <A> to <B>` (INFO) | the two side effects |
| `[pipeline] graduation candidate ... rejected: manual node's own wiring ... conflicts` (WARN) | fresh node kept separate |

The absence of the `[variant_resolver] ... override` line when a manual
edge is visible on a history node is the diagnostic.

## Colour: red must cross a drawn edge

Added 2026-09-22, from a real session. `loadGaitRiteOneFile` was red and
`grSides` and `calculateSymmetryOneVector` downstream of it were green — three
red nodes out of fifteen, where the chain should have carried it to every one.

Node colours are computed from the **recorded** wiring, about 200 lines before
the display overlay reaches the built nodes (`api/pipeline._build_graph`:
run states at the "Computing run states" step, overlay at "manual input
overlay applied to N history node(s)"). So when the cascade asked `grSides`
"what feeds `grTableIn`?", history answered "nothing" — that connection exists
only as an edge the user drew — and a step with no upstream has nothing to
inherit red from.

The arithmetic is the proof: 8 call sites + 7 variables = 15 nodes, 3 red =
the two `loadGaitRiteOneFile` call sites plus `var__GAITRiteLoaded`, and then
it stops at the first drawn edge. After wiring grouping the two call sites
merge and it reads 2 red, which is what the log showed.

**Applied at BOTH propagation passes.** There are two and each rebuilds its
input mapping from the recorded call sites, so fixing only the first would
leave the pass that decides the node's final colour still blind:

| pass | where | keyed by |
|---|---|---|
| 1 — per call site | `api/pipeline._compute_run_states(..., propagation_input_params=)` | `(fn, call_id)` |
| 2 — on the grouped wiring | `graph_builder.group_call_sites_by_wiring` | `(fn, wiring_id)` |

No flag distinguishes them: the helper recomputes the wiring id from each
entry, which for a call-site key derives the group it belongs to and for an
already-grouped key returns that key's own wid — members of a group share
their params, because the id hashes exactly those.

Two things it must not touch, both load-bearing:

- **Identity.** `wiring_id` hashes `input_params`, so folding an override into
  the dict node ids derive from would rename the node and orphan its saved
  position, scope membership and config (the placement-id lookup trap). This
  is the same rule as §"Node identity does not change", one layer down, and it
  is why the helper returns a copy. `TestRunStatePropagationFollowsManualEdges::
  test_identity_is_untouched` pins it.
- **The own-state check.** Pass 1 asks scidb "has this call site done its
  recorded work", and a drawn edge does not change that question.
  `check_multiple_nodes_state` never sees the overlay.

One supporting change: `propagate_run_states` now flattens a LIST binding.
A manual edge beside a still-visible history edge is an `EachOf`, and
`set(params.values())` raised on an unhashable list — a path that was
unreachable until the overlay made it reachable. Every source counts: if any
producer of any of them is red, the consumer cannot be current.

## Known limitations

- `manual_edge_handle_index` keeps one manual edge per `(fn, wid, handle)`;
  two *manual* variable edges on the same handle of a history node see only
  the last one. (Two edges = history + manual is handled.)
- The Inputs column picker is per parameter, so on an `EachOf` handle one
  column set applies to every source (column-selection.md).
- A manual variable edge onto a param history bound as a **constant**
  (`param__` handle) is a different handle kind and is not covered.
- ~~The run state of a node with an overlay still reflects its history
  (green) although the effective wiring never ran~~ — half fixed 2026-09-22
  (§Colour). The overlay now feeds the DAG *cascade*, so an overlaid node
  inherits red from whatever its drawn edge points at, and red no longer
  stops at the last history edge. Its OWN state is still its history's, which
  is a different question ("has this call site done its recorded work"); the
  "needs run" badge for "the effective wiring never ran" is still a follow-up.
- ~~Node config is not migrated on graduation~~ — fixed the same day:
  `pipeline_store.migrate_node_config` (see `gui-run-options-flow.md`
  §Where the config is stored). Orphans from BEFORE the fix (the
  `fn__grSides__5c9r0r` warning) are not adopted automatically.

## Tests

- `tests/test_graph_builder.py`: `TestManualInputOverrides`,
  `TestOverlayManualInputs`, `TestSupersededManualInputOverrides`,
  `TestRunStatePropagationFollowsManualEdges` (the colour consumer — the old
  behaviour pinned, red crossing the drawn edge and on downstream, identity
  untouched, no-edges short-circuit, and an `EachOf` list not breaking the
  cascade).
- `tests/test_variant_resolver.py`: `TestReconcileManualInputsHiddenEdges`
  (the former reconnect suite; the multitype case now expects `EachOf`),
  `TestReconcileManualInputsUnboundParams`.
- `tests/test_api.py`: `TestManualInputEdgesOnHistoryNodes` — display, both
  run paths, column selection reaching the binding, graduation refusal,
  bare-node graduation kept, and the post-run supersession end to end.

## See also

- `scistack-gui-disconnected-wiring-reconnect.md` — the hidden-edge
  precedent this generalises.
- `function-node-input-params.md` — what an empty type in `input_params`
  can mean (one meaning fewer now: a manual edge no longer leaves it empty).
- `column-selection.md` §From the GUI.
- `.claude/plan-manual-input-edges-on-history-nodes.md`.
