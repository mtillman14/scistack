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
| recorded PathInput P (hidden or not), manual edge from PathInput Q | `Q`, which replaces P: one PathInput per handle (`manual_path_input_overrides`, 2026-09-28) |
| output: manual edge from the node to variable Z | the run saves into `Z` on both Run paths (`execution_service._apply_manual_output_types`) |

PathInputs and outputs: see `hidden-path-input-identity.md`.

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

**Nothing moves any more.** Rewritten 2026-09-22 for allocated node ids
(`docs/claude/node-identity.md`, D-2026-09-22-1).

Provenance still records the run under the **effective** wiring `W2`. What
changed is what that means for identity. The node id is no longer a hash of
the recorded bindings, so `W2` does not name a new node: it is attributed to
the node that already **stated** it — the drawn edge is part of what the node
states, so before the run the node states `W2`, the run records `W2`, and it
is the same node (`domain/node_identity.py` rule 2; a GUI-started run does not
even need the inference, because it records the association at dispatch).

Afterwards:

- the node keeps its id, its saved position, its scope membership and every
  `_intent` statement keyed on it — including `schemaLevel`, `runOptions` and
  `whereFilters`, which used to be stranded silently;
- `_node_wiring` holds two rows for it, one per shape it has run as. That is
  the honest description of what happened: one node, rewired. `W2` is its
  **current** shape and `W1` is history — the canvas draws the first, and a
  Run executes the first, because re-running a shape the user deliberately
  moved away from is not what clicking their node means;
- its recorded `input_params` now carry the binding, so
  `manual_input_overrides` finds nothing new to overlay and the manual edge
  and the DB-derived edge dedup to one line on the canvas;
- the manual edge row is **not** rewritten. It already names the node that ran.

For an `EachOf` overlay the run splits into one wiring per source, and the
node states **every** one of them (`graph_builder.stated_wiring_claims`), so
neither source forks a node.

### What this replaced

Until 2026-09-22 a second node `fn__{fn}__W2` appeared, and the next build had
to repair it: `graph_builder.superseded_manual_input_overrides` detected the
overlap, rewrote the manual edge's `target` onto the new node, dropped the old
node's overlay, and carried its statements across
(`api/pipeline._migrate_node_statements`). Before that, only `columnSelections`
was carried at all — so every other setting silently stopped applying, and a
real user re-entered `schemaLevel` six minutes after the run that dropped it.

Both functions are **deleted**, not left as dead paths. A duplicate that formed
BEFORE this change stays two nodes until you hide or rewire one — there is no
absorb, because that would be a migration and this is a clean break
(D-2026-09-22-4). It can no longer form again.

## Reading scidb.log

| Line | Meaning |
|---|---|
| `[graph_builder] manual_input_overrides(fn, wid): {...} (recorded=..., hidden handles=...)` (DEBUG) | the rule fired for this node |
| `[graph_builder] <node> input_params after manual overlay: {...} (origins={param: history/manual/unbound}, manual_inputs=..., constants=...)` (DEBUG) | what each handle says and why |
| `[pipeline] manual input overlay applied to N history node(s): {...}` (INFO) | per build summary |
| `[variant_resolver] target for 'fn' (wiring W): manual edge(s) override {...} — substituting bindings=... (call_id recomputed)` (INFO) | the run will use the drawn edge |
| `[node_wiring] <node> now runs as wiring <W> (run_id=..., scope=...)` (INFO) | the node claimed the shape it was rewired into — its ABSENCE is why a duplicate appeared |
| `[node_identity] wiring <W> of '<fn>' attributed to <node> — that node STATES it` (INFO) | a script/terminal run was inferred (rule 2) |
| `[execution] run <id> on node <n> claims N wiring(s)` (INFO) | a GUI-started run recorded its association at dispatch |
| `[pipeline] N wiring(s) are history rather than a node's current shape` (INFO) | the node was rewired; its older shapes stay as history |
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
  `TestOverlayManualInputs`, `TestStatedWiringClaims` (which replaced
  `TestSupersededManualInputOverrides`),
  `TestRunStatePropagationFollowsManualEdges` (the colour consumer — the old
  behaviour pinned, red crossing the drawn edge and on downstream, identity
  untouched, no-edges short-circuit, and an `EachOf` list not breaking the
  cascade).
- `tests/test_variant_resolver.py`: `TestReconcileManualInputsHiddenEdges`
  (the former reconnect suite; the multitype case now expects `EachOf`),
  `TestReconcileManualInputsUnboundParams`.
- `tests/test_api.py`: `TestManualInputEdgesOnHistoryNodes` — display, both
  run paths, column selection reaching the binding, graduation refusal,
  bare-node graduation kept, and the post-run case end to end: ONE node, the
  edge where it was drawn, every setting still applying, nothing stranded.
- `tests/test_node_identity.py`, `tests/test_node_wiring.py`,
  `tests/test_wiring_parity.py` — the identity model underneath all of it.

## See also

- `scistack-gui-disconnected-wiring-reconnect.md` — the hidden-edge
  precedent this generalises.
- `function-node-input-params.md` — what an empty type in `input_params`
  can mean (one meaning fewer now: a manual edge no longer leaves it empty).
- `column-selection.md` §From the GUI.
- `.claude/plan-manual-input-edges-on-history-nodes.md`.

## Hidden nodes take no part in colour (2026-10-01)

The same rule seen from the node side: **a hidden node is not a producer of
anything on screen.** Run states are computed on ungrouped call sites, BEFORE
`filter_hidden`, so a hidden node used to stay in the cascade. A hidden,
disconnected phantom `pandas.read_csv` node (`fn__pandas.read_csv__4b9cac03…`,
hidden twice by the user) was forced red. By the worst-producer rule it held
`DemographicsTable` red after the visible node had re-run it green.

- `graph_builder.hidden_fn_fkeys` maps hidden node ids to their pre-grouping
  call sites, with the same `token_for(fn, wiring_id(...))` derivation that
  `hidden_wirings` uses.
- `run_state.propagate_run_states(hidden_fkeys=)` drops those call sites
  entirely: no own state, no outputs, no inputs. Both passes pass it: pass 1 in
  `api/pipeline._compute_run_states`, and the grouped pass in
  `group_call_sites_by_wiring(hidden_node_ids=)`. Without the grouped pass, a
  hidden node that alone produced a variable a visible node reads would stall
  that consumer, which is then marked red as a "possible cycle".

Tests: `scistack-gui/tests/test_run_state.py::TestHiddenNodes` and
`test_hidden_fn_fkeys_maps_a_hidden_node_id_to_its_call_sites`.

## A hand-placed entity node always folds into its DB twin (2026-10-01)

`merge_manual_nodes` graduates a hand-placed node when exactly one DB node shares its
(type, label). It used to refuse when the twin's placement already had a saved
position in that scope (`test_graduation_skipped_if_own_placement_already_exists`),
which left BOTH nodes drawn, permanently. On 2026-10-01 a position left over from a
09-30 node (`var__GaitRiteLoaded_UA::main`) blocked the user's newly placed
`var__GaitRiteLoaded_UA__k7huza`, and running `grSides` showed two
`GaitRiteLoaded_UA` nodes.

Now, for **entity nodes** (`graph_builder.ENTITY_NODE_TYPES`: variable, PathInput,
Parameter), which each name one thing, graduation goes ahead with
`GraduationAction.take_old_position=True`. The hand-placed position replaces the
twin's (user decision), via `layout.graduate_manual_node(take_old_position=)`.
Edges move as in any graduation. **Function nodes keep the old rule**: a second
same-named function node can be a new call site, and they graduate by wiring.
Placement stays per scope.

Tests: `test_graph_builder.py::TestMergeManualNodes::
test_an_entity_node_graduates_onto_an_already_placed_twin` /
`test_a_function_node_still_skips_an_already_placed_twin`, and
`test_layout.py::TestGraduateManualNode::test_take_old_position_replaces_the_canonical_position`.

## A manual edge whose history twin is hidden binds nothing (2026-10-01)

A user-drawn edge stays in `_pipeline_edges` forever (hide, never delete). Once
the call site runs, the history edge over the same connection supersedes it, and
build_edges draws only the history edge. If the user later disconnects that
wire, the visible edge is hidden, which is the history `e__...` id. The manual
twin is still stored, and it is still invisible.

Before this fix, `manual_edge_handle_index` indexed **every** stored manual edge,
so the invisible twin kept binding its variable. Reconnecting a different
variable then produced an EachOf, `{'v': ['GAITRiteLoaded', 'GaitRiteLoaded_UA']}`,
and MATLAB command generation refused it ("more than one candidate producer
type"). A stale twin with no replacement also silently "covered" the hidden
handle, so the node never showed as disconnected.

**One owner:** `graph_builder.manual_edge_is_hidden(edge, hidden_edge_ids,
manual_nodes)`. A manual edge is hidden if its own id is hidden or
`history_twin_edge_id(edge)` is hidden. That function is the one spelling of
the twin id (var `e__{type}__{fn}__{tok}`, Parameter `e__{argument}__{fn}__{tok}`,
PathInput `e__{name}__{param}__{fn}__{tok}`). build_edges and
`manual_edge_handle_index` both apply the rule. `hidden_edge_ids` is a
**required** keyword on the index, so no caller can build it from raw rows.

A deliberate re-draw of the same connection is not caught: `layout_service.put_edge`
auto-unhides the history edge.

**scidb.log:** `[graph_builder] N stored manual edge(s) are hidden and bind nothing
(own id hidden, or their history twin is): manual__x (src -> tgt.handle; twin e__... hidden)`.
It is logged at INFO once per edge per process, and at debug after that.

Tests: `scistack-gui/tests/test_hidden_manual_twin.py`.

**Second reader (same day).** The first fix covered only the handle index.
`calculateSymmetryOneVector`'s node is still a manual row (a `fn__{fn}__{token}`
id in `_pipeline_nodes`), so `derive_target_for_node` resolved it from
`edge_resolver.resolve_function_edges` over **raw** rows, and the MATLAB run
failed the same way. Now `graph_builder.visible_manual_edges` is the one filter.
`resolve_function_edges`, `infer_manual_fn_output_types` and
`infer_manual_fn_param_to_class` take `hidden_edge_ids` as a **required** keyword
and filter through it. The MATLAB command service filters its edges once at load.
`history_twin_edge_id` also covers output edges (`e__{fn}__{tok}__{type}`).

**Parameter edges follow the current wiring (same day, 12:38).** A drawn
`formulaNum -> calculateSymmetryOneVector` edge "just disappeared". The node had
been rewired, and its current wiring had never run, so it had no constant handle
and `formulaNum` showed as `in__formulaNum`. `group_call_sites_by_wiring` took
ports from `is_current` call sites only, but mapped **every** call site's
`const_fns`. So build_edges drew history's `param__formulaNum -> node` onto a
handle the node does not render, which React Flow drops silently. It also
dropped the user's drawn edge as superseded by that invisible twin. Now
`const_fns` follows `is_current` too (the key stays, so the Parameter node
remains). scidb.log says `Parameter edge(s) come only from a node's history
wiring and are not drawn`. Tests: `tests/test_history_parameter_edges.py`.
PathInput edges followed on the same day (below).

**PathInput edges and the disconnected state follow the current wiring too
(same day).** Two gaps were left:
- A node rewired from PathInput A to B and run kept drawing `A -> node` forever,
  next to `B` on the same port. `group_call_sites_by_wiring` mapped every call
  site into `grouped.path_inputs`. It now maps `is_current` call sites only. The
  key stays, with `"recorded": True`, so the PathInput node is still built and
  `build_path_input_nodes` does not mark it declared-only. Identity is
  unaffected: it is resolved before grouping, from the raw aggregate. The only
  readers after grouping are `build_path_input_nodes`, `build_edges` and
  run-state propagation.
- Hiding that stale edge, or a history **variable** edge with no drawn edge over
  the port, turned the whole node red. `hidden_wirings` checked every recorded
  wiring and keyed its result by node TOKEN, and `wiring_disconnected_fkeys` maps
  a token to every call site of the node. It now takes a **required**
  `is_current` and checks current wirings only. Its callers pass
  `identity.is_current` (`api/pipeline._build_graph`,
  `execution_service.disconnected_report_entries`).

scidb.log: `PathInput edge(s) come only from a node's history wiring and are
not drawn`. Tests: `tests/test_history_path_input_edges.py`.
