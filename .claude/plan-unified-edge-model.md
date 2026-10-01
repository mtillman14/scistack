# Plan: one edge model, kind-agnostic

*2026-10-01. Status: PLANNED, on branch `refactor/unified-edge-model`. Step 1 is detailed; steps 2 and 3 are outlines,
to be decided after step 1 ships. Decision entry: D-2026-10-01-1 in
`docs/claude/decisions.md`.*

## Why

There is no single "edge" concept, so every reader translates between three
representations:

1. **Two edge populations with incompatible identities.** History edges are
   regenerated every build with ids that encode their endpoints, one format
   per kind: `e__{type}__{fn}__{tok}`, `e__{arg}__{fn}__{tok}`,
   `e__{pi}__{param}__{fn}__{tok}`, `e__{fn}__{tok}__{type}`. Drawn edges have
   random `manual__xxxx` ids. "Same wire?" is answered by rebuilding history
   ids from drawn edges (`history_twin_edge_id`), and hides are stored by id.
2. **A port's name depends on what feeds it.** The same argument is `in__X` or
   `param__X`.
3. **Each reader works out "which edges exist" itself, from raw rows.**

2026-10-01 alone produced five bugs of this class, each found by a failed run:
the stale variable twin, the edge-resolver second reader, the history
Parameter edge, PathInput history edges, and the `in__` Parameter copy
(commits 92a8927a, 6d2fa2c0, 8d419d3d, 007e00d7).

## Target

- **One edge key:** `(source node, target node, target port)`, where the port
  is the argument name. History and drawn edges compute it from the same fields.
- **One effective edge set per scope:** history edges plus drawn edges, minus
  hidden connections, deduplicated by key. EVERY reader uses it: canvas, run,
  disconnected check, MATLAB command, run-state propagation.
- **Kind-specific code only in binding semantics:** two variables on a port make
  an EachOf; a Parameter fans out; a drawn PathInput replaces the recorded one;
  glue chains. Edge existence, visibility and identity are kind-agnostic.

## Step 1: one effective edge set (no id, port, data or frontend change)

Highest value, lowest risk. It closes the "readers disagree" class.

### Readers today (inventory 2026-10-01)

| reader | reads raw | file |
|---|---|---|
| graph build | `get_manual_edges` x3, `get_hidden_edge_ids(db, scope)` x3 | `api/pipeline.py` |
| run targets, name-scoped | `get_manual_edges`, `get_hidden_edge_ids(db)` (ALL scopes) | `services/execution_service.py: derive_fn_targets` |
| run target, node-scoped | same | `derive_target_for_node` |
| "why can't this run" | same | `disconnected_reason` |
| pipeline-run skip report | same | `disconnected_report_entries` |
| MATLAB command | same, filtered once via `visible_manual_edges` | `services/matlab_command_service.py` (x2) |
| overlay / reconcile | receive edges from callers | `domain/graph_builder.py`, `domain/variant_resolver.py` |

Mutators also read raw rows. That is correct, and they stay as they are:
`layout_service`, `scope_service`, `layout.py`, `pipeline_store`.
The frontend never reads raw manual edges; it draws `get_pipeline`'s edges.

### Design

- New module `scistack_gui/domain/edge_view.py` with one owner:
  `effective_edges(db, scope) -> EdgeView`. `EdgeView` is a frozen value with:
  - `drawn`: manual edges that are visible (`manual_edge_is_hidden` is false)
    and not superseded by a visible history edge with the same key;
  - `hidden_edge_ids`, for the scope;
  - `handle_index()`: what `manual_edge_handle_index` returns today, built from `drawn`;
  - `edges_into(node_id)` / `edges_out_of(node_id)`.
- `manual_edge_handle_index`, `visible_manual_edges` and the edge-resolver
  functions take an `EdgeView` (or its `drawn` list) instead of raw rows plus
  hidden ids. The required `hidden_edge_ids` keyword goes away, since the view
  already applied it.
- One INFO line per construction: scope, drawn/hidden/superseded counts, and
  the caller. A reader can be matched to the canvas in scidb.log.

### Guard

An AST test (as for DuckDB fetch locking and filter-after-identity) in which only
`edge_view.py` and the listed mutators may call `pipeline_store.get_manual_edges`
or `get_hidden_edge_ids`. A new reader then cannot diverge without failing CI.

### Parity test, the main safety net

On a fixture database, for every function node, the input bindings derived by
the graph build (overlay), `derive_target_for_node`, `disconnected_reason`
and the MATLAB command's `variable_inputs`/`sweep_params` must agree with
the canvas's edges into that node. Cases to cover:
- a hidden history edge with a drawn replacement (the GAITRiteLoaded case);
- a stale drawn twin under a hidden history edge;
- a rewired-and-run node: Parameter and PathInput history wirings;
- a Parameter on `in__X` beside history `param__X`;
- duplicate drawn edges;
- a hidden node;
- a glue chain.

All of today's regression tests carry over unchanged.

### Order of work

1. Write the parity test against today's code. Expect it to pass. Where it
   fails, that is a live divergence to record.
2. Add `edge_view.py` and the AST guard (with an allow-list of today's callers,
   so it passes).
3. Move readers one at a time, shrinking the allow-list: graph build, then
   `derive_target_for_node`, `derive_fn_targets`, `disconnected_reason`,
   `disconnected_report_entries`, then the MATLAB command service. Run the
   parity test after each.
4. Remove `hidden_edge_ids` from the domain signatures once no caller passes
   raw rows.

### Decided (user, 2026-10-01): hides on the run path use the node's scope, option (a)

**The scope of hides on the run path.** The canvas hides edges per scope (one
hypothesis tab). Execution unions every scope's hides, so hiding an edge in a
hypothesis tab also disconnects it for runs from `main`. Options:
- **(a), CHOSEN:** execution uses the clicked node's scope
  (`intent_store.scope_of_node`), as hidden NODES have done since 2026-09-20.
  The run then matches the canvas the user is looking at. This is a live bug
  today, though narrow: an edge hidden in one hypothesis tab also disconnects
  that node for runs from another tab. All four execution readers call
  `get_hidden_edge_ids(db)` unscoped. The name-scoped fallback (no node id)
  keeps the union, matching hidden nodes.
- (b) keep the union. Rejected.

The parity test covers it: an edge hidden in a hypothesis tab, then run from `main`.

### Risks

- Execution builds less context than the graph build. If `EdgeView` needs
  history edges (for superseded-by-visible-twin), it needs the aggregate.
  Measure with a timing line. If it is costly, `drawn` uses the twin rule only
  (as `visible_manual_edges` does today) and keeps superseded copies; they bind
  the same source, so the result is identical.
- Edge handling sits next to node identity. Step 1 changes no ids and no
  identity input, and the identity pass is untouched.

### Progress (2026-10-01)

- [x] Parity test written against the old code: 6 passed, 1 xfail (scope). Commit 42108147.
- [x] `domain/edge_view.py`: `effective_edges(db, scope, caller=)` and `run_scope(db, node_id)`.
- [x] Readers moved onto it:
  - `_build_graph` and `ensure_node_identities` (union scope, as before);
  - `derive_fn_targets` (union, name-scoped);
  - `derive_target_for_node` and `disconnected_reason` (node scope);
  - `disconnected_report_entries` and `generate_matlab_pipeline_command` (the pipeline's scope);
  - `generate_matlab_command` (node scope);
  - `pipeline_interface` and `build_pipeline_nodes` (the submodule's scope; they used raw rows, hides included).
- [x] AST guard `tests/test_edge_view_guard.py` with the mutator allow-list.
- [x] Found while moving readers: `candidate_edge_id` was a SECOND spelling of history edge ids. It keyed a
  Parameter by its declared name instead of the argument, so a redrawn
  `gaitrite_config -> gaitRiteConfig` never unhid. It is deleted; `put_edge` and `rebase_node` use
  `history_twin_edge_id`, which now works without a port where the id encodes none and reads
  hand-placed Parameter/PathInput node labels.
- [x] Scope scenario xfail marker removed.
- [x] pytest passes (user), committed a3bea3fd.
- [ ] Drop the now-redundant `hidden_edge_ids` filtering keyword from the edge-resolver functions and
  `manual_edge_handle_index` (idempotent today, so harmless). Deferred to a cleanup commit.

## Step 2: one port name per argument (outline)

Every input port is `in__X`, whatever feeds it. `param__X` goes away: from
history Parameter edges (`build_edges`), `FunctionNode.tsx`
(`constant_params` handles), `plot_service`, `VariantSelectionContext`,
`VariantDagPopup` and `edge_resolver`'s Parameter-handle branch. It removes
the `in__`/`param__` alias bugs. It is a clean break: hidden Parameter edges
come back once. Both vite targets need rebuilding (frontend bundle trap).

### Step 2 progress (2026-10-01)

- [x] `ids.PARAM_HANDLE_PREFIX` and `param_handle` deleted. Every input port is `in__X`.
- [x] `build_edges` and `inbound_edge_candidates_by_handle` write `in__{arg}` for Parameter edges.
  Edge ids are unchanged, so existing hides keep working: unlike the plan's guess, no hidden
  Parameter edge reappears.
- [x] `edge_resolver` binds a Parameter only through `in__X`. `history_twin_edge_id` reads `in__` only.
- [x] `execution_service` disconnect checks and `plot_service.axis_node_bindings` (Parameter edge
  recognised by its SOURCE: a `param__` id or a hand-placed parameterNode).
- [x] `edge_view` drops stored edges on the retired `param__X` port (WARN once per edge id,
  clean break). The canvas still shows the connection through history's `in__X` edge when the
  node ran with the Parameter.
- [x] `FunctionNode.tsx`: both renderings use `in__X` and skip a name already in input_params.
  Both vite bundles rebuilt; 516 frontend tests pass.
- [x] pytest passes (user), committed 51362853.

## Step 3: hides keyed by connection (built 2026-10-01)

Design chosen: the edge id IS the connection key, rather than threading a new
hidden-key type through every reader. That keeps every `id in hidden` check and
removes every per-kind id format.

- [x] `graph_builder.endpoint_ref(node_id, manual_nodes)` gives the canonical endpoint:
  `fn:{name}:{token}`, `var:{type}`, `param:{declared}`, `pi:{declared}`, `node:{bare id}`.
  Hand-placed nodes resolve through their labels; placements are ignored.
- [x] `connection_id(source, target, target_handle, manual_nodes)` is
  `e__{src}__{tgt}__{argument}`, and `edge_connection_id(edge)` applies it to an edge dict.
  This is THE id of a connection.
- [x] `build_edges`: all four history edge kinds take `connection_id` as their id.
- [x] `history_twin_edge_id` and `inbound_edge_candidates` deleted.
  `inbound_edge_candidates_by_handle` returns connection ids and takes `parameter_names`
  (argument -> declared name), so a Parameter is named by its declaration.
- [x] `manual_edge_is_hidden`, `manual_input_overrides`, `hidden_wirings` (new `fn_parameter_names`),
  `reconcile_manual_inputs` (target `parameter_names`) and `disconnected_reason` /
  `disconnected_report_entries` (through the candidate owner) now use connection ids.
- [x] `pipeline_store.get_hidden_edge_ids` returns the connection id of each hide's STORED
  endpoints and port, falling back to the saved id. Existing GUI hides keep working: the frontend
  has sent endpoints and ports since 2026-08-09. `unhide_connection` is used by `put_edge`, and
  `rebase_node` re-mints with `connection_id`.
- [x] Fixed on the way: a variable feeding two arguments of one function no longer shares one id,
  so hiding one no longer hides both.
- [x] pytest passes (user).

Hide matching (`pipeline_store._hidden_connection_id`):
- a hide with endpoints AND a port matches by the connection of those (every GUI input-edge hide);
- otherwise it matches by its saved id, which is a connection id for anything hidden since step 3.

Clean-break effects: an OUTPUT edge (no port) hidden before step 3 has an old-format saved id, so it
reappears once. A hide on a retired `param__X` port reappears once. First pytest run: the parity test
deleted with no port, which exposed that port-less hides must fall back to the saved id.

The planned cleanup (dropping the redundant `hidden_edge_ids` filtering keyword) is NOT done:
`hidden_edge_ids` still carries connection ids for history edges, so the keyword is still needed.
