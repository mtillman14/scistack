# Plan: GUI Nested Pipelines + Endpoint-First Surface (GUI stage)

Status: APPROVED 2026-07-16 (G1–G4 all confirmed by user). Execution
order confirmed: stage 4 (MATLAB parity) first, then Part A (nesting),
then Part B item 1 (endpoint presentation).

**Checkpoint 1 — persistence (A1) IMPLEMENTED + VERIFIED 2026-07-16
(full GUI suite + scihist state suite green, user-run).** As built:
- `pipeline_store.py`: `_pipelines` + `_pipeline_uses` tables;
  `pipeline_id` column on `_pipeline_nodes` (ALTER + NULL backfill to
  `main`, same pattern as the config column); root row auto-inserted.
  CRUD: `list/create/rename/delete_pipeline` (delete refuses root +
  still-used pipelines, cascades own nodes/edges/uses; rename updates
  pipelineNode labels on parent canvases). Uses:
  `add_pipeline_use` (use row + canvas node whose node_id IS the use_id,
  node_type='pipelineNode'; direct+transitive cycle rejection),
  `remove_pipeline_use` (row + node + touching edges),
  `get_pipeline_uses`, `update_use_binding` (key whitelist: key_map/
  params/iterate). Edges stay scope-less (scope = endpoints' scope;
  service layer filters).
- `layout.py`: positions per scope (`{pipeline_id: {node_id: {x,y}}}`,
  `positions_scoped` flag; flat files migrate under `main` on load);
  `read_layout(pipeline_id="main")` keeps the flat per-scope shape so all
  existing callers see pre-nesting behavior; scope params (default root)
  on `write_node_position`/`write_manual_node`; `delete_node`/
  `graduate_manual_node` scope-aware; name-derivation helpers scan the
  merged position view.
- scidb: `Pipeline.interface()` (composed consumed-not-produced inputs /
  produced outputs — the pipeline node's ports; A2 groundwork).
- Tests: `scistack-gui/tests/test_pipeline_scopes.py` (new, 16 tests);
  two exact-shape assertions updated in `test_layout.py`; interface test
  in scidb's `test_pipeline_registry.py`.

**Checkpoint 1 test run also surfaced pre-existing GUI↔scidb drift**
(none from the scoping work; fixed + VERIFIED 2026-07-16 alongside):
- `api/variables.py` records endpoint queried the DELETED
  `_record_metadata` table → rewritten against `{name}_data` JOIN
  `_record`/`_schema` with `branch_params_batch` (the batched provenance
  helper — no N+1), unknown names validated against `_variables` and
  returning the empty shape.
- GUI tests still encoded tri-state "partial → grey"; scidb node state is
  BINARY (green/red) since the bipartite change. Tests updated: partial/
  stale = red; GUI grey now ONLY means the pending-constant downgrade
  (run_state.py's own semantics, unchanged).
- `test_project_api` fixture imported the long-deleted
  `scilineage.lineage_fcn` → `@scistack`.
- `test_partial_run_only_greys_its_own_call_site` green-assertion failure:
  a REAL scidb regression, root-caused statically — the invocation-
  membership rewrite of `check_node_state` dropped the `call_id` filter
  ("call sites never blur" held for invocation_id but NOT for the
  expected-set union across variant configs), so one call site's partial
  run reddened every sibling call site. Fixed in the scidb layer
  (2026-07-16): `function_variant_configs` now carries `path_inputs` +
  member `invocation_ids`; new `config_call_id()` shares
  `pipeline_variants`' reconstruction recipe; `expected_invocations_for_
  function(call_id=)` scopes configs, realized-inputless pairs, and the
  declared-inputs fallback; `check_node_state` passes call_id through.
  Regression tests: `scihist/tests/test_state.py::TestCallSiteScoping`.
- `api/project.py` serialized plain @scistack functions as `str(fn)`
  (a `.fcn`-wrapper leftover) → `__name__` first.

**Checkpoint 2 — services/API IMPLEMENTED + VERIFIED 2026-07-16 (full GUI
suite green, user-run).** Two fixes landed during verification:
- Graduations move positions (= the DB-derived scope-membership record)
  and delete manual rows, so `_build_graph` refreshes
  positions_by_scope/manual_nodes after executing graduation actions —
  filtering on the pre-graduation snapshot double-placed a just-graduated
  node (root + sub). Bonus behavior locked in by test: graduation
  PRESERVES sub-scope membership (place `bandpass_filter` inside a
  sub-pipeline → the canonical call-site node lives there).
- Edge filtering judges endpoints by `node_scope`, not kept-node
  membership: dangling manual edges (legacy `fn__{name}` endpoint ids
  that match no built node) default to root and stay on the root canvas
  exactly as pre-scoping.

As built:
- `domain/scope_filter.py` (pure): `node_scope` — manual nodes belong by
  `pipeline_id`; DB-derived nodes by WHERE THEIR POSITION IS SAVED
  (dragging onto a sub-canvas writes the position into that scope, which
  IS the membership record); unsaved -> root. `filter_graph_to_scope`
  (edges survive only with both endpoints); `document_interface` — the
  GUI-document mirror of scidb's `Pipeline.interface()`, recursing
  through nested uses.
- `services/scope_service.py`: scope CRUD + use CRUD with layout side
  effects (positions per scope; `drop_scope_positions`/
  `drop_node_positions` added to layout.py); `build_pipeline_nodes` —
  pipelineNode entries with child name, binding, ports.
- `_build_graph(db, pipeline_id)`: full build then scope filter;
  pipelineNode metas excluded from the generic manual-node merge (they
  are built by scope_service with ports); graduation now sees positions
  merged across scopes; response gains `pipeline_id`.
- FastAPI: new `api/scopes.py` router (GET/POST/PUT/DELETE /pipelines,
  /pipelines/{pid}/interface, /pipelines/{pid}/uses,
  /pipeline-uses/{use_id}[/binding]); ValueError -> 400.
  `GET /api/pipeline?pipeline_id=`, `GET /api/layout?pipeline_id=`,
  `PUT /api/layout/{id}` body gains pipeline_id.
- JSON-RPC: 8 new methods (list/create/rename/delete_pipeline,
  get_pipeline_interface, add/remove_pipeline_use, update_use_binding);
  get_pipeline/get_layout/put_layout accept pipeline_id. Handler
  exceptions already map to RPC errors.
- Tests appended to test_pipeline_scopes.py: TestScopeApi (CRUD, 400
  guards, use flow incl. pipelineNode on the parent canvas with binding +
  position, cycle/binding-whitelist 400s, remove-use cleanup),
  TestScopedGraph (scope exclusion both directions, position-based
  DB-derived membership move, edge filtering), TestDocumentInterface
  (var->fn->var document ports + the same ports on the pipeline node).

**Checkpoint 3 — execution rearchitecture (G2) IMPLEMENTED + VERIFIED
2026-07-16 (full GUI suite green, user-run).** As built:
- `services/execution_service.py`: `derive_fn_targets` (the per-function
  target derivation EXTRACTED from api/run.py's thread — DB variants with
  manual-wiring overrides, else manual-edge + pending-constant fallback —
  so per-node and pipeline runs derive identically);
  `build_backend_pipeline` (document scope -> in-session scidb.Pipeline:
  function-node targets register via `for_each(..., pipeline=pipe)` —
  explicit target, never ambient; use rows -> `use(child.bind(**binding))`;
  per-request memo preserves diamond dedup); `plan_pipeline` (the R2
  plan-preview data); `run_pipeline` (modes all/until/endpoints ->
  backend verbs, endpoints with include_used=True).
- scidb: `Pipeline.discard()` — transient compiles drop out of the
  session never-run bookkeeping (long-running server would accumulate
  them); compile paths discard in finally.
- api/run.py: inline derivation block replaced by the shared helper;
  `_run_pipeline_in_thread` + `start_pipeline_run` reuse the run
  registry/relay/message contract (v1: cooperative cancel is a no-op for
  pipeline runs — Pipeline._run has no between-step hook; force-cancel
  works).
- Endpoints: `GET /api/pipelines/{pid}/plan?target=`,
  `POST /api/pipelines/{pid}/run` (mode/target/finalized/skip_computed;
  validation 400s synchronously); JSON-RPC `get_pipeline_plan` /
  `start_pipeline_run`.
- Tests: TestExecutionCompiler in test_pipeline_scopes.py (derivation
  parity, root-scope compile + interface, plan green after seed run,
  composed cross-scope plan, skip_computed no-op run via invocation
  count, run validation 400s, transient-compile cleanup).

**Checkpoint 4 — frontend IMPLEMENTED 2026-07-16 (awaiting user visual
verification + `npm run build`).** As built (all in `frontend/src`, shared
by the standalone and webview builds — no extension changes needed, the
extension forwards JSON-RPC methods generically):
- `api.ts`: scope routes added to the fetch table under the SAME names as
  the JSON-RPC handlers (list/create/rename/delete_pipeline,
  add/remove_pipeline_use, update_use_binding, get_pipeline_interface,
  get_pipeline_plan, start_pipeline_run); `get_pipeline`/`get_layout` now
  send `?pipeline_id=`; non-ok fetch responses throw `Error(detail)` so
  backend 400 messages surface verbatim (parity with the RPC error path).
- `context/ScopeContext.tsx`: `currentScope` + `breadcrumb` (navigation
  PATH of `{use_id, pipeline_id, name, binding}` crumbs, root always
  first — G3), descend/ascendTo/jumpTo/renameInPath, `graphVersion` bump
  for refetch-after-mutation; exports `bindingSummary` (badge/crumb text).
- `context/PlanRunContext.tsx`: pending PlanRequest ({pipeline_id, mode,
  target?, finalized?, label}) — every pipeline run control funnels here.
- `components/DAG/PipelineNode.tsx`: double-bordered magenta node; ports
  from `data.inputs/outputs` (`in__`/`out__` handles like FunctionNode);
  compact binding badge; ▶ Run requests a plan for mode=all on the CHILD.
- `PipelineDAG.tsx`: scoped fetch (positions never carry across a scope
  switch; fitView on switch), pipelineNode in nodeTypes, double-click
  descend (crumb carries binding), sidebar-pipeline drop →
  add_pipeline_use (cycle 400s alerted verbatim), pipelineNode delete →
  remove_pipeline_use, palette drops + drags write layout with
  pipeline_id=currentScope, function-node right-click menu "Run until
  here" (mode=until, target=fn name), top-right Panel "Run endpoints"
  with draft/finalized toggle.
- `components/DAG/Breadcrumb.tsx`: `main ▸ loading (low_hz=30) ▸ filters`;
  click ascends; HIDDEN at root with no path (root-scope pixel parity).
- `Sidebar/EditTab.tsx`: "Pipelines" section (list from list_pipelines,
  current scope highlighted, click jumps, drag places a use, + creates,
  ✎ inline-renames — also patches crumbs via renameInPath — × deletes;
  store 400s shown verbatim under the section; root has no ✎/×).
- `Sidebar/PipelineSettingsPanel.tsx` (+ Sidebar Node-tab wiring):
  binding editor — key_map/params/iterate rows, values JSON-parsed with
  string fallback; save via update_use_binding, unknown-key 400 verbatim;
  ports summary; "Open pipeline" descends.
- `components/PipelineRunController.tsx`: plan-preview dialog (R2) —
  ordered step table (step, owner pipeline, combo count, green/red chip;
  endpoint rows tinted + tagged), Run/Cancel; runs post
  start_pipeline_run with a frontend run_id and stream run_output/
  run_done into the EXISTING run console.
- `RunLogContext`/`RunsTab`: RunEntry gains `kind`; pipeline cards have
  NO soft-cancel (v1 no-op) — force-cancel button only.
Backend untouched (no contract bugs found).

**Post-checkpoint fixes (2026-07-18, found during user visual verification
with gui_test_data):**
- State rename: GUI third state `grey` → `pending` (yellow #eab308/#fefce8;
  orange belongs to path inputs) = "GUI change only, not in database";
  scidb stays binary green/red. run_state.py + both node components + 5
  test files (test names asserting red under grey names also corrected).
- Stuck-"Running…" chain, three independent causes, all fixed:
  1. `uvicorn` without a WS protocol lib rejects /ws upgrades — dependency
     is now `uvicorn[standard]` (pyproject); users must reinstall.
  2. api/ws.py shared ONE queue across all pump tasks and gather leaked
     the pump on disconnect → an orphaned pump could consume run_done for
     a dead connection. Rewritten: per-client outbox queues, pump
     explicitly cancelled in the connection's finally, fan-out on the
     loop; every drop point logs `[ws] DROPPED … <why>`. Regression:
     tests/test_ws.py (the reconnect test HANGS on the old design).
  3. Frontend useWebSocket logs connect/close/error to the console.
- Run-status honesty: for_each never raises on iteration failures
  (continue-and-report), so "the call returned" ≠ success. scidb
  `Pipeline.last_run_report` (per-step {completed, failed, total,
  cancelled} from scifor's authoritative summary progress event, collected
  via a chained `_progress_fn` in `_execute_step`; skip_computed combos
  never inflate counts). GUI: eager thread tallies summary events,
  pipeline thread reads the report; both set success=False when failed>0,
  emit ⚠/✗ console lines, and put completed_combos/failed_combos on
  run_done. Tests: TestLastRunReport (scidb), report pass-through (GUI).
- Schema-iteration default: for_each only auto-iterates when
  schema_filter/schema_level is set — GUI passed neither, so canvas runs
  pooled ALL schema rows into one call (per-combo functions crash on
  multi-row tables; silently broken until the honesty fix exposed it).
  Eager runs now default schema_level=all schema keys (except explicit
  as_table); compiled pipeline steps carry EXPLICIT metadata iterables
  (full grid, like a hand-written script) — explicit, not schema_level,
  so binding `iterate` overrides compose per key instead of conflicting.
  Test: test_compiled_steps_iterate_schema_grid.

Grounding: `docs/claude/scistack-gui-backend-internals.md` (dual-protocol
server, service layer, `_pipeline_*` DuckDB tables + JSON layout);
backend stages 1–3 all verified (registry, composition, bindings,
endpoint verbs). Existing GUI features (drag-drop pipeline, code
discovery, variant definition, node creation) are assumed working and
untouched.

## Part A — Nested pipelines (pipeline-as-node)

User requirement: a node that itself represents a pipeline; double-click
opens the encapsulated pipeline (hiding all other pipelines' nodes);
navigation between pipeline screens.

### A1. Data model (GUI persistence — scistack-gui layer)

The GUI's flat document model gains pipeline scoping:

- New `_pipelines` table: `(pipeline_id, name)`. A reserved root pipeline
  (`main`) exists implicitly; migration assigns all existing nodes to it
  (one-time, sentinel-guarded — same pattern as the manual-nodes JSON
  migration).
- `_pipeline_nodes` gains a `pipeline_id` column (scope of every node).
- New `_pipeline_uses` table: `(use_id, parent_pipeline_id,
  child_pipeline_id, binding_json)` — one row per pipeline-node placed on
  a parent canvas. `binding_json` holds `{key_map, params, iterate}`
  (empty = identity). The pipeline NODE on the canvas is
  `node_type='pipeline'`, `node_id=use_id` — so the same child pipeline
  placed twice (with different bindings = two variants, matching the
  backend's binding-signature dedup) is two nodes.
- Layout JSON becomes per-scope: positions keyed
  `{pipeline_id: {node_id: {x, y}}}` (migration: existing positions →
  root scope).
- Cycle guard on `_pipeline_uses` insert (mirror backend
  `PipelineCycleError` — reject at edge creation with a clear message).

**Deliberate consistency with the no-spec-persistence decision:** these
tables are the GUI's DOCUMENT (what the user drew), not backend spec
persistence. At run time the GUI constructs in-session backend `Pipeline`
objects from its document (below); the backend still never stores
pipelines.

### A2. Pipeline node rendering (ports)

A pipeline node shows its INTERFACE: input ports = variable types consumed
inside but not produced inside; output ports = types produced inside.
Backend helper (new, tiny, in scidb — it is pure graph logic that MATLAB/
CLI can also use): `Pipeline.interface() -> {"inputs": [...classes],
"outputs": [...classes]}` computed from the composed steps'
`input_classes`/`output_classes`. The GUI service mirrors the same
computation over its document for unsaved/edited pipelines.
Edges on the parent canvas connect variable nodes to pipeline-node ports
using the existing edge machinery.
Binding badge: non-identity bindings render like variant badges today
(`low_hz=30`, `session→subject`).

### A3. Navigation

- **Descend:** double-click a pipeline node → canvas swaps to the child
  pipeline's scope (ONLY its nodes; nothing from other pipelines).
- **Breadcrumb path:** `main ▸ loading ▸ filters` — composition is a DAG
  (one pipeline used by many parents), so the breadcrumb is the
  navigation PATH taken, not a unique address; clicking a crumb ascends
  to that scope. Entering via a BOUND use shows the binding in the crumb
  (`loading (low_hz=30)`) — context for why constants display overridden.
- **Sidebar:** flat list of all pipelines (from `_pipelines`) for direct
  jumps; current scope highlighted.
- Frontend state: `current_scope` (pipeline_id) + `breadcrumb` (list of
  use_ids); all node/edge queries and layout reads become scope-filtered.

### A4. Execution integration (where stages 1–3 pay off)

The GUI currently executes per-node `for_each` in worker threads. New
model — the GUI service builds in-session backend pipelines from the
document:

1. For each GUI pipeline: `db.pipeline(name)` (or `Pipeline(name, db)`
   without activation), register each function node's call via
   `for_each(..., pipeline=pipe)` marshalled exactly as the run service
   already marshals eager calls.
2. Pipeline-use rows → `parent.use(child.bind(**binding_json))`.
3. Run controls map to backend verbs: node context-menu **"Run until
   here"** → `run_until(step)`; pipeline-node **"Run"** → child
   `run_all()` semantics via `run_until` on its own steps; canvas **"Run
   endpoints"** → `run_endpoints()`.
4. **Plan-preview dialog** (R2, was never built): before any run, call
   `plan(target)` and show the step list with green/red state + combo
   counts + owner pipeline; Run/Cancel. Non-blocking backend (N3) means
   the GUI owns this gate, as designed.
5. Roll-up state: a pipeline node is red if any step in its subtree is
   red — derivable from `plan()` entries (each carries owner pipeline).

### A5. Out of scope for this stage

MATLAB-registered steps in the GUI (stage 4 descriptors could surface
later); editing a child pipeline "in place" on the parent canvas
(expand-in-situ) — navigation-only v1; auto-layout of imported pipelines.

## Part B — Gap analysis: other missing GUI features

Ranked; 1–2 are the "crucial" ones given recent backend work.

1. **Endpoint-first presentation (the original vision, now unlockable).**
   - Endpoint nodes styled distinctly (plot/stat kinds via
     `_endpoint_kind`); optionally an "endpoints rail" listing
     `pipe.endpoints()` as cards — the inverted view where processing is
     the collapsible ancestry.
   - **Artifact preview panel:** clicking a plot node shows its rendered
     figure(s) (paths from records, or from `show()` drafts), with
     stamp-derived provenance caption (function, inputs, branch_params —
     `read_artifact_stamp` exists). Stat nodes: formatted JSON table.
   - **Draft/finalized toggle** on run controls (D3 flag is plumbed
     through everything already).
   - **`show()` button** on endpoint nodes: draft-run + preview, zero DB
     writes — the everyday "let me look at it" loop.
   - **Report button:** `db.inspect.write_report(dir)` + open
     `index.html` (report CLI shipped 2026-07-07; no GUI trigger yet).
2. **Pull-execution controls** — "Run until here" + plan-preview dialog
   (folded into A4 above; listed here because they matter even before
   nested pipelines ship).
3. **Binding editor** — placing a pipeline node opens a small form:
   key_map (dropdowns: child's keys → project schema keys), params
   (discovered constants of the child subtree, validated via `bind()`'s
   bind-time errors surfaced inline), iterate overrides.
4. **Discovery convergence (small, decide-once):** GUI `registry.py`
   collects ANY top-level callable; `scidb.discover` collects
   `@scistack`-tagged only. Two palettes will drift. Proposal: GUI
   palette prefers tagged functions when any exist in a module, falls
   back to all-callables otherwise (zero-friction for untagged projects,
   signal-respecting for tagged ones).
5. **Not needed now:** GUI spec persistence (document tables suffice);
   MATLAB pipeline surfacing (post-stage-4); multi-DB pipelines (backend
   C3 forbids).

## Decisions needed / proposed

- **G1.** Pipeline-node identity = use_id (same child twice = two nodes,
  bindings live on the use edge) — matches backend variant semantics.
  → **Confirm.**
- **G2.** Execution rearchitecture per A4 (GUI builds in-session backend
  Pipelines and runs through run_until/plan instead of per-node eager
  for_each). The alternative — keep per-node execution and bolt nested
  display on top — preserves none of the pull-execution/plan benefits.
  → **Confirm.**
- **G3.** Breadcrumb = navigation path (DAG-aware), sidebar for direct
  jumps. → **Confirm.**
- **G4.** Part B priority order (endpoint presentation first after
  nesting) and the discovery-convergence proposal (B4). → **Confirm.**

## Suggested sequencing vs. stage 4 (MATLAB parity)

Stage 4 (M1–M4 also still awaiting confirmation) and this GUI stage are
independent — different layers, no shared files except tiny scidb
helpers (`Pipeline.interface()`). Recommendation: implement stage 4
first (small, already designed), then GUI Part A, then Part B item 1;
but either order works.
