# Prompt: Nested-pipeline frontend (checkpoint 4)

Implement the frontend checkpoint of the nested-pipelines feature in
`scistack-gui/frontend/` (React + Vite, React Flow canvas; also mind the
VS Code webview build under `scistack-gui/extension/` if it shares the
frontend — check before assuming).

## Read first

1. `.claude/plan-gui-nested-pipelines.md` — the approved plan (G1–G4) and
   the as-built records for backend checkpoints 1–3 (all verified and
   committed; the backend contract below is live and pytest-covered in
   `scistack-gui/tests/test_pipeline_scopes.py`).
2. `docs/claude/endpoint-first-pipelines.md` — the design story.
3. `docs/claude/scistack-gui-backend-internals.md` — server architecture
   (NOTE: predates the nesting work; trust the plan file over it where
   they disagree).

## What to build

**1. Scope-aware canvas.** The graph fetch is now
`GET /api/pipeline?pipeline_id=<id>` (default `main`, the root). Add
frontend state: `currentScope` (pipeline_id) and `breadcrumb` (the
navigation PATH — a list of `{use_id, pipeline_id, name, binding}` —
composition is a DAG, so the crumb is the path taken, not an address;
decision G3). All node/edge queries, layout reads (`GET
/api/layout?pipeline_id=`), and position writes (`PUT /api/layout/{id}`
body now takes `pipeline_id`) must send the current scope. Dropping a
palette node onto the canvas creates it IN the current scope.

**2. `pipelineNode` component.** The graph response includes nodes with
`type: "pipelineNode"`; their `data` is `{label, child_pipeline_id,
binding, inputs, outputs}` — `inputs`/`outputs` are variable-type name
lists and are the node's connection ports (render like a function node's
handles). Non-empty `binding` ({key_map/params/iterate}) renders as a
compact badge (like variant badges). Double-click descends: push the
crumb, set `currentScope = child_pipeline_id`, refetch. The same child
placed twice is two nodes with different `use_id`s (decision G1 — the
node's React Flow id IS the use_id).

**3. Navigation chrome.** Breadcrumb bar (`main ▸ loading (low_hz=30) ▸
filters`; clicking a crumb ascends to that scope) + a sidebar section
listing all pipelines from `GET /api/pipelines` (`{pipelines: [{
pipeline_id, name}], uses: [...]}`) for direct jumps; current scope
highlighted. Create/rename/delete via `POST /api/pipelines {name}`,
`PUT /api/pipelines/{pid} {name}`, `DELETE /api/pipelines/{pid}` — the
backend 400s with a clear message on duplicates, root mutations, deleting
a still-used pipeline, and cycles; surface those messages verbatim.

**4. Placing a pipeline node.** Dragging a pipeline from the sidebar onto
the canvas → `POST /api/pipelines/{currentScope}/uses
{child_pipeline_id, binding?, x, y}` → returns `{use_id}`; refetch.
Removing one → `DELETE /api/pipeline-uses/{use_id}`. Editing its binding
(a small form: key_map entries, params entries, iterate entries) →
`PUT /api/pipeline-uses/{use_id}/binding {binding}` (unknown keys 400).

**5. Plan-preview dialog + run controls (R2/G2).**
- `GET /api/pipelines/{pid}/plan?target=<step>` returns ordered
  `[{step, pipeline, endpoint, state: green|red|unknown, n_combos}]` —
  render as the pre-run dialog (step list, owner pipeline, green/red
  chips, combo counts; endpoint rows visually distinct), with Run/Cancel.
- Run buttons → `POST /api/pipelines/{pid}/run {mode, target?, finalized?,
  skip_computed?}` → `{run_id}`; progress arrives on the EXISTING
  WebSocket messages (`run_output`, `run_done`, `dag_updated`) — reuse
  the current run console. Wire three controls: function-node context
  menu "Run until here" (mode=until, target=fn name), pipeline-node "Run"
  (descend-less run of the child: mode=until per its steps is NOT
  available — use mode=all on the CHILD pipeline_id), and a canvas-level
  "Run endpoints" (mode=endpoints, with a draft/finalized toggle).
  Cooperative cancel is a no-op for pipeline runs (v1); force-cancel
  works — disable the soft-cancel button for pipeline runs.

## Constraints

- Backend is DONE — do not modify Python except for genuine contract bugs
  you can demonstrate; if one appears, fix it in the owning layer and add
  a pytest.
- Root-scope behavior with zero sub-pipelines must be pixel-identical to
  today's GUI (everything defaults to `main`).
- Keep frontend code in the existing style (check neighboring components
  for state management + fetch patterns before introducing anything new).
- Verification is visual: hand the user concrete steps (which buttons to
  click, what they should see) plus `npm run build` / dev-server
  commands. The user runs everything themselves — never invoke python,
  pytest, npm, or node; hand over copy-pasteable commands.
- Update `.claude/plan-gui-nested-pipelines.md` checkpoint-4 status and
  the auto-memory when done.
