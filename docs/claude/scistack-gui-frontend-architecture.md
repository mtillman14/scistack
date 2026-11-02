# scistack-gui frontend architecture

Written 2026-07-18, immediately after the nested-pipelines frontend
checkpoint (checkpoint 4 of `.claude/plan-gui-nested-pipelines.md`).
Companion to `archive/scistack-gui-backend-internals.md` (server side) and
`endpoint-first-pipelines.md` (the design story). Read this before
touching `scistack-gui/frontend/src` — it records the load-bearing
decisions that are easy to violate from inside a single component.

## One codebase, two hosts

There is exactly ONE React app (`frontend/src`), built twice:

| Target | Command | Output | Transport |
|---|---|---|---|
| standalone | `npm run build` (`VITE_BUILD_TARGET` default) | `scistack_gui/static/`, served by FastAPI | `fetch()` + WebSocket |
| webview | `VITE_BUILD_TARGET=webview npx vite build` (or `extension/npm run build:webview`) | `extension/dist/webview/` single bundle | `postMessage` JSON-RPC via the extension host |

**Never** import anything VS-Code-specific in components. The split
lives entirely in two modules:

- `api.ts` — `callBackend(method, params)`. In webview mode it sends
  JSON-RPC to the extension (which forwards to the Python server's
  method table in `server.py`); in standalone mode it translates the
  SAME method name through a route table into a REST call. **The route
  table's method names must match the JSON-RPC handler names in
  `server.py` exactly** — that identity is what lets components stay
  transport-blind. When the backend gains an endpoint, add it in three
  places: FastAPI router, `server.py` `_HANDLERS`, and the `api.ts`
  route table, all under one name.
- `hooks/useBackendMessage.ts` — push notifications (`run_output`,
  `run_done`, `run_progress`, `dag_updated`). Wraps WebSocket
  (standalone) / postMessage notifications (webview). Handlers must
  tolerate BOTH shapes: `msg.type` + flat fields (WebSocket) and
  `msg.method` + `msg.params` (JSON-RPC notification). Every existing
  consumer does `const t = msg.type ?? msg.method; const params =
  msg.params ?? msg`.

Error contract: `callBackend` REJECTS with `Error(message)` in both
modes — webview via RPC error mapping, standalone because `callFetch`
throws `Error(detail)` on non-ok responses (added at checkpoint 4).
Backend `ValueError`s become 400s with user-facing messages (duplicate
names, cycles, root-mutation guards, binding-key whitelist); the GUI
policy is to surface `err.message` VERBATIM (alert, or inline red
text), never to rephrase.

The extension host (`extension/src/dagPanel.ts`) forwards methods
generically; it only intercepts `restart_python`, `reveal_in_editor`,
and `start_run` when `params.language === 'matlab'`. New backend
methods therefore need NO extension changes.

Known asymmetry: `cancel_run`/`force_cancel_run` exist only as JSON-RPC
handlers — there are no REST routes for them, so cancel buttons are
webview-only in effect. Pre-existing; don't "fix" it casually from the
frontend side (the fix belongs in the API layer if ever wanted).

**This asymmetry has bitten for real once already** (found 2026-08-22):
`PathInputSettingsPanel`/`SweepSettingsPanel` called
`update_path_input`/`update_sweep`/`add_path_input_alternate`/
`remove_path_input_alternate` — present in neither `server.py`'s METHODS
table nor any FastAPI router (`scistack_gui/api/layout.py` only has
GET/POST `/path-inputs`, DELETE `/path-inputs/{name}`, POST
`/path-inputs/{node_id}/deep-copy`, GET/POST `/sweeps`, DELETE
`/sweeps/{name}` — no PUT, no `/alternates`). These were removed (or
never built) when PathInputs/Sweeps/Constants moved to being
source-scanned (commits `066cc53`, `6738212`, same day) — the frontend
panels were never updated to match, so every edit silently no-opped
(PathInput, error swallowed in `.catch(console.error)`) or visibly
errored (Sweep, shown in its error banner) instead of ever reaching
source. Symptom from the user's side: "editing a value in the GUI doesn't
change it — it stays at the default." Fixed by deleting the dead calls
and the four dead `api.ts` route entries, and rewriting both panels
read-only (matching `ConstantSettingsPanel`'s existing "source-scanned,
edit via source + Refresh Code" precedent) — see
`.claude/plan-gui-three-bug-fixes-26-08-22.md`. There's no compiler check
that would have caught this: `api.ts`'s route table, `server.py`'s
METHODS table, and the FastAPI routers are three independently-maintained
sources of truth kept in sync by convention only. When an RPC method
stops being backed on the Python side, grep the frontend for its name
before assuming the frontend copy is still safe to leave in place.

## Context providers (App.tsx nesting order)

```
RunLogProvider            run console state (Runs tab reads, run
                          starters write)
  SelectedNodeProvider    which canvas node the sidebar Node tab shows
    ScopeProvider         nested-pipeline navigation (see below)
      PlanRunProvider     pending plan-preview request
        <header/canvas/sidebar> + PipelineRunController
```

One context per concern is the house pattern — resist merging them.

### ScopeContext — the navigation model

- `currentScope` = the pipeline_id the canvas shows; derived as the
  LAST crumb of `breadcrumb`, never stored separately.
- `breadcrumb` is the navigation PATH (decision G3): a list of
  `{use_id, pipeline_id, name, binding}`. Composition is a DAG, so a
  pipeline has no unique address — the crumb records how you got there.
  Root crumb (`main`) is always element 0. Direct sidebar jumps produce
  `[root, target]` with `use_id: null` (path unknown, root kept as the
  escape hatch). Crumbs entered through a bound use carry the binding
  so the breadcrumb can display `loading (low_hz=30)`.
- `graphVersion`/`bumpGraph()` — the refetch bus for scope mutations.
  The backend does NOT broadcast `dag_updated` for scope CRUD
  (create/rename/delete pipeline, add/remove use, binding update), so
  the mutating component must call `bumpGraph()`; `PipelineDAG`'s fetch
  effect depends on `[fetchPipeline, graphVersion]` and `EditTab`'s
  pipelines list re-fetches on it too.
- `bindingSummary()` lives here — single source for the compact
  `a→b, k=v` text used by the node badge, breadcrumb, and anywhere else.

Root id is hard-coded `'main'` in the frontend (matches
`pipeline_store.ROOT_PIPELINE_ID`).

### PlanRunContext + PipelineRunController — the run funnel

Decision: EVERY pipeline-run control goes through the plan-preview
dialog (R2 gate). Controls don't run anything themselves; they call
`requestPlan({pipeline_id, mode, target?, finalized?, label})`:

- function-node right-click "Run until here" → `{currentScope,
  mode:'until', target: fnName}`
- pipeline-node ▶ Run → `{child_pipeline_id, mode:'all'}` — a
  descend-less child run; per-step `until` on a child is deliberately
  not offered (backend has no such verb across the boundary)
- canvas "Run endpoints" panel → `{currentScope, mode:'endpoints',
  finalized}` (toggle lives on the canvas panel)

`PipelineRunController` (mounted once in App) renders the dialog for
the pending request (`get_pipeline_plan`), and on confirm generates the
run_id FRONTEND-SIDE, registers it in a `useRef<Set>` BEFORE awaiting
`start_pipeline_run`, and calls `RunLog.startRun(id, label,
'pipeline')`. The ref-before-await ordering matters: output messages
can arrive before the HTTP response resolves (same reasoning as the
`runIdRef` comment in FunctionNode). The controller then routes
`run_output`/`run_done` for its run_ids into RunLogContext — the run
console is fully reused; per-node eager runs (FunctionNode) keep their
own identical listener.

`RunEntry.kind: 'function' | 'pipeline'` drives the Runs-tab
differences: pipeline cards have no source-open double-click and no
soft-cancel (cooperative cancel is a v1 no-op for pipeline runs —
`Pipeline._run` has no between-step hook), only a force-cancel ✕.

**Exception to the plan-dialog funnel:** the 👁 Show button on endpoint
nodes (plot_/stat_, tagged `data.endpoint_kind` by scidb's
`_endpoint_kind`) fires `mode:'show'` DIRECTLY — it is the everyday
"look at it" loop, deliberately ungated. Draft outputs write no records;
they arrive only on the `show_rendered` push message ({step, rendered}),
consumed by `Sidebar/EndpointPanel` (shown above FunctionSettingsPanel
for endpoint nodes: draft section + finalized manifest from
`get_endpoint_artifacts`, images via `/api/artifacts/file?path=` which is
project-dir guarded, 403 outside). The header 📄 Report button calls
`write_report` and opens the self-contained index.html through the same
file route. VS Code webview mode has no HTTP origin for image bytes —
EndpointPanel degrades to paths + provenance text there (v1).

## The canvas (PipelineDAG.tsx)

Node type registry: `variableNode`, `functionNode`, `constantNode`,
`pathInputNode`, `pipelineNode` — must match the `type` strings the
backend emits in `GET /api/pipeline`.

Scope-fetch invariants (easy to regress):

1. Graph BEFORE layout: `get_pipeline` runs graduation side effects
   that rewrite layout.json; `get_layout` must be awaited after it.
2. Both calls carry `pipeline_id: currentScope`; so does EVERY
   `put_layout` (palette drop AND drag-stop). For DB-derived nodes the
   saved position's scope IS the membership record (backend decision,
   checkpoint 2) — a `put_layout` without the scope silently teleports
   the node to the root canvas.
3. On refresh, on-screen positions win over saved ones so nodes don't
   jump — EXCEPT across a scope switch (`loadedScope` ref), where the
   previous nodes belong to another canvas and must not leak
   positions. Scope switches also re-run `fitView`.
4. `fetchPipeline` runs on EVERY `dag_updated` broadcast, not just the
   mutation that triggered it — the websocket message goes to every
   connected client, including one mid-edit in a sidebar text field. A
   node's `put_layout` (e.g. dropping a brand new node elsewhere on the
   canvas) broadcasts `dag_updated` just like any other mutation, so it
   can land while an unrelated node has an uncommitted draft sitting in
   its live-previewed `data` (fields that update the canvas on every
   keystroke via `onLiveChange` but only persist to the backend on
   blur/Enter — `useCommittedInput`'s pattern, used by
   `FunctionSettingsPanel`'s where-filter fields). Found 2026-08-22 via a
   PathInput template vanishing after a new-node drag (before that
   panel's fields became read-only — see the asymmetry note above; the
   bug generalizes to anything using this live-preview pattern, not just
   PathInputs). Fixed at the source: `ScopeContext` exposes
   `markNodeDirty`/`clearNodeDirty`/`getDirtyPatch`, a ref-backed
   per-node pending-patch registry. A field's `onLiveChange` calls
   `markNodeDirty(id, patch)` alongside its optimistic canvas update; the
   save path calls `clearNodeDirty(id)` once the backend confirms it;
   `fetchPipeline`'s node-merge re-applies any still-pending patch on top
   of the freshly fetched node, the same way it already preserves
   on-screen positions. Any new live-preview-before-save field needs to
   wire into this or it inherits the same race.

Pipeline-node identity: the React Flow node id IS the use_id (G1).
Consequences: deleting a pipelineNode calls `remove_pipeline_use`
(NOT `delete_layout`); the same child placed twice is two nodes;
the binding editor keys off `selectedNode.id` as use_id.

Drag payloads (dataTransfer keys):
- `application/scistack-node` `{nodeType, label}` — palette
  functions/variables/constants/path inputs (creates a manual node in
  the current scope).
- `application/scistack-pipeline` `{pipeline_id, name}` — sidebar
  pipelines (creates a USE via `add_pipeline_use`; cycle 400 alerted
  verbatim). Checked first in `onDrop`.

Handles: `in__{name}` / `out__{name}` on both function and pipeline
nodes; manual edges persist source/target handle ids, and the backend's
edge resolver parses them — don't change the prefix scheme.

Root-scope pixel parity: with no sub-pipelines and no navigation, the
root canvas must look like the pre-nesting GUI. That's why Breadcrumb
returns `null` when `breadcrumb.length === 1`. (The Run-endpoints panel
and sidebar Pipelines section are new-by-requirement chrome — the
parity rule as applied means: existing elements unchanged.)

## Sidebar

Tabs: Runs / Edit / Project, plus an auto-activated Node tab when a
node is selected. Each node type has a `is<X>Node` type guard and a
settings panel; `pipelineNode` → `PipelineSettingsPanel` (binding
editor: key_map/params/iterate rows; values are JSON-parsed with
raw-string fallback, so `30` → number, `[1,2]` → list, `s01` → string —
matching how the backend feeds `binding_json` into `child.bind(**b)`).

`EditTab` owns the pipelines list (`list_pipelines`), refreshed on
mount, on `graphVersion`, and on `dag_updated`. Rename must do three
things: refetch the list, `renameInPath()` (crumb labels), and
`bumpGraph()` (pipelineNode labels on canvases). Deleting the CURRENT
scope jumps to root. Root row hides ✎/× (backend would 400 anyway;
hiding is GUI-appropriate noise reduction).

## Conventions

- Inline `styles: Record<string, React.CSSProperties>` per file, dark
  chrome (`#12122a`/`#1a1a2e`/`#2a2a4a`), monospace for identifiers.
  Accent colors: function `#7b68ee`, variable `#4a90d9`, constant
  `#2a9d8f`, path input `#d97706`, **pipeline `#a21caf`** (double
  border + `⧉` glyph).
- Run-state vocabulary (renamed 2026-07-18): `green | pending | red`.
  scidb's node state is BINARY green/red; **`pending` is GUI-only** — an
  unrun constant value staged in the GUI, nothing in the database — minted
  by `domain/run_state.py`'s downgrade. It renders YELLOW (border
  `#eab308`, fill `#fefce8`), deliberately not orange, which belongs to
  path-input nodes. The plan dialog's `green | red | unknown` is a
  separate vocabulary (`unknown` = pessimistic MATLAB-step state).
  The app has no dark mode of its own — dark screenshots come from
  browser extensions inverting the light fills, so the BORDER hue is
  what must stay distinctive.
- No test runner exists for the frontend; `npm run build` (`tsc` first)
  is the type gate and verification is visual, user-run.
- The user runs all python/npm/node commands themselves — hand over
  copy-pasteable commands, never invoke them.
