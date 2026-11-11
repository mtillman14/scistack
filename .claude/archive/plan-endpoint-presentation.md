# Plan: Endpoint-first presentation (Part B item 1)

Status: IMPLEMENTED 2026-07-18 (awaiting user test run + visual check).
As-built deltas: show keeps skip_computed=True from the GUI button — after
a finalized run the draft may skip everything and render nothing; the
panel's "No outputs rendered" message covers it and the Recorded section
shows the finalized artifacts anyway (revisit if it annoys). show-mode
"requires target" is validated sync in start_pipeline_run; a processing
target 400s asynchronously via pipe.show's ValueError in the run console.
gui_test_data.py gained stat_vo2_summary + VO2Summary for the visual demo
(wire MaxVO2 -> stat node manually; no plot_ demo — PathOutput wiring has
no GUI affordance yet).

Verification fix 2026-07-19 (user-found): a freshly dragged endpoint node
had no badge/Show button until a run existed — the drop path builds node
data from get_function_full_info, which lacked endpoint_kind (the graph
post-pass only tags on refetch). get_function_full_info now returns
endpoint_kind (scidb._endpoint_kind) and onDrop forwards it. Regression:
TestFunctionInfoEndpointKind. Everything else user-verified working:
badge/panel after run, finalized records, report tab.

Second verification fix 2026-07-19 (user-found): delete + re-drop + rewire
of an already-computed node stayed RED — the DB state check for manual
nodes exists (_own_state_for_function during _build_graph) and re-drop
unhide already worked, but NOTHING triggered a rebuild on wiring changes;
the canvas kept the frontend-local red until the next run's dag_updated.
layout_service now broadcasts dag_updated on node create/delete and edge
create/delete (shared seam — both transports), deliberately NOT on
position-only writes (a rebuild per drag would be pathological). Frontend
onDrop guards against appending a duplicate node if the refetch wins the
race. Tests: TestWiringMutationBroadcasts (test_api.py). Backend groundwork all exists: `_endpoint_kind`
(scidb.foreach), `Pipeline.endpoints()/show()` (stage 3),
`inspector.report(fn=)` → FigureEntry/StatEntry manifests with stamp
verification (endpoints-viz-stats work), `db.inspect.write_report(dir)`.

## Backend (scistack_gui)

- `services/endpoint_service.py` (new):
  - `endpoint_artifacts(db, fn_name)` — `db.inspect.report(fn=fn_name)`
    dataclasses → JSON-safe dict {figures, stats, warnings}.
  - `artifact_file_path(db, path)` — resolve + guard: must live under the
    PROJECT DIR (db path parent, resolved); ValueError otherwise (→ 403).
  - `write_report(db)` — `db.inspect.write_report(<db_dir>/scidb_report)`;
    returns index.html path (embed=True → self-contained single file).
- `api/artifacts.py` (new router):
  - `GET /api/endpoints/{fn_name}/artifacts`
  - `GET /api/artifacts/file?path=` → FileResponse (403 outside project)
  - `POST /api/report` → {"index_path": ...}
- Execution: `run_pipeline`/`start_pipeline_run` gain mode="show"
  (requires target; `pipe.show(target)` — draft, zero DB writes); the run
  thread pushes `{"type": "show_rendered", run_id, step, rendered: [...]}`
  before run_done so the preview panel can display draft outputs (drafts
  have NO records, the pushed paths/payloads are the only handle).
- Graph: functionNode data gains `endpoint_kind` ("plot"|"stat") via a
  post-pass in _build_graph using scidb's `_endpoint_kind` (owning layer).
- JSON-RPC: get_endpoint_artifacts, write_report handlers.

## Frontend

- FunctionNode: endpoint kind badge (`◫ plot` cyan #0891b2 / `Σ stat`
  violet #6d28d9); endpoint nodes get a split Run/👁 Show button — Show
  fires mode="show" directly (no plan dialog: it's the everyday
  look-at-it loop), reusing the node's own run console wiring
  (kind='pipeline' card → force-cancel only).
- `Sidebar/EndpointPanel.tsx` (new): rendered INSIDE the Node tab above
  FunctionSettingsPanel when the selected fn is an endpoint. Sections:
  - Draft results: listens for `show_rendered` messages for this fn —
    plot: <img src="/api/artifacts/file?path=..">; stat: payload table.
  - Recorded artifacts: `get_endpoint_artifacts` — figures with stamp
    provenance caption (schema, branch_params, stamp_ok / STALE warning,
    missing-file warning); stats as key/value tables.
  - VS Code webview mode: no HTTP origin for images — show paths +
    provenance text only (v1; asWebviewUri plumbing deferred).
- App header: 📄 Report button → write_report → standalone: open
  index.html via the artifacts file route in a new tab; VS Code: alert
  with the path.

## Tests (scistack-gui/tests/test_endpoints_gui.py, new)

- endpoint_kind present on plot_/stat_ nodes in /api/pipeline.
- artifacts endpoint shape for a finalized stat_ run; empty for unknown fn.
- /api/artifacts/file: serves a file inside the project dir; 403 outside.
- run mode "show": validation (requires target; non-endpoint target 400
  from pipe.show's ValueError), and a real show run renders a stat draft
  (rendered payload list non-empty, no new records).
- POST /api/report returns an existing index.html.

## Out of scope (v1)

Endpoints rail (inverted card view); artifact preview in the VS Code
webview (needs asWebviewUri); MATLAB endpoint steps in show().
