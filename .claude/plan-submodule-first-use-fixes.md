# Submodule first-use fixes

From `scistack-debug.log`, session 2026-11-18 10:08–11:59 (v0.4.5+): the user
extracted `load_filter_emg` (10 nodes) and `load_filter_xsens` (8 nodes), then
hand-converted the Parameters, the PathInput and an output into ports.

Concept doc: `docs/claude/submodule-ports.md`. Owners named below come from its
Owners table.

## Inventory

| # | Symptom in the log | Count | Cause | Status |
|---|---|---|---|---|
| A | `put_edge FAILED: would join var__FilteredEMG::pipe_… to var__FilteredEMG::main through submodule ports` | 5 ERROR | `_refuse_illegal_port_wire` traces `view.drawn` of the *asking* canvas. The inner segment `manual__giohmx` (var → output node) was hidden on the submodule canvas at 11:40:16 but still counts. `edge_view.port_wiring` drops hidden segments (`live`); put_edge does not | Confirmed bug, **blocker** |
| B | `DAG propagation stalled … cycle among filterDelsys, renameFields` | 30 | Extract moved node statements (`0 statement(s) moved`) but not **edge hides**. Main had 7 hidden edge ids, the new submodule has 0, so removed history edges (`RenamedEMG → filterDelsys`, `RawEMG → renameFields`) came back. They became EachOf next to the manual edges (`loaded_data: ['RenamedEMG','RawEMG']`), which makes a cycle, so the steps go red | Confirmed bug |
| C | `[scope_filter] main: N edge(s) point at a node not on this canvas: (…, 'use_8f00…')` | 126 | False positive. `resolve_scope_view` runs before `build_pipeline_nodes` adds the placement nodes, so every edge into a placement looks dangling | Log-noise bug, which hides real dangling edges |
| D | `canvas refresh … wiped the just-placed edge manual__7ifcuq … -> server_omitted`, then `glue node 'glue_delsysEMG_config' has 3 incoming edges` | 3 redraws | The server stores the edge `runConfig → glue (on main)` but leaves it off the canvas. **Likely cause:** glue nodes have `var__glue_<fn>__<suffix>` ids, so `graph_builder.endpoint_ref` reads them as `var:glue_<fn>`. Every glue node of one glue function then shares a connection id, against its own docstring ("a glue node … is `node:{bare id}`"). The new edge on main collides with the connection of the glue node inside the submodule (the history edge drawn through it, or that edge's hide). `document_view` keeps one edge per connection (`edges_merged: 4`), and scope filtering then drops the survivor. The user redrew the edge twice, and put_edge accepted the exact duplicates | Likely cause found; confirm with logging |
| E | `placement_tunables FAILED: pipe_b4d1… is not a library pipeline` | 1 | ⚙ is shown on every placement, but `library_service._requirements` needs a library owner | Confirmed bug (parity decided) |
| F | `edge edge_b9c4… (runConfig -> use_…, port in__glue_delsysEMG_config) names a port that does not exist` | repeats | The user hid the input node. The outer edges naming that port stay forever | Small bug |
| G | Workflow: extract pulled the Parameters and PathInput **inside**. About 25 actions followed to turn them back into ports by hand | n/a | The doc's rule table: a node inside the selection is not a port | Design gap, **= GitHub #22** |
| H | Nodes and edges appear, vanish, then reappear (**GitHub #23**). After `delete_layout var__FilteredEMG` (11:40:15), refreshes #58–#60 still drew it (12 nodes, then 11 at #61). The new edge `manual__apy990` was wiped by 5 refreshes in a row before #63 drew it, 8 s later | about 35 wipe WARNs | (1) `PipelineDAG.fetchPipeline` replaces nodes and edges wholesale with each response. `CanvasRefreshLog` already knows which optimistic items are still pending, but only *logs* the wipe (`stale_snapshot`). (2) Deletes aren't tracked as pending at all, so a stale snapshot redraws a deleted node. (3) Every write starts 2–3 refreshes (`server`, `put_edge`/`put_layout`, then `db_watcher` 2 s later for the GUI's own write), and each `get_pipeline` takes 2–3.5 s, which keeps the window wide | Confirmed; the diagnostics from v0.1.54 have now settled the cause |
| I | Submodule output won't connect to an output variable, and drawing it "removes" the PathInput → glue edge (**GitHub #24**) | 5 ERROR | = **A** (the refusal makes the drawn edge vanish) + **D** (the PathInput → glue edge was never on the server's canvas). It looked removed because the next refresh, started by the output-edge attempt, dropped the optimistic copy (H) | Fixed by A + D + H |

Not caused by submodules (no action in this plan):

- Discovery skipped files (`sys.path.append()` at top level), slow imports and
  `get_age_group` shadowing: these come from the user's scripts, and the
  warnings already say how to fix them.
- `clear_data … Permission denied` (2026-11-17, build `1f73fb7-dirty`): this
  predates `4c15d44c` (Clear All Data on Windows). Re-check on the current build.
- `glue … targets port 'in__value', but … now takes ['path']`: the old glue
  edges from before #21. Under the no-migrations rule these get redrawn.
- `variant_cards(...)`, MATLAB DB lock, `renameFields`/`filterDelsys` MATLAB
  field errors, and `runConfig` discovery: unrelated.

## Stages

### Stage: put_edge sees the same live segments as the canvas (A) — BUILT 2026-11-18, tests pass

- **Owner:** `edge_view` owns "which segments are live". Factor the `live`
  filter inside `port_wiring` into one function, `edge_view.live_segments(db,
  drawn, manual_nodes)`. `port_wiring` and `layout_service._refuse_illegal_port_wire`
  both call it, so the second copy is gone.
- **Logging:** the rejection log lists each `via` edge with its hidden state
  (`manual__giohmx[hidden]`), so this class of error explains itself.
- **Test** (`scistack-gui/tests/test_submodule_ports.py`): wire var → output
  node, hide that segment, wire fn → output node, then on the parent connect
  `out__X` to a variable. Assert put_edge is accepted. Keep the current refusal
  test, where the var → port segment is live.

### Stage: extract carries edge hides with the nodes (B) — BUILT 2026-11-18

- **Owner:** `intent_store.move_edge_hides(db, src, dst, is_moved)`, called once
  by `scope_service.extract_to_submodule` after every `move_node_to_canvas`.
  It's a whole-selection step, not inside `move_node_to_canvas`, because "both
  ends moved" is only known after all the moves. It moves hide statements made
  on *src* whose two stored ends both pass extract's own `is_moved`. A hide
  with one end left behind (a crossing connection) stays.
- **Logging:** `[intent_store] moved N edge hide(s) …`, and extract's summary
  line counts "removed line(s) kept removed inside". The `run_state` stall
  warning names each stalled step's unresolved inputs and their producers.
- **Tests:** `TestRemovedLinesMoveWithExtract` (3 tests) in
  `test_submodule_ports.py`.
- **The user's DB:** no repair in code (no migrations). Remove the two stale
  edges inside `load_filter_emg`, or Undo and re-extract.

### Stage: scope_filter diagnostic knows placements (C) — BUILT 2026-11-19, tests unrun (built differently: the check moved to the finished canvas)

- Pass the scope's placement use ids into `resolve_scope_view` (they come from
  `pipeline_store.get_pipeline_uses`, already read in `graph_service`) and count
  them as kept for the dangling check only. Membership doesn't change.
- **Test (caplog):** a parent canvas with a wired placement doesn't warn. A real
  dangling edge still warns.

### Stage: the vanishing PathInput -> glue edge (D, part of #24) — DIAGNOSTICS + DEDUP BUILT 2026-11-18

**Hypothesis revised.** The glue-identity collision is real: `endpoint_ref` reads
`var__glue_<fn>__<suffix>` as `var:glue_<fn>`, against its docstring. But it does
not explain this log. `edges_merged` there only folded the user's three identical
copies into one, and the inner glue's hides are on the submodule's scope, not
`main`'s. Where the edge is lost is still unknown.

- **Built: a tracer.** `graph_service._DrawnEdgeTracer` takes every stored drawn
  edge with both ends on the canvas being built. It checks them after
  build_edges/filter_hidden/adoption, after `document_view`, and after
  `resolve_scope_view`. A lost edge gets one WARN, `drawn edge X … lost at
  <stage>: <why>`, naming either the edge it was merged into or where its ends
  resolve.
- **Built: identical edges are not stored twice.** `put_edge` returns
  `{"ok": true, "existing_edge_id": …}` when a drawn edge already joins the same
  nodes (compared by bare id) through the same handles.
- **Built: tests.** `TestIdenticalEdge` and `TestGlueOnTheParent` (the #24
  shape, with a removed inner glue of the same function). If the last one fails,
  its assertion message holds the tracer's WARN.
- **Next:** fix at the stage the tracer names, from either the failing test or
  the user's log after reproducing in the GUI. The `endpoint_ref` glue fix stays
  a separate candidate. Do it only if the tracer points at a connection merge,
  because it changes hide identity (one-off reappearing glue edges, no
  migration).

### Stage: the canvas keeps what the user just did (H = #23) — BUILT 2026-11-18 (frontend + extension tests pass; GUI pytest unrun; bundles rebuilt)

The v0.1.54 diagnostics settled the cause (memory `project_canvas_refresh_flicker`).
The fix stays in the existing owners:

- **`CanvasRefreshLog` decides what a refresh may remove.** It already holds
  `pending` adds with `confirmedAt`. Add pending **removals** (node or edge
  ids, recorded by `delete_layout`/`delete_edge` in `PipelineDAG`). A new
  method, `overlay(start, snapshot)`, returns the snapshot with three changes:
  - a pending add whose verdict is `stale_snapshot` is kept;
  - a pending removal is left out while the snapshot started before the delete
    was confirmed;
  - an item is let go as soon as a snapshot started *after* its confirmation
    (`server_omitted` keeps meaning a real server bug, and stays a WARN).

  `fetchPipeline` draws `overlay(...)` in place of `data.nodes`/`data.edges`.
  It is one function, unit-tested in `canvasRefreshLog.test.ts` from these log
  sequences (#58–#63 and #14–#17).
- **Fewer refreshes per write.** When the server emits `dag_updated`, the
  extension (`session.ts`) records the time. A watcher burst that arrives
  within `DB_WATCH_DEBOUNCE_MS` + 1 s of the server's own emit is logged as
  "own write, skipped" and doesn't refresh. External writes (MATLAB, scripts)
  still refresh. Extension test: a burst after an emit is skipped, and one
  with no emit refreshes.
- **Logging:** the refresh report also counts `kept_pending` and
  `suppressed_removed`, and names node wipes, not only edge wipes.
- **Not in this stage:** making `get_pipeline` faster (2–3.5 s here). The
  overlay makes that cost invisible, so it gets its own timing issue if it
  still matters afterwards.

### Stage: tunables for every submodule (E) (decided 2026-11-18: parity) — BUILT 2026-11-19, pytest unrun; frontend tests pass, bundles rebuilt

User decision: library and local submodules behave the same. A library is only
the shareable kind.

- **What's library-only today:** `library_service._requirements` refuses a
  pipeline without a library owner. `tunables` uses the owner only for
  `owner["library"]` (a label) and `parameter_defaults` (the "library
  default" tag). Step values, bindings (`set_step_value`,
  `promote_step_value`), Parameters and PathInputs are already per placement
  and generic. `project_tunables` skips local placements.
- **Change:** the owner becomes optional in `_requirements` and `tunables`.
  For a local submodule, `library` is `None`, there are no defaults, and
  `source` is `project`/`missing`. `project_tunables` lists every
  placement. Library-only actions stay library-only because they compare
  against a library's own schema or files: `suggest_key_map`,
  `placement_check`, `declare_requirements`, `missing_roots`. A local
  submodule shares the project's schema keys, so there is nothing to map.
- **Frontend:** `TunablesPanel` hides its library line when `library` is
  null. No ⚙ gating.
- **Logging:** `tunables %s: local submodule (no library owner)`.
- **Tests:** `placement_tunables` on an extracted submodule returns its
  Parameters and step values. `set_step_value` on it changes the binding, and
  a Python run uses it (`PipelineBinding.rewrite` is already generic).
- **Known gap, unchanged:** MATLAB runs refuse a placement `binding` for both
  kinds (`submodule-ports.md`, Known gaps).

### Stage: hiding a port hides its outer edges (F) — BUILT 2026-11-19, tests unrun

- When an input or output node is hidden (the `delete_layout` path for
  `PORT_NODE_TYPES`), hide every outer edge naming `in__/out__<label>` on that
  submodule's placements. Use the same hide (`_hide_edge_in_its_scope`), so
  Undo restores both.
- **Test:** hide the input node and assert no dangling entry in `port_wiring`.

### Stage: sources and sinks stay on the parent (G = #22) (decision 2) — BUILT 2026-11-19, tests unrun

Issue #22's rule: in the selection, a node with **no inputs** stays on the parent
canvas and feeds a new input node, and a node with **no outputs** stays on the
parent and is fed by a new output node. Both get wired automatically.

- **Owner:** `ports.plan_boundary` already makes ports for every edge that
  crosses the selection, and `scope_service.extract_to_submodule` already
  rewrites those edges into segments. So the change is one step **before**
  that: `ports.split_selection(selection, edges, manual_nodes) -> (moved,
  kept_outside)`. It drops sources and sinks from the selection, so
  `plan_boundary` sees them as outside and does the rest unchanged. One owner,
  no second port rule.
- **What counts:** a source is a node with no incoming **wiring** edge
  (`EdgeView.wiring_list`, not `drawn_list`: a glue chain's PathInput is a
  source, and its glue node is not). A sink is a node with no outgoing wiring
  edge. Functions always move (decision 2 below).
- **Edge cases:** a selection that is all sources and sinks is refused with a
  message. A source feeding two functions inside gets one input node (the port
  name is the node's label, deduped as `plan_boundary` already does).
- **Logging:** `extract_to_submodule: kept outside as sources [...] / sinks
  [...]`.
- **Tests:** a new rule-table test, `PathInput → fn → var` selected whole,
  gives an input node for the PathInput, an output node for the var, and
  neither inside. Plus the user's `load_filter_emg` shape (2 Parameters, a
  PathInput, a glue chain, 3 functions, a terminal variable). Update the doc
  table in `submodule-ports.md` (rows A-…, B-E now differ).
- **Interaction with B:** fewer nodes move, so fewer hides need moving. B is
  still needed for edges between moved nodes.

## Order

A → B → D → H → G → C → E → F.

- A and D fix #24 (and with them the user's current canvas).
- B removes the false red/cycle state.
- H fixes #23, and makes every other fix visible without the flicker.
- G is #22.
- C, E and F are cleanup.

## Tests (user runs)

`cd scistack-gui && pytest tests/test_submodule_ports.py -v` (new tests go there; one package at a time)
then the frontend tests (`canvasRefreshLog.test.ts`, PipelineNode) and the extension test for the watcher, and GUI steps added to
`docs/gui-manual-testing-todo.md`.

## Decisions

1. Tunables: full parity for local submodules (user, 2026-11-18).
2. #22 applies to entity nodes only (Parameters, PathInputs, constants,
   variables). A selected function always moves (user, 2026-11-18).
3. A variable inside connected through an output port to the same variable
   outside: keep refusing, but the message names the fix ("connect the step
   that makes VarA to the output port, or connect the port to the step that
   uses VarA"). G makes this case rare, because a sink variable now stays
   outside on its own. Pending user confirmation.

## H as built (2026-11-18)

- `canvasRefreshLog.ts`: `apply` returns `kept` (stale pending adds, no longer
  in `dropped`), `suppressed` (pending removals the snapshot could not have
  seen) and `returned` (a delete the server still draws after its confirm).
  `addRemoval`/`confirmRemoval`/`forgetRemoval`. `overlayNodes`/`overlayEdges`
  draw the snapshot with them. `dropped` now holds only `server_omitted`.
- `PipelineDAG.tsx`: fetchPipeline draws the overlay (not on a scope switch);
  node/edge deletes record removals.
- `client_canvas.py`: logs kept (INFO), suppressed (INFO), returned (WARN).
- Extension `ownWrites.ts` + `session.ts`: a watcher burst whose every change
  sits within 1 s of a server `dag_updated`/`db_written` is skipped.
  `layout_service._notify_db_written` is pushed for position-only put_layout
  (drags), which the canvas ignores.

## G as built (2026-11-19)

- `ports.split_selection(selected, edges, manual_nodes, owner_of)` +
  `entity_type` / `BOUNDARY_ENTITY_TYPES`. Counts in/out over the edges DRAWN
  on the canvas (`here`, the same list crossing is computed from), not
  `wiring_list`: whether a node has an incoming/outgoing edge is the same
  either way, and drawn ids match the selection's spellings.
- `scope_service._selection_owner` (bare or placed spelling) replaces the
  inline `is_moved`. Refusal when nothing moves.
- Port naming: `var_label_of` returns None for a non-variable manual node, so
  a PathInput -> glue port is named after the PathInput (was the glue node's
  `var__glue_…` label — the `in__glue_delsysEMG_config` port in the log).
- Found by the tests: a removed line with one end left outside kept its row
  spanning both canvases, hidden only on the parent, so the submodule read it
  again. `move_edge_hides` now copies such a hide to both canvases.
- Tests changed: SPLITS table, folded-node tests (PathInput stays), the
  hypothesis-copy test and the undo test got a node that really moves,
  TestAlreadyRunDownstream extracts the step too.

## C as built (2026-11-19)

Not "pass placements into resolve_scope_view": the dangling check moved out
of it into `scope_filter.warn_dangling_edges`, called once by graph_service
after `build_pipeline_nodes`. resolve_scope_view is also adoption's per-scope
membership computation (`adoption.visibility_by_node`), which drew no canvas
and warned anyway; passing placements would have needed the uses in both.
Tests: `TestDanglingDiagnostic` (wired placement: no WARN across two builds;
unit: a real dangling edge warns).

## E as built (2026-11-19)

- `_requirements` returns the owner or None and never raises; `placement_check`
  (and so `declare_requirements`) refuses a non-library pipeline itself;
  `parameter_users` skips ownerless rows. `tunables`: `library` None, no
  defaults, INFO `local submodule … (no library owner)`. `project_tunables`
  lists every placement.
- Frontend: `Tunables.library: string | null`; ProjectTunables hides the
  library tag when null ("No submodule is placed."). TunablesPanel never showed
  the library, so it is unchanged apart from its comment.
- Tests: `TestLocalSubmoduleTunables` (list, set a step value → binding,
  project view). The Python run reading a local placement's binding is already
  covered by `TestParamsOnPorts.test_compiling_refuses_a_value_with_two_owners`
  (it compiles a local placement's binding).
- Found by test_the_project_view_lists_nested_placements: step values were read
  over the placement's whole closure, so listing the local `analysis` showed the
  nested `emg`'s `filter_emg.gain` too (two owners for one knob). Now
  `_own_step_values`: own canvas only, for tunables and set_step_value alike.
  Parameters/PathInputs still come from the closure (project-level, one owner:
  the registry).

## F as built (2026-11-19)

`layout_service.delete_layout` reads the node's row first; for a port node it
calls `_hide_port_outer_edges`, which finds the outer edges through the view
(`effective_edges(db, None).drawn_list`, no new raw read) and hides each with
`_hide_edge_in_its_scope`. Tests: `TestRemovingAPortNode` (input and output;
the other port's edge stays; Undo brings the edge back). Existing dangling
edges (port removed before this) are not migrated (beta rule): re-add the input
node with the same name, then remove it again.
