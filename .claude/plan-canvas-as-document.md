# Plan: the canvas is a document (history annotates, never draws)

*Drafted 2026-10-21. Status: proposal, awaiting discussion. Prompted by the
clear-data feature (`.claude/plan-clear-data.md`): "delete the data, keep the
pipeline" turned out to need a canvas freeze-and-restore, because on a project
that has run, most of the pipeline is not stored anywhere.*

## 1. Diagnosis: the split that was never made

`intent-and-fact.md` (2026-09-19) separated intent from fact for
**settings**: columns, run options, schema location, hides and constants all
became `_intent` statements. `node-identity.md` (2026-09-22) did the same for
**identity**: function nodes got allocated ids, and `_node_wiring` records which
wirings a node ran as.

Neither touched **topology**, meaning which nodes and edges exist. On every
graph build the canvas is still computed as

```
canvas = (nodes/edges synthesised from provenance)
       ∪ (manual nodes/edges)
       − (hides of history edges)
       then: graduate manual nodes onto history twins, overlay manual edges on history nodes
```

A fact (what ran) is still the default source of intent (what should
run). Every recurring GUI bug in this area comes from that union. Each one
was fixed locally, where it surfaced, so the set of rules keeps growing:

| bug class (memory / doc) | mechanism |
|---|---|
| history-edge trap (`project_edge_based_inputs`) | a run project has NO stored edges; a reader of stored edges sees nothing |
| manual edge onto a history node did nothing (`manual-edges-on-history-nodes.md`) | two input sources; needed an overlay + 4 consumers |
| hidden manual twin (`project_hidden_manual_twin`) | a stale manual edge under a hidden history edge |
| graduation orphans config (D-2026-09-22-4, still open) | manual id swaps to the DB id when history appears |
| placement-id lookup trap | positions keyed by pre/post-graduation ids |
| declared-only nodes on canvas (`project_declared_only_off_canvas`) | "exists" decided by history presence |
| 5 canvas-vs-run divergences in one day (`edge-model.md`) | per-kind ids for history vs drawn edges |
| pending constants auto-cleaned once they reach history (`auto_clean_pending_constants`) | a value moves from intent to fact and back |
| hidden PathInput minted a phantom node (`hidden-path-input-identity.md`) | hide filter vs identity ordering on the derived graph |
| clear-data needs a canvas snapshot | deleting fact deletes topology |

Measured footprint of the reconciliation machinery today (grep counts):
`graduat*` 17 source files / 148 test refs; `build_edges` 7 / 66;
`hidden_edge_ids` 7 / 40; `manual_input_overrides` 5 / 33;
`reconcile_manual_inputs` 4 / 24; `merge_manual_nodes` 6 / 16.
`api/pipeline._build_graph` alone is ~1,070 lines.

## 2. The proposal in one sentence

**The canvas is a stored document (nodes, edges, Parameter values,
PathInputs, placements). Nothing on a read path creates topology from
history. History reaches the canvas only by (a) annotation, joined by node
id, and (b) one explicit write, *adoption*, that turns an unclaimed recorded
wiring into document rows, once.**

```
              ┌──────────── intent (GUI store) ────────────┐
 user edits → │ document: nodes · edges · values · places  │ → RunPlan → for_each
              └───────────────▲────────────────────────────┘        │
                              │ adopt (one writer, idempotent)      ▼
              ┌───────────────┴──── fact (scidb) ──────────────────────┐
              │ _record · _invocation* · _run   (+ _node_wiring join)  │
              └───────────────┬────────────────────────────────────────┘
                              ▼ annotate (read-only, joined by node id)
                     run state · counts · variant cards · NOT REFLECTED
```

This is the third application of the same rule:
`intent-and-fact.md` (settings) → `node-identity.md` (ids) → this (topology).

## 3. The three roles history keeps, and the one it loses

| role | today | proposed |
|---|---|---|
| **annotation**: colour, record counts, variant cards, recorded-vs-stated divergence | joined by node id | unchanged |
| **attribution**: which node a run belongs to | `_node_wiring`, at dispatch / inferred | unchanged |
| **adoption**: a script/MATLAB-terminal run's wiring with no node | re-derived on every build, never stored | **one write**: node + edges + values persisted, association recorded |
| **default topology** | every build | **gone** |

## 4. What collapses

- **One kind of node.** No manual-vs-DB-derived node, no graduation, no
  `merge_manual_nodes`, `_find_db_fn_candidate`,
  `_wiring_conflicts_with_candidate`, `graduate_manual_node`, rekeying. The
  orphan window D-2026-09-22-4 left open closes, because there is no swap.
- **One kind of edge.** An edge is a stored row. Removing one is a hide of a
  stored row (never-delete ethos), not a hide of a re-synthesised history
  edge. Gone: history edge synthesis on read, `manual_edge_is_hidden`,
  hidden-twin handling, `drop_superseded_manual_edges`, the reconnect case,
  `manual_input_overrides` + overlay (the edges ARE the inputs).
- **One execution path.** Today a history node runs its *recorded* call sites
  with manual overrides spliced in (`reconcile_manual_inputs`), while a fresh
  node runs from its edges. Under the proposal every node runs from its
  stored edges and value statements, which is today's fresh-node path. The
  recorded wiring becomes something to compare against (the NOT REFLECTED
  chip), not something to execute.
- **Values are stated and stay stated.** A Parameter's values are statements.
  History only colours which ran. `auto_clean_pending_constants` (intent →
  fact → intent oscillation) goes away.
- **Clear data is trivial.** Delete fact plus history-class GUI tables. The
  document is untouched, by construction. `plan-clear-data.md`'s whole GUI
  half (snapshot, restore, layout sidecar rollback, `export_globals` gap)
  disappears; only its scidb half remains.
- **Export/import simplify.** `canvas_snapshot.capture` no longer has to
  resolve a derived graph to find history-only nodes; the document is the
  export. The "history-only Parameter values are lost on export" gap closes.

## 5. Adoption: the one place fact turns into intent

`services/adoption.py` (writer) over a pure `domain/adoption.plan_adoption(
recorded_wirings, document, node_wiring) -> AdoptionPlan`.

- **Input:** recorded wirings (`fn`, bindings, constants, PathInputs, outputs,
  glue chain, scope from run origin) with no `_node_wiring` claim in the
  scope. The wiring recipe moves into scidb first (architecture review §1;
  `call_id`'s forward/backward precedent), so the GUI reads a scidb shape
  instead of reconstructing one.
- **Output:** function node (allocated id), variable nodes it needs (one per
  type per scope, reuse if present), Parameter nodes + value statements,
  PathInput nodes, glue nodes, edges by `connection_id`, and the
  `_node_wiring` row. One INFO log line per adopted wiring (NOTE 2).
- **Idempotent:** keyed by `(scope, wiring_id)` in an `_adopted` marker. A node
  the user later hides is NOT re-adopted on the next build, because hiding is
  intent and wins.
- **Who triggers it:**
  - GUI runs: never needed (attributed at dispatch to the node that stated it).
  - script / MATLAB-terminal runs: on the next graph build. See Q1.
  - first open of an existing database: the same adoption over all of
    history. This is not a migration path. It is the script-run path run once
    over a database that predates it, so nothing is beta-migration-only code
    (`feedback_beta_no_deprecation`).
- **Ambiguity** follows `node-identity.md` §7b: a node that states W claims
  W; several do, oldest + popup; never auto-merge.

## 5a. One file (user requirement, 2026-10-21)

Everything lives in the project's ONE `.duckdb` file. The document tables
(`_pipeline_nodes`, `_pipeline_edges`, `_intent`, `_node_wiring`) are already
in it, and the new `_adopted` marker goes there too. The one existing sidecar,
`<name>.layout.json` (node positions; `layout.py`'s own docstring calls moving
them into DuckDB "the real fix"), moves into a `_node_position` table in a
stage of its own (Stage 0a). That also removes the layout write-race lock and the
clear-data layout-rollback problem. The only other file is the opt-in
`--backup` copy a clear makes, and only when asked.

## 6. What does NOT change

- scidb, provenance, every content id. Fact is still never edited.
- `_intent`, scopes, placement ids, `_node_wiring`, the five intent rules.
- Script runs read source only (intent-and-fact §7). The canvas is a peer
  surface. Adoption makes a script run *visible*; it never makes the
  canvas an authority over the script.
- Run-state prediction (`expected_invocations_for_function`). It now
  predicts from the document's stated wiring instead of the recorded one.
  That is what the user means anyway: "is what I drew up to date?"

## 7. Risks, stated plainly

1. **Size.** This touches the GUI's centre: graph build, run targets, the
   MATLAB command, code export, Plot Studio pickers (graph without state).
   It also touches ~150 graduation assertions and the edge-parity suite. It is
   weeks of work, not days. Staged below so each stage leaves a working GUI.
2. **Multi-call-site nodes.** A history node today groups several call sites
   (constant variant rows, Cartesian products of values). The document must
   express that as *one node with value statements*, so the Cartesian
   expansion comes from values, not from recorded rows. A recorded
   combo that is not a full Cartesian product (a one-off run) needs a
   representation: adopt it as hidden combos (`_pipeline_hidden_combos`, by
   `call_id`) or accept a wider product. This is the main design question
   left inside the proposal. Stage 1 measures how common it is on real
   databases before deciding.
3. **Behaviour change users will notice.** Deleting a step's edge no longer
   "comes back from history". Editing a script so it records a NEW wiring
   adopts a SECOND node (the old one stays, since it's intent) instead of the
   canvas silently re-shaping. Arguably correct; it must be surfaced (toast
   "1 step from a script run was added").
4. **Transition double-counting.** If adoption ships before the build stops
   deriving topology, an adopted node + its history twin must resolve to one
   node (same allocated id via `_node_wiring`, edges dedup by
   `connection_id`). Needs a test before Stage 3 lands.

## 8. Stages (each ends green, with logging + tests per NOTE 2)

0a. **Positions into DuckDB** (`_node_position (node_id, scope, x, y)`),
   `layout.py` keeps its API and stops reading/writing `.layout.json`. Clean
   break, no import of old files (`feedback_beta_no_deprecation`): positions
   in an existing `.layout.json` are lost once, and nodes re-layout.
   Approved 2026-10-21. BUILT 2026-10-21 (tables `_pipeline_node_positions`,
   `_pipeline_palette_parameters`, `_pipeline_notes`; palette + notes moved
   too, they were in the same file); pytest unrun; GUI §0zzzzzzb.
0. **Observe.** Add a per-build INFO line: nodes/edges/values that came
   from stored rows vs. derived from history, per scope. Record golden
   canvases (labels, types, connection ids, values, positions, run state)
   for the aim2 example dataset + the user's real databases' logs. These
   goldens are the regression oracle for every later stage.
1. **Wiring recipe in scidb** (architecture review §1) + forward==backward
   parity tests for `wiring_id`. Measure risk 2 (non-Cartesian recorded
   combos) on real databases from the Stage 0 logs.
2. **Adoption** (`plan_adoption` + writer + `_adopted` marker), run at build
   alongside today's derivation. Test: adopt-all, then the derived graph and the
   document graph are equal (the golden). This stage alone unblocks a clean
   **clear data** (adopt everything → delete fact), so the clear feature can
   ship here with no throwaway code.
3. **Build reads the document.** `_build_graph` loses topology derivation;
   annotations join by node id. Graduation / merge / history-edge hides
   become unreachable. The goldens must still match.
4. **Execution from the document.** Run targets, MATLAB command and code
   export from stated edges + values. `reconcile_manual_inputs`, overlays and
   history-wiring execution deleted. Edge-parity suite rewritten as
   "document → RunPlan → recorded wiring" parity.
5. **Delete the dead machinery** and its tests (graduation, merge, hide-of-
   history-edge, auto-clean pending constants, `_db_path_input_params`).
   Update `edge-model.md`, `manual-edges-on-history-nodes.md` (retire),
   `node-identity.md`, `intent-and-fact.md` §4, decisions.md ADR.
6. **MATLAB terminal runs** go through the same adoption (they are script
   runs); verify with a real MATLAB run.

## 8a. GUI ⇄ code round trip (user question, 2026-10-21)

- **GUI → code.** The GUI run and the code export render the SAME RunPlan,
  built from the document (intent-and-fact §6 already names export "the proof
  that RunPlan is complete"). Today export walks `derive_fn_targets`
  (history + overlays), a second derivation. **Gap found:** no test runs an
  exported script. New test tier: build in GUI → run in GUI → export →
  run the script → assert 0 new records, the same invocation ids, and every
  recorded wiring claimed by the existing nodes (none adopted).
- **Code → GUI.** A script run whose wiring a node already states is claimed
  by that node (`node-identity.md` §7a step 1). A new constant value on a
  claimed node is adopted as a value statement on that node's Parameter. A new
  wiring is adopted as a new node.
- **What only the GUI can say** (hides, hidden combos, glue, per-node schema
  selection): export must render each one or refuse with a reason. Stage 4
  adds a guard: every RunPlan field is consumed by both renderers.

## 8b. Undo/redo and audit (user question, 2026-10-21)

- **Undo/redo** (`history.py`, row diffs at `Handler.invoke`) keeps working
  unchanged, since topology is still rows in tracked tables. Positions become
  ordinary rows (Stage 0a) instead of the tracked `.layout.json` file write.
- **Adoption is not an undo step.** It runs during a graph build, not as a
  user action. To get rid of an adopted node, you hide it. Undoing adoption
  would revert the `_adopted` marker and re-adopt on the next build.
- **Topology audit: does not exist today.** Undo records are in memory (500
  max) and lost on restart (`undo-redo.md`). Proposed: `_canvas_change`
  (append-only, same .duckdb) written at the same choke point from the diff
  `history.recording` already computes: `(change_id, at, operation, scope,
  rows_json)`. Position-only drags coalesced. No user identity
  (`feedback_no_user_identity`). Adoption writes a row there too
  (`operation="adopt"`, with the run_id it came from). Optional follow-up:
  undo survives a restart by reading this log.
- **Run audit (GUI or not):** unchanged. `_run` (timestamp, function, where,
  origin `gui`/`script`), `_run_invocation`, `_record_save`; `scidb runs` /
  `audit`. **Clear-data interaction:** the earlier clear plan deletes `_run`.
  See Q4.

## 9. Open questions for the user

- **Q1 Adoption trigger.** (a) automatic on the next build + toast
  (recommended: script users see their steps with no extra action), or
  (b) a banner "N recorded steps aren't on the canvas — Add".
- **Q2 Non-Cartesian recorded combos** (risk 2): adopt as hidden combos
  (recommended: no widening of what runs), or widen to the product.
- **Q3 Clear data now or after Stage 2?** (a) ship the scidb half + script-only
  CLI now, GUI clear at Stage 2 (recommended), or (b) ship the
  snapshot-based GUI clear from `plan-clear-data.md` now, knowing its GUI half
  is deleted at Stage 3.
- **Q4 Clear data vs. run audit.** (a) keep `_run` rows (that a run happened,
  when, origin) and drop only their links to deleted invocations, so the run
  log survives a clear (recommended), or (b) wipe the run log too.
- **Q5 Topology audit log** `_canvas_change` (§8b): build it (recommended),
  and should undo survive a restart by reading it?

## 10. User decisions (2026-10-21, branch `canvas-as-document`)

- **Q1:** automatic adoption on the next build, with a notification.
- **Q3:** clear-data's scidb half + script-only CLI first; GUI clear at Stage 2.
- **Q4:** a clear KEEPS `_run` rows and drops only their links to deleted invocations.
- **Q5:** build `_canvas_change`, and back undo/redo with it so it survives a
  restart, as long as that stays cheap. It is cheap because a `ChangeRecord` is already
  plain before/after row diffs, and `undo` already refuses on conflict, which
  is what makes replaying an old record after a restart (or after a script
  run) safe.
- **The three canvas cases (user's framing, adopted as the spec):** (1) GUI
  used before: existing nodes keep their positions, and any recorded run not
  covered by the stored topology has its nodes + edges added; (2) GUI used
  and up to date: nothing added; (3) GUI never used: populated entirely from
  recorded runs. All three are the same adoption pass over different
  starting documents.
- **Q2:** explained in plain terms; awaiting answer.
- **Q2 (answered):** (a) a recorded non-Cartesian subset is adopted with the
  missing combinations as hidden combos (`_pipeline_hidden_combos` by call_id).
- **Positions:** clean break approved; `.layout.json` positions are lost once.
- **Q6 (open): same node rewired vs. a new step, for SCRIPT runs.** A recorded
  run has no node id (GUI runs record it at dispatch; scripts cannot).
  Options: (A) optional `step="name"` on `for_each`, recorded on `_run`; code
  export writes it and the node carries it, so the same step name means the same node
  (certain); (B) heuristic "same fn + same outputs means rewire" (wrong for
  branches, rejected); (C) default to a new node + a notification action
  "merge into <node>" that moves the wiring onto the old node and hides its
  old edges. Recommended: A, with C as the fallback for unnamed calls.

- **Design walkthrough (2026-10-22):** `docs/claude/canvas-as-document-explained.md`
  (glossary + running example + the deviations from this plan).
- **D1 (2026-10-22): no superfluous re-runs.** A node's STORED edges decide its
  wiring. The recorded rows of that same wiring supply the exact typed constants,
  column selectors and AcrossVariants pooling. "Edges + values" applies only when
  no recorded row of the stated wiring exists. Follow-up, not now: adopt recorded
  selectors (the `columns` aspect) and pooling (a new per-input statement) so that
  the fallback path also carries them after a clear-data.
- **D2 (2026-10-22): removing an edge, drawn or adopted, is a hide** (auditable
  via `_intent` + `_canvas_change`, undoable); never a hard delete.
- **D3 (2026-10-22): keep today's entity folding.** The edge model
  (`endpoint_ref` = `var:{type}` / `param:{name}` / `pi:{name}`) makes two boxes
  of one entity indistinguishable to edges, hides and runs. Boxes with their own
  meaning (a variant selector per box) are a possible future feature.
- **Q6: deferred** (user, 2026-10-22). Until it lands, a script run that records
  a new wiring for an existing step adopts a second node, with a toast.

## 11. Progress (branch `canvas-as-document`)

- **Stage 0a** committed 4cabe32a (tests pass, user 2026-10-21).
- **Stage 0** built 2026-10-21, pytest unrun: `domain/canvas_shape.py`
  (`canvas_shape`, `diff_shapes`, `canvas_sources`), per-build INFO line
  `[pipeline] canvas_sources scope=...`, `python -m scistack_gui.canvas_shape_cli
  capture|compare DB SHAPES.json` for real databases, tests
  `tests/test_canvas_shape.py` (populated_db golden: script-run canvas is 100%
  derived today).
- **Q3 scidb clear** built, pytest unrun (see plan-clear-data.md status).
- **Q5 backend** built, pytest unrun: `_canvas_change` log in history.py
  (persist on store/undo/redo, reload newest 500 on first read, `list_changes`).
  Frontend stack seeding after a restart: deferred to the Stage 2 frontend work.
- Q6 (step names): not answered; proceeding on the recommendation (A + C)
  when Stage 2 needs it.
- **Stages 1-6 BUILT 2026-10-22, pytest unrun** (user runs the suites). The
  map of where each piece lives is `docs/claude/canvas-as-document-explained.md`
  Part 8; ADR D-2026-10-22-1.
  - Stage 1: the recipe was already scidb's; added `scidb.wirings` (step shape,
    `missing_combinations`, `non_cartesian_report`) + forward==backward parity
    tests, CLI `canvas_shape_cli combos`.
  - Stage 2: adoption (`domain/adoption.py`, `services/adoption.py`, `_adopted`),
    stated values, hidden combos for non-Cartesian steps, audit event,
    notification; deferred inside undo recordings. GUI clear-data
    (`services/clear_service.py`, dialog) shipped here (Q3).
  - Stage 3: `document_view` in `_build_graph`; inputs from stored edges;
    stored-row membership in `scope_filter`.
  - Stage 4: `derive_target_for_node` from stored edges (D1);
    `derive_fn_targets` = union over stored nodes; `_scope_function_node_ids`
    reads the built canvas; document disconnection.
  - Stage 5: deleted function graduation, overlays, `drop_superseded_manual_edges`,
    `reconcile_manual_inputs`, `_apply_manual_output_types`; removal = hide (D2);
    docs updated (edge-model, node-identity §11, intent-and-fact §12,
    manual-edges-on-history-nodes retired, decisions.md).
  - Stage 6: no MATLAB-specific code (adoption is origin-agnostic); needs a
    real MATLAB terminal run (GUI §0zzzzzzc step 7).
  - Not done: §8a's "run an exported script" test tier; Q6; the D1 follow-up
    (adopt recorded selectors and pooling); Q5's frontend undo-stack seeding.

## 12. Follow-ups (2026-10-22, after v0.1.52), built, pytest unrun

1. **D1 follow-up: recorded settings survive a clear-data.** Adoption stores
   what every recorded row of a node's current wiring agrees on as the node's
   own settings (`services/adoption.recorded_node_settings`): column
   selectors (`columnSelections`) and run options `distribute`, `as_table`,
   `across_variants` (`runOptions`); never over a setting the user made; a
   disagreement is logged and left unset. scidb's `pipeline_variants` now
   reports raw `distribute` / `as_table`. The fallback run path applies
   `runOptions.across_variants` as binding pooling
   (`execution_service._apply_node_pooling`).
2. **§8a round-trip test:** `tests/test_export_round_trip.py`: canvas → GUI
   run → export → run the script in its own process → 0 new records, same
   invocation ids, nothing adopted.
3. **Q5 frontend: undo after a restart.** Changes record their surface; RPC
   `history_recent`; `seedUndo(pageUndo)` on canvas mount
   (docs/claude/undo-redo.md "Undo survives a restart").
4. **Q6: step names, option A.** `for_each(step=)` (scidb, scifor discards,
   MATLAB both), `_run_step`, variants report `steps`; GUI
   `step_names.py` (node's `step` in its config blob; export assigns names,
   GUI/compiled/MATLAB runs pass them); identity: a named run's wiring is
   claimed by the node of that step name (after an existing record);
   `IdentityPlan.rewired_by_step` + `adoption.retire_rewired_edges` hide the
   old wiring's stored edges so the node follows the script. Option C (a
   "merge into" action for UNNAMED script runs) is not built. No UI to rename
   a step yet (export assigns `fn`, `fn_2`, ...). The MATLAB *pipeline*
   command does not pass step names (single-node MATLAB runs do).
5. **New Python functions from the GUI.** `functions_dir` (config, default
   `src/scistack_functions/`, folded into discovery like `glue_dir`; shared
   `_fold_code_dir` / `_set_code_dir`); `services/function_code_service.py`
   (create / read / save with rollback, reusing glue's helpers); RPCs
   `create_function`, `get_function_code`, `save_function_code`; the
   functions tab's "+" offers New function (default) / Python library /
   MATLAB library; sidebar `FunctionCodeSection` edits a GUI-created
   function's body. Python only.
