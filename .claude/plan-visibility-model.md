# Plan: one visibility model across canvases (hypotheses, submodules, ports)

**Status 2026-11-19: all stages built, tests unrun (user runs them).** D2 and D3
confirmed by the user. Deviations from the text below:
- Combos stay global (corrected in the table).
- The per-canvas getters keep their name, require a canvas (default `main`,
  `None` raises); `list_hidden_parameter_values` stays a listing.
- Stage 3's all-readers parity test became the owner guard
  (`test_visibility_owner_guard.py`): every reader asks one owner, so they
  cannot disagree; plus integration tests for the XSENS shape
  (`TestHiddenCopyInsideTheSubmodule`, MATLAB `test_running_the_submodule_ignores_...`).
- Readers that judge exactly one canvas (graph build, combo filters, ports of a
  scope) keep a per-canvas set, now from `Visibility`; per-node readers use the
  node's own canvas.

Supersedes the narrow fix in `plan-submodule-run-hidden-node-scope.md` (its error-message,
check-order and test items are folded in as Stage "Readers" and Stage "Messages").

## The class of bug

A hide is stored as `(scope, subject_ref)` in `_intent` (aspect `hidden`), but:

1. **The ref is ambiguous.** Entity nodes (variable, Parameter, PathInput) appear on many
   canvases. A hide may be stored bare (`var__FilteredXSENS`) or placed
   (`var__FilteredXSENS::main`). Edge ends use either spelling.
2. **Edges have no canvas.** Manual edges are GLOBAL statements. Which canvas an edge is
   on is guessed by each reader from its ends.
3. **So every reader picks "whose hides apply" itself.** Today there are five answers:
   - the canvas *asking* (`effective_edges(scope)`), matched with a `strip_placement`
     fallback, so a hide inside a submodule cuts main's lines (today's XSENS bug);
   - *every* canvas unioned (`get_hidden_node_ids(db, None)`: graph_service
     `hidden_everywhere`, `trace_through_ports`, `get_hidden_edge_ids(None)`), so a node
     hidden in hypothesis A loses its claim in hypothesis B;
   - the function node's canvas, for Parameter value hides
     (`_hidden_constant_values(scope_of_node(fn))`), so unticking a value on the main
     Parameter does nothing when the step runs inside a submodule;
   - global, for hidden combos, which are also unioned INTO hidden node ids;
   - connection ids built from bare ids, so two canvases' edges share a hide id (the
     glue-id bug fixed in 308c4253).

Each fix so far patched one reader. The concept "is this thing hidden?" has ~15 owners.

## The model (one paragraph)

**Everything you can see is an *occurrence* on exactly one canvas, and a hide belongs to
the occurrence, on that canvas.** A reader never says which canvas's hides to use. It
asks about an occurrence, and the occurrence knows its canvas. Hypothesis canvases are
independent because their occurrences are different. A submodule's inside is one canvas
shared by every placement, so its hides apply to every placement. That's the one
deliberate sharing, as you described. Lines into and out of a placement's ports are
occurrences on the *parent* canvas.

### Rules

| Thing | Its canvas | Hidden when |
|---|---|---|
| Function / glue / port / Merge node | its `_pipeline_nodes.pipeline_id` row (ids are unique) | `(canvas, bare id)` hidden |
| Entity node end (`var__`, `param__`, `pathInput__`) | the `::P` suffix; with no suffix, the canvas of the other end of the edge | `(canvas, bare id)` hidden |
| Placement `use_…` | `_pipeline_uses.parent_pipeline_id` | `(parent, use_id)` hidden |
| Edge (stored or history-derived) | the canvas of its ends; they must agree (one segment, one canvas) | `(canvas, connection id)` hidden, OR either end hidden on that canvas |
| Connection traced through ports | none; it is made of segments | any segment is not live |
| Parameter value | the canvas of the Parameter occurrence that feeds the argument (after tracing ports) | `(that canvas, name, value)` hidden |
| Hidden combo (call site) | global by design (a call_id is one computation everywhere; the run path relies on it) | always; still part of every canvas's hidden node ids, as before |

What the table leaves out on purpose:
- There's no "asking scope" for structural questions.
- There's no `None` (every-canvas) union.
- There's no `strip_placement` fallback.

The asking canvas still decides *which steps run* (membership). It never decides *what is hidden*.

### Existing data: no migration

Stored hides are already `(scope, ref)`. They're read as `(scope, strip_placement(ref))`,
so old rows keep meaning what the user did on the canvas where they did it. Edge-hide
connection ids gain the canvas as a key, not inside the id, so stored ids still match.
Nothing is rewritten and nothing is dropped.

## One owner: `scistack_gui/domain/visibility.py`

```python
vis = Visibility.load(db)          # every hide, every canvas, once per build/run
vis.canvas_of_end(end, edge)       # THE occurrence rule (table above)
vis.canvas_of_edge(edge)           # ends agree, else WARN "edge spans canvases" + None
vis.node_hidden(canvas, node_id)   # exact (canvas, bare) membership
vis.edge_live(edge)                # not hidden on its canvas, neither end hidden there
vis.hidden_values(canvas, param)   # Parameter value hides of one occurrence
vis.hidden_on(canvas)              # listing for the "show hidden" UI / restore
```

- Only `visibility.py` and `intent_store` touch `ASPECT_HIDDEN`. An owner guard test
  pins that (an owner guard, not an import pin).
- Writers (`hide_node`, `hide_edge`, `hide_parameter_values`) stay in intent_store. They
  refuse a placed id whose `::P` disagrees with the canvas passed and log a WARN. That
  stops new ambiguous rows at the source.
- Logging: one INFO per load (`[visibility] N hides on M canvases`). DEBUG per dropped
  edge, naming the occurrence and canvas. DEBUG when an end's canvas came from the
  other end. WARN for an edge whose ends disagree.

## Hierarchy semantics (decisions to confirm)

- **D1. Hypothesis canvases are fully independent.** Duplicating a hypothesis copies its
  hides at copy time (`scope_service._clone_nodes`, unchanged). After that, nothing is shared.
- **D2. Inside a submodule is shared by every placement.** There are no per-placement
  hides of inner nodes. To make one placement differ, Duplicate the submodule (the
  existing button). This is the 80/20 choice.
- **D3. Removing a port** (hiding an input/output node inside the submodule) removes it
  for every placement. Outer lines into it are shown as missing-port lines on each parent
  (dashed, bind nothing, selectable to delete). This is the same as removed-argument
  edges. They're never hidden silently.
- **D4. Placement-only state stays on the parent:** the outer lines, the placement node,
  and its bindings/tunables. That's already true.

## Stages (one commit each; you run the suites between)

1. **Owner + matrix tests.** Add `visibility.py` with the rules and logging; no reader
   changes yet. Add a table-driven case file `visibility-cases.json`: hide on
   {main, hypothesis B, submodule inside} × occurrence on {main, B, sub, outer segment,
   traced connection, Parameter value}. Unit tests over the cases.
2. **edge_view.** `effective_edges` and `trace_through_ports` use `vis.edge_live`.
   `severed` is derived from segments the same way. This fixes the XSENS run and
   adds its regression test (extract with source and sink left outside, run the
   submodule directly: MATLAB command has both steps, Python run produces the output).
3. **Readers.** graph_service (`hidden_everywhere`, `filter_hidden`, scope_filter),
   execution_service (`hidden_by_scope`, derive_target_for_node), matlab_command_service,
   code_export_service, run_service, scope_service, layout_service: each takes `vis`.
   Integration parity test: one project (main + hypothesis + submodule placed twice), and
   every reader agrees on the live set for each canvas.
4. **Parameter value hides by source occurrence.** `_hidden_constant_values` asks
   `vis.hidden_values` for the Parameter that actually feeds each argument after port tracing.
5. **Remove the old forms.** `get_hidden_node_ids(db, None)`, `get_hidden_edge_ids(db,
   None)`, and the `scope` parameter on structural readers go away. Combos get their own
   reader. Owner guard test.
6. **Messages.** When every step is dropped, the "no runnable step" errors (MATLAB and
   Python) list each step's reason. Check `open_arguments` before
   `document_disconnected_ports`.
7. **Docs.** Add `docs/claude/visibility-model.md` and index it. Point `edge-model.md` and
   `canvas-as-document-explained.md` §Hide at it. Archive
   `plan-submodule-run-hidden-node-scope.md`.

## Not in this plan
- Stale EMG lines into the renamed port `in__glue_delsysEMG_config` (a7e97fee). Under D3
  they show as missing-port lines. Whether to move them automatically is a separate decision.
- Per-placement overrides of inner hides (D2 says no).
