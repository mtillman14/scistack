# Submodule ports

*Written 2026-11-03. Plan: `.claude/plan-submodule-ports.md`. Read this before
touching extract-to-submodule, a placement's ports, or how a node inside a
submodule gets its wiring.*

## Terms

| Term | Meaning |
|---|---|
| Submodule | A pipeline's definition: the inner canvas |
| Placement | One use of a submodule on a parent canvas (`_pipeline_uses` row) |
| Input / output node | Generic node on the inner canvas (`inputPortNode`, `outputPortNode`). Owns a port's name; its height on the canvas is the port's order, top to bottom |
| Port | The handle on a placement: `in__<name>`, `out__<name>`. One per input/output node |
| Inner / outer edge | An ordinary edge on the inner / parent canvas |
| Resolved connection | The end-to-end wire after tracing through ports. What runs and identity see |
| Open port | A port with no outer edge on any placement |

## The rule

An input or output node exists for each connection crossing the boundary, and
for nothing else.

**Sources and sinks stay outside** (GitHub #22, 2026-11-18). A selected *data*
node (variable, Parameter, PathInput) that nothing feeds on the canvas is a
source; one that feeds nothing is a sink. Extract leaves both on the parent, so
their edges cross the boundary and get ports, wired automatically. Steps
(functions, merges, placements) always move. Owner:
`ports.split_selection`, run before `plan_boundary`, so there is still one port
rule. A selection of only sources and sinks is refused. To put a PathInput
inside on purpose (a library that loads its own data), move it in afterwards.

**Glue stays outside unless it sits inside** (user decision 2026-11-19). A glue
node is a situation-specific adapter that makes code from different origins fit,
which a reusable submodule should not carry. A selected glue moves only when
everything feeding it and everything it feeds move. This is repeated until nothing
changes, so a glue chain off an outside source stays out whole.
`PathInput -> glue -> fn` keeps the glue beside the PathInput, feeding the new
input port. `fn -> var -> glue -> fn` moves it, because the first step always
needs it to feed the second. An explicitly selected glue the rule keeps out stays
out. It is listed as `glue` in the extract's `kept outside` INFO line
(`SelectionSplit.glue`). Existing submodules keep their glue: move it out by
hand.

Worked example `pathinputA -> fcnB -> varC -> fcnD -> varE`: A is a source and
E a sink, so selecting them changes nothing.

| Submodule contains | Input nodes | Output nodes | Parent canvas |
|---|---|---|---|
| A, E or A+E only | refused | | |
| B (or A-B) | A | C | `A -> [sub] -> C -> D -> E` |
| B-C (or A-C) | A | C | `A -> [sub] -> D -> E` |
| B-D (or A-D) | A | E | `A -> [sub] -> E` |
| B-E (or A-E) | A | E | `A -> [sub] -> E` |
| C | C (from B) | C | `A -> B -> [sub] -> D -> E` |
| C-D (or C-E) | C (from B) | E | `A -> B -> [sub] -> E` |
| D (or D-E) | C | E | `A -> B -> C -> [sub] -> E` |

`tests/test_submodule_ports.py` runs every row (`SPLITS`, `TestSourcesAndSinks`).
Selecting B and D without C is refused: a path leaves the selection and
returns (user decision 2026-11-03).

A port is named after the variable crossing, else the outside node (a
PathInput or Parameter). An input port fed by a glue node is named after the
**argument** it feeds (`config`, not `glue_delsysEMG_config`): the argument is
what any reuser supplies. A glue node's id starts `var__`, but it is never
read as a variable.

## Storage: segments, read as one

`B -> C` crossing the boundary is stored as:

```
B -> [output node "VarC"]      inner edge, on the submodule's canvas
[placement].out__VarC -> C     outer edge, on the parent's canvas
```

Ordinary rows, no new tables or columns. Both ends of every edge are on one
canvas. Extract rewrites a crossing edge into this form (user decision
2026-11-03), and a manually built submodule stores the same rows.

A single flat edge can't work because a library has no outside to point at, a
manually inserted port may have nothing connected yet, and extracted and manual
submodules would be stored differently.

## One wiring per submodule

A node states one wiring, so a submodule has one (user decision 2026-11-03,
reversing per-placement wiring the same day: it broke that assumption and
cost placement paths, per-placement compiles and display workarounds).

- A second placement can't connect a port to something different. `put_edge`
  refuses, and the message points at ⎘ Duplicate (`ports.placement_conflict`).
- Ends are compared by what they are (`endpoint_ref`), so a duplicated hypothesis's
  own node for the same variable matches.
- A placement with nothing connected conflicts with nothing. A port's connections
  are the union of its placements'.
- Placements still differ by `binding` (`params`, `key_map`, `iterate`).
- Different inputs: ⎘ Duplicate on the placement (`duplicate_placement`). The copy
  replaces the submodule on that placement in place, keeping its use id, position,
  binding and outer edges. It has fresh node ids and is independent, so later edits
  to one don't reach the other.
- Removing a line on one placement doesn't disconnect the steps while another
  placement still holds that connection.

## Identity

| Identity | With vs without submodule |
|---|---|
| `graph_builder.connection_id` of a resolved connection | Identical |
| `wiring_id`, computation identity, skip check | Identical; nothing re-runs |
| `canvas_snapshot.signature`, library `definition_hash` | Differs: they describe the canvas |

**What a connection id is made of.** `connection_id` is
`e__{source ref}__{target ref}__{port}`, where `graph_builder.endpoint_ref` gives each
end's ref. The ref says **what** the node is, not which copy of it on the canvas: a
function is `fn:{name}:{token}`, and a variable is `var:{type}`, the same whether the
node came from history, was placed by hand, or has a placement suffix. Parameters,
PathInputs and Merges are keyed by their declared name. Hides, duplicate checks, adoption
and severed port connections all match on this id, so two edges with the same id are one
wire. **A glue node is keyed by its own id** (`node:{bare id}`), never by its function.
Two glue nodes running one function are two different sources. A glue id
(`var__glue_x__abc`) parses like a variable's, so until 2026-11-19 every `glue_x` shared
one id. Removing the line into a submodule's `glue_x` severed `runConfig -> [port] ->
glue_x`, and that id also hid a fresh `runConfig -> glue_x` line drawn on `main`
(scistack-debug.log 2026-11-19, `TestGlueOnTheParent`). `edge_view` adds severed ids
to `hidden` after it has built `drawn`, so such a clash shows up as a drawn edge that
the build leaves out. It now logs a WARN (`shares its connection id … severed at a
removed port`).
The one-wiring check across placements (`ports.placement_conflict`) asks a different
question: is this the same **source**? There, a glue node counts as its function
(`glue:{label}`, `ports._same_source_ref`), because a duplicated hypothesis tab holds its
own copy of the glue, and that copy feeds its placement just as well.

## Owners

| Concept | Owner |
|---|---|
| Port names and order | Input/output nodes and their positions, read by `ports.ports_of_scope` |
| Outside -> port -> inside | `ports.resolve_wiring`, called once from `edge_view.effective_edges` |
| What a canvas draws | `EdgeView.drawn_list` |
| What a node is wired to | `EdgeView.wiring_list` |
| One wiring per submodule | `ports.placement_conflict`, called by `put_edge` |
| A value has one owner | `execution_service._refuse_params_on_ports`, at compile |
| What an extract moves (sources, sinks and outside glue stay) | `ports.split_selection`, before `plan_boundary` |
| Which ports a selection needs | `ports.plan_boundary`, written by `scope_service.extract_to_submodule` |
| Moving a node to another canvas | `layout.move_node_to_canvas`: row, position, edges, statements |
| A removed line between two moved nodes | `intent_store.move_edge_hides`, called once by `extract_to_submodule` after the moves |
| A step into a submodule | `context/descendInto.ts`, called by `ScopeProvider.descend` |
| Whether a canvas response is drawn | `CanvasRefreshLog.whyDiscard`, called by `PipelineDAG.fetchPipeline` |
| A connection's legality through ports | `ports.illegal_reason`; `put_edge` refuses, a stored one binds nothing |
| Which segments a trace sees (a removed segment is out, whichever canvas asks) | `edge_view.trace_through_ports`, used by the canvas (`_trace_ports`) and `put_edge` |
| What to do instead of an illegal connection | `ports.illegal_remedy`, shown in `put_edge`'s message |

With no input/output node on any canvas, `port_wiring` is `None` and
`wiring_list` is exactly `drawn_list`.

**Choosing a reader:** drawing or cycle checking uses `drawn_list`. Anything
asking what a node takes or produces uses `wiring_list`.

## Moving a node: three ids can disagree

A node that was folded into its entity has its parts under different spellings:

| Part | Where | Example |
|---|---|---|
| Row (`_pipeline_nodes`) | Bare id, one per node across all canvases | `var__X`, `pipeline_id = main` |
| Position | Placed id, one per canvas | `var__X::main` |
| Statements (`_intent`) | Bare id, plus a `scope` column | `var__X`, `scope = main` |

So the canvas's id doesn't find the row. An update under `var__X::main` matches
nothing, without any error, and `_resolve_in_scope` falls back to the row and keeps
drawing the node on the old canvas.

Statements apply on the canvas they were stated on (`intent_store.scope_of_node`),
so a node's settings must move with it or they stop reaching it. Other canvases' own
statements and legacy `global` ones stay where they are.

A duplicated hypothesis draws `var__X::hyp` from the original's row, so the bare row
moves only if it is on the source canvas. Extracting from the copy leaves the original
alone. Don't fix this in `_resolve_in_scope`: its row fallback is what keeps that
original visible.

Use `layout.move_node_to_canvas`, not the separate row and position calls.

**Removed lines move too.** A hide of an edge is a statement on the canvas it was
made on. When both ends of a removed line are extracted, the hide moves to the
submodule (`intent_store.move_edge_hides`); otherwise a recorded edge the user
removed comes back inside, beside the drawn one, and the EachOf reads as a cycle
(2026-11-18: filterDelsys and renameFields went red). A hide with one end left
behind is copied, so it applies on both canvases: the removed line keeps its
stored row, which now spans the two, and without the copy the submodule's
wiring read it again (2026-11-19, surfaced by a sink left outside, #22).
This is a whole-selection step, not part of `move_node_to_canvas`, because "both
ends moved" is only known once every node has moved.

## Moving between canvases on screen

The canvas replaces its nodes with each response, and those can come back out of order.

- **Which responses are drawn:** not one for a canvas the user has left, and not one
  older than what's on screen. A response that is merely superseded is still drawn: the
  newer one may fail, and steady `db_watcher` traffic could otherwise starve the canvas.
- **While the new canvas loads:** nodes are cleared on a scope switch, so nothing from
  the old canvas can be dragged or clicked. Every write already names the new scope, and
  a position written there moves the node.
- **Breadcrumb:** a pipeline already on the path goes back to its crumb, because nesting
  can't repeat (`add_pipeline_use` refuses a cycle).

## Behaviour

- **Removing a line:** a segment lives on one canvas, so its hide applies
  whichever canvas is asking. Its traced connection joins `hidden_edge_ids`, so a
  node with recorded history really disconnects. Redrawing the line restores it.
- **Open input:** steps fed by it have no targets (log: "fed by a submodule port
  with nothing connected").
- **`binding.params` on a port-fed argument:** compiling refuses it, so every run,
  plan preview and Python export reports it, however the binding was written.
- **Open output from a bare function:** no output variable, so no targets.
- **Adoption:** a recorded `B -> C` with ends on two canvases is drawn on none, so
  adoption never stores it. `retire_rewired_edges` compares resolved connections,
  so a named script run can't hide a segment.
- **Removal of a port:** hide its node. `_pipeline_hidden_ports` is gone. Its
  outer edges on every placement are hidden with it
  (`layout_service._hide_port_outer_edges`, the edge-removal hide, so Undo
  restores both; 2026-11-19, stage F).

- **Tunables (⚙):** every placement, library or local (user decision 2026-11-18:
  parity; a library is only the shareable kind). `library_service.tunables` has
  `library: None` and no library defaults for a local one. Still library-only,
  because they compare against a library's own files or schema:
  `placement_check`, `declare_requirements`, `suggest_key_map`. Step values are
  the ones on the placement's own canvas (`_own_step_values`): a submodule
  nested inside it is its own placement and owns its own. A MATLAB run
  still refuses any placement `binding` (below).

## Known gaps

### MATLAB pipeline runs refuse a placement's `binding`

A `binding` is one placement's settings for its submodule: `params` overrides
constants, `key_map` renames schema keys, `iterate` overrides looped values. Python
applies them by making adapted copies of the steps (`PipelineBinding.rewrite`).

The generated MATLAB script includes submodule steps but flattens them into one
`scidb.Pipeline`, so it has no use edge to carry a binding. Rather than ignore one
and run with the wrong values:

- a run refuses, naming the placement (`matlab_command_service.unapplied_bindings`);
- a MATLAB `</>` code export warns, since a script can be read and edited.

**`scidb.Pipeline.m` is not the blocker.** It already has `bind` and `uses`, tested
for `params` in `TestPipelineRegistry.m`. Read from the code, not run:

| Key | To support it | Why |
|---|---|---|
| `params` | Generate one `scidb.Pipeline` per submodule, joined by `.use(child.bind(...))`. Resolve `{"parameter": name}` values first (`library_service.resolved_binding_params`) | `replay_matlab_step` already applies the descriptor's `constant_inputs` |
| `iterate` | The same generator change | Replay already applies the descriptor's `metadata_iterables` |
| `key_map` | Also rewrite the step's stored settings on replay | Python renames keys throughout its copy of a step. MATLAB receives only renamed iterables and path templates, then replays its stored `where`, `schema_filter`, `schema_keys`, `share_limits` and `locations` under the OLD names. Fixing this means sending those across the bridge and rebuilding MATLAB objects, or duplicating the rewrite in MATLAB (a second owner) |

`Pipeline.m`'s header documents the `where` case as a known limit.

**Order:** `params` and `iterate` first, keeping the refusal for `key_map`. Needs the
MATLAB test suite, which only the user can run.

### Existing databases (no migration)

A submodule made before ports keeps running through its old crossing edges, which
`resolve_wiring` reads as direct, but shows no ports. Extract it again, or add ports
by hand. A repair action was considered and not built, under the no-migrations rule.

### One wiring: costs accepted

- Different inputs per placement need ⎘ Duplicate, so later edits to one don't reach
  the other. A library placed twice with different inputs needs forking.
- Removing a line on one placement doesn't disconnect the steps while another
  placement holds that connection.

### A variable through a port to a variable

`VarA` inside -> output node -> placement -> a `VarA` node on the parent is refused
("two variables need a function between them"), with a message naming the fix:
connect the step that makes VarA to the output node, or connect the port to the
step that uses VarA (user decision 2026-11-18; a pass-through was considered and
not built). Until 2026-11-18 put_edge traced the parent's view, which still held a
segment removed inside the submodule, so it refused even after the user rewired
the inside correctly (GitHub #24). Both readers now trace through
`edge_view.trace_through_ports`.

### A connection between two functions through ports

`put_edge` refuses it (`ports.illegal_reason`), which is stricter than a direct edge
between two functions, which isn't refused and binds nothing. Kept for now because a
silent no-op through a port is harder to notice.
