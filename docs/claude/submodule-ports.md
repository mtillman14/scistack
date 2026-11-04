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
for nothing else. Worked example `pathinputA -> fcnB -> varC -> fcnD -> varE`:

| Submodule contains | Input nodes | Output nodes | Parent canvas |
|---|---|---|---|
| A | none | A | `[sub] -> B -> C -> D -> E` |
| A-B | none | C (from B) | `[sub] -> C -> D -> E` |
| A-C | none | C (from inner C) | `[sub] -> D -> E` |
| A-D | none | E | `[sub] -> E` |
| A-E | none | none | `[sub]` |
| B | A | C | `A -> [sub] -> C -> D -> E` |
| B-C | A | C | `A -> [sub] -> D -> E` |
| B-D | A | E | `A -> [sub] -> E` |
| B-E | A | none | `A -> [sub]` |
| C | C (from B) | C | `A -> B -> [sub] -> D -> E` |
| C-D | C (from B) | E | `A -> B -> [sub] -> E` |
| C-E | C (from B) | none | `A -> B -> [sub]` |
| D | C | E | `A -> B -> C -> [sub] -> E` |
| D-E | C | none | `A -> B -> C -> [sub]` |
| E | E (from D) | none | `A -> B -> C -> D -> [sub]` |

`tests/test_submodule_ports.py` runs every row. Selecting B and D without C is
refused: a path leaves the selection and returns (user decision 2026-11-03).

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

## Owners

| Concept | Owner |
|---|---|
| Port names and order | Input/output nodes and their positions, read by `ports.ports_of_scope` |
| Outside -> port -> inside | `ports.resolve_wiring`, called once from `edge_view.effective_edges` |
| What a canvas draws | `EdgeView.drawn_list` |
| What a node is wired to | `EdgeView.wiring_list` |
| One wiring per submodule | `ports.placement_conflict`, called by `put_edge` |
| A value has one owner | `execution_service._refuse_params_on_ports`, at compile |
| Which ports a selection needs | `ports.plan_boundary`, written by `scope_service.extract_to_submodule` |
| A connection's legality through ports | `ports.illegal_reason`; `put_edge` refuses, a stored one binds nothing |

With no input/output node on any canvas, `port_wiring` is `None` and
`wiring_list` is exactly `drawn_list`.

**Choosing a reader:** drawing or cycle checking uses `drawn_list`. Anything
asking what a node takes or produces uses `wiring_list`.

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
- **Removal of a port:** hide its node. `_pipeline_hidden_ports` is gone.

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

### A connection between two functions through ports

`put_edge` refuses it (`ports.illegal_reason`), which is stricter than a direct edge
between two functions, which isn't refused and binds nothing. Kept for now because a
silent no-op through a port is harder to notice.
