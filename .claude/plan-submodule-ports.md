# Submodule ports: terminology and framework

Status: implemented on the working tree 2026-11-03, uncommitted. **Per-placement wiring was reversed the same day: a submodule has one wiring, and Duplicate covers different connections. Sections below that describe per-placement behaviour are history; `docs/claude/submodule-ports.md` is current.**
Scope: `scistack-gui` document model, run compiler, canvas. No scidb schema change.

## Problem

Today a connection crossing a submodule boundary is one stored edge whose ends
are in different scopes, plus a second, display-only edge from the kept node to
a port on the placed submodule. Ports are inferred from variable types.

- Extract and `scope_filter.document_interface` decide ports separately, so
  they can disagree and React Flow silently drops the line (C alone, E alone,
  B and D without C).
- PathInput and Parameter crossings get no port and no line, and the surviving
  edge can't be seen or removed on any canvas.
- An extracted submodule (crossing edges) and a hand-built one (inner variable
  node matched to an outer one by name) are two representations of one concept.
- The crossing edge breaks the rule in the `pipeline_store` docstring ("both
  endpoints are always in one scope"), and `canvas_snapshot` captures only
  same-scope edges, so an export loses the crossing wiring.
- A single edge can't vary by placement, but one child can be placed twice.
- `docs/claude/portability.md` mentions "port bindings" on
  `_pipeline_uses.binding`. Those don't exist: the allowed keys are `key_map`,
  `params`, `iterate`.

## Terminology

| Term | Meaning |
|---|---|
| Submodule | A pipeline's definition: the inner canvas. One per child pipeline. |
| Placement | One use of a submodule on a parent canvas (`_pipeline_uses` row). |
| Input node / output node | Generic node on the inner canvas (Simulink Inport/Outport). Owns a port's name and order. |
| Port | The handle on the placement's edge. Exactly one per input/output node. |
| Inner edge | Edge on the inner canvas, including to or from an input/output node. |
| Outer edge | Edge on the parent canvas, to or from a port. |
| Resolved connection | The end-to-end wire after tracing through ports, e.g. `B -> C`. What runs and identity see. |
| Open port | A port with no outer edge on this placement. |

## Rule

An input/output node exists for each connection crossing the boundary, and for
nothing else. A node's position inside or outside decides which.

## Worked example: `pathinputA -> fcnB -> varC -> fcnD -> varE`

| Submodule contains | Input nodes | Output nodes | Parent canvas |
|---|---|---|---|
| A | none | A | `[sub] -> B -> C -> D -> E` |
| A-B | none | C (from B) | `[sub] -> C -> D -> E` |
| A-C | none | C (from inner C) | `[sub] -> D -> E` |
| A-D | none | E (from D) | `[sub] -> E` |
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

A-D no longer shows the stray `C` port: C doesn't cross the boundary.
Exposing an internal value is a deliberate act (add an output node).

## Rules for ports

- **Names:** auto-named after the crossing node (variable type, PathInput or
  Parameter name). For a function's output, use the variable it feeds. Collisions get a suffix.
- **Fan-out:** one outside source feeding several inner nodes is one port. One
  inner source feeding several outside nodes is one port.
- **Order:** owned by the input/output node. The number is the position; nothing stores it twice.
- **Manual:** the user inserts input/output nodes on a submodule's inner canvas.
  Same rows as extract creates. Refused on a canvas that is never placed (`main`).
- **Removal:** hide per project rule; replaces `_pipeline_hidden_ports`.
- **Legality:** a resolved connection must be legal as if drawn directly. Direct
  `sub1.out -> sub2.in` where both inner ends are functions resolves to fn -> fn:
  refused, with a message asking for a variable between them.
- **Open input port:** steps it feeds are disconnected (red, not runnable), same as a hidden input.
- **Open output port from a bare function:** that function has no output variable, so it isn't runnable.
- **Parameter values:** `binding.params` sets values for Parameters inside the
  submodule. An argument fed by a port is not a `params` target (refused with a
  message), so a value has one owner.

## Storage: same tables, same identity

Recommended form: **segmented**. One crossing is:

- one row in `_pipeline_nodes` (the input/output node), and
- one ordinary inner edge and one ordinary outer edge in the existing edge rows.

No new tables or columns for connections. Every edge's ends are in one scope, so
the `pipeline_store` docstring becomes true. A flat edge can't represent two
placements with different wiring, or a library with no outside.

This changes the earlier "edges wouldn't change" idea: extract rewrites each
crossing edge into two. Run, identity and history only ever see the resolved form.

| Identity | With vs without submodule |
|---|---|
| `graph_builder.connection_id` of a resolved connection | Identical (placement suffixes already ignored) |
| Computation identity: function + inputs + code + settings; skip check | Identical; nothing re-runs |
| `canvas_snapshot.signature` / library `definition_hash` | Differs, correctly: it describes the canvas, not the computation |

## Ownership

| Concept | Owner | Consumers |
|---|---|---|
| A submodule's ports (names, order) | input/output nodes via new `domain/ports.py` | `build_pipeline_nodes`, canvas, library review |
| Outside -> port -> inside tracing | `domain/ports.resolve_connections` | applied once in `edge_view.effective_edges` |
| Drawn vs resolved edges | `edge_view`: `drawn` per canvas, new `resolved` | graph build, run targets, MATLAB command, reports, plot service |
| Selection -> ports | `extract_to_submodule`, using `ports` to create them | none |

`document_interface`'s type inference, the display-only edge, and the variable-only
label lookup in extract are retired. `tests/test_edge_view_guard.py` keeps new
readers from bypassing the view.

Ports are a canvas concept, so tracing lives in the GUI domain layer (NOTE 3).
scidb only ever receives compiled `for_each` calls with concrete variables.

## Placed twice

Each placement has its own outer edges, so each can be wired differently.
`build_backend_pipeline` compiles the child's steps per placement, with ports
substituted. Same resolved inputs = one computation (existing dedup); different
inputs = separate variants, like differing `binding.params`.

## Work

- `ports_domain`: `domain/ports.py`, node types, naming, resolution, legality.
- `edge_view_resolved`: add `resolved` to `EdgeView`; move run/identity readers to it.
- `history_dedup`: recorded `B -> C` matches resolved `B -> C` by `connection_id`,
  so history and adoption don't redraw it. Highest risk: read `services/adoption.py` first.
- `extract_rewrite`: extract creates input/output nodes and segmented edges in its transaction.
- `compile_per_placement`: `build_backend_pipeline` and the MATLAB command per placement.
- `export`: confirm closure export is self-contained; open ports survive the round trip.
- `canvas_ui`: input/output node components, insert action, port order and names.
  Rebuild the webview bundle.
- `retire_old`: remove inference, display-only edges, `_pipeline_hidden_ports`.
- `docs`: correct `portability.md`, add `docs/claude/submodule-ports.md`, update
  `docs/gui/process.ui.md` and `docs/gui-manual-testing-todo.md`.

No migration (beta): existing submodules need re-extracting.

## Logging (NOTE 2)

- INFO per `effective_edges` call: ports traced, resolved connections, open ports.
- WARN for an illegal or dangling trace, naming the placement, port and edge.
- INFO at extract: nodes moved, ports created, edges rewritten.

## Tests (user runs them)

- Parametrized over every row of the worked example: the expected ports, every
  outer edge has a real port, and resolved connections equal the original DAG.
- `connection_id` and run targets identical with and without the submodule.
- Run with no submodule, extract, re-plan: everything is skipped as already computed.
- Parameter crossing behaves like PathInput crossing.
- One child placed twice: same wiring dedups, different wiring gives two variants.
- Manual submodule with the same structure produces the same rows as extract.
- Undo restores the exact rows. Export/import round trip keeps ports.
- Illegal fn -> fn through ports refused. Open ports disconnected, with a log line.
- Port-fed argument refused as a `binding.params` target.

## Findings from reading adoption and run targets (2026-11-03)

- Adoption does not redraw crossing wires: `plan_adoption` stores only edges a
  canvas draws, and a history edge with ends in two scopes is drawn on none.
  No adoption change is needed, but a test must pin it.
- `adoption.retire_rewired_edges` hides any stored edge on the node whose
  connection isn't in the new wiring. A port edge's own connection never is,
  so compare resolved connections and leave port edges alone.
- Hiding a segment must hide the resolved connection. `edge_view` adds the
  traced connection ids of hidden segments to `hidden_edge_ids`, so deleting
  an outer edge really disconnects a node that has recorded history.
- `derive_target_for_node` is the one place a node's run wiring is read, so it
  takes the placement. `build_backend_pipeline`'s `_built` memo keys on
  (child, resolved wiring), not child alone, because `binding` and wiring must
  stay paired: one compile per placement wiring avoids a params x wiring cross product.
- Nested submodules: a context is a path of placements, not one placement.
- Extract leaves an already-hidden crossing edge as it is.

## Decided (2026-11-03)

- Segmented storage (Option 2): extract rewrites each crossing edge into an
  inner and an outer edge, replacing "edges wouldn't change".
- Per-placement wiring: first yes, then REVERSED. One wiring per submodule; use Duplicate.
- Non-contiguous selection (B and D without C): refuse with a clear message.
  Extract owns this check; test it, and log the refused selection.
