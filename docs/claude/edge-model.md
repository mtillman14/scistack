# The edge model: one id, one view, one port per argument

*Written 2026-10-01 after the unified-edge-model refactor (branch
`refactor/unified-edge-model`, D-2026-10-01-1, plan
`.claude/plan-unified-edge-model.md`). Read this before touching how edges are
drawn, hidden, bound to inputs, or matched against history.*

## Why it changed

On 2026-10-01 alone, five canvas-vs-run divergences each had to be patched
with a node-kind-specific rule. They came from three causes:

1. **Per-kind edge ids.** History edges had four formats, one per kind, while
   drawn edges had random ids. "Is this drawn edge the same wire as that history
   edge?" was answered by rebuilding history ids per kind.
2. **Two port names for one argument.** A Parameter-fed argument was `param__X`
   once the node had run with it, and `in__X` before.
3. **Every reader filtered raw rows itself.** The canvas, the run, the
   disconnected check and the MATLAB command each read the raw stored edges.

## The model

### One id per connection: `graph_builder.connection_id`

```
connection_id(source, target, target_handle, manual_nodes) = e__{src}__{tgt}__{argument}
```

`endpoint_ref` canonicalises each end:

| endpoint | ref |
|---|---|
| function node | `fn:{name}:{token}` |
| variable (DB-derived or hand-placed) | `var:{type}` |
| Parameter | `param:{declared name}` |
| PathInput | `pi:{declared name}` |
| anything else (glue, unknown) | `node:{bare id}` |

A hand-placed node is read through its stored label, and placement suffixes
(`::scope`) are ignored. The argument is the `in__` port; an output edge has
none.

- **History edges** (`build_edges`, all four kinds) take it as their id.
- **A drawn edge** over the same connection computes the same id from its own
  endpoints (`edge_connection_id`), whatever kind they are.
- **Candidates** for "is this call site's input hidden?" come from one function,
  `inbound_edge_candidates_by_handle`. It names a Parameter by its DECLARATION
  (the recorded `parameter_names`), not the argument.

Two different sources on one port are two connections, so hiding PathInput A
never hides a drawn PathInput B. One variable feeding two arguments is two
connections as well.

### Hides match by connection: `pipeline_store._hidden_connection_id`

- **Stored with endpoints and a port** (every GUI input-edge hide; the frontend
  has sent both since 2026-08-09): it matches the connection of those.
- **Otherwise** (an API delete without a port, or an output edge): it matches
  the saved id, which is a connection id for anything hidden since step 3.

`get_hidden_edge_ids(db, scope)` returns these connection ids. `unhide_connection`
removes every hide over a connection. `put_edge` uses it to restore a hidden
edge when the same connection is redrawn; with no port given, it restores the
single hidden connection between the two nodes.

### One view every reader uses: `domain/edge_view.effective_edges(db, scope)`

It returns the drawn edges the canvas shows (visible, not on a retired port),
the scope's hidden connection ids, and the manual nodes. Every reader goes
through it:

| reader | scope |
|---|---|
| graph build | that canvas |
| node identity | all scopes |
| run targets (no node named) | all scopes |
| run targets (node named), "why can't this run", MATLAB single command | the clicked node's scope (`run_scope`) |
| pipeline-run report, MATLAB pipeline command | that pipeline |
| submodule interface | the submodule |

The visibility rule (`visible_manual_edges`) is applied there and NOWHERE else. The
binding functions (`manual_edge_handle_index`, `edge_resolver.resolve_function_edges`,
`infer_manual_fn_output_types`, `infer_manual_fn_param_to_class`) take the view's edges and filter
nothing. Functions that ask whether a HISTORY edge is hidden take `hidden_edge_ids`, because history
edges are rebuilt from the database and are not in the view's drawn list.

`tests/test_edge_view_guard.py` fails if a new reader reads stored edges or
hidden ids directly. Mutators (`put_edge`, `delete_edge`,
`extract_to_submodule`, `read_layout`) are on its allow-list, each with a reason.

### One port per argument: `in__X`

Whatever feeds an argument (variable, Parameter, PathInput, glue), its port is
`in__X`. `param__X` was retired. A stored edge on it is dropped by `edge_view`
(WARN once per edge id): redraw it.

## What is still kind-specific, on purpose

Binding semantics, in `edge_resolver` / `manual_input_overrides` /
`manual_path_input_overrides`:
- two variables on a port make an EachOf;
- a Parameter fans out over its values;
- a drawn PathInput replaces the recorded one;
- a glue node chains.

Edge existence, visibility and identity are not kind-specific.

## Safety nets

- `tests/test_edge_parity.py`: the canvas, the run and the disconnected check
  agree on a node's inputs across today's bug scenarios, including a hypothesis
  tab.
- `tests/test_edge_view_guard.py`: no raw stored-edge reader outside the view.
- `TestConnectionId` (`test_graph_builder.py`): every kind round-trips through
  `build_edges`.

## Clean-break effects (beta rule: no migration)

- An output edge hidden before step 3 (no port, old-format id) reappears once.
- A stored drawn edge on `param__X` is not drawn and binds nothing. Redraw it
  onto `in__X`.
- A hide on a `param__X` port reappears once.
