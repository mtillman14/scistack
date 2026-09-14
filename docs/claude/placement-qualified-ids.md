# Placement-qualified ids vs. bare ids: which layer sees which

*Written 2026-09-14, after the "Schema Level / Distribute checkboxes snap back"
investigation. Concerns `scistack-gui`. Companion to
`docs/claude/gui-run-options-flow.md` and `.claude/plan-placement-qualified-node-ids.md`.*

## The two id shapes

A DB-derived canvas node has a **bare canonical id** that names real, shared
data:

| Kind | Bare id |
|---|---|
| function call site (one wiring) | `fn__{fn}__{wiring_id}` |
| variable type | `var__{Type}` |
| Parameter | `param__{name}` |
| PathInput | `pathInput__{name}` |

The same wiring can be independently *placed* on more than one pipeline scope
(root `main`, or a hypothesis `pipe_{hex}`), e.g. a duplicated hypothesis that
re-runs identical wiring. Each placement gets a **placement-qualified id**:

```
{bare_id}::{pipeline_id}          e.g. fn__loadGaitRiteOneFile__cccfc8d46e3ddc62::main
```

Helpers, all in `scistack_gui/domain/graph_builder.py`:
`placement_id`, `parse_placement_id`, `strip_placement`, `PLACEMENT_SEP`.
`::` never appears in a pipeline_id or a label, so the split is unambiguous.

A node is qualified when `layout.json` holds its position under the qualified
key (graduation writes it that way; dragging an existing DB-derived node onto a
sub-canvas can still write a *bare* key — both are legal, see
`scope_filter._resolve_in_scope`).

## The pipeline: where the id changes shape

`api/pipeline._build_graph` builds **one** scope-agnostic graph and filters it
down to the requested scope afterwards. The id a node carries depends on where
in that pipeline you are standing:

```
 graph_builder.build_*_nodes           ids are BARE      ← config/state lookups here
 graph_builder.build_edges             ids are BARE        only ever see bare ids
 merge_manual_nodes / graduation
        │
        ▼
 scope_filter.resolve_scope_view       id REWRITTEN to the qualified form
   `{**n, "id": resolved}`             (data dict is shared, not copied)
        │
        ▼
 apply_placement_configs               ← the only place that can look up by
 endpoint tagging                         the id the FRONTEND will see
        │
        ▼
 GET /api/pipeline → React Flow        ids are QUALIFIED (when placed that way)
```

Everything downstream of the frontend uses the **qualified** id, because that
is all the frontend has:

- `FunctionSettingsPanel.updateNodeData` → `put_node_config(node_id=…::main)`
- single-node Run → `execution_service` `get_node_config(db, node_id)` with the
  frontend's id
- pipeline Run → `execution_service._scope_function_node_ids` yields the
  **position key**, i.e. the qualified id for a qualified placement
- hidden nodes/edges/ports, layout positions — all keyed the same way

## The trap

Any lookup keyed by node id that runs **inside `build_*_nodes`** uses the bare
id and will silently miss a row the frontend saved under the qualified id. The
write succeeds, the read returns nothing, no error anywhere. It surfaces as a
UI setting that "doesn't save" — actually it saves and never comes back.

This is exactly what happened with `_node_config` on 2026-09-14:

1. `update_node_config(node_id='fn__…__cccfc8d46e3ddc62::main', keys=['schemaLevel'])` — written.
2. The VS Code extension's DuckDB file watcher (`extension.ts` `setupDbWatcher`,
   2 s debounce) fired `dag_updated` because `_node_config` lives in the
   DuckDB file.
3. `build_function_nodes` looked up `node_configs.get('fn__…__cccfc8d46e3ddc62')` — miss.
4. `resolve_scope_view` renamed the node to `…::main` without touching `data`.
5. The refetched node had no `schemaLevel`/`runOptions`; the checkbox snapped
   back ~7 s after the click.

Secondary effect: the frontend builds the save payload from `node.data`, so once
a refetch had stripped the keys, the next toggle saved *only* the changed key
(`keys=[]` even appeared) and wiped earlier settings from the DB.

## The rule

**Look up per-node persisted state by the id the frontend will see, i.e. after
`resolve_scope_view`, or by both shapes.** Concretely:

- Config: `graph_builder.apply_placement_configs(nodes, node_configs)` runs in
  `_build_graph` right after `resolve_scope_view`. Qualified config wins; the
  bare-id result from `build_function_nodes` is the fallback (a node placed
  bare then graduated keeps its old config until re-saved). A config keyed for
  *another* scope's placement is neither applied nor flagged.
- Anything new that is keyed by node id (a future per-node note, pin, colour…)
  must either follow the same two-pass pattern or be applied post-resolution.
- If a consumer only ever wants the bare id, call `strip_placement` **first**
  before any `startswith("fn__")`-style parsing.

## Diagnostic

`apply_placement_configs` WARNs when a `_node_config` row's id matches no node
in the resolved graph **by exact or bare id**:

```
[graph_builder] N saved node config(s) match no node in the resolved graph (by exact or bare id): [...]
```

That is the signature of an id-shape mismatch (or of a hidden/removed wiring —
read the id). If the node whose checkbox misbehaves is in that list, the toggle
cannot stick; if it is *not* in the list and still misbehaves, the problem is
elsewhere (frontend merge, dirty-patch guard, watcher timing).

Other lines to follow in `scidb.log`:

- `[pipeline_store] update_node_config (node_id=…, keys=[…])` — what was
  written and under which id. `keys=[]` means the panel had nothing in
  `node.data` to send — rehydration is already broken.
- `[pipeline] rehydrated placement-qualified config on N node(s)` — the second
  pass did something.
- `RPC << get_pipeline` ~2 s after a config write with no `put_layout` between
  — that is the DuckDB watcher, not a user action.

## Known soft spot (not fixed)

A toggle made while a watcher-triggered `get_pipeline` (~5 s on a real
project) is already in flight can be reverted on screen when that fetch lands,
because the fetch read the DB before the toggle's write. The write is durable
and the *next* watcher refetch restores it, so this is a transient flicker, not
data loss. If it becomes annoying, `updateNodeData` could `markNodeDirty` until
the first refetch after the save completes (see `ScopeContext`'s dirty-patch
doc) rather than clearing on save success.

## Tests

`scistack-gui/tests/test_graph_builder.py::TestPlacementQualifiedConfigRehydration`
— the bare/qualified precedence, the other-scope exclusion, and the orphan WARN
(both that it fires and that present-node keys don't trip it).
