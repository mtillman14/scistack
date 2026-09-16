# Plan: carry node config through graduation

Date: 2026-09-15. Follow-up (2) from
`plan-manual-input-edges-on-history-nodes.md`. Symptom in the user's log:

```
WARN [graph_builder] 3 saved node config(s) match no node in the resolved graph
     (by exact or bare id): ['fn__calculateSymmetryOneVector__9ya0wl',
     'fn__grSides__5c9r0r', 'fn__loadDemographics__ypth9f']
     -- a setting saved under one of these ids will not rehydrate
```

Those are settings (Schema Level, filters, run options, column picks) the
user made on three fresh nodes that later graduated into their history
nodes. The settings were never carried across, so they now point at ids
that no longer exist on the canvas.

## What graduation does today

`api/pipeline._build_graph` decides a fresh node is the same call site as a
history node (`merge_manual_nodes` + the two wiring passes) and calls
`layout_store.graduate_manual_node(old_id, new_id)`, which:

1. moves the saved **position** from `old_id` to `new_id` in every scope
   (`layout.py:622`);
2. `pipeline_store.graduate_manual_node`: **deletes** the `_pipeline_nodes`
   row (which also drops its legacy `config` column) and rewrites manual
   **edge** endpoints `old_id → new_id`.

Nothing touches `_node_config` — the table the panel writes every setting
to (`update_node_config`, keyed by the canvas id the panel saw). The row
under the fresh node's id is left behind, orphaned. `new_id` is
placement-qualified (`placement_id(canonical, scope)` → `fn__x__wid::main`);
fresh-node config keys are usually bare (`fn__grSides__5c9r0r`), sometimes
qualified.

Both graduation routes (bare fresh node → single candidate; wired fresh
node → history node with the same wiring after its first run) end in the
same call, so one fix covers both.

## Decisions

- **Config is a third thing graduation must move**, beside position and
  edges — same owner, `pipeline_store.graduate_manual_node`, not a new
  side-effect list in `_build_graph`.
- **The fresh node's settings win** (decided 2026-09-15 after review; the
  first draft said the target keeps its keys). The realistic route to a
  conflict is a wired fresh node the user configured and RAN — its
  settings produced the very history it graduates into — so after
  graduation the node must run the way it just ran, not the way some
  earlier session configured its twin. A conflict only exists when the
  history node was configured before; that user saw it on the canvas
  while building a duplicate, which is unusual on purpose. The panel
  saves the whole config object on every change, so "per key" is in
  practice "fresh wins outright"; the overwritten target values are
  logged verbatim at INFO (never silently gone).
- **The old row is renamed, not left behind.** It is a move: the content
  lives on under `new_id`. Leaving the source row would keep the orphan
  WARN firing forever for a setting that *did* rehydrate.
- **Key shape on the target:** write under `new_id` as graduation names it
  (placement-qualified). That is the id positions are written under, hence
  the id `resolve_scope_view` gives the node and the id the panel will save
  under next. The API test below pins that the setting actually shows on
  the graduated node's `data`, so if the shape assumption is wrong the test
  says so rather than the user.

## Stages

### 1. `pipeline_store.migrate_node_config(db, old_id, new_id) -> dict`

Collects every config for the old node — `_node_config` rows whose bare id
is `strip_placement(old_id)` (bare or `::scope`), plus the legacy
`_pipeline_nodes.config` column — merges them (qualified over bare over
legacy), then writes `{**target_config, **old_config}` under `new_id` — the
fresh node's keys win. Deletes the migrated `_node_config` row(s) for the
old id. Returns `{"moved": [...keys], "replaced": {key: previous_value}}`.
INFO line: `graduation: config keys [...] moved from <old> to <new>`, plus
`replaced <new>'s previous values {...}` when the target had any; DEBUG
when there was nothing to move.

Called from `pipeline_store.graduate_manual_node` **before** the
`_pipeline_nodes` delete (the legacy column must still be readable).

### 2. `_build_graph` uses the result for this response

`build_function_nodes` and `apply_placement_configs` already ran on the
pre-graduation `node_configs` snapshot when graduations execute
(`api/pipeline.py:~1085`), so the first response after a graduation would
still show defaults. Same pattern as the in-memory edge patch right below
that call: after each `graduate_manual_node`, re-read `get_node_config(db,
new_id)` and `_apply_saved_config` it onto the graduated node's `data`
(look the node up by exact id, then bare). One-line log.

### 3. Orphan warning points at the fix

`apply_placement_configs`' WARN gains one sentence: an orphan whose bare id
parses as `fn__{fn}__{6 chars}` is a fresh-node id from before this fix;
the config can be re-applied by hand on the graduated node. No automatic
adoption of the three existing orphans: their manual nodes are gone, so
the only possible target is "the one history node with that function
name", and guessing a target for settings that change what a run does is
not something to do silently. (If you want them adopted, the same
single-candidate rule graduation uses can be applied once, behind a
`scidb` CLI mutate command — separate decision.)

### 4. Tests (scistack-gui, user-run)

- `test_pipeline_store.py` (or wherever `update_node_config` is tested):
  `migrate_node_config` — bare source; `::scope` source; legacy column
  source; target already has the same key (fresh wins, previous value in the log); source row
  gone afterwards; nothing to move is a no-op.
- `test_api.py` / `test_pipeline_scopes.py`: PUT a fresh `bandpass_filter`
  node, PUT its config (`schemaLevel`, `columnSelections`), GET
  `/api/pipeline` → node graduated; the graduated node's `data` carries
  the setting **in that same response**; `get_node_configs` has no row
  under the fresh id; no "match no node" WARN in caplog. Second test:
  target already configured → the fresh node's value replaces it and the
  previous value appears in caplog at INFO. Third: sub-scope graduation (`test_graduation_preserves_sub_
  scope_membership` shape) keeps the setting on the sub canvas.
- Existing graduation/collision tests unchanged (collision losers are
  demoted, never graduated, so nothing to migrate).

### 5. Docs

- `docs/claude/gui-run-options-flow.md`: add "graduation moves the row"
  to the config-storage section and the log line to look for.
- `docs/claude/manual-edges-on-history-nodes.md`: strike the "not migrated
  on graduation" limitation.
- Memory: update `project_manual_edges_on_history_nodes` open items.

## Out of scope

- Adopting the three pre-existing orphans (see stage 3).
- `delete_node` (user deletes a fresh node): its config row stays orphaned
  by design — "never delete, mark hidden" — and the WARN keeps naming it.
  If that noise matters, the orphan check can skip ids of nodes that were
  explicitly deleted; separate small change.
