# How run options reach a run (GUI -> Python, GUI -> MATLAB)

*Written 2026-09-14, from the `distribute=true` investigation recorded in
`.claude/plan-matlab-run-options-distribute.md`. Concerns `scistack-gui`, with
consequences in `scimatlab` and `scifor`.*

## What a run option is

`for_each` takes four flags the user can set per function node:

| Option | Meaning | Identity-bearing |
|---|---|---|
| `dry_run` | Enumerate combos, call nothing, save nothing | no |
| `save` | Write the results to the database | no |
| `distribute` | Split each returned value across the next-deeper schema key | **yes** |
| `as_table` | Pass named inputs as whole tables rather than per-combo values | **yes** |

"Identity-bearing" is the part that makes these more than presentation.
`distribute` and `as_table` are both folded into the `invocation_id` and the
`call_id` (`docs/claude/graph-database-state.md` §Identity,
`scidb.foreach_config.call_id_from_version_keys`). Flipping either one does not
modify a run — it names a *different* run. Any code that derives or filters
targets must therefore use the same values the run will actually use, or it
computes a call_id for a call nobody makes. This is why
`filter_hidden_targets` takes them as explicit arguments.

## The two sources, and why there are two

The GUI stores run options in exactly one place — the node's saved config — but
**reads them from two**, depending on what is being run.

```
                    ┌─────────────────────────┐
  checkbox toggled  │  FunctionSettingsPanel  │
  ──────────────────▶  toggleRunOption        │
                    └───────────┬─────────────┘
                                │ updateNodeData({runOptions})
                   ┌────────────┴────────────┐
                   ▼                         ▼
      React Flow node.data          put_node_config(node_id, config)
      (live, this session)          -> _node_config table (durable)
                   │                         │
                   │                         │
        SINGLE-NODE RUN              PIPELINE RUN / rehydration
                   │                         │
                   ▼                         ▼
   FunctionNode.tsx:183            execution_service.py:777-789
   run_options: data.runOptions    node_config["runOptions"]
                   │                         │
                   └────────────┬────────────┘
                                ▼
                        POST /api/run
```

A single-node Run reads the **live canvas node data**, so a toggle applies
immediately without a round trip. A pipeline run reads the **stored config**,
because it runs nodes the user never selected and whose React state the backend
cannot see. Rehydration after a `dag_updated` also comes from the stored config.

The consequence to remember: **these two can disagree.** If the write half
fails, the panel keeps showing a toggle that a single-node run honours and a
pipeline run does not. That is not hypothetical — it is precisely the state the
2026-09-14 investigation found (see "Failure modes" below).

## Where the config is stored

`_node_config(node_id, config)` — its own table, written by
`pipeline_store.update_node_config` as an **upsert**, keyed by the full node id.

It is deliberately *not* a column on `_pipeline_nodes`. That table holds
manually-placed nodes, and `merge_manual_nodes` graduates its rows into
DB-derived nodes as functions acquire history. Storing config there means either
(a) config exists only for nodes that have never run, or (b) writing config for
a graduated node manufactures a manual-node row that `merge_manual_nodes` then
treats as a second node. A separate table keyed by node id has neither problem
and no lifecycle coupling at all.

`_pipeline_nodes.config` is still **read** as a fallback, so configs saved
before the split are not orphaned. Nothing writes it any more.

Node ids come in **three** shapes and all must work as keys:

- `fn__{fn_name}` — the legacy/manual form, a node placed but never run.
- `fn__{fn_name}__{call_id}` — the composite (bare canonical) form for a node
  with history.
- `fn__{fn_name}__{call_id}::{scope}` — the **placement-qualified** form, which
  is what the canvas actually shows (and therefore what the panel saves under)
  whenever the node has a qualified placement in `layout.json`. Rehydration of
  this shape happens in a second pass, `graph_builder.apply_placement_configs`,
  AFTER `scope_filter.resolve_scope_view` — `build_function_nodes` only ever
  sees the bare id. See `docs/claude/placement-qualified-ids.md`.

## Where run options are consumed

### Python

`execution_service.py:789` passes `distribute` straight into `for_each`. This
path has always worked and is why the feature looked fine in Python.

### MATLAB

MATLAB never receives a run option as an argument. It receives a **generated
script**, and the option must be rendered into the `scidb.for_each(...)` call as
a name/value pair:

```matlab
scidb.for_each(@loadGaitRiteOneFile, ...
    struct('gaitRitePath', ..., 'gaitRiteConfig', ...), ...
    {GAITRiteLoaded()}, ...
    'subject', {...}, 'session', {...}, ...
    'distribute', true);
```

`+scidb/for_each.m` parses these at `:1791` and forwards them to the bridge
(`:277`, `:305`) and into the scifor opts (`:571`). Defaults live at `:1723-1725`
(`as_table = string.empty`, `distribute = false`).

There are **three** places in `api/matlab_command.py` that emit a `for_each`
call, and an option added to one and not the others is silently absent from the
other two:

1. the template / first-run branch (no DB variants — canvas wiring only),
2. `_for_each_call_lines` called for a single function's command,
3. `_for_each_call_lines` called per step by `generate_matlab_pipeline_command`.

`as_table` accepts two shapes on the MATLAB side (`for_each.m:217-223`): a
logical scalar, or a string array of parameter names. Both must round-trip.

Only non-default values are emitted. A script that says nothing about
`distribute` and a script that says `'distribute', false` behave identically,
and omitting keeps generated output diffable.

## Failure modes this design has actually hit

**Silent drop in generation (2026-09-14).** `run_options` reached
`generate_matlab_command` — `dagPanel.ts:433` forwards the whole params dict —
and the generator simply never read the key. The script was well-formed, the run
succeeded, 560 records were saved, and `distribute` was nowhere. Nothing failed,
so nothing was reported.

The diagnostic: `scifor`'s `resolve_distribute_target:` line
(`scifor/foreach.py:377,393`). It is emitted whenever `distribute` is truthy and
names the target key. Its **absence** from a run whose request logged
`distribute: True` localises the loss to everything upstream of scifor. It is at
INFO for this reason — at DEBUG, a working run and a dropped one were
indistinguishable in `scidb.log`.

**Silent drop in persistence (same investigation).** `update_node_config` was a
bare `UPDATE ... WHERE node_id = ?`. A node id with no matching row updated zero
rows and reported success, so the checkbox reset on every rebuild. Hence the
upsert, and hence the WARN when a config write matches nothing.

**Silent drop in rehydration (2026-09-14, later the same day).** With the
upsert in place the write landed — under
`fn__loadGaitRiteOneFile__cccfc8d46e3ddc62::main`. `build_function_nodes`
looked the config up by the bare id, before scope resolution, and missed. The
VS Code extension's DuckDB file watcher (`extension.ts` `setupDbWatcher`, 2 s
debounce) turned every config write into a `dag_updated`, the refetched node
came back configless, and the checkbox snapped back ~7 s after each click
(2 s debounce + a ~5 s `get_pipeline`). Fixed by `apply_placement_configs`;
that pass also WARNs about *orphan* configs (a saved id no node in the resolved
graph carries) so the next id-shape mismatch names itself in `scidb.log`.

A secondary effect worth knowing: `updateNodeData` builds the payload from
`node.data`, so once a refetch has stripped the keys, the *next* toggle saves
only the key just changed (`update_node_config ... keys=[]` appeared in the
log) — wiping the earlier settings from the DB too. Rehydration working is what
keeps the payload complete.

All three bugs were invisible in the same way, and each hid the next: the checkbox
not sticking looked like the cause of distribute not working, when in fact the
request log showed `distribute: True` on the wire for every attempted run.

## Reading a run in `scidb.log`

Follow one run by its `run_id`:

1. `[server] Parsed request: run_id=..., run_options={...}` — what the GUI asked
   for. If the option is wrong *here*, the defect is the panel or the stored
   config.
2. `generate_matlab_command: fn=..., run_options=...` — what generation saw and
   emitted. A gap between 1 and 2 is a routing loss; an option present here but
   absent from the script is a rendering loss.
3. `resolve_distribute_target: '<key>'` (scifor) — the option took effect, and
   this names the schema key it fanned out to.
4. `for_each_save: ... columns=[...]` — ground truth. With `distribute` active
   the distribute key appears as a column; without it, it does not.

Step 4 is the one that cannot lie. In the 2026-09-14 run the columns were
`['subject','session','speed','trial','GAITRiteLoaded']` — no `cycle` — which
settled it regardless of what any other layer claimed.

## A second consumer of node config — single-route on purpose

Run options are not the only thing `_node_config` carries. Since the column
selection UI it also holds `columnSelections` — `{param: {"columns": [...],
"iterate": bool}}`, the GUI's spelling of `MyVar["col"]` /
`MyVar.for_columns([...])`.

It uses the same table, the same upsert and the same two rehydration passes
(`_apply_saved_config` by bare id, `apply_placement_configs` by qualified
id), and it is in `_SAVED_CONFIG_KEYS` — which is the whole of what stops the
snap-back bug above from happening again to a different key.

What it deliberately does **not** copy is the two-source read. Run options
have one because a single-node Run reads live canvas data; column selections
are read from the **stored config only**, on every path — per-node run,
pipeline run, MATLAB generation and code export all go through
`execution_service.column_selections_for_nodes`. There is therefore no
second value that can disagree with the first, which is the failure mode this
document's "these two can disagree" warning is about. The cost is that a
column pick needs its `put_node_config` to land before the run reads it; the
write is synchronous from the panel and a failed write already WARNs.

See `docs/claude/column-selection.md` §From the GUI.

## Related

- `docs/claude/column-selection.md` — the second consumer of node config.
- `docs/claude/for-each-kwargs.md` — the option semantics themselves.
- `docs/claude/graph-database-state.md` §Identity — why `distribute`/`as_table`
  are part of `invocation_id` and `call_id`.
- `docs/claude/gui-vscode-extension.md` — the host/webview split that
  `generate_matlab_command` is dispatched across.
