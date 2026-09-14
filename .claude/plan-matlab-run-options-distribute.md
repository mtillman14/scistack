# Plan: `distribute=true` (and the rest of run_options) on the MATLAB path

Date: 2026-09-14
Status: proposed, awaiting approval

## Symptom

`distribute=true` has no effect for MATLAB functions run from the GUI. Python
runs are unaffected. Separately, the distribute checkbox does not stay checked.

## Evidence (`/workspace/scidb.log`, 2026-09-14)

Two runs were dispatched with `distribute: True`:

- `run_id=ekxah02g` @ 11:28:05 — died in generation:
  `matlab_command.py:862 _group_variants -> TypeError: unhashable type: 'dict'`
  (the `gaitRiteConfig` dict constant; addressed by `ff3d4972`).
- `run_id=xp0lxryj` @ 11:30:49 — ran to completion with **no distribution**:
  - `for_each(loadGaitRiteOneFile) done in 143.8s: completed=560, failed=392, total=952`
  - `for_each_save: result_tbl shape=(560, 5), columns=['subject','session','speed','trial','GAITRiteLoaded']`
  - no `cycle` column; 560 records = one per combo, no fan-out.
  - `scifor`'s `resolve_distribute_target:` line never appears.

## Root cause A — run_options never reach the generated script

`run_options` travels: webview -> `POST /api/run` -> `_route_matlab_run`
(`api/run.py:884`) returns `{run_id, host_execution_required, language}` ->
`dagPanel.ts:433` forwards the **full params** to `generate_matlab_command`.

The data arrives. It is then dropped:

- `services/matlab_command_service.py:467` builds the `_fmt(...)` call from
  `variants`, `schema_filter`, `schema_level`, `path_inputs`, `sweeps`,
  `output_types`, `glue`, … and never reads `params["run_options"]`.
- `api/matlab_command.py` contains **no occurrence of the string `distribute`**.
  `_for_each_call_lines` (`:906`) emits the inputs struct, outputs cell, schema
  kwargs and `'glue'` — nothing else.

So every generated call runs at MATLAB's defaults
(`+scidb/for_each.m:1723-1725`: `as_table = string.empty`, `distribute = false`).

The MATLAB side is complete and correct — `for_each.m` parses `'distribute'`
(`:1791`), forwards it to the bridge (`:277`, `:305`) and into the scifor opts
(`:571`). This is purely a generation gap in the GUI layer.

`as_table`, `dry_run` and `save` ride in the same dropped dict; they only
happened to match the defaults in this session.

## Root cause B — node config writes can land nowhere (the checkbox)

`pipeline_store.update_node_config` (`:442`) is a bare
`UPDATE _pipeline_nodes SET config = ? WHERE node_id = ?`. If no row matches it
affects **zero rows**, silently: no insert, no log, no error. The frontend's
`.then(() => clearNodeDirty(id))` records it as saved.

`_pipeline_nodes` rows exist for manually-placed nodes. A function that has run
renders as a DB-derived node with a composite id `fn__{fn}__{call_id}`, so for
`loadGaitRiteOneFile` (560 records, long since graduated) the UPDATE matches
nothing and the toggle is discarded.

Read-back compounds it: `api/pipeline.py:592-608` builds `saved_configs` **only**
from manual nodes, keyed by `fn_name`. `graph_builder.build_function_nodes:1558`
then applies it, and `_apply_saved_config` (`:2129`) handles the manual-node
path. Nothing serves a config for a graduated node.

This is NOT the cause of the distribute failure — the log proves
`distribute: True` was on the wire for both runs. It is a second defect, and it
also means `execution_service.py:777-789` (pipeline runs, which read run options
from the *stored* config) can disagree with what the panel displays.

## Root cause C — observability gap

`scifor/src/scifor/foreach.py:377` and `:393` log the resolved distribute target
at **`Log.debug`**. At INFO — the level the session ran at — a run with
distribute on and a run with it silently dropped are indistinguishable. This is
why the bug survived until the output was inspected by hand.

---

## Stage 1 — emit run options into the generated MATLAB script

`scistack-gui/scistack_gui/api/matlab_command.py`

1. New `_format_run_option_pairs(run_options) -> str`. Emits only values that
   differ from MATLAB's defaults, as `'name', value` pairs matching
   `_format_schema_kwargs`'s existing shape:
   - `distribute` true  -> `'distribute', true`
   - `dry_run` true     -> `'dry_run', true`
   - `save` false       -> `'save', false`
   - `as_table` true    -> `'as_table', true`; a list of names -> a MATLAB
     string array (`for_each.m:217-223` accepts logical scalar *or* string
     array, so both forms must round-trip).
   Defaults are omitted rather than emitted, so generated scripts stay
   diffable against the ones in the tests today.
2. Add `run_options: dict | None = None` to `generate_matlab_command` and to
   `_for_each_call_lines`; append the pairs to the call tail alongside `glue`.
3. Apply in all **three** emission sites: the template/first-run branch
   (`:721-729`), the single-function branch (`:769`), and the pipeline branch
   (`:1200`, taking `step.get("run_options")`).

`scistack-gui/scistack_gui/services/matlab_command_service.py`

4. Read `params.get("run_options")` and pass it into `_fmt(...)` (`:467`).
5. In `generate_matlab_pipeline_command`, source each step's options from that
   node's stored config and put them on the step dict. While here, feed the
   node's real `distribute`/`as_table` to `filter_hidden_targets` (`:648`)
   instead of the hardcoded `distribute=False, as_table=None` — those two are
   identity-bearing, so a hardcoded pair can mismatch the call_id and
   mis-filter hidden combos.

## Stage 2 — make node config persist for every node

Recommended: a dedicated `_node_config` table keyed by `node_id`, independent of
`_pipeline_nodes`.

6. `pipeline_store`: `_ensure_tables` creates `_node_config(node_id, config)`;
   `update_node_config` becomes an upsert against it; `get_node_config(db, node_id)`
   and `get_node_configs(db)` read it. Keep the `_pipeline_nodes.config` read as
   a fallback so existing saved manual-node configs are not orphaned.
7. `api/pipeline.py:592-608`: build `saved_configs` keyed by `node_id` from the
   new table, falling back to the current fn_name lookup so nothing regresses.
8. `graph_builder.build_function_nodes`: prefer a node_id-keyed config, fall
   back to the fn_name-keyed one.

> **Decision needed.** The alternative is upserting into `_pipeline_nodes`
> directly — fewer moving parts, but it manufactures a manual-node row for a
> derived node, which `merge_manual_nodes` may then graduate into a duplicate.
> A separate table avoids that interaction entirely. Confirm before I build.

## Stage 3 — logging (CLAUDE.md note 2)

9. `scifor/foreach.py:377,393` — promote `resolve_distribute_target` from
   `Log.debug` to `Log.info`. This is the scifor layer's own decision to report
   (note 3), and it is the single line whose absence would have shown this bug
   immediately.
10. `generate_matlab_command` — log the run options the script was generated
    with, and log explicitly when a non-default option is emitted into the call.
    A requested option that reaches generation and produces no pair is a WARN.
11. `pipeline_store.update_node_config` — log the node_id and rows affected;
    WARN on zero (which is the exact signature of defect B).

## Stage 4 — tests

12. `scistack-gui/tests/test_matlab.py`
    - `run_options={'distribute': True}` -> generated command contains
      `'distribute', true`; default/absent -> contains no `distribute`.
    - same for `as_table` (bool and name-list forms), `dry_run`, `save`.
    - all three emission sites, including the never-run template branch.
13. `scistack-gui/tests/test_pipeline_store.py` — config round-trips for a
    composite `fn__{fn}__{call_id}` node id that has no `_pipeline_nodes` row;
    regression test for the silent zero-row UPDATE.
14. `scistack-gui/tests/test_graph_builder.py` — a saved `runOptions` is present
    on a rebuilt DB-derived function node.
15. `scifor/tests/test_foreach_standalone.py` — assert the distribute target key
    is resolved and reported for the `distribute=True` + dropped-deepest-key
    shape this bug hit (schema `[subject, session, speed, trial, cycle]`,
    `cycle` unpopulated -> target `cycle`).

Test runs are the user's to invoke (`project_pytest_one_package_at_a_time`:
one package per pytest invocation — `scistack-gui` and `scifor` separately).

## Not in scope

- The `unhashable type: 'dict'` crash — already fixed by `ff3d4972`; Stage 4's
  generation tests should carry a dict-valued constant so it stays fixed.
- Reworking `saved_configs`' fn_name-wide semantics into per-call-site config.
  Stage 2 adds the node_id path and leaves the existing fallback intact.
