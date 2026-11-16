# Plan: saved plots and stats become permanent DAG nodes (issue #19)

*Drafted 2026-11-16. Approved with the default decisions and BUILT the same day (all stages). Tests not yet run by the user; GUI not checked by eye. See "As built" at the end.*

## The issue

> Plots/Stats can already be "saved", but this is really a bookmark inside the
> Studio. For reproducibility, "save" should also offer (maybe default to)
> creating a new node on the DAG canvas. This needs a PathOutput or some
> auto-file-save mechanism. There can be many of these, one per plot, and they
> usually share a root folder. How do we avoid repeating that root folder in
> every saved plot?

## What exists today (read before building)

| Piece | Where | What it does now |
|---|---|---|
| Saved plot | `scistackplotdb/saved.py`, `_saved_plot` table (`VersionedStore`) | Display intent only: spec + view, versioned by `plot_id`. Doc: `docs/claude/saved-plots.md` |
| Saved stats table | `scistackstatsdb`, `_stats_spec` table | Same store rules. It can link to a plot (`linked_plot`) |
| Plot "Add to pipeline" | `plot_service.add_to_pipeline` → `scistackplotdb.endpoint.generate_endpoint` | Appends a `plot_` function to `scistack_plots.py`, declares `<Var>Figure`, and **returns** the `for_each` snippet. The user wires the node by hand |
| Stats "Add to pipeline" | `stats_service.add_to_pipeline` → `scistackstatsdb.generate_stats_endpoint` | The same, into `scistack_stats.py`. There is no PathOutput: the stat record lives in the DB |
| PathOutput | `scifor/pathoutput.py` | Pure template. Relative templates resolve against the **cwd**, and there is no root concept. The default template is `plots/<slug>_{key}.png` (`default_path_template`) |
| PathOutput on the canvas | — | **Doesn't exist.** `custom_plot_service.py:212` says it outright: "The output path of a plot_ endpoint is never an edge in the GUI". So a `plot_` node placed on the canvas today has no way to get its `filename` argument in a GUI run |
| Project root | `scifor/project/root.py: project_root()` | The one owner of "which directory". The GUI pins it |
| Machine roots | `scifor/machine.py` (`{rto}/rest` in scistack.toml) | Already make an absolute folder portable across machines |

Locked decisions this plan does **not** reopen:
- Export emits literal seaborn code, never a `scistackplot.render(...)` call ([[project-scistackplot-layer]]).
- A saved plot is display intent, so it is not an intent-store aspect ([[project_saved_plots]]).
- `save=` is the one draft switch, and endpoints record by default ([[project-endpoints-always-record]]).

## Answer to the root-folder question

**One project setting, with per-node templates relative to it.**

```toml
# scistack.toml
[outputs]
folder = "outputs"          # relative to the project root, absolute, or "{results}/aim2" (machine root)
```

- Each node stores only its relative template, e.g. `figures/StepLength_{subject}.png`.
- `PathOutput` resolves a **relative** template against `scifor.output_root()`. That is the same pattern `PathInput` uses with `project_root()`/`bound_to`. Absolute templates are unchanged.
- **Owners:** `scifor/project/root.py: output_root()` owns resolution (it is pinned, like `project_root`). `scidb.config_file` is the one reader/writer of `[outputs] folder`. `scifor.machine.expand_roots` expands `{root}` as it already does elsewhere.
- When `folder` is unset, the default is `project_root()`. Existing scripts that rely on cwd == project root keep writing where they write now.

I rejected two alternatives:
- *Named output roots, like `[path_outputs]` entities.* That is a new entity kind with TOML, GUI and MATLAB surfaces, built for a need nobody has stated. A user who wants one plot elsewhere can type an absolute or `{machine-root}` template on that node.
- *A `{root}` placeholder inside the template.* `PathOutput` already substitutes every `{key}` from combo metadata, so a root named like a schema key would be ambiguous.

**Known consequence (document it, don't build for it):** `PathOutput` is excluded from version-key hashing. Changing `folder` therefore does not make nodes stale, so `skip_computed` keeps the old files where they are. A forced run writes to the new folder. The run logs one INFO line naming the output root it used, so the logs show which folder a run wrote to.

## What "save to the DAG" does

The Save row in Plot Studio and Stat Studio gets one checkbox: **"Add to pipeline"**. It is checked by default when the source is the database. It is hidden in CSV mode, where no pipeline is possible.

When it is checked, Save does what it does today and then, in one backend call:

1. **Writes the endpoint function** into `scistack_plots.py` / `scistack_stats.py` between generated markers:
   `# scistack:generated plot_id=<id>` … `# scistack:end`.
   The function name and output variable come from the **saved name** (`plot_<slug>`, `<Name>Figure` / `<Name>Stats`), not from the spec.
2. **Places it on the canvas** in the current scope: the function node, an edge from each input variable to its port, the output variable node, and the PathOutput binding (below). This uses the existing `put_layout` and `put_edge`. No new write surface is added.
3. **Records the link** in the saved plot's envelope, as `endpoint = {function_name, file}`. The envelope is the one owner of "which function this saved plot generated".

**Re-saving** a linked plot replaces the block between its markers. That changes the function hash, so the node turns red and the next run redraws the figure. There is one function per saved plot, never a pile of `plot_x_2`.

- If the markers are gone (the user edited the file), the re-save is refused: "plot_x was edited by hand; uncheck Add to pipeline or restore the markers". The saved plot itself is still saved.
- Another plot's function already using the name → refused. This is today's "already defines" check, extended to compare `plot_id`.

**Inputs the canvas can't express (v1 refuses, never guesses):** a variant row pinned to a specific older code version, or several variant rows. In those cases the function is still written and the snippet is still returned, exactly as today. The node is not placed, and the save result says why. That is the 80/20 cut. The common case, one variable with current code plus optional grouping variables or columns, wires cleanly.

**Stats nodes get no file in v1.** The `stat_` record in the database is the reproducible artifact, and `scidb report` already writes `stats.csv`. A file output can come later if wanted (see the decisions below).

## PathOutput on the canvas

This mirrors how ColName was added ([[project-colname-input-gui]]):

- **Meaning owner:** `scidb.intent.ASPECT_PATHOUTPUT` = `{param: template}` per node, normalized by `normalize_pathoutput_inputs`.
- **Run:** `execution_service._attach_pathoutput_inputs` → `domain/pathoutput_inputs.apply_to_target` → binding kind `edge_resolver.BINDING_PATHOUTPUT` → `PathOutput(template)` in the target's inputs. Every consumer of bindings must handle the new kind with ref None. Find them with `findReferences` on `BINDING_COLNAME`.
- **UI:** the port shows an editable template text field in the node's Inputs section, with the resolved example path (first combo) underneath. Users can also set this by hand on any hand-written `plot_` node, which fixes today's gap.
- **MATLAB:** v1 is Python only, because Plot Studio generates Python. If a MATLAB emit site sees the aspect, it logs WARN and skips it. A MATLAB-language `plot_` node can follow later through the bridge's existing per-combo pre-resolution.
- **Parent folders:** the `plot_` wrapper creates missing parent directories before saving. This was out of scope before ([[project_plotting_leaf_nodes]]), and it is required now that the root is a setting.

## One owner for the wiring

`generate_endpoint` and `generate_stats_endpoint` already know `param → input`. Plot's `EndpointCode` gains `inputs: dict[str, InputRef]`, as stats already has. The **snippet text and the canvas edges are both rendered from that one dict**, so they cannot diverge. A parity test compiles the placed canvas (`build_backend_pipeline`) and compares its step's inputs to `EndpointCode.inputs`.

## Stages (named, in build order)

### Investigate
Check these before writing code, and add log lines where they are missing:
- What `row_expression` emits for a freshly saved single-variable plot. Is it a bare variable or `Variant(…, code_version="latest")`? Does the canvas default load the same rows ([[project_default_variant_selection]])? This decides what counts as "wireable".
- What a GUI run does today with a canvas `plot_` node that has an unbound `filename`. I expect a missing-argument failure. Capture that failure as a test.
- The GUI run's cwd ([[project_run_working_directory]]), which tells us where a relative PathOutput lands now.

### Output folder (scifor + scidb)
- `output_root()` / `pin_output_root()` in `scifor/project/root.py`. `PathOutput.resolve` joins relative templates onto it.
- Read and write `[outputs] folder` through `scidb.config_file`. The GUI pins it at startup next to `project_root`, and it gets one row in the Paths popup.
- Log: `[pathoutput] root=<path> (source=setting|default)`, once per run.
- Tests (scifor): relative joins, absolute unchanged, default = project root. Test (scidb config): round trip keeps unknown keys.

### PathOutput binding (scidb intent + GUI)
- Aspect, normalizer, binding kind and Inputs field, as above.
- Test: a canvas `plot_` node with a template runs and writes `<output_root>/figures/…png` for each combo. Use the integration suite with a tiny `plot_` fixture.

### Endpoint wiring owner (scistackplotdb)
- `EndpointCode.inputs`, with `_foreach_call` rendering from it. `classify_wireable(code) -> (ok, reason)` is the one place that decides what v1 can place.
- Tests: snippet unchanged for existing fixtures (snapshot); `classify_wireable` cases.

### Place on canvas (GUI service)
- `endpoint_node_service.place_endpoint(db, code, scope, plot_id)` does the marker-block write/replace, declares the output, reloads the registry, and calls `put_layout` + `put_edge` and the PathOutput aspect. It is shared by plot and stats. The two `add_to_pipeline` functions call it, which also removes their duplicated file-append code.
- Log: `[endpoint-node] wrote|replaced <fn> in <file>; placed node <id> in scope <s>; edges <n>; skipped: <reason>`.
- Tests: place → node + edges + aspect exist; re-save replaces the block (one `def`); a hand-edited block is refused; an unwireable input is refused with its reason while the function is still written; the parity test.

### Save offers it (frontend)
- A checkbox in the Save row (`useSavedPlots` hook, and the matching Stat Studio save). `plot_saved_save` / the stats save take `add_to_pipeline: bool` and return `endpoint` plus any `skipped` reason, shown inline.
- The envelope's `endpoint` link is read leniently, by the restore rules.
- Rebuild both vite bundles. Add a GUI manual-test section.
- Hot spot note: this goes in `useSavedPlots`, not in `PlotStudio.tsx` (grugbrain rule).

### Docs
- `docs/claude/saved-plots.md` §"Saved plots on the DAG"; `input-markers-colname-pathinput-pathoutput.md` §output root; `config-file-formats.md` `[outputs]`; the GUI manual-testing doc.

## Decisions needed from you

1. **Checkbox default.** My recommendation: checked by default for database plots. The issue leans that way, and reproducibility is the point.
2. **Stats file output.** My recommendation: none in v1, because the DB record plus `scidb report` already cover it. The alternative is a `stats.csv` per saved table through PathOutput.
3. **Root.** My recommendation: one `[outputs] folder`. The alternative is named output roots.
4. **Unwireable inputs** (pinned old version, several variant rows). My recommendation: refuse placement with the reason, and still write the function. The alternative is to extend canvas edges to carry Variant pins first, which is a bigger change.

## As built (2026-11-16)

The user approved the default decisions (1–4 as recommended) and asked for everything at once.

**Deviations from the plan above:**
- **The link is the marker, not an envelope field.** `save_plot` writes the envelope before the
  code is generated, and two places holding the link would drift. `endpoint_node_service.generated_function_name`
  reads it back, so a re-save keeps the first function name, even after a rename.
- **The output folder is not "pinned" like the project root.** It is read live from scistack.toml
  (`scifor.project.outputs.output_folder`, like `[aliases]`). **Unset means unchanged** (the path
  stays relative), not "joined onto project_root()". That keeps every existing PathOutput test and
  script byte-identical.
- **The stats node rides on the plot save.** Since #12 the saved figure carries its stats settings,
  so the one Save places `plot_` and, when exportable, `stat_` (schemaLevel `[]`). StatStudio's own
  table save gets no checkbox.
- **No resolved-example line** under the node's output-path field (it would need a backend call per
  keystroke); the tooltip explains placeholders and the folder.
- **Placement scope is root** (`pipeline_id` on the request, unset by the panel): the Studio tab
  does not know the canvas scope.

**Files:**
- scifor: `project/outputs.py` (new), `pathoutput.py` (`under_output_folder`), `project/config_file.py`
  (`[outputs]` table, cache clear). Test: `tests/test_project_outputs.py`.
- scidb: `provenance/intent.py` (`ASPECT_PATHOUTPUT`, `normalize_pathoutput_inputs`),
  `run/foreach_load.py` (plot wrapper mkdir). Test: `tests/test_pathoutput_canvas_support.py`.
- scistackplotdb: `endpoint.py` (`EndpointInput`, `endpoint_inputs`, `why_not_wireable`,
  `EndpointCode.inputs/as_table`; `_foreach_call` renders from the map). Tests appended to `tests/test_endpoint.py`.
- GUI backend: `services/endpoint_node_service.py` (new), `domain/pathoutput_inputs.py` (new),
  `domain/edge_resolver.py` (`BINDING_PATHOUTPUT`), `stores/intent_store.py`,
  `domain/graph_builder.py` (rehydrate + fed), `services/execution_service.py` (attach, render,
  MATLAB WARN), `services/plot_service.py` (`generate_code` split out; `add_to_pipeline` uses the
  shared writer), `services/stats_service.py` (same), `services/saved_plot_service.py`
  (`add_to_pipeline`, `pipeline_id`), `api/plot.py`, `project/config.py` (`describe_output_folder`,
  `set_output_folder`), `services/project_service.py`, `api/project.py` (`set_output_folder`).
  Tests: `tests/test_pathoutput_inputs.py`, `tests/test_endpoint_node_service.py`.
- Frontend: `OutputFolderEditor.tsx` (new), `PathsPopup.tsx`, `api.ts`, `generated/api-schema.json`
  (hand-edited; regenerate) + `api-types.ts` (`npm run gen:types`), `Sidebar.tsx`,
  `FunctionSettingsPanel.tsx` (`OutputPathField`), `DAG/FunctionNode.tsx` (port label),
  `PlotStudio/{SavedPlotsRail,useSavedPlots,usePresets,savedPlots,savedPlots.test}.ts(x)`.
- Docs: saved-plots.md § Saved plots on the DAG; input-markers-…md § The output folder / Output paths
  on the canvas; config-file-formats.md § `[outputs]`; decisions.md D-2026-11-16; GUI manual
  testing § 0z…z (issue #19).

**Verified here:** frontend `tsc` (only the pre-existing `useStatMarks.ts:37` error, present at
HEAD) and `npm test` (762 pass); pyright found no undefined names or bad imports in the 20 edited
Python files. **Not run:** pytest (user), `gen_ts_types.py` (user), bundle rebuild, GUI by eye.
