# Plot Studio: variant-axis leakage, popup node mapping, and resolve timeouts

Diagnosed from `/workspace/scidb.log` (session 2026-09-10 17:23–17:29) against the
FilteredEMG pipeline. Five findings, four of them bugs. Prerequisite reading:
`docs/claude/plot-variant-rows.md`, `docs/claude/variant-selection.md`,
`docs/claude/synthetic-factors.md`.

---

## Finding 1 — single-level branch params become variant Factors (BUG)

**Symptom.** `filterDelsys.config` and `filterDelsys.Fs` appear in the Factors
section, tagged `variant`, demanding a role.

**Evidence.** `scidb.log:5523`

```
loaded FilteredEMG: 2 record(s), levels=['pass'],
    variants=['filterDelsys.config', 'filterDelsys.Fs']
```

Two records, one per `pass` level, and both carry the same `config` and the same
`Fs`. Neither column distinguishes any two records.

**Cause.** `scistackplotdb/load.py:attach_variants` attaches **every** key that
`scidb.provenance_query.branch_params_batch` returns — and that function returns
every upstream *constant*, varying or not (`provenance_query.py:249`). Code axes
have exactly the guard branch params are missing: `code_version_ordinals`
restricts the chain to functions holding more than one version, which
`attach_variants`' own docstring calls out —

> "scidb omits single-version functions, so an unedited project gets no code
> columns at all and nothing changes for it."

There is no equivalent sentence for branch params because there is no equivalent
code. A variant column exists to stop records that differ from being overplotted
as replicates; a column with one level can never do that.

**Not a bug, and must stay:** a *multi-level* branch param that no Series row has
answered belongs in Factors. That is `variants._answered`'s intersection rule
("nothing about 'current code' decides which filter cutoff to plot") and the
`variant` tag on it is correct. Only the one-level case is wrong.

**Fix.** In `attach_variants`, drop a branch-param key whose values are constant
across the loaded records — same test the code chain already applies, in the same
place — and say so in the log. Belongs in `scistackplotdb`, not the GUI
(CLAUDE.md NOTE 3): it is a statement about what the provenance graph means, and
the CSV path and any library caller need it too.

**Open question this fix does not answer.** The user reports the `variant` tag on
`Fs` but not on `config`, yet both are in `variant_columns` and `FactorInfo`
builds `is_variant` from that one set (`table.py:247`). Nothing in the current
code produces that asymmetry, so either the observation was approximate or there
is a second effect. Stage 1 below adds the level-count logging that settles it
before any behaviour changes.

---

## Finding 2 — the popup matches axes to nodes by the wrong name (BUG)

**Symptoms.** Both of these, and they are one cause:

- entries under *"Not on this canvas (defined in a nested pipeline)"* for axes
  whose nodes are plainly on the canvas;
- every Parameter node dimmed with "not a variant here".

**Cause.** `VariantDagPopup.tsx` maps a branch-param axis to a node by string
equality between two names from **different namespaces**:

| | value here | namespace |
|---|---|---|
| `axis.param` | `"config"`, `"Fs"` | the **function's argument name** — scidb's `fn.param` branch-param key |
| `node.data.label` | `"delsys_config"`, `"delsys_sampling_frequency"` | the **Parameter entity's name** (`graph_builder.py:1370`) |

Two sites: `unmapped` (line 270) and `axisForParameter` (line 206, reached from
`ParameterNode.tsx:118` via `data.label`).

They agreed only by coincidence — this project *did* have Parameter entities
literally named `Fs` and `config`. The log shows them being deleted and the ports
rewired:

```
17:25:55  delete_node  param__config
17:26:00  delete_node  param__Fs
17:26:08  put_edge     param__delsys_sampling_frequency -> fn__filterDelsys  target_handle='param__config'
17:26:15  put_edge     param__delsys_sampling_frequency -> fn__filterDelsys  target_handle='param__Fs'
17:26:19  put_edge     var__glue_config_filter          -> fn__filterDelsys  target_handle='param__config'
```

From that moment `"config"` and `"Fs"` match no label, so both axes fall out of
the graph and into the unmapped list. Renaming a Parameter, or feeding a function
port from a glue node instead of a Parameter, silently breaks variant selection.

So: **a logic error, not stale data.** The "nested pipeline" wording compounds it
— the code assumes nesting is the *only* reason an axis has no node.

**The correct key is already on the wire.** A parameter→function edge carries
`targetHandle = f"param__{function's argument name}"` (`graph_builder.py:1702`,
and the manual edges above), which is exactly `axis.param`. Map by
`(target function label, targetHandle)` → source node id — the edge-based rule
the rest of the codebase already committed to (memory: *Edge-based function
inputs — inputs from edges only, no name matching*). This also makes glue nodes
and PathInputs selectable, since the axis binds to whatever feeds the port.

Reword the fallback list to "No node on this canvas" — nesting becomes one
possible reason rather than the asserted one.

---

## Finding 3 — everything dimmed except Variable and Glue nodes (BUG, partly)

Three separate reasons, and the third is the one that is simply backwards:

1. **Parameter nodes** — Finding 2. Fixing the mapping undims them.
2. **Function nodes** — `VariantFunctionNode` dims on `inert || !axis`
   (`FunctionNode.tsx:559`), and `axis` is a `Code:<fn>` axis. This project has
   none (nothing has two recorded versions), so every function node dims. That is
   the documented intent ("a greyed node is telling you it does not distinguish
   these records"), but when *every* control is grey the popup reads as broken
   rather than as informative. Add a one-line header saying what is selectable —
   the absence of a control only informs if the user knows it is deliberate.
3. **Variable / PathInput / Glue nodes** — these have **no variant-mode branch at
   all**; they render exactly as on the canvas, undimmed. That is inverted: they
   are the nodes that can never be an axis, so they are the ones that should
   always be inert. After Finding 2, a glue node feeding a live port *should*
   become selectable; a variable node never should.

---

## Finding 4 — "Series", not "Variants" (NOT A BUG)

Intentional. Stage 3 of `.claude/plan-plot-studio-todos-26-09-09.md` gave
`VariantSet` a `variable` field, so a row is now "one series: a variable,
optionally narrowed to one pipeline variant" — the section was renamed to match
(`PlotStudio.tsx:855`). The rows are still `spec.variant_sets`; the Select button
still opens the variant DAG. `docs/claude/plot-variant-rows.md` still calls it the
Variants section and should be updated.

One real consequence: `hasVariants` (line 737) now also fires on
`stackable.length > 0`, so the section appears for projects with no variants at
all — deliberate, and the reason the section is visible here.

---

## Finding 5 — `plot_resolve` timed out (BUG: no cancellation, no cache)

**Nothing hangs.** A fixed 30 s frontend timeout (`frontend/src/api.ts:133`,
applied to every method) meets a resolve that legitimately costs 4–16 s and
inflates under self-inflicted concurrency.

**Measured cost.** `FilteredEMG` is a 12-field struct of 1-D arrays. Every
resolve re-explodes it:

```
17:26:29  downsampled 8559600 row(s) to 20046      resolve TOTAL=4.023s   (1 figure)
17:27:58  4453200 + 4106400 rows                   resolve TOTAL=7.766s   (2 figures)
17:28:12                                           resolve TOTAL=10.138s
17:28:22                                           resolve TOTAL=16.190s
17:28:29                                           resolve TOTAL=15.975s
17:28:30                                           resolve TOTAL=14.560s
17:28:41 … 17:28:59                                11.056s, 12.335s, 11.990s, 13.481s
```

The per-request work never changes; the wall clock triples. That is contention.

**Four compounding causes.**

1. **No cancellation.** `server.py:1673` spawns a thread per request and nothing
   stops a superseded one. `PlotStudio.tsx:397` debounces 180 ms and then fires
   **two** RPCs (`plot_resolve` + `plot_capabilities`); `plot_resolve` is not in
   `COALESCABLE_METHODS` (`api.ts:88`). The log interleaves — a melt at
   17:28:13.77 lands inside a resolve that started at 17:28:06.73 — so four to
   six full resolves run at once, each slowing the others.
2. **Nothing between the raw frame and the figure is cached.** `get_source`
   caches the variable frame only. Each cycle re-melts (two `melted
   'FilteredEMG'` lines per cycle, one for resolve and one for capabilities) and
   `reduce.resolve` re-explodes 4–8 M rows from scratch.
3. **Every figure of a fan-out is reduced to render one.** `resolve_figures`
   documents this ("the fan-out's size and labels are not knowable without doing
   so"), and both downsample lines fire per resolve. With `pass` on ITERATE it is
   a flat 2× on the dominant cost.
4. **`plot_capabilities` pays the same price**, because `variant_summary` and
   `apply_variant_sets` run against the full frame.

**Fix, in cost order.** Cancellation first (it alone removes the 4× inflation and
is what makes the timeout unreachable): a per-panel request generation on the
frontend that drops stale responses, and coalescing so only the newest
`plot_resolve` is in flight. Then cache the exploded frame keyed by
`(variables, x_measure, factor_variables)` so a role toggle does not re-explode
millions of rows. Then defer non-selected figures. A larger timeout for the plot
methods is the last resort, not the fix — and if it is raised, the panel must say
"still working" rather than sit silent for 30 s.

---

## Stages

**Stage 1 — diagnostics first (CLAUDE.md NOTE 2).** No behaviour change.
- `attach_variants`: log each branch-param axis with its distinct-level count, so
  "distinguishes nothing" is visible in the log rather than inferred.
- `variant_graph`: log each axis's `function`/`param` and its level count.
- `VariantDagPopup`: log the axis→node binding it computed and every axis it
  failed to bind, with the labels and handles it tried.
- `reduce.resolve`: promote the explode line to INFO and add the row count before
  and after, so the dominant cost is attributable without DEBUG.
- Re-run the FilteredEMG plot and read the log. **Confirms or refutes Finding 1's
  open question before anything is changed.**

**Stage 2 — Finding 1.** Drop constant branch-param axes in `attach_variants`.
Tests: an axis with one level never reaches `factors`; an axis with two still
does, still tagged `is_variant`, and is still answerable by a Series row.

**Stage 3 — Finding 2 + 3.** Rebind the popup by `(function, targetHandle)`.
Tests: a Parameter renamed away from the function's argument name still binds; an
axis fed through a glue node binds to the glue node; an axis with genuinely no
node lands in the fallback list; variable/pathinput nodes are always inert. The
existing `test_plot_service.py` assertion that variant-mode components never
reference `callBackend` must keep passing.

**Stage 4 — Finding 5.** Cancellation + coalescing, then the exploded-frame
cache, then deferred fan-out figures. Test: two rapid spec changes leave one
in-flight resolve; a repeated resolve of an unchanged table does not re-explode.

**Stage 5 — docs.** Update `docs/claude/plot-variant-rows.md` for the Series
rename and the axis→node binding rule; note in
`docs/claude/synthetic-factors.md` that a variant axis requires >1 level.
