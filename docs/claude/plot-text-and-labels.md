# Plot text: sizes and display labels

*Written 2026-09-24, **before** the build. It records the design and the
decisions behind it. Plan and stage notes: `.claude/plan-plot-text-sizes-and-aliases.md`.
When a stage lands, update the "Status" line and correct anything the build
changed.*

**Status:** Automatic size rebuilt as one dial 2026-10-14 (ADR D-2026-10-14-1; tests pass, GUI §0zzzo unchecked). Stages 1-5 built 2026-09-24; tests pass. Stage 6 (the GUI Labels section and the project alias write) built 2026-09-24: npm tests pass, pytest unrun. GUI not checked by eye (manual tests §0zj, §0zk). Stage 7 (the README section and ADR D-2026-09-24-2) built. All stages built.

This covers two questions about any piece of text in a figure: **how big
is it**, and **what does it say**. Both are display-only. Neither changes
which rows are drawn, their order, or anything a run computes.

## Part 1: how big: `StyleOptions.text`

### The elements

| `TextSizes` field | What it sizes | Unset → |
|---|---|---|
| `base` | the reference (matplotlib `font.size`, ratio 1) | **auto** (see "Automatic size"); a number = fixed |
| `target` | not a size: `print` / `slide`, the default bands | `print` |
| `title` | figure title / `suptitle` | reference × 1.44 |
| `x_label` | x axis title, `supxlabel` | × 1.2 |
| `y_label` | y axis title **and facet-panel y titles** | × 1.2 |
| `x_ticks` | x tick labels | × 1.0 |
| `y_ticks` | y tick labels | × 1.0 |
| `groups` | nested-x bracket / group label rows | × 1.0 |
| `legend` | legend entries | × 1.0 |
| `legend_title` | legend title | × 1.2 (kept through a legend shrink) |
| `differences` | difference-bar labels | × 1.0 |
| `ratios` | not a size: `{element: ratio}` over the defaults | `{}` |
| `floors` / `ceilings` | not a size: `{element: pt}`, auto only | `{}` (band × ratio) |

`None` always means "derived": reference × ratio (`textsize.DEFAULT_TEXT_RATIOS`
and `ratios`), clamped into the element's band when auto. Since 2026-10-14 a
fixed `base` uses the same ratios, so it no longer reproduces matplotlib's
`medium`/`large`/`small` (beta: no migration).

`font_size` and `tick_font_size` were replaced by this group as a clean break
(beta rule). A saved plot that still has them reverts them to defaults, and
`restore_spec` adds a note for each.

### One owner: `scistackplot/textsize.py`

`resolve_sizes(style)` resolves every `None` to points. `rc_params(sizes)` is
the one `rc_context` dict. Both `render_matplotlib` and the generated
code use it, so the saved figure and the exported `plot_` step cannot size
text differently. matplotlib has only one `axes.labelsize`, so the x and y
titles also get an explicit `fontsize=` from the resolved sizes. That is the
only place where sizes are passed per artist.

The plotly preview maps the same resolved sizes onto `tickfont`, `title.font`
and `legend.font` at 1 pt = 1 px, the convention `font_size` already used
(`figure-size.md`).

### Pinned sizes and automatic fitting

A size you set is **pinned**. The automatic fitting (`ticklabels.fit_labels`,
legend fitting) may still rotate, wrap, thin or move the text, but it never
shrinks a pinned size. If the pinned text then overlaps, the fit reports
it (`meta.label_fit`, the GUI notice names the element) and does not
override you. The x tick pin has always worked this way. The rule now covers
`groups` and `legend` too.

### Automatic size (`base` unset): `scistackplot/autosize.py`

*Rebuilt 2026-10-14 as ONE dial (plan `.claude/plan-dimension-text-sizing.md`,
ADR D-2026-10-14-1, superseding the per-element search of D-2026-10-06-2).
tests pass (user, 2026-10-14); GUI §0zzzo not checked by eye.*

A new plot leaves `base` unset, which means **auto**. `base` is the
**reference**: one number every element is sized from. Auto searches it; a
typed number fixes it.

**The model** (user, 2026-10-14):

- **Ratios** are the user's intended relative sizes, stored and never altered
  by sizing. One table, `textsize.DEFAULT_TEXT_RATIOS`, overridden per element
  by `TextSizes.ratios`. A fixed `base` and auto use the same ratios.
- **Bands**: each element's floor and ceiling default to the target's band
  (`textsize.BANDS`: print 8–12, slide 14–28) × its ratio, and each is
  editable in pt (`TextSizes.floors` / `ceilings`).
- **Drawn** = `clamp(reference × ratio, floor, ceiling)`
  (`textsize.drawn_sizes`). A clamp changes what is drawn, never the stored
  ratio, so a larger canvas restores the proportions. A fixed `base` is drawn
  unclamped (manual is anything).
- An element given its own pt size is **fixed**: outside the scaling, pinned
  against the fitting's shrink, as before.

| Element | Ratio | Print band | Slide band |
|---|---|---|---|
| `x_ticks`, `y_ticks`, `legend`, `groups`, `differences` | 1.0 | 8–12 | 14–28 |
| `x_label`, `y_label`, `legend_title` | 1.2 | 9.6–14.4 | 16.8–33.6 |
| `title` | 1.44 | 11.52–17.28 | 20.16–40.32 |

With the defaults the reference runs exactly over the target band (print
12 → 8) and nothing clamps; clamps bite once the user edits a ratio or a pt
bound. The legend title keeps its ratio to the entries through a legend-fit
shrink (`ResolvedSizes.legend_title_for`).

Why the band follows the **destination** and not the width: the saved file is
exactly W × H, so a point here is a point on paper. A 7.2 in figure can be a
journal's double column or half a slide. Choosing Slide never resizes the
figure.

**The search** (`choose_reference`, pure, injected `measure`). Candidates run
from the largest reference at which any scaled element is still below its
ceiling down to the smallest at which any is still above its floor
(`textsize.reference_range`), in 0.5 pt steps. Binary search, first layout at
the top: 1 layout when the top is clean, about 5 otherwise. **One failing rule
anywhere steps ALL the scaled text down together**, so proportions hold. A
layout is clean when every rule in `LayoutReport.failures()` passes:

| Rule | Measured by |
|---|---|
| 1. No text into text: categorical x tick fit needs no shrink/thin/rotate; numeric x and y drawn labels do not collide; no run into the neighbouring panel; bracket rows fit; legend fits at the right | `fit_labels`, `mpl._tick_and_title_rules`, `GridReach`, legend fit |
| 2. No text off the canvas (drawn labels only) | `mpl._overflow_by_element`, `drawn_ticks` |
| 3. Too tall: every panel shows ≥ 3 y tick labels | `_tick_and_title_rules` (`fewest_y_ticks`) |
| 4. Too wide: every numeric x axis shows ≥ 3 x tick labels | same (`fewest_x_ticks`) |
| 5. No y title taller than its panel | same (`y_title_overruns`) |

How each rule is measured, its tolerance, and how to add one: `auto-text-layout-rules.md`.

Rules 3/4 exist because matplotlib's locator thins ticks as the font grows
(`Axis.get_tick_space`). Panels whose labels are switched off (shared y) are
not counted; categorical axes are the tick fit's. Wrapping, prefix-strip,
wrapped legend title and short handles are harmless (`HARMLESS_STEPS`).

At the bottom with rules still failing, the bottom is drawn, the existing
fitting (rotate / thin / legend below) handles the rest, and the INFO line
says `at the bottom, still failing: …`.

Removed with the per-element search: `MIN_DATA_FRACTION` (the plot-area
rule; panel sizing is not text's job), per-element blame, `VERIFY_ROUNDS`,
`CEILING_RATIOS`, `textsize.MEDIUM/LARGE/SMALL`.

**Only DRAWN tick labels are measured** (`mpl.drawn_ticks`, 2026-10-14). `ax.get_yticklabels()` also returns labels for the locator's positions just past each end of the view; matplotlib never draws them, but they sit outside the axes and used to measure as "y_ticks runs 0.43in off the canvas" (C-Brace log). Every tick measurement in `render/mpl.py` goes through `drawn_ticks`; tests in `tests/test_drawn_ticks.py`.

**Where it runs, and how every consumer gets the same numbers:**

| Path | What it does |
|---|---|
| `autosize.settle(resolved, width_in, height_in)` | searches, logs one INFO, sets `ResolvedPlot.auto_text` (`AutoTextSize.reference`); memoised on `(id(resolved), W, H, text)` |
| `render.mpl.render` | settles first (a trial is already settled, so it never recurses) |
| `layout_decisions` | settles at the size it decides at (the pane's in Fit pane) and returns `auto_text`, plus `y_axes` / `x_axes` (`base.AxisText`) |
| `render_plotly(decisions=…)` | **applies** `decisions["auto_text"]`, never a second search; copies the export's y ticks and numeric x ticks |
| `write_figure` (Save) | through `render_matplotlib` |
| `generate_plot_function` | settles figure 1 once; `_EXPORT_AUTO` holds the reference and `_export_sizes` bakes the drawn sizes as literals, with a `# text sizes: auto…` comment |
| every renderer | `textsize.sizes_for(resolved)` = `resolve_sizes(style, reference=…)` |

`resolve_sizes` on an unsettled auto spec (outside a render) uses the top of
the range and logs a DEBUG line, `text size is auto but unsettled`. If that
line sits next to a render line, some consumer skipped `sizes_for`.

**Known limits.**
- A fan-out's generated code uses figure 1's sizes for every figure (one
  function, one `rc_context`). The preview and Save size each figure on its own.
- Search time on real data has not been measured; the INFO line carries the ms.

**Logging.** INFO, once per settle: `auto text size: print at 8.00 x 6.00 in:
reference 10.5pt of 8-12 (next step up fails: …); title=15.12, x_label=12.6,
…, y_ticks=10 (clamped floor); 4 layout(s) in 380 ms`. DEBUG, one line per
layout: `auto text size layout k: reference R pt -> failures | clean in ms`,
and `auto-size rules: …` with the fewest-tick counts, collisions and title
overruns. Trial layouts run inside `scistacklog.Log.trial(...)`: their own
INFO/WARN are demoted to DEBUG and prefixed `[auto-size trial reference R pt]`.

### In the GUI

The Figure size section has **Font (pt)** (`base`) and a **Text sizes (pt)**
grid with one box per element. The rows are the keys of
`layout.meta.text_sizes` (`ResolvedSizes.to_dict()`), so the element list is
Python's. An empty box's placeholder is the size Python resolved
(`auto · 11.7`); the frontend never multiplies `base` by a ratio. Clearing a
box **deletes** the key (`textSizes.withTextSize`) rather than storing null,
because `PlotSpec.to_dict` drops nulls and a stored null would make a
reopened saved plot read as "● modified". The logic lives in
`frontend/.../PlotStudio/textSizes.ts` (node-tested).

**Auto in the GUI (2026-10-14).** An empty Font box is auto; its placeholder is
`auto · N`, N being the chosen reference. Under the size boxes one line
(`textSizes.autoSummary`) says `Reference 10.5 pt of 8–12; one step larger:
<rule>`, `(the top)`, or `(the bottom); still failing: …`. Each element box
shows its drawn size; its tooltip (`rowTitle`) says when it is "held at its
floor/ceiling, not at its ratio". The **Proportions** grid has one row per
element: Ratio, Floor, Ceiling (pt) and Drawn. Empty cells show Python's
effective value (`meta.text_sizes.ratios` / `bounds`); typing writes
`style.text.ratios` / `floors` / `ceilings` (`withTextTable`; an emptied
table is deleted, as Python's `to_dict` drops it). Drawn shows `9.6 · floor`
in the caution colour when clamped, `· fixed` for a typed size. Its own Reset
(`resetTextTables`) clears the tables; the size-box Reset keeps them. The
**Print / Slide** select writes `style.text.target`. It is built from
`meta.text_sizes.targets` (Python's `TEXT_TARGETS`) and is disabled while
Font is fixed. The target is stored even when it is the default, because
`to_dict` always writes it. Reset keeps `base` and `target`.

## Part 2: what it says: display labels

### Three layers, first match wins

1. **The plot:** `PlotSpec.aliases` (`{thing: Alias(name, levels)}`, the same shape as the file),
   stored with the saved plot.
2. **The project:** `[aliases]` in `scistack.toml`
   (`[tool.scistack.aliases]` in `pyproject.toml`), shared by every plot.
3. **The raw** level or name.

`style.title` / `x_label` / `y_label` sit above all three for the axis and
figure titles they name. A plot-layer entry of `""` means "show the raw text
even though the project aliases it"; deleting the entry falls back to the
project.

**Why there is no per-plot .toml:** a plot's state has one home, the
saved-plot row (`saved-plots.md`), and the generated code is built from its
spec. A side file would be a second owner that neither carries (the rule from
CLAUDE.md NOTE 4).

**Why a project layer:** a schema key's levels mean the same thing in every
figure. Without this layer, "BL = Baseline" would be restated in every plot
and would drift between them. `[schema_keys]` level order is the precedent:
it is presentation information about levels, it lives in the same file, and
it is read the same way.

```toml
[aliases.session]                 # one entry per drawn thing: schema key,
name = "Session"                  # variable, "Var.Column", ColName, Variant

[aliases.session.levels]          # its values; keys are level TEXT, quoted
"BL" = "Baseline"                 # "01" stays "01"
"01" = "Visit 1"

[aliases."Demographics.Sex"]
name = "Sex"
levels = { "F" = "Female", "M" = "Male" }   # inline form is equivalent

[aliases.StepLength]
name = "Step length (cm)"
```

**Why one entry per thing, with `levels` as a sub-table** (user,
2026-09-24): `name` renames the thing where the figure says what it IS
(axis title, legend title); `levels` renames its values wherever they
appear (ticks, legend entries, brackets, panel titles). Keeping both under
one entry means a reader never has to know whether `session` is "a schema
key" or "a variable" to find it. `levels` must be a sub-table, not keys
beside `name`: a flat table would be ambiguous for a level literally called
`name`. An earlier draft split by kind (`[aliases.variables]`,
`[aliases.schema_keys]`, `[aliases.levels.X]`); that only disambiguated a
variable and a schema key with the same name, which `scidb.aliases` now
WARNs about instead.

### What a label may and may not do

- **It only changes text.** Filters, the location picker, facet
  `Matcher`s, variant pins, y-limit keys, colour identity and the plot-data
  CSV all use **raw** levels. A facet layout rule written against `R_HAM`
  still matches after `R_HAM` is shown as "Right hamstring".
- **It is one-to-one within a factor, after merging.** Two levels of one
  factor that read the same are refused by `validate`, which names where
  each clashing alias came from. The first reason is that two different
  marks with the same label make the figure lie. The second is that the
  generated code renames the data column. That is equivalent to relabelling
  the display only when the map is one-to-one.
- **It is matched as text.** `"01"` is `"01"`, the same as `[schema_keys]`
  and `LevelGroup.mapping`.
- **A stale alias is inert.** An alias for a level that is not in the data
  does nothing. It is not deleted, because the level may come back.
- **Synthetic factors** are keyed by their synthetic name (`ColName`,
  `Variant`; see `synthetic-factors.md`). A project `ColName` alias applies to
  that field name in every struct variable.

### Who owns what

| Concept | Owner | Consumers |
|---|---|---|
| The `[aliases]` grammar: read, render, validate | `scidb.aliases` | `ScidbSource`, the GUI config writer |
| Which config file | `scidb.schema_order.locate_config` (reused) | `scidb.aliases.project_aliases` |
| Writing `scistack.toml` | `scistack_gui.config._render_scistack_toml` (whole-file rewrite; carries `[aliases]` and `[schema_keys]`, `[aliases]` text from `scidb.aliases.render_aliases_table`) | Paths popup, `plot_project_alias_set` (via `scidb.aliases.with_alias`) |
| Project aliases → plotting | `ScidbSource._aliases_source` → `LongTable.aliases_source` (a live READER, set once at build) → `LongTable.project_aliases()` | `scistackplot.aliases.display_text` |
| Merge order + level/name text | `scistackplot.aliases` (`display_text` → `DisplayText`, on `ResolvedPlot.text`) | `reduce._build_figure`, both renderers, `x_axis_title` (and so codegen) |
| The labelable factors and levels shown in the GUI | `layout.meta.labelable` (Python) | the Plot Studio Labels section |

`scistackplot` never reads a config file itself; it must work without scidb.
A CSV or DataFrame source has an empty project layer.

### Why a table carries a reader, not the aliases (Stage 3)

`ScidbSource` gives each table it builds `LongTable.aliases_source`, a
zero-argument function that reads `db.dataset_aliases` (live, cached on the
file's mtime). Plotting code asks `table.project_aliases()` when it needs
them. A snapshot does not work for two reasons:

- A built table is **cached** by the source. A snapshot taken at build time
  would go stale on the first edit, and dropping the table to refresh it
  (as a `[schema_keys]` edit does) would rebuild data for a change of text.
- The plan cache in `reduce` keys on **`id(table)`**. Handing out a fresh
  copy with fresh aliases on each `get_table` call would miss that cache
  every time and re-reduce.

So an alias edit changes no table and no plan, and the next render reads the
new text. `BaseSource._attach_aliases_source` sets the reader once, as a
`replace` before the table is published to the memo. Derived tables
(`replace(table, …)`) carry it automatically. A CSV or DataFrame source
returns `None` from `_aliases_source`, so it has no project layer.

### Where the labels are applied, and why there

`resolve` merges the plot's and the project's aliases **once per resolve**
(`display_text`) and logs a summary. `_build_figure` tells that
`DisplayText` which factor plays which role in the figure (`for_figure`:
x layers, colour, sample colour, dash layers) and refuses the figure if two
drawn levels would read the same (`check_distinct`). It builds the labels
and an aliased copy of the nested-axis plan from it, then stores it as
`ResolvedPlot.text`. Renderers ask `resolved.text` for every level they
draw as text: `color_level`, `sample_level`, `x_tick`, `dash_id`,
`panel_title`. None of them calls `str(level)` for text any more. Before
this, about 50 sites did, and each was an owner of "how a level reads". The
guard test in `test_display_aliases.py` aliases every level and name to
`«…»`. It renders five figure shapes: a nested bar (ticks and brackets), a
faceted box (panel titles), an iterated bar (figure titles), a dashed band
(dash legend) and a coloured sample (sample legend). It fails if any raw
level or name survives in either renderer's visible text.

**Identity never changes.** `panel.title`, `figure_key`, `x_order`,
`x_plan.order`, colour levels, offsets and y-limit keys all stay raw. That
is why a facet layout rule written against `R_HAM` still matches, and why
aliasing needs no re-reduce. On a plain categorical plotly axis the
categories stay raw, and the aliased text is set as `ticktext` over them.

Not aliased: hover text, `ResolvedPlot.figure_label` (the GUI's figure
pager), and the plot-data CSV (raw by decision).

`aliases` is in `_PLAN_IRRELEVANT_FIELDS`, and the project layer is **not**
in `ScidbSource._cache_generation`. Editing an alias re-renders; it never
rebuilds a table or re-reduces. (Level order is different: it is baked into
tables, so it *is* in the generation.)

### Exported code bakes the labels in, and relabels the drawn text

The generated `plot_` function carries the **merged** aliases of the factors
it draws as a literal (`_aliases`, looked up by `_alias(factor, value)`). It
does not read `scistack.toml` at run time, because that would be a hidden
input outside lineage (the same reasoning as the literal `ylim` in
`codegen._y_limit_plan`). The consequence: **after editing a project alias,
re-export the step to update it.** The GUI preview and saved figures update
immediately.

It applies them the way the renderers do: **text only.** `df` keeps its
raw levels, and every emitted `order=`/`col_order=`/nested order, spaghetti
position, dash-id sort and overlay palette stays raw. Once everything is
drawn, the code relabels the text:

- x ticks through `_alias` (nested composed keys part by part on `" · "`);
- legend entries (colour levels, sample levels, dash ids part by part on
  `" | "`);
- the legend title and seaborn's column-name entries (the name aliases);
- facet y titles, per factor.

The axis titles and the overlay legend's title are written already aliased
(`x_axis_title`, and the y title from the measure's name alias). The relabel
runs before the fitted-tick replay, which then operates on the aliased
labels, the same ones the preview fitted.

**Why not rename the data (the original plan):** codegen emits many raw
literals. A rename would have to translate every one of them, and it would
move the dash styles, which the preview assigns by sorting the raw ids.
Relabelling keeps identity raw, as `ResolvedPlot.text` does.
`codegen._export_aliases` also runs `check_distinct`, so a spec the preview
refuses cannot be exported either. With no aliases, nothing is emitted: the
generated code is exactly what it was.

### The Labels section (GUI)

Below Figure size. **Title / X label / Y label** are the `style` fields; a
blank box deletes the key. Then one block per entry of
`layout.meta.labelable`, which is Python's list (`scistackplot.aliases.labelable`):
the measure, then every factor the figure draws as text, each with the `key`
an alias is written under (a grouping column's `Var.Column`, the key the
lookup reads first).

A box holds the **plot's** alias. An empty box shows, greyed, what the
figure draws without it: the project's alias ("… (project)") or the raw
text. "↑ project" writes the plot's value to `scistack.toml` and clears the
plot's copy; "✕ project" removes the project's.

The write path: `plot_project_alias_set` → `plot_service.set_project_alias`
→ `scistack_gui.config.set_project_alias`, which uses the one whole-file
writer and `scidb.aliases.with_alias` / `render_aliases_table`, then
forgets the mtime-cached parse. A pyproject.toml project and a project with
no config are refused as `{ok: false, error}` answers. The panel then
re-resolves the unchanged spec (`refreshResolve`), because nothing else
would redraw it. No table is rebuilt, since tables read the project live.

The plot-layer edits (`aliasEdit.ts`) delete a cleared key and drop an
emptied entry, so a reopened saved plot does not read as "● modified". The
plot-layer `""` ("show raw here over the project") exists in the spec and
the merge but has no GUI control yet.

## Diagnosing

- "My alias doesn't show": `grep "\[aliases\]" scidb.log` shows which config
  was read and the tables it declared. The per-resolve INFO line
  `labels: session 3/4 aliased (project 3, plot 0) …` shows what matched.
  A level that does not match is usually a text mismatch (`1` vs `"01"`).
- "The exported figure has the old label": the step was exported before the
  project edit. Re-export it.
- "The text is the wrong size": the INFO figure-size line lists the resolved
  sizes and marks pinned ones. A size that seems ignored is usually unset and
  derived from `base`.
- "Auto chose a small size": `grep "auto text size" scidb.log`. The INFO line
  names each element's size and the problem that stopped the next size up.
  At DEBUG, each trial layout's sizes and blame are listed, and the trial's
  own lines carry `[auto-size trial …]`. A binding of "plot area N% of the
  canvas" shrinks every element: the figure is too small for its text.
- "Preview and file disagree on size": the preview applies
  `layout_decisions["auto_text"]` from the size it decided at (the pane in
  Fit pane). Save decides at the size it writes.
