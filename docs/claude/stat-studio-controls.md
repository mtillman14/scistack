# StatStudio — control map

What each StatStudio control writes, and who owns the answer. The webview
(`frontend/src/components/StatStudio/StatStudio.tsx`) edits a
`scistackstats.StatsSpec` dict through `statsSpec.ts`. It formats nothing:
tables, captions and option lists come from `stats_compute` (`Grid`s +
`catalog`).

## Two views of one spec

**Step by step** (`Guided/GuidedStudio.tsx`, the default for a new table) and
**Advanced** (the rail below) edit the same `StatsSpec`. The view and the
wizard's progress are stored in `spec.guide`, so a saved table reopens in its
view. The wizard is described in [statstudio-guided.md](statstudio-guided.md).

## Opening

The same ways in as Plot Studio ([studio-unification.md](studio-unification.md)).

| From | How | Backend |
|---|---|---|
| Plot Studio toolbar **StatStudio** | `open_stat_panel` (host) → new tab `view: 'stat'` | `stats_open {seed, plot_id}` → `seed_from_plot`; a saved plot is LINKED (`link_design`: one shared design) |
| Variable sidebar **Open Stat Studio →**, right-click **📊 Statistics**, palette **SciStack: Summarize Variable…** | `open_plot_panel {view: 'stat', variable, from}` | `stats_open {variable}` → `plot_service.opening_spec` (the figure's own opening design) → `seed_from_plot` |
| `stat_` node **📊 Open in Stat Studio** | `open_plot_panel {view: 'stat', statNode}` | `stats_open {stat_function}` → `scistackstats.embedded_spec` of the generated source |
| A saved table (right rail ▸ Saved tables) | in this tab | `stats_open {stats_id}` (salvage, notes shown; shared design applied) |

## Rail (Advanced)

Groups in the order Plot Studio's are, built from the same `Studio/RailGroup`
blocks. **Data** and **Structure** are Plot Studio's own groups, mounted with
`<PlotStudio design={…}>` on `spec.design` (design-only mode: no figure, no
resolve). Role words come from `stats_open`'s `role_labels`.

| Group > Section | Control | Writes | Owner of the meaning |
|---|---|---|---|
| Data (Plot Studio's) | Variants rows, Schema keys, Filters, X variable, Time zone | `design.variant_sets`, `.location_filter`, `.filters`, … | as in plot-studio-controls.md |
| Measures (where Plot Studio has Chart) | list / ✕ / Add (+ columns) | `dvs` (`DV.variable`, `DV.columns`) | `tables.analysis_spec` (ColName → filter + ITERATE) |
| Structure (Plot Studio's) | Combine, Grouping (Compare), Compare to reference, Factors (Separate tables / Separate sections / Average over), Layout | `design.roles`, `design.groups`, `design.level_groups`, … | plot roles (`scistackplot.model.roles`), words `scistackplot.model.role_labels` |
| Statistics > Descriptives | Cell, statistic checkboxes | `descriptives.cell`, `.statistics` | `describe.CELL_LAYOUTS`, `STATISTICS`; numbers `csvstats.descriptive` |
| Statistics > What the tests see | readout chips (within/between with evidence) + override; Variants: Pinned / Sweep | `comparisons[0].overrides`, `variant_mode` | `design.derive_design`, `tables.analysis_spec(sweep=)` |
| Statistics > Comparisons | family (default "Recommended from the data"), the Recommended box (tests + evidence), Missing data (use every observation / complete cases), pairwise/omnibus, test overrides (any test; greyed only for the data's shape, reason on hover), equal var, correction, family scope, effect size, difference spread, α, CI | `comparisons[i].*` (incl. `missing`) | `catalog` (`recommend`, `why_unavailable`), `effects`, `run` |
| Statistics > Comparisons | one-line model formula (the first analysis' from `readout[].model`, rebased on the design) + **Model formula…** | — (the popup writes) | `scistackstats.model` |
| Model formula popup | text editor (`<dv> ~` fixed), errors underlined at their span, resolved terms, Add covariate… (GroupingDagPopup `purpose="covariate"`), Add random term…, Reset to default, Apply | `comparisons[i].formula`, `.covariates`; factor changes → `design.roles`/`groups` | `stats_formula_check` (live) / `stats_formula_apply` → `model.check`, `model.apply_to_design`; grammar `csvstats.lmm_formula` |
| Format > Numbers | decimals (auto), p decimals, stars | `formatting` | `format` |
| Format > Brackets on the figure | show, pairs, label | `overlay` | `overlays.bars_for` |

**Right rail** (Plot Studio's `SavedPlotsRail` + `PresetsSection` with
`words`): Saved tables (save, open, rename, remove; `_stats_spec` rows) and
Presets (`_stats_preset` rows, `stats_preset_rename` included), owner
`scistackstatsdb.saved`.

## Main pane

- **One shared design** with the linked saved plot (`scistackplotdb.designs`):
  a design edit → `design_update` → broadcast `design_changed`; an incoming
  one → `design_apply`. A one-line note says the two share one design. No
  banner and no Sync button (the old `stats_divergence` / `stats_sync` are gone).
- **Decision banner** (amber, both tabs): one per (analysis, factor) from
  `strata[].action` (`catalog.needed_action`), with a button per choice that
  sets the analysis override. The Comparisons tab shows a "!" badge and the
  design chip says "needs a decision" while one is pending. Collected by
  `StatStudio/statsActions.ts` (`pendingActions`, `recommendedFor`).
- **Tabs**: Descriptives (one Grid per run of DVs sharing columns) |
  Comparisons (primary grid, omnibus grid, sweep grid, APA sentences).
- **Toolbar** (Plot Studio's order): tabs, then Format, Save to file…
  (`pick_save_path` host), Copy (html = Word-pasteable rich table), Export
  code (`stats_export_code` → `generate_stats_function`, shown in an overlay),
  Add to pipeline (`stats_add_to_pipeline`); the last two are disabled with
  `not_exportable` as the tooltip.

## Plot Studio side (`PlotStudio/StatsLink.tsx`)

- StatStudio button (`open_stat_panel`, `from: plot-studio`).
- The shared design: every edit → `design_update` (a no-op when unlinked);
  a change is broadcast as `design_changed`; StatStudio's arrive the same way
  and go through `design_apply` onto the CURRENT spec.
- Test brackets: `stats_overlay_for_plot` → `derived_bars` → sent on
  resolve, export, save and add-to-pipeline (`api/plot._with_derived`).

## Not yet

Hiding one bracket from the GUI (supported in `overlay.hidden`, no
control); SPM cluster shading on the figure; a plotted specification curve
(the sweep is a table); long runs on a job with progress (P8; compute is
one RPC); compact letter display and the other deferred publication
extras (statistics-design.md S19).
