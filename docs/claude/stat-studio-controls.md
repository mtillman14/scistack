# StatStudio — control map

What each StatStudio control writes, and who owns the answer. The webview
(`frontend/src/components/StatStudio/StatStudio.tsx`) edits a
`scistackstats.StatsSpec` dict through `statsSpec.ts`. It formats nothing:
tables, captions and option lists come from `stats_compute` (`Grid`s +
`catalog`).

## Opening

| From | How | Backend |
|---|---|---|
| Plot Studio toolbar **StatStudio** | `open_stat_panel` (host) → new tab `view: 'stat'` | `stats_open {seed, plot_id}` → `seed_from_plot` (linked when the plot is saved) |
| A saved table (rail ▸ Save list) | in this tab | `stats_open {stats_id}` (salvage, notes shown) |

## Rail

| Section | Control | Writes | Owner of the meaning |
|---|---|---|---|
| Measures | list / ✕ / Add (+ columns) | `dvs` (`DV.variable`, `DV.columns`) | `tables.analysis_spec` (ColName → filter + ITERATE) |
| Design | role per factor (Group / Separate tables / Separate sections / Collapse), ↑↓ | `design.roles`, `design.groups` (innermost first) | plot roles (`scistackplot.roles`) |
| Design | Variants: Pinned / Sweep | `variant_mode` | `tables.analysis_spec(sweep=)` |
| Design | readout chips (within/between with evidence) + override | `comparisons[0].overrides` | `design.derive_design` |
| Descriptives | Cell, statistic checkboxes | `descriptives.cell`, `.statistics` | `describe.CELL_LAYOUTS`, `STATISTICS`; numbers `csvstats.descriptive` |
| Comparisons | family (default "Recommended from the data"), the Recommended box (tests + evidence), Missing data (use every observation / complete cases), pairwise/omnibus, test overrides (any test; greyed only for the data's shape, reason on hover), equal var, correction, family scope, effect size, difference spread, α, CI | `comparisons[i].*` (incl. `missing`) | `catalog` (`recommend`, `why_unavailable`), `effects`, `run` |
| Format | decimals (auto), p decimals, stars | `formatting` | `format` |
| Brackets on the figure | show, pairs, label | `overlay` | `overlays.bars_for` |
| Save | name + Save; saved list; presets | `_stats_spec` / `_stats_preset` rows | `scistackstatsdb.saved` |

## Main pane

- **Banner** (linked + diverged): `stats_divergence` (`scistackplot.design.design_diff`).
  **Sync to PlotStudio** = `stats_sync to_plot` → `plot_saved_save` (new
  version of the linked saved plot) → `broadcast_panels stats_design_sync`.
- **Decision banner** (amber, both tabs): one per (analysis, factor) from
  `strata[].action` (`catalog.needed_action`), with a button per choice that
  sets the analysis override. The Comparisons tab shows a "!" badge and the
  design chip says "needs a decision" while one is pending. Collected by
  `StatStudio/statsActions.ts` (`pendingActions`, `recommendedFor`).
- **Tabs**: Descriptives (one Grid per run of DVs sharing columns) |
  Comparisons (primary grid, omnibus grid, sweep grid, APA sentences).
- **Toolbar**: format + Copy (html = Word-pasteable rich table), Save to
  file… (`pick_save_path` host), Add to pipeline (`stats_add_to_pipeline`,
  disabled with `not_exportable` as the tooltip).

## Plot Studio side (`PlotStudio/StatsLink.tsx`)

- StatStudio button; banner "Stats table X differs (…)" + **Sync to
  StatStudio** (`stats_sync to_stats` → `stats_saved_save` → broadcast
  `plot_design_sync`).
- Test brackets: `stats_overlay_for_plot` → `derived_bars` → sent on
  resolve, export, save and add-to-pipeline (`api/plot._with_derived`).
- Incoming `stats_design_sync` → `stats_sync to_plot` onto the CURRENT spec.

## Not yet

Hiding one bracket from the GUI (supported in `overlay.hidden`, no
control); SPM cluster shading on the figure; a plotted specification curve
(the sweep is a table); long runs on a job with progress (P8; compute is
one RPC); compact letter display and the other deferred publication
extras (statistics-design.md S19).
