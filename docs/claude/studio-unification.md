# Studio unification: one concept, one owner, across Process, Plot and Stats

Plan and as-built notes: `.claude/plan-unify-process-plot-stats.md` (decisions
2026-11-01, all stages built the same day). Read this before adding a
control to Plot Studio, StatStudio or the function sidebar. If the concept
already shows on another surface, the control must come from the shared
owner below. Don't write a second copy.

## The principle

Making a figure and making a statistics table are the same task up to the
last step. Both pick **data**, then decide what each schema key **does**,
and only then produce an output. The canvas handles the same first concepts
for a processing step: which locations, which filters, which keys the step
runs once per.

```
            Process (function sidebar)   Plot Studio            StatStudio
View        —                            Step by step | Advanced (same switch)
Data        inputs on the canvas         Data group             Data group (Plot Studio's own)
Locations   Schema keys (same picker)    Schema keys            Schema keys
Filters     Filters (where=)             Filters                Filters
1st output  —                            Chart                  Measures
Roles       Schema level ("Separate")    Structure              Structure (Plot Studio's own)
Output      run → records                Statistics, Appearance Statistics (descriptives, tests), Format
Toolbar     Run / Show                   Format · Save · Copy · … · Export code · Add to pipeline
Right rail  —                            Saved plots · Presets  Saved tables · Presets (same components)
```

## Concept map: who owns what

| Concept | Owner | Consumers |
|---|---|---|
| What each role is CALLED, per surface (`figure`, `table`, `process`) | `scistackplot.model.role_labels` (`ROLE_LABELS`, `role_label_for`, `role_labels_payload`) | `capability.role_label` (Plot Studio's role options), `scistackstats.explain` (wizard titles and choices, via `concept_for` and `_step_factors(surface=)`), `stats_open` reply `role_labels` (StatStudio's design rail), `node_schema_level` reply `iterate_label` (function sidebar) |
| Which PlotSpec fields are the DESIGN | `scistackplot.model.design.DESIGN_FIELDS` | `copy_design`, `design_fingerprint`, the shared design store |
| The design a fresh figure or table opens on, for a variable | `scistack_gui.services.plot_service.opening_spec` (default_spec + stored variant pins) | `plot_describe`, `stats_open {variable}` |
| Plot → table settings | `scistackstats.seed.seed_from_plot` | every StatStudio opening, `stats_plot_guide` |
| The shared design of a linked saved plot + its stats tables | `scistackplotdb.designs` (`_plot_design`, a `VersionedStore` keyed by plot id; `link_design`, `save_design`, `load_design`, `apply_design`) | `saved_plot_service.open_plot/save`, `stats_service.open_stats/saved_save`, `design_service.update/apply` (RPCs `design_update`, `design_apply`), the plots bundle section |
| The spec a generated `stat_` function embeds | `scistackstats.codegen.embedded_spec` (inverse of `generate_stats_function`; `ast.literal_eval`, never runs it) | `stats_open {stat_function}` (the `stat_` node's 📊 Open in Stat Studio) |
| The Data and Structure controls | Plot Studio's own groups (`PlotStudio.tsx`) | Plot Studio; StatStudio mounts them with `<PlotStudio design={…}>` (design-only mode) |
| Rail building blocks (`Group`, `Section`) | `frontend/src/components/Studio/RailGroup.tsx` | Plot Studio, StatStudio |
| Saved-work rail and Presets | `PlotStudio/SavedPlotsRail.tsx`, `PlotStudio/PresetsSection.tsx` (`words` prop) | Plot Studio, StatStudio |
| Screens a figure shares with a table | `scistackstats.spec.SHARED_GUIDE_STEPS` (lead `GUIDE_STEPS`); `PLOT_GUIDE_STEPS` = shared + `figure` | `guide.guide` (table), `guide.plot_guide` (figure, RPC `stats_plot_guide`) |
| Every lay sentence on a guided screen | `scistackstats.explain` (`concept_for(id, surface)` swaps the role concepts' titles) | both step-by-step views; the frontend words nothing |
| The wizard UI and its navigation | `StatStudio/Guided/GuidedStudio.tsx`, `guideSteps.ts` | StatStudio; Plot Studio through `Studio/plotGuide.ts` (`asGuideSpec`, `roleChanges`) |
| `design_changed` broadcast rules | `frontend/src/components/Studio/sharedDesign.ts` | `StatsLink.tsx` (Plot side), `StatStudio.tsx` |
| Ways into a studio | `frontend/src/components/Studio/studioOpen.ts` (`STUDIO_ENTRY_POINTS`) | every caller of `open_plot_panel`; logged by the host (`[Studio] open` in the extension's output) and by the webview (`report_studio_open` → scistack.log) |

## The vocabulary

| Role (`scistackplot.Role`) | figure | table | process (Schema Level) |
|---|---|---|---|
| `GROUP` | Compare | Compare | — |
| `FACET` | Separate panels | Separate sections | — |
| `ITERATE` | Separate figures | Separate tables | Separate (one run per level) |
| `COLLAPSE` | Average over | Average over | — (unticked keys reach the function together) |

Process only has the ITERATE idea. An unticked Schema Level key is
**passed together**, not averaged: the function gets the rows. So the canvas
never says "Average over". `role_label_for(COLLAPSE, "process")` falls back
to the figure word with a warning, rather than inventing one.

Guard: `frontend/src/components/Studio/studioOpen.test.ts` fails if a
PlotStudio, StatStudio or Studio `.tsx` file holds a role word outside a comment.

## Entry points (Stage 2)

| Way in | Plot Studio | StatStudio | `from` |
|---|---|---|---|
| Variable sidebar | Open Plot Studio → | Open Stat Studio → | `variable-sidebar` |
| Right-click a variable | 📈 Plot | 📊 Statistics | `variable-menu` |
| Schema locations row | opens on that location | — (not yet: StatStudio has no location seed) | `location` |
| Endpoint node | `plot_` → 📈 Open in Plot Studio | `stat_` → 📊 Open in Stat Studio | `plot-node` / `stat-node` |
| Command Palette | SciStack: Plot Variable… | SciStack: Summarize Variable… | `palette` |
| From the other studio | — | Plot Studio's StatStudio button | `plot-studio` |

StatStudio opens in its own VS Code tab, so in a browser build its menu
items are disabled with a tooltip saying so (as Plot Studio's StatStudio
button already was).

## The shared design (Stage 6)

- **Linking.** `stats_open {seed, plot_id}` (Plot Studio's StatStudio button
  on a SAVED plot) calls `link_design`: the design row is created from the
  figure's spec. Nothing else creates one. An unlinked plot keeps its
  design in its saved envelope.
- **Reading.** Opening the newest version of a linked saved plot, or a
  saved table with `linked_plot`, applies the stored design
  (`apply_design` → `copy_design`: design fields only). An older plot
  version opens exactly as saved.
- **Writing.** Every design edit in either studio calls `design_update`
  (debounced 500 ms); the backend appends a version only when the
  `design_fingerprint` changed and answers `changed`. Saving the figure or
  the table writes it too (`write_if_linked`).
- **Live.** A real change is broadcast as `design_changed {plot_id, version,
  origin}`. The other tab ignores its own origin, calls `design_apply` on its
  current spec and adopts the result. Its own `design_update` then answers
  `changed: false`, so the echo stops.
- **Gone:** `stats_divergence`, `stats_sync`, both Sync banners, and the
  `plot_design_sync` / `stats_design_sync` messages.

## Plot Studio's step by step (Stage 5)

- `stats_plot_guide {spec}` seeds the figure as a table, then
  `scistackstats.plot_guide` builds Measure, Independent unit and What to
  compare (the SAME builders as the table's wizard, with figure nouns)
  plus `figure` ("Your figure": the design restated, then a pointer to
  Advanced). No test runs, and csv-stats is never imported.
- The view is per viewer (`scistack.plotStudio.view` in localStorage): a
  fresh figure opens the way the last one was left, step by step the first
  time. A saved plot and a `plot_` node's figure open in Advanced.
- A wizard choice is turned back into `setRole` calls (`roleChanges`), so a
  new grouping layer is still placed by depth (`groups.placeGroupLayer`).

## Traps

- **Design-only Plot Studio.** `PlotStudio design={spec, onChange,
  roleLabels, between, onDescribe}` renders only Data, the `between` slot,
  and Structure, plus their pickers. It never calls `plot_resolve`, only
  `plot_capabilities`. The spec round trip is compared as JSON in both
  directions; a new prop that changes `design.spec` on every render would
  loop.
- **Variant pins.** Editing Variants in StatStudio's Data group writes the
  variable's `variant_selection` pins, exactly as in Plot Studio (it is the
  same control).
- **Bundles.** Both vite targets must be rebuilt after any `.tsx` change,
  and `extension/dist/extension.js` after any `extension/src` change
  (Linux esbuild from the scratchpad; see the frontend bundle memory).
- **Wording.** The new lay sentences (`_FIGURE_LAY`, `_step_figure`) are
  drafts for the user's wording review, like the rest of `explain.py`.
