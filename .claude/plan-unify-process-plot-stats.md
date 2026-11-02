# Plan: unify Process, Plot and Stats in the GUI

Status: ALL STAGES BUILT 2026-11-01 (user asked for every stage after the
decisions). Frontend tsc + npm test (681) pass, both bundles and the extension
rebuilt; pytest passes (user, 2026-11-01). GUI not visually checked
(docs/gui-manual-testing-todo.md 0zzzzzzy). Reference: docs/claude/studio-unification.md.

Goal: making a figure and making a statistics result should take the same
steps, in the same order, with the same words and the same controls. The
canvas (Process) should use those same words and controls wherever it handles
the same concepts.

## What's different today (findings)

| Concept | Process (canvas sidebar) | Plot Studio | Stat Studio |
|---|---|---|---|
| Where you start | a function node | variable (sidebar button, right-click 📈 Plot), location, `plot_` node, palette, CSV | **only** the Plot Studio toolbar, or a saved table |
| The data | inputs wired on canvas | Data > **Variants** rows (DagPicker), `variant_sets` | **Measures** list + Add (`dvs`), different control |
| Which locations | **Schema Selection** | Data > **Schema keys** (SchemaLocationPicker) | inherited from the plot, no control |
| Filters | **Data Filters** | Data > **Filters** | inherited, no control |
| Roles of keys | **Schema Level** ("runs once per") | Group / Separate panels / Separate figures / Collapse (average) | rail: Group (columns) / Separate sections / Separate tables / Collapse (average); wizard: Compare / Analyse separately / Average over |
| Guidance | none | none (ⓘ hints) | Step by step wizard (default) + Advanced |
| Window | canvas + sidebar | left rail (5 groups) · figure + toolbar · right rail (Saved, Presets) | left rail (7 sections incl. Save + presets) · results + export toolbar |
| Save / presets | — | right rail | left rail Save section |
| Output actions | Run / Show | toolbar: Save image, CSV, Export code, Add to pipeline | toolbar: Copy, Save to file…, Add to pipeline |
| Back from the pipeline | — | `plot_` node → 📈 Open in Plot Studio | `stat_` node: no way back into Stat Studio |
| Design between a figure and its table | — | own copy (PlotSpec) | own copy (`StatsSpec.design`), kept in step by **Sync** banners |

NOTE 4 violations found:

- **Role labels have three owners**: `scistackplot.capability._ROLE_LABELS`,
  hard-coded `ROLE_CHOICES` in `StatStudio.tsx:91`, and
  `scistackstats.explain` (`role.compare/separate/average`).
- **The design of a linked figure + table has two owners** (two copies plus
  sync banners, `stats_divergence`). Earlier locked decision: link on the
  stats side only, sync via `DESIGN_FIELDS`. Q2 asks whether to revisit.

## Target shape: one "endpoint studio" flow, two outputs

Both studios become the same shell with the same first half:

```
1 Data      what is analysed        Variants rows · Schema keys · Filters      (shared component)
2 Design    how keys are used       Grouping · Combine · Compare · Roles       (shared component)
3 Output    figure  | table         Chart type…           | Descriptives · Tests…
4 Finish    looks   | format        Appearance            | Format · Brackets
toolbar     Save · Copy · Export code · Add to pipeline   (same buttons, same order)
right rail  Saved · Presets                               (same rail)
```

Steps 1–2 are literally the same React components writing the same
`PlotSpec` design fields (`scistackplot.design.DESIGN_FIELDS` stays the owner
of which fields those are). Only steps 3–4 differ.

## Stages

Each stage: logging first, tests guarding the owner, both bundles rebuilt,
GUI tutorial pages + `docs/gui-manual-testing-todo.md` updated.

### Stage 0 — diagnostics
- Log every studio open with its entry point (`[Studio] open kind=plot|stat
  from=variable|node|plot|saved|palette`), so we can see which paths users take.
- Guard test (like `guideSteps.test.ts`): no role label strings in `.tsx`.

### Stage 1 — one vocabulary owner
- One table of role labels per surface in `scistackplot.roles` (role ×
  {figure, table, wizard, process}); `capability`, `scistackstats.explain`
  and the Stat Studio rail all read it; StatStudio gets labels from the
  backend payload. Proposed words in Q1.
- Process: Schema Level wording shares the "Separate …" term for ticked keys.
- Rename sidebar sections so the three surfaces match: Schema Selection ↔
  Schema keys, Data Filters ↔ Filters (one name each; Q1).

### Stage 2 — same entry points
- Stat Studio opens from everywhere Plot Studio does: variable sidebar
  (**Open Stat Studio →** next to Open Plot Studio →), right-click
  **📊 Statistics**, Schema Locations, palette **SciStack: Summarize Variable…**.
- Backend: `seed_from_variable` = the Plot Studio default spec builder (one
  owner of "default design for a variable") → `seed_from_plot`. No second
  default-design rule.
- `stat_` node → **📊 Open in Stat Studio** (spec recovered from the embedded
  spec in the generated function), mirroring `plot_` nodes.

### Stage 3 — shared Data + Design sections
- Extract Plot Studio's Data and Structure groups into shared components
  (`components/Studio/DataSection.tsx`, `DesignSection.tsx`).
- Stat Studio's Measures + Design sections are replaced by them. Measures
  become Variants rows (same DagPicker, + columns for table variables).
- Stat Studio gains Schema keys and Filters controls (today only inherited).

### Stage 4 — same shell
- Shared `StudioShell`: left rail groups (collapsible, same summaries
  mechanism as `sidebarGroups.ts`), centre pane + toolbar, right rail.
- Stat Studio: Save + presets move to the right rail; toolbar = Save ·
  Copy · Save to file · Export code · Add to pipeline, same order as Plot.
- Plot Studio toolbar gets **Copy** as text label parity (already has Copy PNG).

### Stage 5 — guided parity (depends on Q3)
- Option A: Plot Studio gets a Step by step view. Its first screens
  (Measure, Independent unit, What to compare) ARE the Stat Studio screens,
  built by the same `scistackstats.explain` builders moved to a shared
  layer, then plot-specific screens (Chart type, Summary, Show sample).
- Option B: no wizard in Plot; Stat's wizard stays its own.

### Stage 6 — one design owner for linked figure + table (depends on Q2)
- A saved **design** row (`scistackplotdb`) that a saved plot and a saved
  table both reference. Editing the design in either studio edits the one
  row; the sync banners and `stats_sync` go away (kept only for unlinked
  copies). Needs care with versions: saved plots/tables pin a design version.

### Stage 7 — Process alignment
- Function sidebar's Schema Selection and Data Filters use the shared
  Data components (same picker, same labels).
- Endpoint nodes (`plot_`, `stat_`) on the canvas look and behave the same:
  same badge, Run/Show, Open in studio.

## User decisions (2026-11-01)

- Q1 vocabulary: **Compare · Separate panels|sections · Separate
  figures|tables · Average over**. The verb stays fixed and the output noun
  comes from the surface. Ticked Schema Level keys on the canvas read
  "Separate".
- Q2: **one shared saved design** (Stage 6). This supersedes the earlier
  "link on stats side only + sync" decision (record it as a new ADR).
- Q3: **yes**: Plot Studio gets Step by step, and its first screens are
  the Stat Studio screens (Stage 5 option A).
- Q4: build **Stages 0–4 first**, then pause for the user's visual check
  before Stages 5–7.

## As built (2026-11-01)

- **Stage 0.** `report_studio_open` (api/plot.py) → `[Studio] open kind= from=`
  in scistack.log; the extension logs the same per tab; entry-point list
  `frontend/src/components/Studio/studioOpen.ts`; guard test there fails on a
  role word in a studio `.tsx`.
- **Stage 1.** `scistackplot/role_labels.py` (`ROLE_LABELS`, `role_label_for`,
  `role_labels_payload`); `capability.role_label` delegates; `explain.py`
  titles and choices read it (`concept_for`, `_step_factors(surface=)`);
  `stats_open` returns `role_labels`; `node_schema_level` returns
  `iterate_label`; function sidebar sections renamed Filters / Schema keys.
- **Stage 2.** `plot_service.opening_spec` (one owner of the opening design);
  `stats_open {variable}` uses it; `stats_open {stat_function}` +
  `scistackstats.codegen.embedded_spec`; DAG menus (📊 Statistics, 📊 Open in
  Stat Studio), sidebar Open Stat Studio →, palette Summarize Variable….
  Not done: opening StatStudio on ONE schema location (no location seed yet).
- **Stage 3.** `PlotStudio design={spec, onChange, roleLabels, between,
  onDescribe}` design-only mode; StatStudio mounts it for Data + Structure.
- **Stage 4.** `Studio/RailGroup.tsx` (Group, Section); `SavedPlotsRail` and
  `PresetsSection` take `words`; StatStudio toolbar order matches Plot
  Studio's; new RPCs `stats_export_code`, `stats_preset_rename`.
- **Stage 5.** `SHARED_GUIDE_STEPS` / `PLOT_GUIDE_STEPS` (spec.py),
  `explain.build_plot_steps` + `_step_figure`, `guide.plot_guide`, RPC
  `stats_plot_guide`; Plot Studio Step by step | Advanced switch reusing
  `GuidedStudio` (`surface="figure"`) through `Studio/plotGuide.ts`.
- **Stage 6.** `scistackplotdb/designs.py` (`_plot_design` VersionedStore,
  in the plots bundle section); `services/design_service.py` + RPCs
  `design_update` / `design_apply`; saved plot open/save and stats
  open/save read and write it; `Studio/sharedDesign.ts` broadcast rules;
  `stats_divergence`, `stats_sync` and both Sync banners removed.
- **Stage 7.** Endpoint nodes were already alike (badge, Run/Show); the
  function sidebar already used the same location picker. Its `where=`
  Filters stay their own control (record conditions on variables, not the
  plot's level filters): same name, different concept, not merged.

Tests to run (user): scistackplot `test_role_labels.py`,
`test_role_availability.py`; scistackplotdb `test_shared_designs.py`,
`test_saved_plots.py`; scistackstats `test_explain.py`, `test_codegen.py`;
scistack-gui `test_stats_service.py`, `test_client_timing.py`.
