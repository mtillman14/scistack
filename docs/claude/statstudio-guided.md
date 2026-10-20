# StatStudio Guided ("Step by step") — how it works

Plan: `.claude/plan-statstudio-guided.md` (approved 2026-10-20). Read
[statistics.md](statistics.md) first for the statistics owners. This page
covers only the wizard.

## Shape

```
StatsSpec (one spec, two views)
  .guide = GuideState(mode "guided"|"advanced", step, done)   ← view state only; compute ignores it
        │
        ├── stats_preview  (design steps: no test, no csv-stats import)
        │      guide(spec, tables)                   → {"steps", "concepts", "has_results": False}
        └── stats_compute  (steps with needs_results, and the Advanced view)
               run(...) ; guide(spec, tables, report) → same payload, has_results True
                                │
   scistackstats.guide.guide_facts ── facts from their owners (tables, design, catalog, run, effects)
   scistackstats.explain.build_steps ── the lay wording: one StepGuide per spec.GUIDE_STEPS id
                                │
   frontend Guided/GuidedStudio.tsx ── lays the steps out; guideSteps.ts: navigation + applyAction
```

## Owners

| Concept | Owner |
|---|---|
| Step ids and their order | `scistackstats.spec.GUIDE_STEPS` |
| Persisted view state (mode, step, done) | `scistackstats.spec.GuideState` (`StatsSpec.guide`) |
| "New tables open guided" | `scistackstats.seed.seed_from_plot` |
| "Old saves open advanced" | `GuideState` default + `restore._salvage_block` |
| Every lay sentence, concept, choice label | `scistackstats.explain` (`CONCEPTS`, step builders) |
| The facts behind them | `scistackstats.guide.guide_facts` (reads `tables`, `design`, `catalog`, `run`, `effects`) |
| Cell counts per factor-level combination | `scistackstats.design.cell_counts` (empty_cells reads it) |
| Effective role of each factor as the rows were read | `AnalysisFrame.roles` (`tables.analysis_frame`) |
| Navigation (back to done steps, never past a blocked one) | frontend `Guided/guideSteps.ts` (`canGoTo`, `next`, `clampToBlocked`) |
| Turning a choice into a spec edit | frontend `guideSteps.applyAction` → `statsSpec.ts` helpers |
| What the statistics see (no view state) | frontend `statsSpec.computeKey` |

## Action vocabulary (explain → applyAction)

`set_role {factor, role}` · `set_primary {factor}` (innermost grouping
layer) · `set_override {analysis, factor, kind|null}` · `set_analysis {patch}`
(a null value clears the field to its default) · `exclude_level {factor,
level}` (a filter's `include` loses it, else `exclude` gains it). A choice
with `action: null` keeps things as they are.

## Traps

- **The frontend words nothing.** `guideSteps.test.ts` greps the Guided
  `.tsx` files for statistics terms. Add wording to `explain.py`, never to
  the component.
- **`stats_preview` must not call `catalog()`**: its correction list asks the
  engine, which is csv-stats' 1-2 s cold import.
- **Moving between steps must not recompute.** The compute effect keys on
  `computeKey(spec)` (the spec without `guide`). A full compute covers the
  preview for the same key (`haveResultsFor`).
- **A new guide can block an earlier step** (an edit made an answer
  incomplete). `clampToBlocked` pulls the wizard back, and the parent
  applies it on every new guide payload.
- **Steps that read results** (`needs_results`) show "Running the tests…"
  until `has_results`. Next is disabled while waiting.
- **The unit step offers choices only when there is no unit.** Changing the
  unit happens in "What to compare" (Average over). The outermost averaged key
  is the unit (`roles.collapse_steps`).
- **Empty cells vs incomplete units.** The units of a group that was never
  measured at a level also count as incomplete (structural, not missing
  data). The data-check step says so in a sentence.
- Wording status: drafted 2026-10-20; **the user reviews it in a separate
  pass** before it ships (plan Q3).
