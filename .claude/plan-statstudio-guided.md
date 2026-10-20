# Plan — StatStudio Guided mode (a step-by-step wizard in plain language)

Status: APPROVED 2026-10-20. Decisions: Q1 Guided is the default for new tables; a saved table reopens in the mode it was saved in. Q2 can jump back to any done step, never forward past a blocked one. Q3 the user reviews the lay wording in a separate pass before it ships. Q4 Stage 0 is in this plan and goes first.

## Goal

Someone with no statistics training opens StatStudio from a figure and gets
walked through designing the analysis **one decision at a time**. Each step
has a plain-language explanation of the idea behind the decision, shown
prominently, and that explanation uses **their own data** ("Each of your 22
participants was measured in every session, so...").

The backend stays as it is. The wizard edits the same `StatsSpec` the current
rail edits, so switching between **Guided** and **Advanced** (today's rail) is
free and loses nothing.

## Principles

1. **Same spec, two views.** Guided and Advanced read and write one
   `StatsSpec` through the existing `statsSpec.ts` helpers. The wizard never
   owns separate state. There's no new saved format, and save/presets/sync
   work unchanged.
2. **The frontend still knows no statistics vocabulary.** This is the rule in
   StatStudio.tsx's header comment. Every explanation, both general and
   data-specific, comes from **one owner in scistackstats** (`explain.py`,
   new). Python exports, captions and a future MATLAB/CLI surface can reuse
   it, and the GUI and those surfaces can't drift apart (NOTE 4).
3. **One decision per screen**, with a recommended answer already selected
   and a reason given. "Just use the recommendation" should take about 6
   clicks of Next.
4. **Data before tests.** The wizard shows the shape of the data (who was
   measured in what, what's missing, which combinations are empty) BEFORE
   proposing a test. The 2026-10-20 log is the motivating case: a full
   `session*group*minute` LMM was proposed on data with empty cells, and all
   135 comparisons were refused.
5. **Blocking is explicit.** A step with an unresolved decision
   (`catalog.needed_action`, empty cells) can't be skipped. Its explanation
   says why and what each choice means.

## The steps

Each step = a question (headline) + choices as large cards + an explanation
panel ("What does this mean?") + a "Your data" panel (live numbers).

| # | Step (snake_case id) | Question shown | Writes | Data-specific explanation from |
|---|---|---|---|---|
| 1 | `measure` | "What did you measure?" | `dvs` | DV name, units, n records, % missing (the plotdb numeric-coverage WARN, e.g. Dist_m 618/1428) |
| 2 | `unit` | "Who or what is one independent thing you measured?" (subject) | the collapse/sample key in `design.roles` | `tables.analysis_frame` UNIT: "22 subjects; their repeated measurements are linked, so we count each subject once" |
| 3 | `factors` | "What are you comparing?" Per schema key: **Compare** / **Analyze separately for each** / **Average over** | `design.roles`, `design.groups` | levels per key; a warning when a key multiplies the cells (6 minutes × 5 sessions × 2 groups = 60 cells for 22 people) |
| 4 | `within_between` | "Was every subject measured at every level?" (repeated vs separate groups), one card per factor | `comparisons[i].overrides` + `needed_action` choices | `derive_design` evidence (`units_multi/units_single`): "every subject is in only one group → separate groups" |
| 5 | `data_check` | "Is anything missing?" A who-has-what grid (cells × n) + missing-data policy | `comparisons[i].missing` | `incomplete_units`, cell counts from describe (`n per cell 0..15`), **empty-combination check (new)** with remedies offered as buttons ("analyze each session separately", "drop the session × group interaction") |
| 6 | `question` | "What is your main question?" Choose the primary factor + whether to run the overall test first | `comparisons[i].primary`/omnibus | `comparison_tiers`: "Main question: do CBrace and Traditional differ? We'll compare them at each session." |
| 7 | `test` | "Which test?" The recommendation is preselected; other tests are behind "Choose a different test" | `family`, `pairwise_test`, `omnibus_test` | `catalog.recommend` evidence put into lay terms: "The values are skewed (not bell-shaped), so we use a rank-based test that doesn't assume they are" |
| 8 | `multiple_comparisons` | "You're making 24 comparisons. How should we guard against false alarms?" | `correction`, `family_scope` | `run.family_statement` + count: "with 24 tests at 5%, about 1 'significant' result is expected by chance alone" |
| 9 | `effect_size` | "How should the size of the difference be reported?" | `effect_size`, `difference spread`, α, CI | `scistackstats.effects` names → lay: "Hodges–Lehmann shift = the typical difference between groups, in metres" |
| 10 | `results` | "Your results" — the existing grids + APA sentences + a "How to read this" line per column | — | `format` grids (unchanged) + `explain.read_results` |

Advanced-only (no wizard step; reachable through "Open in Advanced"): model
formula / covariates / random terms, variant sweep, format, brackets, presets.
Optional future steps: `covariates` (after 6) and `brackets` (after 10).

### Layout (one screen)

```
┌ Step rail ─────┐┌ Question ───────────────────────────┐┌ What does this mean? ─┐
│ ✓ Measure      ││ Was every subject measured at       ││ (prominent, always    │
│ ✓ Unit         ││ every level?                        ││  open, plain words,   │
│ ● Within/betw. ││  [card: session — repeated ✓ rec.]  ││  "Learn more" expands │
│ ○ Data check   ││  [card: group — separate groups]    ││  the technical name)  │
│ ○ ...          ││ ┌ Your data ─────────────────────┐  │├───────────────────────┤
│                ││ │ 22 subjects; 12 missed ≥1 visit │  ││ Why we recommend this │
│ [Advanced ⇄]   ││ └────────────────────────────────┘  ││ (evidence, in lay     │
│                ││              [Back]  [Next →]       ││  terms)               │
└────────────────┘└─────────────────────────────────────┘└───────────────────────┘
```

At narrow widths the explanation panel stacks above the cards. Technical
terms always show beside the lay term the first time ("repeated measures
(*within-subject*)"), so users learn the vocabulary the results table uses.

## Backend additions (additive only; nothing existing is undone)

### B1. `scistackstats/explain.py` — the one owner of lay text
- `CONCEPTS: dict[str, Concept]`. Each `Concept` has `title`, `lay` (1–3
  sentences), `technical_name` and `learn_more`, covering unit,
  pseudoreplication, within/between, mixed membership, missing data
  (use_all / complete_cases), empty cells, omnibus vs pairwise, each test in
  `catalog`, normality, equal variances, multiple comparisons, each
  correction, each effect size, p-value, CI, α.
- `explain_step(step_id, spec, compute_result) -> StepExplanation`
  (headline, concept ids, data-specific sentences, recommendation +
  lay reasons, blocking reason or None). It is built ONLY from the existing
  outputs (`derive_design`, `recommend` evidence, `needed_action`,
  `incomplete_units`, `family_statement`), so the facts keep one owner and
  `explain` owns only the wording.
- `read_results(grid) -> per-column "how to read this"`.
- An AST/import guard test: `explain` imports no csvstats, and the frontend
  has no hard-coded statistics terms (a vocabulary lint over
  `StatStudio/Guided/*.tsx`, e.g. no "Shapiro", "Holm", "within-subject").

### B2. Estimability check (fixes what the log showed)
- `scistackstats.design.empty_cells(frame, factors)` → the factor-level
  combinations with zero rows (owner: design, beside `incomplete_units`).
- `catalog.recommend` consults it. When the full-interaction fixed design is
  rank deficient it does NOT recommend a model csv-stats will refuse. It
  returns a `needed_action` ("empty_cells") with remedies: analyze a factor
  separately, drop the interaction, or exclude a level. Remedies are applied
  through existing spec fields (roles → iterate, formula without the
  interaction, filters).
- Also: fix the false `group identifies the experimental unit` model problem
  (a between factor that a nested unit `group / subject` sits under is not
  the unit).
- Logging: one INFO line per stratum with the empty cells; tests for the
  C-Brace shape (one session × group cell empty in every minute).

### B3. `stats_compute` gains `guide`
- The result gets `guide: {steps: [StepExplanation...], concepts: {...}}`.
  Cheap: it is text over data the compute already has.
- **`stats_preview`** (new RPC): rows + design + readout + empty cells +
  explanations, **without running tests**. Steps 1–6 call it so moving
  between early steps doesn't pay the ~1–3 s test run. Steps 7+ call
  `stats_compute`. Log the timing of both (`[timing] stats_preview`).
- Instrument the unexplained 1.7 s gap seen before `engine csvstats 0.4.2`
  (cold csvstats import) with a `[timing]` line. Optionally warm-import it
  when StatStudio opens.

## Frontend (scistack-gui/frontend/src/components/StatStudio/)

- `Guided/GuidedStudio.tsx` — the shell: step rail, question pane,
  explanation pane, Back/Next.
- `Guided/steps.ts` — PURE: step order, `stepStatus(spec, guide)`
  (done / current / blocked / skipped), next/previous, which steps apply
  (e.g. skip `within_between` when there's one factor). Vitest-tested.
- `Guided/steps/*.tsx` — one small component per step. They use only
  `statsSpec.ts` helpers to write (extended where a step needs a new
  setter) and render text from `guide`.
- `Guided/ExplainPanel.tsx` — renders a `StepExplanation` + concepts
  ("Learn more" expands).
- `StatStudio.tsx` — a Guided | Advanced toggle in the header. The default
  is Guided for a new stats table and Advanced for a saved one opened from
  the list (open question Q1). The mode is remembered per user in
  localStorage-equivalent webview state.
- Logging: every step transition and choice goes to scidb.log via the
  existing client log RPC (`[stats guided] step=within_between choice=...`).
  These are named steps, never "Step N".
- Rebuild BOTH vite targets (the frontend-bundle trap) and update
  `docs/gui-manual-testing-todo.md` with a new section.

## Stages

| Stage | What | Tests |
|---|---|---|
| 0 (DONE, tests pass 2026-10-20) | B2 estimability check + false unit warning + timing line (backend fixes from the log) | scistackstats pytest: empty-cells fixture in the C-Brace shape; recommend never proposes a rank-deficient model; nested-unit warning |
| 1 (CODE DONE, pytest unrun: tests/test_explain.py) | B1 `explain.py`: concepts + `explain_step` for steps 1–4 | pytest: every catalog test/correction/effect has a concept (coverage test); sentences built from fixtures |
| 2 (CODE DONE, pytest unrun: test_stats_service.py) | B3 `stats_preview` + `guide` in compute | scistack-gui pytest (api), timing logged |
| 3 (DONE: npm test + tsc pass, both bundles rebuilt) | Frontend shell + steps.ts + steps 1–4 + mode toggle | vitest steps.ts; vocabulary lint; tsc |
| 4 (DONE with 3) | Steps 5–6 (data check grid with remedies; main question) | vitest + pytest for remedy → spec |
| 5 (DONE with 3) | Steps 7–9 (test, corrections, effect size) | pytest explain for each recommendation path |
| 6 (DONE with 3) | Step 10 results + "how to read this" | pytest `read_results` |
| 7 (DONE) | Docs: update stat-studio-controls.md + statistics.md; new docs/claude/statstudio-guided.md; GUI manual-testing section | — |

## Open questions (for the user)

- Q1: Should Guided be the default for every newly opened StatStudio, and
  should a saved table open in the mode it was saved in? (Recommend: Guided
  for new, last-used for saved.)
- Q2: Should the wizard be strictly linear (Next only, rail shows progress),
  or should completed steps in the rail be clickable to jump back?
  (Recommend: jump back to any done step, never forward past a blocked one.)
- Q3: Who writes the lay text? I draft it in `explain.py`, and you review the
  wording as the domain owner (you wrote csv-stats). Should it get a
  separate review pass?
- Q4: Is Stage 0 (backend fixes from the log) in scope here, or a separate
  task first?

## As built (2026-10-20) — deviations from the draft

- **One RPC payload for every step**: `scistackstats.guide.guide()` (facts) +
  `scistackstats.explain.build_steps()` (wording) instead of a per-step
  `explain_step`. It ships on `stats_preview` (no tests) and on
  `stats_compute`.
- **No "drop the interaction" remedy** for empty cells: the model module
  rebases a stored formula's fixed part onto the design's crossed factors,
  so a main-effects-only formula can't be expressed yet. The offered remedies
  are "analyse each X separately" (role iterate) and "leave out X = level"
  (a filter), plus "keep everything" (the default: pairs run, no overall test).
- **No per-column `read_results`**: the results step gives one "each row
  compares…" sentence and the concepts (difference, CI, p, adjusted p,
  statistic, effect size) in the panel.
- **Measure step**: no "% missing" yet (that coverage number lives in
  scistackplotdb's describe, not in the stats rows).
- Frontend tests are node --test (the project's runner), not vitest.
- New RPC `stats_guide_log` writes `[stats guided] show/choose/next/back/jump/view`
  lines to scidb.log.
- The unit step offers choices only when there is no unit; the unit changes
  through "What to compare" (Average over).
