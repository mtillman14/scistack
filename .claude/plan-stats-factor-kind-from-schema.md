# Plan: StatStudio factor kinds from the schema, plus a clear "action needed" notice (2026-10-19)

## Problem
scidb.log 2026-10-19 16:39: GAITRiteLoaded, `session` was derived as
`mixed_membership`. Some subjects have several sessions and some have one,
because their later sessions aren't recorded yet. Every comparison in all 3
columns was refused ("set within or between first"). The only notice was one
line of text in the design sidebar, so the comparison tables just looked
incomplete.

## Why the data alone can't decide
`design._classify` looks only at rows. A subject with one session could be
missing data (a within-subject factor) or could belong to one group (a
between-subject factor). The rows look identical either way.

## (1) Use the schema to decide (recommended)
The schema hierarchy already answers the question for schema keys:
- **A key nested BELOW the unit** (session under subject): each unit can have
  several levels by construction, so the factor is **within**. A unit at only
  one level is *missing data*, not a separate group.
- **A key AT or ABOVE the unit** (for example a site above subject): every unit
  has exactly one level, so it is **between**. The data already derives this,
  so nothing changes.

Rule: when the data says MIXED and the factor is a schema key deeper than the
unit's deepest key, set it to **within**, recorded as
`derived="within (schema: session is below subject)"`. Add an info warning
`incomplete_within`: "session: N subject(s) lack some sessions; they are left
out of tests that need those sessions". csv-stats already drops and reports
units that are missing a level. An explicit user override still wins.

Data answers that agree with the schema don't change: all-single is between,
all-multi is within. So a study whose schema nests one condition per subject
below subject still comes out between.

**Limits (to be honest about):** this is exact for schema keys only.
Non-schema factors (ColName, Variant, own columns such as Gait.Side, and
variable-column factors such as Demographics.X that change over time) have no
hierarchy to consult. MIXED stays possible for them, and (2) handles it.

Where: `scistackstats/design.py` (`derive_design`; the one owner of factor
kind). `AnalysisFrame` gains `schema_keys` (outermost first) from the
LongTable's `schema_levels`. Tests: the 2026-10-19 shape (session below
subject, some subjects at 1 session) derives within; a key above the unit
derives between; a non-schema mixed factor stays mixed; an override wins; the
`incomplete_within` warning carries the counts.

## (2) An unmissable notice when the user must act
For whatever stays MIXED, or for any refusal whose kind is "design":
- **Comparisons tab:** a banner ABOVE the table, instead of a table of
  "not run" rows: "Comparisons need one decision: `session` has subjects at
  one level and at several. Is it [within] [between]?" The two buttons apply
  the override directly (the same `setOverride` the sidebar dropdown uses).
- **Design sidebar:** the mixed factor's chip turns amber and its dropdown is
  highlighted.
- **The Comparisons tab label** gets a "!" badge while a decision is pending.
- The refusal is logged at WARN (done 2026-10-19 in `run.py`).

The backend owns the message and the choices: the stratum refusal gains
`action: {factor, choices: ["within", "between"]}`. The frontend only renders
it, so the wording has one owner.

## Order
1. (1) in scistackstats, with tests.
2. (2) backend `action` field, then the frontend banner, chip and badge.
   Rebuild both bundles. Add to docs/gui-manual-testing-todo.md.
3. Document in docs/claude/statistics.md (the factor-kind rules).

## Revision after user feedback (2026-10-19): data is never dropped silently
User: "Data should really never be dropped, certainly not silently."

Finding: csv-stats (`core.py`, `subjects_dropped_incomplete`) ALREADY drops
every subject missing any within-subject level from a repeated-measures test
(listwise deletion), and records it in `n.excluded_detail` and `warnings`.
scistackstats carries that record into `Comparison.warnings/n` and
`OmnibusResult.warnings/n`, but no table caption, GUI element or log line
shows it. So choosing "within" by hand today drops subjects SILENTLY. This
is a bug whatever we decide about option (1).

Two different kinds of exclusion:
- **Inherent:** a paired comparison (FV vs SS in session BL) has no
  difference to compute for a subject that lacks one of the two. No paired
  method can use that subject.
- **A method choice:** RM-ANOVA across sessions drops a subject missing ANY
  session, along with all of their valid data. A linear mixed model uses
  every observation instead (assuming data are missing at random).

Proposed stages, in order:
0. **Never silent (do first; it's a bug fix).** One owner in scistackstats
   reads `excluded_detail` and puts it in the table caption ("2 subjects
   excluded: SS05, SS12, missing a session"), the n column ("n = 15 of 17"),
   the GUI result and a scidb.log WARN.
1. **Use all the data where a method allows it.** When a within factor has
   incomplete units, the omnibus is a linear mixed model (`y ~ factors +
   (1|subject)`, statsmodels MixedLM, added to csv-stats). RM-ANOVA is used
   only when data are complete. Primary pairwise comparisons stay paired; the
   pairs they cannot form are reported per stage 0.
2. Option (1) schema-derived kind (above), with an `incomplete_within` note
   that names the subjects.
3. Option (2) banner, chip and badge (above).
Open question for the user: is a mixed model acceptable as the default
omnibus when data are incomplete?

## FINAL PLAN (user decisions 2026-10-19, second revision; supersedes "Order" and the revision above)
User:
- csv-stats implements every family: t-tests, ANOVAs, nonparametric tests,
  LMMs. Methods are offered together ("and", not "or").
- Use all the data where a method allows it.
- The USER chooses whether rows with missing data are dropped. The choice is
  recorded and shown wherever results appear.
- The GUI recommends the most appropriate test from the data (normality etc.),
  but the user can pick ANY test that runs on that data shape and run it.

Current state: `Analysis.family` defaults to "parametric" and is never chosen
from the data. csv-stats computes Shapiro/Levene into `assumptions`, but
`catalog.select_tests` ignores them. Overrides (`omnibus_test`/`pairwise_test`)
exist, and `available_tests` already greys a test with a reason.

### Stage A: exclusions are never silent (bug fix)
One owner, `scistackstats.run._exclusions(record)`, reads csv-stats
`n.excluded_detail` and `warnings`. It feeds: the caption sentence
(`format`: "SS05, SS12 excluded: missing a session"), an "n = 15 of 17"
column, `Comparison/OmnibusResult.excluded` for the GUI, and a scidb.log WARN
naming the units. Tests cover each surface.

### Stage B: missing-data policy, chosen by the user and recorded
`Analysis.missing: "use_all" | "complete_cases"` (default `use_all`). It's
validated in `spec`, saved with presets/saved tables (`restore.STATS_SETTINGS`),
embedded in codegen, and stated in every caption: "Missing data: all
observations used (linear mixed model)" or "Missing data: complete cases only;
2 subjects excluded: …". The GUI shows it as a radio in the analysis, next to
the test choice, with the counts of what each option would keep.

### Stage C: csv-stats grows LMMs (in the csv-stats repo; needs a release)
- `lmm(data, dv, between, within, subject, ...)`: requirements §3.5, C3
  record, random intercept for the subject, REML. One effect per fixed term,
  plus the coefficient table, variance components and convergence in
  `details`. Convergence or singular fit becomes a warning, never an exception.
- `lmm_contrasts(...)`: pairwise contrasts of one factor's levels within each
  context, from the fitted model. This is the use-all-data pairwise option.
- df: OPEN QUESTION (see below).
- Validated against reference values (R lme4/lmerTest, recorded as fixtures).

### Stage D: every applicable test is offered; the recommendation comes from data
- catalog: add `lmm` (omnibus) and `lmm_contrasts` (pairwise), `anova_rm`,
  `anova_mixed`, plus `kruskal`/`friedman` where applicable.
  `why_unavailable` is ONLY about data shape and type (factor kinds, levels,
  unit, scalar vs 1-D). Missing data never makes a test unavailable: with
  `use_all` a complete-cases-only test (paired t, RM-ANOVA, Friedman) still
  runs and reports its exclusions per Stage A.
- `recommend(design, rows, missing)` (new, catalog): incomplete within + use_all
  gives LMM; Shapiro on the paired differences / residuals failing gives the
  rank-based test; Levene failing gives Welch; and so on. It returns
  `{test, evidence}` and the GUI shows "Recommended: Wilcoxon — differences
  not normal (Shapiro W=0.81, p=0.003)". The default selection is the
  recommendation; picking any other runnable test is recorded as "chosen".
  Assumption checks run through csv-stats (`csvstats.core` owns the math).
  Caveat shown in docs only: pre-testing normality is debated; the user decides.

### Stage E: within/between from the schema (option 1 above)
### Stage F: action-needed banner, chip and badge (option 2 above); also
surfaces the missing-data choice when units are incomplete.

Each stage: logging and tests per NOTE 2; GUI stages go into
docs/gui-manual-testing-todo.md; docs/claude/statistics.md updated (owners
table: missing policy, recommend, exclusions).

### Open questions
1. LMM df: start with Wald (z/chi², `df_method: "wald"`, honest per §3.5) and
   add Satterthwaite later, or Satterthwaite from the start (larger; needs
   reference values from R that I can't generate here)?
2. Default `missing`: `use_all` (recommended), or ask the user the first time
   incomplete units appear?

## Status (2026-10-19): all stages implemented, all tests pass
User decisions: Wald first; default `missing = "use_all"`; go ahead.
- A: `scistackstats/exclusions.py`; n "X of Y", notes, WARN (tests pass).
- C: `csv-stats/src/csvstats/lmm.py` (`lmm`, `lmm_pairwise`) + `core.assumption_checks`;
  csv-stats 0.3.0 (version strings fixed: __init__ said 0.2.0); scistackstats
  pins `csv-stats[full]>=0.3.0`. **csv-stats needs a release (or an editable
  install) before the GUI can use it.** On an older install: a "gap" refusal for
  lmm, plus a WARN and parametric fallback for the recommendation.
- B: `Analysis.missing` (+ `MISSING_LABELS`); complete_cases filters in
  `run_analysis` with a stratum `excluded` record; `format.missing_sentence`
  in both captions.
- D: `catalog.recommend` / `Recommendation`, `Selection.recommended`, family
  "auto" (default) + "mixed_model"; `COMPLETE_CASES_TESTS`; GUI menus grey only
  for shape.
- E: `AnalysisFrame.below_unit`, `Factor.basis`, `design.incomplete_units`.
- F: `catalog.needed_action`, `StratumResult.action`; GUI banner, badge and
  amber chip (`StatStudio/statsActions.ts`).
Docs: statistics.md (owners and traps), stat-studio-controls.md,
csv-stats-v1-requirements.md, csv-stats README. GUI §0zzzzj.
