# Plan: Statistics steps in the Best Popcorn + Best Player examples

Request (2026-10-20): add StatStudio (GUI) and statistics (code) steps to the
two docs example projects (`docs/examples/`).

## Current state

| Page | Stats today |
|---|---|
| best-popcorn-scifor.md / -scidb.md | SciPy Friedman + Wilcoxon (Bonferroni), hand-written. No scistackstats. |
| best-popcorn.ui.md | Stat Studio Steps 9-11, written 2026-10-17 for the **Advanced rail**. New tables now open in the **Step by step** wizard (71b55d9a), so these steps no longer match what a reader sees. |
| best-player-scifor.md / -scidb.md | No statistics at all. |
| best-player.ui.md | Placeholder ("Coming soon"). |

scistackstats is Python-only (no MATLAB bridge), so the code steps get a
MATLAB tab that says so and shows the Python.

## Steps

1. **Best Popcorn, code (scifor + scidb):** new step after "Which one is best?":
   "The same statistics from the figure's design" -- `PlotSpec` (brand GROUP,
   participant COLLAPSE) -> `seed_from_plot` -> family nonparametric ->
   `describe_table` + `run` -> `export_comparisons(..., "md")`. scifor builds the
   table with `LongTable.from_frame`; scidb uses `scistackstatsdb.load_stats_tables(db, spec)`.
   Point out: every pair, Holm, effect size named, same numbers the GUI shows.
   Update "complete script", summary tables, "What you'll learn".
2. **Best Popcorn, GUI:** rewrite Steps 9-11 for the Step by step wizard
   (Measure -> Independent unit -> What to compare -> Same or different people ->
   Missing data -> Main question -> Test (choose Friedman / Wilcoxon) -> Many
   comparisons -> Size of the difference -> Results), then Advanced view for
   brackets + save. Keep screenshot placeholders.
3. **Best Player, code (scifor + scidb):** new step "Is Jordan really the better
   scorer?" on `points`: player compared, game averaged over -> unit = game
   (players compared within the same game, since the opponent affects everyone
   that night); Jordan/Harper missed games -> incomplete units -> `use_all` ->
   mixed model + mixed-model contrasts, Holm. Descriptives mean [95% CI]: Jordan's
   CI from 2 games is the statistical version of `min_games`. scidb version: the
   Morgan exclusion carries into the stats with no filter.
4. **Best Player, GUI:** see open question Q1.
5. **Numbers:** I can't run Python. I hand over one script per example; the user
   runs it and pastes the output; I fill the expected-output blocks and the
   prose that depends on them (significant or not).
6. GUI manual-testing doc: not touched (docs-only change).

## Decisions (2026-10-20)

- Q1: full no-code Best Player GUI walkthrough (import, inventory, omit Morgan
  in Plot Studio's Schema keys, points per player, Stat Studio). No season
  totals or sweep: those need functions.
- Q2: game as the unit. **Correction after reading `tables.analysis_frame`:**
  the unit id is composite (the unit plus every schema key above it), and the
  schema is `[player, game]`, so the unit is "game within player": one unit
  per box score line. `player` therefore derives as **between** (independent
  groups, n = games played), not within/mixed model. The docs describe what
  the code actually does; tell the user.
- Q3: Best Popcorn GUI steps rewritten for the Step by step wizard.

## Open questions (answered above)

- Q1: Best Player GUI page: a full no-code walkthrough, or Stat Studio only?
- Q2: Best Player design: game as the unit (mixed model) vs. game lines as
  independent observations?

## Status (2026-10-20)

All six pages written; numbers from `tmp_example_stats.py` (user ran it,
output `tmp_example_stats_out.txt`). mkdocs build not run by Claude. GUI
steps (wizard screens, ColName filter, Schema keys omission) not checked in
the running GUI; screenshot placeholders `📷 Screenshot` added.
