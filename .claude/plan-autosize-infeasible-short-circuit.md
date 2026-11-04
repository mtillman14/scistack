# Plan: stop the auto text size search when the bottom fails too

## Diagnosis (scistack-debug.log, 2026-11-03, C-Brace CosmedData bar plot)

- `plot_resolve` was 0.3–2 s until 11:03:51, then 17–28 s on every call.
- Data side is fast: `resolve_one` TOTAL=0.29 s, DuckDB held 0.08 s.
- `layout_decisions` TOTAL=22.1 s = 5 auto-size trials x ~4.3 s each
  (`auto text size layout 1..5: reference 12/10/9/8.5/8pt -> x ticks overlap`).
- Trigger: grouping changed `["minute","group"]` -> `["minute","session","group"]`,
  so 23 x slots became 105. Narrowest slot 4.7 pt, widest label ~10 pt even at
  the 8 pt floor. No reference can fit, so the binary search walks all the way
  down (top, then 4 halvings) and keeps the bottom anyway.
- Every slow call in the session is "5 trial(s)"; every fast one is "1 trial(s)".
- Per trial (`render_mpl`): x_labels 1.7–2.4 s (fit_labels runs twice by design,
  ~37 candidates x 105 labels each), marks 0.7 s, legend_and_layout 0.6 s,
  grid_reach 0.6 s.

## Fix (scistackplot layer: `text/autosize.py::choose_reference`)

1. After the top probe fails, probe the BOTTOM next. If the bottom fails, stop:
   monotonicity ("larger text never makes a layout cleaner") already says every
   candidate in between fails, so the answer is the bottom. 5 trials -> 2
   (~22 s -> ~9 s). Cost when the bottom is clean: +1 probe vs today.
2. Logging: one INFO line when the short-circuit fires
   (`auto text size: bottom 8pt also fails (x ticks overlap); skipped N
   candidate(s)`), so the log says why only 2 layouts ran.
3. Tests (`scistackplot/tests/`): fake `measure` that fails everywhere ->
   exactly 2 probes, reference == bottom, `failing` set; fake that is clean
   only at the bottom / only in the middle -> same answer as before.
4. Doc: update `docs/claude/auto-text-layout-rules.md` "At the bottom" and the
   `choose_reference` docstring.

## Possible follow-up (not in this change)

- Cheaper per-trial cost: `fit_labels` could stop once an upright floor-font
  label set is known to be beaten by every rotation candidate, etc. Measure first.
- GUI hint already exists (WARN "widen the figure ... or show fewer positions");
  surface it more prominently so the user knows 105 positions is the cause.
