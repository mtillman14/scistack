# "Show sample" lines span the innermost grouping layer only

**Date:** 2026-09-21. **Ask:** bar grouped `[ColName, session]` (ColName
innermost, session the outer bracket), Show sample = subject, Join lines —
the lines must connect a subject's points *within one session, across the
ColName levels*, never from `pre·B` on to `post·A`. "Very typical": the line
should always be confined to the innermost grouping level.

## The rule (one owner: `roles`)

A joined overlay line **spans the innermost tick layer** (`GroupingLayers.
ticks[-1]`, the layer whose levels are labelled under the marks) and **never
crosses an outer layer** (`ticks[:-1]`, the brackets). The run identity is
therefore *(shown keys, bracket values[, the line on a spaghetti], colour)*
— today it is *(shown keys, colour)*.

The automatic join question becomes "does the shown identity recur across
the **span** layer?" instead of "across the deepest grouping layer":

| span (innermost tick) | shown key | decision |
|---|---|---|
| schema key, deeper than the shown key (`session`, shown `subject`) | join — a subject has a value at every session (within each bracket) |
| schema key, at/above the shown key (`session`, shown `trial`) | points — a trial belongs to one session |
| synthetic, depth-less (`ColName`, `Variant`, `Code:<fn>`) | **join** — every record has every column / variant, so the identity recurs |
| any other depth-less layer (a derived bucket) | points — cannot be told. (A joined variable such as `Demographics.InterventionGroup` HAS a depth — pinned by subject — so it is the schema-key row: a subject belongs to one group, points.) |
| shown key depth-less | points — cannot be told (unchanged) |
| no tick layer at all (one unlabelled mark) | points — nothing to join across |

Cases that CHANGE versus today: `[ColName]` or `[ColName, session]` with
`subject` shown (today points via "no schema-key layer" / join across
everything → now join within the bracket); `[subject, ColName]` with `trial`
shown (today "belongs to one subject" points → now a line per trial across
the columns, which is the same record's columns). Every other decision is
unchanged, only the span of the drawn line is.

## Stages

### 1. `roles` — span/brackets + the join rule (+ tests)
* `GroupingLayers.span -> str | None` (`ticks[-1]`), `.brackets -> list[str]`
  (`ticks[:-1]`, outermost first).
* `overlay_join`: compare the deepest shown key against the span per the
  table above; reasons name the span and, when there are brackets, "within
  each <bracket>". Detect a synthetic span via `FactorInfo` (variant / code
  origin / ColName origin), a field via `is_field`.
* DEBUG log of the inputs: span, brackets, shown depth, span depth, decision.
* `overlay_granularity`: "Lines join the points across <span> (within each
  <bracket>)."
* Tests in `test_show_sample.py`: the six rows above.

### 2. `reduce` + `render.base` — the run column (+ renderer tests)
* `resolved.SAMPLE_RUN = "__run"`: `_overlay_frame` composes the bracket
  layers (`x_layers[:-1]`) and, on a spaghetti, the `__line` id into one
  run key (empty string when there is nothing to split by).
* `render.base.sample_series` groups by `[SERIES, SAMPLE_RUN]` and still
  yields `(identity=SERIES value, rows)` — offsets stay keyed by `__series`,
  so subject 01 keeps its slot in every bracket; mpl and plotly are untouched
  beyond that seam.
* DEBUG log in `_attach_overlay`: runs per identity, the span/brackets.
* `plot_geometry.py`: `mpl_sample_runs` / `plotly_sample_runs` readers
  (list of `[(x, y), …]` per drawn run). Tests in
  `test_show_sample_render.py`: `[ColName, session]` → two runs per subject,
  each inside one bracket's position range; `[session, ColName]` → mirror;
  both backends draw identical runs; spaghetti runs never cross lines.

### 3. `codegen`
* `run_keys = [_series, *brackets, (_line), (hue)]` in `_sample_draw_lines`
  (the `_sample` frame keeps factor columns, so the brackets are there by
  name). Test in `test_show_sample_codegen.py`: the executed script draws
  the same run count/positions as the preview.

### 4. Report, docs, manual-testing list
* `capability.sample_overlay_summary`: add `span` and `brackets`.
* `docs/claude/show-sample-overlay.md` "The join rule" section rewritten;
  `docs/gui-manual-testing-todo.md` entry.

### 5. (Optional — user's call) the spaghetti's OWN polylines
`_draw_spaghetti` / plotly / codegen join a subject across every tick,
brackets included (`[subject, session, speed]` draws `slow·pre → slow·post →
fast·pre → fast·post`). The same span rule would apply via the marks'
`__series` + brackets. Not asked for; listed so the decision is explicit.

## Known limitation kept
A coloured **innermost** layer still yields one-point runs unless "Colour
points by" is set (a line crossing two mark colours has no colour to be) —
unchanged from `docs/claude/show-sample-overlay.md`.

## Status 2026-09-21

All five stages built (user approved stage 5 too). Frontend: `tsc` clean,
`npm test` 90/90, both vite bundles rebuilt. Python: scistackplot pytest all pass (user-run). Uncommitted.
Not visually checked (manual test §0i in `docs/gui-manual-testing-todo.md`).
