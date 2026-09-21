# Plan: colour is paint; "Show sample" on spaghetti; geometry tests

**Date:** 2026-09-21. **Branch:** refactor/intent-and-fact.

## Findings (scidb.log 2026-09-21 16:00–16:01)

* Toggling the colour tag on a grouping layer rearranged the bars because
  `roles.grouping_layers` drops the coloured layer from the tick axis and
  dodges it innermost (`roles.py:758`). The summary partition is unchanged
  (`_summarize` groups by `__x` + `__color`), so the heights are the same
  numbers in different places — but the coloured layer's nesting position is
  discarded. Design decision of 2026-09-17 ("Colour is not a tick"); the
  user rejects it: colour must be aesthetic only.
* The `groups` reorder at 16:00:36 was an arrow click, not a side effect
  (`setColor` in PlotStudio.tsx only writes `color`).
* "Show sample" is refused on spaghetti by `spec.OVERLAY_KINDS`
  (`roles.overlay_unavailable`).

## Decision to confirm

**A (recommended) — colour is paint.** The coloured layer stays exactly where
the grouping list puts it; ticks = every grouping layer; marks are painted by
the coloured layer's level and the legend names the levels. Toggling colour
changes no position, label or number. Consequence: colouring no longer
"makes room" — `MAX_X_LAYERS` counts every layer.

**C — colour is paint + drops its tick labels.** Same geometry as A, but the
coloured layer's tick labels/brackets are omitted (the legend labels it), so
the cap benefit survives. More plumbing in `_plan_nested_x` / `_add_x_groups`
/ codegen `order=`. Can be added on top of A later if the cap bites.

## Stage 0 — observability first (NOTE 2)

* `reduce._build_figure`: one INFO line per figure —
  `grouping: ticks=[…] colour=X → marks partitioned by […]` — so the log
  states what partitions the data, separately from what paints it.
* `test_grouping_layers.py`: for every categorical kind and every colour
  choice, the partition (set of group keys) and the tick layers are
  identical. Pure Python, no renderer.

## Stage 1 — colour is paint (scistackplot + GUI text)

* `roles.grouping_layers`: `ticks = reversed(groups)` (spaghetti:
  `reversed(rest)`); `GroupingLayers.labelled_ticks == ticks`;
  `layer_cap_reason` / `role_for_new_grouping` count every layer;
  `capability.grouping.hint` + `labelled_layers` wording.
* `reduce._summarize`: group by `__x` (+carry); `__color` is a function of
  `__x` and is carried, never a splitter.
* Renderers: no dodge on categorical kinds (one mark per composed x).
  mpl `_draw_bars` / `_draw_distribution` / scatter: width = full slot;
  plotly bar/box/violin traces get a shared `offsetgroup` so `*mode:
  "group"` stops reserving a slot per colour trace. `dodge_slots` becomes
  `(0, 1)` everywhere; the overlay's identity offsets spread over the whole
  tick (`overlay_offsets(ids, 1)`).
* codegen: `catplot(..., hue=color, dodge=False)`; the sample overlay's
  emitted `_dodge` goes.
* Tests to update: `test_x_nesting.py` ("colour is not a tick"),
  `test_show_sample_render.py::test_mpl_points_sit_inside_their_colour_slot`,
  `test_show_sample_codegen.py` dodge assertions, `test_render.py` bar width
  expectations, `test_role_availability.py` cap cases.
* GUI: `GroupingList` hint ("Tick the colour to paint a layer by legend"),
  `labelled = layers.length`, comment at PlotStudio.tsx:3493; `groups.ts`
  unchanged. Rebuild BOTH vite targets; commit + push so the runtime clone
  sees it.
* Docs: `grouping-and-collapse.md` — replace the "Colour is not a tick" trap
  with the paint rule + the date numbers/layout changed;
  `docs/gui-manual-testing-todo.md` entries.

## Stage 2 — "Show sample" on spaghetti

* `spec.OVERLAY_KINDS += SPAGHETTI`; `overlay_unavailable` message.
* `reduce._overlay_frame`: new `__line` column = the parent mark's series id
  (`_series_key(group, [*unit_layers, *series_layers])`) — the overlay row's
  line, whether the lines are the first grouping layer or a collapsed unit.
* `base.sample_positions` (the ONE owner): spaghetti branch —
  tick index + `series_offsets[__line]` + identity offset scaled to the
  inter-line spacing (`overlay_offsets(ids, n_lines)`). Both renderers
  already go through it.
* Join rule unchanged (`overlay_join` compares the shown key with every
  grouping layer incl. the lines layer: trial below session → points only;
  subject-as-unit case → points land on their own line).
* codegen `_sample_draw_lines`: spaghetti positions = the emitted `_offsets`
  of the line + own offset; `_line` column in `_sample`.
* `ylimits` overlay mode: verify kind-agnostic; capability
  `sample_overlay.available` flips on; the GUI section needs no change.
* Tests: `test_show_sample_spaghetti.py` — availability; every overlay point
  sits within its own line's slot (both backends, same x); trial points not
  joined; subject-collapsed (units) case; codegen lands where the preview
  draws.

## Stage 3 — programmatic visual confirmation

* `scistackplot/tests/geometry.py`: `mark_geometry(resolved, backend) ->
  DataFrame[panel, x_position, y, y_low, y_high, colour_level, label]` read
  off what was DRAWN — mpl `ax.patches` / `ax.lines` / `ax.collections` and
  tick texts; plotly trace `x` / `y` / `error_y` and `tickvals`/`ticktext`.
* `test_colour_is_paint.py`: for bar/box/violin/scatter/strip/spaghetti ×
  2–3-layer grouping lists × colour ∈ {None, each layer}: geometry minus the
  colour column is identical; tick labels and bracket spans identical;
  legend lists the coloured layer's levels.
* `test_drawn_numbers.py`: each drawn bar height / box median equals an
  independent pandas recomputation from the raw frame for that x position —
  "the plot draws what it says".
* Cross-backend parity folded in (mpl geometry == plotly geometry) — the
  existing `test_both_backends_place_every_point_at_the_same_x` pattern.
* Not doing pixel baselines (`matplotlib.testing.image_comparison`): brittle
  across matplotlib versions; geometry is what the eye reads anyway.

## Order

0 → 1 → 3 (lock the invariant before touching spaghetti) → 2.
