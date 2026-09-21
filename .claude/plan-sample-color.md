# Plan: colour the "Show sample" overlay by its own key

**Date:** 2026-09-21. **Status:** BUILT 2026-09-21 (all 6 stages, uncommitted); scistackplot suite fully green 2026-09-21 (user-run); TS 89/89 green, both bundles rebuilt; GUI not visually checked (manual-testing item 0f). Auto rule needed NO change — `overlay_join` already ignored depth-less layers; only the renderers split runs per colour.
**Builds on:** `docs/claude/show-sample-overlay.md`, `docs/claude/grouping-and-collapse.md`.

## The ask

Bar ± error, grouping `[session, Demographics.InterventionGroup]` with the
intervention group coloured, `subject` / `trial` collapsed, `speed` separate
figures, `ColName` separate panels. Show sample = `subject`, joined (auto:
lines). The user wants **each subject's line in its own colour**, while the
bars keep the intervention-group colour — the two colourings independent.

Today the overlay takes its mark's colour (`palette_for(resolved, level)` in
both renderers and the codegen), so every subject inside one intervention
group is the same colour and the lines are indistinguishable.

## Design

One new spec field, one rule owner, one new column, one palette.

* **`PlotSpec.sample_color: str | None = None`** — the *shown* key whose
  levels colour the overlay. `None` = today (mark's colour). Same contract as
  `show_sample` itself: a name that is no factor is refused by `validate`; a
  name that is a factor but not shown right now (not collapsed, or not
  ticked) is **inert** — ignored, reported by capability — so the dropdown
  state is never something the spec must adjudicate.
* **`roles.overlay_color(spec, steps) -> str | None`** — the ONE owner of
  "is the setting active": the key if it is in `steps.shown`, else `None`
  (debug log naming why). `reduce`, `codegen`, `capability` all call it.
* **Overlay frame** gets `__sample_color` (`resolved.SAMPLE_COLOR`) = the
  key's level, beside `__color` (the mark's, still used for the dodge slot).
  `ResolvedPlot.sample_color` (name) and `sample_color_order` (declared level
  order across the figure, via `_level_order` over the panels' sample
  frames) — decided once per figure like `sample_offsets`, so subject 03 is
  the same colour in every panel; in `to_json` under `sample`.
* **Palette: separate from the marks'.** `base.SAMPLE_PALETTE` — a 20-entry
  qualitative list (tab20) — and `sample_palette_for(resolved, level,
  fallback)` indexed by `sample_color_order` (the same never-by-enumeration
  rule as `palette_for`, for the same facet-grid reason). The overlay keeps
  its dark edge, smaller marker and alpha. Independence is by construction:
  the mark colour is read off `__color`, the point colour off
  `__sample_color`; neither reads the other.
* **Legend**: the sample levels become a second block in the one legend,
  the way the dash styles already are (`_dash_legend_handles` /
  `_dash_legend_traces`): title `Intervention / subject`; plotly entries
  carry `legendgroup=f"sample:{level}"` so clicking a subject hides that
  subject's points and lines. `shows_legend` counts sample levels too.
  Past 20 levels the colours cycle and a WARN says so (as the dashes do).
* **Codegen** mirrors it: `_sample_palette = dict(zip(_sample_levels,
  sns.color_palette("tab20", …)))`, `color=_sample_palette[...]` per part,
  a `Line2D` handle block appended to the seaborn legend.
* **Capability report** `sample_overlay.color = {setting, active, options}`
  where `options` = `shown` (what may colour) — the GUI displays it.
* **GUI**: a **Colour points by** `<select>` under *Join points* in the Show
  sample section: `None (mark's colour)` + one option per shown key.
  `showSample.ts`: `sampleColorChoice` / `setSampleColor` (React-free,
  tested). Changing it does not move the fan-out cursor (like colour).
* **Plan cache**: `sample_color` is presentation over the same overlay rows,
  but the column is built in `_overlay_frame` inside `_build_figure`, which
  runs per build, not per plan — so it stays out of `_PLAN_IRRELEVANT_FIELDS`
  only if the plan key would otherwise miss; verify in Stage 2 with a test
  that toggling `sample_color` does not rebuild the plan (`_plan_cache` hit).

## Stages (each with tests before moving on)

1. **Spec + rule** — `spec.py` field/round-trip/`from_dict`; `roles.py`
   `overlay_color` + validate (unknown refused, unshown inert w/ debug log).
   Tests: `test_show_sample.py` — round trip, refused, inert, active.
2. **Reduce + ResolvedPlot** — `SAMPLE_COLOR` constant; `_overlay_frame`
   column; `ResolvedPlot.sample_color`/`sample_color_order`; `to_json`;
   INFO-level log `sample colour: subject (12 level(s))`. Tests:
   `test_show_sample_reduce.py` — column present, order = declared order,
   same order for every panel, plan-cache hit on toggle.
3. **Renderers** — `base.SAMPLE_PALETTE`, `sample_palette_for`,
   `sample_legend_levels`; mpl `_draw_sample` + legend block; plotly
   `_sample_traces` + legend entries; `shows_legend`. Tests:
   `test_show_sample_render.py` — points coloured by subject while bars keep
   the group colour (both backends), legend lists subjects, >20 warns,
   `sample_color=None` byte-identical to today.
4. **Codegen** — `_sample_draw_lines` palette + legend. Tests:
   `test_show_sample_codegen.py` — emitted palette keyed by the sample key;
   the existing "lands where the preview draws" test still passes.
5. **Capability + GUI** — `sample_overlay.color`; `showSample.ts` +
   `showSample.test.ts`; `PlotStudio.tsx` dropdown; rebuild BOTH vite
   targets. `npm test`, `tsc`.
6. **Docs** — `docs/claude/show-sample-overlay.md` (new section + owner
   rows), `docs/gui-manual-testing-todo.md` (the user's exact scenario as
   the check).

## Not doing

* Colouring the overlay by a key that is NOT shown (e.g. by `InterventionGroup`
  when only `subject` is shown) — that is the mark's colour already.
* A separate palette picker for the sample — one fixed palette; revisit if
  the tab20 hues clash with the marks' palette in practice.
* Touching `sample_offsets` — placement is unchanged.

## Addendum (2026-09-21): lines across colour levels

The user's assignment has `session` as the COLOURED layer and
`InterventionGroup` as the tick, show sample = `subject`. Today Lines /
Auto(lines) draw nothing:

* `base.sample_series` (and both renderers, and `codegen` grouping by
  `[series, hue]`) group the overlay per colour level first, so subject 01's
  `pre` and `post` rows are one-row series — no line. The doc's rule: a line
  joins "the shown keys' values PLUS the colour level".
* Auto declines because `Demographics.InterventionGroup` has no schema depth
  (`overlay_join`: "cannot be placed").

### Rule (one owner, `roles.overlay_join` / a new `roles.overlay_identity`)

* **With `sample_color` set, a line's identity is the shown keys alone** and
  it joins across colour levels: each point is placed in ITS OWN row's dodge
  slot (`base.sample_positions` becomes per-row: slot looked up from the
  row's `__color`), so the line runs from the `pre` slot to the `post` slot
  inside one tick. The line's colour is the sample colour — that is what
  makes crossing unambiguous.
* **Without it, unchanged**: a line crossing two mark colours has no colour
  to be, so identities stay split per colour level (today's behaviour).
* Codegen: group `_sample` by `[series]` only when a sample colour is set
  (it already maps `_dodge` per row).

### Auto-join and depth-less layers

A grouping layer with no depth that is **constant within the shown
identity** (every subject has exactly one InterventionGroup — checked on
the table, `nunique() == 1` per identity) cannot split a repeated measure,
so it is dropped from the depth comparison; the rule then runs on the
schema layers alone (`subject` above `session` → lines). A field that
varies within the identity still declines, with a reason that says which.
Debug log lists the layers considered and the ones dropped.

### Extra tests

* `test_show_sample_render.py`: with colour=session, x=group,
  sample_color=subject, join forced: one polyline per subject whose x
  positions are the two dodge slots of one tick (both backends).
* `test_show_sample_codegen.py`: the generated points/lines land where the
  preview draws them for the same assignment.
* `test_show_sample.py`: auto = lines for exactly that assignment; auto =
  points (with the "varies within" reason) for a field that varies within
  the subject.

## Revision (2026-09-21): any coloured layer, simpler Auto rule

* Rendering holds for ANY coloured layer (session, InterventionGroup,
  speed, a Variant, none): identity = shown keys, position = tick + the
  ROW's own dodge slot + identity offset. Independence is structural: the
  sample key is a collapsed key, the coloured layer a grouping layer, and no
  factor is both — the two colourings can never name the same key.
* Auto rule, REPLACING the `nunique()` check above: **depth-less grouping
  layers are excluded from the depth comparison** — constant within the
  identity they drop out; varying within it they ARE repeated measures. The
  comparison runs on schema-key layers only; the reason names the excluded
  layers. No data scan.
* Tests cover colour = session, colour = InterventionGroup, colour = none,
  and a depth-less shown key (still declines).
