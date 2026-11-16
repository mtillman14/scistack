# Plan: statistics on the figure, one Studio tab (issue #12)

Status: **built 2026-11-15** (all stages incl. one saved record); e2e smoke test and codegen of notes/caption not done; all unrun (no Python here). Saved tables rail kept for figure-less tables.

## What is wrong today

Test brackets on a figure already work (`scistackstats.overlays.bars_for` →
`derived_bars` → `diffbars.with_derived_bars`). Nobody finds them, because
reaching them takes six steps and two saves:

1. save the plot, 2. click StatStudio, 3. open Format > Brackets on the figure
(shown only when linked), 4. tick it, 5. save the table, 6. reopen the plot
(Plot Studio reads its linked table once, when the plot id changes).

There is no way from StatStudio to a figure at all. The cause is structural:
the figure and the table live in two tabs, so they can only meet through
saved rows (`_plot_design`, `linked_plot`, `StatsSpec.overlay`).

## Decisions (user, 2026-11-15)

- **One tab, two views.** A Studio tab has a `Figure | Statistics` switch
  over one design held in memory.
- **Plot Studio can draw test results by itself**: a toggle runs the
  recommended test for the figure's design.
- **What the figure can show:** brackets (with a hide control per bracket),
  a caption line, the omnibus result per panel, and effect size or mean
  difference as the bracket label.
- **The figure owns the request** for what is drawn on it. `StatsSpec.overlay`
  goes away (beta: no migration).

## The model

One Studio tab holds two specs and nothing else:

| Thing | Owner | Notes |
|---|---|---|
| Design (data, roles, grouping, filters) | the tab's `PlotSpec` (design fields, `DESIGN_FIELDS`) | `StatsSpec.design` is always a copy of it; never edited separately |
| How the figure looks | the tab's `PlotSpec` (display fields) | unchanged |
| Which statistical results the figure draws | **new** `PlotSpec.stat_marks` (display) | replaces `StatsSpec.overlay` |
| Test settings (family, test, correction, alpha, missing, formula, DVs, format) | the tab's `StatsSpec` | seeded once by `seed_from_plot`, whichever view asks first |
| The drawn facts (bars, panel notes, caption text) | **new** `scistackstats.marks.marks_for(report, stats_spec, plot_spec)` | grows out of `overlays.bars_for`; never stored |
| Every label and sentence on the figure | `scistackstats.format` | stars, `p_phrase`, `effect`, new `figure_caption`, new `omnibus_line` |
| Drawing them | `scistackplot` (`diffbars`, new `PanelNote`, the footnote owner in `render/base.py`) | the plot layer still never imports statistics |

`PlotSpec.stat_marks` (a small dataclass, classified `display` in
`DESIGN_FIELDS` and in `presets.FIELD_CLASSES`):

```
StatMarks(
    brackets: bool = False,
    pairs: "significant" | "all" | "adjacent" = "significant",
    label: "stars" | "p" | "effect" | "difference" = "stars",
    hidden: tuple[str, ...] = (),        # comparison ids; hidden, never deleted
    omnibus: bool = False,               # one line per panel
    caption: bool = False,               # one line under the figure
)
```

scistackplot stores it and draws what it is handed. It computes nothing. With
`stat_marks` on and no derived facts supplied (a script calling scistackplot
alone), it draws none and logs one INFO line saying who supplies them
(`scistackstats.marks`).

Data flow, per figure request (the existing `derived_bars` path, widened):

```
tab: PlotSpec + StatsSpec ──stats_marks RPC──► run (csv-stats) ► marks_for ► {bars, panel_notes, caption, reason, notes, test}
                                                      │ (debounced, latest wins; keyed on design + stats settings + stat_marks,
                                                      │  so a colour change never re-runs a test)
resolve / export / save / add-to-pipeline  ◄── derived {bars, panel_notes, caption}   (api/plot._with_derived, the one merge)
```

## What the user sees

**Figure view** (Plot Studio as today), Statistics group, new section
**Test results** above the manual Difference bars:

- ☐ Brackets — Pairs (significant / all / adjacent), Label (stars / p /
  effect size / mean difference)
- ☐ Omnibus result in each panel
- ☐ Caption under the figure
- One line naming what ran: "Paired t-tests, Holm within each panel
  (recommended from the data)", and a link **Open the table** (switches view).
- When nothing can be drawn, the reason in the same place, never silence:
  a pending decision (`catalog.needed_action`) with a link to it, a refusal,
  compare-to-reference active (S9), "no comparison is significant".
- Test brackets are listed in the Difference bars list with a "test" tag and
  a hide toggle (`stat_marks.hidden`). A manual bar still wins a duplicate pair.

**Statistics view** (StatStudio as today, in the same tab): the Comparisons
tab gets **Show on figure** — the one click of issue #12. It sets
`stat_marks.brackets` and switches to the Figure view. Format > Brackets on
the figure is removed (its controls are the Figure view's now). The
step-by-step wizard's results step gets the same button.

The switch keeps both views alive: scroll position, the open rail groups and
the last result stay when you come back.

## Stages

Each stage leaves every suite passing and is usable alone.

### Marks owner (scistackplot + scistackstats, no GUI change)

- `scistackplot`: `StatMarks` on `PlotSpec` (strict `from_dict`, classified in
  both field tables, codegen ignores it); `PanelNote(match, text, origin)`
  drawn as a second line under the panel title in both renderers (the title
  is already measured by layout, so no new headroom rule); a caption line in
  the footnote owner next to "Not plotted"; `with_derived_bars` becomes
  `with_derived(spec, Derived(bars, panel_notes, caption))`.
- `scistackstats`: `overlays.py` → `marks.py`. `marks_for` reads the rule
  from `plot_spec.stat_marks`; labels `effect` and `difference` from
  `format.effect` / the comparison's difference; `format.omnibus_line` (terms
  that contain the primary factor, the ones the comparisons grid already
  shows) and `format.figure_caption` (test, correction family, star key,
  missing-data sentence). `StatsSpec.overlay` and `OverlaySpec` are removed.
- Logging: `[marks] analysis a1: 3 bracket(s), 2 panel note(s), caption yes;
  reason …` at INFO; crowded panels stay WARN.
- Tests: `test_overlays.py` → `test_marks.py` (each label kind, hidden ids,
  omnibus line per stratum, caption text, S9 and design-mismatch refusals);
  `scistackplot/tests/test_derived_bars.py` grows panel notes and caption in
  mpl and plotly; the unclassified-field guards cover `stat_marks`;
  export parity (`test_compare_export.py` pattern): the exported figure bakes
  the same bars, notes and caption the preview drew.

### Live marks in Plot Studio (still two tabs)

Delivers the figure-first half of the request before the tab merge.

- Backend: RPC `stats_marks {plot_spec, stats_spec | null}`. With no
  `stats_spec` it seeds one (`seed_from_plot`). Works on an unsaved figure.
  `stats_overlay_for_plot` and `overlay_for_plot` are removed. Timing line
  `[timing] stats_marks: load / run / marks`.
- Frontend: `PlotStudio/TestResultsSection.tsx` (the section above) and a
  small hook in `useStudioCore`'s place for derived facts; `StatsLink.tsx`
  loses its bracket fetch. The request's `derived_bars` becomes `derived`.
- Tests: service test on an unsaved spec (brackets present, never stored in
  the saved plot); frontend unit tests for the request key (display edit →
  same key; design, test setting or `stat_marks` edit → new key).

### One tab

- `components/Studio/Studio.tsx`: the tab root. It mounts `PlotStudio` and
  `StatStudio` side by side, hides the inactive one, and binds their designs
  in memory. `PlotRoot` and the `stat` branch of `main.tsx` both mount it;
  `__SCISTACK_VIEW__.view` only picks the opening view.
- Design binding: the backend sends the `DESIGN_FIELDS` list with
  `plot_describe`; `Studio/sharedDesign.ts` copies those fields (a consumer
  of the list, not a second owner). A parity test holds it to
  `copy_design` on fixture specs.
- `PlotStudio` gets `active`: no resolve while hidden; one resolve on return
  if the design changed. `StatStudio` gets the same for `stats_compute`.
- Gone from the frontend: the StatStudio toolbar button, the
  `design_update` / `design_apply` / `design_changed` calls inside a tab, the
  host's `open_stat_panel`. The browser build gets statistics for the first
  time (it was disabled only because it needed a second tab).
- Every way in keeps working (`studioOpen.ts`): variable → either view;
  `stat_` node → Statistics; `plot_` node → Figure (Statistics disabled with
  the reason when the node's own function draws the figure).
- Logging: `[Studio] view figure → statistics` (webview + scistack.log).
- Tests: `sharedDesign` copy parity; e2e smoke (`tests/e2e`): open a
  variable, tick Brackets, the figure's `layout.meta.difference_bars` holds a
  `test` bar; switch view and back, the design edit made in Statistics shows
  in the figure.

### Show on figure and hide

- The Comparisons tab button and the wizard's button; the hide toggle on
  test bars in the Difference bars list; the "what ran" line and reasons.
- Tests: frontend unit tests for the hide toggle (`stat_marks.hidden`),
  e2e: Show on figure lands on the Figure view with brackets.

### Saved work (needs your yes; see Open question)

- A save writes one record: the figure plus the stats settings. Reopening a
  saved plot redraws its test results from current data.
- `_plot_design`, `link_design`, `StatsSpec.linked_plot`, `design_service`
  and the Saved tables list retire. Existing `_stats_spec` rows stay
  readable (opened into a tab; the next save writes the new record); none is
  deleted.
- Until this stage, saving works as today: two lists, and a tab that opens
  a saved plot takes the first linked table's settings.

### Docs and checklist

- `docs/claude/studio-unification.md` (one tab; the sync section becomes
  history), `statistics.md` (owners table, the `marks` flow),
  `difference-bars.md` (Derived bars), `stat-studio-controls.md`,
  `plot-studio-controls.md`, an ADR in `decisions.md`, a section in the GUI
  manual-testing doc.

## Kept out (said no, or later)

- Generated `plot_` code keeps baking brackets, notes and caption as
  literals: it reproduces the approved figure. A pipeline figure that
  re-tests on new data is the difference-bars roadmap item, not this plan.
- SPM cluster shading, compact letter display, a plotted specification curve.
- Brackets for a DV other than the figure's y measure (the table may hold
  several DVs; the figure draws the analysis of its own).
- A merged single rail. Each view keeps its rail; Data and Structure are the
  same component in both already.

## Risks

- **PlotStudio is under the feature pause.** `PlotStudio.tsx` itself gains
  one prop and one mounted section and loses `StatsLink`; the new code lives
  in `Studio/` and `TestResultsSection.tsx`.
- **A hidden view must not compute.** If `active` leaks, every design edit
  runs a resolve and a test run. Covered by a unit test on the gating and
  the `[timing]` lines.
- **Unexamined p-values on a figure.** The Figure toggle runs the
  recommended test without the user reading the table. Mitigation: the
  "what ran" line is always shown beside the toggle, a pending decision
  blocks drawing, and the caption states the test.
- **Caption length.** A long caption wraps; it uses the footnote's existing
  wrap and the layout already reserves measured footnote height.

## Open question

Saved work: one saved record per tab (figure + stats settings), as the
Saved work stage proposes, or keep saved plots and saved tables as two
lists? One record is what makes a saved plot reopen with its brackets and
lets the design-sync store go; two lists is no change.
