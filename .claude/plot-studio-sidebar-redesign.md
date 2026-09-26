# Plot Studio left sidebar — redesign options

Status: IMPLEMENTED 2026-09-26 — Design A + figure toolbar (user choice). Map: docs/claude/plot-studio-controls.md. Not yet checked by eye (manual test 0zy).
Source: `scistack-gui/frontend/src/components/PlotStudio/PlotStudio.tsx:2206-3100`
(sidebar JSX), `styles.rail` (`width: 260`), `Section` (flat, always open).

## Current layout (top to bottom)

| # | Section | Contents |
|---|---------|----------|
| 1 | Variants | variant rows, add, readout |
| 2 | Schema keys | location-picker button |
| 3 | Filters | per-factor LevelPickers, range filter |
| 4 | Grouping | Group by… button, group rows, refusals, bucket editors, BucketAdder, GroupingList (order + colour) |
| 5 | Factors | role `<select>` per non-grouped factor (figures / subplot / collapse) |
| 6 | Plot type | kind radios, Line weight, Per-record value |
| 7 | Layout (faceted) | N rows/cols, row/col rule slots, layout notes |
| 8 | Y axis | scope checkboxes, Min/Max, applied-limits note |
| 9 | Summary (summarizing) | Centre, Spread, Weight by N, sample note |
| 10 | Show sample | key ticks, Join, Colour points by, Show in legend, Weight |
| 11 | Figure size | Aspect, Width/Height/**Font**, px readout, **Text sizes grid**, **Preview at**, **Tick rotation**, **Show tick labels**, **Hide labels legend repeats**, fit notice, Use this size |
| 12 | Labels | titles, aliases (LabelsSection) |
| 13 | actions | Format, Save image, Save all, Save data (CSV), Export code, Add to pipeline |

Right rail: Saved plots (collapsible).

## Problems found

- **Fit at 236 px usable width**: Text-sizes grid (2 cols of right-aligned label +
  box, "Legend title") overflows; Width/Height/Font row is 3 inputs wide;
  `select maxWidth 130` + monospace factor names collide; "Hide labels the legend
  repeats" wraps badly beside its checkbox.
- **Plot type is 6th** but is the first real decision; it gates Grouping's
  hint, Summary, Show sample, Per-record value and line weight.
- **One concept, three places**: "what is collapsed and how it is summarised"
  is split across Factors (collapse role), Summary (centre/spread/weight-by-N)
  and Show sample (which collapsed keys are overlaid). The sample note in
  Summary already explains Factors.
- **Roles in two controls**: Grouping's GroupingList toggles group/iterate and
  owns order + colour; Factors' `<select>` owns every other role.
- **Layout (facet grid)** is separated from the Subplot role that creates it by
  Plot type.
- **Figure size is a junk drawer**: typography (Font, text sizes), x-tick
  behaviour (rotation, thinning, legend-repeat hiding) and a *view* setting
  (Preview at) live there. Font/text sizes belong with Labels; ticks belong
  with axes; Preview at belongs with the figure.
- **Mark weights split**: Line weight under Plot type, sample Weight under Show
  sample.
- **Output actions at the very bottom** of a long scroll; the data-depth chooser
  opens even further down.
- **19 static italic hints** (`styles.hint`) are a large share of the height and
  are read once.

## Cross-cutting fixes (apply to any design)

1. Width-responsive rows: measure the rail with a ResizeObserver (inline styles
   cannot use container queries) and expose `narrow | normal | wide`.
   Text sizes: 1 column (label left, box right) when narrow, 2 when wide.
   Needed anyway for a future resizable rail (min ~240, default ~280, max ~520).
2. Static hints collapse behind a per-section ⓘ (or one global "Explain"
   toggle, remembered per viewer). DYNAMIC notes stay always visible:
   refusals, layout notes, y-limit readout, label-fit verdict, sample note.
3. Plot type radios → compact 2-column grid of kind tiles (or a `<select>`).
4. Checkbox-first rows for long boolean labels (box left, label wraps).
5. Output actions leave the scroll: pinned footer, or a toolbar over the figure.
6. Move Preview at + "Use this size" to the figure toolbar (view, not spec).

## Design A — Workflow accordion (recommended)

Same single scrolling column, regrouped into 5 collapsible groups in workflow
order. Collapsed headers show a one-line summary of current values, so the
whole rail fits on one screen when collapsed. Output actions pinned to a footer.

```
▾ DATA            3 variants · 12/14 subjects · 1 filter
    Variants / Locations button / Filters
▾ CHART           Bar · mean ± SEM
    Plot type tiles · Per-record value
▾ STRUCTURE       x: Side > Condition · colour Side · 2 subplots
    Group by… · buckets · grouping order/colour
    Other factors: role select (Figures / Subplot / Collapse)
    Facet grid (only if Subplot) — rows/cols + slots
▸ STATISTICS      mean ± SEM over Subject · sample: Subject (lines)
    Centre / Spread / Weight by N / sample note
    Show sample: keys · Join · Colour points by · Legend
▸ APPEARANCE      7.2×4.8 in · 9 pt · y auto
    Size: Aspect · W · H
    Y axis: scope · Min/Max · readout
    X ticks: rotation · show every · hide legend-repeated
    Marks: Line weight · Sample weight
    Text: Font · text sizes (grid) · Reset
    Labels: titles · aliases
─────────────────────────────────────
[PNG▾] [Save image] [CSV] [Code] [Add to pipeline]   ← pinned
```

## Design B — Tabbed rail

Icon strip (activity-bar style) down the rail's left edge: Data · Structure ·
Stats · Style · Text · Export. One tab's content at a time, each ≈ one screen.
Structure tab merges Grouping + Factors into a single roles table
(drag to order, role chip per factor, colour dot).

## Design C — Meaning in the rail, presentation in a Format popover/toolbar

Rail keeps only what changes WHAT is drawn (Data, Chart, Structure,
Statistics). A toolbar over the figure holds Preview at, Format…, and the output
actions. Format… opens a wide floating panel (non-modal, draggable, remembers
position) with Size / Axes / Text / Labels / Marks in columns — room for the
text-size grid and the alias table. Mirrors the backend split (PlotSpec meaning
vs StyleOptions).

## Comparison

| | Current | A accordion | B tabs | C split |
|---|---|---|---|---|
| Scroll length | very long | short when collapsed | ≈ none | medium rail, no scroll in popover |
| Fits 260 px | no (text sizes, W/H/Font) | yes with responsive rows | yes | yes (popover is wide) |
| Workflow order | poor | good | good | good |
| See cross-effects (collapse → sample) | scattered | same group | tab switch hides | same rail |
| Discoverability | everything visible | headers + summaries | hidden behind icons | Format hidden behind a button |
| Resizable rail readiness | poor | good (breakpoints) | good | best (rail narrow ok) |
| Cost | — | low–medium (regroup JSX, Group component, footer, ResizeObserver) | medium (+ roles table) | medium–high (floating panel, toolbar, focus mgmt in webview) |
| Risk | — | low: same controls, moved | medium: roles merge touches setRole/moveGroupLayer UI | medium: popover in VS Code webview, preview while editing |

## Recommendation

A, taking two pieces of C: the output actions + Preview at go into a figure
toolbar (not a rail footer), and the Labels/aliases editor may later graduate
to a popover if the alias list grows. Consider B's merged roles table as a
follow-up within A's STRUCTURE group.

## Implementation notes (when chosen)

- Presentation only; no backend changes. Roles/capabilities stay owned by Python.
- Add a `Group` component (collapsible, summary line, open state persisted per
  viewer in localStorage with try/catch) next to `Section`.
- Summary lines computed from spec + capabilities in a pure `.ts` helper with
  unit tests (like figureSize.ts / textSizes.ts).
- Log group open/close? No — UI state only; but log rail width changes at
  DEBUG if the resizable rail lands.
- Update docs/gui-manual-testing-todo.md with a section per moved control.
- Rebuild BOTH vite targets.
