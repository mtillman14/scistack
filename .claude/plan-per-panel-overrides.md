# Plan: per-panel y-limits and y-axis title

Status: APPROVED 2026-09-27 (D1 as proposed). Amended: hide (D5), grid toggle (D6).
Progress: ALL 6 STAGES DONE 2026-09-27; every test passes; uncommitted. Stage 5: npm test 445/445, tsc clean,
both vite targets built; GUI §0zzh unchecked. Stage 6: docs/claude/per-panel-overrides.md. Stage 4:
(`codegen._panel_override_export`, tests/test_panel_overrides_export.py;
"first column" is read off the drawn subplotspec at run time). Stage 3:
`base.shows_panel_y_title` + `is_leftmost`; meta `y_title`, `override`,
`unmatched_overrides`. Stage 2:
`ylimits.pinned_ends` owns D2; `panel_overrides` is in `_PLAN_IRRELEVANT_FIELDS`.

## Goal

In a faceted figure, let one panel carry its own
1. y limits (Min / Max, each end independently), and
2. y-axis title text,

without changing any other panel.

## What the code already does (why the plan is shaped this way)

- `Panel.y_limits` is already per panel and is the authority
  (`resolved.py:146`). It is set in `reduce.py:~1312` by
  `ylimits.limits_for(limits, {**figure_key, **key}, scope, spec.y_axis)`.
  `ResolvedPlot.y_limits` = the panels' shared value or `None`. `None` already
  makes both renderers stop sharing the axis and show every panel's tick labels
  (`render.base.shows_y_labels`). **So a per-panel override only has to change
  `Panel.y_limits`; nothing downstream needs a new code path.**
- On a faceted panel, **the y-axis title already is the facet values' text**
  (`render.base.panel_y_title` → `DisplayText.panel_title`). `style.y_label` is
  only drawn on unfaceted panels. So today a faceted figure can't show units at
  all, except by aliasing each level (e.g. `RQUAD` → `RQUAD (µV)`), and that
  alias also renames the level everywhere else it appears. #2 = "replace this
  panel's title text".
- Codegen already writes per-panel limits into the export
  (`_YLimitPlan.by_panel` + a fallback, looked up through `g.axes_dict` with
  `_panel_key_text`), and already writes per-panel y titles
  (`codegen.py:~1640`, `_ax.set_ylabel(...)` in an `axes_dict` loop).

## Design decisions

- **D1 — how an override names its panel.** It names the panel's FACET values
  only, not the ITERATE values. An override on `ColName=RQUAD` therefore
  applies in every subject's figure. Values are stored as TEXT
  (`str(value)`, NaN → `"nan"`), keeping `"01"` as `"01"`. That's the same
  spelling the export's `axes_dict` lookup uses, so preview and export match on
  one string. (An alternative is to also allow ITERATE values in the match,
  e.g. "RQUAD for subject 3 only". I'd leave that out for now: the matcher
  would allow it later without changing the stored shape.)
- **D2 — which value wins.** Panel override end > figure `y_axis.minimum/maximum`
  end > the value computed from `scope`. If both ends are set on a panel, the
  data is never consulted for that panel (the same rule as `YAxis.is_manual`).
- **D3 — an override whose panel doesn't exist.** When the level has been
  filtered out, renamed, or the factor is no longer FACET, the override is
  kept in the spec, does nothing, and is logged ("never delete"). The GUI lists
  it under "not in this figure", with a Clear button.
- **D4 — no-op when unfaceted.** A figure with one keyless panel ignores
  every override; the figure-level `y_axis` and `style.y_label` already cover it.

- **D5 — a y title can be hidden, and hiding it gives the space back.** Each
  panel's title is in one of three states: inherit (the facet text), custom
  text, or hidden (`y_label_hidden=True`, drawn as `""`). "Hidden" is its own
  flag, never the empty string, so clearing the text box means "inherit".
  The space comes back through the existing measurement: `mpl._grid_reach`
  measures `get_tightbbox` on the final layout, and an empty label contributes
  nothing to it. Plotly's `_frame` then sizes the margins and gaps from that
  measurement. There's no new spacing code. Limits of this:
  - Inner gaps are ONE width for the whole grid (the largest inner-left reach).
    A y title is ROTATED, so its horizontal room is one line of text whatever
    its length (corrected 2026-09-27; "the widest title" was wrong). Hiding one
    inner title frees nothing while another inner title shows; hiding the
    titles of every non-first column frees the gap between columns.
    The left margin follows column 0 the same way.
  - Tick NUMBERS are separate and stay. A per-panel limit override un-shares
    the axis, which turns tick numbers on for every panel (`shows_y_labels`)
    and so can WIDEN the gaps. This is correct (differently-scaled panels
    must show their numbers), and the GUI note should say so.

- **D6 — grid toggle "Y titles: first column only".** `StyleOptions.y_titles:
  "every_panel" | "first_column"` (default `"every_panel"`, today's
  behaviour). "First column" uses the same occupancy rule as the tick numbers
  (`shows_y_labels`: nothing directly to the left), so in a partial wrapped
  grid a panel with an empty cell on its left counts as first. The rule moves
  into one helper, `base.is_leftmost(resolved, row, col)`, which
  `shows_y_labels` also calls.
  Which setting wins: a panel's own `y_label_hidden` (now `bool | None`,
  where `None` = follow the grid toggle) > the grid toggle > shown. So one inner
  panel can still be forced to show its title. The panel's "Show" checkbox has
  three states: follow grid / show / hide.
  Trade-off (the GUI will say so): on a faceted figure the y title IS the panel's
  identity. Hiding inner-column titles is only readable when the columns
  already say what they are, e.g. a two-factor grid, or custom text on the
  first column such as "Hamstrings (L | R)".
  NOT added: an "x labels on the bottom row only" toggle, because that is
  already the fixed rule (`shows_x_labels`: tick labels and x title only where
  nothing is directly below). NOT added: a "panel titles" toggle, because a
  faceted panel draws no caption; the facet values are its y title. The
  figure title is one per figure and is already hidden by `style.title = ""`.

## One owner

New module `scistackplot/panels.py` owns:
- `PanelOverride` dataclass (`match: dict[str, str]`, `y_minimum`,
  `y_maximum`, `y_label`, `y_label_hidden`; `to_dict`/`from_dict`; `is_empty`).
- `panel_key_text(value) -> str` (moves `codegen._panel_key_text` here;
  codegen imports it, so there is one spelling of a facet value as text).
- `override_for(spec, key) -> PanelOverride | None`: the ONLY matcher. An
  exact match on every facet factor in `key`. If several match, the last one
  wins, with a WARN.
- `unmatched(spec, panels) -> list[PanelOverride]` for the log line and the GUI.

Consumers: `ylimits.limits_for` (limits), `render.base.panel_y_title` (title),
codegen (both), GUI (display + write). None of them match keys itself.

Spec: `PlotSpec.panel_overrides: list[PanelOverride]` (a list, not a dict,
because a key is a dict). Round-trips through `to_dict`/`from_dict`, TOML and
saved plots. No migration: a missing field means `[]`.

## Stages

### Stage 1 — spec + owner (scistackplot)
- `panels.py` as above; `PlotSpec.panel_overrides` + serialisation.
- Move `_panel_key_text` into `panels.panel_key_text`; codegen imports it.
- Tests `tests/test_panel_overrides.py`: round trip; `"01"` stays `"01"`;
  NaN → `"nan"`; exact match only (a partial key doesn't match); last match
  wins + WARN; an empty override is ignored.

### Stage 2 — limits in the preview
- `limits_for(..., panel=override_or_None)`: apply D2. Keep `_ordered`
  for swapped ends.
- `reduce` panel assembly passes `panels.override_for(spec, key)`.
- Check that the plan memo (`_Plan`, `_with_presentation`) doesn't keep old
  panels after a change that touches only `panel_overrides`. Write a test for
  it rather than assuming.
- Logging: one INFO per figure, `panel overrides: 2 applied (ColName=RQUAD
  y 0 to 400; ...), 1 not in this figure`.
- Tests: override on one panel of a globally-scoped grid → that panel differs,
  the others keep the global range, and `ResolvedPlot.y_limits is None`
  (tick labels shown on every panel). One end only → the other end is still
  computed. Both ends → data not consulted (works on an empty panel). Figure
  manual + panel override → the panel wins. Unmatched → no change + logged.

### Stage 3 — y title in the preview
- `panel_y_title`: if the override has `y_label`, return it; otherwise the
  current facet text. Aliases are not applied to the override text; it is
  shown exactly as typed.
- `layout.meta.panels[*]` gains `override` (the matched entry, or null) and
  `y_title` (the text drawn), so the GUI shows what the backend decided.
- (Dropped: "a longer override title widens the gap". A rotated title's
  horizontal room does not depend on its length.)
- Tests: override title drawn in both renderers; the others unchanged;
  unfaceted figure ignores it.
- Hidden (D5): `panel_y_title` returns `""`. Geometry tests (in
  `test_grid_gaps.py`): in a 1×3 grid with shared limits, hiding columns
  2–3's titles shrinks `GridReach.left_inner_pt` and the plotly inner gap;
  un-hiding restores the original value exactly. Same test driven by the grid
  toggle `y_titles="first_column"` (D6), plus: a partial wrapped grid
  treats a panel with an empty cell on its left as first; a panel override
  `y_label_hidden=False` beats the toggle. Hiding column 0's title
  shrinks the left margin. Hiding one inner title while another
  inner title shows leaves the gap unchanged.
- Log at DEBUG which panels' titles are hidden, next to the existing
  `panel text reach` line, so a gap that doesn't shrink can be explained
  from scidb.log.

### Stage 4 — export matches preview (codegen)
- Limits: when any override matches, `_y_limit_plan` returns per-panel
  (`share=False`, `sharey=False`). The overrides are written as a literal
  `_PANEL_YLIM = {("RQUAD",): (0.0, 400.0), ...}` (either end may be `None`)
  and applied after the existing limit code in the `axes_dict` loop as
  `_ax.set_ylim(bottom=..., top=...)`, so one end stays autoscaled.
- Titles: the existing `set_ylabel` loop looks up `_PANEL_YLABEL` first
  (a hidden title is written as `""`). The toggle is resolved in Python
  (per panel, same helper) and baked into `_PANEL_YLABEL`, so the exported
  code does not repeat the leftmost rule. Parity test: the exported axes have
  the same reach as the preview (the gaps match).
- Log what was written into the export ("export panel overrides: 2 limits, 1 title").
- Tests: exec the generated code and compare each axis's ylim/ylabel with
  the preview's `Panel.y_limits` / `y_title` (same as the existing
  export-parity tests). Per the "export matches preview" rule: when they
  differ, fix codegen; never loosen the test.

### Stage 5 — GUI (Plot Studio)
- New section **Appearance > Panels**, shown only when the figure has more
  than one keyed panel (read from `layout.meta.panels`, not worked out in TS).
  - Panel dropdown (`display_title` of each panel, plus a "not in this
    figure (N)" group for unmatched overrides).
  - Min / Max `StepperInput`s with an empty box = inherit. The placeholder
    shows the value in effect (`panels[i].y_limits`).
  - Y title text box; empty = the facet text (placeholder shows `y_title`).
    A "Show" checkbox next to it (unchecked = `y_label_hidden`), which keeps
    any typed text so re-showing brings it back.
  - Appearance > Panels, above the dropdown: "Y titles: every panel / first
    column only" (writes `style.y_titles`), with a note on the identity
    trade-off. The per-panel Show control has three states: follow grid / show / hide.
  - A note under the limits when an override has un-shared the axis: "Panels
    now have different scales, so every panel shows its tick numbers."
  - Clear (this panel) button. Blanking every field removes the entry from
    the spec; that removes a setting, not data.
- Pure logic in `panelOverrides.ts` (+ `.test.ts`, register in
  `tsconfig.test.json`): upsert/clear an entry by match, build the match
  from a meta panel's key (via the backend's text, never re-stringified in TS).
- The Y axis section's readout says "N panels overridden" when relevant.
- `sidebarGroups.ts` Appearance summary mentions panel overrides.
- Update `docs/claude/plot-studio-controls.md` map and
  `docs/gui-manual-testing-todo.md` (new §). Rebuild BOTH vite targets.
- Later (not in this plan): click a panel in the preview to select it.
  plotly only reports clicks on marks, so this needs its own hit-testing
  against panel domains.

### Stage 6 — docs
- `docs/claude/per-panel-overrides.md`: identity (D1), which value wins (D2),
  inert overrides (D3), one owner, codegen replay.

## Commands for the user (tests)
```
cd /workspace/scistackplot && pytest tests/test_panel_overrides.py -q
cd /workspace/scistackplot && pytest tests -q
cd /workspace/scistack-gui/frontend && npx tsc -p tsconfig.test.json && node --test <compiled panelOverrides.test.js>
```
(Exact frontend test command to be copied from the existing
`sidebarGroups.test.ts` workflow.)
