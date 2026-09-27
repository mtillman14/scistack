# Per-panel overrides

How one faceted panel gets its own y range and y title, how a grid hides the
y titles of its inner columns, and which module owns each decision. Written
2026-09-27. The plan is `.claude/plan-per-panel-overrides.md` (decisions D1-D6).

## The fields

```python
PlotSpec.panel_overrides: list[PanelOverride]
StyleOptions.y_titles: "every_panel" | "first_column"   # default every_panel

PanelOverride(
    match={"muscle": "SOL"},   # the panel's FACET values, as text
    y_minimum=None, y_maximum=None,   # this panel's ends; None inherits
    y_label=None,              # replaces the facet text; None inherits
    y_label_hidden=None,       # True hide / False force on / None follow y_titles
)
```

A field left `None` inherits. An override with every field `None` is empty and
is skipped. `y_label_hidden` is its own flag, never `y_label == ""`: an emptied
text box means "inherit", and text typed before a hide survives it.

## Identity: facet values as text (D1)

An override names its panel by the panel's **facet values only**, never by
grid position and never by the figure's ITERATE values. So an override on
`muscle=SOL` applies to the SOL panel of every subject's figure, and survives a
change of grid size, filters or variants.

Values are stored as **text**: `panels.panel_key_text` is `str(value)`, with a
missing/NaN level spelled `"nan"`. `"01"` stays `"01"` and is not the level
`1`. The exported code's `axes_dict` lookup uses the same spelling
(`str(v)`), so preview and export match on one string. The GUI never turns a
value into text; it writes back the `match` the backend sent.

The match is **exact**: the same set of facet factors, the same text. A partial
key never matches (`{muscle: SOL}` is not the panel `{muscle: SOL, side: L}`).
An empty `match` matches nothing, so an unfaceted figure takes no override;
its own `y_axis` and `style.y_label` already cover it. When several entries
match (a hand-edited spec; the GUI never writes duplicates), the **last one
wins** and a WARN says so.

## Owners

| Concept | Owner | Consumers |
|---|---|---|
| which override belongs to a panel | `panels.override_for` (and `overrides_by_key` for the export's table, `unmatched` for inert ones) | `reduce`, `render.base`, `resolved.to_dict`, codegen |
| facet value → text | `panels.panel_key_text` | the matcher, codegen, `render.base.panel_override_meta` |
| which typed end wins (D2) | `ylimits.pinned_ends` | `ylimits.limits_for`, codegen |
| "first column" | `render.base.is_leftmost` | `shows_y_labels` (tick numbers), `shows_panel_y_title` |
| whether and what a panel's y title draws | `render.base.shows_panel_y_title` + `panel_y_title` | both renderers, `resolved.to_dict`, GUI meta |
| what the GUI shows | `render.base.panel_override_meta` → `layout.meta.panel_overrides` | `PanelsSection.tsx` |
| editing the list | `panelOverrides.ts` (`upsertOverride`, `clearOverride`) | `PanelsSection.tsx` |

## Limits (D2)

End by end: **panel end > figure end (`y_axis.minimum/maximum`) > computed**.
Both ends typed (from either source) means the data is never read, so a range
can be set on an empty panel; a swapped pair is put in order. One end typed
keeps the other computed; with no computed range at all, the panel
autoscales rather than invent the other end.

Overrides are applied per panel in `reduce._build_figure`, on top of the
plan's computed ranges, and never change those ranges. That is why
`panel_overrides` is in `reduce._PLAN_IRRELEVANT_FIELDS`: typing a Max only
redraws. `_with_presentation` puts the new overrides back onto a cached plan's
spec, because `_build_figure` reads `plan.spec`.

No renderer change was needed. `ResolvedPlot.y_limits` is the panels' common
range or `None`; once one panel differs it is `None`, so both renderers stop
sharing the axis and **every panel shows its tick numbers**
(`shows_y_labels`). That is deliberate: numbers hidden on panels with different
scales would read as one scale.

## Titles (D5, D6)

On a faceted panel the y title **is** the facet text (`panel_y_title`): the
panel's identity, which is why panels carry no caption. The rule:

1. Not faceted (empty key): the figure's y label on the leftmost panel, as
   before. Overrides and the toggle do not apply.
2. `shows_panel_y_title`: the panel's `y_label_hidden` if set; else
   `y_titles == "first_column"` → `is_leftmost` (nothing directly to the
   left, the tick numbers' rule, so a wrapped grid's second-row first panel
   counts); else shown.
3. Shown: the override's `y_label` exactly as typed (no aliases), else the
   aliased facet text.

A hidden title is `""`. **Room is given back by measurement, not by new
spacing code**: `mpl._grid_reach` measures each panel's text with
`get_tightbbox`, an empty label measures nothing, and plotly's `_frame` sizes
margins and gaps from that (docs/claude/plot-panel-spacing.md). Two limits:

- There is **one** column-gap width for the grid: the largest inner-left reach.
- A y title is **rotated**, so its horizontal room is one line of text,
  whatever its length. Hiding one inner title frees nothing while another
  inner title shows; hiding every non-first-column title frees the gap. A
  longer title does not widen the gap.

DEBUG `y titles hidden: SOL (0,1); ...` sits just before the `panel text
reach` line, so a gap that did not shrink can be explained from `scidb.log`.

## Export (codegen)

`_panel_override_export` builds the replay, keeping only overrides whose
panel is in the data. Without that filter, an override for a panel the preview
doesn't draw would make the export's panels stop sharing an axis while the
preview's still share.

- **Limits:** any override with an end sets `sharey=False`. A literal
  `_panel_ylims = {key: (low|None, high|None)}` (from `pinned_ends`) is
  applied after every other y-range line, including the bar sticky-edge
  release, with `set_ylim(bottom=…, top=…)`. An end given as `None` keeps
  what the axis has.
- **Titles:** `_ytitles = {key: (text|None, hidden|None)}` is baked in, since
  text and hide depend only on the facet values. **"First column" is decided
  at run time** from the drawn axes (`get_subplotspec().colspan.start == 0`),
  because the generated function draws one iteration and a figure missing a
  panel has a different grid from figure 0. Seaborn grids fill row-major with
  no hole on the left, so this equals `is_leftmost`.
- With no overrides and `every_panel`, the generated code is unchanged.
- The export writes an INFO log line `export panel overrides: N limit(s), N title(s) baked in,
  y titles <mode> (N override(s) match no panel in the data)`.

Parity tests (`tests/test_panel_overrides_export.py`) read every expectation
off `resolve`, per the export-matches-preview rule.

## GUI

Appearance › Panels (`PanelsSection.tsx`), shown only for two or more keyed
panels (`offersPanels`). Everything displayed comes from
`layout.meta.panel_overrides`: `panels[]` (`match`, `display_title`,
`y_title` drawn, `grid_row/col`, `y_limits` drawn, `override`), `unmatched[]`,
`y_titles`, `shares_y`. Editing: `upsertOverride` edits the last entry for a
match (matching the backend's "last one wins") and drops duplicates; an entry
left empty is removed. That removes a setting, never data. Inputs are keyed by
panel so a half-typed box never carries over. Log line:
`[Plot Studio] panel_overrides_set`. The GUI check to do by eye is §0zzh in
`docs/gui-manual-testing-todo.md`.

## Logs

- `panel overrides [subject=01]: N applied (muscle=SOL y 0 to 5; ...), N not in
  this figure (...)`: INFO, once per figure, only when the spec has overrides.
- `panel overrides: N entries match panel ...; the last one is used`: WARN.
- `y titles hidden: ...`: DEBUG, next to `panel text reach`.
- `export panel overrides: ...`: INFO, on export.

## Not built

- "x labels on the bottom row only": already the fixed rule (`shows_x_labels`).
- Panel captions / "titles on the top row": panels have none. **Column
  headers** (facet names once above each column, pairing with first-column-only
  y titles) is the open follow-up; it needs measured vertical room.
- Click-a-panel-in-the-preview to select it: plotly reports clicks on marks
  only; it needs hit-testing against panel domains.
- Matching on ITERATE values too ("RQUAD for subject 3 only"): the stored shape
  allows it later without a change.
