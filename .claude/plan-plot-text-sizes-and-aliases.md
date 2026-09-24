# Plan: per-element text sizes + display aliases in Plot Studio

Drafted 2026-09-24. Decisions locked 2026-09-24 (see "Decisions"). Awaiting build approval.
Concept doc: `docs/claude/plot-text-and-labels.md`.

## What exists today

- `StyleOptions.font_size` (base, 14 pt) sets matplotlib `font.size` inside
  `rc_context`. Every other size is relative to it (`medium`, `large`,
  `small` for bracket labels).
- `StyleOptions.tick_font_size` pins the **x** tick size only. `fit_labels`
  keeps a pinned value even when the labels overlap.
- `StyleOptions.title` / `x_label` / `y_label` exist in the spec and are read
  by `reduce._labels_for` and `reduce.x_axis_title`. **The GUI has no control
  for any of them.**
- A faceted panel's y title is the **facet level**, not `y_label`
  (`render.base` near line 94). To rename those you have to alias the level.
- `FactorInfo.label` / `MeasureInfo.label` feed `.display`, but no source
  ever sets them. The seam exists and nothing uses it.
- About 50 places across `render/*`, `reduce`, `xaxis`, `codegen` and
  `spaghetti` call `str(level)` to produce visible text. That is the
  divergence risk for aliases: each of those places is currently its own
  owner of "how a level reads".

## Part 1: text sizes for each element

### Spec

The flat `font_size` and `tick_font_size` fields are replaced by a nested
dataclass. Per the beta rule this is a clean break. `restore_spec` reverts the
old keys to defaults and adds a note for each one.

```python
@dataclass(frozen=True)
class TextSizes:
    base: float = 14.0             # was font_size
    title: float | None = None     # figure title / suptitle
    x_label: float | None = None   # x axis title, supxlabel
    y_label: float | None = None   # y axis title, facet-panel y titles
    x_ticks: float | None = None   # was tick_font_size (pin semantics kept)
    y_ticks: float | None = None
    legend: float | None = None    # entries
    legend_title: float | None = None
    groups: float | None = None    # nested-x bracket/group rows (user: own size)
StyleOptions.text: TextSizes
```

`None` means "derived from `base`", using the same ratios matplotlib uses
today. A fresh spec therefore draws exactly what it draws now.

### One owner: `scistackplot/textsize.py`

- `resolve_sizes(style) -> ResolvedSizes`: the point size of each element,
  with every `None` resolved.
- `rc_params(sizes) -> dict`: `font.size`, `figure.titlesize`,
  `xtick.labelsize`, `ytick.labelsize`, `legend.fontsize`,
  `legend.title_fontsize`. matplotlib has only one `axes.labelsize`, so the x
  and y titles are passed as an explicit `fontsize=` from `ResolvedSizes`.
- Consumers:
  - `render_matplotlib` (the `rc_context` plus explicit title sizes)
  - `codegen`: emits the same rc dict, which replaces the current
    single-key `rc_context`
  - `render_plotly`: `tickfont`, `title.font`, `legend.font`
  - `fit_labels` (the x-tick pin)
  - legend fitting (a pinned legend size is **not** shrunk, the same rule
    as the tick pin; the legend can still move below the plot)
  - bracket (group) labels: their own `groups` size. When unset, it is the
    x-tick size times the current `small` ratio. `fit_labels` keeps a pinned
    `groups` size, the same as a pinned tick size.

### Logging

One DEBUG line per render with the resolved size table. The existing INFO
figure-size line also gets `text: base=14 title=… x_ticks=pinned 10 …`.
When a pinned size overlaps, the notice names the pinned element.

### Tests

- `test_textsize.py`: resolution and rc keys. All `None` must produce the
  same sizes as today's defaults (a regression guard).
- `test_fontsize.py`: extend it. Save to `BytesIO`, then check the size of
  each element separately. This covers ticks created lazily at save time.
- A codegen parity test: the exported script's figure has the same size for
  each element as `render_matplotlib`.
- `test_restore.py`: add `text` to `full_spec()`; the old keys salvage with
  notes.

### GUI

The "Figure size" section gets a **Text (pt)** block: Base, plus eight optional
boxes (Title, X label, Y label, X ticks, Y ticks, Groups, Legend, Legend title). Each
optional box shows its derived value as a placeholder ("auto · 14"). Clearing
a box sets it back to `None`. Saved plots pick this up automatically, because
the sizes are in the spec. The existing tick-font control moves into this
block.

## Part 2: display aliases, at two levels: project and plot

Aliases come from two sources, merged in this order (first wins):

1. **the plot**: `PlotSpec.aliases`, stored with the saved plot;
2. **the project**: a `[aliases]` table in `scistack.toml`
   (`[tool.scistack.aliases]` in `pyproject.toml`);
3. the raw level or name.

There is no per-plot .toml file. A plot's state already has one home, the
saved-plot row in DuckDB, plus the generated code built from its spec. A
side file would be a second owner that neither of those carries.

### Project layer: `[aliases]` in the project config

```toml
[aliases.session]                 # one entry per drawn thing: schema key,
name = "Session"                  # variable, "Var.Column", ColName, Variant

[aliases.session.levels]          # its values; keys are level TEXT, quoted
"BL" = "Baseline"                 # "01" stays "01"
"01" = "Visit 1"

[aliases."Demographics.Sex"]
name = "Sex"
levels = { "F" = "Female", "M" = "Male" }   # inline form is equivalent

[aliases.StepLength]
name = "Step length (cm)"
```

- **Reader and owner of the grammar: `scidb.aliases`**, a new module
  in the scidb layer next to `scidb.schema_order`. It reuses
  `schema_order.locate_config` (the same config, the same `LOCATE_TTL`,
  content cached on mtime), so there is no second copy of "which config".
  Edits are read **live**, like level order.
  - `project_aliases() -> dict[str, Alias]` (`Alias(name, levels)`)
  - `render_aliases_table(aliases) -> str`: the TOML text, with keys always
    quoted and escaped through the existing `_toml_str` rules.
  - `validate(aliases, known_names)`: WARN when an entry names nothing known, when a variable and a schema key share a name (they would share one entry),
    and WARN when two levels of one factor share an alias. The project file
    is hand-edited, so it is never refused at read time.
- **The file has one writer.** `scistack_gui.config._render_scistack_toml`
  already rewrites the whole file and carries `[schema_keys]` across. It
  gains an `aliases=` argument, carries `[aliases]` the same way, and calls
  `scidb.aliases.render_aliases_table` for the text. Nested tables go
  last, for the same "a table swallows every key after it" reason.
- **Packaged projects** (`pyproject.toml`) are hand-edit only.
  `_reject_packaged_project` already refuses GUI writes there. The GUI's
  "save to project" is disabled, with that reason shown.
- **Flow into plots:** `ScidbSource` gives each table it builds a live READER
  of the project layer (`LongTable.aliases_source`; see Stage 3 notes for why not a copy).
  `scistackplot` itself never reads a config file, because it must
  work without scidb. A `CsvSource` / `DataFrameSource` has an empty layer.
- **It does not change the cache key.** Aliases are display-only, so an edit
  must not rebuild tables. Unlike `[schema_keys]`, they are **not** part of
  `ScidbSource._cache_generation`. They are read fresh on each resolve,
  cheaply (mtime).

### Plot layer: `PlotSpec.aliases`

```python
@dataclass(frozen=True)
class Alias:            # the same shape as one [aliases.X] entry
    name: str | None = None           # the thing's own display name
    levels: dict[str, str] = {}       # {level text: alias}
PlotSpec.aliases: dict[str, Alias]    # keyed by factor / variable name
```

`style.title` / `x_label` / `y_label` remain the per-plot axis and figure
title overrides and win over any name alias. They get GUI boxes. A plot entry
of `""` means "show the raw text here even though the project aliases it".
Deleting the entry falls back to the project alias.

### Rules

1. **Aliases change text only.** Filters, the location picker, facet
   `Matcher`s, variant pins, y-limit keys, colour identity and the plot-data
   CSV all keep the **raw** levels (user, 2026-09-24: CSV raw).
2. **The merged map must be one-to-one within each factor.** `validate`
   refuses a plot whose merged aliases give two levels the same text. The
   message names each clashing entry's origin (plot or project), so a project
   edit that causes the clash is findable.
3. Levels are matched as text, the same as `LevelGroup.mapping` and
   `[schema_keys]`.
4. An alias for a level that is not in the data is inert. `reconcile` notes
   plot-layer ones and does not remove them.
5. Synthetic factors: `ColName` and `Variant` are keyed by those names. A
   project `ColName` alias therefore applies to that field name in every
   variable. That is acceptable for v1; see the doc.

### One owner of display text: `scistackplot/aliases.py`

- `merged_aliases(spec, table) -> MergedAliases`: plot over project over raw,
  with the origin recorded for each entry (for logs and clash messages).
- `level_text(merged, factor, level)` and `name_text(merged, name)`.
- Applied **after the plan cache**, in the step that builds `ResolvedPlot`
  from `_Plan`. `_Plan` stays keyed without labels, so an alias edit
  re-renders without re-reducing, in the same way `style` is in
  `_PLAN_IRRELEVANT_FIELDS`. `labels` joins that tuple.
- What it writes: `XAxisPlan.tick_labels`, `XGroup.label`, panel titles,
  `Labels.x/y/color/dash/title`, and a level→text map for the legend.
  `FactorInfo.label` / `MeasureInfo.label` are set from the merged names, so
  the existing `.display` readers pick them up.
- The ~50 `str(level)` sites are split into *identity* sites (keep the raw
  value) and *text* sites (read the resolved text). Guard test: render every
  kind × {ticks, brackets, colour legend, facet, ITERATE, show-sample legend}
  with every level aliased to `«…»`. Then collect **all** mpl text artists
  and all plotly text, and assert that no raw level string appears.

### Codegen

The generated code gets the **merged** map as a literal `LEVEL_LABELS` /
`NAME_LABELS`. After all filtering and reshaping, and just before drawing,
it renames the plotted columns' values, and it translates the
`order=`/`hue_order=` lists through the same owner. This is safe only because
of rule 2. The literal is baked in on purpose, like the y limits
(`codegen._y_limit_plan`). A `plot_` step reading `scistack.toml` at run time
would be a hidden input, outside lineage. Therefore **a project alias edit
reaches an exported step only when it is re-exported**. That trade-off is
written down in the doc. Parity test: the multiset of text strings in the
exported figure equals the one from `render_matplotlib`. Risk to check in
stage 5: ruled facet layouts match on panel text, so the rename must come
after the layout is planned.

### Logging

- INFO `[aliases] using <config>: names=N, levels for session(4),
  subject(12)`, logged when the content changes.
- WARN for unknown factors and duplicate aliases in the project file.
- Per resolve, INFO: `labels: session 3/4 aliased (project 3, plot 0);
  1 alias names no level in the data (session='05')`. DEBUG: the full
  merged map with origins.

### GUI

A **Labels** section with:
- Title, X label and Y label boxes (the existing spec fields).
- For each factor that currently produces visible text, a level → alias
  table. Each row shows its origin (*project* / *this plot* / raw) and has
  two actions: "Save for this plot" and "Save to project".
- Name aliases for measure, colour, facet and ITERATE factors, with the
  same two scopes.

Python owns the list of labelable factors, their levels and the current
merged text with origin. It ships as `layout.meta.labelable` from the
resolved plot. The project write is a new RPC `plot_project_alias_set`,
which goes through `scistack_gui.config` (the one file writer) and
`scidb.aliases` (the grammar), then re-resolves. Duplicate aliases
are shown inline from `validate`.

## Stages

1. [BUILT 2026-09-24, tests pass] `TextSizes` + `textsize.py` + mpl/codegen/plotly/fit consumers + tests +
   restore fixture.
2. [BUILT 2026-09-24, npm tests pass, not checked by eye] GUI Text block; rebuild both vite bundles; manual-test doc entry.
3. [BUILT 2026-09-24, pytest unrun] `scidb.aliases` (read, render, validate) + `[aliases]` round-trip in
   `_render_scistack_toml` + `ScidbSource` → `LongTable.aliases_source` (live reader) +
   tests (live edit reaches the next resolve; table cache is NOT
   invalidated; Paths popup keeps `[aliases]`).
4. [BUILT 2026-09-24, pytest unrun] `PlotSpec.aliases` + `aliases.py` + applying labels after the plan +
   validate/reconcile + the raw-text guard test.
5. [BUILT 2026-09-24, pytest unrun; RELABEL, not rename — see notes] Codegen: literal maps + rename + parity test.
6. [BUILT 2026-09-24, pytest unrun, npm pass, not eye-checked] GUI Labels section + `meta.labelable` + `plot_project_alias_set`;
   bundles; manual-test doc.
7. [BUILT 2026-09-24] README sections (scistackplot, scidb config) + ADR entry in
   `docs/claude/decisions.md`. Add `[aliases]` to `config-file-formats.md`.

## Decisions (user, 2026-09-24)

1. Project-wide aliases: **yes**, overridable per plot.
2. Bracket/group labels: **their own size** (`TextSizes.groups`).
3. Plot-data CSV: **raw** levels.
4. No per-plot .toml; the plot layer lives in the `PlotSpec` (saved plots).

## Stage 1 build notes (2026-09-24)

- `TextSizes` sits in `spec.py`, and `StyleOptions.from_dict` builds it. The
  strict reader refuses the old `font_size` key; `restore_spec` notes it.
- `textsize.py`: `resolve_sizes`, `rc_params`, `ResolvedSizes` (`pinned`,
  `legend_title_for`, `to_dict`, `describe`).
- mpl: `rc_context(rc_params(...))`; explicit y-title size;
  `_font_pt` removed (no relative-name lookups left). The legend ladder is
  shared by both placements (`_legend_ladder`); a fixed legend size is the
  whole ladder. The legend title size is fixed or follows the entries.
  `_Legend.describe()` gains `title_font_pt`.
- plotly: per-axis `title.font`/`tickfont`, legend and title fonts, bracket
  annotation = `groups` (it was 0.8 x, a second owner). `meta.text_sizes` is
  there for the Stage 2 placeholders.
- codegen: emits the `rc_params` dict literally (single-quoted repr), plus
  `yaxis.label.set_size` only when the y and x title sizes differ. It draws
  no brackets, so `groups` does not apply there.
- GUI (minimal, to keep it working against the strict reader): "Font (pt)"
  writes `text.base` and "Tick font (pt)" writes `text.x_ticks` through a new
  `setText`. tsc is clean, npm tests pass (344), and both bundles are rebuilt.
- Tests: new `test_textsize.py`; `test_fontsize.py` rewritten to cover each
  element; `test_restore.py` fixture and two drift tests; small edits in
  `test_fitted_codegen.py` and `test_plot_service.py`.
- Docs: README, figure-size.md, manual-test §0zj.

## Stage 2 build notes (2026-09-24)

- `frontend/.../PlotStudio/textSizes.ts` (pure, in `tsconfig.test.json`):
  - The rows come from `meta.text_sizes` keys (Python owns the element list;
    an unknown key still gets a box). There is a fallback list before the
    first render.
  - `placeholderFor` shows Python's resolved size, never a ratio computed
    in TS.
  - `withTextSize` DELETES a cleared key. A stored null would read as
    "modified" against a saved spec, because `to_dict` drops nulls.
  - `resetTextSizes` keeps `base`. `fixedTickNote` is added to the overlap
    notice.
- `PlotStudio.tsx`:
  - `setTextSize(key, value)` and `resetText` replace Stage 1's `setText`.
  - The `TextSizesPatch` interface was removed; the `TextSizesValue` type is
    used instead.
  - New `SizeInput`: blank → null; only a positive value commits.
  - The block is a 2-column grid under the pixel readout, with a Reset
    button shown only when something is fixed. The old "Tick font (pt)" row
    is removed (it is now "X ticks").
- tsc is clean, npm has 352 passing (8 new), and both bundles are rebuilt and
  grepped. Manual-test §0zj was rewritten to cover the whole feature.

## Stage 3 build notes (2026-09-24)

- `scidb/aliases.py` owns the grammar:
  - `normalize` is the one reader; it WARNs and drops what it cannot use,
    and never raises.
  - Reading: `aliases_in(config)` caches on mtime and logs INFO once per
    read. `project_aliases()` reads live through
    `schema_order.locate_config`, so it uses the same file as
    `[schema_keys]`.
  - `validate(table, schema_keys=, variables=)` WARNs about unknown names,
    a schema key and a variable sharing a name, and duplicate level aliases.
    It returns the messages.
  - Writing: `render_aliases_table(raw, root=)` normalises first, keeps bare
    keys unquoted, always quotes level keys, and escapes control characters.
    `with_alias(raw, thing, name=, level=, alias=)` is the GUI's future edit.
  - Plain `AliasTable` dicts cross the layer boundary (TypedDict
    `AliasEntry`). scistackplot reads them with its own `Alias.from_dict`,
    because it cannot import scidb.
- `DatabaseManager.dataset_aliases`: a live property, validated when the
  content changes (object identity against `_validated_aliases`), with
  variables from `list_variables()`.
- **DEVIATION from the plan:** tables carry `LongTable.aliases_source`, a
  zero-argument READER, instead of a `LongTable.aliases` snapshot. The plan
  cache keys on `id(table)`, so a per-call copy with fresh aliases would
  re-reduce on every resolve, and a snapshot baked into the cached table
  would go stale. `BaseSource._attach_aliases_source` sets it once at build
  (a `replace`, before publishing to the memo). `ScidbSource._aliases_source`
  reads `db.dataset_aliases`. `LongTable.project_aliases()` returns
  `{thing: Alias}` and, on failure, WARNs and returns `{}`. Derived tables
  carry it automatically (`replace`).
- `scistackplot.spec.Alias(name, levels)`, with `to_dict`/`from_dict`, is
  exported now; Stage 4 uses it for `PlotSpec.aliases`.
- GUI `_render_scistack_toml(aliases=)`: all 5 callers pass
  `section.get("aliases")`. It is emitted last, after `[schema_keys]`.
- Tests: `scidb/tests/test_aliases.py`,
  `scistackplot/tests/test_project_aliases.py`,
  `scistackplotdb/tests/test_source_project_aliases.py`, and 2 tests in
  `scistack-gui/tests/test_config.py`.
- Open for Stage 4: a grouping column's factor is named by its COLUMN
  (`FactorVariable.factor_name`, e.g. `Sex`), but the alias key is
  `Demographics.Sex` (its `label`). The merge must look up the qualified
  label first, then the factor name.

## Stage 4 build notes (2026-09-24)

- `PlotSpec.aliases: dict[str, Alias]` (`to_dict`/`from_dict`). `"aliases"`
  was added to `_PLAN_IRRELEVANT_FIELDS`, so `_with_presentation` restores it
  onto a cached plan by construction: an edit never re-plans.
- `scistackplot/aliases.py` (the one owner):
  - `DisplayText`: merged `names`/`levels` with each entry's origin, the
    FactorVariable lookup `keys` (qualified label first), and the figure's
    roles (`x_layers`, `color`, `sample_color`, `dash_layers` outermost
    first).
  - Its methods: `level`, `name`, `color_level`, `sample_level`, `x_tick`,
    `dash_id` (split on `" | "`, raw on wrong arity), `panel_title`,
    `figure_title`, `x_plan` (an aliased COPY: `tick_labels` and group
    labels; `order` untouched), and `for_figure`.
  - Functions: `display_text(spec, table)` (merges and reads the project layer
    NOW), `merge`, `check_distinct` (over the TABLE's levels of each drawn
    factor; raises `AliasError(RoleError)` naming origins), `log_summary`
    (INFO once per resolve), and `name_text`.
- reduce:
  - `resolve`/`resolve_one` merge once and log once, then pass `text=` to
    `_build_figure`.
  - `_build_figure` calls `for_figure(...)` and `check_distinct(...)` over
    the x layers, colour, sample colour, dash layers, facets and figure keys,
    then `_labels_for(text=)` and `figure_text.x_plan(...)`. The result is
    stored as `ResolvedPlot.text`.
  - `x_axis_title(..., text=None)` merges itself when it is not given text
    (codegen).
- Renderers, the text sites only:
  - mpl: every colour `label=` goes through `color_level`; distribution and
    bar ticks and scatter/strip/spaghetti ticks through `x_tick`; the dash
    handle through `dash_id`; the sample handles and legend ordering through
    `sample_level`.
  - base: `panel_y_title` → `text.panel_title(panel.key)`. `panel.title`
    stays the raw IDENTITY, which the layout rules match.
  - plotly: trace names; sample trace names (own colour → `sample_level`,
    else `color_level`); dash legend names; right-margin entries; `_x_ticks`
    (the positional non-plan case aliased; a NEW branch gives a plain
    categorical axis `tickvals` = raw categories and `ticktext` = aliased,
    only when they differ).
- NOT aliased (by design, or deferred):
  - hover text (`_level_hover`, sample hover, spaghetti hovertext);
  - `ResolvedPlot.figure_label` (the GUI's figure pager);
  - the plot-data CSV (raw, per the user's decision);
  - codegen (Stage 5).
- `reconcile` → `_stale_aliases`: notes (kept) for a thing that is neither a
  factor nor a measure, and for aliased levels absent from the data.
- `ResolvedPlot.to_dict` panels gain `display_title`.
- Tests: `scistackplot/tests/test_display_aliases.py` (its basename differs
  from scidb's `test_aliases.py`). It holds the merge/one-to-one/labels/
  nested/facet/live-project/no-re-plan/round-trip/reconcile unit tests, plus
  the GUARD, which has five scenarios (nested bar, faceted box, iterated bar,
  dashed band, coloured sample). The guard aliases everything to `«…»`,
  collects the mpl and plotly visible text, and asserts that no raw level or
  name survives. The `test_restore.py` fixture now sets `aliases`.

## Stage 5 build notes (2026-09-24)

- **DEVIATION:** the exported code RELABELS the drawn text; it does not
  rename `df`'s values. Codegen emits raw literals everywhere (col_order,
  nested order, declared orders, spaghetti positions, overlay palettes), and
  it sorts dash ids by their RAW text as the preview does. A rename would
  have to translate all of them, and it would reorder the dash styles.
  Relabelling mirrors `ResolvedPlot.text`: identity stays raw.
- `codegen._ExportAliases` / `_export_aliases`: the drawn factors (x layers,
  colour, sample colour, dash layers outermost first, facets) and only the
  levels whose text differs. `check_distinct` runs at export too.
- `_alias_definition_lines` emits `_aliases` + `_alias()` before the call,
  only when a drawn factor has a level alias.
- Facet y titles use `_alias` when a facet factor is aliased.
- `_alias_relabel_lines` is emitted after the overlay (which rebuilds the
  legend) and the suptitle, and before `_fitted_tick_lines`:
  - x ticks when categorical: single `_alias(x, t)`; nested split and joined
    on `" · "`;
  - `_legend_text` = names (colour/sample column → name alias, `_dash` →
    the dash layers' names) under the levels (sample, then colour, which
    wins), applied to the legend title and entries;
  - dash ids part by part on `" | "`.
- Names: the y title is `style.y_label or text.name(y, y)`; the overlay
  legend title uses name aliases; the heatmap title uses the measure's name
  alias. `x_axis_title` was already aliased (Stage 4).
- `_fitted_tick_lines`: the wrap map is keyed on the ALIASED order, because
  the relabel runs first.
- With no aliases the output is byte-identical to before (tested).
- Tests: `scistackplot/tests/test_codegen_aliases.py`:
  - none-emits-nothing;
  - single-axis parity with render_matplotlib (ticks, legend entries,
    x/y titles);
  - facet-title parity;
  - the project layer baked as a literal;
  - a clash refused at export;
  - the exported-figure guard over 4 scenarios (nested bar, faceted box,
    dashed band, coloured sample).
- Known remaining difference, which predates aliases: seaborn's relplot
  legend lists column-name subtitles where the preview has one title line.
  They are aliased now, but laid out differently.
- Follow-up 2026-09-24, prompted by the user's rule that the export always
  matches the preview: catplot with the colour ON x drew no legend. seaborn
  0.13 treats that hue as redundant, and `legend="auto"` then hides it,
  while the preview lists the colours. `_plot_call` now emits `legend=True`
  for `sns.catplot` when `color == x` and there are ≥2 colour levels.
  Tests: `test_codegen.py::test_the_legend_is_kept_when_the_colour_is_the_x_axis`
  (bar and box; the export's entries equal the preview's), and
  `test_a_nested_axis_needs_no_legend_override`. The single-axis alias parity
  test checks the legend again.

## Stage 6 build notes (2026-09-24)

- `scistackplot.aliases.labelable(text, table, measure=, factors=)` gives the
  measure first, then each drawn factor with its role, `key`
  (`DisplayText.entry_key`: a grouping column's `Var.Column`, so the GUI
  writes where the lookup reads first), name, levels `{raw, text, origin}`
  (capped at `MAX_LABELABLE_LEVELS=200`, with `truncated`). New
  `DisplayText.name_origin` and `entry_key`. `_build_figure` stores it on
  `ResolvedPlot.labelable`, and plotly ships it as `layout.meta.labelable`.
- The project write:
  - `scistack_gui.config.set_project_alias(db_path, thing, name=_UNCHANGED|
    str|None, level=, alias=)`. It goes through `scidb.aliases.with_alias`
    and the one whole-file writer, then calls `scidb.aliases.clear_cache()`
    (mtime tick). It refuses pyproject.toml (ValueError naming
    `[tool.scistack.aliases.<thing>]`) and never creates a config
    (FileNotFoundError).
  - `plot_service.set_project_alias` turns a refusal into `{ok: false,
    error}`, clears `_last_resolved` (it holds stale text) and drops no table.
  - Handler `plot_project_alias_set` → `/plot/project-alias`
    (needs_db=False, holds_db_lock=False). The route is in `api.ts`.
- Frontend:
  - `aliasEdit.ts` (pure, node-tested): a cleared value deletes its key, an
    emptied entry is removed, and `aliases` stays `{}`.
    `placeholderFor(project → "X (project)")`, `projectAction`
    (save/remove/null), `aliasedCount`.
  - `LabelsSection.tsx`: Title/X/Y boxes, then one block per labelable entry
    (a name row, and "▸ N levels" with one row per level). Rows show the raw
    text, the plot box, and a "↑ project"/"✕ project" button (hidden for a
    CSV via `projectEnabled`).
  - PlotStudio: `setTitleText` (blank deletes the key), `setAliases`,
    `refreshResolve` (re-resolves the SAME spec after a project write, since
    nothing else would redraw), `writeProjectAlias`.
  - tsc is clean and npm passes 360; both bundles are rebuilt and grepped.
- Tests:
  - `test_display_aliases.py`: the labelable list, meta, and the qualified
    key.
  - `test_config.py`: writes a level and keeps the file, set/clear
    name/level, read back at once, refuses pyproject, never creates a config.
  - `test_plot_service.py`: ok answer and dropped kept resolve; refusal as
    an answer.
- Not built: a GUI way to set the plot-layer `""` ("show raw here over the
  project"). The spec and the merge support it; it can only be typed into a
  saved spec.
- Manual test: §0zk.

## Stage 7 build notes (2026-09-24)

- `scistackplot/README.md` has a new "What the text reads as: display
  aliases" section. The text-size paragraph was already updated in Stage 1.
- `docs/claude/decisions.md` has the new entry D-2026-09-24-2.
- `docs/claude/config-file-formats.md` has had its `[aliases]` section since
  Stage 3. The scidb README documents no config tables, so nothing was added
  there.
- All 7 stages are built. What is left is the user's pytest run for Stage 6
  and the eye checks in §0zj and §0zk.
