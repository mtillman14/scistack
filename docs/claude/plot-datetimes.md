# Datetimes in plots

Built 2026-10-22. Plan: `.claude/plan-plot-datetimes.md`. Storage side:
`docs/claude/datetime-roundtrip.md`. Tests: `scistackplot/tests/test_temporal.py`,
`scistackplotdb/tests/test_datetimes.py`, frontend `timeZone.test.ts`.

## User rules (2026-10-22)

- P1: a datetime follows the same rules as every other data type. It can be the
  measure (y now; x once continuous x-vs-y plots exist) and any factor role
  (group, colour, facet, iterate, filter).
- P2: mixed zones are a per-plot choice: `PlotSpec.time_zone` = `"local"`
  (each value at its own wall clock, the default) or a zone label (`"UTC"`,
  `"Europe/Berlin"`, `"+02:00"`), which converts every aware value.
- Q1: a datetime measure offers no **bar** (a bar from 0 is a bar from 1970).

## One owner: `scistackplot.temporal`

| concern | function |
|---|---|
| is it a datetime | `is_temporal_value` / `is_temporal_column` / `has_temporal_values`; `shape.classify_value` calls a datetime **SCALAR** |
| the mode | `normalize_mode` (validated by `scicanonicalhash.zone_from_label`) |
| measure -> axis numbers | `wall_clock_ms(series, mode)`: float64 **wall-clock ms since 1970** in the mode; naive values at their own clock; `TemporalReport` notes |
| the derived table | `apply_temporal(spec, table)` (4th derived table, after `apply_cell_collapse`, before `validate`/filters/y-limits) -> `(table, TemporalAxis)` |
| the axis | `TemporalAxis` (mode, `tick_format` by span, notes) on `ResolvedPlot.temporal`, plotly `type: "date"` (`plotly_axis`), matplotlib `MPL_DATE_AXIS_SOURCE` (executed by the renderer, emitted verbatim by codegen) |
| factor identity labels | `level_labels(series)`: the value in its own zone (`2026-07-01 14:00 CEST`; full zone names if abbreviations clash), precision as fine as the data needs, ordered by time; applied ONCE in `LongTable.from_frame` |
| "Convert to" for factors | `display_levels`: the lowest layer of `aliases.display_text` (origin `time_zone`); same-instant levels get `(from <zone>)` |
| y limits typed as dates | `parse_limit` (`ylimits.pinned_ends` reads it); `YAxis.minimum/maximum` keep ISO text (`spec._as_limit`) |
| generated code | `conversion_code` (measure), `factor_label_code` (factor), `mpl_date_axis_code` |
| GUI summary | `time_zone_summary` -> `capabilities.time_zone` |

Why ms: plotly reads numbers on a date axis as ms, so traces, error bars,
hover and ranges need no change. Spreads (SD, CI) are durations in the same
units, and every reducer stays numeric. The resolution is ~0.24 µs in 2026,
plenty for a figure (storage keeps ns).

## Measure path

`LongTable.from_frame` marks `MeasureInfo.temporal=True`, shape SCALAR.
`reduce._build_plan_timed` runs `apply_temporal`; `_Plan.temporal` carries the
axis; `reduce._attach_temporal` puts it on every figure. It appends
` (local time)` / ` (<zone>)` to the y title (unless the user typed one), and
makes a log y axis **inert** with `LOG_REASON` in `layout_notes`.
`time_zone` is in the plan cache key (not a presentation field).

- `capability.available_plots(..., temporal=)` drops BAR; `why_unavailable`
  gives `BAR_REASON`; `roles.validate` refuses a temporal BAR, an unknown zone,
  and (until 2026-10-22) a temporal **x_measure**; it is now a date x axis
  (`apply_temporal_x`, docs/claude/histogram-and-regression.md).
- Compare: **difference -> hours** (`compare._duration_scale`: `ComparisonPlan.unit="h"`,
  `scale=1/3.6e6`; y title `Δ M (h, from session s1)`; then `figure.temporal`
  is None, a number axis). **% change** is inert (`PERCENT_REASON`).
- Difference bars: unavailable for a datetime measure (their labels would be
  ms; formatting a duration is a follow-up).
- Save data: values as ISO wall-clock text in the mode; the header says the
  zone (`When (local time)`, `export._temporal_suffix`); compared -> hours.
- Range filter on a datetime measure: hidden in the GUI (its bounds would be
  ms). Follow-up.

## Factor path

The values become identity labels once, in `LongTable.from_frame`, so every
later `astype(str)`, filter, facet rule, colour pin and alias sees text that
says when. `FactorInfo.temporal_values` keeps `{label: Timestamp}` and
`temporal_format` keeps the strftime. A source's `MISSING_LEVEL` survives
(last level). scidb: `source._as_levels` leaves a datetime grouping column's
values as datetimes (missing named) instead of `astype(str)`. Codegen's
`_factor_level_lines` emits the same labels (a literal map keyed by ISO text,
plus the same formatting for unseen values).

## scidb loading (Stage 0)

`scistackplotdb.load.load_variable` selects each datetime column's `__tz`
companion (`datetime_zone_meta`, found from the table schema by
`sciduckdb.stored_tz_companions`, unit from the stored dtype metadata) and
folds it back with `sciduckdb.merge_tz_columns` right after the fetch. Before
this, every aware value reached a plot as a naive UTC instant.
`data_column_types_for` still hides the companion.

Struct variables: a datetime FIELD beside numeric fields cannot share the one
melted value column, so `_melt_fields` leaves it out with a WARN (group by it
instead). All-datetime structs melt normally.

## GUI

Data > **Time zone** (`TimeZoneSection.tsx`, pure logic `timeZone.ts`): Local
time | Convert to <each zone in the data> | UTC | Other zone… (typed, validated
by the backend: `capabilities.time_zone.problem`). Notes come from
`layout.meta.temporal.notes`. Y axis Min/Max take date text on a time axis
(`LimitInput temporal`), and the applied limits read as dates (`formatLimit`).

## Known limits / follow-ups

- ~~A datetime **x** axis~~: done 2026-10-22 (`temporal_x`; histogram-and-regression.md).
- Range filter and difference bars on a datetime measure.
- Converted mode across a DST fall-back hour: two instants share a wall clock
  (UTC has no such hour).
- Custom `plot_` functions receive the converted ms, not Timestamps.
- StatStudio statistics on datetimes.
