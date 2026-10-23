# Plan: datetimes in plots (measure, factor, zones)

Status: APPROVED 2026-10-22; implementing. Follows .claude/plan-datetime-roundtrip.md.

## User decisions

- P1: datetimes follow the same rules as every other data type. A datetime
  column can be the measured value (y) now and either axis once continuous
  x-vs-y plots exist. Like any variable it can also be a group/label factor.
- P2: mixed zones are a per-plot choice. A Plot Studio control offers
  "Local time" (each value at its own wall clock, the default) or "Convert to
  <zone>".

## Findings

- `scistackplotdb.load_variable` drops the `__tz` companion, so every aware
  value reaches the plot as a naive UTC instant (the datetime-roundtrip
  doc's "plots show the stored instant").
- `scistackplot.shape.classify_value(pd.Timestamp)` is UNKNOWN, so a
  datetime is not a measure at all. Reducers go through `numeric.coerce_numeric`
  (float64).
- Factor levels are stringified with `astype(str)` at about 10 sites
  (reduce, variants, custom). A datetime level would read
  `2026-07-01 14:00:00+02:00` and sort as text.
- Plotly `type: "date"` axes are naive (no zone), and matplotlib date axes
  take float days.

## Design

One owner, `scistackplot/temporal.py`:

- `TimeZoneMode`: `PlotSpec.time_zone`, either `"local"` (the default) or a zone
  label (`scicanonicalhash.zone_label` vocabulary: IANA name, `"UTC"`,
  `"+02:00"`). It is saved with the spec; restore_spec / presets carry it.
- `wall_clock_ms(series, mode) -> (float64 wall-clock ms, report)`: the one
  conversion. Local mode uses each value's own wall clock. Convert mode turns
  aware values into the target zone, then takes the wall clock. Naive values
  are placed at their own wall clock in both modes, counted in the report
  (shown as a note: "3 values had no zone; placed as given").
  - Caveat (documented): in convert mode a DST fall-back hour repeats, so two
    instants in it share a wall clock. UTC as the target has no such hour.
- `level_text(series, mode) -> (labels, chronological order)`: a datetime
  factor's level labels (`2026-07-01 14:00 CEST`, seconds/fraction only when
  present), ordered by instant / wall clock rather than as text.
- `TemporalAxis` (on ResolvedPlot): mode, zone shown in the axis title
  (`"local time"` / the zone), and the tick format chosen from the span.

### Measure (y now; x later)

- Shape: a datetime column is `Shape.SCALAR` with `MeasureInfo.temporal=True`.
  Availability follows the scalar rules (P1).
- `reduce` converts the measure ONCE through `wall_clock_ms` (before
  collapse), so means, medians, SD, CI, sample points, spaghetti and
  y-limits all run on numbers. Spreads are durations.
- Renderers: plotly `yaxis.type = "date"` with ISO wall-clock strings. The
  `date` axis also takes error bars in ms. matplotlib gets a date locator and
  formatter on float days. Hover shows each value with its zone (local mode)
  or the target zone.
- Codegen (export must match preview): the generated code calls the same
  conversion inline (`tz_convert` / `tz_localize(None)`) and sets a
  DateFormatter.
- y-limits: the user's limits are datetimes (ISO text in the spec). The auto
  rule is unchanged on the numbers.
- Not meaningful, so inert with a reason (same mechanism as today's
  "a comparison that cannot apply"): log axis; % change. A difference
  comparison gives a DURATION measure (shown in hours: the y title says so).
- Q1 (user, 2026-10-22): a temporal measure does NOT offer the bar kind
  (`available_plots` leaves it out and gives the reason).

### Factor (group / colour / facet / iterate / filter)

- The datetime column's values become their `level_text` labels at ONE
  point: where a data column is attached as a factor
  (`scistackplotdb._attach_groupings` / `LongTable.from_frame`). Every later
  `astype(str)` then sees text that already says what it means. The level
  order is chronological (LongTable carries the order, as it does for
  zero-padded keys).
- The labels depend on the mode, so a mode change re-resolves the table
  (the GUI already re-resolves on any spec change).

### Data and export

- `plot_data` CSV: a temporal measure is written as ISO 8601 with its offset
  in the display mode (`2026-07-01T14:00:00+02:00`), and a `<measure> zone`
  column in local mode.
- `scistackplotdb.load_variable`: select the stored columns and fold the
  companion back (`sciduckdb.storage_column_names` + `merge_tz_columns`),
  the same seam every other reader uses.

### Plot Studio

- A "Time zone" control (Style or Data group, decided by the
  plot-studio-controls principles). It shows only when the measure or a
  factor is temporal: "Local time" | "Convert to" + a zone picker (the
  zones present in the data first, then UTC, then free text validated by
  `zone_from_label`).
- The notes area shows the naive-count report and any DST caveat hit.
- docs/gui-manual-testing-todo.md gets a new section.

## Stages

0. scistackplotdb: load zones (merge_tz_columns); a test that an aware
   variable reaches the LongTable aware.
1. scistackplot.temporal + PlotSpec.time_zone (+ restore/presets/
   FIELD_CLASSES) + unit tests.
2. Measure path: shape/describe, reduce conversion, ResolvedPlot
   TemporalAxis, plotly + mpl axes, hover, y-limits, inert rules,
   difference -> duration. Golden ResolvedPlot tests in both modes.
3. Factor path: level_text at attach, chronological order. Tests for group /
   colour / facet / filter by a datetime column.
4. Codegen + plot_data (export matches preview: the existing parity
   harness).
5. Plot Studio control + notes; frontend build (BOTH vite targets);
   manual-testing doc.
6. Docs: docs/claude/plot-datetimes.md, scistackplot README section.

Deferred: a datetime X axis (lands with continuous x-vs-y plots, on the
same `wall_clock_ms` / TemporalAxis owner). StatStudio statistics on
datetimes are also deferred.

## Status (2026-10-22)

All stages implemented; tests written, Python tests not run (the user runs
them). The frontend `npm test` passes (669) and both vite targets are rebuilt.
Changes from the draft, found while building:
- Axis numbers are wall-clock MILLISECONDS (plotly date axes read ms as they
  are), not ns.
- A datetime classifies as SCALAR in `shape.classify_value`, so scidb's
  describe offers it as plottable and as a whole-variable grouping.
- The CSV writes wall-clock ISO text with the zone in the HEADER, not a
  per-value offset: after averaging, a row has no single zone.
- Difference bars and the range filter are unavailable for a datetime measure
  (follow-up); a temporal x_measure is refused with a reason (deferred).
- A struct's datetime field beside numeric fields is left out of the melt with
  a WARN.
Doc: docs/claude/plot-datetimes.md.
