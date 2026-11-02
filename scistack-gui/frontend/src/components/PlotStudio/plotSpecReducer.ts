/**
 * The spec edits of Plot Studio as one pure reducer (design review 07 G7).
 *
 * Plot Studio keeps the figure's spec in one undoable state
 * (`useHistoryState`); each control used to edit it with its own inline
 * `setSpec(prev => ...)` updater, spread through a 7,000-line component. The
 * edits that are plain field changes live here instead, as typed actions
 * grouped by the rail group whose controls send them
 * (`sidebarGroups.ts`): Data, Chart, Structure, Statistics, Appearance.
 * `PlotStudio` dispatches them through `setSpec(prev => plotSpecReducer(prev,
 * action))`, so every action is still one undo step.
 *
 * No React and no DOM, so `npm test` runs it (`plotSpecReducer.test.ts`).
 * Edits that need component state (a backend report, a picked location, the
 * schema keys) stay in the component for now.
 */

import { kindForXMeasure, type HistogramOptions, type KdeOptions, type RegressionOptions } from './continuousKinds.js'
import type { Comparison } from './compare.js'
import type { DifferenceBar } from './differenceBars.js'
import type { PanelOverride, YTitles } from './panelOverrides.js'
import { joinSetting, sampleColorSetting, toggleShowSample, type JoinChoice } from './showSample.js'
import type { SpecAliases } from './aliasEdit.js'
import type { SpecColors } from './colorEdit.js'

export type MatchOp = 'starts_with' | 'ends_with' | 'contains' | 'not_contains' | 'equals' | 'regex'

export interface Matcher {
  op: MatchOp
  value: string
  label?: string | null
}

export interface FacetOptions {
  /** Grid size. null/absent means "compute me from the other one and the panel count". */
  n_rows?: number | null
  n_cols?: number | null
  /** One entry per row / column slot; a blank value is an unset slot. */
  rows?: Matcher[]
  cols?: Matcher[]
  share_x?: boolean
  // No `share_y` — it became `Spec.y_axis`, see YAxis below.
}

/**
 * What the y axis spans, and what separates spans.
 *
 * `scope` names the factors that get their OWN limits. Empty means one range
 * across the whole dataset; naming every panel factor means each panel scales
 * to itself; anything between is the useful middle ("one scale per subject").
 * Only factors that separate panels — iterate and facet — may appear, because
 * a colour or replicate factor lives inside a single panel.
 */
export interface YAxis {
  scope?: string[]
  /** A number, or ISO date text on a datetime axis (`temporal.parse_limit`). */
  minimum?: number | string | null
  maximum?: number | string | null
}

/** The `StyleOptions` fields this panel edits. The tick settings are null
 *  for "fitted" (scistackplot.fit_labels) and a value for "fixed". */
export interface StylePatch {
  width?: number
  height?: number
  tick_rotation?: number | null
  tick_every?: number | null
  hide_legend_ticks?: boolean
  /** The "Not plotted" footnote under the figure (StyleOptions.not_plotted_note). */
  not_plotted_note?: boolean
  /** Which panels draw their y title (StyleOptions.y_titles). */
  y_titles?: YTitles
}

/** The spec fields this reducer edits. `PlotStudio`'s `Spec` extends it. */
export interface PlotSpecFields {
  kind: string
  x_measure?: string | null
  color?: string | null
  aggregate?: { statistic: string; error: string; pooled?: boolean }
  cell_statistic?: string
  show_sample?: string[]
  join_sample?: boolean | null
  sample_color?: string | null
  sample_in_legend?: boolean
  show_missing_levels?: boolean
  time_zone?: string
  histogram?: HistogramOptions
  regression?: RegressionOptions
  kde?: KdeOptions
  categorical?: { stat?: string; level?: string | null }
  bar?: { stack?: boolean }
  facet?: FacetOptions
  y_axis?: YAxis
  style?: { [key: string]: unknown }
  aliases?: SpecAliases
  colors?: SpecColors
  panel_overrides?: PanelOverride[]
  difference_bars?: DifferenceBar[]
  comparison?: Comparison | null
}

/** One spec edit. Grouped by the rail group that sends it. */
export type PlotSpecAction =
  // Data
  | { type: 'setXMeasure'; name: string | null }
  | { type: 'setTimeZone'; mode: string }
  // Chart
  | { type: 'setCategorical'; patch: { stat?: string; level?: string | null } }
  | { type: 'setBarStack'; stack: boolean }
  | { type: 'setHistogram'; patch: HistogramOptions }
  | { type: 'setKde'; patch: KdeOptions }
  | { type: 'setRegression'; patch: RegressionOptions }
  // Structure
  | { type: 'setColor'; factor: string | null }
  | { type: 'setFacet'; patch: Partial<FacetOptions> }
  | { type: 'setComparison'; comparison: Comparison | null | undefined }
  // Statistics
  | { type: 'setCellStatistic'; statistic: string }
  | { type: 'setAggregate'; patch: { statistic?: string; error?: string; pooled?: boolean } }
  | { type: 'setShowSample'; name: string; on: boolean }
  | { type: 'setJoinSample'; choice: JoinChoice }
  | { type: 'setSampleColor'; choice: string }
  | { type: 'setSampleInLegend'; on: boolean }
  | { type: 'setDifferenceBars'; bars: DifferenceBar[] }
  // Appearance
  | { type: 'setShowMissingLevels'; on: boolean }
  | { type: 'setNotPlottedNote'; on: boolean }
  | { type: 'setYAxis'; patch: Partial<YAxis> }
  | { type: 'setStyle'; patch: StylePatch }
  | { type: 'setPanelOverrides'; overrides: PanelOverride[] }
  | { type: 'setAliases'; aliases: SpecAliases }
  | { type: 'setColors'; colors: SpecColors }

/** The spec after `action`; `null` (no spec loaded yet) stays `null`. */
export function plotSpecReducer<S extends PlotSpecFields>(spec: S | null, action: PlotSpecAction): S | null {
  if (!spec) return spec
  switch (action.type) {
    case 'setXMeasure':
      // With an x variable only scatter / line / regression draw, so the kind
      // follows (continuousKinds.kindForXMeasure).
      return { ...spec, x_measure: action.name, kind: kindForXMeasure(spec.kind, action.name) }
    case 'setTimeZone':
      return { ...spec, time_zone: action.mode }
    case 'setCategorical':
      return { ...spec, categorical: { ...(spec.categorical ?? {}), ...action.patch } }
    case 'setBarStack':
      return { ...spec, bar: { ...(spec.bar ?? {}), stack: action.stack } }
    case 'setHistogram':
      return { ...spec, histogram: { ...(spec.histogram ?? {}), ...action.patch } }
    case 'setKde':
      return { ...spec, kde: { ...(spec.kde ?? {}), ...action.patch } }
    case 'setRegression':
      return { ...spec, regression: { ...(spec.regression ?? {}), ...action.patch } }
    case 'setColor':
      return { ...spec, color: action.factor }
    case 'setFacet':
      return { ...spec, facet: { ...(spec.facet ?? {}), ...action.patch } }
    case 'setComparison':
      return { ...spec, comparison: action.comparison ?? null }
    case 'setCellStatistic':
      return { ...spec, cell_statistic: action.statistic }
    case 'setAggregate':
      return {
        ...spec,
        aggregate: { statistic: 'mean', error: 'sd', ...(spec.aggregate ?? {}), ...action.patch },
      }
    case 'setShowSample':
      return { ...spec, show_sample: toggleShowSample(spec.show_sample, action.name, action.on) }
    case 'setJoinSample':
      return { ...spec, join_sample: joinSetting(action.choice) }
    case 'setSampleColor':
      return { ...spec, sample_color: sampleColorSetting(action.choice) }
    case 'setSampleInLegend':
      return { ...spec, sample_in_legend: action.on }
    case 'setDifferenceBars':
      return { ...spec, difference_bars: action.bars }
    case 'setShowMissingLevels':
      return { ...spec, show_missing_levels: action.on }
    case 'setNotPlottedNote': {
      // Absent = off: the flag is removed rather than stored as false.
      const style = { ...(spec.style ?? {}) }
      if (action.on) style.not_plotted_note = true
      else delete style.not_plotted_note
      return { ...spec, style }
    }
    case 'setYAxis':
      return { ...spec, y_axis: { ...(spec.y_axis ?? {}), ...action.patch } }
    case 'setStyle':
      return { ...spec, style: { ...(spec.style ?? {}), ...action.patch } }
    case 'setPanelOverrides':
      return { ...spec, panel_overrides: action.overrides }
    case 'setAliases':
      return { ...spec, aliases: action.aliases }
    case 'setColors':
      return { ...spec, colors: action.colors }
  }
}
