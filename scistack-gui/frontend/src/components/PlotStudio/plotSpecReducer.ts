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

import type {
  Aggregation,
  CategoricalOptions,
  FacetOptions,
  MatchOp,
  Matcher,
  PlotSpec,
  Statistic,
  StyleOptions,
  YAxis,
} from '../../generated/api-types.js'

// The spec's types are GENERATED from the backend's wire models
// (scistack_gui.api.spec_wire, the shape of PlotSpec.to_dict()).
export type { FacetOptions, MatchOp, Matcher, YAxis }

/** The `StyleOptions` fields this reducer's Appearance actions edit. The
 *  tick settings are null for "fitted" (scistackplot.fit_labels) and a
 *  value for "fixed". */
export type StylePatch = Partial<
  Pick<
    StyleOptions,
    'width' | 'height' | 'tick_rotation' | 'tick_every' | 'hide_legend_ticks' | 'not_plotted_note' | 'y_titles'
  >
>

/** The spec this reducer edits: the generated `PlotSpec`. */
export type PlotSpecFields = PlotSpec

/** One spec edit. Grouped by the rail group that sends it. */
export type PlotSpecAction =
  // Data
  | { type: 'setXMeasure'; name: string | null }
  | { type: 'setTimeZone'; mode: string }
  // Chart
  | { type: 'setCategorical'; patch: CategoricalOptions }
  | { type: 'setBarStack'; stack: boolean }
  | { type: 'setHistogram'; patch: HistogramOptions }
  | { type: 'setKde'; patch: KdeOptions }
  | { type: 'setRegression'; patch: RegressionOptions }
  // Structure
  | { type: 'setColor'; factor: string | null }
  | { type: 'setFacet'; patch: Partial<FacetOptions> }
  | { type: 'setComparison'; comparison: Comparison | null | undefined }
  // Statistics
  | { type: 'setCellStatistic'; statistic: Statistic }
  | { type: 'setAggregate'; patch: Aggregation }
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
