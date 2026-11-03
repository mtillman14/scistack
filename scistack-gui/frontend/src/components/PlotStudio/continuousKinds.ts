/**
 * Histogram and regression settings: the pure logic behind Chart > Histogram,
 * Chart > Regression and Data > X variable.
 *
 * Every RULE lives in Python: which kinds an X variable allows
 * (`capability.available_plots`), whether bin settings are valid
 * (`bins.histogram_problem`, shipped as `capabilities.histogram.problem`),
 * the bins and fits as drawn (`layout.meta.bins` / `layout.meta.fits`). This
 * module only builds spec patches and reads those reports back as text.
 */

// The settings' types are GENERATED from the backend's wire models
// (scistack_gui.api.spec_wire): the enums and option shapes are Python's.
import type {
  BinMode, FitBy, HistMultiple, HistStat, HistogramOptions, KdeOptions, PlotKind, RegressionOptions,
  StatsCorner,
} from '../../generated/api-types.js'

export type {
  BinMode, FitBy, HistMultiple, HistStat, HistogramOptions, KdeOptions, RegressionOptions, StatsCorner,
}

/** `capabilities.histogram` (capability.histogram_summary). */
export interface HistogramCapability {
  applies: boolean
  temporal: boolean
  settings: HistogramOptions
  problem: string | null
  max_bins: number
}

/** `capabilities.regression` (capability.regression_summary). */
export interface RegressionCapability {
  applies: boolean
  temporal_x: boolean
  settings: RegressionOptions
  problem: string | null
}

/** `layout.meta.bins` (bins.BinEdges.to_dict). */
export interface BinsMeta {
  mode: BinMode
  edges: number[]
  width: number | null
  n_bins: number
  n_values: number
  notes: string[]
  /** Single-value bins in effect, the NaN slot drawn, the figure's NaN
   *  count (drawn or not), and the special slots' labels left to right. */
  exact?: number[]
  nan?: boolean
  n_nan?: number
  special?: string[]
}

/** One entry of `layout.meta.fits` (fits.PanelFit.to_dict, per panel). */
export interface FitMeta {
  level: string | null
  n: number
  slope: number | null
  intercept: number | null
  r_squared: number | null
  p_value: number | null
  reason: string | null
  text: string
}

export interface PanelFitsMeta {
  panel: string
  fits: FitMeta[]
}

/** The kinds an X variable allows (capability.available_plots). */
export const X_MEASURE_KINDS: readonly PlotKind[] = ['scatter', 'line', 'regression']

/** What picking (or clearing) the X variable does to the kind. With an X
 *  variable only scatter, line and regression draw, so any other kind becomes
 *  a regression (the reason to pick one). Cleared, the x-only kinds become a
 *  scatter. The backend then validates the result as usual. An absent kind
 *  is PlotSpec's default, a scatter. */
export function kindForXMeasure(kind: PlotKind = 'scatter', xMeasure: string | null): PlotKind {
  if (xMeasure) return X_MEASURE_KINDS.includes(kind) ? kind : 'regression'
  return kind === 'regression' || kind === 'line' ? 'scatter' : kind
}

/** A bin count as typed: a positive whole number, or null to use the default. */
export function parseBinCount(text: string): number | null {
  const trimmed = text.trim()
  if (!trimmed) return null
  const value = Number(trimmed)
  return Number.isInteger(value) && value > 0 ? value : null
}

/** A bin width or range end as typed: a number on a numeric axis, the text
 *  itself on a datetime one (Python parses and validates it), null when blank. */
export function parseBinValue(text: string, temporal: boolean): number | string | null {
  const trimmed = text.trim()
  if (!trimmed) return null
  if (temporal) return trimmed
  const value = Number(trimmed)
  return Number.isFinite(value) ? value : trimmed
}

/** A stored bin setting back as box text. */
export function binText(value: number | string | null | undefined): string {
  return value === null || value === undefined ? '' : String(value)
}

/** "12 bins of 0.5, from 40 values" — what the drawn figure binned. */
export function binsSummary(meta: BinsMeta | null | undefined, temporal: boolean): string {
  if (!meta) return ''
  const special = meta.special ?? []
  const own = special.length ? `; own bars: ${special.join(', ')}` : ''
  const nan = meta.n_nan && !meta.nan ? `; ${meta.n_nan} NaN not drawn` : ''
  if (meta.n_bins === 0) return special.length ? `No other values to bin${own}` : 'No values to bin.'
  const width =
    meta.width === null ? '' : ` of ${temporal ? durationText(meta.width) : formatNumber(meta.width)}`
  return `${meta.n_bins} bin${meta.n_bins === 1 ? '' : 's'}${width}, from ${meta.n_values} value${
    meta.n_values === 1 ? '' : 's'
  }` + own + nan
}

/** A duration in ms as the shortest exact unit: "1h", "7D", "90min". */
export function durationText(ms: number): string {
  const units: [number, string][] = [
    [86_400_000, 'D'],
    [3_600_000, 'h'],
    [60_000, 'min'],
    [1000, 's'],
  ]
  for (const [size, unit] of units) {
    if (ms >= size && Math.abs(ms / size - Math.round(ms / size)) < 1e-9) {
      return `${Math.round(ms / size)}${unit}`
    }
  }
  return `${formatNumber(ms)}ms`
}

function formatNumber(value: number): string {
  return Number.isInteger(value) ? String(value) : String(Number(value.toPrecision(4)))
}

/** One line per fit for the Regression readout, panel-prefixed when the
 *  figure has several panels; a fit that could not be made says why. */
export function fitsSummary(panels: PanelFitsMeta[] | null | undefined): string[] {
  if (!panels || panels.length === 0) return []
  const many = panels.length > 1
  const lines: string[] = []
  for (const panel of panels) {
    for (const fit of panel.fits) {
      const where = [many ? panel.panel : '', fit.level ?? ''].filter(Boolean).join(' · ')
      const body = fit.reason ? `no line (${fit.reason})` : fit.text.replace(/^[^:]*: /, '')
      lines.push(where ? `${where}: ${body}` : body)
    }
  }
  return lines
}

/** The single-value bins box's text: the list Python kept (`exact_values`,
 *  spec._as_exact_values) or, before the next report, what was typed. */
export function exactValuesText(value: HistogramOptions['exact_values']): string {
  if (Array.isArray(value)) return value.map(formatNumber).join(', ')
  return value ?? ''
}

/** `capabilities.kde` (capability.kde_summary). */
export interface KdeCapability {
  applies: boolean
  settings: KdeOptions
  problem: string | null
}

/** A KDE number box as typed: a number, or null for the default. Python
 *  validates it (`kde.kde_problem`, shipped as `capabilities.kde.problem`). */
export function parseKdeNumber(text: string): number | null {
  const trimmed = text.trim()
  if (!trimmed) return null
  const value = Number(trimmed)
  return Number.isFinite(value) ? value : null
}
