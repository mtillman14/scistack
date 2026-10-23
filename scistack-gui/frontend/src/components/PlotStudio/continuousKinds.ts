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

export type BinMode = 'auto' | 'count' | 'width'
export type HistStat = 'count' | 'percent' | 'density'
export type HistMultiple = 'layer' | 'stack' | 'dodge'
export type FitBy = 'color' | 'pooled'
export type StatsCorner = 'upper_left' | 'upper_right' | 'lower_left' | 'lower_right'

/** `PlotSpec.histogram` (scistackplot.spec.HistogramOptions). */
export interface HistogramOptions {
  bins?: BinMode
  bin_count?: number | null
  /** A number, or a duration text ("1h", "7D") on a datetime axis. */
  bin_width?: number | string | null
  range_min?: number | string | null
  range_max?: number | string | null
  stat?: HistStat
  multiple?: HistMultiple
}

/** `PlotSpec.regression` (scistackplot.spec.RegressionOptions). */
export interface RegressionOptions {
  fit_line?: boolean
  show_stats?: boolean
  ci_band?: boolean
  fit_by?: FitBy
  stats_corner?: StatsCorner
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
export const X_MEASURE_KINDS = ['scatter', 'line', 'regression']

/** What picking (or clearing) the X variable does to the kind. With an X
 *  variable only scatter, line and regression draw, so any other kind becomes
 *  a regression (the reason to pick one). Cleared, the x-only kinds become a
 *  scatter. The backend then validates the result as usual. */
export function kindForXMeasure(kind: string, xMeasure: string | null): string {
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
  if (meta.n_bins === 0) return 'No values to bin.'
  const width =
    meta.width === null ? '' : ` of ${temporal ? durationText(meta.width) : formatNumber(meta.width)}`
  return `${meta.n_bins} bin${meta.n_bins === 1 ? '' : 's'}${width}, from ${meta.n_values} value${
    meta.n_values === 1 ? '' : 's'
  }`
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
