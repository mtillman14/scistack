/**
 * StatStudio's spec edits — pure, React- and DOM-free (node --test).
 *
 * The spec is the backend's `scistackstats.StatsSpec.to_dict()` and travels
 * unchanged. These helpers only return edited copies. Every choice
 * (statistics, cells, tests, effect sizes, corrections) comes from the
 * backend's `catalog`, and every number and caption from its `Grid`s, so
 * nothing about statistics is decided here.
 */

export interface DV {
  variable: string
  columns?: string[]
}

export interface Analysis {
  id: string
  family: string
  omnibus?: boolean
  pairwise?: boolean
  omnibus_test?: string
  pairwise_test?: string
  equal_var?: boolean
  alpha?: number
  ci_level?: number
  tails?: string
  correction?: string
  family_scope?: string
  effect_size?: string
  difference_spread?: string
  overrides?: Record<string, string>
  /** "use_all" (default) or "complete_cases": recorded and stated in every caption. */
  missing?: string
}

export interface Formatting {
  decimals?: number
  p_decimals?: number
  effect_decimals?: number
  stars?: boolean
}

export interface Overlay {
  show?: boolean
  analysis?: string
  pairs?: string
  label?: string
  hidden?: string[]
}

export interface StatsSpec {
  format: number
  design: Record<string, unknown> & { measures?: string[]; roles?: Record<string, string>; groups?: string[] }
  dvs: DV[]
  linked_plot?: string
  descriptives: { cell: string; statistics: string[] }
  comparisons: Analysis[]
  variant_mode: string
  formatting?: Formatting
  overlay?: Overlay
}

export interface GridCell { text: string; span: number }
export interface GridRow { cells: string[]; kind: string }
export interface Grid {
  title: string
  header_rows: GridCell[][]
  rows: GridRow[]
  caption: string
  notes: string[]
  label_columns: number
}

// --- DVs ---------------------------------------------------------------------

/** The DVs a spec analyses: its own list, or the design's plotted variable. */
export function targets(spec: StatsSpec): DV[] {
  if (spec.dvs.length) return spec.dvs
  const first = spec.design.measures?.[0]
  return first ? [{ variable: first }] : []
}

export function addDV(spec: StatsSpec, variable: string, columns?: string[]): StatsSpec {
  const dv: DV = columns && columns.length ? { variable, columns: [...columns] } : { variable }
  const same = (d: DV) => d.variable === dv.variable &&
    JSON.stringify(d.columns ?? null) === JSON.stringify(dv.columns ?? null)
  const current = targets(spec)
  if (current.some(same)) return spec
  return { ...spec, dvs: [...current, dv] }
}

export function removeDV(spec: StatsSpec, index: number): StatsSpec {
  const current = targets(spec)
  if (current.length <= 1) return spec // a table needs one measure
  return { ...spec, dvs: current.filter((_, i) => i !== index) }
}

// --- design (roles and grouping order) ---------------------------------------

/** Set a factor's role in the design carrier. Grouping order keeps GROUP holders only. */
export function setRole(spec: StatsSpec, factor: string, role: string): StatsSpec {
  const roles = { ...(spec.design.roles ?? {}), [factor]: role }
  let groups = (spec.design.groups ?? []).filter(g => roles[g] === 'group')
  if (role === 'group' && !groups.includes(factor)) groups = [...groups, factor]
  return { ...spec, design: { ...spec.design, roles, groups } }
}

/**
 * Move a grouping layer one step OUTWARD (`delta` = +1) or inward (−1).
 * `groups` is innermost first, as the plot layer stores it.
 */
export function moveGroup(spec: StatsSpec, factor: string, delta: 1 | -1): StatsSpec {
  const groups = [...(spec.design.groups ?? [])]
  const i = groups.indexOf(factor)
  const j = i + delta
  if (i < 0 || j < 0 || j >= groups.length) return spec
  ;[groups[i], groups[j]] = [groups[j], groups[i]]
  return { ...spec, design: { ...spec.design, groups } }
}

// --- descriptives ------------------------------------------------------------

export function setCell(spec: StatsSpec, cell: string): StatsSpec {
  return { ...spec, descriptives: { ...spec.descriptives, cell } }
}

/** Toggle a statistic row, keeping the catalog's order (`order`). */
export function toggleStatistic(spec: StatsSpec, id: string, order: string[]): StatsSpec {
  const on = new Set(spec.descriptives.statistics)
  if (on.has(id)) on.delete(id)
  else on.add(id)
  return { ...spec, descriptives: { ...spec.descriptives, statistics: order.filter(s => on.has(s)) } }
}

// --- analyses ----------------------------------------------------------------

export function nextAnalysisId(spec: StatsSpec): string {
  const taken = new Set(spec.comparisons.map(a => a.id))
  let n = spec.comparisons.length + 1
  while (taken.has(`a${n}`)) n += 1
  return `a${n}`
}

export function addAnalysis(spec: StatsSpec, family = 'auto'): StatsSpec {
  return { ...spec, comparisons: [...spec.comparisons, { id: nextAnalysisId(spec), family }] }
}

export function removeAnalysis(spec: StatsSpec, id: string): StatsSpec {
  return { ...spec, comparisons: spec.comparisons.filter(a => a.id !== id) }
}

/** Patch one analysis. A field set to `undefined` is removed (the backend default). */
export function updateAnalysis(spec: StatsSpec, id: string, patch: Partial<Analysis>): StatsSpec {
  return {
    ...spec,
    comparisons: spec.comparisons.map(a => {
      if (a.id !== id) return a
      const next: Record<string, unknown> = { ...a, ...patch }
      for (const [k, v] of Object.entries(patch)) if (v === undefined) delete next[k]
      return next as unknown as Analysis
    }),
  }
}

/** Override a factor's within/between kind, or clear it with `null`. */
export function setOverride(spec: StatsSpec, id: string, factor: string, kind: string | null): StatsSpec {
  const analysis = spec.comparisons.find(a => a.id === id)
  if (!analysis) return spec
  const overrides = { ...(analysis.overrides ?? {}) }
  if (kind) overrides[factor] = kind
  else delete overrides[factor]
  return updateAnalysis(spec, id, { overrides })
}

// --- formatting and brackets ---------------------------------------------------

export function setFormatting(spec: StatsSpec, patch: Partial<Formatting>): StatsSpec {
  const next: Record<string, unknown> = { ...(spec.formatting ?? {}), ...patch }
  for (const [k, v] of Object.entries(patch)) if (v === undefined) delete next[k]
  return { ...spec, formatting: next as Formatting }
}

export function setOverlay(spec: StatsSpec, patch: Partial<Overlay>): StatsSpec {
  return { ...spec, overlay: { ...(spec.overlay ?? {}), ...patch } }
}

/** Hide (or show again) one comparison's bracket by its comparison id. */
export function toggleHiddenBracket(spec: StatsSpec, comparisonId: string): StatsSpec {
  const hidden = new Set(spec.overlay?.hidden ?? [])
  if (hidden.has(comparisonId)) hidden.delete(comparisonId)
  else hidden.add(comparisonId)
  return setOverlay(spec, { hidden: [...hidden] })
}

// --- grids -----------------------------------------------------------------------

/** Total columns of a grid (from its last header row). */
export function gridWidth(grid: Grid): number {
  const last = grid.header_rows[grid.header_rows.length - 1] ?? []
  return last.reduce((n, cell) => n + cell.span, 0)
}

/** A grid as tab-separated text, for "Copy table" into a spreadsheet. */
export function gridToTsv(grid: Grid): string {
  const header = grid.header_rows.map(row =>
    row.flatMap(cell => [cell.text, ...Array(Math.max(0, cell.span - 1)).fill('')]).join('\t')
  )
  const body = grid.rows.map(row => row.cells.join('\t'))
  return [grid.title, ...header, ...body, '', `Note. ${grid.caption}`, ...grid.notes].join('\n')
}
