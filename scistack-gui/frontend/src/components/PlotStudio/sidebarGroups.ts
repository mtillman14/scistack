/**
 * The Plot Studio controls rail, grouped in the order a figure is built:
 * DATA → CHART → STRUCTURE → STATISTICS → APPEARANCE
 * (docs/claude/plot-studio-controls.md).
 *
 * React-free, so `npm test` can run it. Three things live here:
 * - which groups exist and which start open, and the tolerant reader for the
 *   per-viewer open state (localStorage can hold anything, or nothing);
 * - the rail's width class, which the responsive rows read (the rail may
 *   become user-resizable; nothing may assume 260 px);
 * - the one-line summary each collapsed group shows. Summaries only DISPLAY
 *   the spec; they never decide anything the backend decides.
 */

export type GroupId = 'data' | 'chart' | 'structure' | 'statistics' | 'appearance'

export const GROUP_ORDER: GroupId[] = ['data', 'chart', 'structure', 'statistics', 'appearance']

/** Appearance is long and visited last, so it starts closed. */
export const DEFAULT_OPEN: Record<GroupId, boolean> = {
  data: true,
  chart: true,
  structure: true,
  statistics: true,
  appearance: false,
}

export const OPEN_GROUPS_STORAGE_KEY = 'scistack.plotStudio.openGroups'

/** Stored open state, falling back per group to the default for anything unreadable. */
export function parseOpenGroups(raw: string | null | undefined): Record<GroupId, boolean> {
  const open = { ...DEFAULT_OPEN }
  if (!raw) return open
  let parsed: unknown
  try {
    parsed = JSON.parse(raw)
  } catch {
    return open
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return open
  for (const id of GROUP_ORDER) {
    const value = (parsed as Record<string, unknown>)[id]
    if (typeof value === 'boolean') open[id] = value
  }
  return open
}

export function serializeOpenGroups(open: Record<GroupId, boolean>): string {
  const out: Record<string, boolean> = {}
  for (const id of GROUP_ORDER) out[id] = open[id]
  return JSON.stringify(out)
}

// --- rail width ---------------------------------------------------------------

export type RailWidth = 'narrow' | 'normal' | 'wide'

/** Width of the controls column (content box, px) → the layout its rows use. */
export function railWidthClass(px: number): RailWidth {
  if (px < 300) return 'narrow'
  if (px < 420) return 'normal'
  return 'wide'
}

/** Text-size boxes per row: one when narrow, where two overflowed. */
export function textSizeColumns(width: RailWidth): number {
  return width === 'narrow' ? 1 : 2
}

// --- summaries ----------------------------------------------------------------

const SEP = ' · '

export const SPREAD_LABELS: Record<string, string> = {
  sd: 'SD',
  sem: 'SEM',
  ci95: '95% CI',
  iqr: 'IQR',
  none: 'none',
}

function plural(n: number, word: string): string {
  return `${n} ${word}${n === 1 ? '' : 's'}`
}

export function dataSummary(args: {
  variantCount: number
  /** The location picker's own summary, or null when there are no schema keys. */
  locations: string | null
  filterCount: number
}): string {
  const parts: string[] = []
  if (args.variantCount > 1) parts.push(plural(args.variantCount, 'variant'))
  if (args.locations) parts.push(args.locations)
  if (args.filterCount > 0) parts.push(plural(args.filterCount, 'filter'))
  return parts.join(SEP) || 'All data'
}

export function chartSummary(args: {
  kindLabel: string | null
  /** The per-record statistic when a 1-D measure is being reduced, else null. */
  perRecord: string | null
}): string {
  const parts: string[] = [args.kindLabel ?? 'No plot type']
  if (args.perRecord) parts.push(`per-record ${args.perRecord}`)
  return parts.join(SEP)
}

const ROLE_WORDS: [string, string][] = [
  ['facet', 'subplots'],
  ['iterate', 'figures'],
  ['collapse', 'collapse'],
]

export function structureSummary(args: {
  /** Grouping layers, innermost first — as the GroupingList shows them. */
  grouped: string[]
  color: string | null
  roles: Record<string, string>
}): string {
  const parts: string[] = []
  if (args.grouped.length) parts.push(`group ${args.grouped.join(' › ')}`)
  if (args.color) parts.push(`colour ${args.color}`)
  for (const [role, word] of ROLE_WORDS) {
    const names = Object.entries(args.roles)
      .filter(([, r]) => r === role)
      .map(([name]) => name)
    if (names.length) parts.push(`${word} ${names.join(', ')}`)
  }
  return parts.join(SEP) || 'Ungrouped'
}

export function statisticsSummary(args: {
  summarizing: boolean
  centre: string
  spread: string
  pooled: boolean
  /** Keys the sample overlay shows. */
  shown: string[]
}): string {
  const parts: string[] = []
  if (args.summarizing) {
    const spread = SPREAD_LABELS[args.spread] ?? args.spread
    parts.push(args.spread === 'none' ? args.centre : `${args.centre} ± ${spread}`)
    if (args.pooled) parts.push('weighted by N')
  }
  if (args.shown.length) parts.push(`sample ${args.shown.join(', ')}`)
  return parts.join(SEP) || 'Raw values'
}

function num(value: number): string {
  return String(Number(value.toPrecision(4)))
}

export function appearanceSummary(args: {
  width: number
  height: number
  font: number
  yMin: number | null
  yMax: number | null
}): string {
  const y =
    args.yMin === null && args.yMax === null
      ? 'y auto'
      : `y ${args.yMin === null ? 'auto' : num(args.yMin)}–${args.yMax === null ? 'auto' : num(args.yMax)}`
  return [`${num(args.width)}×${num(args.height)} in`, `${num(args.font)} pt`, y].join(SEP)
}
