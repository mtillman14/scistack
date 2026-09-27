/**
 * Structure > Compare: edits `spec.comparison` (difference / % change from one
 * level of a grouping layer). Pure, so `npm test` runs it.
 *
 * The panel displays and never decides: which layers and levels exist, the
 * default a fresh comparison opens with, whether it is paired and why, and
 * what was dropped all come from Python (`capabilities.comparison`,
 * `layout.meta.comparison`; `scistackplot.compare` is the one owner). Plan:
 * .claude/plan-compare-to-reference.md.
 */

export type CompareMode = 'difference' | 'percent'

export interface Comparison {
  layer: string
  level: string
  mode: CompareMode
  active?: boolean
}

/** `capabilities.comparison` (`compare.comparison_summary`). */
export interface CompareCapability {
  available: boolean
  reason: string | null
  layers: { name: string; levels: string[] }[]
  default_layer: string | null
  default_level: string | null
  modes: string[]
  state: {
    set: boolean
    active?: boolean
    inert?: string | null
    paired?: boolean | null
    reason?: string | null
    description?: string | null
  }
}

/** `layout.meta.comparison` (`compare.comparison_meta`): the figure as drawn. */
export interface CompareMeta {
  layer: string
  level: string
  mode: string
  paired: boolean
  reason: string
  description: string
  outcome: { rows_in: number; rows_out: number; dropped: Record<string, string[]> } | null
}

/** The Mode dropdown's value: `off` when no comparison is set or it is off. */
export type ModeChoice = 'off' | CompareMode

export const MODE_LABELS: Record<ModeChoice, string> = {
  off: 'Off',
  difference: 'Difference',
  percent: '% change',
}

export function modeChoice(comparison: Comparison | null | undefined): ModeChoice {
  if (!comparison || comparison.active === false) return 'off'
  return comparison.mode
}

/**
 * The next `spec.comparison` for a Mode pick. Off keeps the layer and level
 * (`active: false`), so turning it back on restores them. On, from nothing,
 * opens at the capability's default layer and level; null when the figure
 * offers none (nothing to compare along).
 */
export function withMode(
  comparison: Comparison | null | undefined,
  choice: ModeChoice,
  capability: CompareCapability | undefined,
): Comparison | null | undefined {
  if (choice === 'off') {
    return comparison ? { ...comparison, active: false } : comparison
  }
  if (comparison) return { ...comparison, mode: choice, active: true }
  const layer = capability?.default_layer
  const level = capability?.default_level
  if (!layer || level == null) return null
  return { layer, level, mode: choice, active: true }
}

/** A new layer: its first level becomes the reference (the old level named
 *  something on the other layer). */
export function withLayer(
  comparison: Comparison,
  layer: string,
  capability: CompareCapability | undefined,
): Comparison {
  const levels = levelsOf(capability, layer)
  return { ...comparison, layer, level: levels[0] ?? comparison.level }
}

export function withLevel(comparison: Comparison, level: string): Comparison {
  return { ...comparison, level }
}

export function levelsOf(capability: CompareCapability | undefined, layer: string): string[] {
  return capability?.layers.find(entry => entry.name === layer)?.levels ?? []
}

/** The layer choices: the capability's grouping layers, plus the current one
 *  when it no longer groups (kept visible so the user can see what to undo). */
export function layerChoices(
  comparison: Comparison | null | undefined,
  capability: CompareCapability | undefined,
): string[] {
  const names = (capability?.layers ?? []).map(entry => entry.name)
  if (comparison && !names.includes(comparison.layer)) names.push(comparison.layer)
  return names
}

/** One line per drop reason: `no reference value: subject=03, subject=07`. */
export function droppedLines(meta: CompareMeta | null | undefined, limit = 5): string[] {
  const dropped = meta?.outcome?.dropped ?? {}
  return Object.entries(dropped).map(([reason, units]) => {
    const shown = units.slice(0, limit).join('; ')
    const more = units.length > limit ? ` +${units.length - limit} more` : ''
    return `Dropped ${units.length} (${reason}): ${shown}${more}`
  })
}

/** The Structure group's summary fragment: `vs session s1 (%)`, or ''. */
export function compareSummary(comparison: Comparison | null | undefined): string {
  if (!comparison || comparison.active === false) return ''
  return `vs ${comparison.layer} ${comparison.level}${comparison.mode === 'percent' ? ' (%)' : ' (Δ)'}`
}
