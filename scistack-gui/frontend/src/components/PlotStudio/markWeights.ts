/**
 * Mark weights — the panel's half of `StyleOptions.sample_weight` (the
 * "Show sample" points + lines) and `StyleOptions.line_weight` (a
 * spaghetti's own points + lines). Multipliers: 1 = the default look.
 *
 * Python owns the sizes and when a knob applies (`scistackplot.weights`,
 * published as `layout.meta.mark_weights`); nothing here computes a size or
 * tests the plot kind. React-free so `npm test` can pin the edit rule.
 */

export type WeightKey = 'sample_weight' | 'line_weight'

/** One entry of `layout.meta.mark_weights` (`ResolvedWeight.to_dict()`). */
export interface MarkWeight {
  applies: boolean
  weight: number
  marker_pt: number
  line_pt: number
}

export interface MarkWeightsMeta {
  sample?: MarkWeight
  lines?: MarkWeight
}

/**
 * `style` with `key` set to `value`. Cleared, invalid or exactly 1 DELETES
 * the key: `PlotSpec.to_dict` would store the default anyway, and a stored
 * default would make a reopened saved plot read as "● modified".
 */
export function withWeight<T extends Record<string, unknown>>(
  style: T | undefined,
  key: WeightKey,
  value: number | null,
): T {
  const next = { ...(style ?? {}) } as Record<string, unknown>
  if (value === null || !Number.isFinite(value) || value <= 0 || value === 1) delete next[key]
  else next[key] = value
  return next as T
}

/** The box's tooltip: what the weight resolves to, in Python's numbers. */
export function weightTitle(resolved: MarkWeight | undefined, what: string): string {
  const head = `Multiplier on the ${what}' point size and line thickness together (1 = default).`
  if (!resolved) return head
  return `${head} Now: points ${resolved.marker_pt.toFixed(1)} pt, lines ${resolved.line_pt.toFixed(2)} pt.`
}
