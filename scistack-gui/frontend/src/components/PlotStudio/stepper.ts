/**
 * The ▲▼ arrows beside a numeric box (font sizes, mark weights, figure size).
 *
 * React-free so `npm test` can pin the rule. One click moves to the next
 * multiple of `step` in that direction, so 11.7 pt steps to 12 then 13, not
 * to 12.7. An empty box ("auto · 11.7") steps from `fallback`, the value it
 * is drawn at, never from 0. The result never goes below `min`.
 */

/** Decimals `step` needs: 0.1 → 1, 10 → 0. */
export function stepDecimals(step: number): number {
  const text = String(step)
  const dot = text.indexOf('.')
  return dot < 0 ? 0 : text.length - dot - 1
}

/** Round to `decimals` places without float noise (0.1 + 0.2 → 0.3). */
export function roundTo(value: number, decimals: number): number {
  const scale = 10 ** decimals
  return Math.round(value * scale) / scale
}

/**
 * The value one arrow click gives. `current` null (an empty box) steps from
 * `fallback`; with neither, from `min`. Returns null only when there is no
 * finite number to step from at all.
 */
export function stepValue(
  current: number | null,
  fallback: number | null,
  direction: 1 | -1,
  step: number,
  min: number,
): number | null {
  const base = current ?? fallback ?? min
  if (!Number.isFinite(base) || !(step > 0)) return null
  const decimals = stepDecimals(step)
  // Relative to the grid of multiples of `step`. The epsilon keeps a value
  // already on the grid (12.0000001 after a conversion) from counting as
  // off it and stepping twice.
  const units = base / step
  const eps = 1e-6
  const next =
    direction > 0 ? (Math.floor(units + eps) + 1) * step : (Math.ceil(units - eps) - 1) * step
  return roundTo(Math.max(min, next), decimals)
}
