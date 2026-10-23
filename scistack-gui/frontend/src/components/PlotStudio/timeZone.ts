/**
 * Data > Time zone: how datetimes holding several zones share the figure
 * (`PlotSpec.time_zone`, owner `scistackplot.temporal`; plan
 * .claude/plan-plot-datetimes.md). Pure logic, tested in timeZone.test.ts.
 *
 * "local" puts each value at its own wall clock (the default); a zone label
 * ("UTC", "Europe/Berlin", "+02:00") converts every aware value to it. The
 * backend validates the zone and says why not (`capabilities.time_zone.problem`).
 */

/** `capabilities.time_zone` (`temporal.time_zone_summary`). */
export interface TimeZoneCapability {
  /** Whether a datetime is in play: the plotted measure or any factor. */
  applies: boolean
  measure: boolean
  factors: string[]
  mode: string
  /** The zones the data holds, offered first. */
  zones: string[]
  /** Why the stored mode cannot be used (an unknown zone), or null. */
  problem: string | null
}

/** `layout.meta.temporal` (`temporal.TemporalAxis.to_dict`). */
export interface TemporalMeta {
  mode: string
  strftime: string
  d3: string
  notes: string[]
}

export const LOCAL = 'local'
export const OTHER = '__other__'

export interface ZoneOption {
  value: string
  label: string
}

/** The dropdown: local time, the data's own zones, UTC, then "Other zone…".
 *  A typed zone that is none of those stays listed, so the control never
 *  shows a different choice from the spec's. */
export function timeZoneOptions(
  capability: TimeZoneCapability | undefined,
  current: string | undefined,
): ZoneOption[] {
  const zones = [...(capability?.zones ?? [])]
  if (!zones.includes('UTC')) zones.push('UTC')
  const mode = normalizeMode(current)
  if (mode !== LOCAL && !zones.includes(mode)) zones.push(mode)
  return [
    { value: LOCAL, label: 'Local time (each value’s own)' },
    ...zones.map(zone => ({ value: zone, label: `Convert to ${zone}` })),
    { value: OTHER, label: 'Other zone…' },
  ]
}

/** The spec's mode as stored: blank or any spelling of "local" is local. */
export function normalizeMode(mode: string | null | undefined): string {
  const text = (mode ?? '').trim()
  return !text || text.toLowerCase() === LOCAL ? LOCAL : text
}

/** What a typed "Other zone" commits: the trimmed text, or null to keep the
 *  current mode (blank). The backend decides whether the zone exists. */
export function typedZone(text: string): string | null {
  const zone = text.trim()
  return zone ? zone : null
}

/** A one-line summary for the Data group header. */
export function timeZoneSummary(capability: TimeZoneCapability | undefined): string | null {
  if (!capability?.applies) return null
  const mode = normalizeMode(capability.mode)
  return mode === LOCAL ? 'local time' : mode
}

// ---------------------------------------------------------------------------
// Y limits on a datetime axis
// ---------------------------------------------------------------------------

const DATE_TEXT = /^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2}(\.\d+)?)?)?$/

/** A typed y limit: a number, or (on a datetime axis) ISO date text, which the
 *  backend reads as a wall clock in the axis's zone (`temporal.parse_limit`).
 *  `null` = blank (compute it); `undefined` = not a value yet (keep typing). */
export function parseLimitText(text: string, temporal: boolean): number | string | null | undefined {
  const trimmed = text.trim()
  if (trimmed === '') return null
  if (temporal) return DATE_TEXT.test(trimmed) ? trimmed.replace('T', ' ') : undefined
  const n = Number(trimmed)
  return Number.isFinite(n) ? n : undefined
}

/** An applied limit as the panel says it: a datetime axis's wall-clock ms as
 *  `YYYY-MM-DD HH:MM`, a number to 3 significant digits. */
export function formatLimit(value: number, temporal: boolean): string {
  if (!temporal) return value.toPrecision(3)
  if (!Number.isFinite(value)) return ''
  // Wall-clock ms read as UTC is the wall clock itself (no zone applied).
  return new Date(value).toISOString().slice(0, 16).replace('T', ' ')
}
