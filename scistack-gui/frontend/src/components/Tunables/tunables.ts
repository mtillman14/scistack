/**
 * Tunables of a placed library pipeline — the pure half (no React, no DOM),
 * so `npm test` runs it (.claude/plan-library-fork-chain.md E1-E5, O26).
 *
 * The backend owns the values (`services/library_service.tunables`: the
 * project's Parameters, the placement's binding); this module only shapes
 * what the panel shows and parses what the user types.
 */

export interface TunableParameter {
  name: string
  /** The project's value(s); null when the project does not declare it. */
  value: unknown[] | null
  source: 'project' | 'library default' | 'missing'
}

export interface TunablePathInput {
  name: string
  template: string | null
  root_folder: string | null
  needs_root: boolean
}

export interface StepValue {
  /** `"<fn label>.<input>"`, the binding key. */
  key: string
  value: unknown
  default: unknown
  /** The project Parameter this step value follows, if promoted (E5). */
  parameter: string | null
}

export interface Tunables {
  use_id: string
  pipeline_id: string
  library: string
  parameters: TunableParameter[]
  path_inputs: TunablePathInput[]
  step_values: StepValue[]
  key_map: Record<string, string>
  /** Project view only (E2): pipeline names from the top. */
  path?: string[]
}

/** A value as the panel shows it. */
export function formatValue(value: unknown): string {
  if (value === null || value === undefined) return '—'
  if (Array.isArray(value)) return value.map(formatValue).join(', ')
  if (typeof value === 'string') return value
  return JSON.stringify(value)
}

/**
 * What the user typed, as the value to store. Keeps the TYPE of the value
 * it replaces when it can (a number stays a number: `2` for a float default
 * is 2, not "2"); JSON otherwise; plain text last.
 */
export function parseStepValue(text: string, previous: unknown): unknown {
  const t = text.trim()
  if (typeof previous === 'number' || /^-?\d+(\.\d+)?([eE][-+]?\d+)?$/.test(t)) {
    const n = Number(t)
    if (t !== '' && Number.isFinite(n)) return n
  }
  if (typeof previous === 'boolean' || t === 'true' || t === 'false') {
    if (t === 'true') return true
    if (t === 'false') return false
  }
  try {
    return JSON.parse(t)
  } catch {
    return text
  }
}

/** The Parameter name "Make this a Parameter" suggests: the input, in caps. */
export function suggestParameterName(key: string): string {
  const input = key.split('.').pop() ?? key
  return input.replace(/[^A-Za-z0-9_]/g, '_').toUpperCase()
}

/** Whether a step value differs from the library's default. */
export function isChanged(sv: StepValue): boolean {
  return sv.parameter !== null || JSON.stringify(sv.value) !== JSON.stringify(sv.default)
}

/** One-line summary for a collapsed panel. */
export function tunableSummary(t: Tunables): string {
  const parts: string[] = []
  if (t.parameters.length) parts.push(`${t.parameters.length} Parameter(s)`)
  const roots = t.path_inputs.filter(p => p.needs_root).length
  if (t.path_inputs.length) {
    parts.push(`${t.path_inputs.length} PathInput(s)${roots ? `, ${roots} need a root folder` : ''}`)
  }
  const changed = t.step_values.filter(isChanged).length
  if (t.step_values.length) {
    parts.push(`${t.step_values.length} step value(s)${changed ? `, ${changed} changed` : ''}`)
  }
  return parts.join(' · ') || 'nothing to tune'
}
