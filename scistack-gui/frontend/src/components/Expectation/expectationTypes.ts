/**
 * Shapes of the expected-locations replies (scidb.provenance.expectation /
 * coverage, through scistack_gui.services.expectation_service). The words —
 * status labels, every row's "why", key descriptions — arrive from the
 * backend; this file only adds what is purely visual: one icon per status
 * (so the two reds and the two greens differ without colour, D8) and the
 * tone colours.
 */

export type Status =
  | 'present'
  | 'missing_unexpected'
  | 'present_unexpected'
  | 'missing_expected'
  | 'excluded'

export type Tone = 'ok' | 'problem' | 'excluded'

/** Display order of the statuses (problems first after present). */
export const STATUS_ORDER: Status[] = [
  'present',
  'missing_unexpected',
  'present_unexpected',
  'missing_expected',
  'excluded',
]

export const STATUS_ICON: Record<Status, string> = {
  present: '●',
  missing_expected: '◌',
  missing_unexpected: '✕',
  present_unexpected: '▲',
  excluded: '·',
}

export const TONE_COLOR: Record<Tone | 'empty', string> = {
  ok: '#4ade80',
  problem: '#f87171',
  excluded: '#888',
  empty: '#555',
}

export interface ExpectationMark {
  variable: string
  keys: Record<string, string>
  expected: boolean
  reason: string
  changed_at: string | null
}

export interface LastRun {
  function_name: string
  outcome: 'failed' | 'no_data'
  reason: string | null
  recorded_at: string | null
  description: string
}

export interface ExpectationRow {
  /** null = every value of that key ("(all)"). */
  values: Record<string, string | null>
  status: Status
  label: string
  tone: Tone
  expected: boolean
  present: boolean
  why: string
  source: string
  mark: ExpectationMark | null
  inherited_from: string | null
  exclusion_reason: string | null
  last_run: LastRun | null
}

export interface KeySetting {
  key: string
  sharing: 'shared' | 'per_parent'
  values: string[] | null
  rule: string
  stated_sharing: 'shared' | 'per_parent' | null
  declared_at: string | null
  description: string
}

export interface ExpectationResult {
  variable: string
  keys: string[]
  keys_rule: string
  columns: string[]
  settings: KeySetting[]
  rows: ExpectationRow[]
  total_rows: number
  truncated: boolean
  counts: Record<Status, number>
  tone_counts: Record<Tone, number>
  notes: string[]
  universe_size: number
  labels: Record<Status, string>
  all_marker: string
  marks: ExpectationMark[]
}

export interface FollowStep {
  variable: string
  keys: string[]
  ignored_keys: string[]
  fed_by: string[]
  has_data: boolean
  tone: Tone | 'empty'
  summary: string
}

export interface FollowResult {
  location: Record<string, string>
  steps: FollowStep[]
  lost_at: string[]
  skipped: Record<string, string>
}

export interface CoverageCell {
  counts: Record<Status, number>
  total: number
  present: number
  problems: number
  tone: Tone | 'empty'
}

export interface CoverageResult {
  by: string[]
  values: string[][]
  variables: string[]
  /** variable -> values joined by "|" -> cell */
  cells: Record<string, Record<string, CoverageCell>>
  funnel: { variable: string; with_data: number; expected: number }[]
  skipped: Record<string, string>
  labels: Record<Status, string>
}

export const keyText = (keys: Record<string, string | null>, allMarker = '(all)') =>
  Object.entries(keys).map(([k, v]) => `${k}=${v ?? allMarker}`).join(', ')

/** The keys a row names (wildcard cells left out): what a mark or a follow uses. */
export const concreteKeys = (values: Record<string, string | null>): Record<string, string> =>
  Object.fromEntries(Object.entries(values).filter(([, v]) => v != null)) as Record<string, string>
