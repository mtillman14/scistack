/**
 * The run report's wire types and the frontend's few rules about it.
 *
 * Every count, status label, sentence and attention item is the backend's
 * (scidb.provenance.run_report through `run_report`; docs/claude/run-report.md).
 * What lives here: which runs a `run_done` asks to be shown, whether the
 * window opens by itself, how a tab is flagged, and number formatting.
 */

export interface RunSummary {
  run_id: string
  group_id: string | null
  function_name: string
  output_types: string[]
  started_at: string | null
  duration_ms: number | null
  total: number
  computed: number
  up_to_date: number
  failed: number
  no_data: number
  saved_records: number
  cancelled: boolean
  stopped: boolean
  language: string | null
  verdict: 'ok' | 'partial' | 'failed' | 'cancelled' | 'stopped' | 'nothing_ran'
  status_line: string
}

export interface Attention {
  severity: 'problem' | 'note'
  kind: string
  text: string
  variable: string | null
}

export interface LocationFilterWire {
  include?: [string, string][][]
  exclude_levels?: Record<string, string[]>
  text?: string
}

export interface RunSettings {
  function_versions: { hash: string; invocations: number; source: { entry: string | null; units: Record<string, string> } }[]
  run_options: string[]
  constants: Record<string, string[]>
  declared_names: Record<string, string>
  path_inputs: Record<string, string[]>
  selectors: Record<string, string[]>
  where: string | null
  locations: LocationFilterWire | null
  on_error: string | null
  skip_computed: boolean | null
  save: boolean | null
}

export interface InputCard {
  param: string
  variable: string
  through_glue: boolean
  records: number
  locations: number
  variants: ({ params: Record<string, string>; records: number } | { more: number })[] | null
}

export interface OutputCard {
  variable: string
  records: number
  locations: number
  new_records: number
  resaved_records: number
}

export interface RunReport {
  summary: RunSummary
  run: { timestamp: string | null; where: string | null; origin: string | null; step: string | null }
  settings: RunSettings
  inputs: InputCard[]
  outputs: OutputCard[]
  exclusions: { keys: Record<string, string>; reason: string; changed_at: string | null }[]
  marks: { variable: string; keys: Record<string, string>; expected: boolean; reason: string }[]
  attention: Attention[]
  notes: string[]
}

export interface CellCounts {
  cells: number
  nan_elements: number
  problems: number
  description: string
}

export interface ColumnProfile {
  dtypes: string[]
  shapes: { shape: string; count: number }[]
  distinct_shapes: number
  cells: CellCounts
}

export interface VariableProfile {
  role: 'input' | 'output' | 'both'
  records: number
  records_with_data: number
  nbytes: number | null
  columns: Record<string, ColumnProfile>
}

/** What one argument or output held at one combination. */
export interface Side {
  records: number
  shape_text: string
  nbytes: number | null
  value: string
  dtype: string
  nan_elements: number
  problems: number
  findings: string
}

export type RowStatus = 'computed' | 'failed' | 'no_data'

export interface ReportRow {
  location: Record<string, string>
  status: RowStatus
  status_label: string
  reason: string
  inputs: Record<string, Side>
  outputs: Record<string, Side>
}

export interface RunReportData {
  run_id: string
  schema_keys: string[]
  variables: Record<string, VariableProfile>
  rows: ReportRow[]
  total_rows: number
  truncated: boolean
  status_counts: Record<RowStatus, number>
  status_labels: Record<RowStatus, string>
  attention: Attention[]
  notes: string[]
}

/** One function node a run covered that left a stored run (`run_done.reports`). */
export interface ReportEntry {
  node_id: string | null
  function_name: string
  run_ids: string[]
}

/** One tab of the window: a function node, opened on one of its runs. */
export interface ReportTab {
  nodeId: string | null
  functionName: string
  /** The run to open on; null = the node's newest (a right-click). */
  runId: string | null
  /** The node's output variable types, to tell two nodes of one function apart. */
  outputs?: string[]
}

export const STATUS_ORDER: RowStatus[] = ['computed', 'failed', 'no_data']

/** An icon per status, so the statuses differ without colour. */
export const STATUS_ICON: Record<RowStatus, string> = {
  computed: '✓',
  failed: '✗',
  no_data: '∅',
}

export const STATUS_COLOR: Record<RowStatus, string> = {
  computed: '#6be16b',
  failed: '#f87171',
  no_data: '#888',
}

export const VERDICT_ICON: Record<RunSummary['verdict'], string> = {
  ok: '✓',
  partial: '⚠',
  failed: '✗',
  cancelled: '⊘',
  stopped: '■',
  nothing_ran: '·',
}

/** `run_done.reports`, defensively: an older backend sends none. */
export function readReports(raw: unknown): ReportEntry[] {
  if (!Array.isArray(raw)) return []
  const out: ReportEntry[] = []
  for (const item of raw) {
    if (!item || typeof item !== 'object') continue
    const e = item as Record<string, unknown>
    const runIds = Array.isArray(e.run_ids) ? e.run_ids.filter((r): r is string => typeof r === 'string' && r !== '') : []
    if (runIds.length === 0) continue
    out.push({
      node_id: typeof e.node_id === 'string' && e.node_id ? e.node_id : null,
      function_name: typeof e.function_name === 'string' ? e.function_name : '',
      run_ids: runIds,
    })
  }
  return out
}

/** The tabs a finished run opens: one per function node, on its last stored
 * run (an EachOf leaves several; the picker reaches the others). */
export function tabsOf(entries: ReportEntry[]): ReportTab[] {
  return entries.map(e => ({
    nodeId: e.node_id,
    functionName: e.function_name,
    runId: e.run_ids[e.run_ids.length - 1],
  }))
}

/**
 * Does the window open by itself when a run ends? Only when the user has
 * not turned it off, and at least one combination was computed or already
 * up to date. A run where nothing ran or everything failed leaves the
 * talking to the failure surfaces (user decision 2026-11-15); right-click
 * still shows its report.
 */
export function shouldAutoOpen(enabled: boolean, summaries: RunSummary[]): boolean {
  if (!enabled) return false
  return summaries.some(s => s.computed + s.up_to_date > 0)
}

/** A tab is flagged when anything is marked "problem"; `pending` until read. */
export function tabTone(report: RunReport | undefined, data?: RunReportData): 'pending' | 'problem' | 'ok' {
  if (!report) return 'pending'
  const items = [...report.attention, ...(data?.attention ?? [])]
  return items.some(a => a.severity === 'problem') ? 'problem' : 'ok'
}

/** Problems first, then notes; the backend's order within each. */
export function attentionOf(report: RunReport, data?: RunReportData): Attention[] {
  const items = [...report.attention, ...(data?.attention ?? [])]
  return [...items.filter(a => a.severity === 'problem'), ...items.filter(a => a.severity !== 'problem')]
}

export const AUTO_OPEN_STORAGE_KEY = 'scistack.runReport.autoOpen'

/** Stored "off" turns it off; anything else (never set) is on. */
export function parseAutoOpen(stored: string | null): boolean {
  return stored !== 'off'
}

/** Records are read without asking up to this many computed combinations. */
export const AUTO_READ_MAX_COMBINATIONS = 500

export function shouldAutoRead(summary: RunSummary): boolean {
  return summary.computed > 0 && summary.computed <= AUTO_READ_MAX_COMBINATIONS
}

export function durationText(ms: number | null | undefined): string {
  if (ms == null) return '?'
  if (ms < 1000) return `${ms} ms`
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`
  const minutes = Math.floor(ms / 60_000)
  return `${minutes} min ${Math.round((ms % 60_000) / 1000)} s`
}

export function bytesText(n: number | null | undefined): string {
  if (n == null) return ''
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} kB`
  if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`
  return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

/** `started_at` is naive UTC from the backend; shown in the viewer's time. */
export function startedText(startedAt: string | null): string {
  if (!startedAt) return '?'
  const d = new Date(/[zZ]|[+-]\d\d:?\d\d$/.test(startedAt) ? startedAt : `${startedAt}Z`)
  if (Number.isNaN(d.getTime())) return startedAt
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
}

/** The `locations=` selection in words. */
export function locationsText(locations: LocationFilterWire | null | undefined): string {
  if (!locations) return 'every location'
  if (locations.text) return locations.text
  const parts = (locations.include ?? []).map(prefix => prefix.map(([k, v]) => `${k}=${v}`).join(' / '))
  let text = parts.length ? parts.join('; ') : 'every location'
  for (const [key, values] of Object.entries(locations.exclude_levels ?? {})) {
    text += `; without ${key} in [${values.join(', ')}]`
  }
  return text
}

export function keysText(keys: Record<string, string>): string {
  const parts = Object.entries(keys).map(([k, v]) => `${k}=${v}`)
  return parts.length ? parts.join(', ') : '(everything)'
}
