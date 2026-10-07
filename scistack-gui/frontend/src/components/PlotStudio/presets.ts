/**
 * Plot presets: the panel-side half (.claude/plan-plot-presets.md, Stage 4).
 *
 * The backend owns everything about a preset: what it keeps and what stays
 * with the variable (`scistackplot.presets.FIELD_CLASSES`), storing it, and
 * applying it to another variable's data. This module only words what the
 * backend answered, for the rail.
 *
 * React-free, so `npm test` runs it (tsconfig.test.json lists it).
 */

import type { RestoreNote } from './savedPlots.js'

/** One preset as the list shows it (`PresetInfo.to_dict` + the list's
 *  `shape_warning`, both from the backend). */
export interface PresetInfo {
  preset_id: string
  name: string
  version: number
  saved_at: string
  hidden: boolean
  made_on: string | null
  made_on_shape: string | null
  /** Why this preset may not fit the panel's variable, or null. */
  shape_warning?: string | null
}

/** What `plot_preset_apply` returns. */
export interface AppliedPreset {
  preset: PresetInfo & { spec: unknown; notes: RestoreNote[] }
  capabilities: unknown | null
}

/** "made on StepLength (scalar)", or '' when the row does not say. */
export function madeOnLabel(preset: Pick<PresetInfo, 'made_on' | 'made_on_shape'>): string {
  if (!preset.made_on) return ''
  return preset.made_on_shape
    ? `made on ${preset.made_on} (${preset.made_on_shape})`
    : `made on ${preset.made_on}`
}

/** The banner's line after an apply: which preset, and what did not carry. */
export function appliedSummary(name: string, notes: readonly RestoreNote[]): string {
  if (notes.length === 0) return `Applied "${name}".`
  const kind = notes.filter(n => n.kind === 'kind_unavailable').length
  const stale = notes.filter(n => n.kind === 'not_in_data').length
  const other = notes.length - kind - stale
  const parts: string[] = []
  if (stale) parts.push(`${stale} setting${stale === 1 ? '' : 's'} not in this variable's data`)
  if (kind) parts.push('plot type changed to fit this variable')
  if (other) parts.push(`${other} setting${other === 1 ? '' : 's'} no longer appl${other === 1 ? 'ies' : 'y'}`)
  return `Applied "${name}": ${parts.join('; ')}.`
}
