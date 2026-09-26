/**
 * Saved plots: the panel-side half (.claude/plan-saved-plots.md, Stage 4).
 *
 * The backend owns everything about a saved plot's SPEC: storage, names,
 * versions, reading an old format (`scistackplot.restore_spec`), and checking
 * it against today's data. This module owns only what the backend stores
 * verbatim and never interprets: the panel's own VIEW settings. It reads
 * them back as leniently as the backend reads a spec, because they drift
 * too. A view key this build no longer knows is ignored, and a bad value
 * falls back to the panel default.
 *
 * It also owns "has this plot changed since it was opened or saved?" and
 * which figure-index reset to skip when a plot is loaded.
 *
 * React-free, so `npm test` runs it (tsconfig.test.json lists it).
 */

import type { PreviewMode } from './preview.js'

/** One saved plot as the list shows it (`SavedPlotInfo.to_dict`). */
export interface SavedPlotInfo {
  plot_id: string
  variable: string
  name: string
  version: number
  saved_at: string
  hidden: boolean
}

/** One setting that did not come back as saved (`RestoreNote.to_dict`). */
export interface RestoreNote {
  path: string
  kind: 'unknown_setting' | 'invalid_value' | 'dropped_entry' | 'measure_replaced' | 'not_in_data' | string
  message: string
  value?: unknown
}

/** The panel settings a saved plot carries besides its spec. */
export interface PlotView {
  previewMode: PreviewMode
  /** Which figure of a Separate-figures fan-out was on screen. */
  figureIndex: number
}

// Fit pane by default (user, 2026-09-26): the boxes then show the pane's size.
export const DEFAULT_VIEW: PlotView = { previewMode: 'pane', figureIndex: 0 }

const PREVIEW_MODES: readonly PreviewMode[] = ['export', 'pane']

/** What Save stores as `view`. */
export function viewState(previewMode: PreviewMode, figureIndex: number): PlotView {
  return { previewMode, figureIndex }
}

/**
 * A stored view read back leniently: each known key that still makes sense is
 * kept, anything else takes the panel default. Never throws, because a view
 * written by another build is data, not a contract.
 */
export function readView(raw: unknown): PlotView {
  const stored = raw !== null && typeof raw === 'object' && !Array.isArray(raw)
    ? (raw as Record<string, unknown>)
    : {}
  const previewMode = PREVIEW_MODES.includes(stored.previewMode as PreviewMode)
    ? (stored.previewMode as PreviewMode)
    : DEFAULT_VIEW.previewMode
  const index = stored.figureIndex
  const figureIndex = typeof index === 'number' && Number.isInteger(index) && index >= 0 ? index : 0
  return { previewMode, figureIndex }
}

/** JSON with object keys sorted, so two equal settings compare equal
 *  however their keys were inserted (edits spread objects in new orders). */
export function stableStringify(value: unknown): string {
  return JSON.stringify(sortKeys(value))
}

function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeys)
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {}
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      const item = (value as Record<string, unknown>)[key]
      if (item !== undefined) out[key] = sortKeys(item)
    }
    return out
  }
  return value
}

/**
 * What "modified" compares: the spec and the view settings that change how
 * the figure LOOKS. The figure index is saved but not compared: stepping
 * through a fan-out is browsing, not an edit.
 */
export function modifiedKey(spec: unknown, view: PlotView): string {
  return stableStringify({ spec, previewMode: view.previewMode })
}

/**
 * Whether the panel differs from what was last opened or saved.
 * `baseline === null` means it always does: a plot restored with notes is
 * not in the current format until it is saved again, and the panel says so
 * rather than looking clean.
 */
export function isModified(baseline: string | null, current: string): boolean {
  return baseline === null || baseline !== current
}

/** The factors that fan the figure set out, as one comparable string. The
 *  figure cursor resets to 0 when this changes, except on a load. */
export function iterateSignature(roles: Record<string, string> | undefined | null): string {
  return Object.entries(roles ?? {})
    .filter(([, role]) => role === 'iterate')
    .map(([name]) => name)
    .sort()
    .join('|')
}

/** The banner's one-line summary of a restore's notes. */
export function notesSummary(notes: readonly RestoreNote[]): string {
  if (notes.length === 0) return ''
  const stale = notes.filter(n => n.kind === 'not_in_data').length
  const drifted = notes.length - stale
  const parts: string[] = []
  if (drifted) parts.push(`${drifted} setting${drifted === 1 ? '' : 's'} no longer appl${drifted === 1 ? 'ies' : 'y'}`)
  if (stale) parts.push(`${stale} not in today's data`)
  return parts.join('; ')
}

/** "2026-09-24T15:04:05+00:00" as the list shows it, in local time. */
export function savedAtLabel(savedAt: string): string {
  const when = new Date(savedAt)
  if (Number.isNaN(when.getTime())) return savedAt
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${when.getFullYear()}-${pad(when.getMonth() + 1)}-${pad(when.getDate())} ${pad(when.getHours())}:${pad(when.getMinutes())}`
}
