/**
 * The step verdict as the canvas shows it (GitHub #28).
 *
 * Every word, glyph and count is scidb's (`scidb.provenance.status`, attached
 * to the node by `services/step_status_service.attach_step_verdicts`). This
 * module only picks the chip's tone, so the node, the Activity Bar tree and
 * the run report draw the same verdict the same way. The node colour stays
 * green/red ("does this need running?"); the chip says why.
 */

export type VerdictWord = 'succeeded' | 'succeeded_with_warnings' | 'not_complete'

export interface VerdictReason {
  kind: string
  count: number | null
  glyph: string
  text: string
  locations?: Record<string, string>[]
}

export interface StepVerdict {
  function_name: string
  word: VerdictWord
  glyph: string
  label: string
  status_line: string
  chip: string
  new_chip: string
  reasons: VerdictReason[]
  new: number
  warnings: number
  excluded: number
  counted: number
  present: number
  needs_running: boolean
  notes?: string[]
}

export type ChipTone = 'problem' | 'warning' | 'info' | 'none'

/** Reasons that mean "something is wrong here", not "run me". */
const PROBLEM_KINDS = new Set(['failed', 'missing_input', 'disconnected', 'cancelled', 'stopped'])

/** The reason chip's tone: a problem (red), a warning or a stale result
 *  (amber), informational (grey: only exclusions), or no chip at all. */
export function chipTone(v: StepVerdict | null | undefined): ChipTone {
  if (!v || !v.chip) return 'none'
  const top = v.reasons[0]
  if (top) return PROBLEM_KINDS.has(top.kind) ? 'problem' : 'warning'
  if (v.warnings) return 'warning'
  return 'info'
}

/** The tooltip: the whole status line, then each reason. */
export function verdictTitle(v: StepVerdict): string {
  const lines = [v.status_line, ...v.reasons.map(r => r.text), ...(v.notes ?? [])]
  return [...new Set(lines.filter(Boolean))].join('\n')
}

/** The states the data view filters to when a chip is clicked. */
export function statesForReason(kind: string | undefined): string[] {
  switch (kind) {
    case 'failed': return ['failed']
    case 'missing_input': return ['missing_input']
    case 'not_run': return ['not_run', 'new']
    case 'code_changed':
    case 'inputs_changed': return ['outdated']
    case 'warning': return ['warning']
    case 'excluded': return ['excluded']
    case 'new': return ['new']
    default: return []
  }
}
