/**
 * The step-by-step wizard's rules — pure, React- and DOM-free (node --test).
 *
 * The backend sends every step (scistackstats.guide / explain): its
 * question, facts, choice cards, recommendation and whether it blocks. This
 * module only decides navigation (user decision 2026-10-20: jump back to any
 * finished step, never forward past a blocked one) and applies a choice's
 * `action` through the spec helpers. It words nothing.
 */

import {
  addAnalysis, excludeLevel, guideState, setGuide, setOverride, setPrimary, setRole, updateAnalysis,
  type Analysis, type StatsSpec,
} from '../statsSpec.js'

export interface GuideAction {
  op: 'set_role' | 'set_primary' | 'set_override' | 'set_analysis' | 'exclude_level'
  factor?: string
  role?: string
  kind?: string | null
  analysis?: string | null
  level?: string
  /** set_analysis: fields to set; a null value clears the field (the backend default). */
  patch?: Record<string, unknown>
}

export interface GuideChoice {
  id: string
  label: string
  lay: string
  recommended: boolean
  selected: boolean
  action: GuideAction | null
  disabled: string | null
  concept: string | null
}

export interface GuideCard {
  id: string
  title: string
  lay: string
  choices: GuideChoice[]
}

export interface GuideStep {
  id: string
  title: string
  question: string
  concepts: string[]
  facts: string[]
  cards: GuideCard[]
  recommendation: string | null
  blocked: string | null
  warnings: string[]
  needs_results: boolean
  table: { columns: string[]; rows: (string | number | null)[][]; empty_rows?: number[] } | null
}

export interface GuideConcept {
  id: string
  title: string
  technical: string
  lay: string
  more: string
}

export interface GuidePayload {
  steps: GuideStep[]
  concepts: Record<string, GuideConcept>
  has_results: boolean
}

export type StepStatus = 'done' | 'current' | 'blocked' | 'open' | 'locked'

/** The step the wizard shows: the stored one, else the first. */
export function currentStep(spec: StatsSpec, steps: readonly GuideStep[]): string | null {
  const stored = guideState(spec).step
  if (stored && steps.some(s => s.id === stored)) return stored
  return steps[0]?.id ?? null
}

/** The index of the first step that blocks, or `steps.length`. */
function firstBlocked(steps: readonly GuideStep[]): number {
  const i = steps.findIndex(s => s.blocked)
  return i < 0 ? steps.length : i
}

/**
 * Whether the user may go to step `id` now. Back to any finished step, or to
 * the first unfinished one; never past a step that blocks.
 */
export function canGoTo(spec: StatsSpec, steps: readonly GuideStep[], id: string): boolean {
  const target = steps.findIndex(s => s.id === id)
  if (target < 0) return false
  if (target > firstBlocked(steps)) return false
  const done = new Set(guideState(spec).done)
  if (done.has(id) || id === currentStep(spec, steps)) return true
  const firstOpen = steps.findIndex(s => !done.has(s.id))
  return target <= (firstOpen < 0 ? steps.length - 1 : firstOpen)
}

export function stepStatus(spec: StatsSpec, steps: readonly GuideStep[], id: string): StepStatus {
  const step = steps.find(s => s.id === id)
  if (!step) return 'locked'
  if (id === currentStep(spec, steps)) return step.blocked ? 'blocked' : 'current'
  if (!canGoTo(spec, steps, id)) return 'locked'
  if (step.blocked) return 'blocked'
  return guideState(spec).done.includes(id) ? 'done' : 'open'
}

/** Show step `id` (if allowed). */
export function goTo(spec: StatsSpec, steps: readonly GuideStep[], id: string): StatsSpec {
  return canGoTo(spec, steps, id) ? setGuide(spec, { step: id }) : spec
}

/** Finish the current step and show the next one; unchanged when it blocks. */
export function next(spec: StatsSpec, steps: readonly GuideStep[]): StatsSpec {
  const id = currentStep(spec, steps)
  const i = steps.findIndex(s => s.id === id)
  if (i < 0 || steps[i].blocked) return spec
  const done = [...new Set([...guideState(spec).done, steps[i].id])]
  const order = steps.map(s => s.id)
  done.sort((a, b) => order.indexOf(a) - order.indexOf(b))
  const following = steps[i + 1]?.id ?? steps[i].id
  return setGuide(spec, { done, step: following })
}

/** Back to the first blocking step when a fresh guide blocks one BEFORE the
 *  step shown (an edit, or new data, made an earlier answer incomplete);
 *  unchanged otherwise. */
export function clampToBlocked(spec: StatsSpec, steps: readonly GuideStep[]): StatsSpec {
  const blocked = firstBlocked(steps)
  const i = steps.findIndex(s => s.id === currentStep(spec, steps))
  return i > blocked ? setGuide(spec, { step: steps[blocked].id }) : spec
}

export function previous(spec: StatsSpec, steps: readonly GuideStep[]): StatsSpec {
  const i = steps.findIndex(s => s.id === currentStep(spec, steps))
  return i > 0 ? setGuide(spec, { step: steps[i - 1].id }) : spec
}

function firstAnalysis(spec: StatsSpec): { spec: StatsSpec; analysis: Analysis } {
  const s = spec.comparisons.length ? spec : addAnalysis(spec)
  return { spec: s, analysis: s.comparisons[0] }
}

/** Apply a choice's action (the vocabulary of scistackstats.explain). */
export function applyAction(spec: StatsSpec, action: GuideAction | null): StatsSpec {
  if (!action) return spec
  switch (action.op) {
    case 'set_role':
      return action.factor && action.role ? setRole(spec, action.factor, action.role) : spec
    case 'set_primary':
      return action.factor ? setPrimary(spec, action.factor) : spec
    case 'set_override': {
      const { spec: s, analysis } = firstAnalysis(spec)
      if (!action.factor) return spec
      return setOverride(s, action.analysis ?? analysis.id, action.factor, action.kind ?? null)
    }
    case 'set_analysis': {
      const { spec: s, analysis } = firstAnalysis(spec)
      const patch: Partial<Analysis> = {}
      for (const [k, v] of Object.entries(action.patch ?? {})) {
        (patch as Record<string, unknown>)[k] = v === null ? undefined : v
      }
      return updateAnalysis(s, analysis.id, patch)
    }
    case 'exclude_level':
      return action.factor && action.level != null ? excludeLevel(spec, action.factor, action.level) : spec
    default:
      return spec
  }
}
