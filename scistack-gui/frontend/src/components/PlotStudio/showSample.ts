/**
 * "Show sample" — the panel's half of `PlotSpec.show_sample` / `join_sample`.
 *
 * The rules live in Python (`scistackplot/roles.py`: `overlay_steps`,
 * `overlay_join`): which collapsed keys can be shown, what a deeper tick
 * implies, whether the points are joined and why. The capability report
 * publishes the answers (`capabilities.sample_overlay`) and the panel only
 * displays them. What is here is the two edits a click makes to the spec —
 * kept React-free so `npm test` can pin them.
 */

/** A row of the report's `factors`: a collapsed key in collapse order. */
export interface SampleFactor {
  name: string
  /** Named in `spec.show_sample`. */
  checked: boolean
  /** Part of a point's identity — checked, or implied by a deeper tick. */
  shown: boolean
}

export interface SampleOverlay {
  available: boolean
  reason: string | null
  factors: SampleFactor[]
  /** Ticked names that are not collapsed right now (inert, kept). */
  ignored: string[]
  shown: string[]
  averaged: string[]
  /** A joined line runs along `span` (the innermost tick) and never crosses
   *  a bracket (the tick layers above it) — `roles.GroupingLayers`. */
  join: {
    join: boolean
    automatic: boolean
    reason: string
    setting: boolean | null
    span: string | null
    brackets: string[]
  }
  granularity: string
  /** `PlotSpec.sample_color`: what the spec asks (`setting`), the key
   *  colouring the points right now (`active`, null = the mark's colour) and
   *  the shown keys that could (`options`). */
  color: { setting: string | null; active: string | null; options: string[] }
}

/** The Join dropdown's three states, and the spec value each writes. */
export type JoinChoice = 'auto' | 'lines' | 'points'

export function joinChoice(setting: boolean | null | undefined): JoinChoice {
  if (setting === null || setting === undefined) return 'auto'
  return setting ? 'lines' : 'points'
}

export function joinSetting(choice: JoinChoice): boolean | null {
  if (choice === 'auto') return null
  return choice === 'lines'
}

/**
 * `spec.show_sample` after ticking or unticking `name`.
 *
 * Order is kept as the user made it; Python reads the set, never the order.
 * Unticking a key that a deeper tick implies changes nothing visible (the
 * report still marks it `shown`), so the checkbox for an implied key is
 * disabled rather than left to look like a control that does nothing.
 */
export function toggleShowSample(current: string[] | undefined, name: string, on: boolean): string[] {
  const names = current ?? []
  if (on) return names.includes(name) ? names : [...names, name]
  return names.filter(n => n !== name)
}

/** Whether a checkbox is ticked: checked, or implied by a deeper tick. */
export function isTicked(factor: SampleFactor): boolean {
  return factor.checked || factor.shown
}

/** Whether a checkbox is locked: implied by a deeper tick but not itself
 *  checked — unticking it would change nothing. */
export function isLocked(factor: SampleFactor): boolean {
  return factor.shown && !factor.checked
}

/** The Colour-points-by dropdown's value: `''` for the mark's colour. */
export const MARK_COLOR = ''

export function sampleColorChoice(setting: string | null | undefined): string {
  return setting ?? MARK_COLOR
}

/** `spec.sample_color` for a dropdown choice: null clears it. */
export function sampleColorSetting(choice: string): string | null {
  return choice === MARK_COLOR ? null : choice
}

/** The Join dropdown's tooltip: the rule, naming the layer a line runs
 *  along (`span`, the innermost grouping layer) and the brackets it never
 *  crosses — decided in `roles`, displayed here. */
export function joinTooltip(join: SampleOverlay['join']): string {
  const span = join.span ? ` (${join.span})` : ''
  const brackets = join.brackets.length ? ` — never across ${join.brackets.join(' · ')}` : ''
  return (
    'Auto joins the points when they are repeated measures: the shown key has a value ' +
    `at every level of the innermost grouping layer${span}. A line spans that layer only${brackets}.`
  )
}
