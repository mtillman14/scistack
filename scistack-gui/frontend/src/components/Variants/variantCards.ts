/**
 * Presentation helpers for the Variants popup (`VariantsPopup.tsx`). Named
 * `variantCards.ts`, not `variantsPopup.ts`: two names differing only in case
 * collide on a case-insensitive filesystem (macOS).
 *
 * React-free on purpose, so `npm test` can execute them (see
 * `tsconfig.test.json`: the include list is explicit, and a file added there
 * must reach neither React nor the DOM).
 *
 * **Nothing here decides anything about variants.** What a card is, its
 * selection, which axes distinguish it, whether it is the default, what a
 * delete removes: all of that arrives decided from scidb, through
 * `services/variant_cards_service.py` (CLAUDE.md NOTE 3/4). This file only
 * WORDS and SHAPES it: labels, the mini-DAG's node list, the location tree.
 * In particular a selection is never built here. Pin and delete send back the
 * card's own `selection` / `card_id`.
 */

export type Verdict = 'current' | 'partially_superseded' | 'superseded'

export interface RunRef {
  run_id: string
  timestamp: string
  user_id: string | null
  where_clause: string | null
  invocation_id: string
  function_hash: string | null
  run_options: string | null
}

export interface CodeRef {
  function_hash: string | null
  version: string | null
  invocations: number
}

export interface UpstreamStep {
  node_id: string
  function_name: string
  output_type: string
  inputs: Record<string, string>
  constants: Record<string, Record<string, number>>
  path_inputs: Record<string, Record<string, number>>
  code: CodeRef[]
  run_options: Record<string, number>
  invocations: number
  uniform: boolean
  parameter_names: Record<string, string>
}

export interface UpstreamEdge {
  source: string
  target: string
  param: string
}

export interface VariantUpstream {
  variables: string[]
  steps: UpstreamStep[]
  edges: UpstreamEdge[]
}

export interface ParameterOffer {
  parameter: string
  value: unknown
  label: string
  declared: boolean
  editable: boolean
  message: string
}

export interface VariantCard {
  card_id: string
  function_name: string | null
  selection: Record<string, unknown>
  distinguishing: Record<string, string>
  selection_exact: boolean
  overlaps_with: string[]
  record_ids: string[]
  record_count: number
  first_saved: string | null
  last_saved: string | null
  verdict: Verdict
  verdict_label: string
  current_location_count: number
  location_keys: string[]
  locations: Record<string, string | number>[]
  runs: RunRef[]
  upstream: VariantUpstream
  is_default: boolean
  is_pinned: boolean
  pin_conflict: string | null
  parameter_offers: ParameterOffer[]
}

export interface VariantPin {
  pin_id: string
  variable: string
  selection: Record<string, unknown>
  reason: string
  pinned_by: string | null
  pinned_at: string
  released_at: string | null
  release_reason: string | null
}

export interface TombstoneSummary {
  tombstone_id: string
  deleted_at: string
  deleted_by: string | null
  reason: string
  targets: Record<string, unknown>[]
  by_variable: Record<string, number>
}

export interface VariantCardsReply {
  variable: string
  cards: VariantCard[]
  varying_axes: string[]
  producer_varies: boolean
  excluded_record_count: number
  command: string
  pin: VariantPin | null
  default_selection: Record<string, unknown> | null
  default_sources: string[]
  pin_history: VariantPin[]
  tombstones: TombstoneSummary[]
}

export interface DeletePlanReply {
  targets: Record<string, unknown>[]
  by_variable: Record<string, number>
  seed_by_variable: Record<string, number>
  downstream_by_variable: Record<string, number>
  lost_locations: Record<string, Record<string, string | number>[]>
  pins_to_release: string[]
  fingerprint: string
  warnings: string[]
  total_records: number
  invocation_count: number
  run_count: number
}

export interface PinConflict {
  node_id: string | null
  function_name: string | null
  variable: string
  pin: VariantPin
}

const CODE_PREFIX = '__code__.'
const RUN_PREFIX = '__run__.'

/** `filter.low_hz` → `low_hz (filter)`; `__code__.f` → `code of f`. */
export function axisLabel(key: string): string {
  if (key.startsWith(CODE_PREFIX)) return `code of ${key.slice(CODE_PREFIX.length)}`
  if (key.startsWith(RUN_PREFIX)) return `run options of ${key.slice(RUN_PREFIX.length)}`
  const dot = key.lastIndexOf('.')
  if (dot <= 0) return key
  return `${key.slice(dot + 1)} (${key.slice(0, dot)})`
}

function valueText(value: unknown): string {
  if (typeof value === 'string') return value
  return JSON.stringify(value)
}

/** The collapsed card's headline: only what tells it apart from its siblings. */
export function cardHeading(card: VariantCard): string {
  const entries = Object.entries(card.distinguishing ?? {})
  if (entries.length === 0) return 'the only variant'
  return entries.map(([k, v]) => `${axisLabel(k)} = ${v}`).join(' · ')
}

/** Every key of the selection, in full, for the expanded card. */
export function selectionLines(selection: Record<string, unknown>): string[] {
  return Object.entries(selection ?? {}).map(([k, v]) => `${axisLabel(k)} = ${valueText(v)}`)
}

export type Badge = { code: 'pinned' | 'default' | 'current' | 'partial' | 'superseded'; label: string }

/**
 * The status chip. With a pin reaching the variable, the question is "is this
 * the default?"; with none, every current variant flows downstream and the
 * question is only whether a load still returns it.
 */
export function statusBadge(card: VariantCard, hasDefault: boolean): Badge {
  if (card.is_pinned) return { code: 'pinned', label: '★ current (pinned)' }
  if (hasDefault && card.is_default) return { code: 'default', label: '☆ current (via an upstream pin)' }
  if (card.verdict === 'superseded') return { code: 'superseded', label: 'replaced by a newer run' }
  if (card.verdict === 'partially_superseded') return { code: 'partial', label: 'partly replaced' }
  if (hasDefault) return { code: 'current', label: 'not the default' }
  return { code: 'current', label: 'current' }
}

/** One-line summary under the popup title. */
export function popupSummary(reply: VariantCardsReply): string {
  const n = reply.cards.length
  const parts = [`${n} variant${n === 1 ? '' : 's'}`]
  if (reply.varying_axes.length > 0) {
    parts.push(`differing in ${reply.varying_axes.map(axisLabel).join(', ')}`)
  }
  if (reply.producer_varies) parts.push('made by different functions')
  if (reply.default_selection && reply.default_sources.length > 0) {
    const own = reply.default_sources.includes(reply.variable)
    parts.push(own ? 'one is pinned as current' : `default follows the pin on ${reply.default_sources.join(', ')}`)
  }
  if (reply.excluded_record_count > 0) {
    parts.push(`${reply.excluded_record_count} hidden record(s) on no card`)
  }
  return parts.join('; ')
}

/** The settings lines a mini-DAG step node shows. */
export function stepLines(step: UpstreamStep): string[] {
  const lines: string[] = []
  const versions = step.code.map(c => c.version).filter((v): v is string => !!v)
  if (versions.length > 0) lines.push(`code ${versions.join(' / ')}`)
  for (const [param, values] of Object.entries(step.constants)) {
    const parameter = step.parameter_names?.[param]
    const name = parameter && parameter !== param ? `${param} ← ${parameter}` : param
    lines.push(`${name} = ${Object.keys(values).join(' | ')}`)
  }
  for (const [param, specs] of Object.entries(step.path_inputs)) {
    lines.push(`${param}: ${Object.keys(specs).map(shortSpec).join(' | ')}`)
  }
  for (const label of Object.keys(step.run_options)) lines.push(label)
  if (!step.uniform) lines.push('⚠ settings differ across records')
  return lines
}

/** A PathInput spec is JSON; show its template, which is what a user wrote. */
function shortSpec(spec: string): string {
  try {
    const parsed = JSON.parse(spec) as Record<string, unknown>
    if (typeof parsed.template === 'string') return parsed.template
  } catch {
    // not JSON: show as is
  }
  return spec
}

export interface MiniNode {
  id: string
  kind: 'variable' | 'step'
  label: string
  lines: string[]
  /** The variable this popup is about: drawn as the sink. */
  isRoot: boolean
}

export interface MiniEdge {
  id: string
  source: string
  target: string
  label: string
}

/** `card.upstream` as nodes and edges for the read-only mini DAG. */
export function upstreamGraph(card: VariantCard, variable: string): { nodes: MiniNode[]; edges: MiniEdge[] } {
  const nodes: MiniNode[] = card.upstream.variables.map(name => ({
    id: `var:${name}`,
    kind: 'variable',
    label: name,
    lines: [],
    isRoot: name === variable,
  }))
  for (const step of card.upstream.steps) {
    nodes.push({
      id: step.node_id,
      kind: 'step',
      label: step.function_name,
      lines: stepLines(step),
      isRoot: false,
    })
  }
  const edges: MiniEdge[] = card.upstream.edges.map(e => ({
    id: `${e.source}->${e.target}:${e.param}`,
    source: e.source,
    target: e.target,
    label: e.param,
  }))
  return { nodes, edges }
}

export interface LocationBranch {
  label: string
  count: number
  children: LocationBranch[]
}

/**
 * Every location of a card as a tree, grouped by schema level in the order
 * scidb gave (`location_keys`), with a count at each branch: 450 leaves read
 * as "SS01 (24) › BL (6) …" rather than a wall of chips.
 */
export function locationTree(card: VariantCard): LocationBranch[] {
  const keys = card.location_keys ?? []
  function build(rows: Record<string, string | number>[], depth: number): LocationBranch[] {
    if (depth >= keys.length) return []
    const groups = new Map<string, Record<string, string | number>[]>()
    for (const row of rows) {
      const value = row[keys[depth]]
      const label = value === undefined ? '—' : String(value)
      const list = groups.get(label)
      if (list) list.push(row)
      else groups.set(label, [row])
    }
    return [...groups.entries()].map(([label, members]) => ({
      label: `${keys[depth]} ${label}`,
      count: members.length,
      children: build(members, depth + 1),
    }))
  }
  return build(card.locations ?? [], 0)
}

/** The delete dialog's per-variable lines, named records first. */
export function deleteLines(plan: DeletePlanReply): string[] {
  return Object.entries(plan.by_variable).map(([variable, n]) => {
    const down = plan.downstream_by_variable[variable] ?? 0
    const lost = plan.lost_locations[variable]?.length ?? 0
    const parts = [`${variable}: ${n} record${n === 1 ? '' : 's'}`]
    if (down > 0) parts.push(down === n ? 'computed from it' : `${down} computed from it`)
    if (lost > 0) parts.push(`nothing left at ${lost} location${lost === 1 ? '' : 's'}`)
    return parts.join(' · ')
  })
}

/** Whether the Delete button may be pressed. */
export function canConfirmDelete(plan: DeletePlanReply | null, reason: string, busy: boolean): boolean {
  return !!plan && plan.total_records > 0 && reason.trim().length > 0 && !busy
}

/** The before-run dialog's explanation. */
export function pinConflictMessage(conflicts: PinConflict[]): string {
  const variables = [...new Set(conflicts.map(c => c.variable))]
  const list = variables.join(', ')
  return (
    `This run writes to ${list}, which ${variables.length === 1 ? 'is' : 'are'} pinned ` +
    `to a current variant. New output that differs from the pinned variant will not ` +
    `be the default unless you move the pin.`
  )
}

/** The distinct pinned variables of a conflict list, for "move the pin". */
export function conflictVariables(conflicts: PinConflict[]): string[] {
  return [...new Set(conflicts.map(c => c.variable))].sort()
}
