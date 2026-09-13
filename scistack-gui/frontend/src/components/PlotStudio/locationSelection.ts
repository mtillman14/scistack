/**
 * The schema location picker's rules, with no React in them.
 *
 * Split out of `SchemaLocationPicker.tsx` and `PlotStudio.tsx` for the reason
 * the extension splits its vscode-free modules out (`tsconfig.test.json`): a
 * rule nothing can execute in a test is a rule that drifts. Everything here is
 * pure — same inputs, same outputs, no DOM, no fetch — so `npm test` can run it
 * under node's own test runner.
 *
 * Two rules live here, and both have a wrong-but-plausible version:
 *
 * - **The selection is a minimal covering set of prefixes.** Ticking every
 *   trial of a subject and ticking the subject must store the SAME thing, or a
 *   trial added to that subject tomorrow lands inside one selection and outside
 *   the other. Re-deriving it from a coverage walk (rather than bookkeeping as
 *   boxes are clicked) is what makes that true by construction.
 * - **Picking one location does not restyle the figure.** It moves schema keys
 *   off AGGREGATE, because collapsing to a mean is the one thing that hides
 *   what the picker was opened to look at — and leaves X/COLOR/FACET alone,
 *   because those already draw every level.
 */

/** One `[key, value]` step of a location path. */
export type PathStep = [string, string]
/** A prefix: the outermost steps of a location. `[]` would mean "everything". */
export type Prefix = PathStep[]

export type LocationState = 'green' | 'amber' | 'red' | 'grey'

export interface LocationCounts {
  green: number
  amber: number
  red: number
  grey: number
}

export interface LocationNode {
  key: string
  value: string
  state: LocationState
  counts: LocationCounts
  is_leaf: boolean
  path: Prefix
  schema_id: number | null
  record_id: string | null
  code_version: string | null
  children: LocationNode[]
}

export interface LocationTree {
  variable: string
  schema_keys: string[]
  variant: Record<string, unknown>
  counts: LocationCounts
  total: number
  green: number
  verdict: LocationState
  basis: 'expected' | 'discovery' | 'mixed' | 'present_only'
  notes: string[]
  roots: LocationNode[]
  selection: Record<string, unknown>
}

// --- prefix algebra ---------------------------------------------------------

const same = (a: PathStep, b: PathStep) => a[0] === b[0] && a[1] === b[1]

/** Is `a` a prefix of (or equal to) `b`? */
export function covers(a: Prefix, b: Prefix): boolean {
  return a.length <= b.length && a.every((step, i) => same(step, b[i]))
}

export type Coverage = 'full' | 'partial' | 'none'

/**
 * How much of this node's subtree the selection holds.
 *
 * `full` also means "draw the checkbox ticked", `partial` "indeterminate" —
 * the tri-state is read off the selection rather than stored beside it, so the
 * boxes cannot disagree with what will be plotted.
 */
export function coverageOf(node: LocationNode, include: Prefix[]): Coverage {
  if (include.some(p => covers(p, node.path))) return 'full'
  if (node.children.length === 0) return 'none'
  const kids = node.children.map(c => coverageOf(c, include))
  if (kids.every(k => k === 'full')) return 'full'
  return kids.some(k => k !== 'none') ? 'partial' : 'none'
}

/**
 * Re-derive the minimal covering set from a coverage walk.
 *
 * Everything selected is stored as NOTHING selected: an empty `include` is the
 * inert state, which is also what an untouched picker means, so "select all"
 * and "never touched it" cannot produce two different specs for one figure.
 */
export function normalize(roots: LocationNode[], include: Prefix[]): Prefix[] {
  if (roots.length > 0 && roots.every(r => coverageOf(r, include) === 'full')) return []
  const out: Prefix[] = []
  const visit = (node: LocationNode) => {
    const cov = coverageOf(node, include)
    if (cov === 'full') { out.push(node.path); return }
    if (cov === 'none') return
    node.children.forEach(visit)
  }
  roots.forEach(visit)
  return out
}

export function childrenAt(roots: LocationNode[], parentPath: Prefix): LocationNode[] {
  if (parentPath.length === 0) return roots
  let level = roots
  let node: LocationNode | undefined
  for (const step of parentPath) {
    node = level.find(n => same(n.path[n.path.length - 1], step))
    if (!node) return []
    level = node.children
  }
  return node ? node.children : []
}

/**
 * Remove one location from the selection, materialising the complement.
 *
 * Unticking a trial inside a ticked subject has to turn "all of subject 01"
 * into its siblings — the covering prefix is exploded one level at a time down
 * to the unticked node. That is the whole reason the stored form is a set of
 * prefixes rather than a set of leaves: the common case stays one entry, and
 * the exception costs exactly the entries it needs.
 */
export function without(
  roots: LocationNode[],
  include: Prefix[],
  path: Prefix
): Prefix[] {
  const next = include.filter(p => !covers(p, path) && !covers(path, p))
  // `[]` is the virtual root: with nothing stored, everything is selected, so
  // the explosion starts at the top.
  const covering: Prefix | undefined =
    include.length === 0 ? [] : include.find(p => covers(p, path))
  if (covering !== undefined) {
    for (let depth = covering.length; depth < path.length; depth++) {
      for (const sibling of childrenAt(roots, path.slice(0, depth))) {
        const step = sibling.path[sibling.path.length - 1]
        if (!same(step, path[depth])) next.push(sibling.path)
      }
    }
  }
  return normalize(roots, next)
}

/** Add one location to the selection. */
export function withPath(
  roots: LocationNode[],
  include: Prefix[],
  path: Prefix
): Prefix[] {
  const next = include.filter(p => !covers(path, p))
  next.push(path)
  return normalize(roots, next)
}

// --- search / problems ------------------------------------------------------

/** A node matches if it does, or anything beneath it does. */
export function matchesQuery(node: LocationNode, query: string): boolean {
  if (!query) return true
  if (node.value.toLowerCase().includes(query)) return true
  return node.children.some(c => matchesQuery(c, query))
}

/**
 * Is there anything wrong in this subtree?
 *
 * Mirrors `scidb.locations.prune_to_problems`. Grey is NOT a problem: an
 * exclusion is a decision the user already made and justified.
 */
export function hasProblem(node: LocationNode): boolean {
  return node.counts.red > 0 || node.counts.amber > 0
}

// --- role adjustment on pick ------------------------------------------------

export type Role = 'iterate' | 'x' | 'color' | 'facet' | 'aggregate' | 'free'

/**
 * The roles a spec should hold once one location has been picked.
 *
 * Picking `subject=01` leaves `trial` unanswered, and a trial factor sitting on
 * AGGREGATE would collapse the very thing the picker was opened to look at —
 * every trial's own values. So any schema key the selection does NOT name is
 * moved off AGGREGATE onto FREE (replicates), and one with no role at all gets
 * FREE too.
 *
 * Deliberately NOT a blanket reassignment: a key already on X, COLOR or FACET
 * is drawing its levels individually, which is what was asked for, and stamping
 * over a chosen x axis because a location was clicked would be astonishing.
 */
export function rolesAfterPick(
  roles: Record<string, Role>,
  path: Prefix,
  schemaKeys: string[]
): Record<string, Role> {
  const named = new Set(path.map(([key]) => key))
  const next = { ...roles }
  for (const key of schemaKeys) {
    if (named.has(key)) continue
    const current = next[key]
    if (current === undefined || current === 'aggregate') next[key] = 'free'
  }
  return next
}

/**
 * The button label for a selection: what the picker would say if it were open.
 *
 * One selection reads as the location itself, because that is the common case
 * and "1 location" would be a worse answer than naming it.
 */
export function describeSelection(include: Prefix[]): string {
  if (include.length === 0) return 'All locations…'
  if (include.length === 1) {
    return include[0].map(([key, value]) => `${key}=${value}`).join(' / ') + '…'
  }
  return `${include.length} locations…`
}
