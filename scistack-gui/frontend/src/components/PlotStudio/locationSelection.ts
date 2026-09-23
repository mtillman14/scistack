/**
 * The schema location picker's rules, with no React in them.
 *
 * Split out of `SchemaLocationPicker.tsx` and `PlotStudio.tsx` for the reason
 * the extension splits its vscode-free modules out (`tsconfig.test.json`): a
 * rule nothing can execute in a test is a rule that drifts. Everything here is
 * pure — same inputs, same outputs, no DOM, no fetch — so `npm test` can run it
 * under node's own test runner.
 *
 * This is the FOURTH implementation of one rule. The other three are
 * `scifor.locations` (a predicate over for_each combos), `scistackplot.reduce`
 * (a pandas mask) and `scistackplot.codegen` (that mask, emitted as source).
 * The meaning is stated once in docs/claude/location-filter-semantics.md and
 * the cases every implementation is tested against are the JSON file beside
 * it, which `locationSelection.test.ts` loads rather than transcribes.
 *
 * Three rules live here, and each has a wrong-but-plausible version:
 *
 * - **A selection is a PAIR.** `include` is a minimal covering set of prefixes
 *   — a set of PLACES, possibly ragged. `exclude_levels` is a standing RULE
 *   per key. Ticking every trial of a subject and ticking the subject must
 *   store the same thing, or a trial added tomorrow lands inside one selection
 *   and outside the other; and unticking a whole SESSION must keep applying to
 *   subjects that do not exist yet, which prefixes cannot express.
 * - **Neither pane stores its checkbox state.** Both are derived from the pair
 *   by the coverage walk below, so the tree and the by-key list cannot
 *   disagree about what is selected.
 * - **Picking one location does not restyle the figure.** It moves schema keys
 *   off AGGREGATE, because collapsing to a mean is the one thing that hides
 *   what the picker was opened to look at — and leaves X/COLOR/FACET alone,
 *   because those already draw every level.
 */

/** One `[key, value]` step of a location path. */
export type PathStep = [string, string]
/** A prefix: the outermost steps of a location. `[]` would mean "everything". */
export type Prefix = PathStep[]

/**
 * What the picker holds, in the spec's own wire shape.
 *
 * Snake_case on purpose: this IS `spec.location_filter`, and a camelCase
 * mirror would be one more translation to keep in step with the Python
 * dataclass.
 */
export interface LocationSelection {
  include: Prefix[]
  exclude_levels: Record<string, string[]>
}

export const EMPTY_SELECTION: LocationSelection = { include: [], exclude_levels: {} }

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

// --- values -----------------------------------------------------------------

const INTEGRAL = /^-?(?:0|[1-9]\d*)(?:\.0+)?$/

/**
 * Every text form a value may legitimately arrive as.
 *
 * The transcription of `scifor.locations.value_spellings` /
 * `scistackplot.spec.value_spellings`. A schema key that round-tripped through
 * DuckDB as a float reaches the plotting frame as `1.0` while this tree —
 * built by scidb — says `1`; compared as raw text they select nothing, in
 * silence. So an integral number matches both spellings.
 *
 * A zero-padded string is NOT a number here: `"01"` matches only `"01"`,
 * because `"01"` and `"1"` can be two genuinely distinct trials and which one
 * is identity is scidb's decision, never a shortcut taken in a comparison.
 */
export function valueSpellings(value: string): string[] {
  const text = String(value)
  if (!INTEGRAL.test(text)) return [text]
  const whole = Math.trunc(Number(text))
  return Array.from(new Set([text, String(whole), `${whole}.0`])).sort()
}

/** Whether two spellings name one value. */
export function sameValue(a: string, b: string): boolean {
  return a === b || valueSpellings(b).includes(String(a))
}

// --- prefix algebra ---------------------------------------------------------

const same = (a: PathStep, b: PathStep) =>
  a !== undefined && b !== undefined && a[0] === b[0] && sameValue(b[1], a[1])

/**
 * Is `a` a prefix of (or equal to) `b`?
 *
 * Matched by KEY in order, not by position: a prefix may SKIP a key, because a
 * selection can be cross-cutting — `[subject=02, trial=3]` names a location
 * whose path also carries a `session`, and a saved record need not fill the
 * schema at all (docs/claude/schema-hierarchy-contiguity.md). Read
 * positionally, `trial=3` would be compared against the path's `session` step,
 * fail, and select nothing in silence. Every step of `a` must still be found,
 * so a deeper prefix never covers a shallower path.
 */
export function covers(a: Prefix, b: Prefix): boolean {
  let at = 0
  for (const step of a) {
    while (at < b.length && !same(step, b[at])) at++
    if (at >= b.length) return false
    at++
  }
  return true
}

export type Coverage = 'full' | 'partial' | 'none'

/** Every schema key this tree actually has a node for. */
export function keysOf(roots: LocationNode[]): Set<string> {
  const keys = new Set<string>()
  const visit = (node: LocationNode) => {
    keys.add(node.key)
    node.children.forEach(visit)
  }
  roots.forEach(visit)
  return keys
}

/** Read a selection out of a spec, tolerating every partial shape. */
export function asSelection(raw: unknown): LocationSelection {
  const value = (raw ?? {}) as Partial<LocationSelection>
  return {
    include: Array.isArray(value.include) ? value.include : [],
    exclude_levels: value.exclude_levels ?? {},
  }
}

export function isInert(selection: LocationSelection): boolean {
  return (
    selection.include.length === 0 &&
    Object.values(selection.exclude_levels).every(values => values.length === 0)
  )
}

/**
 * The selection as it applies to THIS tree.
 *
 * A spec's location filter is one object shared by every variant row, and
 * rows may name different variables — so it can carry steps for a key this
 * variable was never saved at. The Python sides let such a step constrain
 * nothing (`if _k in df.columns`), which makes a prefix naming only absent
 * keys vacuously true, i.e. the whole `include` inert. Reproduced here rather
 * than reinvented: without it, opening the picker on a shallower variable
 * would show every box unticked beside a figure drawing everything.
 *
 * Edits are made to what this returns, so touching the picker drops the
 * foreign steps — keeping a vacuous prefix would silently neutralise every
 * subsequent tick.
 */
export function visibleSelection(
  roots: LocationNode[],
  selection: LocationSelection
): LocationSelection {
  const keys = keysOf(roots)
  const include: Prefix[] = []
  for (const prefix of selection.include) {
    const local = prefix.filter(([key]) => keys.has(key))
    // Constrains nothing this tree can show ⇒ the union covers everything.
    if (local.length === 0) return { include: [], exclude_levels: selection.exclude_levels }
    include.push(local)
  }
  const exclude_levels: Record<string, string[]> = {}
  for (const [key, values] of Object.entries(selection.exclude_levels)) {
    if (values.length > 0) exclude_levels[key] = values
  }
  return { include, exclude_levels }
}

/** Is any step of `path` named by a level rule? */
function excludedOnPath(
  path: Prefix,
  excluded: Record<string, string[]>
): boolean {
  return path.some(([key, value]) =>
    (excluded[key] ?? []).some(level => sameValue(value, level))
  )
}

/**
 * How much of this node's subtree the selection holds.
 *
 * `full` also means "draw the checkbox ticked", `partial` "indeterminate" —
 * the tri-state is read off the selection rather than stored beside it, so the
 * boxes cannot disagree with what will be plotted.
 *
 * A covering prefix no longer short-circuits to `full`: a level rule can hole
 * out a subtree the prefix covers, and that hole is exactly what the amber
 * state exists to show.
 */
export function coverageOf(
  node: LocationNode,
  selection: LocationSelection
): Coverage {
  if (excludedOnPath(node.path, selection.exclude_levels)) return 'none'
  const covered =
    selection.include.length === 0 ||
    selection.include.some(p => covers(p, node.path))
  if (node.children.length === 0) return covered ? 'full' : 'none'
  const kids = node.children.map(child => coverageOf(child, selection))
  if (kids.every(k => k === 'full')) return 'full'
  if (kids.every(k => k === 'none')) return 'none'
  return 'partial'
}

/**
 * Re-derive the minimal covering set from a coverage walk.
 *
 * Everything selected is stored as NOTHING selected: an empty `include` is the
 * inert state, which is also what an untouched picker means, so "select all"
 * and "never touched it" cannot produce two different specs for one figure.
 *
 * Level rules are deliberately NOT consulted: they are a separate clause
 * applied after coverage, and folding an omitted session into the prefixes
 * would freeze it against today's subjects — the drift the rule form exists
 * to avoid.
 */
export function normalize(roots: LocationNode[], include: Prefix[]): Prefix[] {
  const asPlaces: LocationSelection = { include, exclude_levels: {} }
  if (roots.length > 0 && roots.every(r => coverageOf(r, asPlaces) === 'full')) {
    return []
  }
  const out: Prefix[] = []
  const visit = (node: LocationNode) => {
    const cov = coverageOf(node, asPlaces)
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
  selection: LocationSelection,
  path: Prefix
): LocationSelection {
  const include = selection.include
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
  // The last place is gone. An empty `include` would read as EVERYTHING, so
  // the untick of the last box would re-tick them all.
  if (next.length === 0) return nothingSelected(roots, selection)
  return { ...selection, include: normalize(roots, next) }
}

/**
 * "Nothing selected", in the stored form: every top-level value omitted.
 *
 * `include` cannot say it — empty means everything — so it is said with the
 * level rules, which every implementation already honours ("excluding every
 * level of a key selects nothing", location-filter-semantics.md). Rules name
 * the values that exist NOW: a top-level value saved later is not omitted.
 * That reach only lasts while nothing is ticked; the next tick lifts the rule
 * it overrides and stores places again (`withNodes`).
 */
function nothingSelected(
  roots: LocationNode[],
  selection: LocationSelection
): LocationSelection {
  let next: LocationSelection = { ...selection, include: [] }
  for (const root of roots) next = withoutLevel(next, root.key, root.value)
  return next
}

/** The node at exactly `path`, if this tree has one. */
function nodeAt(roots: LocationNode[], path: Prefix): LocationNode | undefined {
  let level = roots
  let node: LocationNode | undefined
  for (const step of path) {
    node = level.find(n => same(n.path[n.path.length - 1], step))
    if (!node) return undefined
    level = node.children
  }
  return node
}

/**
 * Turn these locations fully on — BOTH halves of the selection.
 *
 * The one owner of "tick", for both panes. A box can be unticked or
 * indeterminate for either of two reasons: a place missing from `include`
 * (a right-pane untick) or a level rule (a left-pane untick) — and the rule
 * need not be on the clicked box's own key: omitting trial 1 leaves every
 * subject indeterminate. A tick that repaired only one half was a click that
 * visibly did nothing, which is how indeterminate boxes came to ignore clicks.
 *
 * A rule that reaches into the targets cannot stay a rule — the stored form
 * cannot say "trial 1 is out, except under subject 1". So it is lifted, and
 * what it still omitted OUTSIDE the targets is written down as places. That
 * trades the rule's reach into data that does not exist yet for a click that
 * does what it shows, and only for the rule the user just overrode.
 */
function withNodes(
  roots: LocationNode[],
  selection: LocationSelection,
  targets: LocationNode[]
): LocationSelection {
  const inTargets = (path: Prefix) => targets.some(t => covers(t.path, path))

  // Every rule step found on a target, its ancestors, or its descendants.
  const lifted: PathStep[] = []
  const collect = (node: LocationNode) => {
    for (const step of node.path) {
      const [key, value] = step
      const ruled = (selection.exclude_levels[key] ?? []).some(level => sameValue(value, level))
      if (ruled && !lifted.some(s => same(s, step))) lifted.push(step)
    }
    node.children.forEach(collect)
  }
  targets.forEach(collect)

  if (lifted.length === 0) {
    // Nothing stored means EVERYTHING is selected, so there is nothing to add.
    // All targets go in before one normalize: adding them one at a time could
    // collapse to the inert state midway and then narrow the whole figure to
    // the single location it happened to add next.
    if (selection.include.length === 0) return selection
    const include = selection.include.filter(p => !targets.some(t => covers(t.path, p)))
    for (const target of targets) include.push(target.path)
    return { ...selection, include: normalize(roots, include) }
  }

  const exclude_levels: Record<string, string[]> = {}
  for (const [key, values] of Object.entries(selection.exclude_levels)) {
    const kept = values.filter(v => !lifted.some(s => same(s, [key, v])))
    if (kept.length > 0) exclude_levels[key] = kept
  }
  // Rebuilt from the leaves it must end up covering, never by chaining
  // `without`: removing a ruled ancestor can pass through an empty `include`,
  // which means EVERYTHING, and the figure would silently widen to all of it.
  // Targets always have leaves, so the rebuilt set is never empty.
  const onPath = (path: Prefix) => lifted.some(s => path.some(step => same(s, step)))
  const placed = (path: Prefix) =>
    selection.include.length === 0 || selection.include.some(p => covers(p, path))
  const keep: Prefix[] = []
  const visit = (node: LocationNode) => {
    if (node.children.length > 0) { node.children.forEach(visit); return }
    if (inTargets(node.path) || (!onPath(node.path) && placed(node.path))) keep.push(node.path)
  }
  roots.forEach(visit)
  return { include: normalize(roots, keep), exclude_levels }
}

/** Add one location to the selection — the right pane's tick. */
export function withPath(
  roots: LocationNode[],
  selection: LocationSelection,
  path: Prefix
): LocationSelection {
  const node = nodeAt(roots, path)
  if (node) return withNodes(roots, selection, [node])
  // Not a place this tree has (e.g. a picked path from elsewhere): only the
  // place half can apply.
  if (selection.include.length === 0) return selection
  const next = selection.include.filter(p => !covers(path, p))
  next.push(path)
  return { ...selection, include: normalize(roots, next) }
}

// --- the by-key pane --------------------------------------------------------

/**
 * Each schema key and the levels this tree has for it, in tree order.
 *
 * Only levels PRESENT for this variable and variant: a level with no data
 * here cannot be omitted from a figure that was never going to draw it, and
 * showing it would invite exactly that click.
 */
export function levelsByKey(roots: LocationNode[]): Record<string, string[]> {
  const out: Record<string, string[]> = {}
  const visit = (node: LocationNode) => {
    const seen = (out[node.key] ??= [])
    if (!seen.some(value => sameValue(value, node.value))) seen.push(node.value)
    node.children.forEach(visit)
  }
  roots.forEach(visit)
  return out
}

/** Every node whose own step is `(key, value)`; they all sit at one depth. */
function nodesAtLevel(
  roots: LocationNode[],
  key: string,
  value: string
): LocationNode[] {
  const found: LocationNode[] = []
  const visit = (node: LocationNode) => {
    if (node.key === key) {
      if (sameValue(node.value, value)) found.push(node)
      return
    }
    node.children.forEach(visit)
  }
  roots.forEach(visit)
  return found
}

function rollUp(states: Coverage[]): Coverage {
  if (states.length === 0) return 'none'
  if (states.every(s => s === 'full')) return 'full'
  if (states.every(s => s === 'none')) return 'none'
  return 'partial'
}

/**
 * The by-key pane's state for one level, rolled up over every location that
 * has it.
 *
 * This is what makes the two panes one control: tick "subject 01" on the left
 * and every subject-01 node on the right is ticked; untick one of its trials
 * on the right and this turns `partial` (amber) rather than lying in either
 * direction.
 */
export function levelCoverage(
  roots: LocationNode[],
  selection: LocationSelection,
  key: string,
  value: string
): Coverage {
  return rollUp(
    nodesAtLevel(roots, key, value).map(node => coverageOf(node, selection))
  )
}

/** The same roll-up one step further: a key, over all of its levels. */
export function keyCoverage(
  roots: LocationNode[],
  selection: LocationSelection,
  key: string
): Coverage {
  const levels = levelsByKey(roots)[key] ?? []
  return rollUp(levels.map(value => levelCoverage(roots, selection, key, value)))
}

/** Omit one level everywhere, now and in data that does not exist yet. */
export function withoutLevel(
  selection: LocationSelection,
  key: string,
  value: string
): LocationSelection {
  const current = selection.exclude_levels[key] ?? []
  if (current.some(level => sameValue(value, level))) return selection
  return {
    ...selection,
    exclude_levels: { ...selection.exclude_levels, [key]: [...current, value] },
  }
}

/**
 * Put one level back — every location that has it, fully on.
 *
 * Clearing its own rule is not enough: a ragged untick on the right pane may
 * have removed some of its locations from `include`, and a rule on ANOTHER key
 * (trial 1 omitted) holes out every subject. Both are repaired by `withNodes`,
 * the same tick the right pane uses.
 */
export function withLevel(
  roots: LocationNode[],
  selection: LocationSelection,
  key: string,
  value: string
): LocationSelection {
  // The level's own rule goes even if this tree has no node for it.
  const exclude_levels = { ...selection.exclude_levels }
  const kept = (exclude_levels[key] ?? []).filter(level => !sameValue(value, level))
  if (kept.length > 0) exclude_levels[key] = kept
  else delete exclude_levels[key]
  return withNodes(roots, { ...selection, exclude_levels }, nodesAtLevel(roots, key, value))
}

/**
 * One click on a level: full turns it off, anything else turns it on.
 *
 * Amber resolves to ON rather than OFF so that one click is always
 * recoverable by a second — the alternative loses a ragged selection and
 * cannot give it back.
 */
export function toggleLevel(
  roots: LocationNode[],
  selection: LocationSelection,
  key: string,
  value: string
): LocationSelection {
  return levelCoverage(roots, selection, key, value) === 'full'
    ? withoutLevel(selection, key, value)
    : withLevel(roots, selection, key, value)
}

/** One click on a key: every level of it, in the same direction. */
export function toggleKey(
  roots: LocationNode[],
  selection: LocationSelection,
  key: string
): LocationSelection {
  const turningOff = keyCoverage(roots, selection, key) === 'full'
  let next = selection
  for (const value of levelsByKey(roots)[key] ?? []) {
    next = turningOff
      ? withoutLevel(next, key, value)
      : withLevel(roots, next, key, value)
  }
  return next
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

export type Role = 'group' | 'facet' | 'iterate' | 'collapse'

/**
 * The roles a spec should hold once one location has been picked.
 *
 * Picking `subject=01` leaves `trial` unanswered, and a trial factor sitting on
 * COLLAPSE would average the very thing the picker was opened to look at —
 * every trial's own values. So any schema key the selection does NOT name is
 * moved off COLLAPSE onto ITERATE (separate figures — the visible default for
 * an unassigned factor), and one with no role at all gets ITERATE too.
 *
 * Deliberately NOT a blanket reassignment: a key already grouping or faceting
 * is drawing its levels individually, which is what was asked for, and
 * stamping over a chosen grouping because a location was clicked would be
 * astonishing.
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
    if (current === undefined || current === 'collapse') next[key] = 'iterate'
  }
  return next
}

/**
 * The button label for a selection: what the picker would say if it were open.
 *
 * One selection reads as the location itself, because that is the common case
 * and "1 location" would be a worse answer than naming it. Omitted levels are
 * counted beside it — a rule that changes what the figure draws must be
 * visible without opening the dialog.
 */
export function describeSelection(selection: LocationSelection): string {
  const { include, exclude_levels } = asSelection(selection)
  const omitted = Object.entries(exclude_levels).filter(([, v]) => v.length > 0)
  const places =
    include.length === 0
      ? 'All locations'
      : include.length === 1
        ? include[0].map(([key, value]) => `${key}=${value}`).join(' / ')
        : `${include.length} locations`
  const suffix = omitted.length
    ? ` (${omitted.map(([key, values]) => `−${values.length} ${key}`).join(', ')})`
    : ''
  return `${places}${suffix}…`
}
