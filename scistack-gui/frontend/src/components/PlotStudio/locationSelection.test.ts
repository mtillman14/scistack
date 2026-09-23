/**
 * The schema location picker's rules.
 *
 * Run by `npm test` under node's own test runner, against the React-free module
 * only — the same split `extension/tsconfig.test.json` makes for its vscode-free
 * modules, and for the same reason: a rule nothing can execute is a rule that
 * drifts.
 *
 * The PARITY section loads docs/claude/location-filter-cases.json — the same
 * file `scifor/tests/test_locations.py` and
 * `scistackplot/tests/test_location_filter.py` load. This module is the fourth
 * implementation of one rule, and the only one the user actually clicks on, so
 * a case it fails is a picker whose checkboxes disagree with the figure.
 *
 * The fixture is a [subject, trial] study shaped like the Python one:
 *
 *   S01  t1 green   t2 amber   t3 red
 *   S02  t1 grey (excluded)    t2 green
 */

import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'

import {
  type LocationNode,
  type LocationSelection,
  type Prefix,
  EMPTY_SELECTION,
  asSelection,
  coverageOf,
  covers,
  describeSelection,
  hasProblem,
  keyCoverage,
  levelCoverage,
  levelsByKey,
  matchesQuery,
  normalize,
  rolesAfterPick,
  toggleKey,
  toggleLevel,
  valueSpellings,
  visibleSelection,
  without,
  withoutLevel,
  withPath,
} from './locationSelection.js'

type State = 'green' | 'amber' | 'red' | 'grey'

function leaf(subject: string, trial: string, state: State): LocationNode {
  const counts = { green: 0, amber: 0, red: 0, grey: 0 }
  counts[state] = 1
  return {
    key: 'trial',
    value: trial,
    state,
    counts,
    is_leaf: true,
    path: [['subject', subject], ['trial', trial]],
    schema_id: null,
    record_id: null,
    code_version: null,
    children: [],
  }
}

function subject(value: string, children: LocationNode[]): LocationNode {
  const counts = { green: 0, amber: 0, red: 0, grey: 0 }
  for (const child of children) {
    for (const key of ['green', 'amber', 'red', 'grey'] as const) {
      counts[key] += child.counts[key]
    }
  }
  return {
    key: 'subject',
    value,
    state: counts.red ? 'red' : counts.amber ? 'amber' : counts.green ? 'green' : 'grey',
    counts,
    is_leaf: false,
    path: [['subject', value]],
    schema_id: null,
    record_id: null,
    code_version: null,
    children,
  }
}

const roots: LocationNode[] = [
  subject('S01', [
    leaf('S01', 't1', 'green'),
    leaf('S01', 't2', 'amber'),
    leaf('S01', 't3', 'red'),
  ]),
  subject('S02', [leaf('S02', 't1', 'grey'), leaf('S02', 't2', 'green')]),
]

const S01: Prefix = [['subject', 'S01']]
const S01_T2: Prefix = [['subject', 'S01'], ['trial', 't2']]
const S02: Prefix = [['subject', 'S02']]

/** A selection, spelled short. */
function sel(
  include: Prefix[] = [],
  exclude_levels: Record<string, string[]> = {}
): LocationSelection {
  return { include, exclude_levels }
}

// --- prefixes ---------------------------------------------------------------

test('a prefix covers itself and everything beneath it', () => {
  assert.equal(covers(S01, S01), true)
  assert.equal(covers(S01, S01_T2), true)
  assert.equal(covers(S01_T2, S01), false, 'a deeper path does not cover a shallower one')
  assert.equal(covers(S02, S01_T2), false)
})

test('a path with the same VALUE under a different KEY is not covered', () => {
  // Non-contiguous locations are why prefixes carry key names at all; the
  // algebra has to respect that too, or a `speed=t2` would match `trial=t2`.
  const elsewhere: Prefix = [['subject', 'S01'], ['speed', 't2']]
  assert.equal(covers(S01_T2, elsewhere), false)
})

// --- coverage / tri-state ---------------------------------------------------

test('an empty selection covers EVERYTHING', () => {
  // Changed deliberately when the by-key pane arrived. The inert selection
  // means "everything is drawn", so the boxes must show it: leaving them
  // unticked made the left pane read "nothing selected" over a figure drawing
  // every location, and gave the user no green state to click away from.
  assert.equal(coverageOf(roots[0], sel([])), 'full')
  assert.equal(keyCoverage(roots, sel([]), 'subject'), 'full')
})

test('a subject prefix fully covers its subtree', () => {
  assert.equal(coverageOf(roots[0], sel([S01])), 'full')
  assert.equal(coverageOf(roots[0].children[0], sel([S01])), 'full')
  assert.equal(coverageOf(roots[1], sel([S01])), 'none')
})

test('a partially selected subject is indeterminate', () => {
  assert.equal(coverageOf(roots[0], sel([S01_T2])), 'partial')
})

test('every child selected reads as a full parent', () => {
  const all = roots[0].children.map(c => c.path)
  assert.equal(coverageOf(roots[0], sel(all)), 'full')
})

// --- the minimal covering set ----------------------------------------------

test('unticking every trial of a subject stores the same thing as unticking it', () => {
  // The load-bearing one. If these differed, a trial added to S01 tomorrow
  // would be inside one selection and outside the other.
  //
  // Phrased as UNTICKING because that is the reachable direction: the inert
  // selection already holds everything, so there is no "nothing selected"
  // state to tick up from.
  const byTrials = roots[1].children.reduce<LocationSelection>(
    (acc, child) => without(roots, acc, child.path),
    sel([])
  )
  const bySubject = without(roots, sel([]), S02)
  assert.deepEqual(byTrials.include, bySubject.include)
  assert.deepEqual(bySubject.include, [S01])
})

test('ticking a location while everything is selected changes nothing', () => {
  // `include: []` is everything, so adding to it must be a no-op rather than
  // a narrowing. Getting this backwards turned "everything" into "only the
  // location just clicked".
  assert.deepEqual(withPath(roots, sel([]), S01_T2), sel([]))
})

test('selecting everything collapses to the inert empty selection', () => {
  const all = withPath(roots, withPath(roots, sel([]), S01), S02)
  assert.deepEqual(all.include, [], 'everything selected is stored as nothing selected')
})

test('normalize collapses a fully covered parent', () => {
  const leaves = roots[0].children.map(c => c.path)
  assert.deepEqual(normalize(roots, leaves), [S01])
})

// --- unticking, and the explosion it forces ---------------------------------

test('unticking one trial of a ticked subject materialises its siblings', () => {
  const next = without(roots, sel([S01]), S01_T2)

  assert.deepEqual(next.include, [
    [['subject', 'S01'], ['trial', 't1']],
    [['subject', 'S01'], ['trial', 't3']],
  ])
  assert.equal(coverageOf(roots[0], next), 'partial')
})

test('unticking from the ALL state explodes the whole tree', () => {
  // `include: []` means everything, so the explosion has to start at the
  // virtual root — otherwise unticking one trial would silently select nothing.
  const next = without(roots, sel([]), S01_T2)

  assert.equal(coverageOf(roots[1], next), 'full', 'the other subject survives whole')
  assert.equal(coverageOf(roots[0], next), 'partial')
  assert.deepEqual(
    next.include.filter(p => p[0][1] === 'S01').map(p => p[1][1]),
    ['t1', 't3']
  )
})

test('unticking a whole subject leaves the other one', () => {
  const next = without(roots, sel([]), S01)
  assert.deepEqual(next.include, [S02])
})

test('unticking the last selection leaves nothing selected', () => {
  // Not `include: []` alone — that is EVERYTHING, and the last untick used to
  // re-tick every box. Nothing is every top-level value omitted.
  const next = without(roots, sel([S01]), S01)
  assert.deepEqual(next, sel([], { subject: ['S01', 'S02'] }))
  for (const node of roots) assert.equal(coverageOf(node, next), 'none')
})

test('ticking out of the nothing state selects only what was ticked', () => {
  const nothing = without(roots, sel([S01]), S01)
  const next = withPath(roots, nothing, S01_T2)
  assert.equal(coverageOf(roots[0].children[1], next), 'full')
  for (const leafNode of [roots[0].children[0], roots[0].children[2], ...roots[1].children]) {
    assert.equal(coverageOf(leafNode, next), 'none', JSON.stringify(leafNode.path))
  }
})

test('re-ticking an exploded sibling set collapses it again', () => {
  const exploded = without(roots, sel([S01]), S01_T2)
  const restored = withPath(roots, exploded, S01_T2)
  assert.deepEqual(restored.include, [S01], 'back to one entry, not three')
})

test('a ragged edit leaves the level rules alone', () => {
  // The two clauses are independent. Unticking a trial must not quietly
  // resurrect a session the user omitted.
  const next = without(roots, sel([S01], { trial: ['t9'] }), S01_T2)
  assert.deepEqual(next.exclude_levels, { trial: ['t9'] })
})

// --- level rules ------------------------------------------------------------

test('an omitted level reads as unticked wherever it appears', () => {
  const omitted = sel([], { trial: ['t2'] })
  assert.equal(levelCoverage(roots, omitted, 'trial', 't2'), 'none')
  assert.equal(levelCoverage(roots, omitted, 'trial', 't1'), 'full')
  // It holes out BOTH subjects, which is the whole point of a rule.
  assert.equal(coverageOf(roots[0], omitted), 'partial')
  assert.equal(coverageOf(roots[1], omitted), 'partial')
})

test('a covering prefix does not hide a hole punched by a rule', () => {
  // The reason coverageOf no longer short-circuits on a covering prefix: the
  // subject is ticked, one of its trials is omitted, and the box must say so.
  assert.equal(coverageOf(roots[0], sel([S01], { trial: ['t2'] })), 'partial')
})

test('the by-key pane turns amber when the tree disagrees with itself', () => {
  // The user's rule: tick S01 on the left, untick one of its trials on the
  // right, and the left entry goes mixed rather than lying either way.
  const ragged = without(roots, sel([]), S01_T2)
  assert.equal(levelCoverage(roots, ragged, 'subject', 'S01'), 'partial')
  assert.equal(levelCoverage(roots, ragged, 'subject', 'S02'), 'full')
  assert.equal(keyCoverage(roots, ragged, 'subject'), 'partial')
})

test('clicking a full level omits it; clicking it again puts it back', () => {
  const off = toggleLevel(roots, sel([]), 'trial', 't2')
  assert.deepEqual(off.exclude_levels, { trial: ['t2'] })

  const on = toggleLevel(roots, off, 'trial', 't2')
  assert.deepEqual(on.exclude_levels, {}, 'the key goes away, not an empty list')
  assert.equal(levelCoverage(roots, on, 'trial', 't2'), 'full')
})

test('clicking an AMBER level turns it fully on, not off', () => {
  // One click must always be recoverable by a second. Resolving amber to OFF
  // would discard the ragged selection with no way back.
  const ragged = without(roots, sel([]), S01_T2)
  assert.equal(levelCoverage(roots, ragged, 'subject', 'S01'), 'partial')

  const next = toggleLevel(roots, ragged, 'subject', 'S01')
  assert.equal(levelCoverage(roots, next, 'subject', 'S01'), 'full')
})

test('putting a level back repairs BOTH halves of the selection', () => {
  // The rule is cleared AND the locations a ragged untick removed are
  // re-ticked; clearing only the rule would be a control that does nothing.
  const ragged = withoutLevel(without(roots, sel([]), S01_T2), 'trial', 't2')
  assert.equal(levelCoverage(roots, ragged, 'trial', 't2'), 'none')

  const restored = toggleLevel(roots, ragged, 'trial', 't2')
  assert.deepEqual(restored.exclude_levels, {})
  assert.equal(levelCoverage(roots, restored, 'trial', 't2'), 'full')
})

test('toggling a key moves every level of it', () => {
  const off = toggleKey(roots, sel([]), 'trial')
  assert.deepEqual(off.exclude_levels.trial, ['t1', 't2', 't3'])
  assert.equal(keyCoverage(roots, off, 'trial'), 'none')

  const on = toggleKey(roots, off, 'trial')
  assert.deepEqual(on.exclude_levels, {})
})

test('the by-key pane lists only levels this tree has', () => {
  // Levels present for THIS variable and variant: a level with no data here
  // cannot be omitted from a figure that was never going to draw it.
  assert.deepEqual(levelsByKey(roots), {
    subject: ['S01', 'S02'],
    trial: ['t1', 't2', 't3'],
  })
})

// --- a filter that names keys this tree does not have ------------------------

test('a prefix naming only foreign keys is inert, not empty', () => {
  // A spec's filter is shared by every variant row, and rows may name
  // different variables. The Python sides let such a step constrain nothing,
  // so the picker must not show every box unticked beside a figure drawing
  // everything.
  const foreign = sel([[['speed', 'SSV']]])
  const visible = visibleSelection(roots, foreign)

  assert.deepEqual(visible.include, [])
  assert.equal(coverageOf(roots[0], visible), 'full')
})

test('a foreign STEP inside a local prefix is dropped, and the rest holds', () => {
  const mixed = sel([[['subject', 'S01'], ['speed', 'SSV']]])
  const visible = visibleSelection(roots, mixed)

  assert.deepEqual(visible.include, [S01])
  assert.equal(coverageOf(roots[0], visible), 'full')
  assert.equal(coverageOf(roots[1], visible), 'none')
})

test('a rule naming a key this tree lacks drops nothing', () => {
  const visible = visibleSelection(roots, sel([], { speed: ['SSV'] }))
  assert.equal(coverageOf(roots[0], visible), 'full')
})

// --- value spellings --------------------------------------------------------

test('an integral number matches both of its spellings', () => {
  // A schema key that round-tripped through DuckDB as a float reaches the
  // frame as 1.0 while this tree says 1; raw text comparison selects nothing,
  // in silence.
  assert.deepEqual(valueSpellings('1'), ['1', '1.0'])
  assert.deepEqual(valueSpellings('1.0'), ['1', '1.0'])
  assert.deepEqual(valueSpellings('-3'), ['-3', '-3.0'])
})

test('a zero-padded value is only itself', () => {
  // "01" and "1" can be two genuinely distinct trials, and which spelling is
  // identity is scidb's decision, not a comparison shortcut.
  assert.deepEqual(valueSpellings('01'), ['01'])
})

test('a non-integral number is only itself', () => {
  assert.deepEqual(valueSpellings('1.5'), ['1.5'])
})

// --- search and problems ----------------------------------------------------

test('a parent matches when a descendant does', () => {
  assert.equal(matchesQuery(roots[0], 't3'), true)
  assert.equal(matchesQuery(roots[1], 't3'), false)
})

test('an empty query matches everything', () => {
  assert.equal(matchesQuery(roots[1], ''), true)
})

test('excluded is not a problem', () => {
  // Mirrors scidb.locations.prune_to_problems: an exclusion is a decision the
  // user already made and justified.
  assert.equal(hasProblem(roots[1]), false, 'S02 holds only green + excluded')
  assert.equal(hasProblem(roots[0]), true)
})

// --- roles after a pick -----------------------------------------------------

const KEYS = ['subject', 'trial']

test('an unanswered key moves off collapse onto separate figures', () => {
  const roles = rolesAfterPick({ trial: 'collapse' }, S01, KEYS)
  assert.equal(roles.trial, 'iterate', 'a mean would hide what the picker was opened for')
})

test('an unanswered key with no role separates figures', () => {
  assert.equal(rolesAfterPick({}, S01, KEYS).trial, 'iterate')
})

test('a chosen grouping is never stamped over', () => {
  // The refinement that keeps "as granular as possible" from becoming "reset
  // my figure": a grouped or faceted key already draws every level.
  for (const role of ['group', 'facet'] as const) {
    assert.equal(rolesAfterPick({ trial: role }, S01, KEYS).trial, role)
  }
})

test('a key the selection NAMES is left alone entirely', () => {
  const roles = rolesAfterPick({ subject: 'collapse' }, S01, KEYS)
  assert.equal(roles.subject, 'collapse', 'the selection answered it; it is one level')
})

test('it does not mutate the roles it was given', () => {
  const before = { trial: 'collapse' as const }
  rolesAfterPick(before, S01, KEYS)
  assert.equal(before.trial, 'collapse')
})

// --- the button label -------------------------------------------------------

test('the label names a single location rather than counting it', () => {
  assert.equal(describeSelection(sel([S01_T2])), 'subject=S01 / trial=t2…')
  assert.equal(describeSelection(EMPTY_SELECTION), 'All locations…')
  assert.equal(describeSelection(sel([S01, S02])), '2 locations…')
})

test('the label says what is omitted, without opening the dialog', () => {
  // A standing rule changes what the figure draws; a button reading "All
  // locations…" over a figure missing a session would be a lie.
  assert.equal(
    describeSelection(sel([], { trial: ['t2', 't3'] })),
    'All locations (−2 trial)…'
  )
  assert.equal(
    describeSelection(sel([S01], { trial: ['t2'] })),
    'subject=S01 (−1 trial)…'
  )
})

// --- the shared parity cases ------------------------------------------------
//
// Loaded, never transcribed. See the module docstring.

interface SharedCase {
  id: number
  name: string
  filter: { include: Prefix[]; exclude_levels: Record<string, string[]> }
  location: Record<string, unknown>
  in: boolean
  why: string
  applies_to: string[]
}

/**
 * Find the shared case file by walking up from this module.
 *
 * `npm test` runs the COMPILED file out of `dist/test`, which sits at a
 * different depth than the source, so a fixed `../../..` would work in exactly
 * one of the two places.
 */
function casesFile(): string {
  let dir = dirname(fileURLToPath(import.meta.url))
  for (let up = 0; up < 10; up++) {
    const candidate = join(dir, 'docs', 'claude', 'location-filter-cases.json')
    try {
      readFileSync(candidate)
      return candidate
    } catch {
      dir = dirname(dir)
    }
  }
  throw new Error(
    'Shared location-filter cases not found by walking up from ' +
      fileURLToPath(import.meta.url) +
      '. Every implementation of the rule reads docs/claude/location-filter-cases.json.'
  )
}

const sharedCases: SharedCase[] = (
  JSON.parse(readFileSync(casesFile(), 'utf8')).cases as SharedCase[]
).filter(c => c.applies_to.includes('tree'))

/** The case's location as a single chain of nodes, one per key. */
function chainFor(location: Record<string, unknown>): LocationNode[] {
  const steps: Prefix = Object.entries(location).map(([key, value]) => [
    key,
    String(value),
  ])
  let node: LocationNode | null = null
  for (let depth = steps.length - 1; depth >= 0; depth--) {
    const [key, value] = steps[depth]
    const counts = { green: 1, amber: 0, red: 0, grey: 0 }
    const built: LocationNode = {
      key,
      value,
      state: 'green',
      counts,
      is_leaf: node === null,
      path: steps.slice(0, depth + 1),
      schema_id: null,
      record_id: null,
      code_version: null,
      children: node ? [node] : [],
    }
    node = built
  }
  return node ? [node] : []
}

for (const shared of sharedCases) {
  test(`shared case ${shared.id}: ${shared.name}`, () => {
    const chain = chainFor(shared.location)
    const selection = visibleSelection(chain, asSelection(shared.filter))
    // The leaf is the location itself; its checkbox is the answer.
    let leafNode = chain[0]
    while (leafNode.children.length > 0) leafNode = leafNode.children[0]

    assert.equal(
      coverageOf(leafNode, selection) === 'full',
      shared.in,
      shared.why
    )
  })
}

test('the shared file actually produced cases', () => {
  // A loader that silently found nothing would turn the whole parity suite
  // green while testing exactly zero rules — the failure this file exists to
  // prevent, reproduced one level up.
  assert.ok(sharedCases.length >= 15, `only ${sharedCases.length} tree cases loaded`)
})

// --- every box responds -----------------------------------------------------
//
// Regression: an indeterminate box ignored clicks whenever the OTHER half of
// the selection had caused the gap — a right-pane tick never lifted a level
// rule, and a left-pane tick lifted only its own key's rule.

const S01_T1: Prefix = [['subject', 'S01'], ['trial', 't1']]

function allNodes(): LocationNode[] {
  const out: LocationNode[] = []
  const visit = (node: LocationNode) => { out.push(node); node.children.forEach(visit) }
  roots.forEach(visit)
  return out
}
const leaves = () => allNodes().filter(n => n.children.length === 0)

test('untick a trial on the right, re-tick its subject on the left', () => {
  const ragged = without(roots, sel([]), S01_T1)
  assert.equal(levelCoverage(roots, ragged, 'subject', 'S01'), 'partial')
  const next = toggleLevel(roots, ragged, 'subject', 'S01')
  assert.deepEqual(next, sel([]), 'everything back is the inert selection')
})

test('a right-pane tick lifts a left-pane rule inside it', () => {
  const ruled = toggleLevel(roots, sel([]), 'trial', 't2')
  assert.equal(coverageOf(roots[0], ruled), 'partial')

  const next = withPath(roots, ruled, S01)
  assert.equal(coverageOf(roots[0], next), 'full')
  // …and keeps omitting t2 where it was not overridden, now as a place.
  assert.equal(coverageOf(roots[1].children[1], next), 'none')
  assert.deepEqual(next.exclude_levels, {})
})

test('a right-pane tick undoes a left-pane untick of the same level', () => {
  const ruled = toggleLevel(roots, sel([]), 'subject', 'S01')
  assert.equal(coverageOf(roots[0], ruled), 'none')
  assert.deepEqual(withPath(roots, ruled, S01), sel([]))
})

test('a right-pane tick BELOW a ruled ancestor keeps the ancestor’s other children out', () => {
  const ruled = toggleLevel(roots, sel([]), 'subject', 'S01')
  const next = withPath(roots, ruled, S01_T2)
  assert.equal(coverageOf(roots[0].children[1], next), 'full')
  assert.equal(coverageOf(roots[0].children[0], next), 'none')
  assert.equal(coverageOf(roots[0].children[2], next), 'none')
  assert.equal(coverageOf(roots[1], next), 'full')
})

test('a left-pane tick lifts a rule on ANOTHER key', () => {
  // Omitting trial t1 leaves both subjects indeterminate on the left.
  const ruled = toggleLevel(roots, sel([]), 'trial', 't1')
  assert.equal(levelCoverage(roots, ruled, 'subject', 'S01'), 'partial')

  const next = toggleLevel(roots, ruled, 'subject', 'S01')
  assert.equal(levelCoverage(roots, next, 'subject', 'S01'), 'full')
  assert.equal(coverageOf(roots[1].children[0], next), 'none', 'S02/t1 still omitted')
  assert.equal(levelCoverage(roots, next, 'trial', 't1'), 'partial')
})

/** Every selection one or two clicks away from "everything", either pane. */
function reachable(): LocationSelection[] {
  const clicks = (s: LocationSelection): LocationSelection[] => [
    ...allNodes().map(n =>
      coverageOf(n, s) === 'full' ? without(roots, s, n.path) : withPath(roots, s, n.path)
    ),
    ...Object.entries(levelsByKey(roots)).flatMap(([key, values]) =>
      values.map(value => toggleLevel(roots, s, key, value))
    ),
  ]
  const once = clicks(sel([]))
  return [sel([]), ...once, ...once.flatMap(clicks)]
}

test('every right-pane box flips on one click, and touches nothing outside it', () => {
  for (const start of reachable()) {
    for (const node of allNodes()) {
      const before = coverageOf(node, start)
      const next =
        before === 'full' ? without(roots, start, node.path) : withPath(roots, start, node.path)
      const label = `${JSON.stringify(node.path)} from ${JSON.stringify(start)}`
      assert.equal(coverageOf(node, next), before === 'full' ? 'none' : 'full', label)
      for (const other of leaves().filter(l => !covers(node.path, l.path))) {
        assert.equal(coverageOf(other, next), coverageOf(other, start), `${label} moved ${JSON.stringify(other.path)}`)
      }
    }
  }
})

test('every left-pane box flips on one click, and touches nothing outside it', () => {
  for (const start of reachable()) {
    for (const [key, values] of Object.entries(levelsByKey(roots))) {
      for (const value of values) {
        const before = levelCoverage(roots, start, key, value)
        const next = toggleLevel(roots, start, key, value)
        const label = `${key}=${value} from ${JSON.stringify(start)}`
        assert.equal(levelCoverage(roots, next, key, value), before === 'full' ? 'none' : 'full', label)
        const has = (l: LocationNode) => l.path.some(([k, v]) => k === key && v === value)
        for (const other of leaves().filter(l => !has(l))) {
          assert.equal(coverageOf(other, next), coverageOf(other, start), `${label} moved ${JSON.stringify(other.path)}`)
        }
      }
    }
  }
})
