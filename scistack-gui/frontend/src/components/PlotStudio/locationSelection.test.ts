/**
 * The schema location picker's rules.
 *
 * Run by `npm test` under node's own test runner, against the React-free module
 * only — the same split `extension/tsconfig.test.json` makes for its vscode-free
 * modules, and for the same reason: a rule nothing can execute is a rule that
 * drifts.
 *
 * The fixture is a [subject, trial] study shaped like the Python one:
 *
 *   S01  t1 green   t2 amber   t3 red
 *   S02  t1 grey (excluded)    t2 green
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  type LocationNode,
  type Prefix,
  coverageOf,
  covers,
  describeSelection,
  hasProblem,
  matchesQuery,
  normalize,
  rolesAfterPick,
  without,
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

test('an empty selection covers nothing explicitly', () => {
  // The picker draws "everything" from `include.length === 0`, not from
  // coverage — an empty selection is INERT, which is a different statement
  // from "every prefix is present".
  assert.equal(coverageOf(roots[0], []), 'none')
})

test('a subject prefix fully covers its subtree', () => {
  assert.equal(coverageOf(roots[0], [S01]), 'full')
  assert.equal(coverageOf(roots[0].children[0], [S01]), 'full')
  assert.equal(coverageOf(roots[1], [S01]), 'none')
})

test('a partially selected subject is indeterminate', () => {
  assert.equal(coverageOf(roots[0], [S01_T2]), 'partial')
})

test('every child selected reads as a full parent', () => {
  const all = roots[0].children.map(c => c.path)
  assert.equal(coverageOf(roots[0], all), 'full')
})

// --- the minimal covering set ----------------------------------------------

test('ticking every trial stores the same thing as ticking the subject', () => {
  // The load-bearing one. If these differed, a trial added to S01 tomorrow
  // would be inside one selection and outside the other.
  const byTrials = roots[0].children.reduce<Prefix[]>(
    (acc, child) => withPath(roots, acc, child.path),
    []
  )
  const bySubject = withPath(roots, [], S01)
  assert.deepEqual(byTrials, bySubject)
  assert.deepEqual(bySubject, [S01])
})

test('selecting everything collapses to the inert empty selection', () => {
  const all = withPath(roots, withPath(roots, [], S01), S02)
  assert.deepEqual(all, [], 'everything selected is stored as nothing selected')
})

test('normalize collapses a fully covered parent', () => {
  const leaves = roots[0].children.map(c => c.path)
  assert.deepEqual(normalize(roots, leaves), [S01])
})

// --- unticking, and the explosion it forces ---------------------------------

test('unticking one trial of a ticked subject materialises its siblings', () => {
  const next = without(roots, [S01], S01_T2)

  assert.deepEqual(next, [
    [['subject', 'S01'], ['trial', 't1']],
    [['subject', 'S01'], ['trial', 't3']],
  ])
  assert.equal(coverageOf(roots[0], next), 'partial')
})

test('unticking from the ALL state explodes the whole tree', () => {
  // `include: []` means everything, so the explosion has to start at the
  // virtual root — otherwise unticking one trial would silently select nothing.
  const next = without(roots, [], S01_T2)

  assert.equal(coverageOf(roots[1], next), 'full', 'the other subject survives whole')
  assert.equal(coverageOf(roots[0], next), 'partial')
  assert.deepEqual(next.filter(p => p[0][1] === 'S01').map(p => p[1][1]), ['t1', 't3'])
})

test('unticking a whole subject leaves the other one', () => {
  const next = without(roots, [], S01)
  assert.deepEqual(next, [S02])
})

test('unticking the last selection leaves nothing selected', () => {
  const next = without(roots, [S01], S01)
  assert.deepEqual(next, [])
})

test('re-ticking an exploded sibling set collapses it again', () => {
  const exploded = without(roots, [S01], S01_T2)
  const restored = withPath(roots, exploded, S01_T2)
  assert.deepEqual(restored, [S01], 'back to one entry, not three')
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

test('an unanswered key moves off aggregate onto free', () => {
  const roles = rolesAfterPick({ trial: 'aggregate' }, S01, KEYS)
  assert.equal(roles.trial, 'free', 'a mean would hide what the picker was opened for')
})

test('an unanswered key with no role gets free', () => {
  assert.equal(rolesAfterPick({}, S01, KEYS).trial, 'free')
})

test('a chosen x axis is never stamped over', () => {
  // The refinement that keeps "as granular as possible" from becoming "reset
  // my figure": X, COLOR and FACET already draw every level.
  for (const role of ['x', 'color', 'facet'] as const) {
    assert.equal(rolesAfterPick({ trial: role }, S01, KEYS).trial, role)
  }
})

test('a key the selection NAMES is left alone entirely', () => {
  const roles = rolesAfterPick({ subject: 'aggregate' }, S01, KEYS)
  assert.equal(roles.subject, 'aggregate', 'the selection answered it; it is one level')
})

test('it does not mutate the roles it was given', () => {
  const before = { trial: 'aggregate' as const }
  rolesAfterPick(before, S01, KEYS)
  assert.equal(before.trial, 'aggregate')
})

// --- the button label -------------------------------------------------------

test('the label names a single location rather than counting it', () => {
  assert.equal(describeSelection([S01_T2]), 'subject=S01 / trial=t2…')
  assert.equal(describeSelection([]), 'All locations…')
  assert.equal(describeSelection([S01, S02]), '2 locations…')
})
