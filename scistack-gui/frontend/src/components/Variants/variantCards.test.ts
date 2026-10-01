import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  axisLabel,
  canConfirmDelete,
  cardHeading,
  conflictVariables,
  deleteLines,
  locationTree,
  pinConflictMessage,
  popupSummary,
  selectionLines,
  statusBadge,
  stepLines,
  upstreamGraph,
} from './variantCards.js'
import type { DeletePlanReply, UpstreamStep, VariantCard, VariantCardsReply } from './variantCards.js'

const step: UpstreamStep = {
  node_id: 'fn:bandpass->Filtered',
  function_name: 'bandpass',
  output_type: 'Filtered',
  inputs: { signal: 'Raw' },
  constants: { low_hz: { '20': 2 } },
  path_inputs: {},
  code: [{ function_hash: 'abc', version: 'v2', invocations: 2 }],
  run_options: { 'distribute=false': 2 },
  invocations: 2,
  uniform: true,
  parameter_names: { low_hz: 'LowCut' },
}

function card(over: Partial<VariantCard> = {}): VariantCard {
  return {
    card_id: 'c1',
    function_name: 'detect',
    selection: { 'bandpass.low_hz': 20, '__code__.bandpass': 'v2' },
    distinguishing: { 'bandpass.low_hz': '20' },
    selection_exact: true,
    overlaps_with: [],
    record_ids: ['r1', 'r2'],
    record_count: 2,
    first_saved: '2026-09-30 10:00',
    last_saved: '2026-09-30 10:00',
    verdict: 'current',
    verdict_label: 'all',
    current_location_count: 2,
    location_keys: ['subject', 'trial'],
    locations: [
      { subject: 'S01', trial: '1' },
      { subject: 'S01', trial: '2' },
      { subject: 'S02', trial: '1' },
    ],
    runs: [],
    upstream: {
      variables: ['Filtered', 'Raw', 'Steps'],
      steps: [step],
      edges: [
        { source: 'var:Raw', target: 'fn:bandpass->Filtered', param: 'signal' },
        { source: 'fn:bandpass->Filtered', target: 'var:Filtered', param: '' },
      ],
    },
    is_default: true,
    is_pinned: false,
    pin_conflict: null,
    parameter_offers: [],
    ...over,
  }
}

test('axis labels name the setting and its function', () => {
  assert.equal(axisLabel('bandpass.low_hz'), 'low_hz (bandpass)')
  assert.equal(axisLabel('__code__.detect'), 'code of detect')
  assert.equal(axisLabel('__run__.load'), 'run options of load')
  assert.equal(axisLabel('plain'), 'plain')
})

test('the heading shows only the distinguishing axes', () => {
  assert.equal(cardHeading(card()), 'low_hz (bandpass) = 20')
  assert.equal(cardHeading(card({ distinguishing: {} })), 'the only variant')
})

test('the expanded selection lists every key', () => {
  assert.deepEqual(selectionLines(card().selection), [
    'low_hz (bandpass) = 20',
    'code of bandpass = v2',
  ])
})

test('badges: pinned beats default beats verdict', () => {
  assert.equal(statusBadge(card({ is_pinned: true }), true).code, 'pinned')
  assert.equal(statusBadge(card(), true).code, 'default')
  assert.equal(statusBadge(card({ is_default: false }), true).label, 'not the default')
  assert.equal(statusBadge(card({ verdict: 'superseded', is_default: false }), false).code, 'superseded')
  assert.equal(statusBadge(card(), false).label, 'current')
})

test('step lines carry code, constants with their Parameter, and run options', () => {
  assert.deepEqual(stepLines(step), ['code v2', 'low_hz ← LowCut = 20', 'distribute=false'])
  assert.ok(stepLines({ ...step, uniform: false }).includes('⚠ settings differ across records'))
})

test('a PathInput shows its template', () => {
  const lines = stepLines({
    ...step,
    constants: {},
    code: [],
    run_options: {},
    path_inputs: { path: { '{"template": "data/{subject}.csv", "root_folder": "/x"}': 2 } },
  })
  assert.deepEqual(lines, ['path: data/{subject}.csv'])
})

test('the mini DAG has every variable and step, root marked', () => {
  const { nodes, edges } = upstreamGraph(card(), 'Steps')
  assert.deepEqual(nodes.map(n => n.id), ['var:Filtered', 'var:Raw', 'var:Steps', 'fn:bandpass->Filtered'])
  assert.equal(nodes.find(n => n.isRoot)?.label, 'Steps')
  assert.equal(edges[0].label, 'signal')
})

test('locations group by schema level with counts', () => {
  const tree = locationTree(card())
  assert.deepEqual(tree.map(b => [b.label, b.count]), [['subject S01', 2], ['subject S02', 1]])
  assert.deepEqual(tree[0].children.map(b => b.label), ['trial 1', 'trial 2'])
  assert.deepEqual(tree[0].children[0].children, [])
})

const plan: DeletePlanReply = {
  targets: [],
  by_variable: { Filtered: 2, Steps: 2 },
  seed_by_variable: { Filtered: 2 },
  downstream_by_variable: { Steps: 2 },
  lost_locations: { Steps: [{ subject: 'S02' }] },
  pins_to_release: [],
  fingerprint: 'f',
  warnings: [],
  total_records: 4,
  invocation_count: 4,
  run_count: 1,
}

test('delete lines say what is computed from it and what is lost', () => {
  assert.deepEqual(deleteLines(plan), [
    'Filtered: 2 records',
    'Steps: 2 records · computed from it · nothing left at 1 location',
  ])
})

test('delete needs a plan, a reason, and not being busy', () => {
  assert.equal(canConfirmDelete(null, 'x', false), false)
  assert.equal(canConfirmDelete(plan, '  ', false), false)
  assert.equal(canConfirmDelete(plan, 'bad run', true), false)
  assert.equal(canConfirmDelete({ ...plan, total_records: 0 }, 'x', false), false)
  assert.equal(canConfirmDelete(plan, 'bad run', false), true)
})

test('the summary names the axes and where the default comes from', () => {
  const reply = {
    variable: 'Steps',
    cards: [card(), card({ card_id: 'c2' })],
    varying_axes: ['bandpass.low_hz'],
    producer_varies: false,
    excluded_record_count: 0,
    command: '',
    pin: null,
    default_selection: { 'bandpass.low_hz': 20 },
    default_sources: ['Filtered'],
    pin_history: [],
    tombstones: [],
  } as VariantCardsReply
  assert.equal(
    popupSummary(reply),
    '2 variants; differing in low_hz (bandpass); default follows the pin on Filtered',
  )
})

test('pin conflicts name each variable once', () => {
  const pin = {
    pin_id: 'p', variable: 'Filtered', selection: {}, reason: '', pinned_by: null,
    pinned_at: '', released_at: null, release_reason: null,
  }
  const conflicts = [
    { node_id: 'a', function_name: null, variable: 'Filtered', pin },
    { node_id: 'b', function_name: null, variable: 'Filtered', pin },
  ]
  assert.deepEqual(conflictVariables(conflicts), ['Filtered'])
  assert.match(pinConflictMessage(conflicts), /writes to Filtered, which is pinned/)
})
