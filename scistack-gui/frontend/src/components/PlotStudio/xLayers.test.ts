/**
 * Nested x placement — the TypeScript half of the rule.
 *
 * The Python half is `PlotSpec.ordered_x_layers` and its tests in
 * `scistackplot/tests/test_x_nesting.py`. The cases below are deliberately the
 * same shapes, because the two run at different moments — this one when the
 * user ticks a factor, that one when a spec arrives without an order — and a
 * disagreement would show as the panel and the figure nesting differently.
 *
 * The fixture is the study that prompted the work: schema
 * [subject, session, …] with a subject-level demographics column joined in, so
 * `InterventionGroup` and `subject` share a depth and `session` is deeper.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import { orderXLayers, placeXLayer } from './xLayers.js'

const DEPTHS = {
  subject: 1,
  InterventionGroup: 1,
  session: 2,
  trial: 3,
  // A variant axis is not a place in the hierarchy.
  'bandpass.low_hz': null,
}

test('a shallower factor lands outside a deeper one', () => {
  assert.deepEqual(
    placeXLayer(['session'], 'InterventionGroup', DEPTHS),
    ['InterventionGroup', 'session']
  )
})

test('a deeper factor lands inside a shallower one', () => {
  assert.deepEqual(
    placeXLayer(['InterventionGroup'], 'session', DEPTHS),
    ['InterventionGroup', 'session']
  )
})

test('equal depths keep the order they arrived in', () => {
  assert.deepEqual(
    placeXLayer(['subject'], 'InterventionGroup', DEPTHS),
    ['subject', 'InterventionGroup']
  )
})

test('a depthless factor goes innermost', () => {
  assert.deepEqual(
    placeXLayer(['session'], 'bandpass.low_hz', DEPTHS),
    ['session', 'bandpass.low_hz']
  )
  assert.deepEqual(
    placeXLayer(['bandpass.low_hz'], 'session', DEPTHS),
    ['session', 'bandpass.low_hz']
  )
})

test('an unknown factor is treated as depthless, never dropped', () => {
  assert.deepEqual(placeXLayer(['session'], 'mystery', DEPTHS), [
    'session',
    'mystery',
  ])
})

test('inserting never reorders what is already there', () => {
  // The user moved `session` outside `InterventionGroup` with the arrows. That
  // is a decision; adding a third layer must not quietly undo it.
  assert.deepEqual(
    placeXLayer(['session', 'InterventionGroup'], 'trial', DEPTHS),
    ['session', 'InterventionGroup', 'trial']
  )
})

test('placing something already placed changes nothing', () => {
  const layers = ['InterventionGroup', 'session']
  assert.equal(placeXLayer(layers, 'session', DEPTHS), layers)
})

// --- orderXLayers: the reconciliation ---------------------------------------

test('a declared order wins over depth', () => {
  assert.deepEqual(
    orderXLayers(
      ['session', 'InterventionGroup'],
      ['session', 'InterventionGroup'],
      DEPTHS
    ),
    ['session', 'InterventionGroup']
  )
})

test('holders the order never mentioned are placed by depth', () => {
  assert.deepEqual(
    orderXLayers([], ['session', 'InterventionGroup'], DEPTHS),
    ['InterventionGroup', 'session']
  )
})

test('a name that no longer holds x drops out', () => {
  assert.deepEqual(
    orderXLayers(['session', 'trial'], ['session'], DEPTHS),
    ['session']
  )
})

test('declared entries keep their place and the rest sort after them', () => {
  assert.deepEqual(
    orderXLayers(['session'], ['session', 'InterventionGroup'], DEPTHS),
    ['session', 'InterventionGroup']
  )
})

test('two depthless factors keep their order', () => {
  // `Infinity - Infinity` is NaN, and a comparator returning NaN leaves the
  // order unspecified — so the comparison is `<`, not a subtraction.
  assert.deepEqual(
    orderXLayers([], ['bandpass.low_hz', 'mystery'], DEPTHS),
    ['bandpass.low_hz', 'mystery']
  )
})
