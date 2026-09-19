/**
 * Grouping-list placement — the TypeScript half of the rule.
 *
 * The Python half is `PlotSpec.ordered_groups` and its tests in
 * `scistackplot/tests/test_grouping_layers.py`. The cases below are
 * deliberately the same shapes, because the two run at different moments —
 * this one when the user ticks a factor, that one when a spec arrives without
 * an order — and a disagreement would show as the panel and the figure
 * nesting differently.
 *
 * The list is INNERMOST FIRST. The fixture is the study that prompted the
 * work: schema [subject, session, …] with a subject-level demographics column
 * joined in, so `InterventionGroup` and `subject` share a depth and `session`
 * is deeper.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import { orderGroups, placeGroupLayer } from './groups.js'

const DEPTHS = {
  subject: 1,
  InterventionGroup: 1,
  session: 2,
  trial: 3,
  // A variant axis is not a place in the hierarchy.
  'bandpass.low_hz': null,
}

test('a shallower factor lands outside (after) a deeper one', () => {
  assert.deepEqual(
    placeGroupLayer(['session'], 'InterventionGroup', DEPTHS),
    ['session', 'InterventionGroup']
  )
})

test('a deeper factor lands inside (before) a shallower one', () => {
  assert.deepEqual(
    placeGroupLayer(['InterventionGroup'], 'session', DEPTHS),
    ['session', 'InterventionGroup']
  )
})

test('equal depths keep the order they arrived in', () => {
  assert.deepEqual(
    placeGroupLayer(['subject'], 'InterventionGroup', DEPTHS),
    ['subject', 'InterventionGroup']
  )
})

test('a depthless factor goes innermost (first)', () => {
  assert.deepEqual(
    placeGroupLayer(['session'], 'bandpass.low_hz', DEPTHS),
    ['bandpass.low_hz', 'session']
  )
  assert.deepEqual(
    placeGroupLayer(['bandpass.low_hz'], 'session', DEPTHS),
    ['bandpass.low_hz', 'session']
  )
})

test('an unknown factor is treated as depthless, never dropped', () => {
  assert.deepEqual(placeGroupLayer(['session'], 'mystery', DEPTHS), [
    'mystery',
    'session',
  ])
})

test('inserting never reorders what is already there', () => {
  // The user moved `InterventionGroup` inside `session` with the arrows. That
  // is a decision; adding a third layer must not quietly undo it.
  assert.deepEqual(
    placeGroupLayer(['InterventionGroup', 'session'], 'trial', DEPTHS),
    ['trial', 'InterventionGroup', 'session']
  )
})

test('placing something already placed changes nothing', () => {
  const layers = ['session', 'InterventionGroup']
  assert.equal(placeGroupLayer(layers, 'session', DEPTHS), layers)
})

test('an unordered holder is placed by depth after the ordered ones', () => {
  assert.deepEqual(
    orderGroups(['session'], ['session', 'InterventionGroup', 'trial'], DEPTHS),
    ['session', 'trial', 'InterventionGroup']
  )
})

test('a name that no longer groups drops out of the order', () => {
  assert.deepEqual(orderGroups(['gone', 'session'], ['session'], DEPTHS), ['session'])
})

test('two depthless holders keep declaration order', () => {
  assert.deepEqual(
    orderGroups([], ['bandpass.low_hz', 'mystery'], DEPTHS),
    ['bandpass.low_hz', 'mystery']
  )
})
