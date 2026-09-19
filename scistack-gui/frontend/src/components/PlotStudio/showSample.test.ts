/**
 * "Show sample" — the two spec edits the panel makes, and how a report row
 * reads as a checkbox. The rules themselves (the cut, the join) are Python's
 * and pinned in `scistackplot/tests/test_show_sample.py`.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  isLocked,
  isTicked,
  joinChoice,
  joinSetting,
  toggleShowSample,
} from './showSample.js'

test('ticking adds, unticking removes, order is the user’s', () => {
  assert.deepEqual(toggleShowSample(undefined, 'trial', true), ['trial'])
  assert.deepEqual(toggleShowSample(['trial'], 'subject', true), ['trial', 'subject'])
  assert.deepEqual(toggleShowSample(['trial', 'subject'], 'trial', false), ['subject'])
  assert.deepEqual(toggleShowSample(['trial'], 'trial', true), ['trial'], 'no duplicates')
})

test('the join dropdown round-trips the three settings', () => {
  assert.equal(joinChoice(null), 'auto')
  assert.equal(joinChoice(undefined), 'auto')
  assert.equal(joinChoice(true), 'lines')
  assert.equal(joinChoice(false), 'points')
  assert.equal(joinSetting('auto'), null)
  assert.equal(joinSetting('lines'), true)
  assert.equal(joinSetting('points'), false)
})

test('an implied key reads ticked and locked; a checked one ticked and free', () => {
  const implied = { name: 'subject', checked: false, shown: true }
  const checked = { name: 'trial', checked: true, shown: true }
  const off = { name: 'cycle', checked: false, shown: false }
  assert.equal(isTicked(implied), true)
  assert.equal(isLocked(implied), true)
  assert.equal(isTicked(checked), true)
  assert.equal(isLocked(checked), false)
  assert.equal(isTicked(off), false)
  assert.equal(isLocked(off), false)
})
