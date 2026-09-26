import { test } from 'node:test'
import assert from 'node:assert/strict'

import { roundTo, stepDecimals, stepValue } from './stepper.js'

test('one click moves by one step', () => {
  assert.equal(stepValue(12, null, 1, 1, 1), 13)
  assert.equal(stepValue(12, null, -1, 1, 1), 11)
  assert.equal(stepValue(8, null, 1, 0.1, 0.1), 8.1)
  assert.equal(stepValue(1600, null, -1, 10, 10), 1590)
})

test('an off-grid value snaps to the next multiple in that direction', () => {
  assert.equal(stepValue(11.7, null, 1, 1, 1), 12)
  assert.equal(stepValue(11.7, null, -1, 1, 1), 11)
  assert.equal(stepValue(8.37, null, 1, 0.1, 0.1), 8.4)
  assert.equal(stepValue(8.37, null, -1, 0.1, 0.1), 8.3)
})

test('an on-grid value with conversion noise steps once, not twice', () => {
  assert.equal(stepValue(12.0000001, null, 1, 1, 1), 13)
  assert.equal(stepValue(0.30000000000000004, null, 1, 0.1, 0.1), 0.4)
})

test('an empty box steps from what it is drawn at, never from 0', () => {
  assert.equal(stepValue(null, 11.7, 1, 1, 1), 12)
  assert.equal(stepValue(null, 1, 1, 0.1, 0.1), 1.1)
  assert.equal(stepValue(null, null, 1, 1, 1), 2)
})

test('never below the minimum', () => {
  assert.equal(stepValue(1, null, -1, 1, 1), 1)
  assert.equal(stepValue(0.1, null, -1, 0.1, 0.1), 0.1)
})

test('decimals follow the step', () => {
  assert.equal(stepDecimals(0.1), 1)
  assert.equal(stepDecimals(10), 0)
  assert.equal(roundTo(0.1 + 0.2, 1), 0.3)
})
