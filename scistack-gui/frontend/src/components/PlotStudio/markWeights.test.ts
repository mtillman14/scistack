/**
 * Mark weights: the "1 or cleared = key removed" rule the saved-plot
 * modified check depends on, and the tooltip reads Python's numbers.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import { weightTitle, withWeight } from './markWeights.js'

test('a weight is stored', () => {
  assert.deepEqual(withWeight({ width: 8 }, 'sample_weight', 2), { width: 8, sample_weight: 2 })
})

test('1, cleared or invalid deletes the key', () => {
  const style = { width: 8, sample_weight: 2, line_weight: 3 }
  assert.deepEqual(withWeight(style, 'sample_weight', 1), { width: 8, line_weight: 3 })
  assert.deepEqual(withWeight(style, 'line_weight', null), { width: 8, sample_weight: 2 })
  assert.deepEqual(withWeight(style, 'line_weight', 0), { width: 8, sample_weight: 2 })
  assert.deepEqual(withWeight(style, 'line_weight', -1), { width: 8, sample_weight: 2 })
  assert.deepEqual(withWeight(style, 'line_weight', Number.NaN), { width: 8, sample_weight: 2 })
})

test('an absent style starts empty', () => {
  assert.deepEqual(withWeight(undefined, 'line_weight', 1.5), { line_weight: 1.5 })
})

test('the input style is not mutated', () => {
  const style = { sample_weight: 2 }
  withWeight(style, 'sample_weight', null)
  assert.deepEqual(style, { sample_weight: 2 })
})

test('the tooltip states the resolved sizes', () => {
  const title = weightTitle({ applies: true, weight: 2, marker_pt: 8.05, line_pt: 2 }, 'sample points')
  assert.match(title, /8\.1 pt/)
  assert.match(title, /2\.00 pt/)
  assert.doesNotMatch(weightTitle(undefined, 'lines'), /Now:/)
})
