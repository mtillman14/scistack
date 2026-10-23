import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  binText,
  binsSummary,
  durationText,
  fitsSummary,
  kindForXMeasure,
  parseBinCount,
  parseBinValue,
} from './continuousKinds.js'

test('picking an X variable turns a categorical kind into a regression', () => {
  assert.equal(kindForXMeasure('box', 'Mass'), 'regression')
  assert.equal(kindForXMeasure('histogram', 'Mass'), 'regression')
})

test('an x-compatible kind is kept when an X variable is picked', () => {
  assert.equal(kindForXMeasure('scatter', 'Mass'), 'scatter')
  assert.equal(kindForXMeasure('line', 'Mass'), 'line')
})

test('clearing the X variable turns the x-only kinds into a scatter', () => {
  assert.equal(kindForXMeasure('regression', null), 'scatter')
  assert.equal(kindForXMeasure('line', null), 'scatter')
  assert.equal(kindForXMeasure('box', null), 'box')
})

test('bin count: positive whole numbers only, blank is the default', () => {
  assert.equal(parseBinCount('12'), 12)
  assert.equal(parseBinCount(' '), null)
  assert.equal(parseBinCount('0'), null)
  assert.equal(parseBinCount('2.5'), null)
})

test('bin width: a number on a numeric axis, text on a datetime one', () => {
  assert.equal(parseBinValue('0.5', false), 0.5)
  assert.equal(parseBinValue('1h', true), '1h')
  assert.equal(parseBinValue('', true), null)
  // Not a number on a numeric axis: kept as typed so Python names the problem.
  assert.equal(parseBinValue('abc', false), 'abc')
})

test('binText round-trips stored values', () => {
  assert.equal(binText(null), '')
  assert.equal(binText(5), '5')
  assert.equal(binText('7D'), '7D')
})

test('durationText picks the largest exact unit', () => {
  assert.equal(durationText(86_400_000 * 7), '7D')
  assert.equal(durationText(3_600_000), '1h')
  assert.equal(durationText(5_400_000), '90min')
  assert.equal(durationText(1500), '1500ms')
})

test('binsSummary reads the drawn bins', () => {
  const meta = { mode: 'width' as const, edges: [0, 1, 2], width: 1, n_bins: 2, n_values: 9, notes: [] }
  assert.equal(binsSummary(meta, false), '2 bins of 1, from 9 values')
  assert.equal(binsSummary({ ...meta, width: 3_600_000 }, true), '2 bins of 1h, from 9 values')
  assert.equal(binsSummary({ ...meta, n_bins: 0, edges: [] }, false), 'No values to bin.')
  assert.equal(binsSummary(null, false), '')
})

test('fitsSummary: one line per fit, panel and level named, failures explained', () => {
  const fit = {
    level: 'BL', n: 10, slope: 1, intercept: 0, r_squared: 0.5, p_value: 0.01, reason: null,
    text: 'BL: R² = 0.50, p = 0.010, n = 10',
  }
  const failed = { ...fit, level: 'POST', reason: 'needs at least 3 points', text: '' }
  assert.deepEqual(fitsSummary([{ panel: '', fits: [fit, failed] }]), [
    'BL: R² = 0.50, p = 0.010, n = 10',
    'POST: no line (needs at least 3 points)',
  ])
  assert.deepEqual(fitsSummary([{ panel: 'L', fits: [fit] }, { panel: 'R', fits: [] }]), [
    'L · BL: R² = 0.50, p = 0.010, n = 10',
  ])
  assert.deepEqual(fitsSummary(null), [])
})
