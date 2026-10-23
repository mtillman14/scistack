/**
 * The pure half of the tunables panel (.claude/plan-library-fork-chain.md E1-E5).
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  formatValue,
  isChanged,
  parseStepValue,
  suggestParameterName,
  tunableSummary,
  type Tunables,
} from './tunables.js'

test('a typed number stays a number, whatever the default was', () => {
  assert.equal(parseStepValue('2', 1.0), 2)
  assert.equal(parseStepValue(' 2.5 ', 1.0), 2.5)
  assert.equal(parseStepValue('3', 'x'), 3)
})

test('booleans, JSON and plain text', () => {
  assert.equal(parseStepValue('false', true), false)
  assert.deepEqual(parseStepValue('[1, 2]', null), [1, 2])
  assert.equal(parseStepValue('hann', 'hamming'), 'hann')
})

test('the suggested Parameter name is the input, in caps', () => {
  assert.equal(suggestParameterName('emg_alice.filter_emg.gain'), 'GAIN')
  assert.equal(suggestParameterName('f.low-hz'), 'LOW_HZ')
})

test('values format for the panel', () => {
  assert.equal(formatValue([20]), '20')
  assert.equal(formatValue(null), '—')
  assert.equal(formatValue('a.csv'), 'a.csv')
})

test('a step value is changed when it differs from the default or follows a Parameter', () => {
  const base = { key: 'f.gain', value: 1.0, default: 1.0, parameter: null }
  assert.equal(isChanged(base), false)
  assert.equal(isChanged({ ...base, value: 2 }), true)
  assert.equal(isChanged({ ...base, parameter: 'GAIN' }), true)
})

test('the summary counts what needs attention', () => {
  const t: Tunables = {
    use_id: 'u', pipeline_id: 'p', library: 'lib', key_map: {},
    parameters: [{ name: 'CUTOFF', value: [20], source: 'library default' }],
    path_inputs: [{ name: 'EmgFile', template: '{subject}.csv', root_folder: null, needs_root: true }],
    step_values: [{ key: 'f.gain', value: 2, default: 1, parameter: null }],
  }
  assert.equal(
    tunableSummary(t),
    '1 Parameter(s) · 1 PathInput(s), 1 need a root folder · 1 step value(s), 1 changed',
  )
})
