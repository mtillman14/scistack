import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  compareSummary,
  droppedLines,
  layerChoices,
  modeChoice,
  withLayer,
  withLevel,
  withMode,
  type CompareCapability,
  type CompareMeta,
  type Comparison,
} from './compare.js'

const CAPABILITY: CompareCapability = {
  available: true,
  reason: null,
  layers: [
    { name: 'session', levels: ['s1', 's2', 's3'] },
    { name: 'side', levels: ['L', 'R'] },
  ],
  default_layer: 'session',
  default_level: 's1',
  modes: ['difference', 'percent'],
  state: { set: false },
}

const SET: Comparison = { layer: 'session', level: 's2', mode: 'difference', active: true }

test('mode is off with no comparison or a switched-off one', () => {
  assert.equal(modeChoice(undefined), 'off')
  assert.equal(modeChoice({ ...SET, active: false }), 'off')
  assert.equal(modeChoice(SET), 'difference')
})

test('turning it on from nothing opens at the backend default', () => {
  assert.deepEqual(withMode(undefined, 'percent', CAPABILITY), {
    layer: 'session',
    level: 's1',
    mode: 'percent',
    active: true,
  })
})

test('turning it on with no layer to compare along sets nothing', () => {
  const none = { ...CAPABILITY, layers: [], default_layer: null, default_level: null }
  assert.equal(withMode(undefined, 'difference', none), null)
})

test('off keeps the layer and level, and on restores them', () => {
  const off = withMode(SET, 'off', CAPABILITY)
  assert.deepEqual(off, { ...SET, active: false })
  assert.deepEqual(withMode(off, 'percent', CAPABILITY), { ...SET, mode: 'percent', active: true })
})

test('a new layer takes its first level as the reference', () => {
  assert.deepEqual(withLayer(SET, 'side', CAPABILITY), { ...SET, layer: 'side', level: 'L' })
  assert.deepEqual(withLevel(SET, 's3'), { ...SET, level: 's3' })
})

test('a layer that no longer groups stays listed', () => {
  assert.deepEqual(layerChoices({ ...SET, layer: 'trial' }, CAPABILITY), ['session', 'side', 'trial'])
  assert.deepEqual(layerChoices(SET, CAPABILITY), ['session', 'side'])
})

test('dropped units are listed per reason, capped', () => {
  const meta: CompareMeta = {
    layer: 'session',
    level: 's1',
    mode: 'difference',
    paired: true,
    reason: '',
    description: '',
    outcome: {
      rows_in: 10,
      rows_out: 8,
      dropped: { 'no reference value': ['subject=01', 'subject=02', 'subject=03'] },
    },
  }
  assert.deepEqual(droppedLines(meta, 2), [
    'Dropped 3 (no reference value): subject=01; subject=02 +1 more',
  ])
  assert.deepEqual(droppedLines(null), [])
})

test('the summary names the reference', () => {
  assert.equal(compareSummary(SET), 'vs session s2 (Δ)')
  assert.equal(compareSummary({ ...SET, mode: 'percent' }), 'vs session s2 (%)')
  assert.equal(compareSummary({ ...SET, active: false }), '')
})
