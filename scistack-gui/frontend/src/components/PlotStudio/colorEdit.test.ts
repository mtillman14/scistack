/**
 * The Colours section's plot-layer edits: cleared = removed, an emptied thing
 * disappears; the picker's drag is debounced to its last value.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  colorPlaceholder,
  colorProjectAction,
  debounced,
  pinnedCount,
  plotColor,
  plotMarkColor,
  withPlotColor,
  type Colorable,
  type ColorableLevel,
} from './colorEdit.js'

const level = (origin: ColorableLevel['origin'], hex = '#0072b2'): ColorableLevel => ({
  raw: 'BL',
  text: 'Baseline',
  hex,
  origin,
})

test('setting and clearing a level colour', () => {
  const set = withPlotColor({}, 'session', 'BL', '#ff0000')
  assert.deepEqual(set, { session: { BL: '#ff0000' } })
  assert.equal(plotColor(set, 'session', 'BL'), '#ff0000')
  assert.deepEqual(withPlotColor(set, 'session', 'BL', null), {})
  assert.deepEqual(withPlotColor(set, 'session', 'BL', ''), {})
})

test('clearing one level keeps the others and other things', () => {
  let colors = withPlotColor({}, 'session', 'BL', '#ff0000')
  colors = withPlotColor(colors, 'session', 'FU', 'rgb(0, 0, 255)')
  colors = withPlotColor(colors, 'subject', '01', 'red')
  assert.deepEqual(withPlotColor(colors, 'session', 'BL', null), {
    session: { FU: 'rgb(0, 0, 255)' },
    subject: { '01': 'red' },
  })
})

test('the input is never mutated', () => {
  const before = { session: { BL: '#ff0000' } }
  withPlotColor(before, 'session', 'BL', null)
  assert.deepEqual(before, { session: { BL: '#ff0000' } })
})

test('plotColor and plotMarkColor read nothing as null', () => {
  assert.equal(plotColor(undefined, 'session', 'BL'), null)
  assert.equal(plotColor({ session: { BL: '' } }, 'session', 'BL'), null)
  assert.equal(plotMarkColor(undefined), null)
  assert.equal(plotMarkColor({ mark_color: '' }), null)
  assert.equal(plotMarkColor({ mark_color: '#123456' }), '#123456')
})

test('the placeholder names where the drawn colour comes from', () => {
  assert.equal(colorPlaceholder(level('project')), '#0072b2 (project)')
  assert.equal(colorPlaceholder(level('palette')), '#0072b2 (palette)')
  assert.equal(colorPlaceholder(level('plot')), '#0072b2')
})

test('the project button follows the Labels rule', () => {
  assert.equal(colorProjectAction('#ff0000', level('plot')), 'save')
  assert.equal(colorProjectAction(null, level('project')), 'remove')
  assert.equal(colorProjectAction(null, level('palette')), null)
})

test('pinnedCount counts plot and project pins', () => {
  const entry: Colorable = {
    factor: 'session',
    key: 'session',
    role: 'colour',
    name: 'session',
    levels: [level('plot'), level('project'), level('palette')],
    truncated: false,
  }
  assert.equal(pinnedCount(entry), 2)
})

test('debounced commits only the last value of a burst', async () => {
  const seen: string[] = []
  const d = debounced((value: string) => seen.push(value), 20)
  d.call('#000001')
  d.call('#000002')
  d.call('#000003')
  assert.deepEqual(seen, [])
  await new Promise(resolve => setTimeout(resolve, 40))
  assert.deepEqual(seen, ['#000003'])
})

test('flush commits at once and cancel drops the pending value', () => {
  const seen: string[] = []
  const d = debounced((value: string) => seen.push(value), 1000)
  d.call('#000001')
  d.flush()
  assert.deepEqual(seen, ['#000001'])
  d.flush()
  assert.deepEqual(seen, ['#000001'], 'nothing pending: flush does nothing')
  d.call('#000002')
  d.cancel()
  d.flush()
  assert.deepEqual(seen, ['#000001'])
})
