/**
 * The Labels section's plot-layer edits: cleared = removed, an emptied entry
 * disappears, `aliases` itself stays (it round-trips as `{}`).
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  aliasedCount,
  placeholderFor,
  plotLevel,
  plotName,
  projectAction,
  withPlotLevel,
  withPlotName,
} from './aliasEdit.js'

test('setting and clearing a name', () => {
  const set = withPlotName({}, 'session', 'Visit')
  assert.deepEqual(set, { session: { name: 'Visit' } })
  assert.equal(plotName(set, 'session'), 'Visit')
  assert.deepEqual(withPlotName(set, 'session', null), {})
  assert.deepEqual(withPlotName(set, 'session', ''), {})
})

test('setting and clearing a level keeps the rest of the entry', () => {
  let aliases = withPlotName({}, 'session', 'Visit')
  aliases = withPlotLevel(aliases, 'session', '01', 'Visit 1')
  assert.deepEqual(aliases, { session: { name: 'Visit', levels: { '01': 'Visit 1' } } })
  assert.equal(plotLevel(aliases, 'session', '01'), 'Visit 1')
  aliases = withPlotLevel(aliases, 'session', '01', null)
  assert.deepEqual(aliases, { session: { name: 'Visit' } })
})

test('an emptied entry is removed and other entries survive', () => {
  const start = { a: { levels: { x: 'X' } }, b: { name: 'B' } }
  const next = withPlotLevel(start, 'a', 'x', '')
  assert.deepEqual(next, { b: { name: 'B' } })
  assert.deepEqual(start.a, { levels: { x: 'X' } }, 'the input is not mutated')
})

test('undefined aliases are treated as empty', () => {
  assert.equal(plotName(undefined, 'x'), null)
  assert.deepEqual(withPlotLevel(undefined, 'k', '1', 'one'), { k: { levels: { '1': 'one' } } })
})

test('an empty box shows what the figure draws without the plot', () => {
  assert.equal(placeholderFor({ raw: 'BL', text: 'Baseline', origin: 'project' }), 'Baseline (project)')
  assert.equal(placeholderFor({ raw: 'BL', text: 'BL', origin: 'raw' }), 'BL')
  // Plot-set: the box holds the value; the placeholder is the raw text.
  assert.equal(placeholderFor({ raw: 'BL', text: 'Mine', origin: 'plot' }), 'BL')
})

test('the project button follows who sets the text', () => {
  const project = { raw: 'BL', text: 'Baseline', origin: 'project' as const }
  const raw = { raw: 'BL', text: 'BL', origin: 'raw' as const }
  assert.equal(projectAction('Mine', raw), 'save')
  assert.equal(projectAction(null, project), 'remove')
  assert.equal(projectAction(null, raw), null)
})

test('the header counts aliased levels', () => {
  assert.equal(
    aliasedCount({
      factor: 'session', key: 'session', role: 'x axis', truncated: false,
      name: { raw: 'session', text: 'session', origin: 'raw' },
      levels: [
        { raw: 'a', text: 'A', origin: 'project' },
        { raw: 'b', text: 'b', origin: 'raw' },
      ],
    }),
    1,
  )
})
