import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  DEFAULT_VIEW,
  isModified,
  iterateSignature,
  modifiedKey,
  notesSummary,
  readView,
  savedAtLabel,
  stableStringify,
  viewState,
} from './savedPlots.js'

test('a saved view reads back unchanged', () => {
  const view = viewState('pane', '16:9', 3)
  assert.deepEqual(readView(JSON.parse(JSON.stringify(view))), view)
})

test('a drifted view keeps what still makes sense and defaults the rest', () => {
  assert.deepEqual(
    readView({ previewMode: 'zoomed', aspectChoice: 4, figureIndex: 2, legacyZoom: 1.5 }),
    { previewMode: 'export', aspectChoice: null, figureIndex: 2 },
  )
  assert.deepEqual(readView({ figureIndex: -1 }).figureIndex, 0)
  assert.deepEqual(readView({ figureIndex: 1.5 }).figureIndex, 0)
})

test('a view that is not a table opens on the defaults', () => {
  for (const raw of [null, undefined, 'pane', 3, ['pane']]) {
    assert.deepEqual(readView(raw), DEFAULT_VIEW)
  }
})

test('key order does not make two equal specs differ', () => {
  assert.equal(stableStringify({ b: 1, a: { d: 2, c: 3 } }), stableStringify({ a: { c: 3, d: 2 }, b: 1 }))
  assert.notEqual(stableStringify([1, 2]), stableStringify([2, 1]))
})

test('stepping through figures is not a modification; changing the look is', () => {
  const spec = { measures: ['X'], kind: 'box' }
  const base = modifiedKey(spec, viewState('export', null, 0))
  assert.equal(modifiedKey(spec, viewState('export', null, 5)), base)
  assert.notEqual(modifiedKey(spec, viewState('pane', null, 0)), base)
  assert.notEqual(modifiedKey(spec, viewState('export', '16:9', 0)), base)
  assert.notEqual(modifiedKey({ ...spec, kind: 'bar' }, viewState('export', null, 0)), base)
})

test('a plot restored with notes stays modified until it is saved', () => {
  assert.equal(isModified(null, 'anything'), true)
  assert.equal(isModified('k', 'k'), false)
  assert.equal(isModified('k', 'j'), true)
})

test('only iterated factors make the fan-out signature', () => {
  assert.equal(iterateSignature({ trial: 'iterate', subject: 'iterate', session: 'group' }), 'subject|trial')
  assert.equal(iterateSignature(null), '')
})

test('the notes summary separates format drift from missing data', () => {
  assert.equal(notesSummary([]), '')
  assert.equal(
    notesSummary([
      { path: 'kind', kind: 'invalid_value', message: '' },
      { path: 'show_sample', kind: 'not_in_data', message: '' },
    ]),
    "1 setting no longer applies; 1 not in today's data",
  )
  assert.equal(
    notesSummary([
      { path: 'a', kind: 'unknown_setting', message: '' },
      { path: 'b', kind: 'dropped_entry', message: '' },
    ]),
    '2 settings no longer apply',
  )
})

test('an unparseable timestamp is shown as stored', () => {
  assert.equal(savedAtLabel('not a date'), 'not a date')
  assert.match(savedAtLabel('2026-09-24T15:04:05+00:00'), /^2026-09-2\d \d\d:\d\d$/)
})
