import test from 'node:test'
import assert from 'node:assert/strict'
import { buildRows, cellFor, dropOffText, filterRows, locationKey, parseKey, type Column } from './dataView.js'

const KEYS = ['subject', 'trial']

function column(variable: string, keys: string[], cells: Record<string, string>): Column {
  return {
    variable, keys, resolved: true, counts: {}, footer: '', present: 0, counted: 0, unchecked: 0, notes: [],
    cells: Object.fromEntries(Object.entries(cells).map(([k, s]) => [k, { state: s as never, glyph: '', detail: '', record_ids: [] }])),
  }
}

test('locationKey is the Python spelling and round-trips', () => {
  assert.equal(locationKey([['subject', '01'], ['trial', '3']], KEYS), 'subject=01|trial=3')
  assert.equal(locationKey({ trial: '3' }, KEYS), 'trial=3')
  assert.deepEqual(parseKey('subject=01|trial=a=b'), { subject: '01', trial: 'a=b' })
})

test('rows follow the hierarchy, numbers in numeric order', () => {
  const rows = buildRows(['subject=2|trial=10', 'subject=2|trial=9', 'subject=10|trial=1'], KEYS)
  assert.deepEqual(rows.map(r => r.value), ['2', '10'])
  assert.deepEqual(rows[0].children.map(r => r.value), ['9', '10'])
})

test('a trial-level column: leaf cell, and the worst state on the subject row', () => {
  const c = column('Filt', KEYS, { 'subject=1|trial=1': 'present', 'subject=1|trial=2': 'failed' })
  assert.equal(cellFor(c, [['subject', '1'], ['trial', '1']])?.state, 'present')
  assert.equal(cellFor(c, [['subject', '1']])?.state, 'failed')
})

test('a subject-level column shows its cell on every trial row', () => {
  const c = column('Mean', ['subject'], { 'subject=1': 'outdated' })
  assert.equal(cellFor(c, [['subject', '1'], ['trial', '7']])?.state, 'outdated')
})

test('filtering keeps the bad leaves and their parents', () => {
  const rows = buildRows(['subject=1|trial=1', 'subject=1|trial=2', 'subject=2|trial=1'], KEYS)
  const c = column('Filt', KEYS, {
    'subject=1|trial=1': 'present', 'subject=1|trial=2': 'failed', 'subject=2|trial=1': 'present',
  })
  const kept = filterRows(rows, [c], ['failed'])
  assert.deepEqual(kept.map(r => r.value), ['1'])
  assert.deepEqual(kept[0].children.map(r => r.value), ['2'])
  assert.equal(filterRows(rows, [c], []).length, 2)
})

test('the drop-off line', () => {
  assert.equal(dropOffText([
    { variable: 'A', key: 'subject', count: 40, lost: [], lost_count: 0 },
    { variable: 'B', key: 'subject', count: 38, lost: [], lost_count: 2 },
  ]), 'subjects: 40 → 38')
})
