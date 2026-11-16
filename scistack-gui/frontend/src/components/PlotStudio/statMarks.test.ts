import { test } from 'node:test'
import assert from 'node:assert/strict'
import type { PlotSpec } from '../../generated/api-types.js'
import { derivedFrom, marksKey, setStatMarks, toggleHiddenBracket, wanted, type MarksResult } from './statMarks.js'

const base = (): PlotSpec => ({
  measures: ['Ratings'],
  roles: { brand: 'group', subject: 'collapse' },
  groups: ['brand'],
  kind: 'bar',
  stat_marks: { brackets: true },
})
const FIELDS = ['measures', 'roles', 'groups', 'aggregate.pooled', 'filters']

test('nothing asked, no key and no request', () => {
  assert.equal(wanted(undefined), false)
  assert.equal(marksKey({ ...base(), stat_marks: {} }, FIELDS), null)
  assert.notEqual(marksKey(base(), FIELDS), null)
})

test('a display edit keeps the key; a design or stat_marks edit changes it', () => {
  const key = marksKey(base(), FIELDS)
  const recoloured = { ...base(), style: { marker_size: 9 }, kind: 'box', aggregate: { error: 'sem' } } as PlotSpec
  assert.equal(marksKey(recoloured, FIELDS), key, 'display fields never re-run a test')
  assert.notEqual(marksKey({ ...base(), groups: ['subject'] }, FIELDS), key)
  assert.notEqual(marksKey({ ...base(), aggregate: { pooled: true } } as PlotSpec, FIELDS), key)
  assert.notEqual(marksKey(setStatMarks(base(), { pairs: 'all' }), FIELDS), key)
  assert.notEqual(marksKey(base(), FIELDS, 'a.csv'), key)
})

test('without the design list every field is part of the key (never stale)', () => {
  const key = marksKey(base(), undefined)
  assert.notEqual(marksKey({ ...base(), kind: 'box' }, undefined), key)
})

test('hiding a bracket is a toggle on stat_marks.hidden', () => {
  let spec = toggleHiddenBracket(base(), 'c1')
  assert.deepEqual(spec.stat_marks?.hidden, ['c1'])
  assert.equal(spec.stat_marks?.brackets, true)
  spec = toggleHiddenBracket(spec, 'c1')
  assert.deepEqual(spec.stat_marks?.hidden, [])
})

test('derivedFrom sends nothing when nothing is drawn', () => {
  const empty: MarksResult = { bars: [], ids: [], panel_notes: [], caption: null, reason: 'none', notes: [], test: null }
  assert.equal(derivedFrom(null), null)
  assert.equal(derivedFrom(empty), null)
  assert.deepEqual(derivedFrom({ ...empty, caption: 'c' })?.caption, 'c')
})
