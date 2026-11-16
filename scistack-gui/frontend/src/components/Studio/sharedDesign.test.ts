import { test } from 'node:test'
import assert from 'node:assert/strict'
import { designOf, mergeDesign, sameDesign } from './sharedDesign.js'

test('design binding copies design fields only, dotted paths included', () => {
  const fields = ['measures', 'groups', 'aggregate.pooled']
  const plot = { measures: ['A'], groups: ['g'], kind: 'bar', aggregate: { pooled: true, error: 'sd' } }
  const stat = { measures: ['B'], kind: 'box', aggregate: { error: 'sem' }, style: { x: 1 } }
  const merged = mergeDesign(stat, plot, fields)
  assert.deepEqual(merged.measures, ['A'])
  assert.deepEqual(merged.groups, ['g'])
  assert.equal(merged.kind, 'box', 'display fields stay')
  assert.deepEqual(merged.aggregate, { error: 'sem', pooled: true })
  assert.equal(sameDesign(merged, plot, fields), true)
  assert.equal(sameDesign(stat, plot, fields), false)
  assert.equal('groups' in mergeDesign(plot, { measures: ['A'] }, fields), false, 'a missing field is removed')
  assert.equal(designOf({}, fields).measures, null)
})
