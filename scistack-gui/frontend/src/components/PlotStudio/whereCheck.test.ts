/**
 * The where= check's frontend half: the request a chip click builds, the
 * edit of one row, and the rows Apply writes back. The verdicts and the
 * factors are Python's (scistackplotdb/tests/test_where_check.py).
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import { editCheckFilter, rowsToApply, whereCheckOpen } from './whereCheck.js'

const FILTERS = [
  { variable: 'Age', column: null, op: '>', value: '80' },
  { variable: 'Sex', column: null, op: '==', value: 'M' },
]

test('a chip click opens a histogram of the filter variable', () => {
  const open = whereCheckOpen({ nodeId: 'fn__f__1', functionName: 'f', filters: FILTERS, filterIndex: 0, against: null })
  assert.equal(open.variable, 'Age')
  assert.equal(open.where_check.against, false)
  assert.equal(open.where_check.filter_index, 0)
})

test('plot against opens on the node input, with every filter ticked', () => {
  const open = whereCheckOpen({ nodeId: 'fn__f__1', functionName: 'f', filters: FILTERS, filterIndex: 1, against: 'Distance' })
  assert.equal(open.variable, 'Distance')
  assert.equal(open.where_check.against, true)
  assert.deepEqual((open.where_check.filters as { enabled: boolean }[]).map(f => f.enabled), [true, true])
})

test('editing one row leaves the others alone', () => {
  const spec = { where_check: { node_id: 'n', filters: [{ variable: 'Age', op: '>', value: '80', enabled: true }, { variable: 'Sex', op: '==', value: 'M', enabled: true }] } }
  const next = editCheckFilter(spec, 0, { value: '70' })
  assert.equal(next.where_check?.filters?.[0].value, '70')
  assert.equal(next.where_check?.filters?.[1].value, 'M')
  assert.equal(spec.where_check.filters[0].value, '80', 'the spec is not mutated')
})

test('an edit with no check, or past the end, changes nothing', () => {
  const none = {}
  assert.equal(editCheckFilter(none, 0, { value: '1' }), none)
  const spec = { where_check: { filters: [{ variable: 'Age', op: '>', value: '80' }] } }
  assert.equal(editCheckFilter(spec, 5, { value: '1' }), spec)
})

test('Apply writes only the ticked rows, without the tick', () => {
  const rows = rowsToApply({
    node_id: 'n',
    filters: [
      { variable: 'Age', op: '>', value: '80', enabled: true },
      { variable: 'Sex', op: '==', value: 'M', enabled: false },
    ],
  })
  assert.deepEqual(rows, [{ variable: 'Age', column: null, op: '>', value: '80' }])
})
