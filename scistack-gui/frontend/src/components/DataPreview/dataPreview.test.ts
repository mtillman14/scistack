import test from 'node:test'
import assert from 'node:assert/strict'
import { cellText, describeLoadedTypes, hasMixedTypes, type ColumnPreview } from './dataPreview'

const column = (loaded_types: Record<string, number>): ColumnPreview => ({
  name: 'tone', stored_type: 'VARCHAR', loaded_types, non_null: 3, n_rows: 4, samples: [],
})

test('loaded types read as counts, and all-blank says so', () => {
  assert.equal(describeLoadedTypes({ str: 2, float: 1 }), 'str x2, float x1')
  assert.equal(describeLoadedTypes({}), 'none')
})

test('more than one loaded type is flagged', () => {
  assert.equal(hasMixedTypes(column({ str: 2, float: 1 })), true)
  assert.equal(hasMixedTypes(column({ str: 3 })), false)
})

test('a blank cell is empty, a zero is not', () => {
  assert.equal(cellText(null), '')
  assert.equal(cellText(0), '0')
})
