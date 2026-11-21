import { test } from 'node:test'
import assert from 'node:assert/strict'
import { dataLines, undoLine, valuesForRequest, type DeletePlanData } from './deletePlan.js'

const opts = [
  { value: 10, declared: true, has_data: true },
  { value: 20, declared: true, has_data: false },
  { value: 30, declared: false, has_data: true },
]

test('every value ticked is the whole Parameter', () => {
  assert.equal(valuesForRequest(opts, new Set([0, 1, 2])), null)
  assert.equal(valuesForRequest([], new Set()), null)
})

test('a subset sends those values', () => {
  assert.deepEqual(valuesForRequest(opts, new Set([0, 2])), [10, 30])
  assert.deepEqual(valuesForRequest(opts, new Set()), [])
})

const data: DeletePlanData = {
  total_records: 6,
  by_variable: { Filtered: 4, Steps: 2 },
  downstream_by_variable: { Steps: 2 },
  lost_locations: { Filtered: [{}, {}] },
  fingerprint: 'f',
  undoable: true,
}

test('data lines name counts, downstream and lost locations', () => {
  assert.deepEqual(dataLines(data), [
    'Filtered — 4 records, 2 locations left with none',
    'Steps — 2 records, all computed downstream',
  ])
})

test('undo line says whether the data comes back', () => {
  assert.match(undoLine(data), /data included/)
  assert.match(undoLine({ ...data, undoable: false }), /CANNOT/)
  assert.equal(undoLine({ ...data, total_records: 0 }), 'Undo with Ctrl/Cmd+Z.')
})
