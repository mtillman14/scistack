import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  colNamesAfterSelectionChange,
  hasIterateInput,
  withColName,
} from './colNameInputs.js'

test('hasIterateInput is true only when some selection iterates', () => {
  assert.equal(hasIterateInput({}), false)
  assert.equal(hasIterateInput({ df: { iterate: false } }), false)
  assert.equal(hasIterateInput({ df: { iterate: false }, other: { iterate: true } }), true)
  assert.equal(hasIterateInput({ df: undefined }), false)
})

test('withColName adds sorted without repeats and removes', () => {
  assert.deepEqual(withColName([], 'label', true), ['label'])
  assert.deepEqual(withColName(['z'], 'a', true), ['a', 'z'])
  assert.deepEqual(withColName(['a'], 'a', true), ['a'])
  assert.deepEqual(withColName(['a', 'b'], 'a', false), ['b'])
  assert.deepEqual(withColName([], 'a', false), [])
})

test('unticking the last per-column input clears every ColName', () => {
  assert.deepEqual(colNamesAfterSelectionChange(['label'], { df: { iterate: false } }), [])
  assert.deepEqual(colNamesAfterSelectionChange(['label'], {}), [])
})

test('ColName is left alone while an input still iterates, or when there is none', () => {
  assert.equal(colNamesAfterSelectionChange(['label'], { df: { iterate: true } }), null)
  assert.equal(colNamesAfterSelectionChange([], {}), null)
})
