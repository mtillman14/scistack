import { test } from 'node:test'
import assert from 'node:assert/strict'
import { declaredNote, impactSummary, numericCheckbox, type SchemaKeyTypeInfo } from './schemaKeyTypes.js'

const info = (key_type: SchemaKeyTypeInfo['key_type']): SchemaKeyTypeInfo => ({
  key: 'trial', key_type, loads_as: 'strings', n_values: 3,
})

test('a placeholder that is not a schema key gets no checkbox', () => {
  assert.equal(numericCheckbox(undefined, false).shown, false)
})

test('undeclared is unchecked and editable; numeric is checked', () => {
  const undeclared = numericCheckbox(info(null), false)
  assert.equal(undeclared.shown, true)
  assert.equal(undeclared.checked, false)
  assert.equal(undeclared.disabled, false)
  assert.match(undeclared.title, /Project-wide/)
  assert.equal(numericCheckbox(info('numeric'), false).checked, true)
  assert.equal(numericCheckbox(info('numeric'), true).disabled, true)
})

test('a declared string is shown read-only with the way to change it', () => {
  const box = numericCheckbox(info('string'), false)
  assert.equal(box.checked, false)
  assert.equal(box.disabled, true)
  assert.match(box.title, /scidb set-key-type trial none/)
})

test('impactSummary names respellings, merges and the re-run', () => {
  const text = impactSummary({
    key: 'trial', from: null, to: 'numeric', n_values: 3,
    respelled: [['001', '1']], merged: { '1': ['001', '1'] }, invalid: [],
  })
  assert.match(text, /1 of 3 stored value\(s\)/)
  assert.match(text, /'001' → '1'/)
  assert.match(text, /'001' \+ '1' → '1'/)
  assert.match(text, /re-run/)
})

test('declaredNote is empty when nothing was declared', () => {
  assert.equal(declaredNote([]), '')
  assert.match(declaredNote(['trial']), /'trial' declared numeric/)
})
