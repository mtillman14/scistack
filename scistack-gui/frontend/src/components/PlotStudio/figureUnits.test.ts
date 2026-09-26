import { test } from 'node:test'
import assert from 'node:assert/strict'

import { fromInches, parseSizeUnit, toInches, unitInfo } from './figureUnits.js'

const DPI = 200

test('inches show as millimetres and saved-raster pixels', () => {
  assert.equal(fromInches(8, 'in', DPI), 8)
  assert.equal(fromInches(8, 'mm', DPI), 203.2)
  assert.equal(fromInches(8, 'px', DPI), 1600)
  assert.equal(fromInches(3.5, 'mm', DPI), 88.9)
})

test('a typed value reads back as itself in its own unit', () => {
  for (const [value, unit] of [[1601, 'px'], [89, 'mm'], [7.25, 'in'], [1590, 'px']] as const) {
    assert.equal(fromInches(toInches(value, unit, DPI), unit, DPI), value)
  }
})

test('switching units converts, never rewrites the stored inches', () => {
  const stored = toInches(1600, 'px', DPI)
  assert.equal(stored, 8)
  assert.equal(fromInches(stored, 'mm', DPI), 203.2)
})

test('an unknown remembered unit falls back to inches', () => {
  assert.equal(parseSizeUnit('px'), 'px')
  assert.equal(parseSizeUnit('furlong'), 'in')
  assert.equal(parseSizeUnit(null), 'in')
  assert.equal(unitInfo('px').step, 10)
})
