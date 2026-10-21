/**
 * Tests for inputPorts — run with `npm test` in frontend/.
 *
 * Regression 2026-10-21: load_xsens_one_file(xsens_file_path, col_names,
 * sheet_name) drew col_names (Parameter-fed) third, after the unwired
 * sheet_name, because variable-fed ports were spread first.
 */

import { test } from 'node:test'
import * as assert from 'node:assert'
import { inputPorts } from './inputPorts.js'

test('draws input_ports in the order sent, Parameter-fed port in the middle', () => {
  const ports = inputPorts({
    input_ports: ['xsens_file_path', 'col_names', 'sheet_name'],
    input_params: { xsens_file_path: '', sheet_name: '' },
    constant_params: ['col_names'],
  })
  assert.deepStrictEqual(ports.map(p => p.name), ['xsens_file_path', 'col_names', 'sheet_name'])
})

test('carries the wired variable type, empty for a Parameter-fed port', () => {
  const ports = inputPorts({
    input_ports: ['signal', 'hz'],
    input_params: { signal: 'Raw' },
    constant_params: ['hz'],
  })
  assert.deepStrictEqual(ports, [
    { name: 'signal', type: 'Raw' },
    { name: 'hz', type: '' },
  ])
})

test('without input_ports, falls back to the names carried, no duplicates', () => {
  const ports = inputPorts({
    input_params: { a: 'A', b: '' },
    constant_params: ['b', 'c'],
  })
  assert.deepStrictEqual(ports.map(p => p.name), ['a', 'b', 'c'])
})
