import { test } from 'node:test'
import assert from 'node:assert/strict'
import { currentPorts, movePort, newPortName, renamePort } from './outputPorts.js'

test('currentPorts: declared, else handles, else one default port', () => {
  assert.deepEqual(currentPorts(['mean', 'peak'], ['A']), ['mean', 'peak'])
  assert.deepEqual(currentPorts(undefined, ['SixMWT', 'TenMWT']), ['SixMWT', 'TenMWT'])
  assert.deepEqual(currentPorts([], []), ['output'])
})

test('newPortName never repeats', () => {
  assert.equal(newPortName([]), 'output')
  assert.equal(newPortName(['output']), 'output_2')
  assert.equal(newPortName(['output', 'output_2']), 'output_3')
})

test('movePort and renamePort', () => {
  assert.deepEqual(movePort(['a', 'b', 'c'], 2, -1), ['a', 'c', 'b'])
  assert.deepEqual(movePort(['a', 'b'], 0, -1), ['a', 'b'])
  assert.deepEqual(renamePort(['a', 'b'], 1, ' peak '), { ports: ['a', 'peak'], renames: { b: 'peak' } })
  assert.deepEqual(renamePort(['a', 'b'], 1, 'b'), { ports: ['a', 'b'], renames: {} })
  assert.deepEqual(renamePort(['a', 'b'], 1, '  '), { ports: ['a', 'b'], renames: {} })
})
