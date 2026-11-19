/**
 * Tests for mergePorts — run with `npm test` in frontend/.
 * Rule: docs/claude/merge-node.md (one empty trailing port, two on a fresh node,
 * slot ids never reused).
 */

import { test } from 'node:test'
import * as assert from 'node:assert'
import { mergePorts, mergeSlotOf } from './mergePorts.js'

const slots = (ports: ReturnType<typeof mergePorts>) => ports.map(p => [p.slot, p.connected])

test('a fresh Merge shows two empty ports', () => {
  assert.deepStrictEqual(slots(mergePorts([])), [[0, false], [1, false]])
})

test('one wired port still shows two, the second empty', () => {
  assert.deepStrictEqual(slots(mergePorts([0])), [[0, true], [1, false]])
})

test('two wired ports grow a third, empty', () => {
  assert.deepStrictEqual(slots(mergePorts([0, 1])), [[0, true], [1, true], [2, false]])
})

test('a disconnected middle port closes up without renaming', () => {
  // Slot 1 disconnected: 0 and 2 stay, the empty one is 3 (1 is never reused).
  assert.deepStrictEqual(slots(mergePorts([2, 0])), [[0, true], [2, true], [3, false]])
})

test('duplicate slots count once', () => {
  assert.deepStrictEqual(slots(mergePorts([0, 0, 1])), [[0, true], [1, true], [2, false]])
})

test('slot handles parse back', () => {
  assert.strictEqual(mergeSlotOf('in__m12'), 12)
  assert.strictEqual(mergeSlotOf('in__data'), null)
  assert.strictEqual(mergeSlotOf(undefined), null)
})
