/**
 * Tests for runFailures — run with `npm test` in frontend/.
 *
 * Pins: a covered node is ASSIGNED its groups (a clean rerun clears the
 * badge), uncovered nodes keep theirs; the live count ignores no-data;
 * framework/library frames collapse between user frames.
 */

import { test } from 'node:test'
import * as assert from 'node:assert'
import {
  applyRunDone,
  clearNode,
  failedCount,
  firstRemedy,
  noteSkip,
  readRunFailures,
  segmentFrames,
} from './runFailures.js'
import type { FailureFrame, FailureGroup } from './runFailures.js'

function group(overrides: Partial<FailureGroup> = {}): FailureGroup {
  return {
    reason: "KeyError: 'chan'",
    fn_name: 'filt',
    exc_type: 'KeyError',
    message: "'chan'",
    no_data: false,
    count: 4,
    combos: ['subject=1'],
    user_frame: null,
    node_id: 'fn__a',
    ...overrides,
  }
}

const frame = (origin: FailureFrame['origin'], line = 1): FailureFrame =>
  ({ file: `/p/${origin}.py`, line, function: 'f', code: '', origin })

test('run_done assigns groups to covered nodes and clears clean ones', () => {
  let state = applyRunDone({}, readRunFailures({ by_node: { fn__a: [group()], fn__b: [group()] }, other: [] }), false)
  assert.strictEqual(failedCount(state.fn__a), 4)
  state = applyRunDone(state, readRunFailures({ by_node: { fn__a: [] }, other: [] }), false)
  assert.strictEqual(state.fn__a, undefined)
  assert.strictEqual(failedCount(state.fn__b), 4)
})

test('no-data groups never make a badge', () => {
  const state = applyRunDone({}, readRunFailures({ by_node: { fn__a: [group({ no_data: true })] }, other: [] }), false)
  assert.strictEqual(state.fn__a, undefined)
})

test('live count rises on combo_skip, ignores no-data, and run_done replaces it', () => {
  let state = noteSkip({}, 'fn__a', { reason: 'x', no_data: false, user_frame: null })
  state = noteSkip(state, 'fn__a', { reason: 'nd', no_data: true, user_frame: null })
  state = noteSkip(state, 'fn__a', null)
  assert.strictEqual(failedCount(state.fn__a), 1)
  state = applyRunDone(state, readRunFailures({ by_node: { fn__a: [group({ count: 9 })] }, other: [] }), true)
  assert.strictEqual(failedCount(state.fn__a), 9)
  assert.strictEqual(state.fn__a.stopped, true)
})

test('a new run clears the node', () => {
  const state = applyRunDone({}, readRunFailures({ by_node: { fn__a: [group()] }, other: [] }), false)
  assert.deepStrictEqual(clearNode(state, 'fn__a'), {})
})

test('older servers without failures read as nothing', () => {
  assert.deepStrictEqual(readRunFailures(undefined), { by_node: {}, other: [] })
})

test('framework and library frames collapse between user frames', () => {
  const segs = segmentFrames([
    frame('user', 1), frame('framework'), frame('library'), frame('user', 2), frame('library'),
  ])
  assert.deepStrictEqual(segs.map(s => s.kind), ['frame', 'collapsed', 'frame', 'collapsed'])
  assert.strictEqual(segs[1].kind === 'collapsed' && segs[1].frames.length, 2)
})

const REMEDY = {
  kind: 'duplicate_location',
  title: 'More than one row for the same subject in Demo',
  explanation: 'e',
  items: [{ location: 'subject=2455369', count: 2, differing_columns: ['age'] }],
  n_more: 0,
  steps: [{ title: 'Change the data', text: 't', action: 'reveal_function' as const }],
}

test('firstRemedy picks the first group with a remedy, nodes before node-less', () => {
  const wire = readRunFailures({
    by_node: { fn__clean: [], fn__a: [group(), group({ reason: 'OutputSaveError: x', remedy: REMEDY })] },
    other: [group({ remedy: { ...REMEDY, kind: 'other' } })],
  })
  const target = firstRemedy(wire)
  assert.strictEqual(target?.nodeId, 'fn__a')
  assert.strictEqual(target?.group.remedy?.kind, 'duplicate_location')
})

test('firstRemedy falls back to node-less groups, and is null without remedies', () => {
  const other = readRunFailures({ by_node: { fn__a: [group()] }, other: [group({ remedy: REMEDY })] })
  assert.strictEqual(firstRemedy(other)?.nodeId, '')
  const none = readRunFailures({ by_node: { fn__a: [group({ remedy: null })] }, other: [] })
  assert.strictEqual(firstRemedy(none), null)
})
