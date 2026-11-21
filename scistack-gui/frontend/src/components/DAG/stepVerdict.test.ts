import test from 'node:test'
import assert from 'node:assert/strict'
import { chipTone, statesForReason, verdictTitle, type StepVerdict } from './stepVerdict.js'

function verdict(over: Partial<StepVerdict> = {}): StepVerdict {
  return {
    function_name: 'f', word: 'succeeded', glyph: '✅', label: 'Succeeded',
    status_line: '✅ Succeeded · 4 of 4 present', chip: '', new_chip: '', reasons: [],
    new: 0, warnings: 0, excluded: 0, counted: 4, present: 4, needs_running: false, ...over,
  }
}

test('no chip for a step that succeeded with nothing to say', () => {
  assert.equal(chipTone(verdict()), 'none')
  assert.equal(chipTone(undefined), 'none')
})

test('a failure or a missing input is a problem; not run or outdated is a warning', () => {
  const failed = verdict({ word: 'not_complete', chip: '✕ 3 failed', reasons: [{ kind: 'failed', count: 3, glyph: '✕', text: '✕ 3 failed' }] })
  assert.equal(chipTone(failed), 'problem')
  const stale = verdict({ word: 'not_complete', chip: '↑ inputs changed', reasons: [{ kind: 'inputs_changed', count: 2, glyph: '↑', text: '↑ inputs changed' }] })
  assert.equal(chipTone(stale), 'warning')
  const unplugged = verdict({ chip: '✂ input not connected', reasons: [{ kind: 'disconnected', count: null, glyph: '✂', text: '✂ input not connected' }] })
  assert.equal(chipTone(unplugged), 'problem')
})

test('warnings are amber, exclusions alone are informational', () => {
  assert.equal(chipTone(verdict({ word: 'succeeded_with_warnings', chip: '⚠ 2 warnings', warnings: 2 })), 'warning')
  assert.equal(chipTone(verdict({ chip: '⊘ 4 excluded', excluded: 4 })), 'info')
})

test('the tooltip lists the status line once, then the reasons', () => {
  const v = verdict({ status_line: 'X', reasons: [{ kind: 'failed', count: 1, glyph: '✕', text: '✕ 1 failed' }], notes: ['n'] })
  assert.equal(verdictTitle(v), 'X\n✕ 1 failed\nn')
})

test('a reason opens the data view on the states that explain it', () => {
  assert.deepEqual(statesForReason('code_changed'), ['outdated'])
  assert.deepEqual(statesForReason('not_run'), ['not_run', 'new'])
  assert.deepEqual(statesForReason(undefined), [])
})
