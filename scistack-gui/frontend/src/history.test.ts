/**
 * The undo stack's rules — run with `npm test` in frontend/.
 * docs/claude/undo-redo.md.
 */

import { test } from 'node:test'
import * as assert from 'node:assert'
import {
  EMPTY,
  MAX_ENTRIES,
  MERGE_WINDOW_MS,
  dropNotice,
  dropped,
  movedToFuture,
  movedToPast,
  outcome,
  peekRedo,
  peekUndo,
  pushLocal,
  pushServer,
  type LocalEntry,
} from './history.js'

function local(before: number, after: number, at: number, mergeKey = 'spec'): LocalEntry<number> {
  return { kind: 'local', label: 'edit', before, after, apply: () => {}, mergeKey, at }
}

test('one gesture = one step: the same change id is not pushed twice', () => {
  let s = pushServer(EMPTY, 'c1', 'move node')
  s = pushServer(s, 'c1', 'move node')
  assert.equal(s.past.length, 1)
  s = pushServer(s, 'c2', 'connect')
  s = pushServer(s, 'c1', 'move node')  // interleaved request of the first gesture
  assert.equal(s.past.length, 2)
})

test('a new change clears the redo side', () => {
  let s = pushServer(EMPTY, 'c1', 'a')
  s = movedToFuture(s, peekUndo(s)!)
  assert.equal(peekRedo(s)?.label, 'a')
  s = pushServer(s, 'c2', 'b')
  assert.equal(s.future.length, 0)
})

test('typing merges into one step, keeping the first before', () => {
  let s = pushLocal(EMPTY, local(0, 1, 1000))
  s = pushLocal(s, local(1, 2, 1000 + MERGE_WINDOW_MS - 1))
  assert.equal(s.past.length, 1)
  const top = peekUndo(s) as LocalEntry<number>
  assert.equal(top.before, 0)
  assert.equal(top.after, 2)
})

test('a pause starts a new step', () => {
  let s = pushLocal(EMPTY, local(0, 1, 1000))
  s = pushLocal(s, local(1, 2, 1000 + MERGE_WINDOW_MS + 1))
  assert.equal(s.past.length, 2)
})

test('the window slides: steady typing stays one step', () => {
  let s = EMPTY
  for (let i = 0; i < 10; i++) s = pushLocal(s, local(i, i + 1, 1000 + i * (MERGE_WINDOW_MS - 50)))
  assert.equal(s.past.length, 1)
})

test('a different merge key or a server step in between does not merge', () => {
  let s = pushLocal(EMPTY, local(0, 1, 1000, 'a'))
  s = pushLocal(s, local(1, 2, 1001, 'b'))
  assert.equal(s.past.length, 2)
  s = pushServer(s, 'c1', 'alias')
  s = pushLocal(s, local(2, 3, 1002, 'b'))
  assert.equal(s.past.length, 4)
})

test('an edit that ends where it began leaves no step', () => {
  let s = pushLocal(EMPTY, local(5, 5, 1000))
  assert.equal(s.past.length, 0)
  s = pushLocal(s, local(0, 1, 1000))
  s = pushLocal(s, local(1, 0, 1100))
  assert.equal(s.past.length, 0, 'merged back to the start')
})

test('moves are by identity, so an edit pushed meanwhile is untouched', () => {
  let s = pushServer(EMPTY, 'c1', 'a')
  const undone = peekUndo(s)!
  s = pushServer(s, 'c2', 'b')  // arrived while c1's undo was in flight
  s = movedToFuture(s, undone)
  assert.deepEqual(s.past.map(e => e.label), ['b'])
  assert.deepEqual(s.future.map(e => e.label), ['a'])
  s = movedToPast(s, undone)
  assert.deepEqual(s.past.map(e => e.label), ['b', 'a'])
})

test('dropped removes the entry from either side', () => {
  let s = pushServer(EMPTY, 'c1', 'a')
  const e = peekUndo(s)!
  s = dropped(s, e)
  assert.equal(s.past.length, 0)
})

test('the stack is bounded', () => {
  let s = EMPTY
  for (let i = 0; i < MAX_ENTRIES + 5; i++) s = pushServer(s, `c${i}`, 'x')
  assert.equal(s.past.length, MAX_ENTRIES)
  assert.equal((s.past[0] as { changeId: string }).changeId, 'c5')
})

test('what each backend answer means', () => {
  assert.equal(outcome('ok'), 'move')
  assert.equal(outcome('noop'), 'skip')
  assert.equal(outcome('empty'), 'skip')
  assert.equal(outcome('unknown'), 'drop')
  assert.equal(outcome('conflict'), 'drop')
  assert.match(dropNotice('undo', 'connect', 'conflict', ['_intent(x) changed since']), /Cannot undo "connect".*_intent/)
  assert.match(dropNotice('redo', 'connect', 'unknown'), /no longer available/)
})
