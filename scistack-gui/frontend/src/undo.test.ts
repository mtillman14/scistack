/**
 * The undo runner against a fake backend — run with `npm test` in frontend/.
 * docs/claude/undo-redo.md.
 */

import { test } from 'node:test'
import * as assert from 'node:assert'
import { UndoStack, taskChange, type Send } from './undo.js'

type Reply = { status: string; conflicts?: string[] }

function fakeBackend(replies: Record<string, Reply[]>) {
  const calls: string[] = []
  const send: Send = async (method, params) => {
    calls.push(`${method}:${params.change_id}`)
    const queue = replies[params.change_id as string] ?? []
    return queue.shift() ?? { status: 'ok' }
  }
  return { send, calls }
}

test('undo then redo of a server step', async () => {
  const { send, calls } = fakeBackend({})
  const stack = new UndoStack(send, 't')
  stack.pushServer('c1', 'connect')
  await stack.undo()
  assert.deepEqual(calls, ['history_undo:c1'])
  assert.equal(stack.getSnapshot().redoLabel, 'connect')
  await stack.redo()
  assert.deepEqual(calls, ['history_undo:c1', 'history_redo:c1'])
  assert.equal(stack.getSnapshot().undoLabel, 'connect')
})

test('a step that changed nothing is skipped: one key press still does something', async () => {
  const { send, calls } = fakeBackend({ c2: [{ status: 'empty' }] })
  const stack = new UndoStack(send, 't')
  stack.pushServer('c1', 'a')
  stack.pushServer('c2', 'b')
  await stack.undo()
  assert.deepEqual(calls, ['history_undo:c2', 'history_undo:c1'])
  assert.equal(stack.getSnapshot().canUndo, false)
})

test('a conflict drops the step, says why, and leaves the rest', async () => {
  const { send } = fakeBackend({ c2: [{ status: 'conflict', conflicts: ['scistack.toml was edited since'] }] })
  const stack = new UndoStack(send, 't')
  stack.pushServer('c1', 'a')
  stack.pushServer('c2', 'b')
  await stack.undo()
  const snap = stack.getSnapshot()
  assert.match(snap.notice, /Cannot undo "b".*scistack.toml/)
  assert.equal(snap.undoLabel, 'a')
  assert.equal(snap.canRedo, false)
})

test('a transport failure keeps the step for another try', async () => {
  const send: Send = async () => { throw new Error('offline') }
  const stack = new UndoStack(send, 't')
  stack.pushServer('c1', 'a')
  await stack.undo()
  assert.equal(stack.getSnapshot().undoLabel, 'a')
  assert.match(stack.getSnapshot().notice, /offline/)
})

test('a second press while one is in flight does nothing', async () => {
  let release: () => void = () => {}
  const calls: string[] = []
  const send: Send = (method, params) => {
    calls.push(`${method}:${params.change_id}`)
    return new Promise(resolve => { release = () => resolve({ status: 'ok' }) })
  }
  const stack = new UndoStack(send, 't')
  stack.pushServer('c1', 'a')
  stack.pushServer('c2', 'b')
  const first = stack.undo()
  await stack.undo()  // returns at once: busy
  release()
  await first
  assert.deepEqual(calls, ['history_undo:c2'])
})

test('local steps apply their values', async () => {
  const stack = new UndoStack(async () => ({ status: 'ok' }), 't')
  let value = 0
  const apply = (v: number) => { value = v }
  value = 1
  stack.pushLocal({ kind: 'local', label: 'edit', before: 0, after: 1, apply, at: 1 })
  await stack.undo()
  assert.equal(value, 0)
  await stack.redo()
  assert.equal(value, 1)
})

test('onServerApplied runs after a server step moves', async () => {
  const stack = new UndoStack(async () => ({ status: 'ok' }), 't')
  const seen: string[] = []
  stack.onServerApplied = (entry, direction) => seen.push(`${direction}:${entry.changeId}`)
  stack.pushServer('c1', 'alias')
  await stack.undo()
  assert.deepEqual(seen, ['undo:c1'])
})

test('requests started in one task share a change; the next task gets a new one', async () => {
  const a = taskChange()
  const b = taskChange()
  assert.equal(a, b)
  await new Promise(resolve => setTimeout(resolve, 0))
  assert.notEqual(taskChange().id, a.id)
})

test('clear forgets every step', () => {
  const stack = new UndoStack(async () => ({ status: 'ok' }), 't')
  stack.pushServer('c1', 'a')
  stack.clear()
  assert.equal(stack.getSnapshot().canUndo, false)
})
