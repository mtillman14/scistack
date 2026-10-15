import { test } from 'node:test'
import assert from 'node:assert/strict'

import { adoptedFigureIndex } from './figureCursor.js'

test('an answer for the position the user is on is adopted', () => {
  assert.equal(adoptedFigureIndex(3, 3, 3), 3)
})

test('a clamp for the position the user is on is adopted', () => {
  // The fan-out shrank from 12 figures to 5 while the cursor sat on 10.
  assert.equal(adoptedFigureIndex(10, 10, 4), 4)
})

test('an answer the user has clicked past never drags the cursor back', () => {
  // Request sent for figure 1; the user is on 3 by the time it lands.
  assert.equal(adoptedFigureIndex(1, 3, 1), null)
  assert.equal(adoptedFigureIndex(4, 2, 4), null)
})

test('an error answer leaves the cursor alone', () => {
  assert.equal(adoptedFigureIndex(2, 2, undefined), null)
  assert.equal(adoptedFigureIndex(2, 2, null), null)
})

/** Replays the panel's resolve loop: one request in flight, the latest
 *  cursor queued behind it, every answer offered to `adoptedFigureIndex`.
 *  Returns the cursor once nothing is left to send — or throws if it never
 *  settles, which is what the alternation bug did. */
function simulate(clicks: number[]): number {
  let cursor = 0
  let inFlight: number | null = null
  let queued: number | null = null
  const send = (index: number) => {
    if (inFlight === null) inFlight = index
    else queued = index
  }
  send(cursor)
  for (const target of clicks) {
    cursor = target
    send(cursor)
  }
  for (let round = 0; round < 100; round++) {
    if (inFlight === null) return cursor
    const answered: number = inFlight
    inFlight = null
    const adopted = adoptedFigureIndex(answered, cursor, answered)
    const moved = adopted !== null && adopted !== cursor
    if (moved) cursor = adopted
    if (queued !== null) {
      inFlight = queued
      queued = null
    }
    // A changed cursor schedules a resolve for itself, as the panel's effect does.
    if (moved) send(cursor)
  }
  throw new Error('the cursor never settled')
}

test('rapid clicking settles on the last figure clicked', () => {
  assert.equal(simulate([1, 2, 3]), 3)
  assert.equal(simulate([1, 2, 3, 4, 5, 6, 7, 6, 5]), 5)
  assert.equal(simulate([1, 0, 1, 0, 1]), 1)
})
