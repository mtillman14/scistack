import { test } from 'node:test'
import assert from 'node:assert/strict'

import { FigureStepTimer } from './figureStepTiming.js'

test('one step splits into queued, round trip and draw', () => {
  const t = new FigureStepTimer()
  t.stepped(2, 1000)
  t.sent(2, 1180)
  t.answered(2, 2080)
  assert.deepEqual(t.drawn(2, 2200), {
    figure_index: 2,
    clicks: 1,
    queued_ms: 180,
    round_trip_ms: 900,
    draw_ms: 120,
    total_ms: 1200,
  })
  // Reported once.
  assert.equal(t.drawn(2, 2300), null)
})

test('rapid clicks are one step to the last figure asked for', () => {
  const t = new FigureStepTimer()
  t.stepped(1, 0)
  t.sent(1, 180)
  t.stepped(2, 300)
  t.stepped(3, 400)
  // The answer for figure 1 lands and is drawn: not the end of the step.
  t.answered(1, 1000)
  assert.equal(t.drawn(1, 1050), null)
  t.sent(3, 1050)
  t.answered(3, 1900)
  const step = t.drawn(3, 2000)
  assert.ok(step)
  assert.equal(step.clicks, 3)
  assert.equal(step.queued_ms, 650)
  assert.equal(step.round_trip_ms, 850)
  assert.equal(step.total_ms, 2000)
})

test('a draw with no arrow click is not timed', () => {
  const t = new FigureStepTimer()
  t.sent(0, 0)
  t.answered(0, 100)
  assert.equal(t.drawn(0, 150), null)
})

test('a draw before the answer is not the end of the step', () => {
  // The old figure re-drawing (theme, resize) while the request runs.
  const t = new FigureStepTimer()
  t.stepped(4, 0)
  t.sent(4, 180)
  assert.equal(t.drawn(4, 200), null)
  t.answered(4, 900)
  assert.equal(t.drawn(4, 950)?.total_ms, 950)
})

test('reset forgets an unfinished step', () => {
  const t = new FigureStepTimer()
  t.stepped(1, 0)
  t.sent(1, 180)
  t.reset()
  t.answered(1, 900)
  assert.equal(t.drawn(1, 950), null)
})
