/**
 * Tests for canvasRefreshLog — run with `npm test` in frontend/.
 *
 * 2026-10-22: a just-placed node/edge disappears for a moment, then comes
 * back. These pin the verdicts the diagnostics report into scistack.log.
 */

import { test } from 'node:test'
import * as assert from 'node:assert'
import { CanvasRefreshLog, PENDING_TTL_MS, edgeMatchKey, nodeMatchKey } from './canvasRefreshLog.js'

const T = { get_pipeline_ms: 700, get_layout_ms: 150 }

test('a refresh started before the write was confirmed is a stale snapshot', () => {
  const log = new CanvasRefreshLog()
  const r = log.start('dag_updated:db_watcher', 1000)
  log.addPending('fn__x__a', 'node', 1100)
  log.confirm('fn__x__a', 1300)
  const report = log.apply(r, { scope: 'main', nodeIds: ['var__A'], edgeIds: [] }, T, 1900)
  assert.strictEqual(report.dropped.length, 1)
  assert.deepStrictEqual(report.dropped[0], {
    kind: 'node', id: 'fn__x__a', age_ms: 800, confirm_after_start_ms: 300,
    verdict: 'stale_snapshot',
  })
})

test('an unconfirmed write is a stale snapshot too', () => {
  const log = new CanvasRefreshLog()
  const r = log.start('dag_updated:server', 1000)
  log.addPending('manual__e', 'edge', 900)
  const report = log.apply(r, { scope: 'main', nodeIds: [], edgeIds: [] }, T, 1800)
  assert.strictEqual(report.dropped[0].verdict, 'stale_snapshot')
  assert.strictEqual(report.dropped[0].confirm_after_start_ms, null)
})

test('missing after a refresh that started past the confirm is server_omitted', () => {
  const log = new CanvasRefreshLog()
  log.addPending('fn__x__a', 'node', 1000)
  log.confirm('fn__x__a', 1200)
  const r = log.start('dag_updated:server', 1250)
  const report = log.apply(r, { scope: 'main', nodeIds: [], edgeIds: [] }, T, 2000)
  assert.strictEqual(report.dropped[0].verdict, 'server_omitted')
  assert.strictEqual(report.dropped[0].confirm_after_start_ms, -50)
})

test('a dropped item stays pending; the refresh that carries it settles it', () => {
  const log = new CanvasRefreshLog()
  log.addPending('manual__e', 'edge', 0)
  const first = log.apply(log.start('a', 10), { scope: 'main', nodeIds: [], edgeIds: [] }, T, 100)
  assert.strictEqual(first.dropped.length, 1)
  const second = log.apply(log.start('b', 200), { scope: 'main', nodeIds: [], edgeIds: ['manual__e'] }, T, 300)
  assert.strictEqual(second.dropped.length, 0)
  assert.strictEqual(second.settled, 1)
  assert.strictEqual(log.pendingCount(), 0)
})

test('superseded_by names the newer refresh already started', () => {
  const log = new CanvasRefreshLog()
  const older = log.start('a', 0)
  log.start('b', 10)
  const report = log.apply(older, { scope: 'main', nodeIds: [], edgeIds: [] }, T, 50)
  assert.strictEqual(report.superseded_by, 2)
  assert.strictEqual(log.apply(log.start('c', 60), { scope: 'main', nodeIds: [], edgeIds: [] }, T, 70).superseded_by, null)
})

test('pending items expire after the TTL and forget() drops them', () => {
  const log = new CanvasRefreshLog()
  log.addPending('old', 'node', 0)
  log.addPending('refused', 'node', 0)
  log.forget('refused')
  const report = log.apply(log.start('a', 0), { scope: 'main', nodeIds: [], edgeIds: [] }, T, PENDING_TTL_MS + 1)
  assert.strictEqual(report.dropped.length, 0)
  assert.strictEqual(log.pendingCount(), 0)
})

test('an edge returned under the history id for its connection is folded, not dropped', () => {
  const log = new CanvasRefreshLog()
  log.addPending('manual__e', 'edge', 0, edgeMatchKey('var__A', 'fn__f__1', 'in__a'))
  const report = log.apply(log.start('a', 10), {
    scope: 'main', nodeIds: [], edgeIds: ['e__var__A__fn__f__1__a'],
    matchKeys: [edgeMatchKey('var__A', 'fn__f__1', 'in__a')],
  }, T, 100)
  assert.strictEqual(report.dropped.length, 0)
  assert.strictEqual(report.folded, 1)
})

test('entity boxes fold by type+label; function nodes never do', () => {
  assert.strictEqual(nodeMatchKey('variableNode', 'RawGait'), 'node|variableNode|RawGait')
  assert.strictEqual(nodeMatchKey('functionNode', 'filter'), null)
  const log = new CanvasRefreshLog()
  log.addPending('var__RawGait__x1', 'node', 0, nodeMatchKey('variableNode', 'RawGait'))
  const report = log.apply(log.start('a', 10), {
    scope: 'main', nodeIds: ['var__RawGait'], edgeIds: [],
    matchKeys: [nodeMatchKey('variableNode', 'RawGait')!],
  }, T, 100)
  assert.strictEqual(report.folded, 1)
  assert.strictEqual(report.dropped.length, 0)
})
