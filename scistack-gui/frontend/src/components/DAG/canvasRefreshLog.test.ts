/**
 * Tests for canvasRefreshLog — run with `npm test` in frontend/.
 *
 * 2026-10-22: a just-placed node/edge disappears for a moment, then comes
 * back. These pin the verdicts the diagnostics report into scistack.log.
 */

import { test } from 'node:test'
import * as assert from 'node:assert'
import {
  CanvasRefreshLog, PENDING_TTL_MS, edgeMatchKey, nodeMatchKey, overlayEdges, overlayNodes,
} from './canvasRefreshLog.js'

const T = { get_pipeline_ms: 700, get_layout_ms: 150 }

test('a refresh started before the write was confirmed is a stale snapshot, and keeps it', () => {
  const log = new CanvasRefreshLog()
  const r = log.start('dag_updated:db_watcher', 1000)
  log.addPending('fn__x__a', 'node', 1100)
  log.confirm('fn__x__a', 1300)
  const report = log.apply(r, { scope: 'main', nodeIds: ['var__A'], edgeIds: [] }, T, 1900)
  assert.strictEqual(report.dropped.length, 0)
  assert.deepStrictEqual(report.kept[0], {
    kind: 'node', id: 'fn__x__a', age_ms: 800, confirm_after_start_ms: 300,
    verdict: 'stale_snapshot',
  })
})

test('an unconfirmed write is a stale snapshot too', () => {
  const log = new CanvasRefreshLog()
  const r = log.start('dag_updated:server', 1000)
  log.addPending('manual__e', 'edge', 900)
  const report = log.apply(r, { scope: 'main', nodeIds: [], edgeIds: [] }, T, 1800)
  assert.strictEqual(report.kept[0].verdict, 'stale_snapshot')
  assert.strictEqual(report.kept[0].confirm_after_start_ms, null)
})

test('missing after a refresh that started past the confirm is server_omitted', () => {
  const log = new CanvasRefreshLog()
  log.addPending('fn__x__a', 'node', 1000)
  log.confirm('fn__x__a', 1200)
  const r = log.start('dag_updated:server', 1250)
  const report = log.apply(r, { scope: 'main', nodeIds: [], edgeIds: [] }, T, 2000)
  assert.strictEqual(report.dropped[0].verdict, 'server_omitted')
  assert.strictEqual(report.dropped[0].confirm_after_start_ms, -50)
  assert.deepStrictEqual(report.kept, [], 'a real server omission is not papered over')
})

test('a dropped item stays pending; the refresh that carries it settles it', () => {
  const log = new CanvasRefreshLog()
  log.addPending('manual__e', 'edge', 0)
  const first = log.apply(log.start('a', 10), { scope: 'main', nodeIds: [], edgeIds: [] }, T, 100)
  assert.strictEqual(first.kept.length, 1)
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

test('a response for a canvas the user left is discarded', () => {
  // scistack-debug.log 2026-11-04: #13 for main came back after #14 drew the submodule.
  const log = new CanvasRefreshLog()
  const r13 = log.start('dag_updated:db_watcher', 0)
  const r14 = log.start('effect:graph_version=1', 10)
  assert.strictEqual(log.whyDiscard(r14, 'sub', 'sub'), null)
  log.apply(r14, { scope: 'sub', nodeIds: [], edgeIds: [] }, T, 1500)
  assert.strictEqual(log.whyDiscard(r13, 'main', 'sub'), 'scope_changed')
})

test('leaving a canvas discards its response even before anything newer is drawn', () => {
  const log = new CanvasRefreshLog()
  const r = log.start('a', 0)
  assert.strictEqual(log.whyDiscard(r, 'main', 'sub'), 'scope_changed')
})

test('a response older than the picture on screen is discarded', () => {
  const log = new CanvasRefreshLog()
  const older = log.start('a', 0)
  const newer = log.start('b', 10)
  log.apply(newer, { scope: 'main', nodeIds: [], edgeIds: [] }, T, 100)
  assert.strictEqual(log.whyDiscard(older, 'main', 'main'), 'newer_applied')
})

test('superseded but with nothing newer drawn yet is still drawn', () => {
  // The newer one may fail, and steady traffic must not starve the canvas.
  const log = new CanvasRefreshLog()
  const older = log.start('a', 0)
  log.start('b', 10)
  assert.strictEqual(log.whyDiscard(older, 'main', 'main'), null)
})

test('A to B and back: a response from the first visit is still the open canvas', () => {
  const log = new CanvasRefreshLog()
  const firstVisit = log.start('a', 0)
  const b = log.start('b', 10)
  const secondVisit = log.start('c', 20)
  assert.strictEqual(log.whyDiscard(b, 'B', 'A'), 'scope_changed')
  assert.strictEqual(log.whyDiscard(firstVisit, 'A', 'A'), null)
  log.apply(secondVisit, { scope: 'A', nodeIds: [], edgeIds: [] }, T, 100)
  assert.strictEqual(log.whyDiscard(firstVisit, 'A', 'A'), 'newer_applied')
})

test('a discarded refresh settles nothing, and the next applied one still can', () => {
  const log = new CanvasRefreshLog()
  log.addPending('fn__x__a', 'node', 0)
  const r = log.start('a', 10)
  const report = log.discard(r, 'main', 'scope_changed', T, 50)
  assert.strictEqual(report.discarded, 'scope_changed')
  assert.strictEqual(report.settled, 0)
  assert.deepStrictEqual(report.dropped, [])
  assert.strictEqual(log.pendingCount(), 1)
  const next = log.apply(log.start('b', 60), { scope: 'main', nodeIds: ['fn__x__a'], edgeIds: [] }, T, 70)
  assert.strictEqual(next.settled, 1)
  assert.strictEqual(next.discarded, null)
})

test('a discarded refresh does not count as the picture on screen', () => {
  const log = new CanvasRefreshLog()
  const older = log.start('a', 0)
  log.discard(log.start('b', 10), 'main', 'scope_changed', T, 50)
  assert.strictEqual(log.whyDiscard(older, 'main', 'main'), null)
})

// ---- The overlay (GitHub #23, scistack-debug.log 2026-11-18) ----

type N = { id: string }
type E = { id: string; source: string; target: string }

test('#23: five stale refreshes in a row keep the new edge; the first fresh one settles it', () => {
  // manual__apy990 was wiped by five refreshes before #63 drew it, 8 s later.
  const log = new CanvasRefreshLog()
  const stale = [1, 2, 3, 4, 5].map(i => log.start(`r${i}`, i * 100))
  const edge: E = { id: 'manual__apy990', source: 'var__A', target: 'fn__f__1' }
  log.addPending(edge.id, 'edge', 600)
  log.confirm(edge.id, 2600)
  for (const r of stale) {
    const report = log.apply(r, { scope: 'main', nodeIds: [], edgeIds: [] }, T, 3000)
    assert.deepStrictEqual(report.dropped, [])
    assert.deepStrictEqual(overlayEdges([], [edge], report), [edge])
  }
  const fresh = log.apply(log.start('r63', 2700), { scope: 'main', nodeIds: [], edgeIds: [edge.id] }, T, 8600)
  assert.strictEqual(fresh.settled, 1)
  assert.strictEqual(log.pendingCount(), 0)
})

test('#23: a stale snapshot does not redraw a node just deleted, nor its edges', () => {
  // delete_layout var__FilteredEMG at 11:40:15; refreshes #58-#60 still drew it.
  const log = new CanvasRefreshLog()
  const r58 = log.start('dag_updated:db_watcher', 0)
  log.addRemoval('var__FilteredEMG', 'node', 100)
  log.confirmRemoval('var__FilteredEMG', 400)
  const snapNodes: N[] = [{ id: 'var__FilteredEMG' }, { id: 'fn__f__1' }]
  const snapEdges: E[] = [{ id: 'e1', source: 'fn__f__1', target: 'var__FilteredEMG' }]
  const report = log.apply(r58, { scope: 'main', nodeIds: snapNodes.map(n => n.id), edgeIds: ['e1'] }, T, 2500)
  assert.deepStrictEqual(report.suppressed, [{ kind: 'node', id: 'var__FilteredEMG' }])
  assert.deepStrictEqual(overlayNodes(snapNodes, [], report), [{ id: 'fn__f__1' }])
  assert.deepStrictEqual(overlayEdges(snapEdges, [], report), [])
  // #61 started after the confirm and no longer carries it: settled.
  const r61 = log.start('dag_updated:put_layout', 500)
  log.apply(r61, { scope: 'main', nodeIds: ['fn__f__1'], edgeIds: [] }, T, 3000)
  assert.strictEqual(log.removalCount(), 0)
})

test('a removal the server still draws after its confirm comes back (returned)', () => {
  const log = new CanvasRefreshLog()
  log.addRemoval('e__a__b', 'edge', 0)
  log.confirmRemoval('e__a__b', 100)
  const report = log.apply(log.start('a', 200), { scope: 'main', nodeIds: [], edgeIds: ['e__a__b'] }, T, 900)
  assert.deepStrictEqual(report.suppressed, [])
  assert.deepStrictEqual(report.returned, [{ kind: 'edge', id: 'e__a__b' }])
  assert.strictEqual(log.removalCount(), 0)
})

test('an unconfirmed delete is suppressed; a failed one is forgotten', () => {
  const log = new CanvasRefreshLog()
  log.addRemoval('manual__x', 'edge', 0)
  const r = log.apply(log.start('a', 500), { scope: 'main', nodeIds: [], edgeIds: ['manual__x'] }, T, 900)
  assert.strictEqual(r.suppressed.length, 1)
  log.forgetRemoval('manual__x')
  const r2 = log.apply(log.start('b', 1000), { scope: 'main', nodeIds: [], edgeIds: ['manual__x'] }, T, 1100)
  assert.deepStrictEqual(r2.suppressed, [])
})

test('drawn then deleted before any snapshot: the add stops being pending', () => {
  const log = new CanvasRefreshLog()
  log.addPending('manual__x', 'edge', 0)
  log.addRemoval('manual__x', 'edge', 50)
  assert.strictEqual(log.pendingCount(), 0)
  const report = log.apply(log.start('a', 10), { scope: 'main', nodeIds: [], edgeIds: [] }, T, 900)
  assert.deepStrictEqual(report.kept, [])
})

test('deleted then placed again (same entity id): drawn, not suppressed', () => {
  const log = new CanvasRefreshLog()
  log.addRemoval('var__X', 'node', 0)
  log.addPending('var__X', 'node', 50)
  assert.strictEqual(log.removalCount(), 0)
})

test('overlay keeps a pending node only if it is on screen, never twice', () => {
  const report = { kept: [{ kind: 'node' as const, id: 'fn__new' }], suppressed: [] }
  assert.deepStrictEqual(overlayNodes([{ id: 'a' }], [], report), [{ id: 'a' }])
  assert.deepStrictEqual(
    overlayNodes([{ id: 'a' }, { id: 'fn__new' }], [{ id: 'fn__new' }], report),
    [{ id: 'a' }, { id: 'fn__new' }],
  )
})
