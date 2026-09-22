/**
 * Tests for PanelRegistry — run with `npm test` in extension/.
 *
 * The regression these lock down: `plot_save_complete` was forwarded only to
 * the DAG webview, so a save started from the standalone Plot tab finished in
 * the backend (scidb.log 2026-09-11, "save job ps-n68e4u6j complete") while
 * the panel's Save button sat on "Saving…" and refused a second save. Plot
 * tabs are created after the notification handler is registered and there can
 * be more than one, so the forwarding needs a registry to address.
 */

import { test } from 'node:test';
import * as assert from 'node:assert';
import { MessageSink, PanelRegistry } from './panelRegistry';

class FakePanel implements MessageSink {
  received: Record<string, unknown>[] = [];
  throws = false;

  postMessage(msg: Record<string, unknown>): void {
    if (this.throws) throw new Error('webview disposed');
    this.received.push(msg);
  }
}

test('a notification reaches every registered panel', () => {
  const registry = new PanelRegistry();
  const a = new FakePanel();
  const b = new FakePanel();
  registry.add(a);
  registry.add(b);

  const msg = { method: 'plot_save_complete', params: { job_id: 'ps-1' } };
  assert.equal(registry.send(msg), 2);
  assert.deepEqual(a.received, [msg]);
  assert.deepEqual(b.received, [msg]);
});

test('a closed panel stops receiving', () => {
  const registry = new PanelRegistry();
  const open = new FakePanel();
  const closed = new FakePanel();
  registry.add(open);
  const unregister = registry.add(closed);

  unregister();
  assert.equal(registry.size, 1);
  assert.equal(registry.send({ method: 'plot_save_progress' }), 1);
  assert.equal(closed.received.length, 0);
  assert.equal(open.received.length, 1);
});

test('no open panels is not an error, and is reported as zero', () => {
  const registry = new PanelRegistry();
  // The caller logs this count: 0 is how a save whose completion had nowhere
  // to go becomes visible instead of silent.
  assert.equal(registry.send({ method: 'plot_save_complete' }), 0);
});

test('one dead webview does not block delivery to the others', () => {
  const registry = new PanelRegistry();
  const dead = new FakePanel();
  dead.throws = true;
  const alive = new FakePanel();
  registry.add(dead);
  registry.add(alive);

  assert.equal(registry.send({ method: 'plot_save_complete' }), 1);
  assert.equal(alive.received.length, 1);
});

test('registering the same panel twice delivers once', () => {
  const registry = new PanelRegistry();
  const panel = new FakePanel();
  registry.add(panel);
  registry.add(panel);

  assert.equal(registry.size, 1);
  assert.equal(registry.send({ method: 'plot_save_complete' }), 1);
});

// --- server restart -------------------------------------------------------
// A plot tab outlives the Python process it was opened with: `Restart Python`
// respawns the server while the tab stays open. The registry used to carry a
// `rebind` half for that — every panel held its own PythonProcess reference
// and had to be handed the replacement, or its next RPC wrote to the old
// process's destroyed stdin ("Could not open the plot panel: Error
// [ERR_STREAM_DESTROYED]", 2026-09-15).
//
// That half is gone, and so is the bug's cause: a panel now belongs to a
// Session and reads `session.python` at call time, so a restart that swaps
// the process is invisible to every tab. There is nothing left to rebind, and
// a registry with no backend handle in it cannot hand out a stale one.

test('the registry carries no backend handle to go stale', () => {
  const registry = new PanelRegistry();
  const panel = new FakePanel();
  registry.add(panel);

  // A message sink is the whole contract. If this ever grows a second
  // method, the "which process does this tab talk to" question has come
  // back and Session should answer it, not the registry.
  assert.deepEqual(Object.keys(registry.sinks()[0]), Object.keys(panel));
});

test('sinks() is a snapshot, so a panel may close while it is walked', () => {
  // Closing a session disposes its plot tabs, and each dispose unregisters
  // itself — iterating the live set would skip panels.
  const registry = new PanelRegistry();
  const a = new FakePanel();
  const b = new FakePanel();
  const removeA = registry.add(a);
  registry.add(b);

  const walked: MessageSink[] = [];
  for (const sink of registry.sinks()) {
    walked.push(sink);
    if (sink === a) removeA();
  }
  assert.deepEqual(walked, [a, b]);
  assert.equal(registry.size, 1);
});
