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
