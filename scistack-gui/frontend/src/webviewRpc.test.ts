/**
 * Tests for WebviewRpc — run with `npm test` in frontend/.
 *
 * The regression: every webview request carried a 30s timer. "Save figure"
 * asks the extension for a save dialog (`pick_save_path`) through the same
 * call, so a user who took more than 30s choosing where to save got "Could
 * not save: Request pick_save_path timed out", and the path they then picked
 * was dropped on arrival. Waiting on a person is not a lost request; the
 * extension's `rpcTimeoutMs` (extension/src/rpcPending.ts) is the one backstop.
 */

import { test, mock } from 'node:test';
import * as assert from 'node:assert';
import { RpcMessage, WebviewRpc } from './webviewRpc.js';

test('a dialog answered after ten minutes still resolves', async () => {
  mock.timers.enable({ apis: ['setTimeout', 'setInterval', 'Date'] });
  try {
    const sent: RpcMessage[] = [];
    const rpc = new WebviewRpc((msg) => { sent.push(msg); });
    const reply = rpc.call('pick_save_path', { defaultName: 'figure.png' });
    let rejected: unknown = null;
    reply.catch((err) => { rejected = err; });

    mock.timers.tick(10 * 60_000);
    await Promise.resolve();
    assert.equal(rejected, null, 'the webview gave up on a request by itself');

    assert.equal(sent.length, 1);
    assert.equal(rpc.handleResponse({ id: sent[0].id, result: { path: '/tmp/f.png' } }), true);
    assert.deepEqual(await reply, { path: '/tmp/f.png' });
  } finally {
    mock.timers.reset();
  }
});

test('an error reply rejects with the extension message', async () => {
  const sent: RpcMessage[] = [];
  const rpc = new WebviewRpc((msg) => { sent.push(msg); });
  const reply = rpc.call('plot_resolve', {});
  rpc.handleResponse({
    id: sent[0].id,
    error: { message: "SciStack: no response from the Python server for 'plot_resolve' after 300s." },
  });
  await assert.rejects(reply, /no response from the Python server/);
  assert.equal(rpc.size, 0);
});

test('responses are matched by id, not by order', async () => {
  const sent: RpcMessage[] = [];
  const rpc = new WebviewRpc((msg) => { sent.push(msg); });
  const a = rpc.call('get_schema', {});
  const b = rpc.call('get_info', {});
  rpc.handleResponse({ id: sent[1].id, result: 'info' });
  rpc.handleResponse({ id: sent[0].id, result: 'schema' });
  assert.equal(await a, 'schema');
  assert.equal(await b, 'info');
});

test('a response nobody is waiting for is refused', () => {
  const rpc = new WebviewRpc(() => {});
  assert.equal(rpc.handleResponse({ id: 42, result: null }), false);
});
