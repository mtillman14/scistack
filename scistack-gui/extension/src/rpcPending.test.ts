/**
 * Tests for PendingRequests — run with `npm test` in extension/.
 *
 * This is the one place a request to the Python server is timed. The webview
 * used to arm its own 30s timer in front of it, which both overrode
 * `scistack.rpcTimeoutMs` and failed "Save figure" whenever the user spent
 * more than 30s in the save dialog. These lock down that the backstop still
 * exists here (a lost response must not wedge the caller forever) and that a
 * late answer is reported rather than silently dropped.
 */

import { test, mock } from 'node:test';
import * as assert from 'node:assert';
import { PendingRequests } from './rpcPending';

function sink(): { lines: string[]; appendLine(line: string): void } {
  const lines: string[] = [];
  return { lines, appendLine: (line: string) => { lines.push(line); } };
}

test('a response settles its request', async () => {
  const log = sink();
  const table = new PendingRequests(log);
  const reply = table.open(1, 'plot_save_start', 0);
  assert.equal(table.resolve(1, { job_id: 'ps-1' }), true);
  assert.deepEqual(await reply, { job_id: 'ps-1' });
  assert.equal(table.size, 0);
});

test('an error response rejects with the server message and is logged', async () => {
  const log = sink();
  const table = new PendingRequests(log);
  const reply = table.open(1, 'plot_resolve', 0);
  table.reject(1, 'bad spec');
  await assert.rejects(reply, /bad spec/);
  assert.ok(log.lines.some((l) => l.startsWith('RPC error: plot_resolve (id=1')));
});

test('a request with no response is rejected at rpcTimeoutMs, not before', async () => {
  mock.timers.enable({ apis: ['setTimeout', 'Date'] });
  try {
    const log = sink();
    const table = new PendingRequests(log);
    const reply = table.open(7, 'get_pipeline', 300_000);
    let settled = false;
    reply.then(() => { settled = true; }, () => { settled = true; });

    // Well past the webview's old 30s timer: still waiting.
    mock.timers.tick(299_000);
    await Promise.resolve();
    assert.equal(settled, false);

    mock.timers.tick(1_000);
    await assert.rejects(reply, /no response from the Python server for 'get_pipeline'/);
    assert.ok(log.lines.some((l) => l.startsWith('RPC timeout: get_pipeline (id=7)')));
    assert.equal(table.size, 0);
  } finally {
    mock.timers.reset();
  }
});

test('timeout 0 disables the backstop', async () => {
  mock.timers.enable({ apis: ['setTimeout', 'Date'] });
  try {
    const table = new PendingRequests(sink());
    const reply = table.open(1, 'plot_resolve', 0);
    mock.timers.tick(24 * 3600_000);
    assert.equal(table.resolve(1, 'ok'), true);
    assert.equal(await reply, 'ok');
  } finally {
    mock.timers.reset();
  }
});

test('a response after the timeout is logged as late, with the method and delay', async () => {
  mock.timers.enable({ apis: ['setTimeout', 'Date'] });
  try {
    const log = sink();
    const table = new PendingRequests(log);
    const reply = table.open(3, 'plot_resolve', 10_000);
    mock.timers.tick(10_000);
    await assert.rejects(reply);

    mock.timers.tick(5_000);
    assert.equal(table.resolve(3, 'too late'), false);
    const late = log.lines.find((l) => l.startsWith('RPC late response:'));
    assert.ok(late, `no late-response line in ${JSON.stringify(log.lines)}`);
    assert.match(late!, /plot_resolve \(id=3\) answered after 15000ms, 5000ms after/);
  } finally {
    mock.timers.reset();
  }
});

test('a response for an id never sent is logged as unknown', () => {
  const log = sink();
  const table = new PendingRequests(log);
  assert.equal(table.resolve(99, null), false);
  assert.ok(log.lines.some((l) => l.includes('unknown request id=99')));
});

test('failAll rejects every outstanding request', async () => {
  const table = new PendingRequests(sink());
  const a = table.open(1, 'a', 0);
  const b = table.open(2, 'b', 0);
  table.failAll(new Error('Python process exited'));
  await assert.rejects(a, /exited/);
  await assert.rejects(b, /exited/);
  assert.equal(table.size, 0);
});
