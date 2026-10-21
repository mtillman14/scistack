/**
 * Tests for debuggerStatus — run with `npm test` in extension/.
 *
 * Regression: Run attached to 127.0.0.1:5678 even when the server's debugpy
 * listener had failed to start, so every Run logged "connection refused".
 */

import { test } from 'node:test';
import * as assert from 'node:assert';
import { decideAttach } from './debuggerStatus';

test('debug off never attaches', () => {
  assert.deepStrictEqual(
    decideAttach(false, { state: 'listening', port: 5678, error: null }, 5678),
    { attach: false },
  );
});

test('a listening server is attached on the port it reports', () => {
  assert.deepStrictEqual(
    decideAttach(true, { state: 'listening', port: 5679, error: null }, 5678),
    { attach: true, port: 5679 },
  );
});

test('a failed listener is not attached, and says how to fix it', () => {
  const d = decideAttach(
    true,
    { state: 'failed', port: 5678, error: "ModuleNotFoundError: No module named 'debugpy'" },
    5678,
  );
  assert.strictEqual(d.attach, false);
  if (!d.attach) {
    assert.match(d.message ?? '', /No module named 'debugpy'/);
    assert.match(d.message ?? '', /pip install debugpy/);
  }
});

test('a server started without debug asks for a restart', () => {
  const d = decideAttach(true, { state: 'off', port: null, error: null }, undefined);
  assert.strictEqual(d.attach, false);
  if (!d.attach) assert.match(d.message ?? '', /restart/);
});

test('a server older than the status field attaches as before', () => {
  assert.deepStrictEqual(decideAttach(true, undefined, 5678), { attach: true, port: 5678 });
  assert.deepStrictEqual(decideAttach(true, undefined, undefined), { attach: false });
});
