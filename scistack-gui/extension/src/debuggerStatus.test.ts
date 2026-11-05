/**
 * Tests for debuggerStatus — run with `npm test` in extension/.
 *
 * Regression: Run attached to 127.0.0.1:5678 even when the server's debugpy
 * listener had failed to start, so every Run logged "connection refused".
 */

import { test } from 'node:test';
import * as assert from 'node:assert';
import {
  STOP_ON_ERROR_FILTERS,
  attachConfiguration,
  comboLabel,
  decideAttach,
  stoppedMessage,
} from './debuggerStatus';

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

test('the attach treats SciStack as library code', () => {
  const config = attachConfiguration('SciStack', 5678);
  assert.strictEqual(config.justMyCode, true);
  assert.deepStrictEqual(config.connect, { host: '127.0.0.1', port: 5678 });
  for (const module of ['scifor', 'scidb', 'scistack_gui']) {
    assert.ok(config.rules.some(r => r.module === module && r.include === false), module);
  }
});

test('stopping uses the user-uncaught filter', () => {
  assert.deepStrictEqual(STOP_ON_ERROR_FILTERS, ['userUnhandled']);
});

test('the combination names schema keys, not the loop counter', () => {
  assert.strictEqual(comboLabel({ subject: '2455369', bout: 1, __combo: '7' }), 'subject=2455369, bout=1');
  assert.strictEqual(comboLabel(null), '');
});

test('the stop message says where', () => {
  assert.match(stoppedMessage('parse_data', 'subject=2455369'), /parse_data stopped on an error at subject=2455369/);
  assert.match(stoppedMessage(undefined, ''), /the function stopped on an error\. /);
});
