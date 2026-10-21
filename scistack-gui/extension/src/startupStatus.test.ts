/**
 * Tests for startupStatus — run with `npm test` in extension/.
 *
 * Pins the text of the progress notification shown while a server starts.
 */

import { test } from 'node:test';
import * as assert from 'node:assert';
import { formatElapsed, startupStatusMessage, STARTUP_INITIAL_STATUS } from './startupStatus';

test('before any server progress the initial status is shown', () => {
  assert.strictEqual(startupStatusMessage(undefined, 0), `${STARTUP_INITIAL_STATUS} (0s)`);
  assert.strictEqual(startupStatusMessage('   ', 2000), `${STARTUP_INITIAL_STATUS} (2s)`);
});

test('the latest server progress line is shown with elapsed time', () => {
  assert.strictEqual(
    startupStatusMessage('Importing 3/25: gait.py', 12_400),
    'Importing 3/25: gait.py (12s)',
  );
});

test('elapsed time switches to minutes past 60 seconds', () => {
  assert.strictEqual(formatElapsed(59_999), '59s');
  assert.strictEqual(formatElapsed(60_000), '1m 00s');
  assert.strictEqual(formatElapsed(125_000), '2m 05s');
  assert.strictEqual(formatElapsed(-5), '0s');
});
