/**
 * Tests for versionCheck — run with `npm test` in extension/.
 *
 * The extension and the scistack-gui PyPI package share one release tag but
 * are updated independently; these pin when the extension warns.
 */

import { test } from 'node:test';
import * as assert from 'node:assert';
import { checkServerVersion, DEV_VERSION } from './versionCheck';

test('same release is a match', () => {
  assert.deepStrictEqual(checkServerVersion('0.1.30', '0.1.30'), { kind: 'match' });
});

test('different releases warn with the pip fix for the extension version', () => {
  const v = checkServerVersion('0.1.30', '0.1.29');
  assert.strictEqual(v.kind, 'mismatch');
  if (v.kind === 'mismatch') {
    assert.match(v.message, /0\.1\.29/);
    assert.match(v.message, /pip install scistack-gui==0\.1\.30/);
  }
});

test('a server too old to report its version is a mismatch', () => {
  assert.strictEqual(checkServerVersion('0.1.30', undefined).kind, 'mismatch');
  assert.strictEqual(checkServerVersion('0.1.30', null).kind, 'mismatch');
});

test('an unstamped dev extension never warns', () => {
  assert.strictEqual(checkServerVersion(DEV_VERSION, '0.1.29').kind, 'skipped');
});

test('an editable / dev Python install never warns', () => {
  assert.strictEqual(checkServerVersion('0.1.29', '0.1.30.dev4').kind, 'skipped');
  assert.strictEqual(checkServerVersion('0.1.29', '0.1.30.dev4+gabc123').kind, 'skipped');
});
