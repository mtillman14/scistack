/**
 * Tests for webviewBuild + checkWebviewVersion — run with `npm test` in extension/.
 *
 * Regression for 2026-10-16: VS Code ran a hand-built 0.1.0 copy of the
 * extension whose webview bundle predated Copy PNG, under scistack-gui
 * 0.1.37, and nothing anywhere said which bundle the panel had loaded.
 */

import { test } from 'node:test';
import * as assert from 'node:assert';
import { checkWebviewVersion, DEV_VERSION } from './versionCheck';
import { prepareBuildReport } from './webviewBuild';

const EXTENSION = {
  version: '0.1.37',
  path: 'C:\\Users\\me\\.vscode\\extensions\\scistack.scistack-gui-0.1.37',
  mode: 'Production',
};

const BUNDLE = {
  version: '0.1.37',
  commit: 'abc1234',
  built_at: '2026-10-15T22:14:00.000Z',
  target: 'webview',
  view: 'plot',
};

test('a bundle from the server release is a match', () => {
  assert.deepStrictEqual(checkWebviewVersion('0.1.37', '0.1.37'), { kind: 'match' });
});

test('a bundle from another release warns and names both versions', () => {
  const v = checkWebviewVersion('0.1.30', '0.1.37');
  assert.strictEqual(v.kind, 'mismatch');
  if (v.kind === 'mismatch') {
    assert.match(v.message, /0\.1\.30/);
    assert.match(v.message, /0\.1\.37/);
    assert.match(v.message, /Show Running Extensions/);
  }
});

test('a server too old to report its version is a mismatch', () => {
  assert.strictEqual(checkWebviewVersion('0.1.37', undefined).kind, 'mismatch');
});

test('dev builds on either side never warn (same rule as the extension check)', () => {
  assert.strictEqual(checkWebviewVersion(DEV_VERSION, '0.1.37').kind, 'skipped');
  assert.strictEqual(checkWebviewVersion('0.1.37', '0.1.38.dev2+gabc').kind, 'skipped');
  assert.strictEqual(checkWebviewVersion(null, '0.1.37').kind, 'skipped');
});

test('the forwarded report carries the bundle, the loading extension, and the verdict', () => {
  const r = prepareBuildReport({ ...BUNDLE, version: '0.1.30' }, EXTENSION, '0.1.37');
  assert.strictEqual(r.verdict.kind, 'mismatch');
  assert.strictEqual(r.params.version, '0.1.30');
  assert.strictEqual(r.params.commit, 'abc1234');
  assert.strictEqual(r.params.extension_path, EXTENSION.path);
  assert.strictEqual(r.params.extension_version, '0.1.37');
  assert.strictEqual(r.params.extension_mode, 'Production');
  assert.strictEqual(r.params.verdict, 'mismatch');
  assert.match(String(r.params.verdict_detail), /0\.1\.30/);
});

test('the output-channel line names the path the bundle was loaded from', () => {
  const r = prepareBuildReport(BUNDLE, { ...EXTENSION, mode: 'Development' }, '0.1.37');
  assert.strictEqual(r.verdict.kind, 'match');
  assert.match(r.line, /plot view: bundle 0\.1\.37/);
  assert.match(r.line, /commit abc1234/);
  assert.ok(r.line.includes(EXTENSION.path), r.line);
  assert.match(r.line, /Development/);
  assert.match(r.line, /— match$/);
});

test('a skipped verdict says why, in the line and the params', () => {
  const r = prepareBuildReport({ ...BUNDLE, version: DEV_VERSION }, EXTENSION, '0.1.37');
  assert.strictEqual(r.verdict.kind, 'skipped');
  assert.match(r.line, /unstamped dev build/);
  assert.match(String(r.params.verdict_detail), /unstamped dev build/);
});

test('a malformed report still produces a line instead of throwing', () => {
  const r = prepareBuildReport(undefined, EXTENSION, '0.1.37');
  assert.match(r.line, /unknown view: bundle \(not reported\)/);
  assert.strictEqual(r.verdict.kind, 'skipped');
});
