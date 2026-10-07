/**
 * Tests for stamp-version.js — run by `npm test`.
 *
 * Pins that the extension's published version is exactly the tag the PyPI
 * packages were built from, and that tags the Marketplace cannot take fail
 * the release instead of shipping a mangled version.
 */
'use strict';

const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { versionFromTag, stamp } = require('./stamp-version');

test('a release tag gives its version without the v', () => {
  assert.strictEqual(versionFromTag('v0.1.30'), '0.1.30');
  assert.strictEqual(versionFromTag('v1.20.300'), '1.20.300');
});

test('non-release tags are refused', () => {
  for (const bad of ['0.1.30', 'v0.1', 'v0.1.30rc1', 'v0.1.30.dev2', 'v0.1.30-beta.1', 'main', '', undefined]) {
    assert.throws(() => versionFromTag(bad), /vX\.Y\.Z/, `accepted ${bad}`);
  }
});

test('stamp rewrites only the version field', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'stamp-'));
  const file = path.join(dir, 'package.json');
  fs.writeFileSync(file, JSON.stringify({ name: 'x', version: '0.0.0', main: 'm.js' }, null, 2));
  const previous = stamp(file, '0.1.30');
  assert.strictEqual(previous, '0.0.0');
  assert.deepStrictEqual(JSON.parse(fs.readFileSync(file, 'utf8')), {
    name: 'x',
    version: '0.1.30',
    main: 'm.js',
  });
});

test('the committed package.json carries the dev placeholder', () => {
  // Matches versionCheck.DEV_VERSION; a real version committed here would
  // be a second owner of the release version, drifting from the tag.
  const pkg = require('../package.json');
  assert.strictEqual(pkg.version, '0.0.0');
});
