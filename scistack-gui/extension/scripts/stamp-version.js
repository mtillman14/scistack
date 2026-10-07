#!/usr/bin/env node
/**
 * Stamp package.json's version from the release tag — CI only, never committed.
 *
 * The git tag `vX.Y.Z` is the ONE owner of the version: hatch-vcs reads it for
 * every Python package, and this script makes the extension a second consumer
 * of the same tag. package.json keeps the placeholder `0.0.0` in git.
 *
 * The tag comes from GITHUB_REF_NAME (a tag-triggered Actions run), or is
 * passed as the first argument. Anything but a plain `X.Y.Z` is refused: the
 * Marketplace has no pre-release version spelling, and the PyPI job refuses
 * the same tags, so both sides fail together.
 *
 *   node scripts/stamp-version.js            # uses $GITHUB_REF_NAME
 *   node scripts/stamp-version.js v0.1.30
 */
'use strict';

const fs = require('fs');
const path = require('path');

/** `v0.1.30` -> `0.1.30`; throws on anything that is not a clean release tag. */
function versionFromTag(tag) {
  const m = /^v(\d+\.\d+\.\d+)$/.exec(String(tag ?? '').trim());
  if (!m) {
    throw new Error(
      `Release tag must look like vX.Y.Z, got ${JSON.stringify(tag)}. ` +
        'Pre-release / dev tags cannot be published to the Marketplace.',
    );
  }
  return m[1];
}

function stamp(packageJsonPath, version) {
  const text = fs.readFileSync(packageJsonPath, 'utf8');
  const pkg = JSON.parse(text);
  const previous = pkg.version;
  pkg.version = version;
  fs.writeFileSync(packageJsonPath, JSON.stringify(pkg, null, 2) + '\n');
  return previous;
}

if (require.main === module) {
  const tag = process.argv[2] ?? process.env.GITHUB_REF_NAME;
  try {
    const version = versionFromTag(tag);
    const file = path.join(__dirname, '..', 'package.json');
    const previous = stamp(file, version);
    console.log(`[stamp-version] tag ${tag} -> package.json version ${previous} -> ${version}`);
  } catch (err) {
    console.error(`[stamp-version] ${err.message}`);
    process.exit(1);
  }
}

module.exports = { versionFromTag, stamp };
