#!/usr/bin/env node
/**
 * Run `node --test` on every *.test.{js,mjs,cjs} file under the given folders.
 *
 * Why not `node --test dist/test`: Node 20 treats a folder argument as "run
 * the tests in it", but Node 22+ reads every argument as a file glob, so a
 * bare folder matches nothing and fails with MODULE_NOT_FOUND. Listing the
 * files explicitly works the same on every version. The one runner for both
 * `npm test`s (frontend and extension/package.json).
 *
 * Usage: node scripts/node-test.mjs <folder> [<folder> ...]
 */

import { readdirSync, statSync } from 'node:fs'
import { join } from 'node:path'
import { spawnSync } from 'node:child_process'

const TEST_FILE = /\.test\.(c|m)?js$/

function collect(path, out) {
  if (statSync(path).isDirectory()) {
    for (const name of readdirSync(path).sort()) collect(join(path, name), out)
  } else if (TEST_FILE.test(path)) {
    out.push(path)
  }
  return out
}

const folders = process.argv.slice(2)
if (folders.length === 0) {
  console.error('usage: node scripts/node-test.mjs <folder> [<folder> ...]')
  process.exit(2)
}
const files = folders.flatMap(f => collect(f, []))
console.log(`[node-test] ${files.length} test file(s) under ${folders.join(', ')} (node ${process.version})`)
if (files.length === 0) {
  console.error('[node-test] no *.test.js files found: did tsc emit into that folder?')
  process.exit(1)
}
const result = spawnSync(process.execPath, ['--test', ...files], { stdio: 'inherit' })
process.exit(result.status ?? 1)
