import { test } from 'node:test'
import assert from 'node:assert/strict'
import { formatLocation, isLockedForEditing, type EntityEditability } from './sourceLocation.js'

test('formatLocation keeps only the file name for POSIX paths', () => {
  assert.equal(formatLocation({ file: '/proj/src/params.py', line: 42 }), 'params.py:42')
})

test('formatLocation keeps only the file name for Windows paths', () => {
  // Seen 2026-09-23: a MATLAB-declared PathInput reported
  // Y:\...\src\main_entrypoint_aim2.m and the whole path was shown.
  assert.equal(
    formatLocation({ file: 'Y:\\Repo\\src\\main_entrypoint_aim2.m', line: 32 }),
    'main_entrypoint_aim2.m:32',
  )
})

test('formatLocation omits an unknown line', () => {
  assert.equal(formatLocation({ file: '/proj/entities.toml', line: null }), 'entities.toml')
})

const base: EntityEditability = { editable: true, reason: null, file: 'x', line: null, message: '' }

test('a read-only declaration locks the panel', () => {
  assert.equal(isLockedForEditing({ ...base, editable: false, reason: 'read_only' }), true)
})

test('editable, unknown and not-yet-answered all stay unlocked (fail open)', () => {
  assert.equal(isLockedForEditing(base), false)
  assert.equal(isLockedForEditing({ ...base, editable: false, reason: 'unknown' }), false)
  assert.equal(isLockedForEditing(null), false)
})
