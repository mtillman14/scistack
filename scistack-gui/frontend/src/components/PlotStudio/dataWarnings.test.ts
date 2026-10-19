import { test } from 'node:test'
import assert from 'node:assert/strict'
import { unseenWarnings } from './dataWarnings.js'

test('a warning is shown once, however many resolves repeat it', () => {
  const shown = new Set<string>()
  assert.deepEqual(unseenWarnings(shown, ['all NaN']), ['all NaN'])
  assert.deepEqual(unseenWarnings(shown, ['all NaN']), [])
  assert.deepEqual(unseenWarnings(shown, ['all NaN']), [])
})

test('a different message is new', () => {
  const shown = new Set<string>(['all NaN in A'])
  assert.deepEqual(unseenWarnings(shown, ['all NaN in A', 'all NaN in B']), ['all NaN in B'])
})

test('a response without the field shows nothing', () => {
  assert.deepEqual(unseenWarnings(new Set(), undefined), [])
  assert.deepEqual(unseenWarnings(new Set(), []), [])
})
