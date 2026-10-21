import { test } from 'node:test'
import assert from 'node:assert/strict'
import { withPattern, withoutPattern } from './pathInputKeyRegex.js'

test('withPattern sets or replaces one key and never mutates the input', () => {
  const before = { speed: '[A-Za-z]+' }
  const after = withPattern(before, 'subject', 'R2_\\d{3}')
  assert.deepEqual(after, { speed: '[A-Za-z]+', subject: 'R2_\\d{3}' })
  assert.deepEqual(before, { speed: '[A-Za-z]+' })
  assert.deepEqual(withPattern(after, 'speed', '\\w+').speed, '\\w+')
})

test('withPattern keeps the pattern text exactly, spaces included', () => {
  assert.equal(withPattern({}, 'session', ' a b ').session, ' a b ')
})

test('withoutPattern drops the key, leaving an empty map after the last', () => {
  assert.deepEqual(withoutPattern({ subject: 'x' }, 'subject'), {})
  assert.deepEqual(withoutPattern({}, 'subject'), {})
})
