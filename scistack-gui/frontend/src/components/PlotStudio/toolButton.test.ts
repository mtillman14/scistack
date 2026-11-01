import { test } from 'node:test'
import assert from 'node:assert/strict'
import { UNAVAILABLE_LOOK, blockedClickNotice, toolButtonStyle } from './toolButton.js'

const base = { background: '#22223a', color: '#fff', cursor: 'pointer' }

test('an unavailable button does not look like an available one', () => {
  const off = toolButtonStyle(base, true)
  assert.equal(off.cursor, 'not-allowed')
  assert.equal(off.opacity, UNAVAILABLE_LOOK.opacity)
  assert.notDeepEqual(off, toolButtonStyle(base, false))
})

test('an available button keeps its own style untouched', () => {
  assert.equal(toolButtonStyle(base, false), base)
})

test('a blocked click gives the backend reason', () => {
  const reason = 'This plot draws a 1-D measure (a series per record).'
  assert.equal(blockedClickNotice(false, reason), reason)
})

test('a blocked click with no reason still says something', () => {
  assert.equal(blockedClickNotice(false, null), 'Not available for this plot')
  assert.equal(blockedClickNotice(undefined, undefined), 'Not available for this plot')
})

test('an available click proceeds', () => {
  assert.equal(blockedClickNotice(true, 'ignored'), null)
})
