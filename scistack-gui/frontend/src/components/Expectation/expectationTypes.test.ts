import test from 'node:test'
import assert from 'node:assert/strict'
import { STATUS_ICON, STATUS_ORDER, concreteKeys, keyText } from './expectationTypes.js'

test('a wildcard cell is left out of the keys a mark or a follow names', () => {
  assert.deepEqual(concreteKeys({ subject: 'S03', session: null, trial: null }), { subject: 'S03' })
})

test('key text spells a wildcard with the backend marker', () => {
  assert.equal(keyText({ subject: 'S03', session: null }), 'subject=S03, session=(all)')
  assert.equal(keyText({ subject: 'S03', session: null }, '*'), 'subject=S03, session=*')
})

test('every status has its own icon, so the two reds and two greens differ without colour', () => {
  const icons = STATUS_ORDER.map(s => STATUS_ICON[s])
  assert.equal(new Set(icons).size, STATUS_ORDER.length)
})
