/**
 * Tests for placementBadges — run with `npm test` in frontend/.
 * Rule: docs/claude/submodule-ports.md, "A placement's badges" (GitHub #27).
 */

import { test } from 'node:test'
import * as assert from 'node:assert'
import { placementBadges } from './placementBadges.js'

const kinds = (data: Parameters<typeof placementBadges>[0]) =>
  placementBadges(data).map(b => [b.kind, b.severity])

test('nothing to say: no badge', () => {
  assert.deepStrictEqual(kinds({}), [])
  assert.deepStrictEqual(kinds({ open_ports: { inputs: [], outputs: [] }, disconnected_members: [] }), [])
})

test('an open port is an error naming the port', () => {
  const [badge] = placementBadges({ open_ports: { inputs: ['A'], outputs: ['E'] } })
  assert.deepStrictEqual([badge.kind, badge.severity], ['port', 'error'])
  assert.match(badge.text, /2 port\(s\) not connected/)
  assert.match(badge.title, /input\(s\): A/)
  assert.match(badge.title, /output\(s\): E/)
})

test('disconnected steps inside are a warning while something runs', () => {
  const [badge] = placementBadges({ disconnected_members: ['fcn_x'], disconnected_severity: 'warning' })
  assert.deepStrictEqual([badge.kind, badge.severity], ['inside', 'warning'])
  assert.match(badge.title, /fcn_x/)
})

test('disconnected steps inside are an error when nothing runs', () => {
  assert.deepStrictEqual(
    kinds({ disconnected_members: ['fcn_b'], disconnected_severity: 'error' }),
    [['inside', 'error']],
  )
})

test('both badges at once, port first', () => {
  assert.deepStrictEqual(
    kinds({
      open_ports: { inputs: ['A'], outputs: [] },
      disconnected_members: ['fcn_x'],
      disconnected_severity: 'warning',
    }),
    [['port', 'error'], ['inside', 'warning']],
  )
})
