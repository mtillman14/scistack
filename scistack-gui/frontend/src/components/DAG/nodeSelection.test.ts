/**
 * Tests for nodeSelection — run with `npm test` in frontend/.
 */

import { test } from 'node:test'
import * as assert from 'node:assert'
import { SELECTION_COLOR, SELECTION_CSS, carrySelection } from './nodeSelection.js'

function luminance(hex: string): number {
  const n = parseInt(hex.slice(1), 16)
  const [r, g, b] = [(n >> 16) & 255, (n >> 8) & 255, n & 255].map(v => {
    const c = v / 255
    return c <= 0.03928 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4
  })
  return 0.2126 * r + 0.7152 * g + 0.0722 * b
}

function contrast(a: string, b: string): number {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x)
  return (hi + 0.05) / (lo + 0.05)
}

test('selection colour clears 3:1 on both dark and light backgrounds', () => {
  for (const background of ['#12122a', '#1a1a2e', '#ffffff']) {
    assert.ok(
      contrast(SELECTION_COLOR, background) >= 3,
      `${SELECTION_COLOR} on ${background}: ${contrast(SELECTION_COLOR, background).toFixed(2)}`,
    )
  }
})

test('the selected rule beats the inline box-shadow on node roots', () => {
  const rule = SELECTION_CSS.split('.react-flow__node.selected > div')[1].split('}')[0]
  assert.match(rule, /box-shadow:[^;]*!important/)
  assert.ok(rule.includes(SELECTION_COLOR))
})

test('a refresh keeps the selected nodes selected', () => {
  const prev = [{ id: 'a', selected: true }, { id: 'b', selected: false }, { id: 'c' }]
  const next = [{ id: 'a' }, { id: 'b' }, { id: 'c' }]
  assert.deepStrictEqual(carrySelection(prev, next), [
    { id: 'a', selected: true }, { id: 'b' }, { id: 'c' },
  ])
})

test('a selected node the refresh removed does not come back', () => {
  assert.deepStrictEqual(carrySelection([{ id: 'gone', selected: true }], [{ id: 'a' }]), [{ id: 'a' }])
})

test('with nothing selected the snapshot passes through untouched', () => {
  const next = [{ id: 'a' }]
  assert.strictEqual(carrySelection([{ id: 'a' }], next), next)
})
