/**
 * Tests for variableNodeText — run with `npm test` in frontend/.
 *
 * Before 2026-10-16 the node printed every record of the type, all variants
 * pooled. It now shows the popup's current variant, its V-number and its
 * distinguishing settings.
 */

import { test } from 'node:test'
import * as assert from 'node:assert'
import { variableNodeText, type NodeVariantCard, type NodeVariants } from './variableNodeText.js'

function card(over: Partial<NodeVariantCard> = {}): NodeVariantCard {
  return {
    card_id: 'c3',
    ordinal: 3,
    distinguishing: { 'bandpass.low_hz': '30' },
    record_count: 48,
    current_location_count: 48,
    verdict: 'current',
    is_default: true,
    is_pinned: false,
    ...over,
  }
}

function variants(over: Partial<NodeVariants> = {}): NodeVariants {
  return {
    variant_count: 3,
    has_default: false,
    default_sources: [],
    pooled_record_count: 312,
    current: [card()],
    ...over,
  }
}

test('one current variant of several: V-number, its own count, its settings', () => {
  const t = variableNodeText(312, variants())
  assert.strictEqual(t.countLine, 'V3 of 3 · 48 records')
  // The popup's own wording (cardHeading), not a second spelling.
  assert.strictEqual(t.heading, 'low_hz (bandpass) = 30')
  assert.strictEqual(t.more, null)
  assert.match(t.tooltip, /V3 is current\. 3 variants, 312 records in all\./)
})

test('a pinned variant carries the popup glyph', () => {
  const t = variableNodeText(312, variants({
    has_default: true,
    default_sources: ['Filtered'],
    current: [card({ ordinal: 1, is_pinned: true })],
  }))
  assert.strictEqual(t.countLine, '★ V1 of 3 · 48 records')
  assert.match(t.tooltip, /V1 is current \(pinned\)/)
})

test('a default from an upstream pin says where it comes from', () => {
  const t = variableNodeText(312, variants({ has_default: true, default_sources: ['Raw'] }))
  assert.strictEqual(t.countLine, '☆ V3 of 3 · 48 records')
  assert.match(t.tooltip, /via the pin on Raw/)
})

test('a sweep with no pin shows the newest current and how many more', () => {
  const t = variableNodeText(144, variants({
    current: [card(), card({ card_id: 'c2', ordinal: 2 }), card({ card_id: 'c1', ordinal: 1 })],
  }))
  assert.strictEqual(t.countLine, 'V3 of 3 · 48 records')
  assert.strictEqual(t.more, '+2 more current')
  assert.match(t.tooltip, /Also current: V2, V1\./)
})

test('a single variant shows just its count', () => {
  const t = variableNodeText(4, variants({
    variant_count: 1, pooled_record_count: 4,
    current: [card({ ordinal: 1, record_count: 4, distinguishing: {} })],
  }))
  assert.strictEqual(t.countLine, '4 records')
  assert.strictEqual(t.heading, null)
})

test('no current variant is said, not shown as empty', () => {
  const t = variableNodeText(312, variants({ current: [], has_default: true }))
  assert.strictEqual(t.countLine, 'no current variant')
  assert.match(t.tooltip, /None is current/)
})

test('no variant payload falls back to the pooled count', () => {
  assert.strictEqual(variableNodeText(7).countLine, '7 records')
  assert.strictEqual(variableNodeText(0).countLine, null)
  assert.strictEqual(variableNodeText(1).countLine, '1 record')
})
