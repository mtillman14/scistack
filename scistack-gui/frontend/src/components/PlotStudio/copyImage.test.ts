/**
 * Copy-to-clipboard resolution: 300 dpi at the figure's own size (1 pt = 1 px
 * in the preview), shrunk only when a ceiling forces it, never below 1x.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  COPY_DPI,
  MAX_AREA_PX,
  MAX_SIDE_PX,
  copyScale,
  dataUrlToBlob,
  effectiveDpi,
} from './copyImage.js'

test('an 8 x 6 in figure copies at the full 300 dpi', () => {
  const scale = copyScale(8 * 72, 6 * 72)
  assert.equal(scale, COPY_DPI / 72)
  assert.equal(effectiveDpi(scale), 300)
  assert.equal(Math.round(8 * 72 * scale), 2400)
})

test('a very wide figure is capped by the longest side', () => {
  const scale = copyScale(4000, 500)
  assert.ok(scale < COPY_DPI / 72)
  assert.ok(4000 * scale <= MAX_SIDE_PX + 1e-6)
})

test('a large square figure is capped by total pixels', () => {
  const scale = copyScale(1900, 1900)
  const area = 1900 * scale * 1900 * scale
  assert.ok(area <= MAX_AREA_PX + 1)
  assert.ok(1900 * scale <= MAX_SIDE_PX)
})

test('a figure already beyond the ceilings is never scaled below 1x', () => {
  assert.equal(copyScale(20000, 10000), 1)
})

test('a figure with no measured size falls back to the 300 dpi scale', () => {
  assert.equal(copyScale(0, 400), COPY_DPI / 72)
})

test('a base64 data URL decodes to a typed blob of the right bytes', async () => {
  // "PNG" in base64, standing in for the image bytes.
  const blob = dataUrlToBlob('data:image/png;base64,UE5H')
  assert.equal(blob.type, 'image/png')
  assert.equal(await blob.text(), 'PNG')
})

test('a non-base64 data URL is refused rather than copied as garbage', () => {
  assert.throws(() => dataUrlToBlob('data:image/svg+xml,<svg/>'), /base64/)
})
