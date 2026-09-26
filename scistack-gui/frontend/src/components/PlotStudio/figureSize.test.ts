/**
 * Figure size arithmetic — the same cases as `scistackplot/tests/test_figsize.py`.
 *
 * The panel names a preset on every keystroke while the renderer names it
 * once per save (in the log and in `layout.meta.figure_size`); if the two
 * disagreed, the dropdown would say "16:9" over a file the log calls custom.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  CUSTOM_ASPECT,
  aspectName,
  heightFor,
  parseAspectLocked,
  pixelReadout,
  resizeFigure,
} from './figureSize.js'

const GOLDEN = (1 + Math.sqrt(5)) / 2
const PRESETS = [
  { name: '4:3', ratio: 4 / 3, label: '4:3', hint: '' },
  { name: '16:9', ratio: 16 / 9, label: '16:9', hint: '' },
  { name: '3:2', ratio: 3 / 2, label: '3:2', hint: '' },
  { name: 'golden', ratio: GOLDEN, label: 'Golden', hint: '' },
  { name: '2:1', ratio: 2, label: '2:1', hint: '' },
  { name: '1:1', ratio: 1, label: '1:1', hint: '' },
  { name: '3:4', ratio: 3 / 4, label: '3:4', hint: '' },
  { name: '9:16', ratio: 9 / 16, label: '9:16', hint: '' },
  { name: CUSTOM_ASPECT, ratio: null, label: 'Custom', hint: '' },
]

test('the 8 x 6 default is 4:3, not custom', () => {
  assert.equal(aspectName(8, 6, PRESETS), '4:3')
})

test('every preset round-trips through heightFor at journal and slide widths', () => {
  for (const preset of PRESETS) {
    if (preset.ratio === null) continue
    for (const width of [3.5, 7.2, 8, 13.333]) {
      assert.equal(aspectName(width, heightFor(width, preset.name, PRESETS, -1), PRESETS), preset.name)
    }
  }
})

test('16:9 at 7.2 in is 4.05 in — the value the Python side stores', () => {
  assert.equal(heightFor(7.2, '16:9', PRESETS, -1), 4.05)
})

test('custom keeps the height the user typed', () => {
  assert.equal(heightFor(8, CUSTOM_ASPECT, PRESETS, 5.25), 5.25)
})

test('an unlisted ratio and a non-positive size are custom', () => {
  assert.equal(aspectName(8, 5, PRESETS), CUSTOM_ASPECT) // 1.6: neither golden nor 3:2
  assert.equal(aspectName(0, 6, PRESETS), CUSTOM_ASPECT)
  assert.equal(aspectName(8, NaN, PRESETS), CUSTOM_ASPECT)
})

test('the pixel readout is width x height at the save dpi', () => {
  assert.equal(pixelReadout(8, 4.5, 200), '1600 × 900 px')
})

test('unlocked: only the edited side moves, the ratio goes where it goes', () => {
  assert.deepEqual(resizeFigure({ width: 8, height: 6 }, { width: 10 }, false, PRESETS), { width: 10, height: 6 })
  assert.deepEqual(resizeFigure({ width: 8, height: 6 }, { height: 5 }, false, PRESETS), { width: 8, height: 5 })
  assert.equal(aspectName(10, 6, PRESETS), CUSTOM_ASPECT)
})

test('locked at a preset: either edit keeps the preset exactly', () => {
  assert.deepEqual(resizeFigure({ width: 8, height: 4.5 }, { width: 7.2 }, true, PRESETS), { width: 7.2, height: 4.05 })
  assert.deepEqual(resizeFigure({ width: 8, height: 6 }, { height: 3 }, true, PRESETS), { width: 4, height: 3 })
  // Many small steps never drift off the preset through rounding.
  let size = { width: 7.2, height: 4.05 }
  for (let i = 0; i < 50; i++) size = resizeFigure(size, { width: size.width + 0.1 }, true, PRESETS)
  assert.equal(aspectName(size.width, size.height, PRESETS), '16:9')
})

test('locked at a custom size: the other side follows the current ratio', () => {
  // 8 x 5 is 1.6 — custom.
  assert.deepEqual(resizeFigure({ width: 8, height: 5 }, { width: 4 }, true, PRESETS), { width: 4, height: 2.5 })
  assert.deepEqual(resizeFigure({ width: 8, height: 5 }, { height: 10 }, true, PRESETS), { width: 16, height: 10 })
})

test('locked over a size with no ratio just takes the edit', () => {
  assert.deepEqual(resizeFigure({ width: 0, height: 6 }, { width: 8 }, true, PRESETS), { width: 8, height: 6 })
})

test('picking a preset from custom keeps the width and moves the height', () => {
  // The dropdown path (heightFor), whatever the lock says.
  assert.equal(heightFor(10, '4:3', PRESETS, 6), 7.5)
  assert.equal(aspectName(10, heightFor(10, '4:3', PRESETS, 6), PRESETS), '4:3')
})

test('the lock is remembered per viewer and defaults to locked', () => {
  assert.equal(parseAspectLocked(null), true)
  assert.equal(parseAspectLocked('garbage'), true)
  assert.equal(parseAspectLocked('true'), true)
  assert.equal(parseAspectLocked('false'), false)
})
