/**
 * Text sizes block: rows from Python's resolved sizes, placeholders, and the
 * "cleared = key removed" rule the modified check depends on.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  basePlaceholder,
  baseTitle,
  fixedTickNote,
  formatPt,
  hasFixedSizes,
  isAutoText,
  placeholderFor,
  resetTextSizes,
  rowTitle,
  textSizeRows,
  textTarget,
  textTargets,
  withTextSize,
  withTextTarget,
} from './textSizes.js'

/** What `ResolvedSizes.to_dict()` sends for `TextSizes(base=14, title=20)`. */
const META = {
  base: 14,
  title: 20,
  x_label: 14,
  y_label: 14,
  x_ticks: 14,
  y_ticks: 14,
  groups: 11.662,
  legend: 14,
  legend_title: null,
  pinned: ['title'],
}

test('one row per element except base, in the order Python sends', () => {
  const rows = textSizeRows(META, { base: 14, title: 20 })
  assert.deepEqual(
    rows.map(r => r.key),
    ['title', 'x_label', 'y_label', 'x_ticks', 'y_ticks', 'groups', 'legend', 'legend_title'],
  )
  assert.equal(rows[0].label, 'Title')
  assert.equal(rows[0].pinned, true)
  assert.equal(rows[1].pinned, false)
})

test('an element Python adds later still gets a box, labelled by its key', () => {
  const rows = textSizeRows({ ...META, colorbar: 12 }, {})
  const row = rows.find(r => r.key === 'colorbar')
  assert.ok(row)
  assert.equal(row.label, 'colorbar')
  assert.equal(placeholderFor(row), 'auto · 12')
})

test('before the first render the known boxes still show', () => {
  const rows = textSizeRows(undefined, undefined)
  assert.equal(rows.length, 9) // incl. `differences` (difference-bar labels)
  assert.ok(rows.every(r => r.resolved === null))
  assert.equal(placeholderFor(rows[0]), 'auto')
})

test('an empty box shows the size Python resolved', () => {
  const rows = textSizeRows(META, {})
  const groups = rows.find(r => r.key === 'groups')!
  assert.equal(placeholderFor(groups), 'auto · 11.7')
  const legendTitle = rows.find(r => r.key === 'legend_title')!
  assert.equal(placeholderFor(legendTitle), '= legend')
})

test('clearing a size removes the key instead of storing null', () => {
  const set = withTextSize({ base: 12 }, 'legend', 9)
  assert.deepEqual(set, { base: 12, legend: 9 })
  assert.deepEqual(withTextSize(set, 'legend', null), { base: 12 })
  // 0 pt is a matplotlib error, not a size; a negative likewise.
  assert.deepEqual(withTextSize(set, 'legend', 0), { base: 12 })
  assert.deepEqual(withTextSize(undefined, 'title', -3), {})
})

test('reset keeps base and clears every element', () => {
  assert.deepEqual(resetTextSizes({ base: 11, title: 20, legend: 9 }), { base: 11 })
  assert.deepEqual(resetTextSizes({ title: 20 }), {})
  assert.equal(hasFixedSizes({ base: 11 }), false)
  assert.equal(hasFixedSizes({ base: 11, groups: 8 }), true)
})

test('the overlap notice names a fixed tick size', () => {
  assert.equal(fixedTickNote({ x_ticks: 9 }), 'The x tick font is fixed at 9 pt.')
  assert.equal(fixedTickNote({ base: 9 }), null)
  assert.equal(formatPt(11.662), '11.7')
})

/** What an AUTO spec's render sends (`autosize.text_sizes_meta`). */
const AUTO_META = {
  base: 9.5,
  title: 14.4,
  x_label: 12,
  y_label: 12,
  x_ticks: 9.5,
  y_ticks: 12,
  groups: 8,
  legend: 10,
  legend_title: null,
  differences: 12,
  pinned: [],
  auto: {
    target: 'print',
    sizes: { x_ticks: 9.5, y_ticks: 12, legend: 10, base: 9.5 },
    binding: { x_ticks: 'x ticks rotate 45°', legend: 'legend moves below' },
    at_floor: [],
    layouts: 5,
    ms: 420,
  },
  target: 'print',
  targets: ['print', 'slide'],
}

test('the auto block, target and targets are not element boxes', () => {
  const keys = textSizeRows(AUTO_META, {}).map(r => r.key)
  for (const key of ['auto', 'target', 'targets', 'base', 'pinned']) assert.ok(!keys.includes(key), key)
  assert.ok(keys.includes('differences'))
})

test('an auto row says what stopped it growing', () => {
  const rows = textSizeRows(AUTO_META, {})
  const ticks = rows.find(r => r.key === 'x_ticks')!
  assert.equal(ticks.autoReason, 'x ticks rotate 45°')
  assert.equal(placeholderFor(ticks), 'auto · 9.5')
  assert.match(rowTitle(ticks), /next size up fails \(x ticks rotate 45°\)/)
  const yTicks = rows.find(r => r.key === 'y_ticks')!
  assert.equal(yTicks.autoReason, null)
  assert.equal(rowTitle(yTicks), yTicks.title)
})

test('a fixed row carries no auto reason', () => {
  const ticks = textSizeRows(AUTO_META, { x_ticks: 11 }).find(r => r.key === 'x_ticks')!
  assert.equal(ticks.pinned, true)
  assert.equal(ticks.autoReason, null)
})

test('the Font box reads auto, and the chosen size once rendered', () => {
  assert.equal(isAutoText({}), true)
  assert.equal(isAutoText({ base: 12 }), false)
  assert.equal(basePlaceholder(undefined), 'auto')
  assert.equal(basePlaceholder(AUTO_META), 'auto · 9.5')
  assert.equal(basePlaceholder(META), 'auto', 'a fixed-size render has no auto block')
  assert.match(baseTitle(AUTO_META), /5 layout\(s\), 420 ms; limited: x_ticks \(x ticks rotate 45°\)/)
})

test('the target defaults to print and is stored even when default', () => {
  assert.equal(textTarget({}), 'print')
  assert.equal(textTarget({ target: 'slide' }), 'slide')
  assert.deepEqual(withTextTarget({ x_ticks: 9 }, 'print'), { x_ticks: 9, target: 'print' })
  assert.deepEqual(textTargets(undefined), ['print', 'slide'])
  assert.deepEqual(textTargets(AUTO_META), ['print', 'slide'])
})

test('reset keeps the target, and the target is not a fixed size', () => {
  assert.deepEqual(resetTextSizes({ target: 'slide', title: 20 }), { target: 'slide' })
  assert.equal(hasFixedSizes({ target: 'slide' }), false)
})
