/**
 * Text sizes block: rows from Python's resolved sizes, placeholders, and the
 * "cleared = key removed" rule the modified check depends on.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  fixedTickNote,
  formatPt,
  hasFixedSizes,
  placeholderFor,
  resetTextSizes,
  textSizeRows,
  withTextSize,
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
  assert.equal(rows.length, 8)
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
