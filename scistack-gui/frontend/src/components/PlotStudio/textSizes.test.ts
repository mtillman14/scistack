/**
 * Text sizes block: rows from Python's drawn sizes, placeholders, the
 * proportions grid (ratio / floor / ceiling per element), and the
 * "cleared = key removed" rule the modified check depends on.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  autoSummary,
  basePlaceholder,
  baseTitle,
  drawnLabel,
  fixedTickNote,
  formatPt,
  hasFixedSizes,
  hasTextTables,
  isAutoText,
  placeholderFor,
  proportionRows,
  resetTextSizes,
  resetTextTables,
  rowTitle,
  textSizeRows,
  textTarget,
  textTargets,
  withTextSize,
  withTextTable,
  withTextTarget,
  type AutoTextMeta,
  type ResolvedTextSizes,
} from './textSizes.js'

const RATIOS = {
  title: 1.44,
  x_label: 1.2,
  y_label: 1.2,
  x_ticks: 1,
  y_ticks: 1,
  groups: 1,
  legend: 1,
  legend_title: 1.2,
  differences: 1,
}

/** What `ResolvedSizes.to_dict()` sends for `TextSizes(base=14, title=20)`. */
const META: ResolvedTextSizes = {
  base: 14,
  title: 20,
  x_label: 16.8,
  y_label: 16.8,
  x_ticks: 14,
  y_ticks: 14,
  groups: 14,
  legend: 14,
  legend_title: 16.8,
  differences: 14,
  pinned: ['title'],
  clamped: {},
  ratios: RATIOS,
  bounds: {},
}

test('one row per element except base, in the order Python sends', () => {
  const rows = textSizeRows(META, { base: 14, title: 20 })
  assert.deepEqual(
    rows.map(r => r.key),
    ['title', 'x_label', 'y_label', 'x_ticks', 'y_ticks', 'groups', 'legend', 'legend_title', 'differences'],
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
  assert.equal(rows.length, 9)
  assert.ok(rows.every(r => r.resolved === null))
  assert.equal(placeholderFor(rows[0]), 'auto')
})

test('an empty box shows the size Python drew', () => {
  const rows = textSizeRows(META, {})
  assert.equal(placeholderFor(rows.find(r => r.key === 'x_label')!), 'auto · 16.8')
  assert.equal(placeholderFor(rows.find(r => r.key === 'legend_title')!), 'auto · 16.8')
})

test('clearing a size removes the key instead of storing null', () => {
  const set = withTextSize({ base: 12 }, 'legend', 9)
  assert.deepEqual(set, { base: 12, legend: 9 })
  assert.deepEqual(withTextSize(set, 'legend', null), { base: 12 })
  assert.deepEqual(withTextSize(set, 'legend', 0), { base: 12 })
  assert.deepEqual(withTextSize(undefined, 'title', -3), {})
})

test('reset keeps base, target and the proportions, and clears every size', () => {
  assert.deepEqual(resetTextSizes({ base: 11, title: 20, legend: 9 }), { base: 11 })
  assert.deepEqual(resetTextSizes({ title: 20 }), {})
  assert.deepEqual(
    resetTextSizes({ title: 20, ratios: { title: 2 } }),
    { ratios: { title: 2 } },
  )
  assert.equal(hasFixedSizes({ base: 11 }), false)
  assert.equal(hasFixedSizes({ base: 11, groups: 8 }), true)
  assert.equal(hasFixedSizes({ ratios: { title: 2 } }), false, 'a table is not a fixed size')
})

test('the overlap notice names a fixed tick size', () => {
  assert.equal(fixedTickNote({ x_ticks: 9 }), 'The x tick font is fixed at 9 pt.')
  assert.equal(fixedTickNote({ base: 9 }), null)
  assert.equal(formatPt(11.662), '11.7')
})

/** What an AUTO spec's render sends (`autosize.text_sizes_meta`). */
const AUTO_META: ResolvedTextSizes & { auto: AutoTextMeta } = {
  base: 9.5,
  title: 13.68,
  x_label: 11.4,
  y_label: 11.4,
  x_ticks: 9.5,
  y_ticks: 10,
  groups: 9.5,
  legend: 9.5,
  legend_title: 11.4,
  differences: 9.5,
  pinned: [],
  clamped: { y_ticks: 'floor' },
  ratios: RATIOS,
  bounds: { y_ticks: [10, 12], x_ticks: [8, 12], title: [11.52, 17.28] },
  auto: {
    target: 'print',
    reference: 9.5,
    span: [8, 12],
    sizes: { x_ticks: 9.5, y_ticks: 10 },
    clamped: { y_ticks: 'floor' },
    limit: ['x ticks rotate 45°'],
    failing: [],
    at_bottom: false,
    layouts: 4,
    ms: 420,
  },
  target: 'print',
  targets: ['print', 'slide'],
}

test('the auto block, tables, target and targets are not element boxes', () => {
  const keys = textSizeRows(AUTO_META, {}).map(r => r.key)
  for (const key of ['auto', 'target', 'targets', 'base', 'pinned', 'clamped', 'ratios', 'bounds']) {
    assert.ok(!keys.includes(key), key)
  }
  assert.ok(keys.includes('differences'))
})

test('an auto row held at a bound says so', () => {
  const rows = textSizeRows(AUTO_META, {})
  const yTicks = rows.find(r => r.key === 'y_ticks')!
  assert.equal(yTicks.autoReason, 'held at its floor')
  assert.match(rowTitle(yTicks), /held at its floor, not at its ratio/)
  const xTicks = rows.find(r => r.key === 'x_ticks')!
  assert.equal(xTicks.autoReason, null)
  assert.equal(rowTitle(xTicks), xTicks.title)
})

test('a fixed row carries no auto reason', () => {
  const ticks = textSizeRows(AUTO_META, { y_ticks: 11 }).find(r => r.key === 'y_ticks')!
  assert.equal(ticks.pinned, true)
  assert.equal(ticks.autoReason, null)
})

test('the Font box reads auto, the reference once rendered, and the limiting rule', () => {
  assert.equal(isAutoText({}), true)
  assert.equal(isAutoText({ base: 12 }), false)
  assert.equal(basePlaceholder(undefined), 'auto')
  assert.equal(basePlaceholder(AUTO_META), 'auto · 9.5')
  assert.equal(basePlaceholder(META), 'auto', 'a fixed-size render has no auto block')
  assert.equal(autoSummary(AUTO_META), 'Reference 9.5 pt of 8–12; one step larger: x ticks rotate 45°')
  assert.match(baseTitle(AUTO_META), /4 layout\(s\), 420 ms/)
  assert.equal(autoSummary(META), null)
})

test('the summary says the top, and what still fails at the bottom', () => {
  const top = { ...AUTO_META, auto: { ...AUTO_META.auto, reference: 12, limit: [] } }
  assert.equal(autoSummary(top), 'Reference 12 pt of 8–12 (the top)')
  const bottom = {
    ...AUTO_META,
    auto: { ...AUTO_META.auto, reference: 8, at_bottom: true, failing: ['legend moves below'] },
  }
  assert.equal(
    autoSummary(bottom),
    'Reference 8 pt of 8–12 (the bottom); still failing: legend moves below',
  )
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

// --- proportions ---------------------------------------------------------------

test('a ratio, floor or ceiling is set per element and an emptied table is deleted', () => {
  const set = withTextTable({ target: 'print' }, 'ratios', 'title', 2)
  assert.deepEqual(set, { target: 'print', ratios: { title: 2 } })
  const both = withTextTable(set, 'ratios', 'legend', 1.1)
  assert.deepEqual(both.ratios, { title: 2, legend: 1.1 })
  assert.deepEqual(withTextTable(set, 'ratios', 'title', null), { target: 'print' })
  assert.deepEqual(withTextTable(set, 'floors', 'title', 0), set, 'a non-positive bound clears')
  assert.equal(hasTextTables(set), true)
  assert.equal(hasTextTables({ target: 'print' }), false)
})

test('reset proportions clears every table and keeps the sizes', () => {
  const text = { base: 10, x_ticks: 9, ratios: { title: 2 }, floors: { legend: 9 }, ceilings: { title: 20 } }
  assert.deepEqual(resetTextTables(text), { base: 10, x_ticks: 9 })
})

test('the proportions grid shows stored overrides over Python’s effective values', () => {
  const rows = proportionRows(AUTO_META, { floors: { y_ticks: 10 }, ratios: { title: 1.44 } })
  const y = rows.find(r => r.key === 'y_ticks')!
  assert.equal(y.floor, 10)
  assert.equal(y.ceiling, null)
  assert.equal(y.effectiveCeiling, 12)
  assert.equal(y.effectiveRatio, 1)
  assert.equal(y.clamped, 'floor')
  assert.equal(drawnLabel(y), '10 · floor')
  const title = rows.find(r => r.key === 'title')!
  assert.equal(title.ratio, 1.44)
  assert.equal(drawnLabel(title), '13.7')
})

test('a fixed element is marked fixed and never clamped', () => {
  const rows = proportionRows(AUTO_META, { y_ticks: 11 })
  const y = rows.find(r => r.key === 'y_ticks')!
  assert.equal(y.fixed, true)
  assert.equal(y.clamped, null)
})

test('with Font fixed there are no bounds to show', () => {
  const x = proportionRows(META, { base: 14 }).find(r => r.key === 'x_ticks')!
  assert.equal(x.effectiveFloor, null)
  assert.equal(x.effectiveCeiling, null)
  assert.equal(x.effectiveRatio, 1)
})
