import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  clearOverride,
  hiddenFor,
  isEmptyOverride,
  matchId,
  matchLabel,
  offersPanels,
  overrideCount,
  overrideFor,
  sameMatch,
  showState,
  upsertOverride,
  type PanelOverride,
  type PanelOverrideMeta,
} from './panelOverrides.js'

const SOL = { muscle: 'SOL' }
const TA = { muscle: 'TA' }

test('a match is the same whatever its key order', () => {
  assert.ok(sameMatch({ a: '1', b: '2' }, { b: '2', a: '1' }))
  assert.equal(matchId({ a: '1', b: '2' }), matchId({ b: '2', a: '1' }))
  assert.ok(!sameMatch({ a: '1' }, { a: '01' }))
  assert.ok(!sameMatch({ a: '1' }, { a: '1', b: '2' }))
})

test('a patch for a panel with no entry appends one', () => {
  const next = upsertOverride(undefined, SOL, { y_maximum: 400 })
  assert.deepEqual(next, [{ match: SOL, y_maximum: 400 }])
})

test('a patch edits the existing entry and keeps its other fields', () => {
  const list: PanelOverride[] = [{ match: SOL, y_maximum: 400 }, { match: TA, y_label: 'T' }]
  const next = upsertOverride(list, SOL, { y_minimum: 0 })
  assert.deepEqual(overrideFor(next, SOL), { match: SOL, y_maximum: 400, y_minimum: 0 })
  assert.deepEqual(overrideFor(next, TA), { match: TA, y_label: 'T' })
})

test('null clears a field; an entry with nothing left is removed', () => {
  const list: PanelOverride[] = [{ match: SOL, y_maximum: 400 }]
  assert.deepEqual(upsertOverride(list, SOL, { y_maximum: null }), [])
})

test('hidden=false is a setting, not an empty entry', () => {
  const next = upsertOverride([], SOL, { y_label_hidden: false })
  assert.deepEqual(next, [{ match: SOL, y_label_hidden: false }])
  assert.ok(!isEmptyOverride(next[0]))
})

test('duplicates collapse onto the last one, as the backend reads them', () => {
  const list: PanelOverride[] = [
    { match: SOL, y_maximum: 1 },
    { match: SOL, y_maximum: 2, y_label: 'S' },
  ]
  assert.deepEqual(overrideFor(list, SOL)?.y_maximum, 2)
  const next = upsertOverride(list, SOL, { y_minimum: 0 })
  assert.deepEqual(next, [{ match: SOL, y_maximum: 2, y_label: 'S', y_minimum: 0 }])
})

test('clear removes every entry for one panel only', () => {
  const list: PanelOverride[] = [{ match: SOL, y_maximum: 1 }, { match: TA, y_maximum: 2 }]
  assert.deepEqual(clearOverride(list, SOL), [{ match: TA, y_maximum: 2 }])
})

test('the Show control round-trips through y_label_hidden', () => {
  for (const state of ['follow', 'show', 'hide'] as const) {
    const hidden = hiddenFor(state)
    assert.equal(showState(hidden === null ? null : { match: SOL, y_label_hidden: hidden }), state)
  }
  assert.equal(showState(null), 'follow')
})

test('the section is offered for two or more faceted panels', () => {
  const meta = (n: number): PanelOverrideMeta => ({
    panels: Array.from({ length: n }, (_, i) => ({
      match: { muscle: String(i) },
      display_title: String(i),
      y_title: String(i),
      grid_row: 0,
      grid_col: i,
      y_limits: null,
      override: null,
    })),
    unmatched: [],
    y_titles: 'every_panel',
    shares_y: true,
  })
  assert.equal(offersPanels(undefined), false)
  assert.equal(offersPanels(meta(1)), false)
  assert.equal(offersPanels(meta(2)), true)
})

test('the summary counts only entries that set something', () => {
  assert.equal(overrideCount([{ match: SOL, y_maximum: 1 }, { match: TA }]), 1)
  assert.equal(overrideCount(undefined), 0)
})

test('an unmatched entry is named by its values', () => {
  assert.equal(matchLabel({ muscle: 'HAM', side: 'L' }), 'HAM · L')
})
