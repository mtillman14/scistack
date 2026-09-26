/**
 * The controls rail's groups: stored open state, width classes, and the
 * one-line summaries a collapsed group shows.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  DEFAULT_OPEN,
  appearanceSummary,
  chartSummary,
  dataSummary,
  parseOpenGroups,
  railWidthClass,
  serializeOpenGroups,
  statisticsSummary,
  structureSummary,
  textSizeColumns,
} from './sidebarGroups.js'

test('open state: nothing stored gives the defaults', () => {
  assert.deepEqual(parseOpenGroups(null), DEFAULT_OPEN)
  assert.deepEqual(parseOpenGroups(''), DEFAULT_OPEN)
  assert.equal(DEFAULT_OPEN.appearance, false)
})

test('open state: garbage and wrong shapes give the defaults', () => {
  assert.deepEqual(parseOpenGroups('{not json'), DEFAULT_OPEN)
  assert.deepEqual(parseOpenGroups('[true]'), DEFAULT_OPEN)
  assert.deepEqual(parseOpenGroups('42'), DEFAULT_OPEN)
})

test('open state: stored booleans win, per group; unknown keys ignored', () => {
  const open = parseOpenGroups('{"data": false, "appearance": true, "chart": "yes", "bogus": true}')
  assert.equal(open.data, false)
  assert.equal(open.appearance, true)
  assert.equal(open.chart, DEFAULT_OPEN.chart)
  assert.equal('bogus' in open, false)
})

test('open state round-trips', () => {
  const open = { ...DEFAULT_OPEN, structure: false, appearance: true }
  assert.deepEqual(parseOpenGroups(serializeOpenGroups(open)), open)
})

test('rail width classes and text-size columns', () => {
  assert.equal(railWidthClass(256), 'narrow')
  assert.equal(railWidthClass(299), 'narrow')
  assert.equal(railWidthClass(300), 'normal')
  assert.equal(railWidthClass(419), 'normal')
  assert.equal(railWidthClass(420), 'wide')
  assert.equal(textSizeColumns('narrow'), 1)
  assert.equal(textSizeColumns('normal'), 2)
  assert.equal(textSizeColumns('wide'), 2)
})

test('data summary', () => {
  assert.equal(dataSummary({ variantCount: 1, locations: null, filterCount: 0 }), 'All data')
  assert.equal(
    dataSummary({ variantCount: 3, locations: '12 of 14 subjects', filterCount: 1 }),
    '3 variants · 12 of 14 subjects · 1 filter'
  )
  assert.equal(dataSummary({ variantCount: 0, locations: null, filterCount: 2 }), '2 filters')
})

test('chart summary', () => {
  assert.equal(chartSummary({ kindLabel: 'Bar + error', perRecord: null }), 'Bar + error')
  assert.equal(chartSummary({ kindLabel: 'Box', perRecord: 'median' }), 'Box · per-record median')
  assert.equal(chartSummary({ kindLabel: null, perRecord: null }), 'No plot type')
})

test('structure summary: grouping order, colour, then roles', () => {
  assert.equal(structureSummary({ grouped: [], color: null, roles: {} }), 'Ungrouped')
  assert.equal(
    structureSummary({
      grouped: ['Side', 'Condition'],
      color: 'Side',
      roles: { Side: 'group', Condition: 'group', Muscle: 'facet', Subject: 'collapse', Trial: 'collapse' },
    }),
    'group Side › Condition · colour Side · subplots Muscle · collapse Subject, Trial'
  )
  assert.equal(
    structureSummary({ grouped: [], color: null, roles: { Session: 'iterate' } }),
    'figures Session'
  )
})

test('statistics summary', () => {
  assert.equal(
    statisticsSummary({ summarizing: false, centre: 'mean', spread: 'sd', pooled: false, shown: [] }),
    'Raw values'
  )
  assert.equal(
    statisticsSummary({ summarizing: true, centre: 'mean', spread: 'sem', pooled: false, shown: ['Subject'] }),
    'mean ± SEM · sample Subject'
  )
  assert.equal(
    statisticsSummary({ summarizing: true, centre: 'median', spread: 'none', pooled: true, shown: [] }),
    'median · weighted by N'
  )
})

test('appearance summary', () => {
  assert.equal(
    appearanceSummary({ width: 7.2, height: 4.45, font: 9, yMin: null, yMax: null }),
    '7.2×4.45 in · 9 pt · y auto'
  )
  assert.equal(
    appearanceSummary({ width: 8, height: 6, font: 14, yMin: 0, yMax: null }),
    '8×6 in · 14 pt · y 0–auto'
  )
  assert.equal(
    appearanceSummary({ width: 3.5, height: 2.1633, font: 8, yMin: -1.23456, yMax: 2 }),
    '3.5×2.163 in · 8 pt · y -1.235–2'
  )
})
