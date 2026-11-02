/**
 * The Plot Studio spec reducer (plotSpecReducer.ts): each action edits only
 * its own field, merges where the control sends a patch, and leaves "no spec
 * loaded" alone. Guards the extraction of these edits out of PlotStudio.tsx
 * (design review 07 G7): before it, each was an inline updater, untested.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import { plotSpecReducer, type PlotSpecFields } from './plotSpecReducer.js'

const BASE: PlotSpecFields & { measures: string[] } = {
  measures: ['StepLength'],
  kind: 'bar',
  facet: { n_rows: 2, share_x: true },
  style: { width: 6, not_plotted_note: true },
}

test('no spec loaded stays null', () => {
  assert.equal(plotSpecReducer(null, { type: 'setColor', factor: 'subject' }), null)
})

test('a field edit leaves every other field alone and does not mutate', () => {
  const next = plotSpecReducer(BASE, { type: 'setColor', factor: 'session' })
  assert.equal(next?.color, 'session')
  assert.deepEqual(next?.measures, ['StepLength'])
  assert.equal(BASE.color, undefined)
})

test('patch actions merge into the existing options', () => {
  const next = plotSpecReducer(BASE, { type: 'setFacet', patch: { n_cols: 3 } })
  assert.deepEqual(next?.facet, { n_rows: 2, share_x: true, n_cols: 3 })
  const styled = plotSpecReducer(BASE, { type: 'setStyle', patch: { height: 4 } })
  assert.deepEqual(styled?.style, { width: 6, not_plotted_note: true, height: 4 })
})

test('aggregate fills its defaults on first edit', () => {
  const next = plotSpecReducer(BASE, { type: 'setAggregate', patch: { pooled: true } })
  assert.deepEqual(next?.aggregate, { statistic: 'mean', error: 'sd', pooled: true })
})

test('the not-plotted note is removed, not stored as false', () => {
  const off = plotSpecReducer(BASE, { type: 'setNotPlottedNote', on: false })
  assert.equal('not_plotted_note' in (off?.style ?? {}), false)
  const on = plotSpecReducer(off, { type: 'setNotPlottedNote', on: true })
  assert.equal(on?.style?.not_plotted_note, true)
})

test('an x variable moves the kind to one that can draw it', () => {
  const next = plotSpecReducer(BASE, { type: 'setXMeasure', name: 'Speed' })
  assert.equal(next?.x_measure, 'Speed')
  assert.notEqual(next?.kind, undefined)
})

test('comparison undefined is stored as null (raw values)', () => {
  const next = plotSpecReducer(BASE, { type: 'setComparison', comparison: undefined })
  assert.equal(next?.comparison, null)
})
