import { test } from 'node:test'
import assert from 'node:assert/strict'

import { notPlottedColumns, notPlottedRows, notPlottedTruncated, type OmissionReport } from './notPlotted.js'

const report = (items: OmissionReport['items'], truncated = 0): OmissionReport => ({
  drawn: 10, lost: items.length, offered: 10 + items.length, counts: {}, labels: {},
  button: '', sentence: '', items, truncated,
})

test('rows carry each figure label only when there are several figures', () => {
  const item = { keys: { subject: '01' }, variable: 'Speed', reason: 'no_value', label: 'No value', detail: '' }
  assert.equal(notPlottedRows([{ label: 'one', omissions: report([item]) }])[0].figure, '')
  const rows = notPlottedRows([
    { label: 'subject 01', omissions: report([item]) },
    { label: 'subject 02', omissions: null },
    { label: 'subject 03', omissions: report([{ ...item, keys: { subject: '03', trial: '2' } }]) },
  ])
  assert.deepEqual(rows.map(r => r.figure), ['subject 01', 'subject 03'])
  assert.deepEqual(notPlottedColumns(rows), ['subject', 'trial'])
})

test('truncated counts add up over the figures', () => {
  assert.equal(notPlottedTruncated([{ omissions: report([], 3) }, { omissions: report([], 4) }, {}]), 7)
})
