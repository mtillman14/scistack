import test from 'node:test'
import assert from 'node:assert/strict'
import {
  STATUS_ICON,
  STATUS_ORDER,
  attentionOf,
  bytesText,
  durationText,
  locationsText,
  parseAutoOpen,
  readReports,
  shouldAutoOpen,
  shouldAutoRead,
  tabTone,
  tabsOf,
  type RunReport,
  type RunReportData,
  type RunSummary,
} from './runReportTypes.js'

function summary(over: Partial<RunSummary> = {}): RunSummary {
  return {
    run_id: 'r1', group_id: null, function_name: 'f', output_types: ['Out'], started_at: null,
    duration_ms: 10, total: 4, computed: 4, up_to_date: 0, failed: 0, no_data: 0, saved_records: 4,
    cancelled: false, stopped: false, language: 'python', verdict: 'ok', status_line: '', ...over,
  }
}

function report(attention: RunReport['attention']): RunReport {
  return { summary: summary(), attention } as unknown as RunReport
}

test('run_done.reports is read defensively and an entry without a run is dropped', () => {
  assert.deepEqual(readReports(undefined), [])
  assert.deepEqual(readReports('x'), [])
  assert.deepEqual(
    readReports([
      { node_id: 'n1', function_name: 'load', run_ids: ['a', 'b'] },
      { node_id: 'n2', function_name: 'skip', run_ids: [] },
      { node_id: '', function_name: 'loose', run_ids: ['c', null] },
      null,
    ]),
    [
      { node_id: 'n1', function_name: 'load', run_ids: ['a', 'b'] },
      { node_id: null, function_name: 'loose', run_ids: ['c'] },
    ],
  )
})

test('a tab opens on the last stored run of its node, in run order', () => {
  const tabs = tabsOf([
    { node_id: 'n1', function_name: 'load', run_ids: ['a', 'b'] },
    { node_id: 'n2', function_name: 'filter', run_ids: ['c'] },
  ])
  assert.deepEqual(tabs.map(t => [t.functionName, t.runId]), [['load', 'b'], ['filter', 'c']])
})

test('the window opens by itself only when something was computed or up to date', () => {
  assert.equal(shouldAutoOpen(true, [summary()]), true)
  assert.equal(shouldAutoOpen(true, [summary({ computed: 0, up_to_date: 3 })]), true)
  assert.equal(shouldAutoOpen(true, [summary({ computed: 0, failed: 4, verdict: 'failed' })]), false)
  assert.equal(shouldAutoOpen(true, []), false)
  // One clean step is enough for a pipeline run with a failed step.
  assert.equal(shouldAutoOpen(true, [summary({ computed: 0, failed: 4 }), summary()]), true)
  assert.equal(shouldAutoOpen(false, [summary()]), false)
})

test('the off switch is on until it is turned off', () => {
  assert.equal(parseAutoOpen(null), true)
  assert.equal(parseAutoOpen('on'), true)
  assert.equal(parseAutoOpen('off'), false)
})

test('a tab is flagged by a problem from either part, and pending until read', () => {
  assert.equal(tabTone(undefined), 'pending')
  assert.equal(tabTone(report([])), 'ok')
  assert.equal(tabTone(report([{ severity: 'note', kind: 'no_data', text: '', variable: null }])), 'ok')
  const data = { attention: [{ severity: 'problem', kind: 'cells', text: 'NaN', variable: 'Out' }] } as unknown as RunReportData
  assert.equal(tabTone(report([]), data), 'problem')
})

test('attention lists problems before notes across both parts', () => {
  const data = { attention: [{ severity: 'problem', kind: 'cells', text: 'p', variable: null }] } as unknown as RunReportData
  const items = attentionOf(report([{ severity: 'note', kind: 'no_data', text: 'n', variable: null }]), data)
  assert.deepEqual(items.map(a => a.text), ['p', 'n'])
})

test('records are read without asking only for a modest run', () => {
  assert.equal(shouldAutoRead(summary({ computed: 500 })), true)
  assert.equal(shouldAutoRead(summary({ computed: 501 })), false)
  assert.equal(shouldAutoRead(summary({ computed: 0 })), false)
})

test('formatting', () => {
  assert.equal(durationText(950), '950 ms')
  assert.equal(durationText(1500), '1.5 s')
  assert.equal(durationText(125_000), '2 min 5 s')
  assert.equal(durationText(null), '?')
  assert.equal(bytesText(512), '512 B')
  assert.equal(bytesText(2048), '2.0 kB')
  assert.equal(bytesText(null), '')
  assert.equal(locationsText(null), 'every location')
  assert.equal(
    locationsText({ include: [[['subject', 'S01'], ['trial', '2']]], exclude_levels: { minute: ['6'] } }),
    'subject=S01 / trial=2; without minute in [6]',
  )
})

test('every row status has its own icon', () => {
  assert.equal(new Set(STATUS_ORDER.map(s => STATUS_ICON[s])).size, STATUS_ORDER.length)
})
