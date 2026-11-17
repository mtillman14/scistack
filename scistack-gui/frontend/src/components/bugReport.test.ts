import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  MAX_URL_LENGTH,
  NEW_ISSUE_URL,
  TRUNCATION_NOTE,
  bugReportFileName,
  bugReportLink,
  fileNameOf,
} from './bugReport.js'

const BUILD = { version: '0.3.9', commit: 'abc1234', built_at: '2026-11-16T00:00:00Z', target: 'webview' }

function params(url: string): URLSearchParams {
  return new URL(url).searchParams
}

test('link goes to the new-issue page with title, body and the bug label', () => {
  const { url, truncated } = bugReportLink('  Plot crashes ', 'Steps:\n1. open & click', BUILD)
  assert.equal(truncated, false)
  assert.ok(url.startsWith(`${NEW_ISSUE_URL}?`))
  const p = params(url)
  assert.equal(p.get('title'), 'Plot crashes')
  assert.equal(p.get('labels'), 'bug')
  assert.ok(p.get('body')!.startsWith('Steps:\n1. open & click'))
  assert.equal(p.get('template'), null)
})

test('body carries the build the user was running', () => {
  const body = params(bugReportLink('t', 'b', BUILD).url).get('body')!
  assert.ok(body.includes('GUI version: 0.3.9'))
  assert.ok(body.includes('Commit: abc1234'))
  assert.ok(body.includes('Target: webview'))
})

test('a long body is cut to fit the URL limit, says so, and keeps the footer', () => {
  const long = 'é line of text\n'.repeat(2000)
  const { url, truncated } = bugReportLink('title', long, BUILD)
  assert.equal(truncated, true)
  assert.ok(url.length <= MAX_URL_LENGTH)
  const body = params(url).get('body')!
  assert.ok(body.includes(TRUNCATION_NOTE.trim()))
  assert.ok(body.includes('GUI version: 0.3.9'))
  assert.ok(long.startsWith(body.slice(0, 100)))
})

test('a saved project file is named in the body, above the footer', () => {
  const body = params(bugReportLink('t', 'b', BUILD, 'gait-bug-report.scistack.zip').url).get('body')!
  const hint = body.indexOf('drag `gait-bug-report.scistack.zip` into this box')
  assert.ok(hint > 0)
  assert.ok(hint < body.indexOf('GUI version'))
  const without = params(bugReportLink('t', 'b', BUILD).url).get('body')!
  assert.ok(!without.includes('Project file'))
})

test('the file name comes from the export default plus the attachable suffix', () => {
  assert.equal(bugReportFileName('gait.scistack', '.scistack'), 'gait-bug-report.scistack.zip')
  assert.equal(bugReportFileName('gait', '.scistack'), 'gait-bug-report.scistack.zip')
})

test('fileNameOf reads either separator', () => {
  assert.equal(fileNameOf('C:\\Users\\me\\x.scistack.zip'), 'x.scistack.zip')
  assert.equal(fileNameOf('/home/me/x.scistack.zip'), 'x.scistack.zip')
})
