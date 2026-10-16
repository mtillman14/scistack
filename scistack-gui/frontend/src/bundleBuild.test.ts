/**
 * Tests for bundleBuild — run with `npm test` in frontend/.
 *
 * The report is how scidb.log learns which frontend a panel is running
 * (2026-10-16: a stale 0.1.0 bundle hid Copy PNG). It must name the view,
 * survive a missing vite define, and never throw into the render.
 */

import { test } from 'node:test'
import * as assert from 'node:assert'
import { BUNDLE_BUILD, buildReportParams, reportBundleBuild } from './bundleBuild.js'

test('outside a vite build the stamp says unknown instead of throwing', () => {
  assert.strictEqual(BUNDLE_BUILD.version, 'unknown')
  assert.strictEqual(BUNDLE_BUILD.target, 'unbuilt')
})

test('the report carries every stamped field plus the view', () => {
  const build = { version: '0.1.37', commit: 'abc1234', built_at: '2026-10-15T22:14:00Z', target: 'webview' }
  assert.deepStrictEqual(buildReportParams('plot', build), { ...build, view: 'plot' })
})

test('the report goes out as report_client_build', async () => {
  const calls: [string, Record<string, unknown>][] = []
  reportBundleBuild('pipeline', async (method, params) => { calls.push([method, params]) })
  assert.strictEqual(calls.length, 1)
  assert.strictEqual(calls[0][0], 'report_client_build')
  assert.strictEqual(calls[0][1].view, 'pipeline')
})

test('a server without the method does not throw into the caller', async () => {
  const warn = console.warn
  const info = console.info
  console.warn = () => {}
  console.info = () => {}
  try {
    assert.doesNotThrow(() =>
      reportBundleBuild('plot', () => Promise.reject(new Error('Method not found'))),
    )
    await new Promise(resolve => setTimeout(resolve, 0)) // let the rejection settle
  } finally {
    console.warn = warn
    console.info = info
  }
})
