import { test } from 'node:test'
import assert from 'node:assert/strict'

import { MIN_PANE_PX, figureOutputSize, paneExportSize, previewKey, previewRequest } from './preview.js'

test('export mode sends no size: the figure decides at its own', () => {
  assert.deepEqual(previewRequest('export', 913, 604), { mode: 'export' })
  assert.equal(previewKey(previewRequest('export', 1, 1)), 'export')
})

test('pane mode sends the pane, rounded so a drag does not re-render per pixel', () => {
  assert.deepEqual(previewRequest('pane', 913, 604), { mode: 'pane', width_px: 920, height_px: 600 })
  assert.equal(
    previewKey(previewRequest('pane', 911, 601)),
    previewKey(previewRequest('pane', 918, 609)),
  )
})

test('a pane still being laid out is never sent as 0 px', () => {
  const request = previewRequest('pane', 0, 0)
  assert.equal(request.width_px, MIN_PANE_PX)
  assert.equal(request.height_px, MIN_PANE_PX)
})

test('the pane readout is the size the view was decided at, only in pane mode', () => {
  assert.deepEqual(paneExportSize({ mode: 'pane', width_in: 12.777, height_in: 8.333 }), {
    width: 12.78,
    height: 8.33,
  })
  assert.equal(paneExportSize({ mode: 'export', width_in: 8, height_in: 6 }), null)
  assert.equal(paneExportSize(undefined), null)
})

test('the written size: the spec in export mode, the pane in pane mode', () => {
  const spec = { width: 8, height: 6 }
  const pane = { mode: 'pane' as const, width_in: 9.4444, height_in: 5.1111 }
  assert.deepEqual(figureOutputSize('export', pane, spec), spec)
  assert.deepEqual(figureOutputSize('pane', pane, spec), { width: 9.44, height: 5.11 })
  // Before the first pane render: nothing to read yet, the spec stands in.
  assert.deepEqual(figureOutputSize('pane', undefined, spec), spec)
})
