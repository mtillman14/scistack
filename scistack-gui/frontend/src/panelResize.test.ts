/**
 * The side-panel width rule: drag direction per edge, clamping, and the
 * tolerant reader for the stored width.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  clampWidth,
  dragWidth,
  maxWidth,
  parseStoredWidth,
  storageKey,
} from './panelResize.js'

const limits = { min: 200, max: 600 }

test('a right-edge handle grows the panel as it moves right', () => {
  assert.equal(dragWidth(300, 100, 150, 'right', limits), 350)
  assert.equal(dragWidth(300, 100, 50, 'right', limits), 250)
})

test('a left-edge handle grows the panel as it moves left', () => {
  assert.equal(dragWidth(300, 500, 450, 'left', limits), 350)
  assert.equal(dragWidth(300, 500, 550, 'left', limits), 250)
})

test('drags clamp to the limits', () => {
  assert.equal(dragWidth(300, 0, 10_000, 'right', limits), 600)
  assert.equal(dragWidth(300, 0, -10_000, 'right', limits), 200)
})

test('the floor wins when the container is narrower than the minimum', () => {
  assert.equal(clampWidth(150, { min: 200, max: 120 }), 200)
})

test('clamped widths are whole pixels', () => {
  assert.equal(clampWidth(300.6, limits), 301)
})

test('max width leaves the reserve, never negative', () => {
  assert.equal(maxWidth(1000, 320), 680)
  assert.equal(maxWidth(100, 320), 0)
})

test('stored width: anything unreadable means never resized', () => {
  assert.equal(parseStoredWidth(null), null)
  assert.equal(parseStoredWidth(undefined), null)
  assert.equal(parseStoredWidth(''), null)
  assert.equal(parseStoredWidth('wide'), null)
  assert.equal(parseStoredWidth('-40'), null)
  assert.equal(parseStoredWidth('0'), null)
  assert.equal(parseStoredWidth('Infinity'), null)
  assert.equal(parseStoredWidth('312'), 312)
  assert.equal(parseStoredWidth('312.4'), 312)
})

test('each panel has its own storage key', () => {
  assert.notEqual(storageKey('sidebar'), storageKey('plotStudio.controls'))
})
