/**
 * Tests for descendInto — run with `npm test` in frontend/.
 *
 * 2026-11-04: main drawn over an open submodule offered its placement again, and
 * each double-click added another crumb for the same submodule.
 */

import { test } from 'node:test'
import * as assert from 'node:assert'
import { descendInto } from './descendInto.js'

const crumb = (pipeline_id: string) => ({ pipeline_id })
const main = crumb('main')

test('a pipeline not on the path gets a crumb', () => {
  assert.deepStrictEqual(descendInto([main], crumb('a')), [main, crumb('a')])
})

test('a real nested submodule still goes deeper', () => {
  assert.deepStrictEqual(
    descendInto([main, crumb('a')], crumb('b')),
    [main, crumb('a'), crumb('b')],
  )
})

test('the pipeline already open: the same path, so nothing re-renders', () => {
  const path = [main, crumb('a')]
  assert.strictEqual(descendInto(path, crumb('a')), path)
})

test('entering the same submodule repeatedly never grows the path', () => {
  let path = [main]
  for (let i = 0; i < 4; i += 1) path = descendInto(path, crumb('a'))
  assert.deepStrictEqual(path, [main, crumb('a')])
})

test('a pipeline earlier on the path goes back to it', () => {
  assert.deepStrictEqual(
    descendInto([main, crumb('a'), crumb('b')], crumb('a')),
    [main, crumb('a')],
  )
})

test('after a direct jump, a placement of the root still navigates', () => {
  // jumpTo builds [main, x] even when x isn't inside main, so x can place main.
  assert.deepStrictEqual(descendInto([main, crumb('x')], crumb('main')), [main])
})
