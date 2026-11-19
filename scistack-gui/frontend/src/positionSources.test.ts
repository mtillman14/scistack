/**
 * Tests for positionSources — run with `npm test` in frontend/.
 */

import { test } from 'node:test'
import * as assert from 'node:assert'
import { positionSources } from './positionSources.js'

test('names nodes laid out fresh and saved keys that match no node', () => {
  const got = positionSources(
    ['a', 'b::main', 'c'],
    { a: { x: 0, y: 0 }, b: { x: 1, y: 1 } },
    [],
  )
  assert.deepStrictEqual(got, {
    placed_on_screen: 0,
    placed_saved: 1,
    placed_dagre: ['b::main', 'c'],
    saved_unmatched: ['b'],
  })
})

test('a node already on screen keeps its screen position', () => {
  const got = positionSources(['a'], {}, ['a'])
  assert.strictEqual(got.placed_on_screen, 1)
  assert.deepStrictEqual(got.placed_dagre, [])
})
