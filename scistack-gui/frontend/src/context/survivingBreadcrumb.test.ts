/**
 * Tests for survivingBreadcrumb — run with `npm test` in frontend/.
 */

import { test } from 'node:test'
import * as assert from 'node:assert'
import { survivingBreadcrumb } from './survivingBreadcrumb.js'

const crumb = (pipeline_id: string) => ({ pipeline_id })
const main = crumb('main')

test('every pipeline still there: the same breadcrumb, so no re-render', () => {
  const path = [main, crumb('a')]
  assert.strictEqual(survivingBreadcrumb(path, new Set(['main', 'a']), main), path)
})

test('the open submodule is gone: back to its parent', () => {
  assert.deepStrictEqual(
    survivingBreadcrumb([main, crumb('a')], new Set(['main']), main),
    [main],
  )
})

test('a pipeline in the middle is gone: everything below it goes too', () => {
  assert.deepStrictEqual(
    survivingBreadcrumb([main, crumb('a'), crumb('b'), crumb('c')], new Set(['main', 'a', 'c']), main),
    [main, crumb('a')],
  )
})

test('the top of the path is gone: fall back to the root', () => {
  assert.deepStrictEqual(survivingBreadcrumb([crumb('hyp')], new Set(['main']), main), [main])
})
