/**
 * PARITY with the Python owner: docs/claude/schema-branch-cases.json holds the
 * `offered` rule's cases (scifor/tests/test_schema_tree.py runs the rule). Here
 * the expected answer is fed to the panel as the backend payload, and the boxes
 * must be exactly the offered keys, with exactly the disabled ones disabled.
 */
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'

import { levelBoxes } from './schemaLevelBoxes.js'

interface Case {
  id: number
  name: string
  tree: string
  rule: string
  args: { ticked?: string[] }
  expect: { offered: string[]; disabled: string[] }
}

function casesFile(): string {
  let dir = dirname(fileURLToPath(import.meta.url))
  for (let up = 0; up < 10; up++) {
    const candidate = join(dir, 'docs', 'claude', 'schema-branch-cases.json')
    try {
      readFileSync(candidate)
      return candidate
    } catch {
      dir = dirname(dir)
    }
  }
  throw new Error('Shared schema-branch cases (docs/claude/schema-branch-cases.json) not found.')
}

const data = JSON.parse(readFileSync(casesFile(), 'utf8'))
const ALL = ['brand', 'category', 'item', 'location', 'person']
const offeredCases: Case[] = data.cases.filter((c: Case) => c.rule === 'offered')

test('the shared file has offered cases (a silent zero would pass everything)', () => {
  assert.ok(offeredCases.length >= 5)
})

for (const c of offeredCases) {
  test(`offered ${String(c.id).padStart(2, '0')}-${c.name}`, () => {
    const payload = {
      offered: c.expect.offered,
      disabled: Object.fromEntries(c.expect.disabled.map(k => [k, `${k} is on a different branch`])),
    }
    const boxes = levelBoxes(ALL, payload, c.args.ticked ?? [])
    assert.deepEqual(boxes.map(b => b.key), c.expect.offered)
    assert.deepEqual(boxes.filter(b => b.disabled).map(b => b.key), c.expect.disabled)
    for (const b of boxes.filter(x => x.disabled)) assert.match(b.reason ?? '', /different branch/)
  })
}

test('a payload with no offered/disabled shows every key, enabled', () => {
  const boxes = levelBoxes(ALL, {})
  assert.deepEqual(boxes.map(b => b.key), ALL)
  assert.ok(boxes.every(b => !b.disabled))
})

test('a ticked key is never disabled', () => {
  const boxes = levelBoxes(ALL, { disabled: { location: 'why' } }, ['location'])
  assert.equal(boxes.find(b => b.key === 'location')?.disabled, false)
})
