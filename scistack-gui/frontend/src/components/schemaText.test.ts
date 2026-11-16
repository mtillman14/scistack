/**
 * Parity with scifor.parse_schema_text (scifor/tests/test_schema_tree.py): the
 * same shop tree, read from docs/claude/schema-branch-cases.json.
 */
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'

import { declarationOf, describeBranches, flatKeys, formatSchemaText, parseSchemaText } from './schemaText.js'

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

const SHOP: string[][] = JSON.parse(readFileSync(casesFile(), 'utf8')).trees.shop
const SHOP_TEXT = 'brand, category, item; brand, location, person'

test('the shop tree formats and parses as the shared text form', () => {
  assert.equal(formatSchemaText(SHOP), SHOP_TEXT)
  assert.deepEqual(parseSchemaText(SHOP_TEXT), SHOP)
})

test('text without a semicolon is one branch, as today', () => {
  assert.deepEqual(parseSchemaText('subject, session,'), [['subject', 'session']])
  assert.deepEqual(declarationOf(parseSchemaText('subject, session')), ['subject', 'session'])
})

test('branches are sent as one list each', () => {
  assert.deepEqual(declarationOf(parseSchemaText(SHOP_TEXT)), SHOP)
})

test('empty text is no branches; an empty branch is kept for the backend to refuse', () => {
  assert.deepEqual(parseSchemaText('  '), [])
  assert.deepEqual(parseSchemaText('a; ;b'), [['a'], [], ['b']])
})

test('flatKeys lists every key once in first-appearance order', () => {
  assert.deepEqual(flatKeys(SHOP), ['brand', 'category', 'item', 'location', 'person'])
})

test('the live hint names the parsed branches', () => {
  assert.equal(describeBranches([]), '')
  assert.equal(describeBranches([['a', 'b']]), 'keys: a → b')
  assert.match(describeBranches(SHOP), /2 branches: brand → category → item/)
})
