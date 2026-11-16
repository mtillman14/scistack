/**
 * schemaKeysText: the prompt's copy of the schema key rules. The refused cases
 * and the shop tree come from docs/claude/schema-branch-cases.json, the file
 * scifor/tests/test_schema_tree.py runs against `SchemaTree.from_paths`.
 */

import { test } from 'node:test';
import * as assert from 'node:assert';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';

import { describeSchema, flatKeys, formatSchemaText, parseSchemaText, schemaArg } from './schemaKeysText';

function casesFile(): string {
  let dir = __dirname;
  for (let up = 0; up < 12; up++) {
    const candidate = join(dir, 'docs', 'claude', 'schema-branch-cases.json');
    if (existsSync(candidate)) return candidate;
    dir = dirname(dir);
  }
  throw new Error('docs/claude/schema-branch-cases.json not found walking up from ' + __dirname);
}

interface Refused { id: number; name: string; paths: string[][]; match: string }
const data = JSON.parse(readFileSync(casesFile(), 'utf8')) as {
  trees: Record<string, string[][]>;
  refused: Refused[];
};
const SHOP = data.trees.shop;
const SHOP_TEXT = 'brand, category, item; brand, location, person';

test('the shop tree round-trips through the text form', () => {
  assert.strictEqual(formatSchemaText(SHOP), SHOP_TEXT);
  assert.deepStrictEqual(parseSchemaText(SHOP_TEXT), { branches: SHOP, error: null });
});

test('text without a semicolon is one branch, as today', () => {
  assert.deepStrictEqual(parseSchemaText('subject, session,'), {
    branches: [['subject', 'session']],
    error: null,
  });
});

test('empty text asks for a key', () => {
  assert.match(parseSchemaText('  ').error ?? '', /at least one/);
});

for (const c of data.refused) {
  test(`refused ${c.id}-${c.name} (same mistakes as SchemaTree.from_paths)`, () => {
    const parsed = parseSchemaText(formatSchemaText(c.paths));
    assert.ok(parsed.error, `expected an error for ${JSON.stringify(c.paths)}`);
    assert.match(parsed.error ?? '', new RegExp(c.match));
  });
}

test('the CLI argument has no spaces and uses semicolons between branches', () => {
  assert.strictEqual(schemaArg(SHOP), 'brand,category,item;brand,location,person');
  assert.strictEqual(schemaArg([['a', 'b']]), 'a,b');
});

test('flatKeys is first-appearance order', () => {
  assert.deepStrictEqual(flatKeys(SHOP), ['brand', 'category', 'item', 'location', 'person']);
});

test('the live line names the branches', () => {
  assert.match(describeSchema(SHOP), /2 branches: brand → category → item/);
  assert.match(describeSchema([['a', 'b']]), /2 key\(s\): a → b/);
});
