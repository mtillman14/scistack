/**
 * Tests for bundleImportCore — run with `npm test` in extension/.
 *
 * The command must hand every answer to `scistack_gui.bundle_cli` as the
 * same flags `scistack import` takes (one implementation), and must read the
 * CLI's answer from its LAST stdout line even when a library printed first.
 */

import { test } from 'node:test';
import * as assert from 'node:assert';
import {
  BUNDLE_CLI_MODULE,
  buildImportArgs,
  buildInfoArgs,
  formatImportReport,
  keysNeedingAChoice,
  parseCliJson,
} from './bundleImportCore';

test('import args carry every choice as a CLI flag', () => {
  const args = buildImportArgs({
    bundle: '/b/study.scistack',
    into: '/p/study',
    schema: ['participant', 'trial'],
    keyMap: { subject: 'participant', session: null },
    pathRoots: { RawGait: '/data/gait' },
    importHistory: false,
  });
  assert.deepStrictEqual(args.slice(0, 7), [
    '-m', BUNDLE_CLI_MODULE, 'import', '/b/study.scistack', '--into', '/p/study', '--json',
  ]);
  const joined = args.join(' ');
  assert.ok(joined.includes('--schema participant trial'));
  assert.ok(joined.includes('--map subject=participant'));
  assert.ok(joined.includes('--map session='), 'a dropped key is OLD= (empty)');
  assert.ok(joined.includes('--path-root RawGait=/data/gait'));
  assert.ok(args.includes('--no-history'));
});

test('a kept schema passes no --schema and history defaults explicitly', () => {
  const args = buildImportArgs({ bundle: 'b', into: 'i', importHistory: true });
  assert.ok(!args.includes('--schema'));
  assert.ok(!args.includes('--map'));
  assert.ok(args.includes('--history'));
});

test('info args', () => {
  assert.deepStrictEqual(buildInfoArgs('x.scistack'), ['-m', BUNDLE_CLI_MODULE, 'bundle-info', 'x.scistack', '--json']);
});

test('the answer is the last JSON line, whatever was printed before', () => {
  const out = 'some library banner\n{"not": "this"}\n{"ok": true, "info": {"package": "gait"}}\n';
  const r = parseCliJson<{ info: { package: string } }>(out);
  assert.ok(r.ok);
  assert.strictEqual((r as { info: { package: string } }).info.package, 'gait');
});

test('no JSON answer is an error naming the module', () => {
  const r = parseCliJson('Traceback ...\nModuleNotFoundError: scistack_gui\n');
  assert.strictEqual(r.ok, false);
  assert.ok(!r.ok && r.error.includes(BUNDLE_CLI_MODULE));
});

test('only exporter keys missing from my schema need a choice', () => {
  assert.deepStrictEqual(
    keysNeedingAChoice(['subject', 'session', 'trial'], ['participant', 'trial']),
    ['subject', 'session'],
  );
  assert.deepStrictEqual(keysNeedingAChoice(['a', 'b'], ['a', 'b']), []);
});

test('the report names the folder, the history outcome and what to review', () => {
  const md = formatImportReport({
    root: '/p/study',
    db_path: '/p/study/study.duckdb',
    package: 'study',
    schema_keys: ['participant', 'trial'],
    created: ['/p/study/scistack.toml'],
    sections: {
      schema: {
        key_map: { subject: 'participant', trial: 'trial' },
        dropped: [['node f', 'session']],
        flagged: [['PathInput RawGait', "root_folder is still the exporter's"]],
      },
      history: { live: false, archived: '/p/study/.scistack/archive/x' },
      gui: { pipelines: 2 },
    },
    warnings: ['imported into another schema'],
  });
  assert.ok(md.includes('/p/study'));
  assert.ok(md.includes('`subject` → `participant`'));
  assert.ok(md.includes('Archived to `/p/study/.scistack/archive/x`'));
  assert.ok(md.includes('**dropped** node f'));
  assert.ok(md.includes("root_folder is still the exporter's"));
  assert.ok(md.includes('imported into another schema'));
});
