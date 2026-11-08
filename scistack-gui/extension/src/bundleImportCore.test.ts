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
  asksAboutValueRules,
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

test('value rules: a flag only when the dialog asked', () => {
  const base = { bundle: 'b', into: 'i', importHistory: true };
  assert.ok(!buildImportArgs(base).some((a) => a.includes('value-rules')), 'unasked = the CLI default');
  assert.ok(buildImportArgs({ ...base, valueRules: false }).includes('--no-value-rules'));
  assert.ok(buildImportArgs({ ...base, valueRules: true }).includes('--value-rules'));
});

test('same-or-other data is asked only when a value rule would apply', () => {
  const info = {
    path: 'b', package: 'p', schema_keys: ['subject', 'session'], database: null, options: {},
    sections: {}, has_history: true, has_data: false, path_inputs: [], environment: null,
    value_rule_rows: { _expectation_mark: 2 } as Record<string, number> | null,
  };
  const keys = ['subject', 'session'];
  assert.strictEqual(asksAboutValueRules(info, true, keys), true);
  assert.strictEqual(asksAboutValueRules(info, false, keys), false, 'history skipped: no rules travel');
  assert.strictEqual(asksAboutValueRules(info, true, ['participant', 'visit']), false, 'another schema: none apply');
  assert.strictEqual(asksAboutValueRules({ ...info, value_rule_rows: {} }, true, keys), false);
  assert.strictEqual(asksAboutValueRules({ ...info, value_rule_rows: null }, true, keys), true, 'an older bundle is asked');
  assert.strictEqual(asksAboutValueRules({ ...info, has_history: false }, true, keys), false);
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

test('install args and the one-line description', async () => {
  const { buildInstallArgs, describeInstall } = await import('./bundleImportCore');
  assert.deepStrictEqual(buildInstallArgs('/p/x'), ['-m', BUNDLE_CLI_MODULE, 'install', '/p/x', '--yes', '--json']);
  const base = { requirements: [], installed: [], conflicts: [], reason: '', command: '' };
  assert.strictEqual(describeInstall({ ...base, status: 'nothing' }), 'nothing to install');
  assert.strictEqual(describeInstall({ ...base, status: 'installed', installed: ['a==1'] }), 'installed a==1');
  const stopped = describeInstall({
    ...base, status: 'stopped', reason: 'would change packages', conflicts: ['numpy: installed 2, would install 1'],
    command: 'pip install x',
  });
  assert.ok(stopped.includes('stopped') && stopped.includes('numpy') && stopped.includes('pip install x'));
});
