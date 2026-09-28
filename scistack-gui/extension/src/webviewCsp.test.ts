/**
 * Tests for the webview CSP — run with `npm test` in extension/.
 *
 * What they lock down: Plot Studio's "Copy image" needs `img-src blob:`
 * (Plotly.toImage loads its SVG from a blob: URL); scripts stay nonce-only.
 */

import { test } from 'node:test';
import * as assert from 'node:assert';
import { webviewCsp, webviewCspDirectives } from './webviewCsp';

const SOURCE = 'https://file+.vscode-resource.vscode-cdn.net';

function directive(name: string): string {
  const found = webviewCspDirectives(SOURCE, 'abc').find(([n]) => n === name);
  assert.ok(found, `missing ${name}`);
  return found[1];
}

test('img-src allows blob: for Plotly.toImage (Copy image)', () => {
  const sources = directive('img-src').split(/\s+/);
  assert.ok(sources.includes('blob:'), sources.join(' '));
  assert.ok(sources.includes('data:'), sources.join(' '));
  assert.ok(sources.includes(SOURCE), sources.join(' '));
});

test('scripts stay nonce-only and default is none', () => {
  assert.strictEqual(directive('script-src'), "'nonce-abc'");
  assert.strictEqual(directive('default-src'), "'none'");
});

test('meta content is every directive, semicolon-terminated', () => {
  const content = webviewCsp(SOURCE, 'abc');
  assert.match(content, /^default-src 'none';/);
  assert.match(content, new RegExp(`img-src ${SOURCE.replace(/[.+]/g, '\\$&')} data: blob:;`));
  assert.strictEqual(content.split(';').filter(s => s.trim()).length, 5);
});
