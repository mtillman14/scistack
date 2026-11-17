import assert from 'node:assert/strict';
import { test } from 'node:test';

import { isAllowedExternalUrl } from './openExternal';

test('a GitHub new-issue link is allowed', () => {
  assert.equal(
    isAllowedExternalUrl('https://github.com/mtillman14/scistack/issues/new?title=x&body=y&labels=bug'),
    true,
  );
});

test('anything else is refused', () => {
  for (const bad of [
    undefined,
    42,
    '',
    'not a url',
    'http://github.com/x',
    'https://github.com.evil.example/x',
    'https://evil.example/?github.com',
    'https://user:pw@github.com/x',
    'file:///etc/passwd',
    'javascript:alert(1)',
  ]) {
    assert.equal(isAllowedExternalUrl(bad), false, String(bad));
  }
});
