/**
 * Tests for OwnWrites — run with `npm test` in extension/.
 *
 * GitHub #23: every GUI write refreshed the canvas twice, once from the
 * server's own dag_updated and again ~2 s later from the .duckdb watcher.
 */

import { test } from 'node:test';
import * as assert from 'node:assert';
import { OWN_WRITE_WINDOW_MS, OwnWrites } from './ownWrites';

test('a burst right after the server pushed is its own write: skipped', () => {
  const own = new OwnWrites();
  own.noteEmit(10_000);
  assert.equal(own.explains([10_050, 10_300]), true);
});

test('the file event may land just before the push', () => {
  const own = new OwnWrites();
  own.noteEmit(10_000);
  assert.equal(own.explains([9_800]), true);
});

test('no push at all: an external write refreshes', () => {
  assert.equal(new OwnWrites().explains([10_000]), false);
});

test('one unexplained change in the burst (MATLAB between GUI writes) refreshes', () => {
  const own = new OwnWrites();
  own.noteEmit(10_000);
  own.noteEmit(14_000);
  assert.equal(own.explains([10_100, 12_000, 14_100]), false);
  assert.equal(own.explains([10_100, 14_100]), true);
});

test('a change outside the window is not explained', () => {
  const own = new OwnWrites();
  own.noteEmit(10_000);
  assert.equal(own.explains([10_000 + OWN_WRITE_WINDOW_MS + 1]), false);
});

test('an empty burst is not a reason to skip', () => {
  const own = new OwnWrites();
  own.noteEmit(0);
  assert.equal(own.explains([]), false);
});
