/**
 * Tests for runFailures — run with `npm test` in extension/.
 *
 * Pins: a rerun REPLACES a covered node's Problems entries (a clean rerun
 * clears them); uncovered nodes keep theirs; no-data groups and groups with
 * no user frame never become entries; the notice is skipped on cancel.
 */

import { test } from 'node:test';
import * as assert from 'node:assert';
import {
  entriesByFile,
  failureNotice,
  nextDiagnosticState,
  readRunFailures,
  WireFailureGroup,
} from './runFailures';

function group(overrides: Partial<WireFailureGroup> = {}): WireFailureGroup {
  return {
    reason: "KeyError: 'chan'",
    fn_name: 'filt',
    exc_type: 'KeyError',
    message: "'chan'",
    no_data: false,
    count: 3,
    combos: ['subject=1', 'subject=2', 'subject=3'],
    user_frame: { file: '/p/filt.py', line: 12, function: 'filt', code: '', origin: 'user' },
    node_id: 'fn__a',
    ...overrides,
  };
}

test('a failed run adds one entry per group at the user frame', () => {
  const state = nextDiagnosticState(new Map(), readRunFailures({
    by_node: { fn__a: [group()] }, other: [],
  }));
  assert.deepStrictEqual(state.get('fn__a'), [{
    file: '/p/filt.py',
    line: 12,
    message: "filt: KeyError: 'chan' — 3 combo(s) failed, e.g. subject=1",
  }]);
});

test('a clean rerun of a node clears its entries; other nodes keep theirs', () => {
  let state = nextDiagnosticState(new Map(), readRunFailures({
    by_node: { fn__a: [group()], fn__b: [group({ node_id: 'fn__b' })] }, other: [],
  }));
  state = nextDiagnosticState(state, readRunFailures({ by_node: { fn__a: [] }, other: [] }));
  assert.strictEqual(state.has('fn__a'), false);
  assert.strictEqual(state.get('fn__b')?.length, 1);
});

test('no-data groups and frameless groups never become entries', () => {
  const state = nextDiagnosticState(new Map(), readRunFailures({
    by_node: { fn__a: [group({ no_data: true }), group({ user_frame: null })] },
    other: [],
  }));
  assert.strictEqual(state.size, 0);
});

test('entries group by file across nodes', () => {
  const state = nextDiagnosticState(new Map(), readRunFailures({
    by_node: { fn__a: [group()], fn__b: [group({ node_id: 'fn__b', reason: 'ValueError: x' })] },
    other: [],
  }));
  assert.strictEqual(entriesByFile(state).get('/p/filt.py')?.length, 2);
});

test('an older server without failures reads as nothing', () => {
  assert.deepStrictEqual(readRunFailures(undefined), { by_node: {}, other: [] });
});

test('notice names count, function, first reason and file:line', () => {
  const notice = failureNotice(readRunFailures({ by_node: { fn__a: [group()] }, other: [] }), {});
  assert.ok(notice);
  assert.match(notice!.message, /3 combo\(s\) failed in filt: KeyError: 'chan' \(filt\.py:12\)/);
  assert.strictEqual(notice!.nodeId, 'fn__a');
});

test('notice says stopped, and is skipped when cancelled or clean', () => {
  const failures = readRunFailures({ by_node: { fn__a: [group()] }, other: [] });
  assert.match(failureNotice(failures, { stopped: true })!.message, /stopped at the first failure/);
  assert.strictEqual(failureNotice(failures, { cancelled: true }), null);
  assert.strictEqual(failureNotice(readRunFailures({ by_node: { fn__a: [] }, other: [] }), {}), null);
});

test('the backend-worded blame label leads the entry and the notice', () => {
  const g = group({ blame: 'user_code', blame_label: 'Error in your function filt' });
  const failures = readRunFailures({ by_node: { fn__a: [g] }, other: [] });
  const state = nextDiagnosticState(new Map(), failures);
  assert.match(state.get('fn__a')![0].message, /^Error in your function filt\. filt: KeyError/);
  assert.match(
    failureNotice(failures, {})!.message,
    /failed in filt: Error in your function filt — KeyError: 'chan'/,
  );
});
