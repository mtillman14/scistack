/**
 * Tests for matlabConnectionGate — run with `npm test` in extension/.
 *
 * Regression: the first "Run" click on a MATLAB function node, before any
 * MATLAB terminal exists, only starts the connection — MATLAB hasn't run
 * anything — but was reported as a successful run anyway. See
 * plan-matlab-terminal-run-tracking.md, problem P2.
 */

import { test } from 'node:test';
import * as assert from 'node:assert';
import { matlabHolder, needsMatlabConnectionPrompt } from './matlabConnectionGate';

test('first click with no MATLAB terminal yet prompts to connect', () => {
  assert.equal(needsMatlabConnectionPrompt(true, false), true);
});

test('a click once the MATLAB terminal already exists runs normally', () => {
  assert.equal(needsMatlabConnectionPrompt(true, true), false);
});

test('no MathWorks extension installed never prompts (falls through to sidecar/clipboard)', () => {
  assert.equal(needsMatlabConnectionPrompt(false, false), false);
  assert.equal(needsMatlabConnectionPrompt(false, true), false);
});

// --- one MATLAB engine, several databases ---------------------------------

test('a run is refused while another database owns MATLAB', () => {
  const sessions = [
    { id: 'a', label: 'gait.duckdb', matlabBusy: true },
    { id: 'b', label: 'emg.duckdb', matlabBusy: false },
  ];
  // The failure this prevents: b's generated script runs
  // `configure_database(emg)` while a's run is still issuing for_each calls,
  // so a's remaining results are written into emg.
  assert.equal(matlabHolder(sessions, 'b'), 'gait.duckdb');
});

test('a session never blocks itself', () => {
  // Several MATLAB runs against ONE database is the existing supported case.
  const sessions = [{ id: 'a', label: 'gait.duckdb', matlabBusy: true }];
  assert.equal(matlabHolder(sessions, 'a'), undefined);
});

test('an idle engine blocks nothing', () => {
  const sessions = [
    { id: 'a', label: 'gait.duckdb', matlabBusy: false },
    { id: 'b', label: 'emg.duckdb', matlabBusy: false },
  ];
  assert.equal(matlabHolder(sessions, 'b'), undefined);
});
