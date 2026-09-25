/**
 * Tests for MatlabRunTracker — run with `npm test` in extension/.
 *
 * The regression these lock down (todo #13): while MATLAB owns the DuckDB
 * file lock, the DB file-watcher used to fire a DAG refresh on every WAL
 * write MATLAB made *during* the run. Every one of those refreshes issued
 * graph RPCs against a database the GUI cannot open, which is how a single
 * MATLAB run wedged the whole UI.
 */

import { test } from 'node:test';
import * as assert from 'node:assert';
import { MatlabRunTracker } from './matlabRunTracker';

test('db changes refresh immediately when no MATLAB run is active', () => {
  const tracker = new MatlabRunTracker();
  assert.equal(tracker.isActive, false);
  assert.equal(tracker.noteDbChange(), true);
  // Nothing was withheld, so there is nothing to replay.
  assert.equal(tracker.takeDeferredRefresh(), false);
});

test('db changes are withheld while MATLAB owns the database', () => {
  const tracker = new MatlabRunTracker();
  tracker.begin('run-1');

  assert.equal(tracker.isActive, true);
  assert.equal(tracker.noteDbChange(), false);
  assert.equal(tracker.noteDbChange(), false);

  tracker.end('run-1');
  assert.equal(tracker.isActive, false);
  // Many withheld changes collapse into exactly one replayed refresh.
  assert.equal(tracker.takeDeferredRefresh(), true);
  assert.equal(tracker.takeDeferredRefresh(), false);
});

test('a MATLAB run that wrote nothing costs no refresh', () => {
  const tracker = new MatlabRunTracker();
  tracker.begin('run-1');
  tracker.end('run-1');
  assert.equal(tracker.takeDeferredRefresh(), false);
});

test('the database stays owned until the LAST concurrent run ends', () => {
  const tracker = new MatlabRunTracker();
  tracker.begin('run-1');
  tracker.begin('run-2');
  tracker.noteDbChange();

  tracker.end('run-1');
  assert.equal(tracker.isActive, true, 'run-2 still holds the database');
  assert.equal(tracker.noteDbChange(), false);

  tracker.end('run-2');
  assert.equal(tracker.isActive, false);
  assert.equal(tracker.takeDeferredRefresh(), true);
});

test('onAllFinished fires once, when the last run clears', () => {
  const tracker = new MatlabRunTracker();
  let fired = 0;
  tracker.onAllFinished(() => { fired++; });

  tracker.begin('run-1');
  tracker.begin('run-2');
  tracker.end('run-1');
  assert.equal(fired, 0, 'still one run in flight');

  tracker.end('run-2');
  assert.equal(fired, 1);
});

test('run_done for a Python run is not mistaken for a MATLAB one', () => {
  const tracker = new MatlabRunTracker();
  let fired = 0;
  tracker.onAllFinished(() => { fired++; });

  // extension.ts calls end() for EVERY run_done; untracked ids must be
  // inert, or a Python run would clear MATLAB's ownership.
  assert.equal(tracker.end('python-run'), false);
  assert.equal(tracker.end(undefined), false);
  assert.equal(fired, 0);

  tracker.begin('matlab-run');
  assert.equal(tracker.end('python-run'), false);
  assert.equal(tracker.isActive, true);
  assert.equal(tracker.end('matlab-run'), true);
  assert.equal(fired, 1);
});

// --- shared engine vs this session's own MATLAB ---------------------------
// MATLAB is NOT one process per window. `matlab_sidecar._sidecar` is a
// process singleton and every session has its own Python server, so every
// session already has its own sidecar MATLAB — two databases running through
// sidecars are as independent as two Python runs. Only the MathWorks
// terminal is shared (one MATLAB per VS Code window, its design).
//
// The bug this locks down: gating on `isActive` was exactly backwards. A
// sidecar run holds the mark for its whole duration (Python pushes a real
// run_done) and would block the other database pointlessly, while a terminal
// run — the one that genuinely shares an engine — clears it milliseconds
// after dispatch.

test('a sidecar run never claims the shared engine', () => {
  const tracker = new MatlabRunTracker();
  tracker.begin('run-sidecar');
  // No noteSharedEngine call: dispatchMatlabCommand only makes it for the
  // terminal and clipboard tiers.
  assert.equal(tracker.isActive, true, 'it still owns its own database');
  assert.equal(tracker.sharedEngineActive, false, 'but not the shared MATLAB');
});

test('a terminal run claims the shared engine until it ends', () => {
  const tracker = new MatlabRunTracker();
  tracker.begin('run-terminal');
  tracker.noteSharedEngine('run-terminal');
  assert.equal(tracker.sharedEngineActive, true);

  tracker.end('run-terminal');
  assert.equal(tracker.sharedEngineActive, false);
  assert.equal(tracker.isActive, false);
});

test('one session can hold the engine while another sidecar run proceeds', () => {
  const shared = new MatlabRunTracker();
  shared.begin('a');
  shared.noteSharedEngine('a');
  const own = new MatlabRunTracker();
  own.begin('b');

  assert.equal(shared.sharedEngineActive, true, 'A holds the MathWorks terminal');
  assert.equal(own.sharedEngineActive, false, "B's sidecar holds nothing shared");
});

test('the shared mark is ignored for a run that is not in flight', () => {
  // A run_done that arrived first must not leave a permanent claim behind,
  // which would wedge every later MATLAB run in every other database.
  const tracker = new MatlabRunTracker();
  tracker.noteSharedEngine('never-begun');
  assert.equal(tracker.sharedEngineActive, false);
});

// Regression (scidb.log 2026-09-25): a GUI Python run's own WAL writes fired
// the file watcher mid-save, so the canvas rebuilt against the writer (26.5s)
// and drew the pre-save graph. The run's own dag_updated is the refresh.
test('db changes are dropped, not deferred, during a Python run', () => {
  const tracker = new MatlabRunTracker();
  tracker.beginPythonRun('py-1');

  assert.equal(tracker.pythonRunActive, true);
  assert.equal(tracker.isActive, false, 'a Python run never counts as MATLAB');
  assert.equal(tracker.noteDbChange(), false);

  tracker.end('py-1');
  assert.equal(tracker.pythonRunActive, false);
  // Not replayed: the backend pushes its own dag_updated after run_done.
  assert.equal(tracker.takeDeferredRefresh(), false);
  assert.equal(tracker.noteDbChange(), true);
});

test('a multi-target Python run (one run_start per target) clears on one run_done', () => {
  const tracker = new MatlabRunTracker();
  tracker.beginPythonRun('py-1');
  tracker.beginPythonRun('py-1');
  tracker.end('py-1');
  assert.equal(tracker.pythonRunActive, false);
});

test('ending a Python run neither fires onAllFinished nor clears MATLAB', () => {
  const tracker = new MatlabRunTracker();
  let fired = 0;
  tracker.onAllFinished(() => { fired++; });
  tracker.begin('m-1');
  tracker.beginPythonRun('py-1');

  tracker.end('py-1');
  assert.equal(fired, 0);
  assert.equal(tracker.isActive, true);
  // MATLAB still owns the database, so the change is deferred as before.
  assert.equal(tracker.noteDbChange(), false);
  tracker.end('m-1');
  assert.equal(tracker.takeDeferredRefresh(), true);
});

test('clearPythonRuns releases a run whose server died before run_done', () => {
  const tracker = new MatlabRunTracker();
  tracker.beginPythonRun('py-1');
  tracker.clearPythonRuns();
  assert.equal(tracker.pythonRunActive, false);
  assert.equal(tracker.noteDbChange(), true);
});
