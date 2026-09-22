/**
 * Tests for sessionCore — run with `npm test` in extension/.
 *
 * The regressions these lock down:
 *
 *  - Opening a second database used to kill the first one's server and reuse
 *    its panel, so the canvas changed but the header kept the old database's
 *    filename (reported 2026-09-22). One session per database, addressed by a
 *    canonical id, is what makes that impossible.
 *  - Every server was handed `workspaceFolders[0]` as its project root. With
 *    two databases from a multi-root workspace the second one would read the
 *    FIRST project's scistack.toml and discover the wrong code.
 */

import { test } from 'node:test';
import * as assert from 'node:assert';
import * as path from 'node:path';
import {
  LogSink,
  SessionRegistry,
  prefixedLog,
  projectRootForDb,
  sessionIdForDb,
  sessionSlug,
} from './sessionCore';

interface FakeSession {
  id: string;
  dbPath: string;
}

function session(dbPath: string): FakeSession {
  return { id: sessionIdForDb(dbPath), dbPath };
}

const A = path.resolve('/studies/gait/gait.duckdb');
const B = path.resolve('/studies/emg/emg.duckdb');

// --- identity --------------------------------------------------------------

test('two spellings of one database are one session', () => {
  const registry = new SessionRegistry<FakeSession>();
  registry.add(session(A));

  // A relative-ish spelling of the same file must find the open session,
  // or "Open Pipeline" spawns a second server that loses DuckDB's
  // single-writer race against the first.
  assert.ok(registry.byDbPath(path.join('/studies', 'gait', '..', 'gait', 'gait.duckdb')));
  assert.equal(registry.size, 1);
});

test('database identity is case-insensitive on Windows only', () => {
  const upper = 'C:\\Data\\Study.duckdb';
  const lower = 'C:\\data\\study.duckdb';
  assert.equal(sessionIdForDb(upper, 'win32'), sessionIdForDb(lower, 'win32'));
  assert.notEqual(
    sessionIdForDb('/data/Study.duckdb', 'linux'),
    sessionIdForDb('/data/study.duckdb', 'linux'),
  );
});

// --- project root ----------------------------------------------------------

test('the project root is the folder containing the database', () => {
  const first = path.resolve('/studies/gait');
  const second = path.resolve('/studies/emg');
  // The regression: this used to return `first` for both.
  assert.equal(projectRootForDb(B, [first, second]), second);
  assert.equal(projectRootForDb(A, [first, second]), first);
});

test('the innermost containing folder wins', () => {
  const outer = path.resolve('/repo');
  const inner = path.resolve('/repo/analysis');
  const db = path.resolve('/repo/analysis/data/study.duckdb');
  assert.equal(projectRootForDb(db, [outer, inner]), inner);
});

test('a database outside every open folder falls back to the first', () => {
  // The common single-project case where the .duckdb lives on another drive.
  const only = path.resolve('/repo');
  assert.equal(projectRootForDb(path.resolve('/mnt/data/study.duckdb'), [only]), only);
});

test('no folder open means no --project-root', () => {
  assert.equal(projectRootForDb(A, []), undefined);
});

// --- resolution ------------------------------------------------------------

test('an explicit session id wins over the focused one', () => {
  const registry = new SessionRegistry<FakeSession>();
  const a = session(A);
  const b = session(B);
  registry.add(a);
  registry.add(b); // b is active, having just been opened

  const resolved = registry.resolve(a.id);
  assert.equal(resolved.session, a);
  assert.equal(resolved.source, 'explicit');
});

test('with several open and none named, the focused session wins', () => {
  const registry = new SessionRegistry<FakeSession>();
  const a = session(A);
  const b = session(B);
  registry.add(a);
  registry.add(b);
  registry.setActive(a.id);

  const resolved = registry.resolve();
  assert.equal(resolved.session, a);
  assert.equal(resolved.source, 'active');
});

test('a stale session id falls back rather than failing', () => {
  // A plot tab can outlive the session it was opened from; the user's
  // intent is still "plot something".
  const registry = new SessionRegistry<FakeSession>();
  const a = session(A);
  registry.add(a);

  const resolved = registry.resolve(sessionIdForDb('/gone/closed.duckdb'));
  assert.equal(resolved.session, a);
  assert.equal(resolved.source, 'only');
});

test('nothing open resolves to nothing, with a reason', () => {
  const resolved = new SessionRegistry<FakeSession>().resolve();
  assert.equal(resolved.session, undefined);
  assert.equal(resolved.source, 'none');
  assert.match(resolved.detail, /no database/);
});

test('closing the active session promotes a survivor, never a dangling id', () => {
  const registry = new SessionRegistry<FakeSession>();
  const a = session(A);
  const b = session(B);
  registry.add(a);
  registry.add(b);
  assert.equal(registry.active, b);

  registry.remove(b.id);
  assert.equal(registry.active, a);
  // The failure this guards: active pointing at a removed id makes resolve
  // report 'active' and hand back undefined.
  assert.equal(registry.resolve().session, a);

  registry.remove(a.id);
  assert.equal(registry.active, undefined);
  assert.equal(registry.resolve().source, 'none');
});

// --- logging ---------------------------------------------------------------

test('every line of a multi-line write is attributed', () => {
  const lines: string[] = [];
  const channel: LogSink = { appendLine: (l) => lines.push(l) };
  const log = prefixedLog(channel, 'gait.duckdb');

  log.appendLine('Traceback (most recent call last):\n  File "x.py", line 1');

  // A traceback whose first line alone is attributed tells you nothing about
  // which of two servers raised it.
  assert.deepEqual(lines, [
    '[gait.duckdb] Traceback (most recent call last):',
    '[gait.duckdb]   File "x.py", line 1',
  ]);
});

// --- per-session filenames -------------------------------------------------

test('two databases get different MATLAB script filenames', () => {
  // The race this prevents: one fixed `scistack_run.m` in the temp dir, two
  // sessions dispatching close together — the second write lands before the
  // first `run('…')` reads it, and one canvas runs the other's script.
  assert.notEqual(sessionSlug(sessionIdForDb(A)), sessionSlug(sessionIdForDb(B)));
});

test('two projects holding a same-named database still differ', () => {
  // Two `data.duckdb` files is the realistic collision, so the slug must
  // come from the whole path, not the basename.
  const one = sessionIdForDb(path.resolve('/studies/gait/data.duckdb'));
  const two = sessionIdForDb(path.resolve('/studies/emg/data.duckdb'));
  assert.notEqual(sessionSlug(one), sessionSlug(two));
});

test('a session slug is stable and filename-safe', () => {
  const slug = sessionSlug(sessionIdForDb(A));
  assert.equal(slug, sessionSlug(sessionIdForDb(A)), 'must not change per call');
  assert.match(slug, /^[0-9a-f]{8}$/);
});
