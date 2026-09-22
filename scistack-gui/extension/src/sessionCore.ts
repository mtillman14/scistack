/**
 * sessionCore — what a "session" is, independent of VS Code.
 *
 * A **session** is one open database: one Python server process, one pipeline
 * canvas, and the plot tabs opened from it. Before this module the extension
 * held exactly one of each at module scope (`pythonProcess`, `dagPanel`,
 * `dbWatcher`, `lastStartArgs`, a status bar item), so "Open Pipeline" on a
 * second database killed the first one's server and reused its panel — the
 * canvas changed but the header did not, because nothing re-fetches
 * `get_info` (reported 2026-09-22).
 *
 * Everything here is deliberately free of `vscode`, exactly as
 * `panelRegistry.ts` is: a session is an id, a database path and a bag of
 * panels, which makes lookup, routing and resolution testable under
 * `node --test`. The `vscode`-touching half — spawning the server, creating
 * the webviews, the file watcher — lives in `session.ts`.
 */

import * as path from 'path';

/**
 * Anything that takes a line of diagnostic text.
 *
 * `vscode.OutputChannel` satisfies it, and so does `prefixedLog`'s result,
 * which is the point: with two servers writing to one "SciStack" channel,
 * an unprefixed line cannot be attributed to a database. Panels and the
 * process wrapper take this instead of the concrete channel so the prefix
 * is applied once, at the session boundary.
 */
export interface LogSink {
  appendLine(line: string): void;
}

/**
 * A sink that stamps every line with `[<prefix>]`.
 *
 * Deliberately not one Output Channel per session: when two databases fight
 * over a file lock, or a MATLAB run in one blocks a refresh in the other,
 * the thing you need is the INTERLEAVING — which is exactly what separate
 * channels destroy.
 */
export function prefixedLog(sink: LogSink, prefix: string): LogSink {
  return {
    appendLine(line: string): void {
      // Multi-line writes (a stderr chunk) get the prefix on every line, or
      // a traceback's first line is attributed and the rest is anonymous.
      for (const one of line.split('\n')) {
        sink.appendLine(`[${prefix}] ${one}`);
      }
    },
  };
}

/**
 * The identity of a session: its database file, canonicalised.
 *
 * Two spellings of one path must resolve to ONE session, or "Open Pipeline"
 * on an already-open database starts a second server and the two race for
 * DuckDB's single-writer file lock. Case-insensitivity on Windows is part
 * of that: `C:\Data\study.duckdb` and `c:\data\Study.duckdb` are one file.
 */
export function sessionIdForDb(dbPath: string, platform: string = process.platform): string {
  const resolved = path.resolve(dbPath);
  return platform === 'win32' ? resolved.toLowerCase() : resolved;
}

/** Short, human-facing name for a session — what the tab and log prefix use. */
export function sessionLabel(dbPath: string): string {
  return path.basename(dbPath);
}

/**
 * A filesystem-safe token unique to a session, for naming files it owns.
 *
 * The MATLAB dispatch writes its generated script to a temp file and sends
 * MATLAB a one-line `run('<path>')` (see `matlabTerminal.ts`). That filename
 * was fixed, so two sessions dispatching close together raced on one file:
 * the second write lands before the first `run` reads, and the first
 * database's canvas runs the second database's script.
 *
 * A hash rather than the basename: two projects may both hold a
 * `data.duckdb`, and the full path is not a legal filename.
 */
export function sessionSlug(id: string): string {
  // FNV-1a — a filename discriminator, not a security boundary; the point is
  // that two different paths do not collide, which 32 bits comfortably gives
  // for the handful of databases anyone has open at once.
  let hash = 0x811c9dc5;
  for (let i = 0; i < id.length; i++) {
    hash ^= id.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, '0');
}

/**
 * Which workspace folder is "the project" for this database?
 *
 * The extension used to pass `workspaceFolders[0]` to every server, which is
 * right for exactly one session. With two databases open from a multi-root
 * workspace, the second server would be handed the FIRST project's root,
 * read that project's `scistack.toml` and discover the wrong code —
 * `config.resolve_project_root` rule 2 takes `--project-root` at its word.
 *
 * So: the innermost open folder that contains the database, falling back to
 * the first folder (the old behaviour, correct for the common case of a
 * `.duckdb` under `datasets/` in the one open project) and then to
 * undefined, which makes the server fall back to its cwd and the extension
 * warn.
 */
export function projectRootForDb(
  dbPath: string,
  folders: readonly string[],
  platform: string = process.platform,
): string | undefined {
  if (folders.length === 0) return undefined;
  const db = sessionIdForDb(dbPath, platform);

  let best: string | undefined;
  for (const folder of folders) {
    const root = sessionIdForDb(folder, platform);
    const prefix = root.endsWith(path.sep) ? root : root + path.sep;
    if (!db.startsWith(prefix)) continue;
    // Innermost wins: with both /repo and /repo/sub open, a database under
    // /repo/sub belongs to /repo/sub.
    if (best === undefined || folder.length > best.length) best = folder;
  }
  return best ?? folders[0];
}

/** The minimum a registry needs to know about a session. */
export interface SessionLike {
  readonly id: string;
  readonly dbPath: string;
}

/** How `resolve` decided, so the choice can be logged rather than guessed at. */
export type SessionSource = 'explicit' | 'only' | 'active' | 'none';

export interface SessionResolution<S> {
  session: S | undefined;
  source: SessionSource;
  /** One clause naming the reason, for the Output Channel. */
  detail: string;
}

/**
 * The open sessions, and the answer to "which one does this command mean?".
 *
 * A command arrives from one of two places and they differ in how much they
 * know:
 *
 *   - from inside a webview (the canvas's right-click ▸ Plot, its Restart
 *     button) — the host handler is that panel's own closure, so it can name
 *     the session outright. Always correct;
 *   - from the Command Palette or the status bar — nothing in the command
 *     says which database, so the last focused panel decides.
 *
 * `resolve` returns which of those applied. Callers log it, because "the
 * command went to the other database" and "the command did nothing" look
 * identical from the outside otherwise.
 */
export class SessionRegistry<S extends SessionLike> {
  private sessions = new Map<string, S>();
  private activeId: string | undefined;

  get size(): number {
    return this.sessions.size;
  }

  /** Every open session, in the order they were opened. */
  all(): S[] {
    return [...this.sessions.values()];
  }

  get(id: string): S | undefined {
    return this.sessions.get(id);
  }

  /** The session for a database path, whatever spelling it arrives in. */
  byDbPath(dbPath: string): S | undefined {
    return this.sessions.get(sessionIdForDb(dbPath));
  }

  /** Register a session and make it the active one (it was just opened). */
  add(session: S): void {
    this.sessions.set(session.id, session);
    this.activeId = session.id;
  }

  /**
   * Forget a session. If it was the active one, the most recently added
   * survivor takes over — never a dangling id, which would make `resolve`
   * report 'active' and hand back undefined.
   */
  remove(id: string): void {
    this.sessions.delete(id);
    if (this.activeId !== id) return;
    const survivors = this.all();
    this.activeId = survivors.length ? survivors[survivors.length - 1].id : undefined;
  }

  /** Note that a session's panel was focused. Unknown ids are ignored. */
  setActive(id: string): void {
    if (this.sessions.has(id)) this.activeId = id;
  }

  get active(): S | undefined {
    return this.activeId ? this.sessions.get(this.activeId) : undefined;
  }

  /**
   * Which session a command means — explicit id first, then "there is only
   * one", then the last focused panel.
   *
   * The 'only' step is not redundant with 'active': a command can arrive
   * before any panel has ever been focused (the Explorer context menu at
   * startup), and with a single session open there is nothing to be
   * ambiguous about.
   */
  resolve(explicitId?: string): SessionResolution<S> {
    if (explicitId) {
      const session = this.sessions.get(explicitId);
      if (session) {
        return {
          session,
          source: 'explicit',
          detail: `named by the calling panel (${session.dbPath})`,
        };
      }
      // A stale id — the panel outlived its session. Fall through rather
      // than fail: the user's intent is still "plot something".
    }
    const all = this.all();
    if (all.length === 1) {
      return {
        session: all[0],
        source: 'only',
        detail: `the only open database (${all[0].dbPath})`,
      };
    }
    const active = this.active;
    if (active) {
      return {
        session: active,
        source: 'active',
        detail: `the last focused pipeline (${active.dbPath})`,
      };
    }
    return { session: undefined, source: 'none', detail: 'no database is open' };
  }
}
