/**
 * Was a `.duckdb` change this GUI's own write?
 *
 * Since canvas-as-document the server writes layout and edges into the
 * `.duckdb`, then pushes `dag_updated` itself. The file watcher (session.ts)
 * sees the same write ~2 s later and pushed a SECOND refresh, each taking
 * 2-3.5 s of get_pipeline: one of the causes of the canvas flicker
 * (GitHub #23, scistack-debug.log 2026-11-18).
 *
 * The server's push (`dag_updated`, or `db_written` for a drag, which needs no
 * refresh) is the signal: a change event within
 * {@link OWN_WRITE_WINDOW_MS} of one is the server's own write. A burst is
 * skipped only when EVERY change in it is explained, so a MATLAB or script
 * write between two GUI writes still refreshes. A write the server makes
 * without pushing is not explained, so it refreshes as before.
 *
 * vscode-free so `npm test` can run it.
 */

/** How far a change event may sit from the server's push, either side
 *  (the file event can land before or after the notification). */
export const OWN_WRITE_WINDOW_MS = 1000;

/** Pushes older than this are forgotten. */
const KEEP_MS = 60_000;

export class OwnWrites {
  private emits: number[] = [];

  /** The server pushed `dag_updated` or `db_written`: it just wrote the database. */
  noteEmit(now: number): void {
    this.emits = this.emits.filter(t => now - t <= KEEP_MS);
    this.emits.push(now);
  }

  /** Whether every change time in a burst sits next to one of the server's pushes. */
  explains(changeTimes: number[]): boolean {
    if (changeTimes.length === 0) return false;
    return changeTimes.every(c => this.emits.some(e => Math.abs(c - e) <= OWN_WRITE_WINDOW_MS));
  }
}
