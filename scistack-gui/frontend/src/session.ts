/**
 * Which database this webview belongs to.
 *
 * The extension host injects it into the page (`dagPanel.ts` and
 * `plotPanel.ts`) before the bundle loads, because with several databases
 * open the answer is a property of the TAB, not of the backend: two canvases
 * ask the same questions of two different servers.
 *
 * It also removes a class of staleness. The header used to learn the
 * database name from a single `get_info` on mount, so a canvas that was
 * re-pointed at another database went on showing the old filename
 * (reported 2026-09-22) — and `retainContextWhenHidden` kept that stale
 * React state alive indefinitely. A value injected into the page cannot
 * disagree with the page.
 *
 * Absent in the browser build (`scistack-gui` CLI), which has one database
 * per process and learns its name from `get_info` as before.
 */

export interface SessionInfo {
  /** Opaque session id — the canonical database path. */
  id: string
  /** Basename of the .duckdb, or null for a database-less CSV plot tab. */
  dbName: string | null
  dbPath: string | null
}

declare global {
  interface Window {
    __SCISTACK_SESSION__?: SessionInfo
  }
}

export function currentSession(): SessionInfo | undefined {
  return typeof window === 'undefined' ? undefined : window.__SCISTACK_SESSION__
}

/** The database name to show, or "" when only the backend knows it. */
export function injectedDbName(): string {
  return currentSession()?.dbName ?? ''
}
