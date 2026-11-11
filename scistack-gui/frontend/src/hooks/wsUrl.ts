/**
 * The backend WebSocket URL for a page: the SAME origin that served the page.
 *
 * The server that served the page owns the port (`scistack-gui --port N`), so
 * the page reads it back from its own location, the same way every `/api`
 * fetch is a relative path. Until 2026-11-10 this was the literal
 * `:8765`, a second owner of the port: a GUI started on any other port drew
 * its canvas over HTTP but never received run_output / run_done, so every run
 * looked hung. In `vite dev` the dev server proxies `/ws` to the backend
 * (vite.config.ts), so the same rule holds there.
 *
 * React-free so `npm test` can run it (tsconfig.test.json).
 */
export function wsUrl(loc: { protocol: string; host: string }): string {
  const scheme = loc.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${scheme}//${loc.host}/ws`
}
