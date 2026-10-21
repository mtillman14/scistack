/**
 * Text for the "SciStack: starting…" notification shown while a server
 * starts up.
 *
 * Startup can take a minute or more when discovery imports files over a
 * network drive, and with no visible sign of life that reads as a failed
 * start. `SessionManager.spawnReady` shows a progress notification for the
 * whole wait; this module owns what it says. It is vscode-free so it can be
 * unit-tested under node's test runner.
 */

/** Shown until the server sends its first `progress` notification. */
export const STARTUP_INITIAL_STATUS = 'Starting Python…';

/**
 * The notification's message: the server's latest progress line plus how
 * long the start has been running, so a slow step still visibly ticks.
 */
export function startupStatusMessage(lastProgress: string | undefined, elapsedMs: number): string {
  const status = lastProgress?.trim() || STARTUP_INITIAL_STATUS;
  return `${status} (${formatElapsed(elapsedMs)})`;
}

/** `7s`, `1m 05s`. */
export function formatElapsed(elapsedMs: number): string {
  const total = Math.max(0, Math.floor(elapsedMs / 1000));
  const minutes = Math.floor(total / 60);
  const seconds = total % 60;
  if (minutes === 0) return `${seconds}s`;
  return `${minutes}m ${String(seconds).padStart(2, '0')}s`;
}
