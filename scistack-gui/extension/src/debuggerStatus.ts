/**
 * Should a Run attach VS Code's debugger to the Python server?
 *
 * The server reports whether its debugpy listener actually started in the
 * `ready` frame (`server._start_debugger`). Attaching without asking used to
 * fail on every Run with "connection refused 127.0.0.1:5678" whenever
 * `scistack.debug` was on but the interpreter had no debugpy — the server's
 * only record of why was a warning logged before scistack.log existed.
 *
 * Kept free of `vscode` so it runs under `npm test`.
 */

export interface DebuggerStatus {
  state: 'off' | 'listening' | 'failed';
  port: number | null;
  error: string | null;
}

export type AttachDecision =
  | { attach: true; port: number }
  /** `message` is set when the user should be told why (shown once). */
  | { attach: false; message?: string };

/**
 * The ONE rule for attaching. `status` is undefined for a server older than
 * the debugger field — attach as before, on the port this session allocated.
 */
export function decideAttach(
  debugEnabled: boolean,
  status: DebuggerStatus | null | undefined,
  allocatedPort: number | undefined,
): AttachDecision {
  if (!debugEnabled) return { attach: false };
  if (!status) {
    return allocatedPort === undefined ? { attach: false } : { attach: true, port: allocatedPort };
  }
  if (status.state === 'listening' && status.port !== null) {
    return { attach: true, port: status.port };
  }
  if (status.state === 'failed') {
    return {
      attach: false,
      message:
        `SciStack: scistack.debug is on, but the Python server could not start ` +
        `its debugger (${status.error ?? 'unknown error'}), so Run will not stop ` +
        `at breakpoints. Install debugpy into the server's interpreter ` +
        `(pip install debugpy) and restart, or turn scistack.debug off.`,
    };
  }
  // 'off': the setting was turned on after this server started.
  return {
    attach: false,
    message:
      'SciStack: scistack.debug was turned on after this server started; ' +
      'restart the server to enable breakpoints.',
  };
}

/**
 * SciStack's own packages count as library code when debugging, so an error in
 * the user's function leaves "user code" at the loop that catches it, even on
 * an editable install. That is what lets VS Code stop on the failing line.
 */
export const LIBRARY_MODULES = [
  'scifor', 'scidb', 'sciduckdb', 'scicanonicalhash', 'scistacklog',
  'scistack_gui', 'scimatlab', 'scistackplot', 'scistackplotdb',
];

/** VS Code's "User Uncaught Exceptions" breakpoint, in debugpy's own words. */
export const STOP_ON_ERROR_FILTERS = ['userUnhandled'];

/** The attach configuration of a Run's debug session. */
export function attachConfiguration(name: string, port: number) {
  return {
    name,
    type: 'debugpy',
    request: 'attach',
    connect: { host: '127.0.0.1', port },
    justMyCode: true,
    rules: LIBRARY_MODULES.map(module => ({ module, include: false })),
  };
}

/** The combination a run is on, from its `combo_start` progress event. The
 *  loop's own `__combo` counter is left out. */
export function comboLabel(metadata: unknown): string {
  if (!metadata || typeof metadata !== 'object') return '';
  return Object.entries(metadata as Record<string, unknown>)
    .filter(([key]) => !key.startsWith('__'))
    .map(([key, value]) => `${key}=${String(value)}`)
    .join(', ');
}

/** What to tell the user when the debugger stops on an error. */
export function stoppedMessage(functionName: string | undefined, combo: string): string {
  const where = combo ? ` at ${combo}` : '';
  return `SciStack: ${functionName ?? 'the function'} stopped on an error${where}. `
    + 'Continue to go on to the next combination.';
}
