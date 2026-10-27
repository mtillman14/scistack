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
