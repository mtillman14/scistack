/**
 * PythonProcess — manages the child Python JSON-RPC server.
 *
 * Responsibilities:
 *   - Spawns `python -m scistack_gui.server ...` (argv built by
 *     `serverArgs.buildServerArgs` / `buildPlotOnlyServerArgs`)
 *   - Parses newline-delimited JSON-RPC from stdout
 *   - Routes responses (have `id`) back to pending request promises
 *   - Routes notifications (no `id`) to registered listeners
 *   - Logs stderr to the session's log sink
 *   - Handles process crash/exit
 *
 * One of these per open database. It takes its argv, its cwd and its debug
 * port rather than reading them from `vscode.workspace` itself, because with
 * several sessions open those answers differ per session: the project root is
 * the folder containing THIS database, and two servers cannot share one
 * debugpy port.
 */

import { spawn, ChildProcess } from 'child_process';
import * as readline from 'readline';
import * as vscode from 'vscode';
import { LogSink } from './sessionCore';
import { PendingRequests } from './rpcPending';

type NotificationHandler = (method: string, params: Record<string, unknown>) => void;

interface ReadyParams {
  db_name: string;
  schema_keys?: string[];
  /** False for a plot-only server: no database, only the `plot_*` methods. */
  db_loaded?: boolean;
}

export interface PythonProcessOptions {
  /** Run the server from here — the session's own project folder. */
  cwd?: string;
  /**
   * Port for this server's debugpy listener, or undefined to start without
   * one. Each session needs its own: `debugpy.listen` on a port another
   * session already holds fails, and the failure is only a warning in the
   * server's log.
   */
  debugPort?: number;
}

/** How many stderr lines to keep for startup diagnostics. */
const STDERR_TAIL_LINES = 200;

export class PythonProcess {
  private proc: ChildProcess;
  private nextId = 1;
  /** Requests awaiting a response; owns the rpcTimeoutMs backstop. */
  private pending: PendingRequests;
  private notificationHandlers: NotificationHandler[] = [];
  private readyResolve: ((params: ReadyParams) => void) | null = null;
  private readyReject: ((err: Error) => void) | null = null;
  private readyTimer: NodeJS.Timeout | null = null;
  private readyTimeoutMs = 0;
  /** Ring of recent stderr lines, so a failed start can report why. */
  private stderrTail: string[] = [];
  private exitCode: number | null = null;
  /** Resolves on 'close', i.e. after the child's stdio has been drained. */
  private closed: Promise<void>;
  /** The spawn args, kept for the diagnostic report on a failed start. */
  readonly args: string[];

  constructor(
    readonly pythonPath: string,
    args: string[],
    private outputChannel: LogSink,
    options: PythonProcessOptions = {},
  ) {
    this.args = args;
    this.pending = new PendingRequests(outputChannel);

    this.outputChannel.appendLine(`Spawning: ${pythonPath} ${args.join(' ')}`);

    // If the user enabled scistack.debug, pass env vars so server.py starts a
    // debugpy listener that VS Code can attach to (so breakpoints inside user
    // functions invoked by DAG Run buttons get hit).
    const childEnv: NodeJS.ProcessEnv = { ...process.env };
    if (options.debugPort !== undefined) {
      childEnv.SCISTACK_GUI_DEBUG = '1';
      childEnv.SCISTACK_GUI_DEBUG_PORT = String(options.debugPort);
      this.outputChannel.appendLine(
        `debugpy listener will start on 127.0.0.1:${options.debugPort} ` +
        `(attach via "Attach to scistack-gui server" launch config)`
      );
    }

    // Run the server FROM the folder the user opened. Without this the
    // child inherits the extension host's working directory (typically
    // VS Code's own install directory), which makes cwd meaningless as a
    // project-root signal — see config.resolve_project_root, where cwd is
    // the fallback for non-VS-Code callers. --project-root in the argv
    // remains the explicit signal; this just makes the two agree.
    this.proc = spawn(pythonPath, args, {
      stdio: ['pipe', 'pipe', 'pipe'],
      env: childEnv,
      cwd: options.cwd,
    });

    this.closed = new Promise((resolve) => {
      this.proc.on('close', () => resolve());
    });

    // Parse newline-delimited JSON from stdout
    const rl = readline.createInterface({ input: this.proc.stdout! });
    rl.on('line', (line) => this.handleLine(line));

    // Forward stderr to Output Channel, and keep a tail of it: when the
    // server dies before "ready" (e.g. this interpreter has no scistack_gui)
    // its stderr is the only statement of *why*, and the exit code is not.
    this.proc.stderr?.on('data', (data: Buffer) => {
      const text = data.toString().trimEnd();
      this.outputChannel.appendLine(text);
      for (const line of text.split('\n')) {
        this.stderrTail.push(line);
      }
      if (this.stderrTail.length > STDERR_TAIL_LINES) {
        this.stderrTail.splice(0, this.stderrTail.length - STDERR_TAIL_LINES);
      }
    });

    // Handle process exit
    this.proc.on('exit', (code, signal) => {
      this.exitCode = code;
      const msg = `Python process exited (code=${code}, signal=${signal})`;
      this.outputChannel.appendLine(msg);

      // Reject all pending requests
      this.pending.failAll(new Error(msg));

      // Reject ready promise if still waiting
      if (this.readyReject) {
        if (this.readyTimer) {
          clearTimeout(this.readyTimer);
          this.readyTimer = null;
        }
        this.readyReject(new Error(msg));
        this.readyResolve = null;
        this.readyReject = null;
      }
    });

    this.proc.on('error', (err) => {
      this.outputChannel.appendLine(`Python process error: ${err.message}`);
      if (this.readyReject) {
        if (this.readyTimer) {
          clearTimeout(this.readyTimer);
          this.readyTimer = null;
        }
        this.readyReject(err);
        this.readyResolve = null;
        this.readyReject = null;
      }
    });
  }

  /**
   * Wait until the child has closed its stdio, or `timeoutMs` elapses.
   *
   * The ready promise can reject (on 'exit', or on the inactivity timer)
   * while the last stderr chunk is still queued, so a diagnostic report must
   * wait for 'close' or it can quote an empty traceback.
   */
  whenClosed(timeoutMs = 2000): Promise<void> {
    return Promise.race([
      this.closed,
      new Promise<void>((resolve) => setTimeout(resolve, timeoutMs)),
    ]);
  }

  /** Recent stderr from the child process (oldest first). */
  getStderr(): string {
    return this.stderrTail.join('\n');
  }

  /** Exit code, or null while the process is still running. */
  getExitCode(): number | null {
    return this.exitCode;
  }

  /** Why this process can no longer take a request, or null while it can. */
  private deadReason(): string | null {
    if (this.exitCode !== null) return `it exited with code ${this.exitCode}`;
    if (this.proc.killed) return 'it was stopped';
    if (!this.proc.stdin || this.proc.stdin.destroyed) return 'its input stream is closed';
    return null;
  }

  /**
   * Wait for the Python server to signal readiness.
   * Returns the ready notification params (db_name, schema_keys).
   *
   * The ``timeoutMs`` is an *inactivity* timeout: it resets whenever a
   * ``progress`` notification arrives from the server. This lets slow-but-
   * progressing startups (e.g. projects on network drives) complete
   * without falsely timing out, while still killing a truly stuck server.
   */
  waitForReady(timeoutMs: number): Promise<ReadyParams> {
    this.readyTimeoutMs = timeoutMs;
    return new Promise((resolve, reject) => {
      this.readyResolve = resolve;
      this.readyReject = reject;
      this.resetReadyTimer(timeoutMs);
    });
  }

  private resetReadyTimer(timeoutMs: number): void {
    if (this.readyTimer) {
      clearTimeout(this.readyTimer);
    }
    this.readyTimer = setTimeout(() => {
      this.readyTimer = null;
      if (this.readyReject) {
        this.readyReject(new Error(
          `Python server did not become ready within ${timeoutMs}ms ` +
          `of silence (no progress notification received).`
        ));
        this.readyResolve = null;
        this.readyReject = null;
      }
    }, timeoutMs);
  }

  /**
   * Send a JSON-RPC request and return a promise for the result.
   *
   * Every request carries a timeout. The server is supposed to answer every
   * request exactly once — long work is reported asynchronously through
   * run_output/run_done notifications, not by holding an RPC open — so a
   * response that never arrives means the server lost the request, and
   * without a timeout that wedges the caller permanently with no error
   * anywhere. (That is precisely how a MATLAB-locked database used to hang
   * the whole GUI; see scistack_gui/server.py::_handle_request.) The
   * timeout is a backstop, not a work limit: it is deliberately generous
   * and configurable via `scistack.rpcTimeoutMs`.
   */
  request(method: string, params: Record<string, unknown>): Promise<unknown> {
    // A dead process cannot be written to. Say so, instead of letting Node
    // report the symptom ("ERR_STREAM_DESTROYED: Cannot call write after a
    // stream was destroyed"), which names neither the server nor the fix.
    const gone = this.deadReason();
    if (gone) {
      this.outputChannel.appendLine(`RPC refused: ${method} — ${gone}`);
      return Promise.reject(new Error(
        `SciStack: the Python server is not running (${gone}). ` +
        `Reopen the pipeline (or run "SciStack: Restart Python") and try again.`,
      ));
    }
    const id = this.nextId++;
    const timeoutMs = vscode.workspace
      .getConfiguration('scistack')
      .get<number>('rpcTimeoutMs', 300000);
    // Registered before the write, so a response can never beat its entry.
    const reply = this.pending.open(id, method, timeoutMs);

    const msg = JSON.stringify({ jsonrpc: '2.0', method, params, id });
    this.proc.stdin?.write(msg + '\n', (err) => {
      if (err) {
        this.outputChannel.appendLine(`RPC write failed: ${method} — ${err.message}`);
        this.pending.fail(id, new Error(
          `SciStack: could not send '${method}' to the Python server ` +
          `(${err.message}). It may have exited — check the SciStack output channel.`,
        ));
      }
    });
    return reply;
  }

  /**
   * Register a handler for push notifications from Python.
   */
  onNotification(handler: NotificationHandler): void {
    this.notificationHandlers.push(handler);
  }

  /**
   * Kill the Python process.
   */
  kill(): void {
    this.proc.kill();
  }

  private handleLine(line: string): void {
    let msg: Record<string, unknown>;
    try {
      msg = JSON.parse(line);
    } catch {
      this.outputChannel.appendLine(`[stdout non-JSON] ${line}`);
      return;
    }

    // Response (has id)
    if ('id' in msg && msg.id !== null && msg.id !== undefined) {
      const id = msg.id as number;
      // Unmatched ids (unknown, or answered after the timeout gave up) are
      // logged by PendingRequests itself.
      if ('error' in msg) {
        this.pending.reject(id, (msg.error as { message: string }).message);
      } else {
        this.pending.resolve(id, msg.result);
      }
      return;
    }

    // Notification (no id)
    const method = msg.method as string;
    const params = (msg.params ?? {}) as Record<string, unknown>;

    // Special case: progress notification during startup. Resets the
    // inactivity timer so long-but-progressing startups don't time out.
    if (method === 'progress') {
      this.outputChannel.appendLine(`  ${params.message}`);
      if (this.readyResolve) {
        this.resetReadyTimer(this.readyTimeoutMs);
      }
      return;
    }

    // Special case: ready notification
    if (method === 'ready' && this.readyResolve) {
      if (this.readyTimer) {
        clearTimeout(this.readyTimer);
        this.readyTimer = null;
      }
      this.readyResolve(params as unknown as ReadyParams);
      this.readyResolve = null;
      this.readyReject = null;
      return;
    }

    // Special case: error during startup
    if (method === 'error') {
      this.outputChannel.appendLine(`Server error: ${params.message}`);
      if (this.readyReject) {
        if (this.readyTimer) {
          clearTimeout(this.readyTimer);
          this.readyTimer = null;
        }
        this.readyReject(new Error(params.message as string));
        this.readyResolve = null;
        this.readyReject = null;
      }
      return;
    }

    // Forward to all notification handlers
    for (const handler of this.notificationHandlers) {
      handler(method, params);
    }
  }
}
