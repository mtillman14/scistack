/**
 * PythonProcess — manages the child Python JSON-RPC server.
 *
 * Responsibilities:
 *   - Spawns `python -m scistack_gui.server --db <path> [--module <path>] [--project <path>]`
 *   - Parses newline-delimited JSON-RPC from stdout
 *   - Routes responses (have `id`) back to pending request promises
 *   - Routes notifications (no `id`) to registered listeners
 *   - Logs stderr to the VS Code Output Channel
 *   - Handles process crash/exit
 */

import { spawn, ChildProcess } from 'child_process';
import * as readline from 'readline';
import * as vscode from 'vscode';

type NotificationHandler = (method: string, params: Record<string, unknown>) => void;

interface PendingRequest {
  resolve: (value: unknown) => void;
  reject: (reason: Error) => void;
}

interface ReadyParams {
  db_name: string;
  schema_keys: string[];
}

export class PythonProcess {
  private proc: ChildProcess;
  private nextId = 1;
  private pending = new Map<number, PendingRequest>();
  private notificationHandlers: NotificationHandler[] = [];
  private readyResolve: ((params: ReadyParams) => void) | null = null;
  private readyReject: ((err: Error) => void) | null = null;

  constructor(
    pythonPath: string,
    dbPath: string,
    modulePath: string | undefined,
    private outputChannel: vscode.OutputChannel,
    schemaKeys?: string[],
    projectPath?: string,
  ) {
    const args = ['-m', 'scistack_gui.server', '--db', dbPath];
    if (projectPath) {
      args.push('--project', projectPath);
    } else if (modulePath) {
      args.push('--module', modulePath);
    }
    if (schemaKeys && schemaKeys.length > 0) {
      args.push('--schema-keys', schemaKeys.join(','));
    }

    this.outputChannel.appendLine(`Spawning: ${pythonPath} ${args.join(' ')}`);

    // If the user enabled scistack.debug, pass env vars so server.py starts a
    // debugpy listener that VS Code can attach to (so breakpoints inside user
    // functions invoked by DAG Run buttons get hit).
    const cfg = vscode.workspace.getConfiguration('scistack');
    const debugEnabled = cfg.get<boolean>('debug', false);
    const debugPort = cfg.get<number>('debugPort', 5678);
    const childEnv: NodeJS.ProcessEnv = { ...process.env };
    if (debugEnabled) {
      childEnv.SCISTACK_GUI_DEBUG = '1';
      childEnv.SCISTACK_GUI_DEBUG_PORT = String(debugPort);
      this.outputChannel.appendLine(
        `debugpy listener will start on 127.0.0.1:${debugPort} ` +
        `(attach via "Attach to scistack-gui server" launch config)`
      );
    }

    this.proc = spawn(pythonPath, args, {
      stdio: ['pipe', 'pipe', 'pipe'],
      env: childEnv,
    });

    // Parse newline-delimited JSON from stdout
    const rl = readline.createInterface({ input: this.proc.stdout! });
    rl.on('line', (line) => this.handleLine(line));

    // Forward stderr to Output Channel
    this.proc.stderr?.on('data', (data: Buffer) => {
      this.outputChannel.appendLine(data.toString().trimEnd());
    });

    // Handle process exit
    this.proc.on('exit', (code, signal) => {
      const msg = `Python process exited (code=${code}, signal=${signal})`;
      this.outputChannel.appendLine(msg);

      // Reject all pending requests
      for (const [, pending] of this.pending) {
        pending.reject(new Error(msg));
      }
      this.pending.clear();

      // Reject ready promise if still waiting
      if (this.readyReject) {
        this.readyReject(new Error(msg));
        this.readyResolve = null;
        this.readyReject = null;
      }
    });

    this.proc.on('error', (err) => {
      this.outputChannel.appendLine(`Python process error: ${err.message}`);
      if (this.readyReject) {
        this.readyReject(err);
        this.readyResolve = null;
        this.readyReject = null;
      }
    });
  }

  /**
   * Wait for the Python server to signal readiness.
   * Returns the ready notification params (db_name, schema_keys).
   */
  waitForReady(timeoutMs: number): Promise<ReadyParams> {
    return new Promise((resolve, reject) => {
      this.readyResolve = resolve;
      this.readyReject = reject;

      setTimeout(() => {
        if (this.readyReject) {
          this.readyReject(new Error(`Python server did not become ready within ${timeoutMs}ms`));
          this.readyResolve = null;
          this.readyReject = null;
        }
      }, timeoutMs);
    });
  }

  /**
   * Send a JSON-RPC request and return a promise for the result.
   */
  request(method: string, params: Record<string, unknown>): Promise<unknown> {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      const msg = JSON.stringify({ jsonrpc: '2.0', method, params, id });
      this.proc.stdin?.write(msg + '\n', (err) => {
        if (err) {
          this.pending.delete(id);
          reject(err);
        }
      });
    });
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
      const pending = this.pending.get(id);
      if (pending) {
        this.pending.delete(id);
        if ('error' in msg) {
          const err = msg.error as { message: string };
          pending.reject(new Error(err.message));
        } else {
          pending.resolve(msg.result);
        }
      }
      return;
    }

    // Notification (no id)
    const method = msg.method as string;
    const params = (msg.params ?? {}) as Record<string, unknown>;

    // Special case: ready notification
    if (method === 'ready' && this.readyResolve) {
      this.readyResolve(params as unknown as ReadyParams);
      this.readyResolve = null;
      this.readyReject = null;
      return;
    }

    // Special case: error during startup
    if (method === 'error') {
      this.outputChannel.appendLine(`Server error: ${params.message}`);
      if (this.readyReject) {
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
