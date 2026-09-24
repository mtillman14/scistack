/**
 * PendingRequests — the table of JSON-RPC requests awaiting a response, and
 * the ONE owner of the "how long do we wait for a reply?" backstop.
 *
 * Kept vscode-free so `npm test` can cover it: `PythonProcess` reads the
 * `scistack.rpcTimeoutMs` setting and passes the number in.
 *
 * Why this is the only timer: the webview used to arm its own 30s timer on
 * every `callBackend`, in front of this one (300s by default). The shorter
 * always won, so the setting was dead for every webview call — and the 30s
 * timer also covered host-side methods like `pick_save_path`, which wait on a
 * PERSON in a file dialog. A user who took more than 30s choosing where to
 * save a figure got "Request pick_save_path timed out" and their answer was
 * dropped on arrival. Host-side methods never reach this table (they are
 * answered by the VS Code API, which always answers), and Python-bound
 * methods are timed here and nowhere else.
 */

import { LogSink } from './sessionCore';

interface Entry {
  resolve: (value: unknown) => void;
  reject: (reason: Error) => void;
  method: string;
  startedAt: number;
  timer: ReturnType<typeof setTimeout> | null;
}

/** What is remembered about a request after its timer gave up on it. */
interface Expired {
  method: string;
  startedAt: number;
  timedOutAt: number;
}

/** How many timed-out ids to remember for late-response diagnostics. */
const EXPIRED_MEMORY = 100;

export class PendingRequests {
  private entries = new Map<number, Entry>();
  private expired = new Map<number, Expired>();

  constructor(
    private readonly log: LogSink,
    private readonly now: () => number = Date.now,
  ) {}

  /**
   * Register request `id` and return the promise its response settles.
   * `timeoutMs <= 0` disables the backstop.
   */
  open(id: number, method: string, timeoutMs: number): Promise<unknown> {
    return new Promise((resolve, reject) => {
      const startedAt = this.now();
      const timer = timeoutMs > 0
        ? setTimeout(() => {
            if (!this.entries.has(id)) return;
            const elapsed = this.now() - startedAt;
            this.log.appendLine(
              `RPC timeout: ${method} (id=${id}) got no response in ${elapsed}ms. ` +
              `The Python server may have dropped the request — check the ` +
              `stderr above for a traceback.`,
            );
            this.rememberExpired(id, { method, startedAt, timedOutAt: this.now() });
            this.take(id)?.reject(new Error(
              `SciStack: no response from the Python server for '${method}' ` +
              `after ${Math.round(elapsed / 1000)}s.`,
            ));
          }, timeoutMs)
        : null;
      this.entries.set(id, { resolve, reject, method, startedAt, timer });
    });
  }

  /** Deliver a response. Returns false when no request was waiting for it. */
  resolve(id: number, result: unknown): boolean {
    const entry = this.take(id);
    if (!entry) {
      this.logUnmatched(id);
      return false;
    }
    entry.resolve(result);
    return true;
  }

  /** Deliver an error response. Returns false when nothing was waiting. */
  reject(id: number, message: string): boolean {
    const entry = this.take(id);
    if (!entry) {
      this.logUnmatched(id);
      return false;
    }
    this.log.appendLine(
      `RPC error: ${entry.method} (id=${id}, ` +
      `${this.now() - entry.startedAt}ms): ${message}`,
    );
    entry.reject(new Error(message));
    return true;
  }

  /** Fail one request without a server response (e.g. the write failed). */
  fail(id: number, error: Error): void {
    this.take(id)?.reject(error);
  }

  /** Fail every outstanding request — the process is gone. */
  failAll(error: Error): void {
    for (const id of [...this.entries.keys()]) this.fail(id, error);
  }

  get size(): number {
    return this.entries.size;
  }

  private take(id: number): Entry | undefined {
    const entry = this.entries.get(id);
    if (!entry) return undefined;
    if (entry.timer) clearTimeout(entry.timer);
    this.entries.delete(id);
    return entry;
  }

  private rememberExpired(id: number, info: Expired): void {
    this.expired.set(id, info);
    if (this.expired.size > EXPIRED_MEMORY) {
      const oldest = this.expired.keys().next().value as number;
      this.expired.delete(oldest);
    }
  }

  /**
   * A response nobody is waiting for. When it is a request the backstop gave
   * up on, say so with the numbers: "answered 40s after we stopped listening"
   * is what distinguishes a timeout that is too short from a lost request.
   */
  private logUnmatched(id: number): void {
    const late = this.expired.get(id);
    if (late) {
      this.expired.delete(id);
      const now = this.now();
      this.log.appendLine(
        `RPC late response: ${late.method} (id=${id}) answered after ` +
        `${now - late.startedAt}ms, ${now - late.timedOutAt}ms after the ` +
        `timeout gave up on it — result dropped. If this is routine, raise ` +
        `scistack.rpcTimeoutMs or move the method onto a job.`,
      );
      return;
    }
    this.log.appendLine(
      `[stdout] response for unknown request id=${id} — ignored`,
    );
  }
}
