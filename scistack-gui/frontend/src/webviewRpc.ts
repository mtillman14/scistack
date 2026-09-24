/**
 * Request/response bookkeeping for the VS Code webview transport.
 *
 * Kept free of React and the DOM so `npm test` can run it; `api.ts` wires it
 * to `vscode.postMessage` and the window's `message` event.
 *
 * **There is deliberately no timer here.** The webview used to reject any
 * request not answered within 30s. That duplicated the extension's own
 * backstop (`scistack.rpcTimeoutMs`, owned by `extension/src/rpcPending.ts`)
 * and, being shorter, silently overrode it. It also timed methods the
 * extension answers itself — `pick_save_path` / `pick_save_folder` wait on a
 * person in a file dialog, so taking more than 30s to choose where to save a
 * figure failed the save and dropped the chosen path when it arrived.
 *
 * Every request still settles: the extension replies to every message it
 * receives — host-side methods from a try/catch around the VS Code API,
 * Python-bound ones when the server answers, when `rpcTimeoutMs` gives up, or
 * when the process exits.
 */

export type RpcMessage = {
  jsonrpc: '2.0';
  method: string;
  params: Record<string, unknown>;
  id: number;
};

type Pending = {
  resolve: (value: unknown) => void;
  reject: (reason: Error) => void;
};

export class WebviewRpc {
  private nextId = 1;
  private pending = new Map<number, Pending>();

  constructor(private readonly post: (msg: RpcMessage) => void) {}

  call(method: string, params: Record<string, unknown>): Promise<unknown> {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.post({ jsonrpc: '2.0', method, params, id });
    });
  }

  /**
   * Settle the request a response names. Returns false when nothing was
   * waiting for it (a response from before a webview reload, say).
   */
  handleResponse(msg: { id: number; result?: unknown; error?: { message: string } }): boolean {
    const req = this.pending.get(msg.id);
    if (!req) return false;
    this.pending.delete(msg.id);
    if (msg.error) req.reject(new Error(msg.error.message));
    else req.resolve(msg.result);
    return true;
  }

  get size(): number {
    return this.pending.size;
  }
}
