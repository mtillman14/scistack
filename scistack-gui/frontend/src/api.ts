/**
 * Backend communication layer.
 *
 * In VS Code Webview mode: uses postMessage to the extension host.
 * In standalone mode (FastAPI): uses fetch() to localhost.
 *
 * Components import `callBackend` and don't need to know which mode is active.
 */

type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (reason: Error) => void;
};

let nextId = 1;
const pending = new Map<number, PendingRequest>();

// Detect VS Code Webview environment
const isVSCode = typeof acquireVsCodeApi === 'function';
const vscode = isVSCode ? acquireVsCodeApi() : null;

// In VS Code mode, listen for messages from the extension host
if (isVSCode) {
  window.addEventListener('message', (event: MessageEvent) => {
    const msg = event.data;

    // Response to a request (has id)
    if (msg.id !== undefined && msg.id !== null) {
      const req = pending.get(msg.id);
      if (req) {
        pending.delete(msg.id);
        if (msg.error) {
          req.reject(new Error(msg.error.message));
        } else {
          req.resolve(msg.result);
        }
      }
      return;
    }

    // Notification (no id) — dispatched via notificationHandlers
    if (msg.method) {
      _notificationHandlers.forEach(h => h(msg));
    }
  });
}

// Notification handlers (replaces useWebSocket in VS Code mode)
type MessageHandler = (msg: Record<string, unknown>) => void;
const _notificationHandlers = new Set<MessageHandler>();

/** True when running inside the VS Code extension's webview. */
export const isVSCodeMode = isVSCode;

export function addNotificationHandler(handler: MessageHandler): () => void {
  _notificationHandlers.add(handler);
  return () => _notificationHandlers.delete(handler);
}

/**
 * Call a backend method. Works in both VS Code and standalone modes.
 *
 * In VS Code mode: sends a postMessage to the extension host, which
 * forwards it to the Python JSON-RPC server.
 *
 * In standalone mode: translates the method + params into a fetch() call
 * to the equivalent REST endpoint.
 */
export async function callBackend(method: string, params: Record<string, unknown> = {}): Promise<unknown> {
  if (vscode) {
    return callVSCode(method, params);
  }
  return callFetch(method, params);
}

function callVSCode(method: string, params: Record<string, unknown>): Promise<unknown> {
  const id = nextId++;
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    vscode!.postMessage({ jsonrpc: '2.0', method, params, id });
    // Timeout after 30 seconds
    setTimeout(() => {
      if (pending.has(id)) {
        pending.delete(id);
        reject(new Error(`Request ${method} timed out`));
      }
    }, 30000);
  });
}

/**
 * Standalone mode: map method names to REST endpoints.
 */
async function callFetch(method: string, params: Record<string, unknown>): Promise<unknown> {
  const routes: Record<string, { path: string | ((p: Record<string, unknown>) => string); method?: string; body?: boolean }> = {
    get_pipeline:           { path: '/api/pipeline' },
    get_layout:             { path: '/api/layout' },
    get_schema:             { path: '/api/schema' },
    get_info:               { path: '/api/info' },
    get_registry:           { path: '/api/registry' },
    get_function_params:    { path: (p) => `/api/function/${encodeURIComponent(p.name as string)}/params` },
    get_function_source:    { path: (p) => `/api/function/${encodeURIComponent(p.name as string)}/source` },
    get_variable_records:   { path: (p) => `/api/variables/${encodeURIComponent(p.name as string)}/records` },
    get_constants:          { path: '/api/constants' },
    get_path_inputs:        { path: '/api/path-inputs' },
    put_layout:             { path: (p) => `/api/layout/${encodeURIComponent(p.node_id as string)}`, method: 'PUT', body: true },
    delete_layout:          { path: (p) => `/api/layout/${encodeURIComponent(p.node_id as string)}`, method: 'DELETE' },
    put_edge:               { path: (p) => `/api/edges/${encodeURIComponent(p.edge_id as string)}`, method: 'PUT', body: true },
    delete_edge:            { path: (p) => `/api/edges/${encodeURIComponent(p.edge_id as string)}`, method: 'DELETE' },
    put_pending_constant:   { path: (p) => `/api/constants/${encodeURIComponent(p.name as string)}/pending/${encodeURIComponent(p.value as string)}`, method: 'PUT' },
    delete_pending_constant:{ path: (p) => `/api/constants/${encodeURIComponent(p.name as string)}/pending/${encodeURIComponent(p.value as string)}`, method: 'DELETE' },
    create_constant:        { path: '/api/constants', method: 'POST', body: true },
    delete_constant:        { path: (p) => `/api/constants/${encodeURIComponent(p.name as string)}`, method: 'DELETE' },
    create_path_input:      { path: '/api/path-inputs', method: 'POST', body: true },
    update_path_input:      { path: (p) => `/api/path-inputs/${encodeURIComponent(p.name as string)}`, method: 'PUT', body: true },
    delete_path_input:      { path: (p) => `/api/path-inputs/${encodeURIComponent(p.name as string)}`, method: 'DELETE' },
    put_node_config:        { path: (p) => `/api/layout/${encodeURIComponent(p.node_id as string)}/config`, method: 'PUT', body: true },
    start_run:              { path: '/api/run', method: 'POST', body: true },
    refresh_module:         { path: '/api/refresh', method: 'POST' },
    create_variable:        { path: '/api/variables/create', method: 'POST', body: true },
  };

  const route = routes[method];
  if (!route) throw new Error(`Unknown method: ${method}`);

  const url = typeof route.path === 'function' ? route.path(params) : route.path;
  const httpMethod = route.method ?? 'GET';

  const fetchOpts: RequestInit = { method: httpMethod };
  if (route.body && httpMethod !== 'GET') {
    fetchOpts.headers = { 'Content-Type': 'application/json' };
    fetchOpts.body = JSON.stringify(params);
  }

  const res = await fetch(url, fetchOpts);
  return res.json();
}

// Type declaration for acquireVsCodeApi (provided by VS Code Webview runtime)
declare function acquireVsCodeApi(): {
  postMessage(msg: unknown): void;
  getState(): unknown;
  setState(state: unknown): void;
};
