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
/**
 * Read-only methods whose concurrent duplicates may share one request.
 *
 * Independent components legitimately need the same data — EditTab wants the
 * hypothesis *ids* while HypothesisTabs wants the full records, so both call
 * `list_hypotheses` on mount and the backend serves it twice (four times, for
 * the endpoints two components each fetch). Coalescing collapses that without
 * forcing the components into a shared provider.
 *
 * Deliberately an explicit list rather than a `get_`/`list_` prefix rule: a
 * mutation mis-classified as coalescable would be silently dropped when it
 * happened to overlap an identical in-flight call. Adding a read endpoint here
 * is a cheap, reversible win; adding a mutation is a correctness bug.
 */
const COALESCABLE_METHODS = new Set([
  'get_info',
  'get_schema',
  'get_registry',
  'get_notes',
  'get_pipeline',
  'get_layout',
  'get_parameters',
  'get_path_inputs',
  'get_hidden_ports',
  'get_hidden_edges',
  'get_hidden_pipelines',
  'get_variables_list',
  'list_glue',
  'list_pipelines',
  'list_hypotheses',
]);

/** In-flight coalescable requests, keyed by method + params. */
const inFlight = new Map<string, Promise<unknown>>();

export async function callBackend(method: string, params: Record<string, unknown> = {}): Promise<unknown> {
  const send = () => (vscode ? callVSCode(method, params) : callFetch(method, params));

  if (!COALESCABLE_METHODS.has(method)) return send();

  // Same method AND same params — a different pipeline_id is a different
  // request and must not be served another scope's response.
  const key = `${method}:${JSON.stringify(params)}`;
  const existing = inFlight.get(key);
  if (existing) return existing;

  // Cleared on settle, so this only ever merges genuinely concurrent callers
  // and never caches a stale result for a later fetch.
  const request = send().finally(() => { inFlight.delete(key); });
  inFlight.set(key, request);
  return request;
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
    get_pipeline:           { path: (p) => `/api/pipeline?pipeline_id=${encodeURIComponent((p.pipeline_id as string) ?? 'main')}` },
    get_layout:             { path: (p) => `/api/layout?pipeline_id=${encodeURIComponent((p.pipeline_id as string) ?? 'main')}` },
    get_schema:             { path: '/api/schema' },
    get_info:               { path: '/api/info' },
    create_project:         { path: '/api/bootstrap/create', method: 'POST', body: true },
    open_project:           { path: '/api/bootstrap/open', method: 'POST', body: true },
    get_registry:           { path: '/api/registry' },
    get_function_params:    { path: (p) => `/api/function/${encodeURIComponent(p.name as string)}/params` },
    get_function_source:    { path: (p) => `/api/function/${encodeURIComponent(p.name as string)}/source` },
    get_function_doc:       { path: (p) => `/api/function/${encodeURIComponent(p.name as string)}/doc` },
    get_notes:              { path: '/api/notes' },
    set_note:               { path: (p) => `/api/notes/${encodeURIComponent(p.key as string)}`, method: 'PUT', body: true },
    get_variable_records:   { path: (p) => `/api/variables/${encodeURIComponent(p.name as string)}/records` },
    get_variable_plot_data: { path: (p) => `/api/variables/${encodeURIComponent(p.name as string)}/plot-data` },
    get_variables_list:     { path: '/api/variables' },
    get_parameters:         { path: '/api/parameters' },
    get_path_inputs:        { path: '/api/path-inputs' },
    put_layout:             { path: (p) => `/api/layout/${encodeURIComponent(p.node_id as string)}`, method: 'PUT', body: true },
    delete_layout:          { path: (p) => `/api/layout/${encodeURIComponent(p.node_id as string)}`, method: 'DELETE' },
    put_edge:               { path: (p) => `/api/edges/${encodeURIComponent(p.edge_id as string)}`, method: 'PUT', body: true },
    delete_edge:            { path: (p) => `/api/edges/${encodeURIComponent(p.edge_id as string)}`, method: 'DELETE', body: true },
    unhide_edge:            { path: (p) => `/api/edges/${encodeURIComponent(p.edge_id as string)}/unhide`, method: 'POST', body: true },
    get_hidden_edges:       { path: (p) => `/api/edges/hidden${p.pipeline_id ? `?pipeline_id=${encodeURIComponent(p.pipeline_id as string)}` : ''}` },
    put_pending_parameter:  { path: (p) => `/api/parameters/${encodeURIComponent(p.name as string)}/pending/${encodeURIComponent(p.value as string)}`, method: 'PUT' },
    delete_pending_parameter:{ path: (p) => `/api/parameters/${encodeURIComponent(p.name as string)}/pending/${encodeURIComponent(p.value as string)}`, method: 'DELETE' },
    hide_combo:             { path: (p) => `/api/functions/${encodeURIComponent(p.function_name as string)}/hidden_combos`, method: 'POST', body: true },
    unhide_combo:           { path: (p) => `/api/functions/hidden_combos/${encodeURIComponent(p.node_id as string)}`, method: 'DELETE' },
    list_hidden_combos:     { path: (p) => `/api/functions/${encodeURIComponent(p.function_name as string)}/hidden_combos` },
    hide_parameter_value:   { path: (p) => `/api/parameters/${encodeURIComponent(p.name as string)}/hidden_values/${encodeURIComponent(p.value as string)}`, method: 'POST' },
    unhide_parameter_value: { path: (p) => `/api/parameters/${encodeURIComponent(p.name as string)}/hidden_values/${encodeURIComponent(p.value as string)}`, method: 'DELETE' },
    set_parameter_group_checked: { path: (p) => `/api/parameters/${encodeURIComponent(p.name as string)}/group_checked`, method: 'POST', body: true },
    list_hidden_parameter_values: { path: '/api/parameters/hidden_values' },
    create_parameter:       { path: '/api/parameters', method: 'POST', body: true },
    // Glue nodes (docs/claude/free-code-glue-nodes.md). There is deliberately
    // NO run endpoint: a glue node executes only as part of the run of
    // whichever function consumes it.
    list_glue:              { path: '/api/glue' },
    get_glue:               { path: (p) => `/api/glue/${encodeURIComponent(p.name as string)}` },
    get_glue_columns:       { path: (p) => `/api/glue/${encodeURIComponent(p.name as string)}/columns?variable_type=${encodeURIComponent((p.variable_type as string) ?? '')}` },
    create_glue:            { path: '/api/glue', method: 'POST', body: true },
    save_glue:              { path: '/api/glue', method: 'PUT', body: true },
    delete_glue:            { path: (p) => `/api/glue/${encodeURIComponent(p.name as string)}`, method: 'DELETE' },
    // Entity edits write straight to source (docs/claude/entity-editability-model.md).
    // Params match the JSON-RPC handlers field-for-field so callBackend behaves
    // identically over both transports — a mismatch here is invisible in the
    // browser and only breaks the VS Code extension.
    update_parameter:       { path: (p) => `/api/parameters/${encodeURIComponent(p.name as string)}`, method: 'PUT', body: true },
    delete_parameter:       { path: (p) => `/api/parameters/${encodeURIComponent(p.name as string)}`, method: 'DELETE' },
    refresh_parameter_source: { path: (p) => `/api/parameters/${encodeURIComponent(p.name as string)}/refresh-source`, method: 'POST' },
    create_path_input:      { path: '/api/path-inputs', method: 'POST', body: true },
    update_path_input:      { path: (p) => `/api/path-inputs/${encodeURIComponent(p.name as string)}`, method: 'PUT', body: true },
    delete_path_input:      { path: (p) => `/api/path-inputs/${encodeURIComponent(p.name as string)}`, method: 'DELETE' },
    deep_copy_path_input:   { path: (p) => `/api/path-inputs/${encodeURIComponent(p.node_id as string)}/deep-copy`, method: 'POST' },
    put_node_config:        { path: (p) => `/api/layout/${encodeURIComponent(p.node_id as string)}/config`, method: 'PUT', body: true },
    start_run:              { path: '/api/run', method: 'POST', body: true },
    get_matlab_engine_status: { path: '/api/matlab-engine' },
    restart_matlab_engine:  { path: '/api/matlab-engine/restart', method: 'POST' },
    refresh_module:         { path: '/api/refresh', method: 'POST' },
    create_variable:        { path: '/api/variables/create', method: 'POST', body: true },
    create_builtin_function: { path: '/api/functions/builtin', method: 'POST', body: true },
    get_project_code:       { path: '/api/project/code' },
    refresh_project:        { path: '/api/project/refresh', method: 'POST' },
    get_project_paths:      { path: '/api/project/paths' },
    add_project_path:       { path: '/api/project/paths', method: 'POST', body: true },
    remove_project_path:    { path: (p) => `/api/project/paths?path=${encodeURIComponent(p.path as string)}`, method: 'DELETE' },
    set_entities_file:      { path: '/api/project/entities-file', method: 'POST', body: true },
    clear_entities_file:    { path: '/api/project/entities-file', method: 'DELETE' },
    // Nested-pipeline scopes (method names match the JSON-RPC handlers in server.py)
    list_pipelines:         { path: '/api/pipelines' },
    create_pipeline:        { path: '/api/pipelines', method: 'POST', body: true },
    rename_pipeline:        { path: (p) => `/api/pipelines/${encodeURIComponent(p.pipeline_id as string)}`, method: 'PUT', body: true },
    delete_pipeline:        { path: (p) => `/api/pipelines/${encodeURIComponent(p.pipeline_id as string)}`, method: 'DELETE' },
    list_hypotheses:        { path: '/api/hypotheses' },
    create_hypothesis:      { path: '/api/hypotheses', method: 'POST', body: true },
    update_hypothesis:      { path: (p) => `/api/hypotheses/${encodeURIComponent(p.pipeline_id as string)}`, method: 'PUT', body: true },
    delete_hypothesis:      { path: (p) => `/api/hypotheses/${encodeURIComponent(p.pipeline_id as string)}`, method: 'DELETE' },
    get_pipeline_interface: { path: (p) => `/api/pipelines/${encodeURIComponent(p.pipeline_id as string)}/interface` },
    get_hidden_ports:       { path: (p) => `/api/pipelines/${encodeURIComponent(p.pipeline_id as string)}/hidden-ports` },
    hide_port:              { path: (p) => `/api/pipelines/${encodeURIComponent(p.pipeline_id as string)}/hide-port`, method: 'POST', body: true },
    unhide_port:            { path: (p) => `/api/pipelines/${encodeURIComponent(p.pipeline_id as string)}/unhide-port`, method: 'POST', body: true },
    extract_to_submodule:   { path: (p) => `/api/pipelines/${encodeURIComponent(p.pipeline_id as string)}/extract`, method: 'POST', body: true },
    duplicate_pipeline:     { path: (p) => `/api/pipelines/${encodeURIComponent(p.pipeline_id as string)}/duplicate`, method: 'POST', body: true },
    duplicate_hypothesis:   { path: (p) => `/api/hypotheses/${encodeURIComponent(p.pipeline_id as string)}/duplicate`, method: 'POST', body: true },
    export_pipeline:        { path: (p) => `/api/pipelines/${encodeURIComponent(p.pipeline_id as string)}/export` },
    import_pipeline:        { path: '/api/pipelines/import', method: 'POST', body: true },
    export_pipeline_code:   { path: (p) => `/api/pipelines/${encodeURIComponent(p.pipeline_id as string)}/export-code` },
    paste_nodes:            { path: (p) => `/api/pipelines/${encodeURIComponent(p.pipeline_id as string)}/paste-nodes`, method: 'POST', body: true },
    add_pipeline_use:       { path: (p) => `/api/pipelines/${encodeURIComponent(p.parent_pipeline_id as string)}/uses`, method: 'POST', body: true },
    remove_pipeline_use:    { path: (p) => `/api/pipeline-uses/${encodeURIComponent(p.use_id as string)}`, method: 'DELETE' },
    update_use_binding:     { path: (p) => `/api/pipeline-uses/${encodeURIComponent(p.use_id as string)}/binding`, method: 'PUT', body: true },
    get_pipeline_plan:      { path: (p) => `/api/pipelines/${encodeURIComponent(p.pipeline_id as string)}/plan?target=${encodeURIComponent((p.target as string) ?? '')}` },
    start_pipeline_run:     { path: (p) => `/api/pipelines/${encodeURIComponent(p.pipeline_id as string)}/run`, method: 'POST', body: true },
    // Endpoint presentation (plot_/stat_ artifacts, report)
    get_endpoint_artifacts: { path: (p) => `/api/endpoints/${encodeURIComponent(p.fn_name as string)}/artifacts` },
    write_report:           { path: '/api/report', method: 'POST' },
    // Plot Studio (docs/claude/plotting-library-design.md). All POST + body:
    // a spec is a nested object, not something to squeeze into a query string.
    plot_describe:          { path: '/api/plot/describe', method: 'POST', body: true },
    plot_capabilities:      { path: '/api/plot/capabilities', method: 'POST', body: true },
    plot_resolve:           { path: '/api/plot/resolve', method: 'POST', body: true },
    plot_variant_graph:     { path: '/api/plot/variant-graph', method: 'POST', body: true },
    plot_location_tree:     { path: '/api/plot/locations', method: 'POST', body: true },
    plot_export:            { path: '/api/plot/export', method: 'POST', body: true },
    plot_add_to_pipeline:   { path: '/api/plot/add-to-pipeline', method: 'POST', body: true },
    // Returns a job id immediately; progress arrives as notifications. One
    // figure or the whole fan-out — `figure_index` is the only difference, and
    // neither fits a request/response budget: one full-resolution figure is
    // minutes of work, against the 30 s timeout below.
    plot_save_start:        { path: '/api/plot/save', method: 'POST', body: true },
    plot_invalidate:        { path: '/api/plot/invalidate', method: 'POST' },
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
  if (!res.ok) {
    // FastAPI validation guards return {"detail": <message>}; surface the
    // backend's message verbatim (parity with the JSON-RPC error path).
    let detail = `${res.status} ${res.statusText}`;
    try {
      const err = await res.json();
      if (err && typeof err.detail === 'string') detail = err.detail;
    } catch { /* non-JSON error body */ }
    throw new Error(detail);
  }
  return res.json();
}

// Type declaration for acquireVsCodeApi (provided by VS Code Webview runtime)
declare function acquireVsCodeApi(): {
  postMessage(msg: unknown): void;
  getState(): unknown;
  setState(state: unknown): void;
};
