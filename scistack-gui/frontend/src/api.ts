/**
 * Backend communication layer.
 *
 * In VS Code Webview mode: uses postMessage to the extension host.
 * In standalone mode (FastAPI): uses fetch() to localhost.
 *
 * Components import `callBackend` and don't need to know which mode is active.
 */

import { WebviewRpc } from './webviewRpc';
import { UndoStack, taskChange, type Change } from './undo';

// Detect VS Code Webview environment
const isVSCode = typeof acquireVsCodeApi === 'function';
const vscode = isVSCode ? acquireVsCodeApi() : null;

/** Webview requests awaiting the extension's reply. No timer: see webviewRpc.ts. */
const rpc = vscode ? new WebviewRpc((msg) => vscode.postMessage(msg)) : null;

// In VS Code mode, listen for messages from the extension host
if (isVSCode) {
  window.addEventListener('message', (event: MessageEvent) => {
    const msg = event.data;

    // Response to a request (has id)
    if (msg.id !== undefined && msg.id !== null) {
      rpc?.handleResponse(msg);
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

/**
 * The pipeline page's undo stack — the default surface for every undoable
 * request. A Plot Studio passes its own (`{ stack }`). docs/claude/undo-redo.md.
 */
export const pageUndo = new UndoStack((method, params) => callBackend(method, params), 'page');

export interface CallOptions {
  /** The surface whose undo stack records this request; default pageUndo,
   * null = record on the backend but push onto no stack. */
  stack?: UndoStack | null;
  /** Share one undo step across requests in different tasks (`newChange`). */
  change?: Change;
}

/**
 * `{method: label}` for every method the backend records for undo. The
 * backend's Handler table owns the list (`history_status`); this is a cached
 * read of it. A failed read is not cached, so the next request retries.
 */
let undoable: Promise<Record<string, string>> | null = null;
function undoableMethods(): Promise<Record<string, string>> {
  if (!undoable) {
    undoable = send('history_status', {})
      .then(raw => ((raw as { methods?: Record<string, string> })?.methods ?? {}))
      .catch(err => {
        console.warn('[undo] history_status failed; this request is not undoable', err);
        undoable = null;
        return {};
      });
  }
  return undoable;
}

function send(method: string, params: Record<string, unknown>): Promise<unknown> {
  return vscode ? callVSCode(method, params) : callFetch(method, params);
}

/** The backend's `{ok: false, error}` answer — a refusal changes nothing. */
function refused(result: unknown): boolean {
  return typeof result === 'object' && result !== null && (result as { ok?: unknown }).ok === false;
}

export async function callBackend(
  method: string,
  params: Record<string, unknown> = {},
  options: CallOptions = {},
): Promise<unknown> {
  // Taken SYNCHRONOUSLY, before any await: requests started in one task
  // share one undo step (undo.ts taskChange).
  const change = COALESCABLE_METHODS.has(method) || method.startsWith('history_')
    ? null
    : (options.change ?? taskChange());
  if (change) {
    const label = (await undoableMethods())[method];
    if (label !== undefined) {
      change.label ??= label;
      const result = await send(method, { ...params, _change: { id: change.id, label: change.label } });
      const stack = options.stack === undefined ? pageUndo : options.stack;
      if (stack && !refused(result)) stack.pushServer(change.id, change.label);
      return result;
    }
  }
  return callUnrecorded(method, params);
}

function callUnrecorded(method: string, params: Record<string, unknown>): Promise<unknown> {
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
  return rpc!.call(method, params);
}

/**
 * Standalone mode: map method names to REST endpoints.
 */
async function callFetch(method: string, rawParams: Record<string, unknown>): Promise<unknown> {
  const routes: Record<string, { path: string | ((p: Record<string, unknown>) => string); method?: string; body?: boolean }> = {
    get_pipeline:           { path: (p) => `/api/pipeline?pipeline_id=${encodeURIComponent((p.pipeline_id as string) ?? 'main')}${p.run_states === false ? '&run_states=false' : ''}` },
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
    get_variable_columns:   { path: (p) => `/api/variables/${encodeURIComponent((p.variable_type as string) ?? (p.name as string))}/columns` },
    get_variables_list:     { path: '/api/variables' },
    get_parameters:         { path: '/api/parameters' },
    get_path_inputs:        { path: '/api/path-inputs' },
    put_layout:             { path: (p) => `/api/layout/${encodeURIComponent(p.node_id as string)}`, method: 'PUT', body: true },
    delete_layout:          { path: (p) => `/api/layout/${encodeURIComponent(p.node_id as string)}`, method: 'DELETE' },
    put_edge:               { path: (p) => `/api/edges/${encodeURIComponent(p.edge_id as string)}`, method: 'PUT', body: true },
    delete_edge:            { path: (p) => `/api/edges/${encodeURIComponent(p.edge_id as string)}`, method: 'DELETE', body: true },
    unhide_edge:            { path: (p) => `/api/edges/${encodeURIComponent(p.edge_id as string)}/unhide`, method: 'POST', body: true },
    get_hidden_edges:       { path: (p) => `/api/edges/hidden${p.pipeline_id ? `?pipeline_id=${encodeURIComponent(p.pipeline_id as string)}` : ''}` },
    put_pending_constant:   { path: (p) => `/api/parameters/${encodeURIComponent(p.name as string)}/pending/${encodeURIComponent(p.value as string)}`, method: 'PUT' },
    delete_pending_constant:{ path: (p) => `/api/parameters/${encodeURIComponent(p.name as string)}/pending/${encodeURIComponent(p.value as string)}`, method: 'DELETE' },
    hide_combo:             { path: (p) => `/api/functions/${encodeURIComponent(p.function_name as string)}/hidden_combos`, method: 'POST', body: true },
    unhide_combo:           { path: (p) => `/api/functions/hidden_combos/${encodeURIComponent(p.node_id as string)}`, method: 'DELETE' },
    list_hidden_combos:     { path: (p) => `/api/functions/${encodeURIComponent(p.function_name as string)}/hidden_combos` },
    hide_parameter_value:   { path: (p) => `/api/parameters/${encodeURIComponent(p.name as string)}/hidden_values/${encodeURIComponent(p.value as string)}`, method: 'POST', body: true },
    unhide_parameter_value: { path: (p) => `/api/parameters/${encodeURIComponent(p.name as string)}/hidden_values/${encodeURIComponent(p.value as string)}`, method: 'DELETE', body: true },
    set_parameter_group_checked: { path: (p) => `/api/parameters/${encodeURIComponent(p.name as string)}/group_checked`, method: 'POST', body: true },
    list_hidden_parameter_values: { path: (p) => `/api/parameters/hidden_values?pipeline_id=${encodeURIComponent((p.pipeline_id as string) ?? 'main')}` },
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
    delete_parameter:       { path: (p) => `/api/parameters/${encodeURIComponent(p.name as string)}`, method: 'DELETE', body: true },
    refresh_parameter_source: { path: (p) => `/api/parameters/${encodeURIComponent(p.name as string)}/refresh-source`, method: 'POST' },
    create_path_input:      { path: '/api/path-inputs', method: 'POST', body: true },
    update_path_input:      { path: (p) => `/api/path-inputs/${encodeURIComponent(p.name as string)}`, method: 'PUT', body: true },
    rename_path_input:      { path: (p) => `/api/path-inputs/${encodeURIComponent(p.name as string)}/rename`, method: 'POST', body: true },
    delete_path_input:      { path: (p) => `/api/path-inputs/${encodeURIComponent(p.name as string)}`, method: 'DELETE', body: true },
    deep_copy_path_input:   { path: (p) => `/api/path-inputs/${encodeURIComponent(p.node_id as string)}/deep-copy`, method: 'POST' },
    get_entity_editability: { path: (p) => `/api/entities/${encodeURIComponent(p.kind as string)}/${encodeURIComponent(p.name as string)}/editability` },
    put_node_config:        { path: (p) => `/api/layout/${encodeURIComponent(p.node_id as string)}/config`, method: 'PUT', body: true },
    start_run:              { path: '/api/run', method: 'POST', body: true },
    get_schema_level:       { path: '/api/schema-level', method: 'POST', body: true },
    cancel_run:             { path: (p) => `/api/run/${encodeURIComponent(p.run_id as string)}/cancel`, method: 'POST' },
    force_cancel_run:       { path: (p) => `/api/run/${encodeURIComponent(p.run_id as string)}/force-cancel`, method: 'POST' },
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
    unhide_pipeline:        { path: (p) => `/api/pipelines/${encodeURIComponent(p.pipeline_id as string)}/unhide`, method: 'POST' },
    get_hidden_pipelines:   { path: '/api/pipelines/hidden' },
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
    // Libraries (.claude/plan-portability.md Stage 10a).
    list_libraries:          { path: '/api/libraries' },
    library_placement_check: { path: (p) => `/api/libraries/placement-check?pipeline_id=${encodeURIComponent(p.pipeline_id as string)}` },
    declare_library_requirements: { path: '/api/libraries/declare', method: 'POST', body: true },
    share_library_defaults:  { path: (p) => `/api/libraries/share-defaults?pipeline_id=${encodeURIComponent(p.pipeline_id as string)}` },
    share_as_library:        { path: '/api/libraries/share', method: 'POST', body: true },
    add_library:             { path: '/api/libraries', method: 'POST', body: true },
    remove_library:          { path: (p) => `/api/libraries?name=${encodeURIComponent(p.name as string)}`, method: 'DELETE' },
    sync_libraries:          { path: '/api/libraries/sync', method: 'POST' },
    // Whole-project bundles (.claude/plan-portability.md Stage 8).
    get_export_options:     { path: '/api/bundles/export-options' },
    export_project_bundle:  { path: '/api/bundles/export', method: 'POST', body: true },
    paste_nodes:            { path: (p) => `/api/pipelines/${encodeURIComponent(p.pipeline_id as string)}/paste-nodes`, method: 'POST', body: true },
    add_pipeline_use:       { path: (p) => `/api/pipelines/${encodeURIComponent(p.parent_pipeline_id as string)}/uses`, method: 'POST', body: true },
    remove_pipeline_use:    { path: (p) => `/api/pipeline-uses/${encodeURIComponent(p.use_id as string)}`, method: 'DELETE' },
    update_use_binding:     { path: (p) => `/api/pipeline-uses/${encodeURIComponent(p.use_id as string)}/binding`, method: 'PUT', body: true },
    get_pipeline_plan:      { path: (p) => `/api/pipelines/${encodeURIComponent(p.pipeline_id as string)}/plan?target=${encodeURIComponent((p.target as string) ?? '')}` },
    start_pipeline_run:     { path: (p) => `/api/pipelines/${encodeURIComponent(p.pipeline_id as string)}/run`, method: 'POST', body: true },
    // The canvas node's location tree.
    node_location_tree:     { path: '/api/provenance/node-location-tree', method: 'POST', body: true },
    // Variants popup (.claude/plan-variants-popup.md): cards, pins, deletion.
    variable_variants:      { path: '/api/variants/cards', method: 'POST', body: true },
    pin_variant:            { path: '/api/variants/pin', method: 'POST', body: true },
    release_pin:            { path: '/api/variants/release-pin', method: 'POST', body: true },
    pin_newest_variant:     { path: '/api/variants/pin-newest', method: 'POST', body: true },
    delete_variant_plan:    { path: '/api/variants/delete-plan', method: 'POST', body: true },
    delete_variant:         { path: '/api/variants/delete', method: 'POST', body: true },
    run_pin_conflicts:      { path: '/api/variants/run-pin-conflicts', method: 'POST', body: true },
    // Endpoint presentation (plot_/stat_ artifacts, report)
    get_endpoint_artifacts: { path: (p) => `/api/endpoints/${encodeURIComponent(p.fn_name as string)}/artifacts` },
    write_report:           { path: '/api/report', method: 'POST' },
    // Plot Studio (docs/claude/plotting-library-design.md). All POST + body:
    // a spec is a nested object, not something to squeeze into a query string.
    plot_describe:          { path: '/api/plot/describe', method: 'POST', body: true },
    plot_capabilities:      { path: '/api/plot/capabilities', method: 'POST', body: true },
    plot_resolve:           { path: '/api/plot/resolve', method: 'POST', body: true },
    plot_variant_graph:     { path: '/api/plot/variant-graph', method: 'POST', body: true },
    plot_grouping_graph:    { path: '/api/plot/grouping-graph', method: 'POST', body: true },
    plot_grouping_columns:  { path: '/api/plot/grouping-columns', method: 'POST', body: true },
    plot_grouping_default_variant: { path: '/api/plot/grouping-default-variant', method: 'POST', body: true },
    plot_location_tree:     { path: '/api/plot/locations', method: 'POST', body: true },
    plot_export:            { path: '/api/plot/export', method: 'POST', body: true },
    plot_add_to_pipeline:   { path: '/api/plot/add-to-pipeline', method: 'POST', body: true },
    // Named variant pins, saved as statements about the plotted variable.
    plot_variant_sets_save: { path: '/api/plot/variant-sets', method: 'POST', body: true },
    // Returns a job id immediately; progress arrives as notifications. One
    // figure or the whole fan-out — `figure_index` is the only difference, and
    // neither fits a request/response budget: one full-resolution figure is
    // minutes of work, against the 30 s timeout below.
    plot_save_start:        { path: '/api/plot/save', method: 'POST', body: true },
    plot_invalidate:        { path: '/api/plot/invalidate', method: 'POST' },
    // Saved plots: named, per-variable, kept in the project database
    // (.claude/plan-saved-plots.md). "hide" is Remove — nothing is deleted.
    plot_saved_list:        { path: '/api/plot/saved/list', method: 'POST', body: true },
    plot_saved_save:        { path: '/api/plot/saved/save', method: 'POST', body: true },
    plot_saved_open:        { path: '/api/plot/saved/open', method: 'POST', body: true },
    plot_saved_rename:      { path: '/api/plot/saved/rename', method: 'POST', body: true },
    plot_saved_hide:        { path: '/api/plot/saved/hide', method: 'POST', body: true },
    plot_saved_history:     { path: '/api/plot/saved/history', method: 'POST', body: true },
    // Plot presets: settings without the data, project-wide; applied to any
    // variable (.claude/plan-plot-presets.md). "hide" is Remove.
    plot_preset_list:       { path: '/api/plot/presets/list', method: 'POST', body: true },
    plot_preset_save:       { path: '/api/plot/presets/save', method: 'POST', body: true },
    plot_preset_apply:      { path: '/api/plot/presets/apply', method: 'POST', body: true },
    plot_preset_rename:     { path: '/api/plot/presets/rename', method: 'POST', body: true },
    plot_preset_hide:       { path: '/api/plot/presets/hide', method: 'POST', body: true },
    plot_preset_history:    { path: '/api/plot/presets/history', method: 'POST', body: true },
    // One edit to the project's [aliases] in scistack.toml (scidb.aliases).
    plot_project_alias_set: { path: '/api/plot/project-alias', method: 'POST', body: true },
    plot_project_color_set: { path: '/api/plot/project-color', method: 'POST', body: true },
    // An error boundary's report — see components/ClientErrorBoundary.tsx.
    report_client_error:    { path: '/api/client-error', method: 'POST', body: true },
    // Undo / redo (docs/claude/undo-redo.md). The change id is in the body;
    // a request's own undo metadata travels in the X-SciStack-Change header.
    history_status:         { path: '/api/history/status' },
    history_undo:           { path: '/api/history/undo', method: 'POST', body: true },
    history_redo:           { path: '/api/history/redo', method: 'POST', body: true },
  };

  const route = routes[method];
  if (!route) throw new Error(`Unknown method: ${method}`);

  // Undo metadata travels in a header on HTTP (the backend reads it in
  // api/handlers.py install_routes), so it never lands in a URL or a body
  // the route does not have.
  const { _change, ...params } = rawParams;
  const url = typeof route.path === 'function' ? route.path(params) : route.path;
  const httpMethod = route.method ?? 'GET';

  const headers: Record<string, string> = {};
  if (_change) headers['X-SciStack-Change'] = JSON.stringify(_change);
  const fetchOpts: RequestInit = { method: httpMethod, headers };
  if (route.body && httpMethod !== 'GET') {
    headers['Content-Type'] = 'application/json';
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
