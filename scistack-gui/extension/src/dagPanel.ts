/**
 * DagPanel — manages the Webview panel that hosts the React DAG UI.
 *
 * Responsibilities:
 *   - Creates a WebviewPanel with the React bundle loaded
 *   - Generates HTML with a Content Security Policy (CSP)
 *   - Forwards messages between the Webview ↔ Python process
 *   - Handles panel lifecycle (dispose, reveal)
 */

import * as vscode from 'vscode';
import * as path from 'path';
import { PythonProcess } from './pythonProcess';
import { runInMatlabTerminal, isMatlabExtensionAvailable, isMatlabTerminalOpen } from './matlabTerminal';
import { MatlabRunTracker } from './matlabRunTracker';
import { needsMatlabConnectionPrompt } from './matlabConnectionGate';

const DEBUG_SESSION_NAME = 'Attach to scistack-gui server';

export class DagPanel {
  private panel: vscode.WebviewPanel;
  private disposables: vscode.Disposable[] = [];
  private disposeCallbacks: (() => void)[] = [];
  private debugSession: vscode.DebugSession | undefined;
  /**
   * Which MATLAB runs currently own the DuckDB file lock. Shared with
   * `extension.ts`'s DB file-watcher, which must not refresh the DAG while
   * MATLAB has the database — see MatlabRunTracker.
   */
  readonly matlabRuns = new MatlabRunTracker();

  /**
   * Which editor group the pipeline canvas lives in. The Plot Studio opens in
   * the SAME group so it becomes a full-width sibling tab rather than a split;
   * reading it from the panel (instead of assuming ViewColumn.One) keeps that
   * true after the user drags the pipeline tab elsewhere.
   */
  get viewColumn(): vscode.ViewColumn | undefined {
    return this.panel.viewColumn;
  }

  constructor(
    private context: vscode.ExtensionContext,
    private pythonProcess: PythonProcess,
    private outputChannel: vscode.OutputChannel,
  ) {
    this.panel = vscode.window.createWebviewPanel(
      'scistack.dag',
      'SciStack Pipeline',
      vscode.ViewColumn.One,
      {
        enableScripts: true,
        retainContextWhenHidden: true,
        localResourceRoots: [
          vscode.Uri.file(path.join(context.extensionPath, 'dist', 'webview')),
        ],
      }
    );

    this.panel.webview.html = this.getHtml();

    // Forward messages from Webview → Python (or handle host-side methods).
    this.panel.webview.onDidReceiveMessage(
      async (msg: Record<string, unknown>) => {
        const method = msg.method as string;
        // Host-side methods don't go through Python — they drive the VS Code API.
        if (method === 'restart_python') {
          try {
            await vscode.commands.executeCommand('scistack.restartPython');
            this.panel.webview.postMessage({ id: msg.id, result: { ok: true } });
          } catch (err) {
            this.panel.webview.postMessage({
              id: msg.id,
              error: { message: String(err) },
            });
          }
          return;
        }
        if (method === 'open_plot_panel') {
          // The DAG's right-click ▸ Plot. Opening a tab is a host-side act;
          // the webview cannot create one, and the modal it used to show made
          // the canvas unreachable while a figure was open.
          try {
            await vscode.commands.executeCommand(
              'scistack.openPlotPanel',
              (msg.params ?? {}) as Record<string, unknown>,
            );
            this.panel.webview.postMessage({ id: msg.id, result: { ok: true } });
          } catch (err) {
            this.panel.webview.postMessage({
              id: msg.id,
              error: { message: String(err) },
            });
          }
          return;
        }
        if (method === 'pick_save_path') {
          // Only the host can show a file dialog; a webview cannot save a file
          // at all, which is why plotly's own camera button fails here.
          try {
            const params = (msg.params ?? {}) as {
              defaultName?: string;
              formats?: string[];
              // "CSV" for the Plot Studio's "Save data"; images otherwise.
              filterName?: string;
            };
            const folder = vscode.workspace.workspaceFolders?.[0]?.uri;
            const uri = await vscode.window.showSaveDialog({
              defaultUri: folder
                ? vscode.Uri.joinPath(folder, params.defaultName ?? 'figure.png')
                : undefined,
              filters: {
                [params.filterName ?? 'Images']: params.formats ?? ['png', 'svg', 'pdf'],
              },
            });
            this.panel.webview.postMessage({
              id: msg.id,
              result: { path: uri?.fsPath ?? null },
            });
          } catch (err) {
            this.panel.webview.postMessage({
              id: msg.id,
              error: { message: String(err) },
            });
          }
          return;
        }
        if (method === 'reveal_in_editor') {
          try {
            const params = (msg.params ?? {}) as { file?: string; line?: number };
            const result = await this.revealInEditor(params);
            this.panel.webview.postMessage({ id: msg.id, result });
          } catch (err) {
            this.panel.webview.postMessage({
              id: msg.id,
              error: { message: String(err) },
            });
          }
          return;
        }
        // Single-node runs. Whether this is a MATLAB run is decided by
        // PYTHON (api/run.route_matlab_single_run asks matlab_registry),
        // not by the webview's `language` field — that field used to be the
        // only signal, which is why a MATLAB node clicked in a browser (no
        // extension host to intercept it) fell through to the Python
        // registry and failed. Same call-then-inspect shape as
        // start_pipeline_run below.
        if (method === 'start_run') {
          const params = (msg.params ?? {}) as Record<string, unknown>;
          const language = params.language as string | undefined;
          const functionName = params.function_name as string | undefined;
          const variants = params.variants as unknown[] | undefined;
          this.outputChannel.appendLine(
            `start_run: function=${functionName ?? '<?>'} ` +
            `language=${language ?? 'python'} ` +
            `variants=${variants ? variants.length : 0}`,
          );
          // The debugger has to be attached BEFORE Python spawns the run
          // thread, so this one decision still uses the webview's hint —
          // it only costs a pointless attach if the hint is wrong.
          if (language !== 'matlab') {
            await this.ensureDebugAttached();
          }
          try {
            const result = (await this.pythonProcess.request(
              method,
              params,
            )) as { run_id: string; host_execution_required?: boolean; language?: string };
            this.panel.webview.postMessage({ id: msg.id, result });
            if (result.host_execution_required && result.language === 'matlab') {
              await this.handleMatlabRun(result.run_id, params);
            }
          } catch (err) {
            this.panel.webview.postMessage({
              id: msg.id,
              error: { message: String(err) },
            });
          }
          return;
        }
        // Whole-pipeline runs: unlike start_run, the frontend doesn't know
        // up front whether the pipeline scope contains MATLAB steps (it can
        // be a mix of function nodes). Call start_pipeline_run normally and
        // inspect the RESULT — Python's own handler
        // (execution_service.pipeline_has_matlab_steps) already detected
        // this and, instead of spawning its background Python-run thread,
        // returned host_execution_required=true so the SAME run_id can be
        // driven from here instead (mirrors handleMatlabRun's single-node
        // terminal dispatch).
        if (method === 'start_pipeline_run') {
          try {
            const result = (await this.pythonProcess.request(
              method,
              (msg.params ?? {}) as Record<string, unknown>,
            )) as { run_id: string; host_execution_required?: boolean; language?: string };
            this.panel.webview.postMessage({ id: msg.id, result });
            if (result.host_execution_required && result.language === 'matlab') {
              await this.handleMatlabPipelineRun(
                result.run_id,
                (msg.params ?? {}) as Record<string, unknown>,
              );
            }
          } catch (err) {
            this.panel.webview.postMessage({
              id: msg.id,
              error: { message: String(err) },
            });
          }
          return;
        }
        try {
          const result = await this.pythonProcess.request(
            method,
            (msg.params ?? {}) as Record<string, unknown>,
          );
          // Send response back to Webview with the matching id
          this.panel.webview.postMessage({
            id: msg.id,
            result,
          });
        } catch (err) {
          this.panel.webview.postMessage({
            id: msg.id,
            error: { message: String(err) },
          });
        }
      },
      undefined,
      this.disposables,
    );

    this.panel.onDidDispose(() => {
      this.disposables.forEach(d => d.dispose());
      for (const cb of this.disposeCallbacks) cb();
    }, null, this.disposables);
  }

  /**
   * Open a file in an editor column beside the DAG panel and reveal the given line.
   * `line` is 1-based (matching inspect.getsourcelines).
   *
   * UNC paths (`\\server\share\...`) are handled via explicit
   * `Uri.from({scheme:'file', authority, path})` construction because
   * `Uri.file()` has historically had edge cases with UNC canonicalization
   * on Windows. Errors are logged to the output channel before being
   * returned, so failures are visible even when the webview silently
   * swallows the error response.
   */
  private async revealInEditor(
    params: { file?: string; line?: number },
  ): Promise<{ ok: boolean; error?: string }> {
    const { file, line } = params;
    this.outputChannel.appendLine(`reveal_in_editor: file=${file} line=${line}`);
    if (!file) return { ok: false, error: 'No file path provided.' };

    const uri = this.buildFileUri(file);
    this.outputChannel.appendLine(`reveal_in_editor: resolved uri=${uri.toString()}`);

    let doc: vscode.TextDocument;
    try {
      doc = await vscode.workspace.openTextDocument(uri);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.outputChannel.appendLine(
        `reveal_in_editor: openTextDocument failed for ${uri.toString()}: ${msg}`,
      );
      return { ok: false, error: `openTextDocument failed: ${msg}` };
    }

    const zeroBased = Math.max(0, (line ?? 1) - 1);
    const selection = new vscode.Range(zeroBased, 0, zeroBased, 0);
    let editor: vscode.TextEditor;
    try {
      editor = await vscode.window.showTextDocument(doc, {
        viewColumn: vscode.ViewColumn.Beside,
        preserveFocus: false,
        selection,
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.outputChannel.appendLine(
        `reveal_in_editor: showTextDocument failed for ${uri.toString()}: ${msg}`,
      );
      return { ok: false, error: `showTextDocument failed: ${msg}` };
    }

    // Belt-and-suspenders: explicitly center the range in case the editor was
    // already open (selection in showTextDocument only applies on first open).
    editor.revealRange(selection, vscode.TextEditorRevealType.InCenter);
    return { ok: true };
  }

  /**
   * Build a file URI, handling Windows UNC paths (`\\server\share\path`)
   * explicitly. `vscode.Uri.file` accepts UNC but its canonicalization has
   * known edge cases; constructing via `Uri.from` with an explicit
   * authority removes that ambiguity.
   */
  private buildFileUri(file: string): vscode.Uri {
    if (file.startsWith('\\\\') || file.startsWith('//')) {
      // Strip the leading `\\` or `//`, split into authority + path.
      const rest = file.replace(/^[\\/]{2}/, '');
      const slashIdx = rest.search(/[\\/]/);
      if (slashIdx > 0) {
        const authority = rest.substring(0, slashIdx);
        // Normalize backslashes → forward slashes for the path portion and
        // prepend a leading slash as required by file URIs.
        const pathPart = '/' + rest.substring(slashIdx + 1).replace(/\\/g, '/');
        return vscode.Uri.from({ scheme: 'file', authority, path: pathPart });
      }
    }
    return vscode.Uri.file(file);
  }

  /**
   * Gate a MATLAB Run click on MATLAB actually being connected — see
   * matlabConnectionGate.ts. Returns true if the caller must stop and
   * report the click as connect-only (cancelled) instead of generating and
   * dispatching a command.
   *
   * Interim fix for problem P2 in plan-matlab-terminal-run-tracking.md:
   * this only catches the cold-start case (no MATLAB terminal yet). A real
   * run that fails after MATLAB is already connected still reports success
   * — that needs the deferred Stage 2 fix (MATLAB-written run markers).
   */
  private async gateOnMatlabConnection(): Promise<boolean> {
    if (!needsMatlabConnectionPrompt(isMatlabExtensionAvailable(), isMatlabTerminalOpen())) {
      return false;
    }
    const choice = await vscode.window.showInformationMessage(
      'MATLAB is not connected to VS Code yet. Connect now, then click Run again once MATLAB is ready.',
      'Connect',
      'Cancel',
    );
    if (choice === 'Connect') {
      await vscode.commands.executeCommand('matlab.openCommandWindow');
      this.outputChannel.appendLine(
        'gateOnMatlabConnection: opened the MATLAB command window — connecting, not dispatching a run',
      );
    } else {
      this.outputChannel.appendLine('gateOnMatlabConnection: user declined to connect');
    }
    return true;
  }

  /**
   * Stage 4 fallback ladder for an already-generated MATLAB command:
   * MathWorks terminal (Tier 2 — real breakpoint debugging) -> standalone
   * sidecar (Tier 3 — Python-driven, real run_output/run_done via the
   * notify channel; requires a run_id) -> clipboard (last resort).
   *
   * Returns which tier actually served the command. Callers need this:
   * the terminal and clipboard tiers aren't tracked by anything else, so
   * the caller must synthesize its own run_done; the sidecar tier is
   * driven by Python's start_matlab_sidecar_run
   * (_run_matlab_command_in_thread), which pushes a REAL run_done once
   * MATLAB actually finishes — synthesizing one here would show "done"
   * before it's actually done.
   */
  private async dispatchMatlabCommand(
    command: string,
    runId: string | undefined,
    warnings: string[] | undefined,
  ): Promise<'terminal' | 'sidecar' | 'clipboard'> {
    const sent = await runInMatlabTerminal(command, this.outputChannel);
    if (sent) {
      this.outputChannel.appendLine('dispatchMatlabCommand: sent to MATLAB terminal');
      vscode.window.showInformationMessage('Running in MATLAB terminal...');
      return 'terminal';
    }

    if (runId) {
      try {
        const sidecarResult = await this.pythonProcess.request(
          'start_matlab_sidecar_run',
          { command, run_id: runId, warnings: warnings ?? [] },
        ) as { run_id: string; sidecar_available: boolean };
        if (sidecarResult.sidecar_available) {
          this.outputChannel.appendLine(
            'dispatchMatlabCommand: dispatched via standalone MATLAB sidecar',
          );
          vscode.window.showInformationMessage(
            'Running via standalone MATLAB sidecar...',
          );
          return 'sidecar';
        }
        this.outputChannel.appendLine(
          'dispatchMatlabCommand: sidecar unavailable (matlab not on PATH)',
        );
      } catch (err) {
        this.outputChannel.appendLine(
          `dispatchMatlabCommand: sidecar dispatch failed: ${err}`,
        );
      }
    }

    await vscode.env.clipboard.writeText(command);
    this.outputChannel.appendLine(
      'dispatchMatlabCommand: no MATLAB terminal or sidecar available, copied to clipboard',
    );
    vscode.window.showInformationMessage(
      'MATLAB command copied to clipboard. Paste into MATLAB to run.'
    );
    return 'clipboard';
  }

  /**
   * Handle "Run" for a MATLAB function: generate the command, then run it
   * through the Stage 4 fallback ladder (terminal -> sidecar -> clipboard).
   *
   * The JSON-RPC response for `start_run` has already been sent by the
   * caller (Python answered it with `host_execution_required`), so this
   * only emits run_output/run_done on `runId` — exactly like
   * handleMatlabPipelineRun.
   */
  private async handleMatlabRun(
    runId: string,
    params: Record<string, unknown>,
  ): Promise<void> {
    const functionName = params.function_name as string | undefined;
    this.outputChannel.appendLine(
      `handleMatlabRun: requesting generate_matlab_command for ${functionName ?? '<?>'}`,
    );
    const finish = (success: boolean, cancelled = false) => {
      this.matlabRuns.end(runId);
      this.panel.webview.postMessage({
        method: 'run_done',
        params: { run_id: runId, success, duration_ms: 0, cancelled },
      });
    };

    if (await this.gateOnMatlabConnection()) {
      finish(false, true);
      return;
    }

    this.beginMatlabRun(runId);
    try {
      const result = await this.pythonProcess.request(
        'generate_matlab_command',
        params,
      ) as { command: string };
      const command = result.command;
      this.outputChannel.appendLine(
        `handleMatlabRun: got command (${command.length} chars)`,
      );

      const tier = await this.dispatchMatlabCommand(command, runId, undefined);

      // Terminal/clipboard dispatch aren't tracked by anything else — treat
      // "dispatched" as "done" from the GUI's perspective (the DB file
      // watcher triggers a dag_updated once MATLAB actually writes
      // results). The sidecar tier pushes its own real run_done — see
      // dispatchMatlabCommand's docstring.
      if (tier !== 'sidecar') {
        finish(true);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.outputChannel.appendLine(`handleMatlabRun: failed: ${msg}`);
      this.panel.webview.postMessage({
        method: 'run_output',
        params: { run_id: runId, text: `Error: ${msg}\n` },
      });
      // Reset the running state on error so the button doesn't stay stuck.
      finish(false);
    }
  }

  /**
   * Note that a MATLAB run owns the database from here until its run_done.
   *
   * MATLAB holds the DuckDB file lock for the whole run, and the GUI drops
   * its own lock between requests specifically so that can happen (see
   * scistack_gui/db.py). Refreshing the DAG during that window means RPCs
   * that can only fail, so `extension.ts`'s DB file-watcher consults
   * `matlabRuns` before broadcasting `dag_updated`.
   */
  private beginMatlabRun(runId: string): void {
    this.matlabRuns.begin(runId);
    this.outputChannel.appendLine(
      `MATLAB run ${runId} in flight — DAG refreshes deferred until it finishes`,
    );
  }

  /**
   * Handle host-side execution for a whole MATLAB-containing pipeline run.
   * Python's start_pipeline_run already detected the MATLAB step(s)
   * (execution_service.pipeline_has_matlab_steps) and, instead of spawning
   * its own background thread, returned host_execution_required=true —
   * this generates the whole-pipeline script and dispatches it exactly the
   * way handleMatlabRun does for a single node, tagging run_output/run_done
   * with the SAME run_id the frontend's PipelineRunController is already
   * listening on (so the existing run console just works).
   */
  private async handleMatlabPipelineRun(
    runId: string,
    params: Record<string, unknown>,
  ): Promise<void> {
    const pipelineId = params.pipeline_id as string | undefined;
    this.outputChannel.appendLine(
      `handleMatlabPipelineRun: requesting generate_matlab_pipeline_command for ` +
      `${pipelineId ?? '<?>'} (run_id=${runId})`,
    );

    const emit = (text: string) => {
      this.panel.webview.postMessage({
        method: 'run_output',
        params: { run_id: runId, text },
      });
    };
    const finish = (success: boolean, cancelled = false) => {
      this.matlabRuns.end(runId);
      this.panel.webview.postMessage({
        method: 'run_done',
        params: { run_id: runId, success, duration_ms: 0, cancelled },
      });
    };

    if (await this.gateOnMatlabConnection()) {
      emit('MATLAB is not connected yet — connect, then click Run again once it is ready.\n');
      finish(false, true);
      return;
    }

    this.beginMatlabRun(runId);
    try {
      const result = await this.pythonProcess.request(
        'generate_matlab_pipeline_command',
        params,
      ) as { command: string; warnings?: string[] };
      const command = result.command;
      this.outputChannel.appendLine(
        `handleMatlabPipelineRun: got command (${command.length} chars)`,
      );
      for (const w of result.warnings ?? []) {
        emit(`⚠ ${w}\n`);
      }

      const tier = await this.dispatchMatlabCommand(command, runId, result.warnings);

      if (tier !== 'sidecar') {
        // Terminal/clipboard dispatch aren't tracked by anything else —
        // treat "dispatched" as "done" from the GUI's perspective; the DB
        // file watcher triggers dag_updated once MATLAB writes results.
        // The sidecar tier pushes its own real run_output/run_done via the
        // notify channel — see dispatchMatlabCommand's docstring.
        if (tier === 'terminal') {
          emit('▶ Sent whole-pipeline script to MATLAB terminal...\n');
        } else {
          emit('MATLAB pipeline script copied to clipboard. Paste into MATLAB to run.\n');
        }
        finish(true);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.outputChannel.appendLine(`handleMatlabPipelineRun: failed: ${msg}`);
      emit(`Error: ${msg}\n`);
      finish(false);
    }
  }

  /**
   * Update the PythonProcess reference after a restart, so requests from the
   * webview are routed to the new process instead of the killed one.
   */
  updatePythonProcess(proc: PythonProcess): void {
    this.pythonProcess = proc;
  }

  /**
   * Post a notification message to the Webview (from Python push notifications).
   */
  postMessage(msg: Record<string, unknown>): void {
    this.panel.webview.postMessage(msg);
  }

  /**
   * Ensure a debugpy attach session is active before a Run begins, so
   * breakpoints inside user functions get hit. No-op if scistack.debug is
   * disabled or a session is already attached.
   */
  async ensureDebugAttached(): Promise<void> {
    const cfg = vscode.workspace.getConfiguration('scistack');
    if (!cfg.get<boolean>('debug', false)) return;
    if (this.debugSession) return;

    // Also check VS Code's own list in case the user started the session
    // manually (e.g. via F5) — avoid creating a duplicate.
    const existing = this.findExistingDebugSession();
    if (existing) {
      this.debugSession = existing;
      return;
    }

    const port = cfg.get<number>('debugPort', 5678);
    const folder = vscode.workspace.workspaceFolders?.[0];
    const started = await vscode.debug.startDebugging(folder, {
      name: DEBUG_SESSION_NAME,
      type: 'debugpy',
      request: 'attach',
      connect: { host: '127.0.0.1', port },
      justMyCode: false,
    });
    if (!started) {
      this.outputChannel.appendLine(
        'Warning: failed to start debugpy attach session. ' +
        'Is the server running with scistack.debug enabled?'
      );
      return;
    }
    // startDebugging resolves true but doesn't return the session; capture it.
    this.debugSession =
      vscode.debug.activeDebugSession ?? this.findExistingDebugSession();
  }

  /**
   * Detach the debug session (called when run_done arrives).
   */
  async stopDebugSession(): Promise<void> {
    const session = this.debugSession ?? this.findExistingDebugSession();
    this.debugSession = undefined;
    if (session) {
      await vscode.debug.stopDebugging(session);
    }
  }

  private findExistingDebugSession(): vscode.DebugSession | undefined {
    const active = vscode.debug.activeDebugSession;
    if (active && active.name === DEBUG_SESSION_NAME) return active;
    return undefined;
  }

  /**
   * Reveal the panel if it's hidden.
   */
  reveal(): void {
    this.panel.reveal(vscode.ViewColumn.One);
  }

  /**
   * Register a callback for when the panel is disposed.
   */
  onDidDispose(callback: () => void): void {
    this.disposeCallbacks.push(callback);
  }

  private getHtml(): string {
    const webviewDir = path.join(this.context.extensionPath, 'dist', 'webview');
    const webview = this.panel.webview;

    // Resolve the built JS and CSS assets
    const scriptUri = webview.asWebviewUri(
      vscode.Uri.file(path.join(webviewDir, 'index.js'))
    );
    const styleUri = webview.asWebviewUri(
      vscode.Uri.file(path.join(webviewDir, 'index.css'))
    );

    // CSP nonce for inline scripts
    const nonce = getNonce();

    return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <meta http-equiv="Content-Security-Policy"
        content="default-src 'none';
                 style-src ${webview.cspSource} 'unsafe-inline';
                 script-src 'nonce-${nonce}';
                 img-src ${webview.cspSource} data:;
                 font-src ${webview.cspSource};" />
  <link rel="stylesheet" href="${styleUri}" />
  <title>SciStack Pipeline</title>
  <style>
    html, body, #root {
      margin: 0;
      padding: 0;
      width: 100%;
      height: 100%;
      overflow: hidden;
    }
  </style>
</head>
<body>
  <div id="root"></div>
  <script nonce="${nonce}" src="${scriptUri}"></script>
</body>
</html>`;
  }
}

function getNonce(): string {
  let text = '';
  const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  for (let i = 0; i < 32; i++) {
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  }
  return text;
}
