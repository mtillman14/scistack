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
import { runInMatlabTerminal } from './matlabTerminal';

const DEBUG_SESSION_NAME = 'Attach to scistack-gui server';

export class DagPanel {
  private panel: vscode.WebviewPanel;
  private disposables: vscode.Disposable[] = [];
  private disposeCallbacks: (() => void)[] = [];
  private debugSession: vscode.DebugSession | undefined;

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
        // MATLAB function execution: generate command and copy to clipboard
        // instead of running in Python.
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
          if (language === 'matlab') {
            await this.handleMatlabRun(msg.id as number, params);
            return;
          }
          // Python function — auto-attach debugger so breakpoints get hit.
          await this.ensureDebugAttached();
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
    editor.revealRange(selection, vscode.TextEditorRevealKind.InCenter);
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
   * Handle "Run" for a MATLAB function: generate command, then either send
   * to the MathWorks MATLAB terminal or copy to clipboard.
   */
  private async handleMatlabRun(
    msgId: number,
    params: Record<string, unknown>,
  ): Promise<void> {
    const functionName = params.function_name as string | undefined;
    this.outputChannel.appendLine(
      `handleMatlabRun: requesting generate_matlab_command for ${functionName ?? '<?>'}`,
    );
    try {
      const result = await this.pythonProcess.request(
        'generate_matlab_command',
        params,
      ) as { command: string };
      const command = result.command;
      this.outputChannel.appendLine(
        `handleMatlabRun: got command (${command.length} chars)`,
      );

      // Try MathWorks terminal first, fall back to clipboard.
      const sent = await runInMatlabTerminal(command, this.outputChannel);
      if (sent) {
        this.outputChannel.appendLine(
          'handleMatlabRun: sent to MATLAB terminal',
        );
        vscode.window.showInformationMessage('Running in MATLAB terminal...');
      } else {
        await vscode.env.clipboard.writeText(command);
        this.outputChannel.appendLine(
          'handleMatlabRun: no MATLAB terminal found, copied to clipboard',
        );
        vscode.window.showInformationMessage(
          'MATLAB command copied to clipboard. Paste into MATLAB to run.'
        );
      }

      this.panel.webview.postMessage({ id: msgId, result: { ok: true } });

      // Immediately notify the frontend that the MATLAB run was dispatched.
      // We cannot track actual MATLAB execution, so treat dispatch-to-terminal
      // as "done" from the GUI's perspective. The DB file watcher will trigger
      // a dag_updated when MATLAB writes results to the database.
      const runId = params.run_id as string | undefined;
      if (runId) {
        this.panel.webview.postMessage({
          method: 'run_done',
          params: {
            run_id: runId,
            success: true,
            duration_ms: 0,
            cancelled: false,
          },
        });
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.outputChannel.appendLine(`handleMatlabRun: failed: ${msg}`);
      this.panel.webview.postMessage({
        id: msgId,
        error: { message: String(err) },
      });
      // Also reset the running state on error so the button doesn't stay stuck.
      const runId = params.run_id as string | undefined;
      if (runId) {
        this.panel.webview.postMessage({
          method: 'run_done',
          params: {
            run_id: runId,
            success: false,
            duration_ms: 0,
            cancelled: false,
          },
        });
      }
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
