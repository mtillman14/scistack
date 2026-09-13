/**
 * PlotPanel — the Plot Studio as its own VS Code editor tab.
 *
 * It started life as a modal overlay inside the DAG webview, which meant the
 * pipeline canvas was unreachable while a figure was open. As a separate
 * WebviewPanel it becomes a full-width sibling tab in the SAME editor group as
 * "SciStack Pipeline" — switch with the tab bar, or split them yourself by
 * dragging a tab, exactly like any other pair of editors. (Opening at
 * `ViewColumn.Beside` instead would force a permanent 50/50 split, which is a
 * layout decision that belongs to the user, not to this panel.)
 *
 * One tab is reused: plotting a second variable retargets the existing panel
 * (revealing it) rather than accumulating tabs. Pass `newTab` to override.
 *
 * The webview loads the SAME React bundle as the DAG and is switched into plot
 * mode by an injected `window.__SCISTACK_VIEW__` (see frontend/src/main.tsx).
 * A second vite target would double the build for one component.
 */

import * as path from 'path';
import * as vscode from 'vscode';
import { PanelRegistry } from './panelRegistry';
import { PythonProcess } from './pythonProcess';

export interface PlotTarget {
  variable?: string;
  /** Set to plot a CSV file instead of the project database. */
  csvPath?: string;
  /**
   * One schema location to open on, as `[key, value]` steps — what the canvas's
   * location picker hands over when a row is clicked. Carried as opaque data:
   * the extension never interprets it, it only has to survive the trip to the
   * webview so "click a location, see that location" holds from the canvas as
   * well as from inside the panel.
   */
  location?: [string, string][];
}

export class PlotPanel {
  /** The reused panel, if one is open. */
  private static current: PlotPanel | undefined;

  /**
   * Every open plot tab, including the `newTab` ones `current` does not track.
   * Push notifications go here — see `broadcast`.
   */
  private static openPanels = new PanelRegistry();

  private panel: vscode.WebviewPanel;
  private disposables: vscode.Disposable[] = [];
  private unregister: () => void = () => {};

  /**
   * Deliver a push notification from Python to every open plot tab.
   *
   * The Plot Studio is a sibling of the DAG webview, not a child of it, so the
   * extension's `onNotification` forwarding reaches it only through here. The
   * long-running save is a background job that reports `plot_save_progress` /
   * `plot_save_complete` / `plot_save_failed`, and the panel disables its save
   * buttons until one of the last two arrives — a notification that stops at
   * the DAG panel leaves the tab saying "Saving…" for the rest of the session.
   *
   * Returns the number of panels that received it, for the caller's log.
   */
  static broadcast(msg: Record<string, unknown>): number {
    return PlotPanel.openPanels.send(msg);
  }

  static show(
    context: vscode.ExtensionContext,
    pythonProcess: PythonProcess,
    outputChannel: vscode.OutputChannel,
    target: PlotTarget,
    options: { newTab?: boolean; column?: vscode.ViewColumn } = {},
  ): PlotPanel {
    if (!options.newTab && PlotPanel.current) {
      PlotPanel.current.retarget(target);
      return PlotPanel.current;
    }
    const panel = new PlotPanel(
      context,
      pythonProcess,
      outputChannel,
      target,
      options.column ?? vscode.ViewColumn.One,
    );
    if (!options.newTab) PlotPanel.current = panel;
    return panel;
  }

  private constructor(
    private context: vscode.ExtensionContext,
    private pythonProcess: PythonProcess,
    private outputChannel: vscode.OutputChannel,
    private target: PlotTarget,
    column: vscode.ViewColumn,
  ) {
    this.panel = vscode.window.createWebviewPanel(
      'scistack.plot',
      this.title(),
      // The pipeline's own group: a sibling tab at full width, not a split.
      { viewColumn: column, preserveFocus: false },
      {
        enableScripts: true,
        // Plot state (spec, role assignments) is expensive to rebuild and has
        // no persistence of its own, so keep the webview alive when the tab is
        // in the background.
        retainContextWhenHidden: true,
        localResourceRoots: [
          vscode.Uri.file(path.join(context.extensionPath, 'dist', 'webview')),
        ],
      },
    );

    this.panel.webview.html = this.getHtml();
    this.unregister = PlotPanel.openPanels.add(this);

    this.panel.webview.onDidReceiveMessage(
      async (msg: Record<string, unknown>) => {
        const method = msg.method as string;

        if (method === 'pick_save_path') {
          // Only the host can show a file dialog; a webview cannot save a file
          // at all, which is why plotly's own camera button fails here.
          try {
            const params = (msg.params ?? {}) as {
              defaultName?: string;
              formats?: string[];
            };
            const folder = vscode.workspace.workspaceFolders?.[0]?.uri;
            const uri = await vscode.window.showSaveDialog({
              defaultUri: folder
                ? vscode.Uri.joinPath(folder, params.defaultName ?? 'figure.png')
                : undefined,
              // The panel sends the ONE format its dropdown selected, so the
              // dialog cannot offer a second answer to a question already
              // asked — the backend honours the dropdown either way.
              filters: { Images: params.formats ?? ['png'] },
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

        if (method === 'pick_save_folder') {
          // Saving a fan-out writes N files whose names are the figure labels,
          // so the only thing left to choose is WHERE — a folder, not a name.
          try {
            const uris = await vscode.window.showOpenDialog({
              canSelectFiles: false,
              canSelectFolders: true,
              canSelectMany: false,
              defaultUri: vscode.workspace.workspaceFolders?.[0]?.uri,
              openLabel: 'Save figures here',
            });
            this.panel.webview.postMessage({
              id: msg.id,
              result: { path: uris?.[0]?.fsPath ?? null },
            });
          } catch (err) {
            this.panel.webview.postMessage({
              id: msg.id,
              error: { message: String(err) },
            });
          }
          return;
        }
        // Everything else is a plot_* RPC for the shared Python process — the
        // same one the DAG uses, never a second database connection.
        try {
          const result = await this.pythonProcess.request(
            method,
            (msg.params ?? {}) as Record<string, unknown>,
          );
          this.panel.webview.postMessage({ id: msg.id, result });
        } catch (err) {
          this.outputChannel.appendLine(`plot panel: ${method} failed — ${err}`);
          this.panel.webview.postMessage({
            id: msg.id,
            error: { message: String(err) },
          });
        }
      },
      undefined,
      this.disposables,
    );

    this.panel.onDidDispose(() => this.dispose(), undefined, this.disposables);
  }

  /** Point the open panel at a different variable or file. */
  retarget(target: PlotTarget): void {
    this.target = target;
    this.panel.title = this.title();
    this.panel.reveal(this.panel.viewColumn, false);
    this.postMessage({
      method: 'open_plot_studio',
      params: {
        variable: target.variable,
        csv_path: target.csvPath,
        location: target.location,
      },
    });
  }

  /** Post a message into this panel's webview (the `MessageSink` contract). */
  postMessage(msg: Record<string, unknown>): void {
    this.panel.webview.postMessage(msg);
  }

  private title(): string {
    if (this.target.csvPath) return `Plot — ${path.basename(this.target.csvPath)}`;
    return this.target.variable ? `Plot — ${this.target.variable}` : 'Plot';
  }

  private dispose(): void {
    if (PlotPanel.current === this) PlotPanel.current = undefined;
    this.unregister();
    while (this.disposables.length) this.disposables.pop()?.dispose();
  }

  private getHtml(): string {
    const webviewDir = path.join(this.context.extensionPath, 'dist', 'webview');
    const webview = this.panel.webview;
    const scriptUri = webview.asWebviewUri(
      vscode.Uri.file(path.join(webviewDir, 'index.js')),
    );
    const styleUri = webview.asWebviewUri(
      vscode.Uri.file(path.join(webviewDir, 'index.css')),
    );
    const nonce = getNonce();
    // JSON.stringify, not interpolation: a variable name or file path must not
    // be able to break out of the script tag.
    const target = JSON.stringify({
      view: 'plot',
      variable: this.target.variable ?? null,
      csvPath: this.target.csvPath ?? null,
      location: this.target.location ?? null,
    });

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
  <title>${this.title()}</title>
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
  <script nonce="${nonce}">window.__SCISTACK_VIEW__ = ${target};</script>
  <script nonce="${nonce}" src="${scriptUri}"></script>
</body>
</html>`;
  }
}

function getNonce(): string {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let text = '';
  for (let i = 0; i < 32; i++) {
    text += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return text;
}
