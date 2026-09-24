/**
 * PlotPanel — the Plot Studio as its own VS Code editor tab.
 *
 * It started life as a modal overlay inside the DAG webview, which meant the
 * pipeline canvas was unreachable while a figure was open. As a separate
 * WebviewPanel it becomes a full-width sibling tab in the SAME editor group as
 * its pipeline canvas: switch with the tab bar, or split them yourself by
 * dragging a tab, exactly like any other pair of editors. (Opening at
 * `ViewColumn.Beside` instead would force a permanent 50/50 split, which is a
 * layout decision that belongs to the user, not to this panel.)
 *
 * **Every plot tab belongs to a session.** It issues `plot_*` RPCs to that
 * session's server and receives that session's `plot_save_*` notifications,
 * and nothing else: a save completing in one database must not re-enable the
 * Save button of a tab plotting another. The registry that used to hold every
 * plot tab in the window now hangs off `Session.plots` for exactly that
 * reason. The one exception is a CSV tab, whose "session" is the
 * database-less plot-only server (`SessionManager.openPlotOnly`).
 *
 * **Every plot opens its own tab.** There used to be one reused tab, and
 * plotting a second variable retargeted it — which quietly destroyed the
 * figure you were looking at, and made "compare these two" impossible
 * without saving one to disk first. Comparing figures is the normal reason
 * to open two, so a new tab is the normal outcome; closing one is a click.
 *
 * The webview loads the SAME React bundle as the DAG and is switched into plot
 * mode by an injected `window.__SCISTACK_VIEW__` (see frontend/src/main.tsx).
 * A second vite target would double the build for one component.
 */

import * as path from 'path';
import * as vscode from 'vscode';
import type { Session } from './session';
import { answerSetPlotTheme, plotThemeHost } from './plotTheme';

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
  /**
   * Which database to plot from, as a session id. The canvas stamps its own
   * (see `dagPanel.ts`), so a plot opened from one pipeline reads that
   * pipeline's database even when another tab was focused last.
   */
  sessionId?: string;
}

export class PlotPanel {
  private panel: vscode.WebviewPanel;
  private disposables: vscode.Disposable[] = [];
  private unregister: () => void = () => {};
  private unregisterTheme: () => void = () => {};

  static show(
    context: vscode.ExtensionContext,
    session: Session,
    target: PlotTarget,
    options: { column?: vscode.ViewColumn } = {},
  ): PlotPanel {
    return new PlotPanel(
      context,
      session,
      target,
      // This session's pipeline group, so the figure is a sibling tab of the
      // canvas it came from rather than a split the user did not ask for.
      options.column ?? session.dagPanel?.viewColumn ?? vscode.ViewColumn.One,
    );
  }

  private constructor(
    private context: vscode.ExtensionContext,
    /**
     * The database this figure is drawn from. Read through, never copied:
     * `Restart Python` replaces `session.python`, and a tab holding its own
     * reference would keep writing to the old process's destroyed stdin
     * (ERR_STREAM_DESTROYED, seen 2026-09-15 as "Could not open the plot
     * panel").
     */
    private session: Session,
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
    this.unregister = this.session.plots.add(this);
    // Every plot tab, whatever its database, follows the light/dark toggle.
    this.unregisterTheme = plotThemeHost(context.globalState).add(this);

    // Focusing a plot tab says which database later Palette commands mean.
    this.panel.onDidChangeViewState(
      (e) => {
        if (e.webviewPanel.active) this.session.manager.setActive(this.session.id);
      },
      undefined,
      this.disposables,
    );

    this.panel.webview.onDidReceiveMessage(
      async (msg: Record<string, unknown>) => {
        const method = msg.method as string;

        if (method === 'set_plot_theme') {
          // Host-side: the preference outlives this tab (see plotTheme.ts).
          await answerSetPlotTheme(
            plotThemeHost(this.context.globalState), msg, this, this.session.log,
          );
          return;
        }

        if (method === 'pick_save_path') {
          // Only the host can show a file dialog; a webview cannot save a file
          // at all, which is why plotly's own camera button fails here.
          try {
            const params = (msg.params ?? {}) as {
              defaultName?: string;
              formats?: string[];
              // "CSV" for "Save data"; images otherwise.
              filterName?: string;
            };
            const folder = vscode.workspace.workspaceFolders?.[0]?.uri;
            const uri = await vscode.window.showSaveDialog({
              defaultUri: folder
                ? vscode.Uri.joinPath(folder, params.defaultName ?? 'figure.png')
                : undefined,
              // The panel sends the ONE format its dropdown selected, so the
              // dialog cannot offer a second answer to a question already
              // asked — the backend honours the dropdown either way.
              filters: { [params.filterName ?? 'Images']: params.formats ?? ['png'] },
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
        // Everything else is a plot_* RPC for this session's Python process —
        // the same one its canvas uses, never a second database connection.
        try {
          const result = await this.session.python.request(
            method,
            (msg.params ?? {}) as Record<string, unknown>,
          );
          this.panel.webview.postMessage({ id: msg.id, result });
        } catch (err) {
          this.session.log.appendLine(`plot panel: ${method} failed — ${err}`);
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

  /** Post a message into this panel's webview (the `MessageSink` contract). */
  postMessage(msg: Record<string, unknown>): void {
    this.panel.webview.postMessage(msg);
  }

  /**
   * Close this tab. Called when its session closes: a plot tab cannot
   * outlive the server it sends every `plot_*` RPC to.
   */
  close(): void {
    this.panel.dispose();
  }

  /**
   * The tab title. It names the database as well as the variable: with plot
   * tabs open across two databases, "Plot — StepLength" twice over says
   * nothing about which is which.
   */
  private title(): string {
    if (this.target.csvPath) return `Plot — ${path.basename(this.target.csvPath)}`;
    const variable = this.target.variable ? `Plot — ${this.target.variable}` : 'Plot';
    return this.session.isPlotOnly ? variable : `${variable} · ${this.session.label}`;
  }

  private dispose(): void {
    this.unregister();
    this.unregisterTheme();
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
    // Which database this figure is drawn from, so the studio can say so —
    // a figure is only interpretable if you know its source.
    const session = JSON.stringify({
      id: this.session.id,
      dbName: this.session.isPlotOnly ? null : this.session.label,
      dbPath: this.session.dbPath || null,
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
  <script nonce="${nonce}">window.__SCISTACK_SESSION__ = ${session};</script>
  <script nonce="${nonce}">${plotThemeHost(this.context.globalState).initScript()}</script>
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
