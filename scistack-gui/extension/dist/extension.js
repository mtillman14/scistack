"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// src/extension.ts
var extension_exports = {};
__export(extension_exports, {
  activate: () => activate,
  deactivate: () => deactivate
});
module.exports = __toCommonJS(extension_exports);
var path6 = __toESM(require("path"));
var vscode6 = __toESM(require("vscode"));

// src/plotPanel.ts
var path = __toESM(require("path"));
var vscode = __toESM(require("vscode"));

// src/panelRegistry.ts
var PanelRegistry = class {
  constructor() {
    this.panels = /* @__PURE__ */ new Set();
  }
  /** Number of panels currently registered. */
  get size() {
    return this.panels.size;
  }
  /**
   * The registered panels, for a caller that must act on each one (closing
   * a session's plot tabs). A snapshot, because disposing a panel
   * unregisters it and would otherwise mutate the set mid-iteration.
   */
  sinks() {
    return [...this.panels];
  }
  /**
   * Register a panel. Returns the function that removes it again — call it
   * from the panel's dispose, or a closed tab keeps receiving messages.
   */
  add(sink) {
    this.panels.add(sink);
    return () => {
      this.panels.delete(sink);
    };
  }
  /**
   * Post to every registered panel. Returns how many received it, which is
   * what the caller logs — "emitted, 0 panels" is the signature of this bug
   * and is otherwise indistinguishable from a message that was never sent.
   *
   * One panel that throws (a webview disposed between the notification and
   * this loop) must not swallow delivery to the rest, so failures are counted
   * out rather than propagated.
   */
  send(msg) {
    let delivered = 0;
    for (const sink of this.panels) {
      try {
        sink.postMessage(msg);
        delivered += 1;
      } catch {
      }
    }
    return delivered;
  }
};

// src/plotTheme.ts
var PLOT_THEME_STATE_KEY = "scistack.plotTheme";
var DEFAULT_PLOT_THEME = "dark";
function parsePlotThemeMode(value) {
  return value === "dark" || value === "light" ? value : null;
}
var PlotThemeHost = class {
  constructor(memento) {
    this.memento = memento;
    this.sinks = new PanelRegistry();
  }
  /** The stored mode; anything unreadable is the default, not an error. */
  get mode() {
    return parsePlotThemeMode(this.memento.get(PLOT_THEME_STATE_KEY)) ?? DEFAULT_PLOT_THEME;
  }
  /** Register a webview for changes. Call the returned function on dispose. */
  add(sink) {
    return this.sinks.add(sink);
  }
  /**
   * Store `value` and tell every open webview but `origin` (which switched
   * itself already). Returns how many were told, for the caller's log line.
   * Throws on anything that is not a mode, so a malformed request is answered
   * with an error instead of storing garbage.
   */
  async set(value, origin) {
    const mode = parsePlotThemeMode(value);
    if (!mode)
      throw new Error(`not a plot theme: ${JSON.stringify(value)}`);
    await this.memento.update(PLOT_THEME_STATE_KEY, mode);
    const msg = { method: "plot_theme_changed", params: { mode } };
    let notified = 0;
    for (const sink of this.sinks.sinks()) {
      if (sink === origin)
        continue;
      try {
        sink.postMessage(msg);
        notified += 1;
      } catch {
      }
    }
    return { mode, notified };
  }
  /**
   * The `<script>` body that hands a new webview the current mode. JSON, so
   * the value cannot break out of the tag whatever the store holds.
   */
  initScript() {
    return `window.__SCISTACK_PLOT_THEME__ = ${JSON.stringify(this.mode)};`;
  }
};
var shared;
function plotThemeHost(memento) {
  shared ??= new PlotThemeHost(memento);
  return shared;
}
async function answerSetPlotTheme(host, msg, panel, log) {
  try {
    const params = msg.params ?? {};
    const { mode, notified } = await host.set(params.mode, panel);
    log.appendLine(`plot theme: ${mode} (stored; ${notified} other webview(s) told)`);
    panel.postMessage({ id: msg.id, result: { mode } });
  } catch (err) {
    log.appendLine(`plot theme: set failed \u2014 ${err}`);
    panel.postMessage({ id: msg.id, error: { message: String(err) } });
  }
}

// src/plotPanel.ts
var PlotPanel = class _PlotPanel {
  constructor(context, session, target, column) {
    this.context = context;
    this.session = session;
    this.target = target;
    this.disposables = [];
    this.unregister = () => {
    };
    this.unregisterTheme = () => {
    };
    this.panel = vscode.window.createWebviewPanel(
      "scistack.plot",
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
          vscode.Uri.file(path.join(context.extensionPath, "dist", "webview"))
        ]
      }
    );
    this.panel.webview.html = this.getHtml();
    this.unregister = this.session.plots.add(this);
    this.unregisterTheme = plotThemeHost(context.globalState).add(this);
    this.panel.onDidChangeViewState(
      (e) => {
        if (e.webviewPanel.active)
          this.session.manager.setActive(this.session.id);
      },
      void 0,
      this.disposables
    );
    this.panel.webview.onDidReceiveMessage(
      async (msg) => {
        const method = msg.method;
        if (method === "set_plot_theme") {
          await answerSetPlotTheme(
            plotThemeHost(this.context.globalState),
            msg,
            this,
            this.session.log
          );
          return;
        }
        if (method === "pick_save_path") {
          try {
            const params = msg.params ?? {};
            const folder = vscode.workspace.workspaceFolders?.[0]?.uri;
            const uri = await vscode.window.showSaveDialog({
              defaultUri: folder ? vscode.Uri.joinPath(folder, params.defaultName ?? "figure.png") : void 0,
              // The panel sends the ONE format its dropdown selected, so the
              // dialog cannot offer a second answer to a question already
              // asked — the backend honours the dropdown either way.
              filters: { [params.filterName ?? "Images"]: params.formats ?? ["png"] }
            });
            this.panel.webview.postMessage({
              id: msg.id,
              result: { path: uri?.fsPath ?? null }
            });
          } catch (err) {
            this.panel.webview.postMessage({
              id: msg.id,
              error: { message: String(err) }
            });
          }
          return;
        }
        if (method === "pick_save_folder") {
          try {
            const uris = await vscode.window.showOpenDialog({
              canSelectFiles: false,
              canSelectFolders: true,
              canSelectMany: false,
              defaultUri: vscode.workspace.workspaceFolders?.[0]?.uri,
              openLabel: "Save figures here"
            });
            this.panel.webview.postMessage({
              id: msg.id,
              result: { path: uris?.[0]?.fsPath ?? null }
            });
          } catch (err) {
            this.panel.webview.postMessage({
              id: msg.id,
              error: { message: String(err) }
            });
          }
          return;
        }
        try {
          const result = await this.session.python.request(
            method,
            msg.params ?? {}
          );
          this.panel.webview.postMessage({ id: msg.id, result });
        } catch (err) {
          this.session.log.appendLine(`plot panel: ${method} failed \u2014 ${err}`);
          this.panel.webview.postMessage({
            id: msg.id,
            error: { message: String(err) }
          });
        }
      },
      void 0,
      this.disposables
    );
    this.panel.onDidDispose(() => this.dispose(), void 0, this.disposables);
  }
  static show(context, session, target, options = {}) {
    return new _PlotPanel(
      context,
      session,
      target,
      // This session's pipeline group, so the figure is a sibling tab of the
      // canvas it came from rather than a split the user did not ask for.
      options.column ?? session.dagPanel?.viewColumn ?? vscode.ViewColumn.One
    );
  }
  /** Post a message into this panel's webview (the `MessageSink` contract). */
  postMessage(msg) {
    this.panel.webview.postMessage(msg);
  }
  /**
   * Close this tab. Called when its session closes: a plot tab cannot
   * outlive the server it sends every `plot_*` RPC to.
   */
  close() {
    this.panel.dispose();
  }
  /**
   * The tab title. It names the database as well as the variable: with plot
   * tabs open across two databases, "Plot — StepLength" twice over says
   * nothing about which is which.
   */
  title() {
    if (this.target.csvPath)
      return `Plot \u2014 ${path.basename(this.target.csvPath)}`;
    const variable = this.target.variable ? `Plot \u2014 ${this.target.variable}` : "Plot";
    return this.session.isPlotOnly ? variable : `${variable} \xB7 ${this.session.label}`;
  }
  dispose() {
    this.unregister();
    this.unregisterTheme();
    while (this.disposables.length)
      this.disposables.pop()?.dispose();
  }
  getHtml() {
    const webviewDir = path.join(this.context.extensionPath, "dist", "webview");
    const webview = this.panel.webview;
    const scriptUri = webview.asWebviewUri(
      vscode.Uri.file(path.join(webviewDir, "index.js"))
    );
    const styleUri = webview.asWebviewUri(
      vscode.Uri.file(path.join(webviewDir, "index.css"))
    );
    const nonce = getNonce();
    const target = JSON.stringify({
      view: "plot",
      variable: this.target.variable ?? null,
      csvPath: this.target.csvPath ?? null,
      location: this.target.location ?? null
    });
    const session = JSON.stringify({
      id: this.session.id,
      dbName: this.session.isPlotOnly ? null : this.session.label,
      dbPath: this.session.dbPath || null
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
};
function getNonce() {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let text = "";
  for (let i = 0; i < 32; i++) {
    text += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return text;
}

// src/session.ts
var path5 = __toESM(require("path"));
var vscode5 = __toESM(require("vscode"));

// src/dagPanel.ts
var vscode3 = __toESM(require("vscode"));
var path4 = __toESM(require("path"));

// src/sessionCore.ts
var path2 = __toESM(require("path"));
function prefixedLog(sink, prefix) {
  return {
    appendLine(line) {
      for (const one of line.split("\n")) {
        sink.appendLine(`[${prefix}] ${one}`);
      }
    }
  };
}
function sessionIdForDb(dbPath, platform = process.platform) {
  const resolved = path2.resolve(dbPath);
  return platform === "win32" ? resolved.toLowerCase() : resolved;
}
function sessionLabel(dbPath) {
  return path2.basename(dbPath);
}
function sessionSlug(id) {
  let hash = 2166136261;
  for (let i = 0; i < id.length; i++) {
    hash ^= id.charCodeAt(i);
    hash = Math.imul(hash, 16777619) >>> 0;
  }
  return hash.toString(16).padStart(8, "0");
}
function projectRootForDb(dbPath, folders, platform = process.platform) {
  if (folders.length === 0)
    return void 0;
  const db = sessionIdForDb(dbPath, platform);
  let best;
  for (const folder of folders) {
    const root = sessionIdForDb(folder, platform);
    const prefix = root.endsWith(path2.sep) ? root : root + path2.sep;
    if (!db.startsWith(prefix))
      continue;
    if (best === void 0 || folder.length > best.length)
      best = folder;
  }
  return best ?? folders[0];
}
var SessionRegistry = class {
  constructor() {
    this.sessions = /* @__PURE__ */ new Map();
  }
  get size() {
    return this.sessions.size;
  }
  /** Every open session, in the order they were opened. */
  all() {
    return [...this.sessions.values()];
  }
  get(id) {
    return this.sessions.get(id);
  }
  /** The session for a database path, whatever spelling it arrives in. */
  byDbPath(dbPath) {
    return this.sessions.get(sessionIdForDb(dbPath));
  }
  /** Register a session and make it the active one (it was just opened). */
  add(session) {
    this.sessions.set(session.id, session);
    this.activeId = session.id;
  }
  /**
   * Forget a session. If it was the active one, the most recently added
   * survivor takes over — never a dangling id, which would make `resolve`
   * report 'active' and hand back undefined.
   */
  remove(id) {
    this.sessions.delete(id);
    if (this.activeId !== id)
      return;
    const survivors = this.all();
    this.activeId = survivors.length ? survivors[survivors.length - 1].id : void 0;
  }
  /** Note that a session's panel was focused. Unknown ids are ignored. */
  setActive(id) {
    if (this.sessions.has(id))
      this.activeId = id;
  }
  get active() {
    return this.activeId ? this.sessions.get(this.activeId) : void 0;
  }
  /**
   * Which session a command means — explicit id first, then "there is only
   * one", then the last focused panel.
   *
   * The 'only' step is not redundant with 'active': a command can arrive
   * before any panel has ever been focused (the Explorer context menu at
   * startup), and with a single session open there is nothing to be
   * ambiguous about.
   */
  resolve(explicitId) {
    if (explicitId) {
      const session = this.sessions.get(explicitId);
      if (session) {
        return {
          session,
          source: "explicit",
          detail: `named by the calling panel (${session.dbPath})`
        };
      }
    }
    const all = this.all();
    if (all.length === 1) {
      return {
        session: all[0],
        source: "only",
        detail: `the only open database (${all[0].dbPath})`
      };
    }
    const active = this.active;
    if (active) {
      return {
        session: active,
        source: "active",
        detail: `the last focused pipeline (${active.dbPath})`
      };
    }
    return { session: void 0, source: "none", detail: "no database is open" };
  }
};

// src/matlabTerminal.ts
var fs = __toESM(require("fs"));
var os = __toESM(require("os"));
var path3 = __toESM(require("path"));
var vscode2 = __toESM(require("vscode"));
function formatStamp(d) {
  const p = (n, w = 2) => String(n).padStart(w, "0");
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
}
function isMatlabExtensionAvailable() {
  return vscode2.extensions.getExtension("MathWorks.language-matlab") !== void 0;
}
function isMatlabTerminalOpen() {
  return vscode2.window.terminals.some((t) => t.name === "MATLAB");
}
async function runInMatlabTerminal(command, outputChannel2, sessionSlug2) {
  if (!isMatlabExtensionAvailable()) {
    return false;
  }
  try {
    const t0 = Date.now();
    const scriptPath = path3.join(
      os.tmpdir(),
      sessionSlug2 ? `scistack_run_${sessionSlug2}.m` : "scistack_run.m"
    );
    fs.writeFileSync(scriptPath, command, "utf-8");
    const tWritten = Date.now();
    outputChannel2?.appendLine(
      `runInMatlabTerminal: wrote ${command.length}-char script to ${scriptPath} [timing] write=${tWritten - t0}ms`
    );
    await vscode2.commands.executeCommand("matlab.openCommandWindow");
    const tOpened = Date.now();
    outputChannel2?.appendLine(
      `runInMatlabTerminal: [timing] openCommandWindow=${tOpened - tWritten}ms`
    );
    const terminal = vscode2.window.terminals.find((t) => t.name === "MATLAB");
    if (!terminal) {
      outputChannel2?.appendLine(
        "MathWorks extension found but MATLAB terminal not available."
      );
      return false;
    }
    const forMatlab = scriptPath.replace(/\\/g, "/");
    const runLine = `run('${forMatlab}');`;
    outputChannel2?.appendLine(`runInMatlabTerminal: sendText ${runLine}`);
    terminal.sendText(runLine);
    terminal.show();
    const tSent = Date.now();
    outputChannel2?.appendLine(
      `runInMatlabTerminal: [timing] sendText=${tSent - tOpened}ms, dispatch_total=${tSent - t0}ms, sent_at=${formatStamp(new Date(tSent))}`
    );
    return true;
  } catch (err) {
    outputChannel2?.appendLine(`Failed to send to MATLAB terminal: ${err}`);
    return false;
  }
}

// src/matlabRunTracker.ts
var MatlabRunTracker = class {
  constructor() {
    this.inFlight = /* @__PURE__ */ new Set();
    /**
     * The subset of in-flight runs that occupy the window's ONE MathWorks
     * MATLAB — see `noteSharedEngine`.
     */
    this.sharedEngine = /* @__PURE__ */ new Set();
    this.refreshPending = false;
    this.finishedCallbacks = [];
  }
  /** Mark a MATLAB run as owning the database from now until its run_done. */
  begin(runId) {
    this.inFlight.add(runId);
  }
  /**
   * Record that this run went to the **shared** MATLAB — the MathWorks
   * terminal, or the clipboard destined for it — rather than to this
   * session's own sidecar.
   *
   * The distinction exists because only one of the two tiers is shared
   * between databases:
   *
   * - **sidecar** — `scistack_gui.matlab_sidecar._sidecar` is a *process*
   *   singleton, and every session has its own Python server process, so
   *   every session already has its own MATLAB. Two databases running
   *   through sidecars are as independent as two Python runs and must not
   *   block each other.
   * - **terminal / clipboard** — the MathWorks extension owns one MATLAB
   *   per VS Code window, and a SciStack script points it at one database
   *   with `configure_database` before doing anything else.
   *
   * Without this split the gate was exactly backwards: a sidecar run held
   * the mark for its whole duration (Python pushes a real `run_done`) and
   * blocked the other database pointlessly, while a terminal run — the one
   * that genuinely shares an engine — cleared it milliseconds after
   * dispatch.
   */
  noteSharedEngine(runId) {
    if (this.inFlight.has(runId))
      this.sharedEngine.add(runId);
  }
  /** Whether a run is currently occupying the window's shared MATLAB. */
  get sharedEngineActive() {
    return this.sharedEngine.size > 0;
  }
  /**
   * Clear a run's mark. Safe to call for every run_done — Python runs are
   * simply absent from the set. Returns whether this was a tracked MATLAB
   * run, and fires the finished callbacks once the last one clears.
   */
  end(runId) {
    if (!runId)
      return false;
    this.sharedEngine.delete(runId);
    const wasTracked = this.inFlight.delete(runId);
    if (wasTracked && this.inFlight.size === 0) {
      this.finishedCallbacks.forEach((cb) => cb());
    }
    return wasTracked;
  }
  /** Whether any MATLAB run currently holds the database. */
  get isActive() {
    return this.inFlight.size > 0;
  }
  /**
   * Called by the DB file-watcher. Returns true when the caller should
   * refresh the DAG now; false when MATLAB owns the database, in which case
   * the change is remembered for {@link takeDeferredRefresh}.
   */
  noteDbChange() {
    if (this.isActive) {
      this.refreshPending = true;
      return false;
    }
    return true;
  }
  /**
   * Consume the deferred refresh, if any. Returns true at most once per
   * withheld change — a MATLAB run that wrote nothing costs no re-fetch.
   */
  takeDeferredRefresh() {
    if (!this.refreshPending)
      return false;
    this.refreshPending = false;
    return true;
  }
  /** Register a callback fired when the LAST in-flight MATLAB run ends. */
  onAllFinished(callback) {
    this.finishedCallbacks.push(callback);
  }
};

// src/matlabConnectionGate.ts
function needsMatlabConnectionPrompt(matlabExtensionAvailable, matlabTerminalAlreadyOpen) {
  return matlabExtensionAvailable && !matlabTerminalAlreadyOpen;
}
function matlabHolder(sessions2, selfId) {
  return sessions2.find((s) => s.id !== selfId && s.matlabBusy)?.label;
}

// src/dagPanel.ts
var DEBUG_SESSION_BASE = "Attach to scistack-gui server";
var DagPanel = class {
  constructor(context, session, outputChannel2) {
    this.context = context;
    this.session = session;
    this.outputChannel = outputChannel2;
    this.disposables = [];
    this.disposeCallbacks = [];
    /**
     * Which MATLAB runs currently own the DuckDB file lock. Shared with this
     * session's DB file-watcher, which must not refresh the DAG while MATLAB
     * has the database — see MatlabRunTracker. Per panel, i.e. per database:
     * a MATLAB run against one database must not defer the other's refreshes.
     */
    this.matlabRuns = new MatlabRunTracker();
    /** Called when this panel gains or loses focus — see `onDidChangeActive`. */
    this.activeCallbacks = [];
    this.panel = vscode3.window.createWebviewPanel(
      "scistack.dag",
      // The database is in the tab title because there can be several: with
      // two canvases both called "SciStack Pipeline" the tab bar says
      // nothing about which is which.
      `SciStack \u2014 ${session.label}`,
      vscode3.ViewColumn.One,
      {
        enableScripts: true,
        retainContextWhenHidden: true,
        localResourceRoots: [
          vscode3.Uri.file(path4.join(context.extensionPath, "dist", "webview"))
        ]
      }
    );
    this.panel.webview.html = this.getHtml();
    this.disposeCallbacks.push(plotThemeHost(context.globalState).add(this));
    this.panel.onDidChangeViewState(
      (e) => {
        for (const cb of this.activeCallbacks)
          cb(e.webviewPanel.active);
      },
      void 0,
      this.disposables
    );
    this.panel.webview.onDidReceiveMessage(
      async (msg) => {
        const method = msg.method;
        if (method === "restart_python") {
          try {
            await vscode3.commands.executeCommand("scistack.restartPython");
            this.panel.webview.postMessage({ id: msg.id, result: { ok: true } });
          } catch (err) {
            this.panel.webview.postMessage({
              id: msg.id,
              error: { message: String(err) }
            });
          }
          return;
        }
        if (method === "set_plot_theme") {
          await answerSetPlotTheme(
            plotThemeHost(this.context.globalState),
            msg,
            this,
            this.outputChannel
          );
          return;
        }
        if (method === "open_plot_panel") {
          try {
            await vscode3.commands.executeCommand("scistack.openPlotPanel", {
              ...msg.params ?? {},
              // Name the database outright. A plot opened from THIS canvas
              // must read THIS database, whatever tab was focused last.
              sessionId: this.session.id
            });
            this.panel.webview.postMessage({ id: msg.id, result: { ok: true } });
          } catch (err) {
            this.panel.webview.postMessage({
              id: msg.id,
              error: { message: String(err) }
            });
          }
          return;
        }
        if (method === "pick_save_path") {
          try {
            const params = msg.params ?? {};
            const folder = vscode3.workspace.workspaceFolders?.[0]?.uri;
            const uri = await vscode3.window.showSaveDialog({
              defaultUri: folder ? vscode3.Uri.joinPath(folder, params.defaultName ?? "figure.png") : void 0,
              filters: {
                [params.filterName ?? "Images"]: params.formats ?? ["png", "svg", "pdf"]
              }
            });
            this.panel.webview.postMessage({
              id: msg.id,
              result: { path: uri?.fsPath ?? null }
            });
          } catch (err) {
            this.panel.webview.postMessage({
              id: msg.id,
              error: { message: String(err) }
            });
          }
          return;
        }
        if (method === "stop_waiting_for_matlab") {
          try {
            const params = msg.params ?? {};
            const runId = params.run_id;
            if (runId) {
              await this.session.python.request("stop_watching_matlab_run", {
                run_id: runId
              });
              this.matlabRuns.end(runId);
              this.panel.webview.postMessage({
                method: "run_done",
                params: {
                  run_id: runId,
                  success: false,
                  cancelled: true,
                  duration_ms: 0
                }
              });
              this.outputChannel.appendLine(
                `stop_waiting_for_matlab: stopped watching ${runId} on request`
              );
            }
            this.panel.webview.postMessage({ id: msg.id, result: { ok: true } });
          } catch (err) {
            this.panel.webview.postMessage({
              id: msg.id,
              error: { message: String(err) }
            });
          }
          return;
        }
        if (method === "reveal_in_editor") {
          try {
            const params = msg.params ?? {};
            const result = await this.revealInEditor(params);
            this.panel.webview.postMessage({ id: msg.id, result });
          } catch (err) {
            this.panel.webview.postMessage({
              id: msg.id,
              error: { message: String(err) }
            });
          }
          return;
        }
        if (method === "start_run") {
          const params = msg.params ?? {};
          const language = params.language;
          const functionName = params.function_name;
          const variants = params.variants;
          this.outputChannel.appendLine(
            `start_run: function=${functionName ?? "<?>"} language=${language ?? "python"} variants=${variants ? variants.length : 0}`
          );
          if (language !== "matlab") {
            await this.ensureDebugAttached();
          }
          try {
            const result = await this.session.python.request(
              method,
              params
            );
            this.panel.webview.postMessage({ id: msg.id, result });
            if (result.host_execution_required && result.language === "matlab") {
              await this.handleMatlabRun(result.run_id, params);
            }
          } catch (err) {
            this.panel.webview.postMessage({
              id: msg.id,
              error: { message: String(err) }
            });
          }
          return;
        }
        if (method === "start_pipeline_run") {
          try {
            const result = await this.session.python.request(
              method,
              msg.params ?? {}
            );
            this.panel.webview.postMessage({ id: msg.id, result });
            if (result.host_execution_required && result.language === "matlab") {
              await this.handleMatlabPipelineRun(
                result.run_id,
                msg.params ?? {}
              );
            }
          } catch (err) {
            this.panel.webview.postMessage({
              id: msg.id,
              error: { message: String(err) }
            });
          }
          return;
        }
        try {
          const result = await this.session.python.request(
            method,
            msg.params ?? {}
          );
          this.panel.webview.postMessage({
            id: msg.id,
            result
          });
        } catch (err) {
          this.panel.webview.postMessage({
            id: msg.id,
            error: { message: String(err) }
          });
        }
      },
      void 0,
      this.disposables
    );
    this.panel.onDidDispose(() => {
      this.disposables.forEach((d) => d.dispose());
      for (const cb of this.disposeCallbacks)
        cb();
    }, null, this.disposables);
  }
  /**
   * Which editor group the pipeline canvas lives in. The Plot Studio opens in
   * the SAME group so it becomes a full-width sibling tab rather than a split;
   * reading it from the panel (instead of assuming ViewColumn.One) keeps that
   * true after the user drags the pipeline tab elsewhere.
   */
  get viewColumn() {
    return this.panel.viewColumn;
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
  async revealInEditor(params) {
    const { file, line } = params;
    this.outputChannel.appendLine(`reveal_in_editor: file=${file} line=${line}`);
    if (!file)
      return { ok: false, error: "No file path provided." };
    const uri = this.buildFileUri(file);
    this.outputChannel.appendLine(`reveal_in_editor: resolved uri=${uri.toString()}`);
    let doc;
    try {
      doc = await vscode3.workspace.openTextDocument(uri);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.outputChannel.appendLine(
        `reveal_in_editor: openTextDocument failed for ${uri.toString()}: ${msg}`
      );
      return { ok: false, error: `openTextDocument failed: ${msg}` };
    }
    const zeroBased = Math.max(0, (line ?? 1) - 1);
    const selection = new vscode3.Range(zeroBased, 0, zeroBased, 0);
    let editor;
    try {
      editor = await vscode3.window.showTextDocument(doc, {
        viewColumn: vscode3.ViewColumn.Beside,
        preserveFocus: false,
        selection
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.outputChannel.appendLine(
        `reveal_in_editor: showTextDocument failed for ${uri.toString()}: ${msg}`
      );
      return { ok: false, error: `showTextDocument failed: ${msg}` };
    }
    editor.revealRange(selection, vscode3.TextEditorRevealType.InCenter);
    return { ok: true };
  }
  /**
   * Build a file URI, handling Windows UNC paths (`\\server\share\path`)
   * explicitly. `vscode.Uri.file` accepts UNC but its canonicalization has
   * known edge cases; constructing via `Uri.from` with an explicit
   * authority removes that ambiguity.
   */
  buildFileUri(file) {
    if (file.startsWith("\\\\") || file.startsWith("//")) {
      const rest = file.replace(/^[\\/]{2}/, "");
      const slashIdx = rest.search(/[\\/]/);
      if (slashIdx > 0) {
        const authority = rest.substring(0, slashIdx);
        const pathPart = "/" + rest.substring(slashIdx + 1).replace(/\\/g, "/");
        return vscode3.Uri.from({ scheme: "file", authority, path: pathPart });
      }
    }
    return vscode3.Uri.file(file);
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
  async gateOnMatlabConnection() {
    if (!needsMatlabConnectionPrompt(isMatlabExtensionAvailable(), isMatlabTerminalOpen())) {
      return false;
    }
    const choice = await vscode3.window.showInformationMessage(
      "MATLAB is not connected to VS Code yet. Connect now, then click Run again once MATLAB is ready.",
      "Connect",
      "Cancel"
    );
    if (choice === "Connect") {
      await vscode3.commands.executeCommand("matlab.openCommandWindow");
      this.outputChannel.appendLine(
        "gateOnMatlabConnection: opened the MATLAB command window \u2014 connecting, not dispatching a run"
      );
    } else {
      this.outputChannel.appendLine("gateOnMatlabConnection: user declined to connect");
    }
    return true;
  }
  /**
   * Refuse a MATLAB run while another database owns the engine.
   *
   * There is one MATLAB process per VS Code window, and a SciStack run
   * points it at one database with `configure_database` before doing
   * anything else — so two sessions dispatching at once do not run in
   * parallel. The second script repoints the engine mid-run and the first
   * run's remaining `for_each` calls write into the other project's
   * database. Both writes are well-formed, so nothing downstream can
   * detect it; the only place to stop it is before the script is generated.
   *
   * Returns true when the caller must stop (having told the user which
   * database to wait for). The decision itself is
   * `matlabConnectionGate.matlabHolder`, which is unit-tested.
   *
   * **Only the shared engine is gated.** MATLAB is not one process per
   * window in general — `matlab_sidecar._sidecar` is a *process* singleton
   * and every session has its own Python server, so every session already
   * has its own sidecar MATLAB. Two databases running through sidecars are
   * as independent as two Python runs and are never blocked here. What IS
   * shared is the MathWorks extension's terminal: it owns one MATLAB per
   * VS Code window, which is its design, not ours. Hence
   * `MatlabRunTracker.sharedEngineActive` rather than `isActive`.
   *
   * **Coverage, for the tier that is gated.** `handleMatlabRun` calls
   * `finish(true)` as soon as a terminal-tier script is sent, because
   * nothing tells the extension when a MATLAB *terminal* run ends. So two
   * Run clicks in quick succession are caught; clicking Run in database B
   * ten seconds into a two-minute terminal run in A is not, and B's
   * `configure_database` will repoint the engine under A. The real fix is
   * run markers written by MATLAB itself — Stage 2 of
   * `.claude/plan-matlab-terminal-run-tracking.md`, still deferred. A user
   * who needs genuinely parallel MATLAB runs today can have them: that is
   * what the sidecar tier already is.
   *
   * The separate half of this problem — two sessions writing one temp
   * script file — is fixed unconditionally by the per-session filename
   * (`sessionCore.sessionSlug`), which needs no tracking at all.
   */
  async refuseIfMatlabBusyElsewhere() {
    const holder = matlabHolder(
      this.session.manager.everything().map((s) => ({
        id: s.id,
        label: s.label,
        // The SHARED engine only — a sidecar run is this session's own
        // MATLAB process and blocks nobody.
        matlabBusy: s.dagPanel?.matlabRuns.sharedEngineActive ?? false
      })),
      this.session.id
    );
    if (!holder)
      return false;
    const message = `SciStack: MATLAB is running ${holder} right now. One MATLAB session can only be pointed at one database at a time \u2014 wait for that run to finish, then click Run again.`;
    this.outputChannel.appendLine(
      `refuseIfMatlabBusyElsewhere: ${this.session.label} blocked \u2014 MATLAB is held by ${holder}`
    );
    await vscode3.window.showWarningMessage(message);
    return true;
  }
  /**
   * Hand the run to Python's watcher, which is the thing that will tell us
   * how it ended.
   *
   * This is what replaces `finish(true)` on dispatch. Until now a terminal
   * or clipboard run reported success the moment the text left here — before
   * MATLAB had executed a line, and whether or not it then failed. Python
   * watches two signals it can actually observe (the run's own markers, and
   * who holds the DuckDB file) and pushes a real `run_done` on the same
   * run_id, through the same channel the sidecar tier already uses.
   *
   * Returns whether the run is now being watched. False means we must fall
   * back to the old behaviour rather than leave the node spinning for ever —
   * a node that never resolves is worse than one that resolves optimistically.
   */
  async watchMatlabRun(runId, label) {
    try {
      await this.session.python.request("watch_matlab_terminal_run", {
        run_id: runId,
        label
      });
      this.outputChannel.appendLine(
        `watchMatlabRun: ${runId} handed to the run watcher \u2014 the node resolves when MATLAB reports, not now`
      );
      return true;
    } catch (err) {
      this.outputChannel.appendLine(
        `watchMatlabRun: could not watch ${runId} (${err}) \u2014 falling back to resolving on dispatch`
      );
      return false;
    }
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
  async dispatchMatlabCommand(command, runId, warnings) {
    const sent = await runInMatlabTerminal(
      command,
      this.outputChannel,
      sessionSlug(this.session.id)
    );
    if (sent) {
      if (runId)
        this.matlabRuns.noteSharedEngine(runId);
      this.outputChannel.appendLine("dispatchMatlabCommand: sent to MATLAB terminal");
      vscode3.window.showInformationMessage("Running in MATLAB terminal...");
      return "terminal";
    }
    if (runId) {
      try {
        const sidecarResult = await this.session.python.request(
          "start_matlab_sidecar_run",
          { command, run_id: runId, warnings: warnings ?? [] }
        );
        if (sidecarResult.sidecar_available) {
          this.outputChannel.appendLine(
            "dispatchMatlabCommand: dispatched via standalone MATLAB sidecar"
          );
          vscode3.window.showInformationMessage(
            "Running via standalone MATLAB sidecar..."
          );
          return "sidecar";
        }
        this.outputChannel.appendLine(
          "dispatchMatlabCommand: sidecar unavailable (matlab not on PATH)"
        );
      } catch (err) {
        this.outputChannel.appendLine(
          `dispatchMatlabCommand: sidecar dispatch failed: ${err}`
        );
      }
    }
    if (runId)
      this.matlabRuns.noteSharedEngine(runId);
    await vscode3.env.clipboard.writeText(command);
    this.outputChannel.appendLine(
      "dispatchMatlabCommand: no MATLAB terminal or sidecar available, copied to clipboard"
    );
    vscode3.window.showInformationMessage(
      "MATLAB command copied to clipboard. Paste into MATLAB to run."
    );
    return "clipboard";
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
  async handleMatlabRun(runId, params) {
    const functionName = params.function_name;
    this.outputChannel.appendLine(
      `handleMatlabRun: requesting generate_matlab_command for ${functionName ?? "<?>"}`
    );
    const finish = (success, cancelled = false) => {
      this.matlabRuns.end(runId);
      this.panel.webview.postMessage({
        method: "run_done",
        params: { run_id: runId, success, duration_ms: 0, cancelled }
      });
    };
    if (await this.gateOnMatlabConnection()) {
      finish(false, true);
      return;
    }
    if (await this.refuseIfMatlabBusyElsewhere()) {
      finish(false, true);
      return;
    }
    this.beginMatlabRun(runId);
    try {
      const result = await this.session.python.request(
        "generate_matlab_command",
        // run_id is what makes the generated script report for itself: the
        // generator emits scidb.run_marker calls only for a run something
        // is watching (a preview or a clipboard copy passes none).
        { ...params, run_id: runId }
      );
      const command = result.command;
      this.outputChannel.appendLine(
        `handleMatlabRun: got command (${command.length} chars)`
      );
      const tier = await this.dispatchMatlabCommand(command, runId, void 0);
      if (tier !== "sidecar") {
        const watched = await this.watchMatlabRun(runId, functionName ?? runId);
        if (!watched)
          finish(true);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.outputChannel.appendLine(`handleMatlabRun: failed: ${msg}`);
      this.panel.webview.postMessage({
        method: "run_output",
        params: { run_id: runId, text: `Error: ${msg}
` }
      });
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
  beginMatlabRun(runId) {
    this.matlabRuns.begin(runId);
    this.outputChannel.appendLine(
      `MATLAB run ${runId} in flight \u2014 DAG refreshes deferred until it finishes`
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
  async handleMatlabPipelineRun(runId, params) {
    const pipelineId = params.pipeline_id;
    this.outputChannel.appendLine(
      `handleMatlabPipelineRun: requesting generate_matlab_pipeline_command for ${pipelineId ?? "<?>"} (run_id=${runId})`
    );
    const emit = (text) => {
      this.panel.webview.postMessage({
        method: "run_output",
        params: { run_id: runId, text }
      });
    };
    const finish = (success, cancelled = false) => {
      this.matlabRuns.end(runId);
      this.panel.webview.postMessage({
        method: "run_done",
        params: { run_id: runId, success, duration_ms: 0, cancelled }
      });
    };
    if (await this.gateOnMatlabConnection()) {
      emit("MATLAB is not connected yet \u2014 connect, then click Run again once it is ready.\n");
      finish(false, true);
      return;
    }
    if (await this.refuseIfMatlabBusyElsewhere()) {
      emit("MATLAB is busy with another database \u2014 wait for that run to finish.\n");
      finish(false, true);
      return;
    }
    this.beginMatlabRun(runId);
    try {
      const result = await this.session.python.request(
        "generate_matlab_pipeline_command",
        { ...params, run_id: runId }
      );
      const command = result.command;
      this.outputChannel.appendLine(
        `handleMatlabPipelineRun: got command (${command.length} chars)`
      );
      for (const w of result.warnings ?? []) {
        emit(`\u26A0 ${w}
`);
      }
      const tier = await this.dispatchMatlabCommand(command, runId, result.warnings);
      if (tier !== "sidecar") {
        if (tier === "terminal") {
          emit("\u25B6 Sent whole-pipeline script to MATLAB terminal...\n");
        } else {
          emit("MATLAB pipeline script copied to clipboard. Paste into MATLAB to run.\n");
        }
        const watched = await this.watchMatlabRun(runId, pipelineId ?? runId);
        if (!watched)
          finish(true);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.outputChannel.appendLine(`handleMatlabPipelineRun: failed: ${msg}`);
      emit(`Error: ${msg}
`);
      finish(false);
    }
  }
  /**
   * Post a notification message to the Webview (from Python push notifications).
   */
  postMessage(msg) {
    this.panel.webview.postMessage(msg);
  }
  /**
   * Ensure a debugpy attach session is active before a Run begins, so
   * breakpoints inside user functions get hit. No-op if scistack.debug is
   * disabled or a session is already attached.
   */
  async ensureDebugAttached() {
    const cfg = vscode3.workspace.getConfiguration("scistack");
    if (!cfg.get("debug", false))
      return;
    if (this.debugSession)
      return;
    const existing = this.findExistingDebugSession();
    if (existing) {
      this.debugSession = existing;
      return;
    }
    const port = this.session.debugPort ?? cfg.get("debugPort", 5678);
    const folder = this.session.projectRoot ? vscode3.workspace.getWorkspaceFolder(vscode3.Uri.file(this.session.projectRoot)) : vscode3.workspace.workspaceFolders?.[0];
    const started = await vscode3.debug.startDebugging(folder, {
      name: this.debugSessionName(),
      type: "debugpy",
      request: "attach",
      connect: { host: "127.0.0.1", port },
      justMyCode: false
    });
    if (!started) {
      this.outputChannel.appendLine(
        "Warning: failed to start debugpy attach session. Is the server running with scistack.debug enabled?"
      );
      return;
    }
    this.debugSession = vscode3.debug.activeDebugSession ?? this.findExistingDebugSession();
  }
  /**
   * Detach the debug session (called when run_done arrives).
   */
  async stopDebugSession() {
    const session = this.debugSession ?? this.findExistingDebugSession();
    this.debugSession = void 0;
    if (session) {
      await vscode3.debug.stopDebugging(session);
    }
  }
  findExistingDebugSession() {
    const active = vscode3.debug.activeDebugSession;
    if (active && active.name === this.debugSessionName())
      return active;
    return void 0;
  }
  /** This canvas's debug session name — see DEBUG_SESSION_BASE. */
  debugSessionName() {
    return `${DEBUG_SESSION_BASE} (${this.session.label})`;
  }
  /**
   * Reveal the panel if it's hidden.
   *
   * In its own column, not ViewColumn.One: with several canvases open the
   * user may well have dragged one into a split, and revealing it into
   * column one would move their tab for them.
   */
  reveal() {
    this.panel.reveal(this.panel.viewColumn ?? vscode3.ViewColumn.One);
  }
  /** Close this canvas. Its dispose callbacks close the session with it. */
  dispose() {
    this.panel.dispose();
  }
  /**
   * Register a callback for when the panel is disposed.
   */
  onDidDispose(callback) {
    this.disposeCallbacks.push(callback);
  }
  /**
   * Register a callback for when this panel gains or loses focus.
   *
   * This is how a Command Palette invocation finds its database: with two
   * canvases open, "the one you are looking at" is the only sensible
   * default, and nothing else in VS Code reports it for a webview.
   */
  onDidChangeActive(callback) {
    this.activeCallbacks.push(callback);
  }
  getHtml() {
    const webviewDir = path4.join(this.context.extensionPath, "dist", "webview");
    const webview = this.panel.webview;
    const scriptUri = webview.asWebviewUri(
      vscode3.Uri.file(path4.join(webviewDir, "index.js"))
    );
    const styleUri = webview.asWebviewUri(
      vscode3.Uri.file(path4.join(webviewDir, "index.css"))
    );
    const nonce = getNonce2();
    const session = JSON.stringify({
      id: this.session.id,
      dbName: this.session.label,
      dbPath: this.session.dbPath
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
  <title>SciStack \u2014 ${this.session.label}</title>
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
  <script nonce="${nonce}">window.__SCISTACK_SESSION__ = ${session};</script>
  <script nonce="${nonce}">${plotThemeHost(this.context.globalState).initScript()}</script>
  <script nonce="${nonce}" src="${scriptUri}"></script>
</body>
</html>`;
  }
};
function getNonce2() {
  let text = "";
  const possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  for (let i = 0; i < 32; i++) {
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  }
  return text;
}

// src/pythonProcess.ts
var import_child_process = require("child_process");
var readline = __toESM(require("readline"));
var vscode4 = __toESM(require("vscode"));

// src/rpcPending.ts
var EXPIRED_MEMORY = 100;
var PendingRequests = class {
  constructor(log, now = Date.now) {
    this.log = log;
    this.now = now;
    this.entries = /* @__PURE__ */ new Map();
    this.expired = /* @__PURE__ */ new Map();
  }
  /**
   * Register request `id` and return the promise its response settles.
   * `timeoutMs <= 0` disables the backstop.
   */
  open(id, method, timeoutMs) {
    return new Promise((resolve2, reject) => {
      const startedAt = this.now();
      const timer = timeoutMs > 0 ? setTimeout(() => {
        if (!this.entries.has(id))
          return;
        const elapsed = this.now() - startedAt;
        this.log.appendLine(
          `RPC timeout: ${method} (id=${id}) got no response in ${elapsed}ms. The Python server may have dropped the request \u2014 check the stderr above for a traceback.`
        );
        this.rememberExpired(id, { method, startedAt, timedOutAt: this.now() });
        this.take(id)?.reject(new Error(
          `SciStack: no response from the Python server for '${method}' after ${Math.round(elapsed / 1e3)}s.`
        ));
      }, timeoutMs) : null;
      this.entries.set(id, { resolve: resolve2, reject, method, startedAt, timer });
    });
  }
  /** Deliver a response. Returns false when no request was waiting for it. */
  resolve(id, result) {
    const entry = this.take(id);
    if (!entry) {
      this.logUnmatched(id);
      return false;
    }
    entry.resolve(result);
    return true;
  }
  /** Deliver an error response. Returns false when nothing was waiting. */
  reject(id, message) {
    const entry = this.take(id);
    if (!entry) {
      this.logUnmatched(id);
      return false;
    }
    this.log.appendLine(
      `RPC error: ${entry.method} (id=${id}, ${this.now() - entry.startedAt}ms): ${message}`
    );
    entry.reject(new Error(message));
    return true;
  }
  /** Fail one request without a server response (e.g. the write failed). */
  fail(id, error) {
    this.take(id)?.reject(error);
  }
  /** Fail every outstanding request — the process is gone. */
  failAll(error) {
    for (const id of [...this.entries.keys()])
      this.fail(id, error);
  }
  get size() {
    return this.entries.size;
  }
  take(id) {
    const entry = this.entries.get(id);
    if (!entry)
      return void 0;
    if (entry.timer)
      clearTimeout(entry.timer);
    this.entries.delete(id);
    return entry;
  }
  rememberExpired(id, info) {
    this.expired.set(id, info);
    if (this.expired.size > EXPIRED_MEMORY) {
      const oldest = this.expired.keys().next().value;
      this.expired.delete(oldest);
    }
  }
  /**
   * A response nobody is waiting for. When it is a request the backstop gave
   * up on, say so with the numbers: "answered 40s after we stopped listening"
   * is what distinguishes a timeout that is too short from a lost request.
   */
  logUnmatched(id) {
    const late = this.expired.get(id);
    if (late) {
      this.expired.delete(id);
      const now = this.now();
      this.log.appendLine(
        `RPC late response: ${late.method} (id=${id}) answered after ${now - late.startedAt}ms, ${now - late.timedOutAt}ms after the timeout gave up on it \u2014 result dropped. If this is routine, raise scistack.rpcTimeoutMs or move the method onto a job.`
      );
      return;
    }
    this.log.appendLine(
      `[stdout] response for unknown request id=${id} \u2014 ignored`
    );
  }
};

// src/pythonProcess.ts
var STDERR_TAIL_LINES = 200;
var PythonProcess = class {
  constructor(pythonPath, args, outputChannel2, options = {}) {
    this.pythonPath = pythonPath;
    this.outputChannel = outputChannel2;
    this.nextId = 1;
    this.notificationHandlers = [];
    this.readyResolve = null;
    this.readyReject = null;
    this.readyTimer = null;
    this.readyTimeoutMs = 0;
    /** Ring of recent stderr lines, so a failed start can report why. */
    this.stderrTail = [];
    this.exitCode = null;
    this.args = args;
    this.pending = new PendingRequests(outputChannel2);
    this.outputChannel.appendLine(`Spawning: ${pythonPath} ${args.join(" ")}`);
    const childEnv = { ...process.env };
    if (options.debugPort !== void 0) {
      childEnv.SCISTACK_GUI_DEBUG = "1";
      childEnv.SCISTACK_GUI_DEBUG_PORT = String(options.debugPort);
      this.outputChannel.appendLine(
        `debugpy listener will start on 127.0.0.1:${options.debugPort} (attach via "Attach to scistack-gui server" launch config)`
      );
    }
    this.proc = (0, import_child_process.spawn)(pythonPath, args, {
      stdio: ["pipe", "pipe", "pipe"],
      env: childEnv,
      cwd: options.cwd
    });
    this.closed = new Promise((resolve2) => {
      this.proc.on("close", () => resolve2());
    });
    const rl = readline.createInterface({ input: this.proc.stdout });
    rl.on("line", (line) => this.handleLine(line));
    this.proc.stderr?.on("data", (data) => {
      const text = data.toString().trimEnd();
      this.outputChannel.appendLine(text);
      for (const line of text.split("\n")) {
        this.stderrTail.push(line);
      }
      if (this.stderrTail.length > STDERR_TAIL_LINES) {
        this.stderrTail.splice(0, this.stderrTail.length - STDERR_TAIL_LINES);
      }
    });
    this.proc.on("exit", (code, signal) => {
      this.exitCode = code;
      const msg = `Python process exited (code=${code}, signal=${signal})`;
      this.outputChannel.appendLine(msg);
      this.pending.failAll(new Error(msg));
      if (this.readyReject) {
        if (this.readyTimer) {
          clearTimeout(this.readyTimer);
          this.readyTimer = null;
        }
        this.readyReject(new Error(msg));
        this.readyResolve = null;
        this.readyReject = null;
      }
    });
    this.proc.on("error", (err) => {
      this.outputChannel.appendLine(`Python process error: ${err.message}`);
      if (this.readyReject) {
        if (this.readyTimer) {
          clearTimeout(this.readyTimer);
          this.readyTimer = null;
        }
        this.readyReject(err);
        this.readyResolve = null;
        this.readyReject = null;
      }
    });
  }
  /**
   * Wait until the child has closed its stdio, or `timeoutMs` elapses.
   *
   * The ready promise can reject (on 'exit', or on the inactivity timer)
   * while the last stderr chunk is still queued, so a diagnostic report must
   * wait for 'close' or it can quote an empty traceback.
   */
  whenClosed(timeoutMs = 2e3) {
    return Promise.race([
      this.closed,
      new Promise((resolve2) => setTimeout(resolve2, timeoutMs))
    ]);
  }
  /** Recent stderr from the child process (oldest first). */
  getStderr() {
    return this.stderrTail.join("\n");
  }
  /** Exit code, or null while the process is still running. */
  getExitCode() {
    return this.exitCode;
  }
  /** Why this process can no longer take a request, or null while it can. */
  deadReason() {
    if (this.exitCode !== null)
      return `it exited with code ${this.exitCode}`;
    if (this.proc.killed)
      return "it was stopped";
    if (!this.proc.stdin || this.proc.stdin.destroyed)
      return "its input stream is closed";
    return null;
  }
  /**
   * Wait for the Python server to signal readiness.
   * Returns the ready notification params (db_name, schema_keys).
   *
   * The ``timeoutMs`` is an *inactivity* timeout: it resets whenever a
   * ``progress`` notification arrives from the server. This lets slow-but-
   * progressing startups (e.g. projects on network drives) complete
   * without falsely timing out, while still killing a truly stuck server.
   */
  waitForReady(timeoutMs) {
    this.readyTimeoutMs = timeoutMs;
    return new Promise((resolve2, reject) => {
      this.readyResolve = resolve2;
      this.readyReject = reject;
      this.resetReadyTimer(timeoutMs);
    });
  }
  resetReadyTimer(timeoutMs) {
    if (this.readyTimer) {
      clearTimeout(this.readyTimer);
    }
    this.readyTimer = setTimeout(() => {
      this.readyTimer = null;
      if (this.readyReject) {
        this.readyReject(new Error(
          `Python server did not become ready within ${timeoutMs}ms of silence (no progress notification received).`
        ));
        this.readyResolve = null;
        this.readyReject = null;
      }
    }, timeoutMs);
  }
  /**
   * Send a JSON-RPC request and return a promise for the result.
   *
   * Every request carries a timeout. The server is supposed to answer every
   * request exactly once — long work is reported asynchronously through
   * run_output/run_done notifications, not by holding an RPC open — so a
   * response that never arrives means the server lost the request, and
   * without a timeout that wedges the caller permanently with no error
   * anywhere. (That is precisely how a MATLAB-locked database used to hang
   * the whole GUI; see scistack_gui/server.py::_handle_request.) The
   * timeout is a backstop, not a work limit: it is deliberately generous
   * and configurable via `scistack.rpcTimeoutMs`.
   */
  request(method, params) {
    const gone = this.deadReason();
    if (gone) {
      this.outputChannel.appendLine(`RPC refused: ${method} \u2014 ${gone}`);
      return Promise.reject(new Error(
        `SciStack: the Python server is not running (${gone}). Reopen the pipeline (or run "SciStack: Restart Python") and try again.`
      ));
    }
    const id = this.nextId++;
    const timeoutMs = vscode4.workspace.getConfiguration("scistack").get("rpcTimeoutMs", 3e5);
    const reply = this.pending.open(id, method, timeoutMs);
    const msg = JSON.stringify({ jsonrpc: "2.0", method, params, id });
    this.proc.stdin?.write(msg + "\n", (err) => {
      if (err) {
        this.outputChannel.appendLine(`RPC write failed: ${method} \u2014 ${err.message}`);
        this.pending.fail(id, new Error(
          `SciStack: could not send '${method}' to the Python server (${err.message}). It may have exited \u2014 check the SciStack output channel.`
        ));
      }
    });
    return reply;
  }
  /**
   * Register a handler for push notifications from Python.
   */
  onNotification(handler) {
    this.notificationHandlers.push(handler);
  }
  /**
   * Kill the Python process.
   */
  kill() {
    this.proc.kill();
  }
  handleLine(line) {
    let msg;
    try {
      msg = JSON.parse(line);
    } catch {
      this.outputChannel.appendLine(`[stdout non-JSON] ${line}`);
      return;
    }
    if ("id" in msg && msg.id !== null && msg.id !== void 0) {
      const id = msg.id;
      if ("error" in msg) {
        this.pending.reject(id, msg.error.message);
      } else {
        this.pending.resolve(id, msg.result);
      }
      return;
    }
    const method = msg.method;
    const params = msg.params ?? {};
    if (method === "progress") {
      this.outputChannel.appendLine(`  ${params.message}`);
      if (this.readyResolve) {
        this.resetReadyTimer(this.readyTimeoutMs);
      }
      return;
    }
    if (method === "ready" && this.readyResolve) {
      if (this.readyTimer) {
        clearTimeout(this.readyTimer);
        this.readyTimer = null;
      }
      this.readyResolve(params);
      this.readyResolve = null;
      this.readyReject = null;
      return;
    }
    if (method === "error") {
      this.outputChannel.appendLine(`Server error: ${params.message}`);
      if (this.readyReject) {
        if (this.readyTimer) {
          clearTimeout(this.readyTimer);
          this.readyTimer = null;
        }
        this.readyReject(new Error(params.message));
        this.readyResolve = null;
        this.readyReject = null;
      }
      return;
    }
    for (const handler of this.notificationHandlers) {
      handler(method, params);
    }
  }
};

// src/serverArgs.ts
function buildServerArgs({
  dbPath,
  schemaKeys,
  projectRoot,
  logFile
}) {
  const args = ["-m", "scistack_gui.server", "--db", dbPath];
  if (schemaKeys && schemaKeys.length > 0) {
    args.push("--schema-keys", schemaKeys.join(","));
  }
  if (projectRoot) {
    args.push("--project-root", projectRoot);
  }
  if (logFile) {
    args.push("--log-file", logFile);
  }
  return args;
}
function buildPlotOnlyServerArgs(options = {}) {
  const args = ["-m", "scistack_gui.server", "--plot-only"];
  if (options.logFile) {
    args.push("--log-file", options.logFile);
  }
  return args;
}

// src/startupDiagnostics.ts
var import_child_process2 = require("child_process");
var REQUIRED_MODULES = ["scistack_gui", "scidb", "scifor", "duckdb"];
var PROBE_SCRIPT = [
  "import json, sys",
  'info = {"executable": sys.executable, "version": sys.version.split()[0], "prefix": sys.prefix, "modules": {}}',
  "try:",
  "    import importlib.util as u",
  "except Exception:",
  "    u = None",
  `for name in (${REQUIRED_MODULES.map((m) => `"${m}"`).join(", ")}):`,
  "    entry = {}",
  "    try:",
  "        spec = u.find_spec(name) if u else None",
  "        if spec is None:",
  '            entry["found"] = False',
  "        else:",
  '            entry["found"] = True',
  '            entry["location"] = getattr(spec, "origin", None)',
  "    except Exception as e:",
  '        entry["found"] = False',
  '        entry["error"] = "%s: %s" % (type(e).__name__, e)',
  '    info["modules"][name] = entry',
  "print(json.dumps(info))"
].join("\n");
function probeInterpreter(pythonPath, timeoutMs = 1e4) {
  return new Promise((resolve2) => {
    let settled = false;
    const done = (probe) => {
      if (settled)
        return;
      settled = true;
      clearTimeout(timer);
      resolve2(probe);
    };
    let proc;
    try {
      proc = (0, import_child_process2.spawn)(pythonPath, ["-c", PROBE_SCRIPT], {
        stdio: ["ignore", "pipe", "pipe"]
      });
    } catch (err) {
      resolve2({ ok: false, spawnError: String(err) });
      return;
    }
    const timer = setTimeout(() => {
      proc.kill();
      done({ ok: false, spawnError: `probe timed out after ${timeoutMs}ms` });
    }, timeoutMs);
    let stdout = "";
    let stderr = "";
    proc.stdout?.on("data", (d) => stdout += d.toString());
    proc.stderr?.on("data", (d) => stderr += d.toString());
    proc.on("error", (err) => done({ ok: false, spawnError: err.message }));
    proc.on("close", () => {
      const parsed = parseProbeOutput(stdout);
      if (parsed) {
        done(parsed);
      } else {
        done({ ok: false, spawnError: "probe produced no JSON", raw: (stdout + stderr).trim() });
      }
    });
  });
}
function parseProbeOutput(stdout) {
  const lines = stdout.split("\n").map((l) => l.trim()).filter(Boolean);
  for (let i = lines.length - 1; i >= 0; i--) {
    if (!lines[i].startsWith("{"))
      continue;
    try {
      const info = JSON.parse(lines[i]);
      if (info.modules)
        return { ok: true, ...info };
    } catch {
    }
  }
  return null;
}
function missingModuleFromStderr(stderr) {
  const m = /No module named ['"]?([\w.]+)['"]?/.exec(stderr);
  return m ? m[1] : void 0;
}
function isUnrunnable(text) {
  return /ENOENT|not found|cannot find the (file|path) specified|No such file or directory|Microsoft Store/i.test(
    text
  );
}
function pipInstallCommand(pythonPath) {
  const quoted = /\s/.test(pythonPath) ? `"${pythonPath}"` : pythonPath;
  return `${quoted} -m pip install scistack-gui`;
}
function stderrTail(stderr, maxLines) {
  const lines = stderr.split("\n").map((l) => l.trimEnd()).filter((l) => l.trim());
  return lines.slice(-maxLines).join("\n");
}
function diagnoseStartupFailure(ctx) {
  const stderr = ctx.stderr ?? "";
  const probe = ctx.probe;
  const python = ctx.pythonPath;
  const runtimePath = probe?.executable && probe.executable !== python ? `${python} (resolved to ${probe.executable})` : python;
  const detailLines = [
    `Interpreter (configured): ${python}`
  ];
  if (ctx.interpreterSource)
    detailLines.push(`Interpreter source: ${ctx.interpreterSource}`);
  if (probe?.executable)
    detailLines.push(`Interpreter (sys.executable): ${probe.executable}`);
  if (probe?.prefix)
    detailLines.push(`Environment (sys.prefix): ${probe.prefix}`);
  if (probe?.version)
    detailLines.push(`Python version: ${probe.version}`);
  if (ctx.args)
    detailLines.push(`Command: ${python} ${ctx.args.join(" ")}`);
  if (ctx.exitCode !== void 0 && ctx.exitCode !== null) {
    detailLines.push(`Exit code: ${ctx.exitCode}`);
  }
  if (probe?.modules) {
    detailLines.push("Packages:");
    for (const name of REQUIRED_MODULES) {
      const mod = probe.modules[name];
      if (!mod)
        continue;
      const status = mod.found ? `found${mod.location ? ` at ${mod.location}` : ""}` : `NOT FOUND${mod.error ? ` (${mod.error})` : ""}`;
      detailLines.push(`  - ${name}: ${status}`);
    }
  }
  if (probe && !probe.ok && probe.spawnError) {
    detailLines.push(`Interpreter probe failed: ${probe.spawnError}`);
  }
  if (probe?.raw)
    detailLines.push(`Probe output: ${probe.raw}`);
  detailLines.push(`Failure: ${ctx.errorMessage}`);
  const tail = stderrTail(stderr, 20);
  if (tail)
    detailLines.push("Server stderr (tail):", tail);
  const detail = detailLines.join("\n");
  const finish = (kind, message, actions, installCommand) => ({ kind, message, detail, actions, installCommand });
  const unrunnable = isUnrunnable(ctx.errorMessage) || probe !== void 0 && !probe.ok && isUnrunnable(`${probe.spawnError ?? ""}
${probe.raw ?? ""}`);
  if (unrunnable) {
    return finish(
      "interpreter_missing",
      `SciStack: cannot run the Python interpreter "${python}". Set scistack.pythonPath, or pick an interpreter with the Python extension.`,
      ["selectInterpreter", "openSettings", "showOutput"]
    );
  }
  const stderrMissing = missingModuleFromStderr(stderr);
  const guiProbeMissing = probe?.modules?.scistack_gui?.found === false;
  if (guiProbeMissing || stderrMissing === "scistack_gui") {
    return finish(
      "package_missing",
      `SciStack: the Python environment "${runtimePath}" does not have scistack_gui installed. Install it there, or switch to the environment that has it.`,
      ["copyInstallCommand", "selectInterpreter", "showOutput"],
      pipInstallCommand(python)
    );
  }
  const depProbeMissing = REQUIRED_MODULES.filter(
    (m) => m !== "scistack_gui" && probe?.modules?.[m]?.found === false
  );
  const missingDep = depProbeMissing[0] ?? (stderrMissing && stderrMissing !== "scistack_gui" ? stderrMissing : void 0);
  if (missingDep) {
    return finish(
      "dependency_missing",
      `SciStack: the Python environment "${runtimePath}" has scistack_gui but is missing "${missingDep}", which the server imports at startup.`,
      ["copyInstallCommand", "selectInterpreter", "showOutput"],
      pipInstallCommand(python)
    );
  }
  if (/did not become ready/.test(ctx.errorMessage)) {
    return finish(
      "startup_timeout",
      `SciStack: the server on "${runtimePath}" started but never became ready. See the SciStack output for what it was doing.`,
      ["showOutput"]
    );
  }
  const excLine = /^\s*([\w.]*(?:Error|Exception|Exit|Interrupt)\b.*)$/m.exec(tail);
  if (excLine) {
    return finish(
      "server_error",
      `SciStack: the server on "${runtimePath}" crashed during startup \u2014 ${excLine[1].trim()}`,
      ["showOutput"]
    );
  }
  return finish(
    "unknown",
    `SciStack: the server on "${runtimePath}" failed to start \u2014 ${ctx.errorMessage}`,
    ["showOutput"]
  );
}

// src/session.ts
var DB_WATCH_DEBOUNCE_MS = 2e3;
var PLOT_ONLY_LABEL = "plot-only";
var Session = class _Session {
  constructor(manager, id, dbPath, label, projectRoot, log, debugPort, python) {
    this.manager = manager;
    this.id = id;
    this.dbPath = dbPath;
    this.label = label;
    this.projectRoot = projectRoot;
    this.log = log;
    this.debugPort = debugPort;
    this.python = python;
    /**
     * The plot tabs opened from this session.
     *
     * Per session, not static: a `plot_save_complete` from one database
     * delivered into another database's tab would re-enable the wrong Save
     * button. That is the same failure `panelRegistry.ts` documents, one level
     * up — a registry with nothing to scope it to.
     */
    this.plots = new PanelRegistry();
    this.disposed = false;
  }
  get isPlotOnly() {
    return this.dbPath === "";
  }
  /** Build a session around an already-ready server process. */
  static adopt(args) {
    return new _Session(
      args.manager,
      args.id,
      args.dbPath,
      args.label,
      args.projectRoot,
      args.log,
      args.debugPort,
      args.python
    );
  }
  /**
   * Deliver a push notification from this session's server to this
   * session's panels — and to nothing else.
   */
  route(method, params) {
    const plotPanels = this.plots.send({ method, params });
    if (method.startsWith("plot_save_")) {
      this.log.appendLine(
        `[notify] ${method} (job=${params.job_id}) \u2192 ${plotPanels} plot panel(s)`
      );
    }
    if (!this.dagPanel)
      return;
    this.dagPanel.postMessage({ method, params });
    if (method === "run_done") {
      this.dagPanel.stopDebugSession();
      this.dagPanel.matlabRuns.end(params.run_id);
    }
  }
  /**
   * Watch this session's `.duckdb` for external writes (MATLAB, another
   * tool) and refresh the canvas when they settle.
   */
  startDbWatcher() {
    if (this.isPlotOnly)
      return;
    this.stopDbWatcher();
    const pattern = new vscode5.RelativePattern(
      path5.dirname(this.dbPath),
      path5.basename(this.dbPath) + "*"
    );
    this.watcher = vscode5.workspace.createFileSystemWatcher(pattern);
    const onChange = () => this.onDbFileChanged();
    this.watcher.onDidChange(onChange);
    this.watcher.onDidCreate(onChange);
  }
  onDbFileChanged() {
    if (this.watchDebounce)
      clearTimeout(this.watchDebounce);
    this.watchDebounce = setTimeout(() => {
      this.watchDebounce = void 0;
      if (!this.dagPanel)
        return;
      if (!this.dagPanel.matlabRuns.noteDbChange()) {
        this.log.appendLine(
          "DuckDB file changed while MATLAB owns the database \u2014 deferring DAG refresh until the run finishes"
        );
        return;
      }
      this.log.appendLine("DuckDB file changed externally \u2014 refreshing DAG");
      this.dagPanel.postMessage({ method: "dag_updated", params: {} });
    }, DB_WATCH_DEBOUNCE_MS);
  }
  /** Emit the DAG refresh withheld while MATLAB owned the database. */
  flushDeferredDagRefresh() {
    if (!this.dagPanel)
      return;
    if (!this.dagPanel.matlabRuns.takeDeferredRefresh())
      return;
    this.log.appendLine("MATLAB run finished \u2014 applying the deferred DAG refresh");
    this.dagPanel.postMessage({ method: "dag_updated", params: {} });
  }
  stopDbWatcher() {
    this.watcher?.dispose();
    this.watcher = void 0;
    if (this.watchDebounce)
      clearTimeout(this.watchDebounce);
    this.watchDebounce = void 0;
  }
  /**
   * Replace the server process, keeping the panels.
   *
   * Panels read `session.python` at call time rather than holding their own
   * reference, so nothing has to be "rebound" — the class of bug where a
   * plot tab kept writing to a destroyed stdin (ERR_STREAM_DESTROYED,
   * 2026-09-15) cannot arise.
   */
  adoptProcess(python) {
    this.python = python;
    this.log.appendLine("server replaced \u2014 panels now talk to the new process");
  }
  /**
   * Stop this session's server and wait for it to let go of the database.
   *
   * A restart must do this BEFORE spawning the replacement. The server
   * drops the DuckDB file lock between requests, but it holds it for the
   * whole of `init_db` — so a new server started while the old one is
   * mid-request loses the race and dies on "Could not set lock on file".
   * `whenClosed` also gives the old process time to flush its last log
   * lines under its own prefix.
   */
  async stopProcess() {
    this.python.kill();
    await this.python.whenClosed();
  }
  /** Close everything this session owns. Idempotent. */
  dispose() {
    if (this.disposed)
      return;
    this.disposed = true;
    this.log.appendLine("session closing");
    this.stopDbWatcher();
    this.closePlots();
    this.dagPanel?.dispose();
    this.python.kill();
    this.manager.forget(this);
  }
  closePlots() {
    for (const panel of this.plots.sinks()) {
      panel.close();
    }
  }
};
var SessionManager = class {
  constructor(context, channel) {
    this.context = context;
    this.channel = channel;
    this.registry = new SessionRegistry();
    this.changeHandlers = [];
  }
  /** Called whenever the set of sessions, or the active one, changes. */
  onDidChange(handler) {
    this.changeHandlers.push(handler);
  }
  announceChange() {
    for (const handler of this.changeHandlers)
      handler();
  }
  get size() {
    return this.registry.size;
  }
  /** The open databases — what every command acts on. */
  all() {
    return this.registry.all();
  }
  /**
   * Every live server, the database-less CSV one included. For diagnostics
   * and for anything that must not collide across processes (debugpy ports).
   */
  everything() {
    return this.plotOnly ? [...this.registry.all(), this.plotOnly] : this.registry.all();
  }
  get active() {
    return this.registry.active;
  }
  resolve(explicitId) {
    return this.registry.resolve(explicitId);
  }
  /**
   * Resolve a session for a command, logging which rule decided.
   *
   * "The command went to the other database" and "the command did nothing"
   * are indistinguishable from the outside, so the rule is always recorded.
   */
  resolveForCommand(command, explicitId) {
    const { session, source, detail } = this.registry.resolve(explicitId);
    this.channel.appendLine(`[session] ${command} \u2192 ${source}: ${detail}`);
    return session;
  }
  setActive(id) {
    if (this.registry.active?.id === id)
      return;
    this.registry.setActive(id);
    this.channel.appendLine(`[session] active: ${this.registry.active?.label ?? "(none)"}`);
    this.announceChange();
  }
  /** Remove a disposed session. Called by `Session.dispose`, not directly. */
  forget(session) {
    if (this.plotOnly === session)
      this.plotOnly = void 0;
    this.registry.remove(session.id);
    this.channel.appendLine(
      `[session] closed ${session.label} \u2014 ${this.registry.size} still open`
    );
    this.announceChange();
  }
  disposeAll() {
    for (const session of this.registry.all())
      session.dispose();
    this.plotOnly?.dispose();
  }
  /**
   * Open a database, or reveal the session that already has it open.
   *
   * DuckDB is single-writer: a second server on the same file would lose the
   * lock race and report it as a mysterious `DatabaseLockedError`, so the
   * answer to "open this again" is "here it is".
   */
  async open(dbPath, schemaKeys) {
    const id = sessionIdForDb(dbPath);
    const existing = this.registry.get(id);
    if (existing) {
      this.channel.appendLine(
        `[session] ${existing.label} is already open \u2014 revealing its tab`
      );
      existing.dagPanel?.reveal();
      this.setActive(existing.id);
      return existing;
    }
    const label = sessionLabel(dbPath);
    const log = prefixedLog(this.channel, label);
    const folders = (vscode5.workspace.workspaceFolders ?? []).map((f) => f.uri.fsPath);
    const projectRoot = projectRootForDb(dbPath, folders);
    this.warnIfNoProjectRoot(projectRoot, label);
    const debugPort = this.allocateDebugPort();
    const args = buildServerArgs({ dbPath, schemaKeys, projectRoot });
    const python = await this.spawnReady(args, {
      log,
      label,
      cwd: projectRoot,
      debugPort,
      describe: [
        `  DB: ${dbPath}`,
        `  Project root: ${projectRoot ?? "(none \u2014 server will fall back, see above)"}`,
        ...schemaKeys ? [`  Schema keys: [${schemaKeys.join(", ")}] (new DB)`] : []
      ]
    });
    if (!python)
      return void 0;
    const session = Session.adopt({
      manager: this,
      id,
      dbPath,
      label,
      projectRoot,
      log,
      debugPort,
      python
    });
    this.registry.add(session);
    session.dagPanel = new DagPanel(this.context, session, log);
    session.dagPanel.onDidDispose(() => session.dispose());
    session.dagPanel.onDidChangeActive((active) => {
      if (active)
        this.setActive(session.id);
    });
    session.dagPanel.matlabRuns.onAllFinished(() => session.flushDeferredDagRefresh());
    python.onNotification((method, params) => session.route(method, params));
    session.startDbWatcher();
    this.channel.appendLine(
      `[session] opened ${label} (id=${id}, ${this.registry.size} open)`
    );
    this.announceChange();
    return session;
  }
  /**
   * The database-less session that serves CSV plot tabs.
   *
   * At most one, shared by every CSV tab and started on first use: a CSV
   * plot needs no project and no DuckDB (`plot_service` threads `csv_path`
   * through and every entry point is `db_connection(..., needed=not
   * csv_path)`), so there is nothing to keep separate between two of them.
   */
  async openPlotOnly() {
    if (this.plotOnly)
      return this.plotOnly;
    const log = prefixedLog(this.channel, PLOT_ONLY_LABEL);
    const debugPort = this.allocateDebugPort();
    const python = await this.spawnReady(
      buildPlotOnlyServerArgs({ logFile: this.plotOnlyLogFile() }),
      {
        log,
        label: PLOT_ONLY_LABEL,
        debugPort,
        describe: ["  No database \u2014 CSV plotting only"]
      }
    );
    if (!python)
      return void 0;
    const session = Session.adopt({
      manager: this,
      id: PLOT_ONLY_LABEL,
      dbPath: "",
      label: PLOT_ONLY_LABEL,
      projectRoot: void 0,
      log,
      debugPort,
      python
    });
    this.plotOnly = session;
    python.onNotification((method, params) => session.route(method, params));
    this.channel.appendLine("[session] opened the plot-only server (no database)");
    this.announceChange();
    return session;
  }
  /**
   * Where a plot-only server writes its log.
   *
   * It has no database, so `scidb.log`'s "beside the .duckdb" convention has
   * no anchor, and dropping a log into whichever folder the user's CSV
   * happens to live in would scatter logs through their data. The
   * extension's own storage is the one place that is neither.
   */
  plotOnlyLogFile() {
    const dir = this.context.logUri?.fsPath ?? this.context.globalStorageUri?.fsPath;
    return dir ? path5.join(dir, "scistack-plot-session.log") : void 0;
  }
  /** Restart a session's server in place, keeping its panels and tabs. */
  async restart(session) {
    await session.stopProcess();
    if (session.isPlotOnly) {
      const python2 = await this.spawnReady(
        buildPlotOnlyServerArgs({ logFile: this.plotOnlyLogFile() }),
        { log: session.log, label: session.label, debugPort: session.debugPort, describe: [] }
      );
      if (!python2)
        return this.reportDeadAfterRestart(session);
      session.adoptProcess(python2);
      python2.onNotification((method, params) => session.route(method, params));
      return true;
    }
    const args = buildServerArgs({
      dbPath: session.dbPath,
      projectRoot: session.projectRoot
    });
    const python = await this.spawnReady(args, {
      log: session.log,
      label: session.label,
      cwd: session.projectRoot,
      debugPort: session.debugPort,
      describe: [`  DB: ${session.dbPath}`]
    });
    if (!python)
      return this.reportDeadAfterRestart(session);
    session.adoptProcess(python);
    python.onNotification((method, params) => session.route(method, params));
    session.dagPanel?.reveal();
    session.dagPanel?.postMessage({ method: "dag_updated", params: {} });
    session.dagPanel?.postMessage({ method: "db_changed", params: {} });
    return true;
  }
  /**
   * The replacement server never started, and the old one is already
   * stopped. Say so: every later RPC from the still-open panels will fail,
   * and without this line that reads like a crash rather than a restart
   * that did not come back.
   */
  reportDeadAfterRestart(session) {
    const message = `SciStack: ${session.label} has no server after the restart. Close its tab and open the database again.`;
    session.log.appendLine(message);
    vscode5.window.showErrorMessage(message);
    return false;
  }
  /**
   * Spawn a server and wait for its `ready`, or report why it never came.
   *
   * Returns undefined on failure, having already told the user; the caller
   * simply does not create a session.
   */
  async spawnReady(args, opts) {
    const interpreter = await resolvePythonPath();
    if (!interpreter) {
      vscode5.window.showErrorMessage(
        "SciStack: Could not find a Python interpreter. Install the Python extension or set scistack.pythonPath in settings."
      );
      return void 0;
    }
    const { path: pythonPath, source: interpreterSource } = interpreter;
    opts.log.appendLine("Starting SciStack server...");
    opts.log.appendLine(`  Python: ${pythonPath} (from ${interpreterSource})`);
    for (const line of opts.describe)
      opts.log.appendLine(line);
    const python = new PythonProcess(pythonPath, args, opts.log, {
      cwd: opts.cwd,
      debugPort: opts.debugPort
    });
    try {
      const timeout = vscode5.workspace.getConfiguration("scistack").get("startupTimeoutMs", 6e4);
      const ready = await python.waitForReady(timeout);
      opts.log.appendLine(
        `Server ready \u2014 DB: ${ready.db_name}, schema: [${(ready.schema_keys ?? []).join(", ")}]`
      );
      return python;
    } catch (err) {
      python.kill();
      await reportStartupFailure(opts.log, this.channel, python, interpreterSource, err);
      return void 0;
    }
  }
  /**
   * A debugpy port no other session is using.
   *
   * `debugpy.listen` on a port another server already holds fails, and the
   * server treats that as a warning — so with one fixed port the second
   * session silently has no debugger at all.
   */
  allocateDebugPort() {
    const cfg = vscode5.workspace.getConfiguration("scistack");
    if (!cfg.get("debug", false))
      return void 0;
    const base = cfg.get("debugPort", 5678);
    const taken = new Set(this.everything().map((s) => s.debugPort));
    for (let offset = 0; offset < 64; offset++) {
      if (!taken.has(base + offset))
        return base + offset;
    }
    return void 0;
  }
  /**
   * Warn when a database has no project folder to discover code from.
   *
   * With no `--project-root` the server falls back to the extension host's
   * working directory (`config.resolve_project_root` rule 3), which is
   * almost never where the user's code lives — so discovery comes back
   * empty and the canvas is unexplainedly bare.
   */
  warnIfNoProjectRoot(projectRoot, label) {
    if (projectRoot)
      return;
    const message = `SciStack: no folder is open in this window, so there is no project to discover pipeline code from for ${label}. Open your project folder and run "SciStack: Open Pipeline" again, or add paths from the Paths popup.`;
    this.channel.appendLine(message);
    if (warnedNoWorkspaceFolder)
      return;
    warnedNoWorkspaceFolder = true;
    vscode5.window.showWarningMessage(message);
  }
};
var warnedNoWorkspaceFolder = false;
async function resolvePythonPath() {
  const config = vscode5.workspace.getConfiguration("scistack");
  const configured = config.get("pythonPath");
  if (configured)
    return { path: configured, source: "scistack.pythonPath setting" };
  const pythonExt = vscode5.extensions.getExtension("ms-python.python");
  if (pythonExt) {
    if (!pythonExt.isActive)
      await pythonExt.activate();
    const api = pythonExt.exports;
    if (api?.environments?.getActiveEnvironmentPath) {
      const envPath = api.environments.getActiveEnvironmentPath();
      if (envPath?.path) {
        return { path: envPath.path, source: "active interpreter from the Python extension" };
      }
    }
  }
  return { path: "python3", source: "PATH fallback (no Python extension interpreter)" };
}
async function reportStartupFailure(log, channel, failed, interpreterSource, err) {
  const errorMessage = err instanceof Error ? err.message : String(err);
  log.appendLine(`Server failed to start: ${errorMessage}`);
  log.appendLine(`Probing interpreter ${failed.pythonPath}...`);
  await failed.whenClosed();
  const probe = await probeInterpreter(failed.pythonPath);
  const diagnosis = diagnoseStartupFailure({
    pythonPath: failed.pythonPath,
    interpreterSource,
    args: failed.args,
    errorMessage,
    stderr: failed.getStderr(),
    exitCode: failed.getExitCode(),
    probe
  });
  log.appendLine("");
  log.appendLine(`=== SciStack startup failure (${diagnosis.kind}) ===`);
  log.appendLine(diagnosis.detail);
  if (diagnosis.installCommand) {
    log.appendLine(`Install with: ${diagnosis.installCommand}`);
  }
  log.appendLine("=== end of startup failure report ===");
  await showDiagnosisMessage(diagnosis, channel);
}
var ACTION_LABELS = {
  showOutput: "Show Details",
  selectInterpreter: "Select Interpreter",
  openSettings: "Open Settings",
  copyInstallCommand: "Copy Install Command"
};
async function showDiagnosisMessage(diagnosis, channel) {
  const labels = diagnosis.actions.map((a) => ACTION_LABELS[a]);
  const picked = await vscode5.window.showErrorMessage(diagnosis.message, ...labels);
  if (!picked)
    return;
  const action = diagnosis.actions.find((a) => ACTION_LABELS[a] === picked);
  switch (action) {
    case "showOutput":
      channel.show(true);
      break;
    case "selectInterpreter":
      await vscode5.commands.executeCommand("python.setInterpreter");
      break;
    case "openSettings":
      await vscode5.commands.executeCommand(
        "workbench.action.openSettings",
        "scistack.pythonPath"
      );
      break;
    case "copyInstallCommand":
      if (diagnosis.installCommand) {
        await vscode5.env.clipboard.writeText(diagnosis.installCommand);
        vscode5.window.showInformationMessage(
          `SciStack: copied to clipboard \u2014 ${diagnosis.installCommand}`
        );
      }
      break;
  }
}

// src/extension.ts
var sessions;
var outputChannel;
var statusItem = null;
function activate(context) {
  outputChannel = vscode6.window.createOutputChannel("SciStack");
  sessions = new SessionManager(context, outputChannel);
  sessions.onDidChange(updateStatusBar);
  const openPipeline = vscode6.commands.registerCommand(
    "scistack.openPipeline",
    async () => {
      const dbChoice = await vscode6.window.showQuickPick(
        ["Open existing database", "Create new database"],
        { placeHolder: "SciStack: Open or create a .duckdb file?" }
      );
      if (!dbChoice)
        return;
      let dbPath;
      let schemaKeys;
      if (dbChoice === "Open existing database") {
        const dbUris = await vscode6.window.showOpenDialog({
          canSelectFiles: true,
          canSelectFolders: false,
          canSelectMany: false,
          filters: { "DuckDB Database": ["duckdb"] },
          title: "Select SciStack Database",
          defaultUri: vscode6.workspace.workspaceFolders?.[0]?.uri
        });
        if (!dbUris || dbUris.length === 0)
          return;
        dbPath = dbUris[0].fsPath;
      } else {
        const folderUris = await vscode6.window.showOpenDialog({
          canSelectFiles: false,
          canSelectFolders: true,
          canSelectMany: false,
          title: "Select folder for new SciStack database",
          openLabel: "Select Folder",
          defaultUri: vscode6.workspace.workspaceFolders?.[0]?.uri
        });
        if (!folderUris || folderUris.length === 0)
          return;
        const folderPath = folderUris[0].fsPath;
        const nameInput = await vscode6.window.showInputBox({
          prompt: "Database filename",
          placeHolder: "e.g. my_pipeline.duckdb",
          validateInput: (v) => {
            const trimmed = v.trim();
            if (!trimmed)
              return "Provide a filename";
            if (trimmed.includes("/") || trimmed.includes("\\")) {
              return "Filename must not contain path separators";
            }
            return null;
          }
        });
        if (!nameInput)
          return;
        const fileName = nameInput.trim().endsWith(".duckdb") ? nameInput.trim() : `${nameInput.trim()}.duckdb`;
        dbPath = path6.join(folderPath, fileName);
        const keysInput = await vscode6.window.showInputBox({
          prompt: "Schema keys (comma-separated, top-down)",
          placeHolder: "e.g. subject, session",
          validateInput: (v) => {
            const parts = v.split(",").map((s) => s.trim()).filter(Boolean);
            return parts.length === 0 ? "Provide at least one schema key" : null;
          }
        });
        if (!keysInput)
          return;
        schemaKeys = keysInput.split(",").map((s) => s.trim()).filter(Boolean);
      }
      await sessions.open(dbPath, schemaKeys);
      updateStatusBar();
    }
  );
  const restartPython = vscode6.commands.registerCommand(
    "scistack.restartPython",
    async () => {
      const session = sessions.resolveForCommand("restartPython");
      if (!session) {
        vscode6.window.showWarningMessage(
          'SciStack: No pipeline has been opened yet \u2014 run "SciStack: Open Pipeline" first.'
        );
        return;
      }
      outputChannel.appendLine(`Restarting the Python process for ${session.label}...`);
      const ok = await sessions.restart(session);
      if (ok) {
        vscode6.window.showInformationMessage(
          `SciStack: Python process restarted for ${session.label}.`
        );
      }
    }
  );
  const switchSession = vscode6.commands.registerCommand(
    "scistack.switchSession",
    async () => {
      const open = sessions.all();
      if (open.length === 0) {
        vscode6.window.showInformationMessage(
          'SciStack: no database is open \u2014 run "SciStack: Open Pipeline".'
        );
        return;
      }
      const active = sessions.active;
      const picked = await vscode6.window.showQuickPick(
        open.map((s) => ({
          label: s === active ? `$(check) ${s.label}` : s.label,
          description: s.dbPath,
          detail: `project: ${s.projectRoot ?? "(none)"}`,
          session: s
        })),
        { placeHolder: "Switch to which SciStack database?" }
      );
      if (!picked)
        return;
      picked.session.dagPanel?.reveal();
      sessions.setActive(picked.session.id);
    }
  );
  const showSessions = vscode6.commands.registerCommand(
    "scistack.showSessions",
    () => {
      outputChannel.appendLine("");
      outputChannel.appendLine(
        `=== SciStack sessions (${sessions.size} database, ${sessions.everything().length - sessions.size} plot-only) ===`
      );
      const active = sessions.active;
      for (const s of sessions.everything()) {
        outputChannel.appendLine(
          `${s === active ? "*" : " "} ${s.label}  db=${s.dbPath || "(none \u2014 plot only)"}  project=${s.projectRoot ?? "(none)"}  debugPort=${s.debugPort ?? "(off)"}  canvas=${s.dagPanel ? "open" : "none"}  plotTabs=${s.plots.size}`
        );
      }
      outputChannel.appendLine("=== end of session list ===");
      outputChannel.show(true);
    }
  );
  const showMatlabRuns = vscode6.commands.registerCommand(
    "scistack.showMatlabRuns",
    async () => {
      const session = sessions.resolveForCommand("showMatlabRuns");
      if (!session) {
        vscode6.window.showInformationMessage(
          "SciStack: no database is open."
        );
        return;
      }
      try {
        const result = await session.python.request(
          "get_matlab_run_state",
          {}
        );
        outputChannel.appendLine("");
        outputChannel.appendLine(
          `=== MATLAB runs being watched by ${session.label} (${result.runs.length}) ===`
        );
        if (result.runs.length === 0) {
          outputChannel.appendLine("  (none \u2014 no terminal-dispatched run is in flight)");
        }
        for (const run of result.runs) {
          outputChannel.appendLine(`  ${JSON.stringify(run)}`);
        }
        outputChannel.appendLine("=== end of MATLAB run state ===");
        outputChannel.show(true);
      } catch (err) {
        vscode6.window.showErrorMessage(`SciStack: could not read MATLAB run state \u2014 ${err}`);
      }
    }
  );
  const openPlotPanel = vscode6.commands.registerCommand(
    "scistack.openPlotPanel",
    async (target = {}) => {
      const session = await sessionForPlot(target);
      if (!session)
        return;
      PlotPanel.show(context, session, target);
    }
  );
  const plotVariable = vscode6.commands.registerCommand(
    "scistack.plotVariable",
    async () => {
      const variable = await vscode6.window.showInputBox({
        prompt: "Variable type to plot",
        placeHolder: "e.g. StepLength"
      });
      if (!variable)
        return;
      await vscode6.commands.executeCommand("scistack.openPlotPanel", {
        variable: variable.trim()
      });
    }
  );
  const plotCsv = vscode6.commands.registerCommand(
    "scistack.plotCsv",
    async (uri) => {
      const target = uri?.fsPath ?? (await vscode6.window.showOpenDialog({
        canSelectMany: false,
        filters: { "CSV files": ["csv"] }
      }))?.[0]?.fsPath;
      if (!target)
        return;
      await vscode6.commands.executeCommand("scistack.openPlotPanel", {
        csvPath: target
      });
    }
  );
  context.subscriptions.push(
    openPipeline,
    restartPython,
    switchSession,
    showSessions,
    showMatlabRuns,
    openPlotPanel,
    plotVariable,
    plotCsv,
    outputChannel
  );
}
async function sessionForPlot(target) {
  if (target.csvPath && !target.sessionId) {
    return sessions.openPlotOnly();
  }
  const session = sessions.resolveForCommand("openPlotPanel", target.sessionId);
  if (!session) {
    vscode6.window.showWarningMessage(
      "SciStack: Open a pipeline first \u2014 plotting a variable needs its database."
    );
    return void 0;
  }
  return session;
}
function updateStatusBar() {
  const active = sessions.active ?? sessions.all()[0];
  if (!active) {
    statusItem?.dispose();
    statusItem = null;
    return;
  }
  if (!statusItem) {
    statusItem = vscode6.window.createStatusBarItem(vscode6.StatusBarAlignment.Left, 100);
    statusItem.command = "scistack.switchSession";
  }
  const others = sessions.size - 1;
  statusItem.text = `$(database) SciStack: ${active.label}` + (others > 0 ? ` (+${others})` : "");
  statusItem.tooltip = others > 0 ? `${active.dbPath}
Click to switch between ${others + 1} open databases` : active.dbPath;
  statusItem.show();
}
function deactivate() {
  sessions?.disposeAll();
  statusItem?.dispose();
  statusItem = null;
}
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  activate,
  deactivate
});
//# sourceMappingURL=extension.js.map
