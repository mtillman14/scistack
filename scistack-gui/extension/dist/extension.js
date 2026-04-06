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
var vscode3 = __toESM(require("vscode"));

// src/pythonProcess.ts
var import_child_process = require("child_process");
var readline = __toESM(require("readline"));
var vscode = __toESM(require("vscode"));
var PythonProcess = class {
  constructor(pythonPath, dbPath, modulePath, outputChannel2, schemaKeys) {
    this.outputChannel = outputChannel2;
    this.nextId = 1;
    this.pending = /* @__PURE__ */ new Map();
    this.notificationHandlers = [];
    this.readyResolve = null;
    this.readyReject = null;
    const args = ["-m", "scistack_gui.server", "--db", dbPath];
    if (modulePath) {
      args.push("--module", modulePath);
    }
    if (schemaKeys && schemaKeys.length > 0) {
      args.push("--schema-keys", schemaKeys.join(","));
    }
    this.outputChannel.appendLine(`Spawning: ${pythonPath} ${args.join(" ")}`);
    const cfg = vscode.workspace.getConfiguration("scistack");
    const debugEnabled = cfg.get("debug", false);
    const debugPort = cfg.get("debugPort", 5678);
    const childEnv = { ...process.env };
    if (debugEnabled) {
      childEnv.SCISTACK_GUI_DEBUG = "1";
      childEnv.SCISTACK_GUI_DEBUG_PORT = String(debugPort);
      this.outputChannel.appendLine(
        `debugpy listener will start on 127.0.0.1:${debugPort} (attach via "Attach to scistack-gui server" launch config)`
      );
    }
    this.proc = (0, import_child_process.spawn)(pythonPath, args, {
      stdio: ["pipe", "pipe", "pipe"],
      env: childEnv
    });
    const rl = readline.createInterface({ input: this.proc.stdout });
    rl.on("line", (line) => this.handleLine(line));
    this.proc.stderr?.on("data", (data) => {
      this.outputChannel.appendLine(data.toString().trimEnd());
    });
    this.proc.on("exit", (code, signal) => {
      const msg = `Python process exited (code=${code}, signal=${signal})`;
      this.outputChannel.appendLine(msg);
      for (const [, pending] of this.pending) {
        pending.reject(new Error(msg));
      }
      this.pending.clear();
      if (this.readyReject) {
        this.readyReject(new Error(msg));
        this.readyResolve = null;
        this.readyReject = null;
      }
    });
    this.proc.on("error", (err) => {
      this.outputChannel.appendLine(`Python process error: ${err.message}`);
      if (this.readyReject) {
        this.readyReject(err);
        this.readyResolve = null;
        this.readyReject = null;
      }
    });
  }
  /**
   * Wait for the Python server to signal readiness.
   * Returns the ready notification params (db_name, schema_keys).
   */
  waitForReady(timeoutMs) {
    return new Promise((resolve, reject) => {
      this.readyResolve = resolve;
      this.readyReject = reject;
      setTimeout(() => {
        if (this.readyReject) {
          this.readyReject(new Error(`Python server did not become ready within ${timeoutMs}ms`));
          this.readyResolve = null;
          this.readyReject = null;
        }
      }, timeoutMs);
    });
  }
  /**
   * Send a JSON-RPC request and return a promise for the result.
   */
  request(method, params) {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      const msg = JSON.stringify({ jsonrpc: "2.0", method, params, id });
      this.proc.stdin?.write(msg + "\n", (err) => {
        if (err) {
          this.pending.delete(id);
          reject(err);
        }
      });
    });
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
      const pending = this.pending.get(id);
      if (pending) {
        this.pending.delete(id);
        if ("error" in msg) {
          const err = msg.error;
          pending.reject(new Error(err.message));
        } else {
          pending.resolve(msg.result);
        }
      }
      return;
    }
    const method = msg.method;
    const params = msg.params ?? {};
    if (method === "ready" && this.readyResolve) {
      this.readyResolve(params);
      this.readyResolve = null;
      this.readyReject = null;
      return;
    }
    if (method === "error") {
      this.outputChannel.appendLine(`Server error: ${params.message}`);
      if (this.readyReject) {
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

// src/dagPanel.ts
var vscode2 = __toESM(require("vscode"));
var path = __toESM(require("path"));
var DEBUG_SESSION_NAME = "Attach to scistack-gui server";
var DagPanel = class {
  constructor(context, pythonProcess2, outputChannel2) {
    this.context = context;
    this.pythonProcess = pythonProcess2;
    this.outputChannel = outputChannel2;
    this.disposables = [];
    this.disposeCallbacks = [];
    this.panel = vscode2.window.createWebviewPanel(
      "scistack.dag",
      "SciStack Pipeline",
      vscode2.ViewColumn.One,
      {
        enableScripts: true,
        retainContextWhenHidden: true,
        localResourceRoots: [
          vscode2.Uri.file(path.join(context.extensionPath, "dist", "webview"))
        ]
      }
    );
    this.panel.webview.html = this.getHtml();
    this.panel.webview.onDidReceiveMessage(
      async (msg) => {
        const method = msg.method;
        if (method === "restart_python") {
          try {
            await vscode2.commands.executeCommand("scistack.restartPython");
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
          await this.ensureDebugAttached();
        }
        try {
          const result = await this.pythonProcess.request(
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
   * Open a file in an editor column beside the DAG panel and reveal the given line.
   * `line` is 1-based (matching inspect.getsourcelines).
   */
  async revealInEditor(params) {
    const { file, line } = params;
    this.outputChannel.appendLine(`reveal_in_editor: file=${file} line=${line}`);
    if (!file)
      return { ok: false, error: "No file path provided." };
    const uri = vscode2.Uri.file(file);
    const doc = await vscode2.workspace.openTextDocument(uri);
    const zeroBased = Math.max(0, (line ?? 1) - 1);
    const selection = new vscode2.Range(zeroBased, 0, zeroBased, 0);
    const editor = await vscode2.window.showTextDocument(doc, {
      viewColumn: vscode2.ViewColumn.Beside,
      preserveFocus: false,
      selection
    });
    editor.revealRange(selection, vscode2.TextEditorRevealKind.InCenter);
    return { ok: true };
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
    const cfg = vscode2.workspace.getConfiguration("scistack");
    if (!cfg.get("debug", false))
      return;
    if (this.debugSession)
      return;
    const existing = this.findExistingDebugSession();
    if (existing) {
      this.debugSession = existing;
      return;
    }
    const port = cfg.get("debugPort", 5678);
    const folder = vscode2.workspace.workspaceFolders?.[0];
    const started = await vscode2.debug.startDebugging(folder, {
      name: DEBUG_SESSION_NAME,
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
    this.debugSession = vscode2.debug.activeDebugSession ?? this.findExistingDebugSession();
  }
  /**
   * Detach the debug session (called when run_done arrives).
   */
  async stopDebugSession() {
    const session = this.debugSession ?? this.findExistingDebugSession();
    this.debugSession = void 0;
    if (session) {
      await vscode2.debug.stopDebugging(session);
    }
  }
  findExistingDebugSession() {
    const active = vscode2.debug.activeDebugSession;
    if (active && active.name === DEBUG_SESSION_NAME)
      return active;
    return void 0;
  }
  /**
   * Reveal the panel if it's hidden.
   */
  reveal() {
    this.panel.reveal(vscode2.ViewColumn.One);
  }
  /**
   * Register a callback for when the panel is disposed.
   */
  onDidDispose(callback) {
    this.disposeCallbacks.push(callback);
  }
  getHtml() {
    const webviewDir = path.join(this.context.extensionPath, "dist", "webview");
    const webview = this.panel.webview;
    const scriptUri = webview.asWebviewUri(
      vscode2.Uri.file(path.join(webviewDir, "index.js"))
    );
    const styleUri = webview.asWebviewUri(
      vscode2.Uri.file(path.join(webviewDir, "index.css"))
    );
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
};
function getNonce() {
  let text = "";
  const possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  for (let i = 0; i < 32; i++) {
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  }
  return text;
}

// src/extension.ts
var pythonProcess = null;
var dagPanel = null;
var outputChannel;
var lastStartArgs = null;
function activate(context) {
  outputChannel = vscode3.window.createOutputChannel("SciStack");
  const openPipeline = vscode3.commands.registerCommand(
    "scistack.openPipeline",
    async () => {
      const dbChoice = await vscode3.window.showQuickPick(
        ["Open existing database", "Create new database"],
        { placeHolder: "SciStack: Open or create a .duckdb file?" }
      );
      if (!dbChoice)
        return;
      let dbPath;
      let schemaKeys;
      if (dbChoice === "Open existing database") {
        const dbUris = await vscode3.window.showOpenDialog({
          canSelectFiles: true,
          canSelectFolders: false,
          canSelectMany: false,
          filters: { "DuckDB Database": ["duckdb"] },
          title: "Select SciStack Database"
        });
        if (!dbUris || dbUris.length === 0)
          return;
        dbPath = dbUris[0].fsPath;
      } else {
        const dbUri = await vscode3.window.showSaveDialog({
          filters: { "DuckDB Database": ["duckdb"] },
          title: "Create SciStack Database",
          saveLabel: "Create"
        });
        if (!dbUri)
          return;
        dbPath = dbUri.fsPath;
        const keysInput = await vscode3.window.showInputBox({
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
      const moduleChoice = await vscode3.window.showQuickPick(
        ["Select a pipeline module (.py)", "No module"],
        { placeHolder: "Do you have a pipeline .py file to load?" }
      );
      let modulePath;
      if (moduleChoice === "Select a pipeline module (.py)") {
        const moduleUris = await vscode3.window.showOpenDialog({
          canSelectFiles: true,
          canSelectFolders: false,
          canSelectMany: false,
          filters: { "Python": ["py"] },
          title: "Select Pipeline Module"
        });
        if (moduleUris && moduleUris.length > 0) {
          modulePath = moduleUris[0].fsPath;
        }
      }
      await startPipeline(context, dbPath, modulePath, schemaKeys);
    }
  );
  const restartPython = vscode3.commands.registerCommand(
    "scistack.restartPython",
    async () => {
      if (!lastStartArgs) {
        vscode3.window.showWarningMessage(
          'SciStack: No pipeline has been opened yet \u2014 run "SciStack: Open Pipeline" first.'
        );
        return;
      }
      outputChannel.appendLine("Restarting Python process...");
      try {
        await startPipeline(
          context,
          lastStartArgs.dbPath,
          lastStartArgs.modulePath,
          // Don't re-pass schemaKeys: the DB already exists on restart.
          void 0
        );
        vscode3.window.showInformationMessage("SciStack: Python process restarted.");
      } catch (err) {
        vscode3.window.showErrorMessage(`SciStack: Restart failed \u2014 ${err}`);
      }
    }
  );
  context.subscriptions.push(openPipeline, restartPython, outputChannel);
}
async function startPipeline(context, dbPath, modulePath, schemaKeys) {
  lastStartArgs = {
    dbPath,
    modulePath,
    schemaKeys: schemaKeys ?? lastStartArgs?.schemaKeys
  };
  if (pythonProcess) {
    pythonProcess.kill();
    pythonProcess = null;
  }
  const pythonPath = await resolvePythonPath();
  if (!pythonPath) {
    vscode3.window.showErrorMessage(
      "SciStack: Could not find a Python interpreter. Install the Python extension or set scistack.pythonPath in settings."
    );
    return;
  }
  outputChannel.appendLine(`Starting SciStack server...`);
  outputChannel.appendLine(`  Python: ${pythonPath}`);
  outputChannel.appendLine(`  DB: ${dbPath}`);
  if (modulePath)
    outputChannel.appendLine(`  Module: ${modulePath}`);
  if (schemaKeys)
    outputChannel.appendLine(`  Schema keys: [${schemaKeys.join(", ")}] (new DB)`);
  pythonProcess = new PythonProcess(pythonPath, dbPath, modulePath, outputChannel, schemaKeys);
  try {
    const readyParams = await pythonProcess.waitForReady(1e4);
    outputChannel.appendLine(
      `Server ready \u2014 DB: ${readyParams.db_name}, schema: [${readyParams.schema_keys.join(", ")}]`
    );
  } catch (err) {
    vscode3.window.showErrorMessage(`SciStack: Server failed to start \u2014 ${err}`);
    pythonProcess.kill();
    pythonProcess = null;
    return;
  }
  if (dagPanel) {
    dagPanel.reveal();
  } else {
    dagPanel = new DagPanel(context, pythonProcess, outputChannel);
    dagPanel.onDidDispose(() => {
      dagPanel = null;
    });
  }
  pythonProcess.onNotification((method, params) => {
    if (dagPanel) {
      dagPanel.postMessage({ method, params });
      if (method === "run_done") {
        dagPanel.stopDebugSession();
      }
    }
  });
  const statusItem = vscode3.window.createStatusBarItem(
    vscode3.StatusBarAlignment.Left,
    100
  );
  statusItem.text = `$(database) SciStack: ${dbPath.split("/").pop()}`;
  statusItem.tooltip = dbPath;
  statusItem.show();
}
async function resolvePythonPath() {
  const config = vscode3.workspace.getConfiguration("scistack");
  const configured = config.get("pythonPath");
  if (configured)
    return configured;
  const pythonExt = vscode3.extensions.getExtension("ms-python.python");
  if (pythonExt) {
    if (!pythonExt.isActive)
      await pythonExt.activate();
    const api = pythonExt.exports;
    if (api?.environments?.getActiveEnvironmentPath) {
      const envPath = api.environments.getActiveEnvironmentPath();
      if (envPath?.path)
        return envPath.path;
    }
  }
  return "python3";
}
function deactivate() {
  if (pythonProcess) {
    pythonProcess.kill();
    pythonProcess = null;
  }
}
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  activate,
  deactivate
});
//# sourceMappingURL=extension.js.map
