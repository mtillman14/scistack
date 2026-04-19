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
var path4 = __toESM(require("path"));
var vscode5 = __toESM(require("vscode"));

// src/pythonProcess.ts
var import_child_process = require("child_process");
var readline = __toESM(require("readline"));
var vscode = __toESM(require("vscode"));
var PythonProcess = class {
  constructor(pythonPath, dbPath, modulePath, outputChannel2, schemaKeys, projectPath) {
    this.outputChannel = outputChannel2;
    this.nextId = 1;
    this.pending = /* @__PURE__ */ new Map();
    this.notificationHandlers = [];
    this.readyResolve = null;
    this.readyReject = null;
    this.readyTimer = null;
    this.readyTimeoutMs = 0;
    const args = ["-m", "scistack_gui.server", "--db", dbPath];
    if (projectPath) {
      args.push("--project", projectPath);
    } else if (modulePath) {
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
    return new Promise((resolve, reject) => {
      this.readyResolve = resolve;
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

// src/dagPanel.ts
var vscode3 = __toESM(require("vscode"));
var path2 = __toESM(require("path"));

// src/matlabTerminal.ts
var fs = __toESM(require("fs"));
var os = __toESM(require("os"));
var path = __toESM(require("path"));
var vscode2 = __toESM(require("vscode"));
function isMatlabExtensionAvailable() {
  return vscode2.extensions.getExtension("MathWorks.language-matlab") !== void 0;
}
async function runInMatlabTerminal(command, outputChannel2) {
  if (!isMatlabExtensionAvailable()) {
    return false;
  }
  try {
    const scriptPath = path.join(os.tmpdir(), "scistack_run.m");
    fs.writeFileSync(scriptPath, command, "utf-8");
    outputChannel2?.appendLine(
      `runInMatlabTerminal: wrote ${command.length}-char script to ${scriptPath}`
    );
    await vscode2.commands.executeCommand("matlab.openCommandWindow");
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
    return true;
  } catch (err) {
    outputChannel2?.appendLine(`Failed to send to MATLAB terminal: ${err}`);
    return false;
  }
}

// src/dagPanel.ts
var DEBUG_SESSION_NAME = "Attach to scistack-gui server";
var DagPanel = class {
  constructor(context, pythonProcess2, outputChannel2) {
    this.context = context;
    this.pythonProcess = pythonProcess2;
    this.outputChannel = outputChannel2;
    this.disposables = [];
    this.disposeCallbacks = [];
    this.panel = vscode3.window.createWebviewPanel(
      "scistack.dag",
      "SciStack Pipeline",
      vscode3.ViewColumn.One,
      {
        enableScripts: true,
        retainContextWhenHidden: true,
        localResourceRoots: [
          vscode3.Uri.file(path2.join(context.extensionPath, "dist", "webview"))
        ]
      }
    );
    this.panel.webview.html = this.getHtml();
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
          if (language === "matlab") {
            await this.handleMatlabRun(msg.id, params);
            return;
          }
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
    editor.revealRange(selection, vscode3.TextEditorRevealKind.InCenter);
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
   * Handle "Run" for a MATLAB function: generate command, then either send
   * to the MathWorks MATLAB terminal or copy to clipboard.
   */
  async handleMatlabRun(msgId, params) {
    const functionName = params.function_name;
    this.outputChannel.appendLine(
      `handleMatlabRun: requesting generate_matlab_command for ${functionName ?? "<?>"}`
    );
    try {
      const result = await this.pythonProcess.request(
        "generate_matlab_command",
        params
      );
      const command = result.command;
      this.outputChannel.appendLine(
        `handleMatlabRun: got command (${command.length} chars)`
      );
      const sent = await runInMatlabTerminal(command, this.outputChannel);
      if (sent) {
        this.outputChannel.appendLine(
          "handleMatlabRun: sent to MATLAB terminal"
        );
        vscode3.window.showInformationMessage("Running in MATLAB terminal...");
      } else {
        await vscode3.env.clipboard.writeText(command);
        this.outputChannel.appendLine(
          "handleMatlabRun: no MATLAB terminal found, copied to clipboard"
        );
        vscode3.window.showInformationMessage(
          "MATLAB command copied to clipboard. Paste into MATLAB to run."
        );
      }
      this.panel.webview.postMessage({ id: msgId, result: { ok: true } });
      const runId = params.run_id;
      if (runId) {
        this.panel.webview.postMessage({
          method: "run_done",
          params: {
            run_id: runId,
            success: true,
            duration_ms: 0,
            cancelled: false
          }
        });
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.outputChannel.appendLine(`handleMatlabRun: failed: ${msg}`);
      this.panel.webview.postMessage({
        id: msgId,
        error: { message: String(err) }
      });
      const runId = params.run_id;
      if (runId) {
        this.panel.webview.postMessage({
          method: "run_done",
          params: {
            run_id: runId,
            success: false,
            duration_ms: 0,
            cancelled: false
          }
        });
      }
    }
  }
  /**
   * Update the PythonProcess reference after a restart, so requests from the
   * webview are routed to the new process instead of the killed one.
   */
  updatePythonProcess(proc) {
    this.pythonProcess = proc;
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
    const port = cfg.get("debugPort", 5678);
    const folder = vscode3.workspace.workspaceFolders?.[0];
    const started = await vscode3.debug.startDebugging(folder, {
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
    if (active && active.name === DEBUG_SESSION_NAME)
      return active;
    return void 0;
  }
  /**
   * Reveal the panel if it's hidden.
   */
  reveal() {
    this.panel.reveal(vscode3.ViewColumn.One);
  }
  /**
   * Register a callback for when the panel is disposed.
   */
  onDidDispose(callback) {
    this.disposeCallbacks.push(callback);
  }
  getHtml() {
    const webviewDir = path2.join(this.context.extensionPath, "dist", "webview");
    const webview = this.panel.webview;
    const scriptUri = webview.asWebviewUri(
      vscode3.Uri.file(path2.join(webviewDir, "index.js"))
    );
    const styleUri = webview.asWebviewUri(
      vscode3.Uri.file(path2.join(webviewDir, "index.css"))
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

// src/projectInit.ts
var fs2 = __toESM(require("fs"));
var path3 = __toESM(require("path"));
var vscode4 = __toESM(require("vscode"));
function checkProjectConfig(dirPath) {
  const resolved = fs2.statSync(dirPath, { throwIfNoEntry: false });
  if (!resolved) {
    return "ready";
  }
  const dir = resolved.isFile() ? path3.dirname(dirPath) : dirPath;
  if (fs2.existsSync(path3.join(dir, "pyproject.toml")) || fs2.existsSync(path3.join(dir, "scistack.toml"))) {
    return "ready";
  }
  return "no_config_file";
}
function createScistackToml(dirPath) {
  const filePath = path3.join(dirPath, "scistack.toml");
  const content = `# SciStack project configuration
# See documentation for all available options.

# Python pipeline modules (relative paths)
# modules = ["pipelines/my_pipeline.py"]

# Pip-installed packages to scan for pipeline functions
# packages = ["my_scistack_plugin"]

# Auto-discover scistack.plugins entry points (default: true)
# auto_discover = true

# File where 'create_variable' writes new variable classes
# variable_file = "src/vars.py"

# [matlab]
# functions = ["src/"]
# variables = ["src/vars/"]
# variable_dir = "src/vars/"
`;
  fs2.writeFileSync(filePath, content, "utf-8");
  return filePath;
}
async function promptForMissingConfig(dirPath, outputChannel2) {
  const createOption = "Create scistack.toml";
  const continueOption = "Continue anyway";
  const choice = await vscode4.window.showWarningMessage(
    `No pyproject.toml or scistack.toml found in "${path3.basename(
      dirPath
    )}". The server needs a config file to discover pipeline code.`,
    { modal: true },
    createOption,
    continueOption
  );
  if (choice === createOption) {
    const filePath = createScistackToml(dirPath);
    outputChannel2.appendLine(`Created ${filePath}`);
    const doc = await vscode4.workspace.openTextDocument(filePath);
    await vscode4.window.showTextDocument(doc);
    return dirPath;
  }
  if (choice === continueOption) {
    outputChannel2.appendLine(
      "Continuing without config file \u2014 server will use defaults if possible."
    );
    return dirPath;
  }
  return void 0;
}

// src/extension.ts
var pythonProcess = null;
var dagPanel = null;
var outputChannel;
var dbWatcher = null;
var dbWatcherDebounce = null;
var lastStartArgs = null;
function activate(context) {
  outputChannel = vscode5.window.createOutputChannel("SciStack");
  const openPipeline = vscode5.commands.registerCommand(
    "scistack.openPipeline",
    async () => {
      const dbChoice = await vscode5.window.showQuickPick(
        ["Open existing database", "Create new database"],
        { placeHolder: "SciStack: Open or create a .duckdb file?" }
      );
      if (!dbChoice)
        return;
      let dbPath;
      let schemaKeys;
      if (dbChoice === "Open existing database") {
        const dbUris = await vscode5.window.showOpenDialog({
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
        const folderUris = await vscode5.window.showOpenDialog({
          canSelectFiles: false,
          canSelectFolders: true,
          canSelectMany: false,
          title: "Select folder for new SciStack database",
          openLabel: "Select Folder"
        });
        if (!folderUris || folderUris.length === 0)
          return;
        const folderPath = folderUris[0].fsPath;
        const nameInput = await vscode5.window.showInputBox({
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
        dbPath = path4.join(folderPath, fileName);
        const keysInput = await vscode5.window.showInputBox({
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
      const sourceChoice = await vscode5.window.showQuickPick(
        [
          "Select a project (pyproject.toml)",
          "Select a single pipeline module (.py)",
          "No module"
        ],
        { placeHolder: "How should SciStack discover your pipeline code?" }
      );
      if (!sourceChoice)
        return;
      let modulePath;
      let projectPath;
      if (sourceChoice === "Select a project (pyproject.toml)") {
        const projectUris = await vscode5.window.showOpenDialog({
          canSelectFiles: true,
          canSelectFolders: true,
          canSelectMany: false,
          filters: { "TOML": ["toml"] },
          title: "Select pyproject.toml or project directory"
        });
        if (projectUris && projectUris.length > 0) {
          const selectedPath = projectUris[0].fsPath;
          const configStatus = checkProjectConfig(selectedPath);
          if (configStatus === "no_config_file") {
            const result = await promptForMissingConfig(selectedPath, outputChannel);
            if (result === void 0)
              return;
            projectPath = result;
          } else {
            projectPath = selectedPath;
          }
        }
      } else if (sourceChoice === "Select a single pipeline module (.py)") {
        const moduleUris = await vscode5.window.showOpenDialog({
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
      await startPipeline(context, dbPath, modulePath, projectPath, schemaKeys);
    }
  );
  const restartPython = vscode5.commands.registerCommand(
    "scistack.restartPython",
    async () => {
      if (!lastStartArgs) {
        vscode5.window.showWarningMessage(
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
          lastStartArgs.projectPath,
          // Don't re-pass schemaKeys: the DB already exists on restart.
          void 0
        );
        vscode5.window.showInformationMessage("SciStack: Python process restarted.");
      } catch (err) {
        vscode5.window.showErrorMessage(`SciStack: Restart failed \u2014 ${err}`);
      }
    }
  );
  context.subscriptions.push(openPipeline, restartPython, outputChannel);
}
async function startPipeline(context, dbPath, modulePath, projectPath, schemaKeys) {
  lastStartArgs = {
    dbPath,
    modulePath,
    projectPath,
    schemaKeys: schemaKeys ?? lastStartArgs?.schemaKeys
  };
  if (pythonProcess) {
    pythonProcess.kill();
    pythonProcess = null;
  }
  const pythonPath = await resolvePythonPath();
  if (!pythonPath) {
    vscode5.window.showErrorMessage(
      "SciStack: Could not find a Python interpreter. Install the Python extension or set scistack.pythonPath in settings."
    );
    return;
  }
  outputChannel.appendLine(`Starting SciStack server...`);
  outputChannel.appendLine(`  Python: ${pythonPath}`);
  outputChannel.appendLine(`  DB: ${dbPath}`);
  if (projectPath)
    outputChannel.appendLine(`  Project: ${projectPath}`);
  if (modulePath)
    outputChannel.appendLine(`  Module: ${modulePath}`);
  if (schemaKeys)
    outputChannel.appendLine(`  Schema keys: [${schemaKeys.join(", ")}] (new DB)`);
  pythonProcess = new PythonProcess(pythonPath, dbPath, modulePath, outputChannel, schemaKeys, projectPath);
  try {
    const cfg = vscode5.workspace.getConfiguration("scistack");
    const startupTimeoutMs = cfg.get("startupTimeoutMs", 6e4);
    const readyParams = await pythonProcess.waitForReady(startupTimeoutMs);
    outputChannel.appendLine(
      `Server ready \u2014 DB: ${readyParams.db_name}, schema: [${readyParams.schema_keys.join(", ")}]`
    );
  } catch (err) {
    vscode5.window.showErrorMessage(`SciStack: Server failed to start \u2014 ${err}`);
    pythonProcess.kill();
    pythonProcess = null;
    return;
  }
  if (dagPanel) {
    dagPanel.updatePythonProcess(pythonProcess);
    dagPanel.reveal();
    dagPanel.postMessage({ method: "dag_updated", params: {} });
  } else {
    dagPanel = new DagPanel(context, pythonProcess, outputChannel);
    dagPanel.onDidDispose(() => {
      dagPanel = null;
      if (pythonProcess) {
        pythonProcess.kill();
        pythonProcess = null;
      }
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
  setupDbWatcher(dbPath);
  const statusItem = vscode5.window.createStatusBarItem(
    vscode5.StatusBarAlignment.Left,
    100
  );
  statusItem.text = `$(database) SciStack: ${dbPath.split("/").pop()}`;
  statusItem.tooltip = dbPath;
  statusItem.show();
}
async function resolvePythonPath() {
  const config = vscode5.workspace.getConfiguration("scistack");
  const configured = config.get("pythonPath");
  if (configured)
    return configured;
  const pythonExt = vscode5.extensions.getExtension("ms-python.python");
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
function setupDbWatcher(dbPath) {
  if (dbWatcher) {
    dbWatcher.dispose();
    dbWatcher = null;
  }
  if (dbWatcherDebounce) {
    clearTimeout(dbWatcherDebounce);
    dbWatcherDebounce = null;
  }
  const dbDir = path4.dirname(dbPath);
  const dbBase = path4.basename(dbPath);
  const pattern = new vscode5.RelativePattern(dbDir, dbBase + "*");
  dbWatcher = vscode5.workspace.createFileSystemWatcher(pattern);
  const onDbChange = () => {
    if (dbWatcherDebounce) {
      clearTimeout(dbWatcherDebounce);
    }
    dbWatcherDebounce = setTimeout(() => {
      dbWatcherDebounce = null;
      if (dagPanel) {
        outputChannel.appendLine("DuckDB file changed externally \u2014 refreshing DAG");
        dagPanel.postMessage({ method: "dag_updated", params: {} });
      }
    }, 2e3);
  };
  dbWatcher.onDidChange(onDbChange);
  dbWatcher.onDidCreate(onDbChange);
}
function deactivate() {
  if (dbWatcher) {
    dbWatcher.dispose();
    dbWatcher = null;
  }
  if (dbWatcherDebounce) {
    clearTimeout(dbWatcherDebounce);
    dbWatcherDebounce = null;
  }
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
