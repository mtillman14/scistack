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

// src/serverArgs.ts
function buildServerArgs({
  dbPath,
  schemaKeys,
  projectRoot
}) {
  const args = ["-m", "scistack_gui.server", "--db", dbPath];
  if (schemaKeys && schemaKeys.length > 0) {
    args.push("--schema-keys", schemaKeys.join(","));
  }
  if (projectRoot) {
    args.push("--project-root", projectRoot);
  }
  return args;
}

// src/pythonProcess.ts
var STDERR_TAIL_LINES = 200;
var PythonProcess = class {
  constructor(pythonPath, dbPath, outputChannel2, schemaKeys) {
    this.pythonPath = pythonPath;
    this.outputChannel = outputChannel2;
    this.nextId = 1;
    this.pending = /* @__PURE__ */ new Map();
    this.notificationHandlers = [];
    this.readyResolve = null;
    this.readyReject = null;
    this.readyTimer = null;
    this.readyTimeoutMs = 0;
    /** Ring of recent stderr lines, so a failed start can report why. */
    this.stderrTail = [];
    this.exitCode = null;
    const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
    const args = buildServerArgs({ dbPath, schemaKeys, projectRoot: workspaceFolder });
    this.args = args;
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
      env: childEnv,
      cwd: workspaceFolder
    });
    this.closed = new Promise((resolve) => {
      this.proc.on("close", () => resolve());
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
   * Wait until the child has closed its stdio, or `timeoutMs` elapses.
   *
   * The ready promise can reject (on 'exit', or on the inactivity timer)
   * while the last stderr chunk is still queued, so a diagnostic report must
   * wait for 'close' or it can quote an empty traceback.
   */
  whenClosed(timeoutMs = 2e3) {
    return Promise.race([
      this.closed,
      new Promise((resolve) => setTimeout(resolve, timeoutMs))
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
    const id = this.nextId++;
    const timeoutMs = vscode.workspace.getConfiguration("scistack").get("rpcTimeoutMs", 3e5);
    return new Promise((resolve, reject) => {
      const settle = (fn) => {
        const pending = this.pending.get(id);
        if (pending?.timer)
          clearTimeout(pending.timer);
        this.pending.delete(id);
        fn();
      };
      const timer = timeoutMs > 0 ? setTimeout(() => {
        const pending = this.pending.get(id);
        if (!pending)
          return;
        const elapsed = Date.now() - pending.startedAt;
        this.outputChannel.appendLine(
          `RPC timeout: ${method} (id=${id}) got no response in ${elapsed}ms. The Python server may have dropped the request \u2014 check the stderr above for a traceback.`
        );
        settle(() => reject(new Error(
          `SciStack: no response from the Python server for '${method}' after ${Math.round(elapsed / 1e3)}s.`
        )));
      }, timeoutMs) : null;
      this.pending.set(id, {
        resolve: (value) => settle(() => resolve(value)),
        reject: (reason) => settle(() => reject(reason)),
        method,
        startedAt: Date.now(),
        timer
      });
      const msg = JSON.stringify({ jsonrpc: "2.0", method, params, id });
      this.proc.stdin?.write(msg + "\n", (err) => {
        if (err) {
          const pending = this.pending.get(id);
          if (pending)
            pending.reject(err);
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
        if ("error" in msg) {
          const err = msg.error;
          this.outputChannel.appendLine(
            `RPC error: ${pending.method} (id=${id}, ${Date.now() - pending.startedAt}ms): ${err.message}`
          );
          pending.reject(new Error(err.message));
        } else {
          pending.resolve(msg.result);
        }
      } else {
        this.outputChannel.appendLine(
          `[stdout] response for unknown/expired request id=${id} \u2014 ignored`
        );
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
async function runInMatlabTerminal(command, outputChannel2) {
  if (!isMatlabExtensionAvailable()) {
    return false;
  }
  try {
    const t0 = Date.now();
    const scriptPath = path.join(os.tmpdir(), "scistack_run.m");
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
    this.refreshPending = false;
    this.finishedCallbacks = [];
  }
  /** Mark a MATLAB run as owning the database from now until its run_done. */
  begin(runId) {
    this.inFlight.add(runId);
  }
  /**
   * Clear a run's mark. Safe to call for every run_done — Python runs are
   * simply absent from the set. Returns whether this was a tracked MATLAB
   * run, and fires the finished callbacks once the last one clears.
   */
  end(runId) {
    if (!runId)
      return false;
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

// src/dagPanel.ts
var DEBUG_SESSION_NAME = "Attach to scistack-gui server";
var DagPanel = class {
  constructor(context, pythonProcess2, outputChannel2) {
    this.context = context;
    this.pythonProcess = pythonProcess2;
    this.outputChannel = outputChannel2;
    this.disposables = [];
    this.disposeCallbacks = [];
    /**
     * Which MATLAB runs currently own the DuckDB file lock. Shared with
     * `extension.ts`'s DB file-watcher, which must not refresh the DAG while
     * MATLAB has the database — see MatlabRunTracker.
     */
    this.matlabRuns = new MatlabRunTracker();
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
        if (method === "open_plot_panel") {
          try {
            await vscode3.commands.executeCommand(
              "scistack.openPlotPanel",
              msg.params ?? {}
            );
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
              filters: { Images: ["png", "svg", "pdf"] }
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
            const result = await this.pythonProcess.request(
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
            const result = await this.pythonProcess.request(
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
    const sent = await runInMatlabTerminal(command, this.outputChannel);
    if (sent) {
      this.outputChannel.appendLine("dispatchMatlabCommand: sent to MATLAB terminal");
      vscode3.window.showInformationMessage("Running in MATLAB terminal...");
      return "terminal";
    }
    if (runId) {
      try {
        const sidecarResult = await this.pythonProcess.request(
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
    this.beginMatlabRun(runId);
    try {
      const result = await this.pythonProcess.request(
        "generate_matlab_command",
        params
      );
      const command = result.command;
      this.outputChannel.appendLine(
        `handleMatlabRun: got command (${command.length} chars)`
      );
      const tier = await this.dispatchMatlabCommand(command, runId, void 0);
      if (tier !== "sidecar") {
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
    this.beginMatlabRun(runId);
    try {
      const result = await this.pythonProcess.request(
        "generate_matlab_pipeline_command",
        params
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

// src/plotPanel.ts
var path3 = __toESM(require("path"));
var vscode4 = __toESM(require("vscode"));
var PlotPanel = class _PlotPanel {
  constructor(context, pythonProcess2, outputChannel2, target, column) {
    this.context = context;
    this.pythonProcess = pythonProcess2;
    this.outputChannel = outputChannel2;
    this.target = target;
    this.disposables = [];
    this.panel = vscode4.window.createWebviewPanel(
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
          vscode4.Uri.file(path3.join(context.extensionPath, "dist", "webview"))
        ]
      }
    );
    this.panel.webview.html = this.getHtml();
    this.panel.webview.onDidReceiveMessage(
      async (msg) => {
        const method = msg.method;
        if (method === "pick_save_path") {
          try {
            const params = msg.params ?? {};
            const folder = vscode4.workspace.workspaceFolders?.[0]?.uri;
            const uri = await vscode4.window.showSaveDialog({
              defaultUri: folder ? vscode4.Uri.joinPath(folder, params.defaultName ?? "figure.png") : void 0,
              // The panel sends the ONE format its dropdown selected, so the
              // dialog cannot offer a second answer to a question already
              // asked — the backend honours the dropdown either way.
              filters: { Images: params.formats ?? ["png"] }
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
            const uris = await vscode4.window.showOpenDialog({
              canSelectFiles: false,
              canSelectFolders: true,
              canSelectMany: false,
              defaultUri: vscode4.workspace.workspaceFolders?.[0]?.uri,
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
          const result = await this.pythonProcess.request(
            method,
            msg.params ?? {}
          );
          this.panel.webview.postMessage({ id: msg.id, result });
        } catch (err) {
          this.outputChannel.appendLine(`plot panel: ${method} failed \u2014 ${err}`);
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
  static show(context, pythonProcess2, outputChannel2, target, options = {}) {
    if (!options.newTab && _PlotPanel.current) {
      _PlotPanel.current.retarget(target);
      return _PlotPanel.current;
    }
    const panel = new _PlotPanel(
      context,
      pythonProcess2,
      outputChannel2,
      target,
      options.column ?? vscode4.ViewColumn.One
    );
    if (!options.newTab)
      _PlotPanel.current = panel;
    return panel;
  }
  /** Point the open panel at a different variable or file. */
  retarget(target) {
    this.target = target;
    this.panel.title = this.title();
    this.panel.reveal(this.panel.viewColumn, false);
    this.panel.webview.postMessage({
      method: "open_plot_studio",
      params: { variable: target.variable, csv_path: target.csvPath }
    });
  }
  title() {
    if (this.target.csvPath)
      return `Plot \u2014 ${path3.basename(this.target.csvPath)}`;
    return this.target.variable ? `Plot \u2014 ${this.target.variable}` : "Plot";
  }
  dispose() {
    if (_PlotPanel.current === this)
      _PlotPanel.current = void 0;
    while (this.disposables.length)
      this.disposables.pop()?.dispose();
  }
  getHtml() {
    const webviewDir = path3.join(this.context.extensionPath, "dist", "webview");
    const webview = this.panel.webview;
    const scriptUri = webview.asWebviewUri(
      vscode4.Uri.file(path3.join(webviewDir, "index.js"))
    );
    const styleUri = webview.asWebviewUri(
      vscode4.Uri.file(path3.join(webviewDir, "index.css"))
    );
    const nonce = getNonce2();
    const target = JSON.stringify({
      view: "plot",
      variable: this.target.variable ?? null,
      csvPath: this.target.csvPath ?? null
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
};
function getNonce2() {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  let text = "";
  for (let i = 0; i < 32; i++) {
    text += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return text;
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
  return new Promise((resolve) => {
    let settled = false;
    const done = (probe) => {
      if (settled)
        return;
      settled = true;
      clearTimeout(timer);
      resolve(probe);
    };
    let proc;
    try {
      proc = (0, import_child_process2.spawn)(pythonPath, ["-c", PROBE_SCRIPT], {
        stdio: ["ignore", "pipe", "pipe"]
      });
    } catch (err) {
      resolve({ ok: false, spawnError: String(err) });
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

// src/extension.ts
var pythonProcess = null;
var dagPanel = null;
var outputChannel;
var dbWatcher = null;
var dbWatcherDebounce = null;
var lastStartArgs = null;
var warnedNoWorkspaceFolder = false;
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
          title: "Select SciStack Database",
          defaultUri: vscode5.workspace.workspaceFolders?.[0]?.uri
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
          openLabel: "Select Folder",
          defaultUri: vscode5.workspace.workspaceFolders?.[0]?.uri
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
      await startPipeline(context, dbPath, schemaKeys);
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
          // Don't re-pass schemaKeys: the DB already exists on restart.
          void 0
        );
        vscode5.window.showInformationMessage("SciStack: Python process restarted.");
      } catch (err) {
        vscode5.window.showErrorMessage(`SciStack: Restart failed \u2014 ${err}`);
      }
    }
  );
  const openPlotPanel = vscode5.commands.registerCommand(
    "scistack.openPlotPanel",
    (target = {}) => {
      if (!pythonProcess) {
        vscode5.window.showWarningMessage(
          "SciStack: Open a pipeline first \u2014 plotting needs the server process."
        );
        return;
      }
      PlotPanel.show(context, pythonProcess, outputChannel, target, {
        // Same editor group as the pipeline canvas, so the plot is a
        // full-width tab beside "SciStack Pipeline" in the tab bar.
        column: dagPanel?.viewColumn ?? vscode5.ViewColumn.One
      });
    }
  );
  const plotVariable = vscode5.commands.registerCommand(
    "scistack.plotVariable",
    async () => {
      const variable = await vscode5.window.showInputBox({
        prompt: "Variable type to plot",
        placeHolder: "e.g. StepLength"
      });
      if (!variable)
        return;
      await vscode5.commands.executeCommand("scistack.openPlotPanel", {
        variable: variable.trim()
      });
    }
  );
  const plotCsv = vscode5.commands.registerCommand(
    "scistack.plotCsv",
    async (uri) => {
      const target = uri?.fsPath ?? (await vscode5.window.showOpenDialog({
        canSelectMany: false,
        filters: { "CSV files": ["csv"] }
      }))?.[0]?.fsPath;
      if (!target)
        return;
      await vscode5.commands.executeCommand("scistack.openPlotPanel", {
        csvPath: target
      });
    }
  );
  context.subscriptions.push(
    openPipeline,
    restartPython,
    openPlotPanel,
    plotVariable,
    plotCsv,
    outputChannel
  );
}
async function startPipeline(context, dbPath, schemaKeys) {
  lastStartArgs = {
    dbPath,
    schemaKeys: schemaKeys ?? lastStartArgs?.schemaKeys
  };
  warnIfNoWorkspaceFolder();
  if (pythonProcess) {
    pythonProcess.kill();
    pythonProcess = null;
  }
  const interpreter = await resolvePythonPath();
  if (!interpreter) {
    vscode5.window.showErrorMessage(
      "SciStack: Could not find a Python interpreter. Install the Python extension or set scistack.pythonPath in settings."
    );
    return;
  }
  const { path: pythonPath, source: interpreterSource } = interpreter;
  outputChannel.appendLine(`Starting SciStack server...`);
  outputChannel.appendLine(`  Python: ${pythonPath} (from ${interpreterSource})`);
  outputChannel.appendLine(`  DB: ${dbPath}`);
  outputChannel.appendLine(
    `  Project root: ${workspaceFolderPath() ?? "(none \u2014 server will fall back, see above)"}`
  );
  if (schemaKeys)
    outputChannel.appendLine(`  Schema keys: [${schemaKeys.join(", ")}] (new DB)`);
  pythonProcess = new PythonProcess(pythonPath, dbPath, outputChannel, schemaKeys);
  try {
    const cfg = vscode5.workspace.getConfiguration("scistack");
    const startupTimeoutMs = cfg.get("startupTimeoutMs", 6e4);
    const readyParams = await pythonProcess.waitForReady(startupTimeoutMs);
    outputChannel.appendLine(
      `Server ready \u2014 DB: ${readyParams.db_name}, schema: [${readyParams.schema_keys.join(", ")}]`
    );
  } catch (err) {
    const failed = pythonProcess;
    pythonProcess = null;
    failed.kill();
    await reportStartupFailure(failed, interpreterSource, err);
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
    dagPanel.matlabRuns.onAllFinished(flushDeferredDagRefresh);
  }
  pythonProcess.onNotification((method, params) => {
    if (dagPanel) {
      dagPanel.postMessage({ method, params });
      if (method === "run_done") {
        dagPanel.stopDebugSession();
        dagPanel.matlabRuns.end(params.run_id);
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
function workspaceFolderPath() {
  return vscode5.workspace.workspaceFolders?.[0]?.uri.fsPath;
}
function warnIfNoWorkspaceFolder() {
  if (workspaceFolderPath())
    return;
  const message = 'SciStack: no folder is open in this window, so there is no project to discover pipeline code from. Open your project folder and run "SciStack: Open Pipeline" again, or add paths from the Paths popup.';
  outputChannel.appendLine(message);
  if (!warnedNoWorkspaceFolder) {
    warnedNoWorkspaceFolder = true;
    vscode5.window.showWarningMessage(message);
  }
}
async function reportStartupFailure(failed, interpreterSource, err) {
  const errorMessage = err instanceof Error ? err.message : String(err);
  outputChannel.appendLine(`Server failed to start: ${errorMessage}`);
  outputChannel.appendLine(`Probing interpreter ${failed.pythonPath}...`);
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
  outputChannel.appendLine("");
  outputChannel.appendLine(`=== SciStack startup failure (${diagnosis.kind}) ===`);
  outputChannel.appendLine(diagnosis.detail);
  if (diagnosis.installCommand) {
    outputChannel.appendLine(`Install with: ${diagnosis.installCommand}`);
  }
  outputChannel.appendLine("=== end of startup failure report ===");
  await showDiagnosisMessage(diagnosis);
}
var ACTION_LABELS = {
  showOutput: "Show Details",
  selectInterpreter: "Select Interpreter",
  openSettings: "Open Settings",
  copyInstallCommand: "Copy Install Command"
};
async function showDiagnosisMessage(diagnosis) {
  const labels = diagnosis.actions.map((a) => ACTION_LABELS[a]);
  const picked = await vscode5.window.showErrorMessage(diagnosis.message, ...labels);
  if (!picked)
    return;
  const action = diagnosis.actions.find((a) => ACTION_LABELS[a] === picked);
  switch (action) {
    case "showOutput":
      outputChannel.show(true);
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
function flushDeferredDagRefresh() {
  if (!dagPanel)
    return;
  if (!dagPanel.matlabRuns.takeDeferredRefresh())
    return;
  outputChannel.appendLine(
    "MATLAB run finished \u2014 applying the deferred DAG refresh"
  );
  dagPanel.postMessage({ method: "dag_updated", params: {} });
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
      if (!dagPanel)
        return;
      if (!dagPanel.matlabRuns.noteDbChange()) {
        outputChannel.appendLine(
          "DuckDB file changed while MATLAB owns the database \u2014 deferring DAG refresh until the run finishes"
        );
        return;
      }
      outputChannel.appendLine("DuckDB file changed externally \u2014 refreshing DAG");
      dagPanel.postMessage({ method: "dag_updated", params: {} });
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
