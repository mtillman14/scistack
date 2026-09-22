/**
 * Session — one open database, and everything that belongs to it.
 *
 * A session owns a Python server process, the pipeline canvas webview, the
 * `.duckdb` file watcher, the plot tabs opened from that canvas, and the
 * debugpy port they use. Opening a second database opens a second session;
 * they share only the Output Channel (§ `prefixedLog`) and the status bar.
 *
 * Why a process per database, rather than one server serving several: the
 * Python backend is pervasively process-global — `db._db`, `registry._functions`
 * and `_config`, `config._project_root_hint`, plus `scifor.set_schema` and
 * `scidb.configure_database` below it. Threading a session object through all
 * of that would be a rewrite of layers that have no reason to know the GUI
 * has tabs; spawning `scistack_gui.server --db <path>` twice gets the same
 * isolation from the operating system for free. Everything a session writes
 * outside its own process is already keyed to the database file — `scidb.log`
 * (`scidb.log.log_path_for`), `<db>.layout.json`, and the pipeline/intent
 * tables inside the database itself — so two sessions never collide there.
 *
 * The one thing two sessions must NOT do is open the same file: DuckDB is
 * single-writer, so `SessionManager.open` reveals an existing session rather
 * than starting a second server for it.
 */

import * as path from 'path';
import * as vscode from 'vscode';
import { DagPanel } from './dagPanel';
import { PanelRegistry } from './panelRegistry';
import { PlotPanel } from './plotPanel';
import { PythonProcess } from './pythonProcess';
import { buildPlotOnlyServerArgs, buildServerArgs } from './serverArgs';
import {
  LogSink,
  SessionRegistry,
  SessionResolution,
  prefixedLog,
  projectRootForDb,
  sessionIdForDb,
  sessionLabel,
} from './sessionCore';
import {
  diagnoseStartupFailure,
  probeInterpreter,
  StartupAction,
  StartupDiagnosis,
} from './startupDiagnostics';

/** Debounce for the `.duckdb` file watcher, ms. */
const DB_WATCH_DEBOUNCE_MS = 2000;

/** Label for the session that has no database (CSV plotting). */
const PLOT_ONLY_LABEL = 'plot-only';

export class Session {
  /** The pipeline canvas. Undefined for a plot-only session. */
  dagPanel: DagPanel | undefined;

  /**
   * The plot tabs opened from this session.
   *
   * Per session, not static: a `plot_save_complete` from one database
   * delivered into another database's tab would re-enable the wrong Save
   * button. That is the same failure `panelRegistry.ts` documents, one level
   * up — a registry with nothing to scope it to.
   */
  readonly plots = new PanelRegistry();

  private watcher: vscode.FileSystemWatcher | undefined;
  private watchDebounce: ReturnType<typeof setTimeout> | undefined;
  private disposed = false;

  private constructor(
    readonly manager: SessionManager,
    readonly id: string,
    /** Empty for a plot-only session, which has no database. */
    readonly dbPath: string,
    readonly label: string,
    readonly projectRoot: string | undefined,
    readonly log: LogSink,
    readonly debugPort: number | undefined,
    /** Replaced by `restart`, so panels must read it rather than cache it. */
    public python: PythonProcess,
  ) {}

  get isPlotOnly(): boolean {
    return this.dbPath === '';
  }

  /** Build a session around an already-ready server process. */
  static adopt(args: {
    manager: SessionManager;
    id: string;
    dbPath: string;
    label: string;
    projectRoot: string | undefined;
    log: LogSink;
    debugPort: number | undefined;
    python: PythonProcess;
  }): Session {
    return new Session(
      args.manager,
      args.id,
      args.dbPath,
      args.label,
      args.projectRoot,
      args.log,
      args.debugPort,
      args.python,
    );
  }

  /**
   * Deliver a push notification from this session's server to this
   * session's panels — and to nothing else.
   */
  route(method: string, params: Record<string, unknown>): void {
    const plotPanels = this.plots.send({ method, params });
    if (method.startsWith('plot_save_')) {
      // "backend emitted it, 0 panels received it" is the only visible
      // symptom of a routing regression here.
      this.log.appendLine(
        `[notify] ${method} (job=${params.job_id}) → ${plotPanels} plot panel(s)`,
      );
    }
    if (!this.dagPanel) return;
    this.dagPanel.postMessage({ method, params });
    if (method === 'run_done') {
      this.dagPanel.stopDebugSession();
      // Sidecar-driven MATLAB runs end here; the tracker fires the
      // all-finished callback once the last one clears.
      this.dagPanel.matlabRuns.end(params.run_id as string | undefined);
    }
  }

  /**
   * Watch this session's `.duckdb` for external writes (MATLAB, another
   * tool) and refresh the canvas when they settle.
   */
  startDbWatcher(): void {
    if (this.isPlotOnly) return;
    this.stopDbWatcher();
    const pattern = new vscode.RelativePattern(
      path.dirname(this.dbPath),
      path.basename(this.dbPath) + '*',
    );
    this.watcher = vscode.workspace.createFileSystemWatcher(pattern);
    const onChange = () => this.onDbFileChanged();
    this.watcher.onDidChange(onChange);
    this.watcher.onDidCreate(onChange);
  }

  private onDbFileChanged(): void {
    if (this.watchDebounce) clearTimeout(this.watchDebounce);
    this.watchDebounce = setTimeout(() => {
      this.watchDebounce = undefined;
      if (!this.dagPanel) return;
      // MATLAB writes to the WAL throughout a run, not just at the end. A
      // refresh now would fire graph RPCs at a database MATLAB currently
      // holds the file lock on, and every one of them can only fail — so
      // the tracker remembers the change and we refresh once it lets go.
      if (!this.dagPanel.matlabRuns.noteDbChange()) {
        this.log.appendLine(
          'DuckDB file changed while MATLAB owns the database — ' +
          'deferring DAG refresh until the run finishes',
        );
        return;
      }
      this.log.appendLine('DuckDB file changed externally — refreshing DAG');
      this.dagPanel.postMessage({ method: 'dag_updated', params: {} });
    }, DB_WATCH_DEBOUNCE_MS);
  }

  /** Emit the DAG refresh withheld while MATLAB owned the database. */
  flushDeferredDagRefresh(): void {
    if (!this.dagPanel) return;
    if (!this.dagPanel.matlabRuns.takeDeferredRefresh()) return;
    this.log.appendLine('MATLAB run finished — applying the deferred DAG refresh');
    this.dagPanel.postMessage({ method: 'dag_updated', params: {} });
  }

  private stopDbWatcher(): void {
    this.watcher?.dispose();
    this.watcher = undefined;
    if (this.watchDebounce) clearTimeout(this.watchDebounce);
    this.watchDebounce = undefined;
  }

  /**
   * Replace the server process, keeping the panels.
   *
   * Panels read `session.python` at call time rather than holding their own
   * reference, so nothing has to be "rebound" — the class of bug where a
   * plot tab kept writing to a destroyed stdin (ERR_STREAM_DESTROYED,
   * 2026-09-15) cannot arise.
   */
  adoptProcess(python: PythonProcess): void {
    this.python = python;
    this.log.appendLine('server replaced — panels now talk to the new process');
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
  async stopProcess(): Promise<void> {
    this.python.kill();
    await this.python.whenClosed();
  }

  /** Close everything this session owns. Idempotent. */
  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.log.appendLine('session closing');
    this.stopDbWatcher();
    // Plot tabs cannot outlive the server they issue plot_* RPCs to.
    this.closePlots();
    // `disposed` is already set, so the canvas's own dispose callback
    // re-entering here is a no-op.
    this.dagPanel?.dispose();
    this.python.kill();
    this.manager.forget(this);
  }

  private closePlots(): void {
    // Disposing a panel unregisters it, which mutates `plots`; sinks() is a
    // snapshot for exactly this reason.
    for (const panel of this.plots.sinks()) {
      (panel as PlotPanel).close();
    }
  }
}

/**
 * The open sessions, and everything that has to happen to start one.
 *
 * `SessionRegistry` (in `sessionCore.ts`) holds the pure bookkeeping —
 * identity, lookup, "which session does this command mean". This class adds
 * the parts that need `vscode`: the interpreter, the child process, the
 * readiness handshake and the failure report.
 */
export class SessionManager {
  private readonly registry = new SessionRegistry<Session>();

  /**
   * The database-less CSV server, kept OUT of the registry on purpose.
   *
   * The registry answers "which database does this command mean", and a
   * session with no database is never that answer — in it, a window with
   * only a CSV tab open would resolve "Plot Variable…" to the one server
   * structurally incapable of serving it.
   */
  private plotOnly: Session | undefined;
  private readonly changeHandlers: (() => void)[] = [];

  constructor(
    private readonly context: vscode.ExtensionContext,
    private readonly channel: vscode.OutputChannel,
  ) {}

  /** Called whenever the set of sessions, or the active one, changes. */
  onDidChange(handler: () => void): void {
    this.changeHandlers.push(handler);
  }

  private announceChange(): void {
    for (const handler of this.changeHandlers) handler();
  }

  get size(): number {
    return this.registry.size;
  }

  /** The open databases — what every command acts on. */
  all(): Session[] {
    return this.registry.all();
  }

  /**
   * Every live server, the database-less CSV one included. For diagnostics
   * and for anything that must not collide across processes (debugpy ports).
   */
  everything(): Session[] {
    return this.plotOnly ? [...this.registry.all(), this.plotOnly] : this.registry.all();
  }

  get active(): Session | undefined {
    return this.registry.active;
  }

  resolve(explicitId?: string): SessionResolution<Session> {
    return this.registry.resolve(explicitId);
  }

  /**
   * Resolve a session for a command, logging which rule decided.
   *
   * "The command went to the other database" and "the command did nothing"
   * are indistinguishable from the outside, so the rule is always recorded.
   */
  resolveForCommand(command: string, explicitId?: string): Session | undefined {
    const { session, source, detail } = this.registry.resolve(explicitId);
    this.channel.appendLine(`[session] ${command} → ${source}: ${detail}`);
    return session;
  }

  setActive(id: string): void {
    if (this.registry.active?.id === id) return;
    this.registry.setActive(id);
    this.channel.appendLine(`[session] active: ${this.registry.active?.label ?? '(none)'}`);
    this.announceChange();
  }

  /** Remove a disposed session. Called by `Session.dispose`, not directly. */
  forget(session: Session): void {
    if (this.plotOnly === session) this.plotOnly = undefined;
    this.registry.remove(session.id);
    this.channel.appendLine(
      `[session] closed ${session.label} — ${this.registry.size} still open`,
    );
    this.announceChange();
  }

  disposeAll(): void {
    for (const session of this.registry.all()) session.dispose();
    this.plotOnly?.dispose();
  }

  /**
   * Open a database, or reveal the session that already has it open.
   *
   * DuckDB is single-writer: a second server on the same file would lose the
   * lock race and report it as a mysterious `DatabaseLockedError`, so the
   * answer to "open this again" is "here it is".
   */
  async open(dbPath: string, schemaKeys?: string[]): Promise<Session | undefined> {
    const id = sessionIdForDb(dbPath);
    const existing = this.registry.get(id);
    if (existing) {
      this.channel.appendLine(
        `[session] ${existing.label} is already open — revealing its tab`,
      );
      existing.dagPanel?.reveal();
      this.setActive(existing.id);
      return existing;
    }

    const label = sessionLabel(dbPath);
    const log = prefixedLog(this.channel, label);
    const folders = (vscode.workspace.workspaceFolders ?? []).map((f) => f.uri.fsPath);
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
        `  Project root: ${projectRoot ?? '(none — server will fall back, see above)'}`,
        ...(schemaKeys ? [`  Schema keys: [${schemaKeys.join(', ')}] (new DB)`] : []),
      ],
    });
    if (!python) return undefined;

    const session = Session.adopt({
      manager: this,
      id,
      dbPath,
      label,
      projectRoot,
      log,
      debugPort,
      python,
    });
    this.registry.add(session);

    session.dagPanel = new DagPanel(this.context, session, log);
    session.dagPanel.onDidDispose(() => session.dispose());
    session.dagPanel.onDidChangeActive((active) => {
      if (active) this.setActive(session.id);
    });
    // The terminal and clipboard MATLAB tiers finish inside DagPanel and
    // never emit a Python notification, so the flush is registered here
    // rather than off a run_done.
    session.dagPanel.matlabRuns.onAllFinished(() => session.flushDeferredDagRefresh());

    python.onNotification((method, params) => session.route(method, params));
    session.startDbWatcher();

    this.channel.appendLine(
      `[session] opened ${label} (id=${id}, ${this.registry.size} open)`,
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
  async openPlotOnly(): Promise<Session | undefined> {
    if (this.plotOnly) return this.plotOnly;

    const log = prefixedLog(this.channel, PLOT_ONLY_LABEL);
    const debugPort = this.allocateDebugPort();
    const python = await this.spawnReady(
      buildPlotOnlyServerArgs({ logFile: this.plotOnlyLogFile() }),
      {
        log,
        label: PLOT_ONLY_LABEL,
        debugPort,
        describe: ['  No database — CSV plotting only'],
      },
    );
    if (!python) return undefined;

    const session = Session.adopt({
      manager: this,
      id: PLOT_ONLY_LABEL,
      dbPath: '',
      label: PLOT_ONLY_LABEL,
      projectRoot: undefined,
      log,
      debugPort,
      python,
    });
    this.plotOnly = session;
    python.onNotification((method, params) => session.route(method, params));
    this.channel.appendLine('[session] opened the plot-only server (no database)');
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
  private plotOnlyLogFile(): string | undefined {
    const dir = this.context.logUri?.fsPath ?? this.context.globalStorageUri?.fsPath;
    return dir ? path.join(dir, 'scistack-plot-session.log') : undefined;
  }

  /** Restart a session's server in place, keeping its panels and tabs. */
  async restart(session: Session): Promise<boolean> {
    // Stop first, then spawn — see `stopProcess`. The old server must have
    // released the DuckDB file before the new one tries to open it.
    //
    // The cost of getting this wrong is asymmetric: a failed restart leaves
    // the session with a dead process and the panels reporting "the Python
    // server is not running" on every RPC, which reads like a crash.
    await session.stopProcess();

    if (session.isPlotOnly) {
      const python = await this.spawnReady(
        buildPlotOnlyServerArgs({ logFile: this.plotOnlyLogFile() }),
        { log: session.log, label: session.label, debugPort: session.debugPort, describe: [] },
      );
      if (!python) return this.reportDeadAfterRestart(session);
      session.adoptProcess(python);
      python.onNotification((method, params) => session.route(method, params));
      return true;
    }

    const args = buildServerArgs({
      dbPath: session.dbPath,
      projectRoot: session.projectRoot,
    });
    const python = await this.spawnReady(args, {
      log: session.log,
      label: session.label,
      cwd: session.projectRoot,
      debugPort: session.debugPort,
      describe: [`  DB: ${session.dbPath}`],
    });
    if (!python) return this.reportDeadAfterRestart(session);

    session.adoptProcess(python);
    python.onNotification((method, params) => session.route(method, params));
    session.dagPanel?.reveal();
    // Re-fetch the registry (and DAG) so anything added since the last start
    // is reflected, and the header re-reads which database this is.
    session.dagPanel?.postMessage({ method: 'dag_updated', params: {} });
    session.dagPanel?.postMessage({ method: 'db_changed', params: {} });
    return true;
  }

  /**
   * The replacement server never started, and the old one is already
   * stopped. Say so: every later RPC from the still-open panels will fail,
   * and without this line that reads like a crash rather than a restart
   * that did not come back.
   */
  private reportDeadAfterRestart(session: Session): false {
    const message =
      `SciStack: ${session.label} has no server after the restart. ` +
      `Close its tab and open the database again.`;
    session.log.appendLine(message);
    vscode.window.showErrorMessage(message);
    return false;
  }

  /**
   * Spawn a server and wait for its `ready`, or report why it never came.
   *
   * Returns undefined on failure, having already told the user; the caller
   * simply does not create a session.
   */
  private async spawnReady(
    args: string[],
    opts: {
      log: LogSink;
      label: string;
      cwd?: string;
      debugPort?: number;
      describe: string[];
    },
  ): Promise<PythonProcess | undefined> {
    const interpreter = await resolvePythonPath();
    if (!interpreter) {
      vscode.window.showErrorMessage(
        'SciStack: Could not find a Python interpreter. ' +
        'Install the Python extension or set scistack.pythonPath in settings.'
      );
      return undefined;
    }
    const { path: pythonPath, source: interpreterSource } = interpreter;

    opts.log.appendLine('Starting SciStack server...');
    opts.log.appendLine(`  Python: ${pythonPath} (from ${interpreterSource})`);
    for (const line of opts.describe) opts.log.appendLine(line);

    const python = new PythonProcess(pythonPath, args, opts.log, {
      cwd: opts.cwd,
      debugPort: opts.debugPort,
    });

    try {
      const timeout = vscode.workspace
        .getConfiguration('scistack')
        .get<number>('startupTimeoutMs', 60000);
      const ready = await python.waitForReady(timeout);
      opts.log.appendLine(
        `Server ready — DB: ${ready.db_name}, schema: [${(ready.schema_keys ?? []).join(', ')}]`,
      );
      return python;
    } catch (err) {
      python.kill();
      await reportStartupFailure(opts.log, this.channel, python, interpreterSource, err);
      return undefined;
    }
  }

  /**
   * A debugpy port no other session is using.
   *
   * `debugpy.listen` on a port another server already holds fails, and the
   * server treats that as a warning — so with one fixed port the second
   * session silently has no debugger at all.
   */
  private allocateDebugPort(): number | undefined {
    const cfg = vscode.workspace.getConfiguration('scistack');
    if (!cfg.get<boolean>('debug', false)) return undefined;
    const base = cfg.get<number>('debugPort', 5678);
    const taken = new Set(this.everything().map((s) => s.debugPort));
    for (let offset = 0; offset < 64; offset++) {
      if (!taken.has(base + offset)) return base + offset;
    }
    return undefined;
  }

  /**
   * Warn when a database has no project folder to discover code from.
   *
   * With no `--project-root` the server falls back to the extension host's
   * working directory (`config.resolve_project_root` rule 3), which is
   * almost never where the user's code lives — so discovery comes back
   * empty and the canvas is unexplainedly bare.
   */
  private warnIfNoProjectRoot(projectRoot: string | undefined, label: string): void {
    if (projectRoot) return;
    const message =
      `SciStack: no folder is open in this window, so there is no project to ` +
      `discover pipeline code from for ${label}. Open your project folder and ` +
      `run "SciStack: Open Pipeline" again, or add paths from the Paths popup.`;
    this.channel.appendLine(message);
    if (warnedNoWorkspaceFolder) return;
    warnedNoWorkspaceFolder = true;
    vscode.window.showWarningMessage(message);
  }
}

// The "no workspace folder open" warning is shown once per session of VS
// Code: it is advice about how the window is set up, not about this
// particular start, and "Restart Python Process" is hit repeatedly while
// iterating on code.
let warnedNoWorkspaceFolder = false;

interface ResolvedInterpreter {
  path: string;
  /** Human-readable origin, so an error message can say where to change it. */
  source: string;
}

export async function resolvePythonPath(): Promise<ResolvedInterpreter | undefined> {
  // 1. Check extension setting
  const config = vscode.workspace.getConfiguration('scistack');
  const configured = config.get<string>('pythonPath');
  if (configured) return { path: configured, source: 'scistack.pythonPath setting' };

  // 2. Try the VS Code Python extension
  const pythonExt = vscode.extensions.getExtension('ms-python.python');
  if (pythonExt) {
    if (!pythonExt.isActive) await pythonExt.activate();
    const api = pythonExt.exports;
    if (api?.environments?.getActiveEnvironmentPath) {
      const envPath = api.environments.getActiveEnvironmentPath();
      if (envPath?.path) {
        return { path: envPath.path, source: 'active interpreter from the Python extension' };
      }
    }
  }

  // 3. Fallback to "python3" on PATH
  return { path: 'python3', source: 'PATH fallback (no Python extension interpreter)' };
}

/**
 * Explain a failed server start.
 *
 * The bare rejection reason ("Python process exited (code=1)") never says
 * which interpreter was used or what it was missing, which is the whole
 * question when scistack_gui simply is not installed in the environment
 * VS Code picked. So: probe the interpreter, classify, and offer the fix.
 */
async function reportStartupFailure(
  log: LogSink,
  channel: vscode.OutputChannel,
  failed: PythonProcess,
  interpreterSource: string,
  err: unknown,
): Promise<void> {
  const errorMessage = err instanceof Error ? err.message : String(err);
  log.appendLine(`Server failed to start: ${errorMessage}`);
  log.appendLine(`Probing interpreter ${failed.pythonPath}...`);

  // Let the dying child flush its stderr before we quote it.
  await failed.whenClosed();
  const probe = await probeInterpreter(failed.pythonPath);
  const diagnosis = diagnoseStartupFailure({
    pythonPath: failed.pythonPath,
    interpreterSource,
    args: failed.args,
    errorMessage,
    stderr: failed.getStderr(),
    exitCode: failed.getExitCode(),
    probe,
  });

  log.appendLine('');
  log.appendLine(`=== SciStack startup failure (${diagnosis.kind}) ===`);
  log.appendLine(diagnosis.detail);
  if (diagnosis.installCommand) {
    log.appendLine(`Install with: ${diagnosis.installCommand}`);
  }
  log.appendLine('=== end of startup failure report ===');

  await showDiagnosisMessage(diagnosis, channel);
}

const ACTION_LABELS: Record<StartupAction, string> = {
  showOutput: 'Show Details',
  selectInterpreter: 'Select Interpreter',
  openSettings: 'Open Settings',
  copyInstallCommand: 'Copy Install Command',
};

async function showDiagnosisMessage(
  diagnosis: StartupDiagnosis,
  channel: vscode.OutputChannel,
): Promise<void> {
  const labels = diagnosis.actions.map((a) => ACTION_LABELS[a]);
  const picked = await vscode.window.showErrorMessage(diagnosis.message, ...labels);
  if (!picked) return;

  const action = diagnosis.actions.find((a) => ACTION_LABELS[a] === picked);
  switch (action) {
    case 'showOutput':
      channel.show(true);
      break;
    case 'selectInterpreter':
      await vscode.commands.executeCommand('python.setInterpreter');
      break;
    case 'openSettings':
      await vscode.commands.executeCommand(
        'workbench.action.openSettings', 'scistack.pythonPath'
      );
      break;
    case 'copyInstallCommand':
      if (diagnosis.installCommand) {
        await vscode.env.clipboard.writeText(diagnosis.installCommand);
        vscode.window.showInformationMessage(
          `SciStack: copied to clipboard — ${diagnosis.installCommand}`
        );
      }
      break;
  }
}
