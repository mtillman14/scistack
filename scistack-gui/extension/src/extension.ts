/**
 * SciStack GUI — VS Code Extension entry point.
 *
 * activate() is called when the user first triggers a SciStack command.
 * deactivate() is called when the extension is unloaded.
 */

import * as path from 'path';
import * as vscode from 'vscode';
import { PythonProcess } from './pythonProcess';
import { DagPanel } from './dagPanel';
import { PlotPanel, PlotTarget } from './plotPanel';
import {
  diagnoseStartupFailure,
  probeInterpreter,
  StartupAction,
  StartupDiagnosis,
} from './startupDiagnostics';

let pythonProcess: PythonProcess | null = null;
let dagPanel: DagPanel | null = null;
let outputChannel: vscode.OutputChannel;
let dbWatcher: vscode.FileSystemWatcher | null = null;
let dbWatcherDebounce: ReturnType<typeof setTimeout> | null = null;

// Remember the most recent start args so we can restart the Python process
// (e.g. after editing scistack_gui source code) without re-prompting the user.
interface LastStartArgs {
  dbPath: string;
  schemaKeys?: string[];
}
let lastStartArgs: LastStartArgs | null = null;

// The "no workspace folder open" warning is shown once per session: it is
// advice about how the window is set up, not about this particular start,
// and "Restart Python Process" is hit repeatedly while iterating on code.
let warnedNoWorkspaceFolder = false;

export function activate(context: vscode.ExtensionContext) {
  outputChannel = vscode.window.createOutputChannel('SciStack');

  const openPipeline = vscode.commands.registerCommand(
    'scistack.openPipeline',
    async () => {
      // Open existing DB or create a new one?
      const dbChoice = await vscode.window.showQuickPick(
        ['Open existing database', 'Create new database'],
        { placeHolder: 'SciStack: Open or create a .duckdb file?' }
      );
      if (!dbChoice) return;

      let dbPath: string;
      let schemaKeys: string[] | undefined;
      if (dbChoice === 'Open existing database') {
        const dbUris = await vscode.window.showOpenDialog({
          canSelectFiles: true,
          canSelectFolders: false,
          canSelectMany: false,
          filters: { 'DuckDB Database': ['duckdb'] },
          title: 'Select SciStack Database',
          defaultUri: vscode.workspace.workspaceFolders?.[0]?.uri,
        });
        if (!dbUris || dbUris.length === 0) return;
        dbPath = dbUris[0].fsPath;
      } else {
        const folderUris = await vscode.window.showOpenDialog({
          canSelectFiles: false,
          canSelectFolders: true,
          canSelectMany: false,
          title: 'Select folder for new SciStack database',
          openLabel: 'Select Folder',
          defaultUri: vscode.workspace.workspaceFolders?.[0]?.uri,
        });
        if (!folderUris || folderUris.length === 0) return;
        const folderPath = folderUris[0].fsPath;

        const nameInput = await vscode.window.showInputBox({
          prompt: 'Database filename',
          placeHolder: 'e.g. my_pipeline.duckdb',
          validateInput: (v) => {
            const trimmed = v.trim();
            if (!trimmed) return 'Provide a filename';
            if (trimmed.includes('/') || trimmed.includes('\\')) {
              return 'Filename must not contain path separators';
            }
            return null;
          },
        });
        if (!nameInput) return;
        const fileName = nameInput.trim().endsWith('.duckdb')
          ? nameInput.trim()
          : `${nameInput.trim()}.duckdb`;
        dbPath = path.join(folderPath, fileName);

        const keysInput = await vscode.window.showInputBox({
          prompt: 'Schema keys (comma-separated, top-down)',
          placeHolder: 'e.g. subject, session',
          validateInput: (v) => {
            const parts = v.split(',').map((s) => s.trim()).filter(Boolean);
            return parts.length === 0 ? 'Provide at least one schema key' : null;
          },
        });
        if (!keysInput) return;
        schemaKeys = keysInput.split(',').map((s) => s.trim()).filter(Boolean);
      }

      // No "how should SciStack discover your code?" step: pipeline code is
      // discovered from the workspace folder once the database is open (see
      // startPipeline's warnIfNoWorkspaceFolder and serverArgs.ts).
      await startPipeline(context, dbPath, schemaKeys);
    }
  );

  const restartPython = vscode.commands.registerCommand(
    'scistack.restartPython',
    async () => {
      if (!lastStartArgs) {
        vscode.window.showWarningMessage(
          'SciStack: No pipeline has been opened yet — run "SciStack: Open Pipeline" first.'
        );
        return;
      }
      outputChannel.appendLine('Restarting Python process...');
      try {
        await startPipeline(
          context,
          lastStartArgs.dbPath,
          // Don't re-pass schemaKeys: the DB already exists on restart.
          undefined,
        );
        vscode.window.showInformationMessage('SciStack: Python process restarted.');
      } catch (err) {
        vscode.window.showErrorMessage(`SciStack: Restart failed — ${err}`);
      }
    }
  );

  // --- Plot Studio -------------------------------------------------------
  // The studio is its own editor tab (see plotPanel.ts) so the pipeline canvas
  // stays visible beside it. Every entry point — the DAG's right-click, the
  // sidebar button, these commands — funnels through `scistack.openPlotPanel`,
  // so there is exactly one place that decides how a plot tab is opened.

  const openPlotPanel = vscode.commands.registerCommand(
    'scistack.openPlotPanel',
    (target: PlotTarget = {}) => {
      if (!pythonProcess) {
        vscode.window.showWarningMessage(
          'SciStack: Open a pipeline first — plotting needs the server process.'
        );
        return;
      }
      PlotPanel.show(context, pythonProcess, outputChannel, target, {
        // Same editor group as the pipeline canvas, so the plot is a
        // full-width tab beside "SciStack Pipeline" in the tab bar.
        column: dagPanel?.viewColumn ?? vscode.ViewColumn.One,
      });
    }
  );

  const plotVariable = vscode.commands.registerCommand(
    'scistack.plotVariable',
    async () => {
      const variable = await vscode.window.showInputBox({
        prompt: 'Variable type to plot',
        placeHolder: 'e.g. StepLength',
      });
      if (!variable) return;
      await vscode.commands.executeCommand('scistack.openPlotPanel', {
        variable: variable.trim(),
      });
    }
  );

  // Right-click a .csv in the Explorer. This deliberately needs NO project and
  // NO database: it routes to scistackplot's CsvSource, which is the same
  // DataSource protocol the scidb path implements.
  const plotCsv = vscode.commands.registerCommand(
    'scistack.plotCsv',
    async (uri?: vscode.Uri) => {
      const target =
        uri?.fsPath ??
        (
          await vscode.window.showOpenDialog({
            canSelectMany: false,
            filters: { 'CSV files': ['csv'] },
          })
        )?.[0]?.fsPath;
      if (!target) return;
      await vscode.commands.executeCommand('scistack.openPlotPanel', {
        csvPath: target,
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

async function startPipeline(
  context: vscode.ExtensionContext,
  dbPath: string,
  schemaKeys?: string[],
) {
  // Remember args so "Restart Python" can respawn without re-prompting.
  // Preserve the prior schemaKeys if this call didn't supply them (e.g. restart).
  lastStartArgs = {
    dbPath,
    schemaKeys: schemaKeys ?? lastStartArgs?.schemaKeys,
  };

  warnIfNoWorkspaceFolder();

  // Kill existing process if any
  if (pythonProcess) {
    pythonProcess.kill();
    pythonProcess = null;
  }

  // Resolve Python interpreter
  const interpreter = await resolvePythonPath();
  if (!interpreter) {
    vscode.window.showErrorMessage(
      'SciStack: Could not find a Python interpreter. ' +
      'Install the Python extension or set scistack.pythonPath in settings.'
    );
    return;
  }
  const { path: pythonPath, source: interpreterSource } = interpreter;

  // Start the Python JSON-RPC server
  outputChannel.appendLine(`Starting SciStack server...`);
  outputChannel.appendLine(`  Python: ${pythonPath} (from ${interpreterSource})`);
  outputChannel.appendLine(`  DB: ${dbPath}`);
  outputChannel.appendLine(
    `  Project root: ${workspaceFolderPath() ?? '(none — server will fall back, see above)'}`
  );
  if (schemaKeys) outputChannel.appendLine(`  Schema keys: [${schemaKeys.join(', ')}] (new DB)`);

  pythonProcess = new PythonProcess(pythonPath, dbPath, outputChannel, schemaKeys);

  try {
    const cfg = vscode.workspace.getConfiguration('scistack');
    const startupTimeoutMs = cfg.get<number>('startupTimeoutMs', 60000);
    const readyParams = await pythonProcess.waitForReady(startupTimeoutMs);
    outputChannel.appendLine(
      `Server ready — DB: ${readyParams.db_name}, schema: [${readyParams.schema_keys.join(', ')}]`
    );
  } catch (err) {
    const failed = pythonProcess;
    pythonProcess = null;
    failed.kill();
    await reportStartupFailure(failed, interpreterSource, err);
    return;
  }

  // Create or reveal the DAG Webview panel
  if (dagPanel) {
    dagPanel.updatePythonProcess(pythonProcess);
    dagPanel.reveal();
    // Trigger the webview to re-fetch the registry (and DAG) so any new
    // functions/variables added since the last start are reflected.
    dagPanel.postMessage({ method: 'dag_updated', params: {} });
  } else {
    dagPanel = new DagPanel(context, pythonProcess, outputChannel);
    dagPanel.onDidDispose(() => {
      dagPanel = null;
      if (pythonProcess) {
        pythonProcess.kill();
        pythonProcess = null;
      }
    });
    // MATLAB has released the database — replay whatever the file-watcher
    // withheld while it was running. Registered here (once per panel)
    // rather than in the run_done branch below, because the terminal and
    // clipboard tiers finish inside DagPanel and never emit a Python
    // notification at all.
    dagPanel.matlabRuns.onAllFinished(flushDeferredDagRefresh);
  }

  // Forward push notifications from Python → Webviews
  pythonProcess.onNotification((method, params) => {
    // The Plot Studio opens as its own editor tab, so it is NOT reached by
    // posting to the DAG panel. Its long-running save is a background job that
    // announces itself only through `plot_save_*` notifications; without this
    // line the tab that started the save never hears it finish and its Save
    // button stays on "Saving…" forever.
    const plotPanels = PlotPanel.broadcast({ method, params });
    if (method.startsWith('plot_save_')) {
      // Logged because "backend emitted it, 0 panels received it" is the only
      // visible symptom of a routing regression here — the Python side always
      // reports a clean send.
      outputChannel.appendLine(
        `[notify] ${method} (job=${params.job_id}) → ${plotPanels} plot panel(s)`,
      );
    }
    if (dagPanel) {
      dagPanel.postMessage({ method, params });
      // When a run finishes, auto-detach the debugger if we auto-attached it.
      if (method === 'run_done') {
        dagPanel.stopDebugSession();
        // Sidecar-driven MATLAB runs end here; the tracker fires the
        // callback registered above once the last one clears.
        dagPanel.matlabRuns.end(params.run_id as string | undefined);
      }
    }
  });

  // Watch the DuckDB file for external changes (e.g. MATLAB writes).
  // Debounce with a 2-second window so rapid writes don't flood the UI.
  setupDbWatcher(dbPath);

  // Status bar
  const statusItem = vscode.window.createStatusBarItem(
    vscode.StatusBarAlignment.Left, 100
  );
  statusItem.text = `$(database) SciStack: ${dbPath.split('/').pop()}`;
  statusItem.tooltip = dbPath;
  statusItem.show();
}

function workspaceFolderPath(): string | undefined {
  return vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
}

/**
 * Warn when there is no folder open in this VS Code window.
 *
 * The extension no longer asks the user to point at a project or module up
 * front; the server auto-discovers pipeline code from the project root, and
 * with no `--project` that root is the workspace folder
 * (`config.resolve_project_root` rule 2). With no folder open, rule 3 takes
 * over and the root becomes the extension host's working directory — almost
 * never where the user's code lives, so discovery comes back empty. That is
 * the one case the removed picker used to cover, so name it explicitly
 * rather than leaving an empty canvas unexplained.
 */
function warnIfNoWorkspaceFolder() {
  if (workspaceFolderPath()) return;

  const message =
    'SciStack: no folder is open in this window, so there is no project to ' +
    'discover pipeline code from. Open your project folder and run ' +
    '"SciStack: Open Pipeline" again, or add paths from the Paths popup.';
  outputChannel.appendLine(message);
  if (!warnedNoWorkspaceFolder) {
    warnedNoWorkspaceFolder = true;
    vscode.window.showWarningMessage(message);
  }
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
  failed: PythonProcess,
  interpreterSource: string,
  err: unknown,
): Promise<void> {
  const errorMessage = err instanceof Error ? err.message : String(err);
  outputChannel.appendLine(`Server failed to start: ${errorMessage}`);
  outputChannel.appendLine(`Probing interpreter ${failed.pythonPath}...`);

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

  outputChannel.appendLine('');
  outputChannel.appendLine(`=== SciStack startup failure (${diagnosis.kind}) ===`);
  outputChannel.appendLine(diagnosis.detail);
  if (diagnosis.installCommand) {
    outputChannel.appendLine(`Install with: ${diagnosis.installCommand}`);
  }
  outputChannel.appendLine('=== end of startup failure report ===');

  await showDiagnosisMessage(diagnosis);
}

const ACTION_LABELS: Record<StartupAction, string> = {
  showOutput: 'Show Details',
  selectInterpreter: 'Select Interpreter',
  openSettings: 'Open Settings',
  copyInstallCommand: 'Copy Install Command',
};

async function showDiagnosisMessage(diagnosis: StartupDiagnosis): Promise<void> {
  const labels = diagnosis.actions.map((a) => ACTION_LABELS[a]);
  const picked = await vscode.window.showErrorMessage(diagnosis.message, ...labels);
  if (!picked) return;

  const action = diagnosis.actions.find((a) => ACTION_LABELS[a] === picked);
  switch (action) {
    case 'showOutput':
      outputChannel.show(true);
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

interface ResolvedInterpreter {
  path: string;
  /** Human-readable origin, so an error message can say where to change it. */
  source: string;
}

async function resolvePythonPath(): Promise<ResolvedInterpreter | undefined> {
  // 1. Check extension setting
  const config = vscode.workspace.getConfiguration('scistack');
  const configured = config.get<string>('pythonPath');
  if (configured) return { path: configured, source: 'scistack.pythonPath setting' };

  // 2. Try the VS Code Python extension
  const pythonExt = vscode.extensions.getExtension('ms-python.python');
  if (pythonExt) {
    if (!pythonExt.isActive) await pythonExt.activate();
    // The Python extension exports an API to get the active interpreter
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
 * Emit the DAG refresh that was withheld while MATLAB owned the database.
 *
 * No-op when nothing was withheld: a MATLAB run that wrote nothing (or that
 * failed before writing) shouldn't cost a full graph re-fetch.
 */
function flushDeferredDagRefresh(): void {
  if (!dagPanel) return;
  if (!dagPanel.matlabRuns.takeDeferredRefresh()) return;
  outputChannel.appendLine(
    'MATLAB run finished — applying the deferred DAG refresh',
  );
  dagPanel.postMessage({ method: 'dag_updated', params: {} });
}

function setupDbWatcher(dbPath: string): void {
  // Dispose any previous watcher.
  if (dbWatcher) {
    dbWatcher.dispose();
    dbWatcher = null;
  }
  if (dbWatcherDebounce) {
    clearTimeout(dbWatcherDebounce);
    dbWatcherDebounce = null;
  }

  const dbDir = path.dirname(dbPath);
  const dbBase = path.basename(dbPath);
  // Watch for .duckdb and .duckdb.wal files.
  const pattern = new vscode.RelativePattern(dbDir, dbBase + '*');
  dbWatcher = vscode.workspace.createFileSystemWatcher(pattern);

  const onDbChange = () => {
    if (dbWatcherDebounce) {
      clearTimeout(dbWatcherDebounce);
    }
    dbWatcherDebounce = setTimeout(() => {
      dbWatcherDebounce = null;
      if (!dagPanel) return;
      // MATLAB writes to the WAL throughout a run, not just at the end. A
      // refresh now would fire graph RPCs at a database MATLAB currently
      // holds the file lock on, and every one of them can only fail — so
      // the tracker remembers the change and we refresh once it lets go.
      if (!dagPanel.matlabRuns.noteDbChange()) {
        outputChannel.appendLine(
          'DuckDB file changed while MATLAB owns the database — ' +
          'deferring DAG refresh until the run finishes',
        );
        return;
      }
      outputChannel.appendLine('DuckDB file changed externally — refreshing DAG');
      dagPanel.postMessage({ method: 'dag_updated', params: {} });
    }, 2000);
  };

  dbWatcher.onDidChange(onDbChange);
  dbWatcher.onDidCreate(onDbChange);
}

export function deactivate() {
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
