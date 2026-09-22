/**
 * SciStack GUI — VS Code Extension entry point.
 *
 * activate() is called when the user first triggers a SciStack command.
 * deactivate() is called when the extension is unloaded.
 *
 * This file is the command layer and nothing else. Everything that used to
 * live here at module scope — the Python process, the canvas, the `.duckdb`
 * watcher, the status bar item, `lastStartArgs` — belongs to a `Session`
 * (see `session.ts`), because there can now be one per open database. While
 * those were singletons, "Open Pipeline" on a second database killed the
 * first one's server and reused its canvas: the graph changed but the
 * header did not, since nothing re-fetches `get_info` (reported 2026-09-22).
 */

import * as path from 'path';
import * as vscode from 'vscode';
import { PlotPanel, PlotTarget } from './plotPanel';
import { Session, SessionManager } from './session';

let sessions: SessionManager;
let outputChannel: vscode.OutputChannel;
let statusItem: vscode.StatusBarItem | null = null;

export function activate(context: vscode.ExtensionContext) {
  outputChannel = vscode.window.createOutputChannel('SciStack');
  sessions = new SessionManager(context, outputChannel);
  sessions.onDidChange(updateStatusBar);

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
      // discovered from the workspace folder containing this database once
      // it is open (see SessionManager.open and serverArgs.ts).
      await sessions.open(dbPath, schemaKeys);
      updateStatusBar();
    }
  );

  const restartPython = vscode.commands.registerCommand(
    'scistack.restartPython',
    async () => {
      // The ACTIVE session, not a remembered set of start args: with two
      // databases open, "restart" can only sensibly mean the one you are
      // looking at.
      const session = sessions.resolveForCommand('restartPython');
      if (!session) {
        vscode.window.showWarningMessage(
          'SciStack: No pipeline has been opened yet — run "SciStack: Open Pipeline" first.'
        );
        return;
      }
      outputChannel.appendLine(`Restarting the Python process for ${session.label}...`);
      const ok = await sessions.restart(session);
      if (ok) {
        vscode.window.showInformationMessage(
          `SciStack: Python process restarted for ${session.label}.`,
        );
      }
    }
  );

  /**
   * List the open databases, and switch to one.
   *
   * With several sessions open, "which one am I about to act on?" needs an
   * answer that is visible rather than inferred — this command is it, and
   * the status bar item runs it.
   */
  const switchSession = vscode.commands.registerCommand(
    'scistack.switchSession',
    async () => {
      const open = sessions.all();
      if (open.length === 0) {
        vscode.window.showInformationMessage(
          'SciStack: no database is open — run "SciStack: Open Pipeline".',
        );
        return;
      }
      const active = sessions.active;
      const picked = await vscode.window.showQuickPick(
        open.map((s) => ({
          label: s === active ? `$(check) ${s.label}` : s.label,
          description: s.dbPath,
          detail: `project: ${s.projectRoot ?? '(none)'}`,
          session: s,
        })),
        { placeHolder: 'Switch to which SciStack database?' },
      );
      if (!picked) return;
      picked.session.dagPanel?.reveal();
      sessions.setActive(picked.session.id);
    }
  );

  /**
   * Report what is open, for when a tab misbehaves.
   *
   * The question a multi-session bug always starts with is "which server is
   * that tab actually talking to?", and nothing in VS Code shows it.
   */
  const showSessions = vscode.commands.registerCommand(
    'scistack.showSessions',
    () => {
      outputChannel.appendLine('');
      outputChannel.appendLine(
        `=== SciStack sessions (${sessions.size} database, ` +
        `${sessions.everything().length - sessions.size} plot-only) ===`,
      );
      const active = sessions.active;
      for (const s of sessions.everything()) {
        outputChannel.appendLine(
          `${s === active ? '*' : ' '} ${s.label}` +
          `  db=${s.dbPath || '(none — plot only)'}` +
          `  project=${s.projectRoot ?? '(none)'}` +
          `  debugPort=${s.debugPort ?? '(off)'}` +
          `  canvas=${s.dagPanel ? 'open' : 'none'}` +
          `  plotTabs=${s.plots.size}`,
        );
      }
      outputChannel.appendLine('=== end of session list ===');
      outputChannel.show(true);
    }
  );

  // --- Plot Studio -------------------------------------------------------
  // The studio is its own editor tab (see plotPanel.ts) so the pipeline canvas
  // stays visible beside it. Every entry point — the DAG's right-click, the
  // sidebar button, these commands — funnels through `scistack.openPlotPanel`,
  // so there is exactly one place that decides how a plot tab is opened.

  const openPlotPanel = vscode.commands.registerCommand(
    'scistack.openPlotPanel',
    async (target: PlotTarget = {}) => {
      const session = await sessionForPlot(target);
      if (!session) return;
      PlotPanel.show(context, session, target);
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
  // DataSource protocol the scidb path implements. With no pipeline open it
  // starts the database-less plot-only server rather than refusing.
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
    switchSession,
    showSessions,
    openPlotPanel,
    plotVariable,
    plotCsv,
    outputChannel
  );
}

/**
 * Which session a plot tab should talk to.
 *
 * A CSV needs no database at all, so it gets the plot-only server unless a
 * pipeline explicitly asked for it (plotting a CSV from inside a project is
 * still just a CSV, but the tab is then part of that project's session and
 * closes with it). Everything else needs a real database.
 */
async function sessionForPlot(target: PlotTarget): Promise<Session | undefined> {
  if (target.csvPath && !target.sessionId) {
    return sessions.openPlotOnly();
  }
  const session = sessions.resolveForCommand('openPlotPanel', target.sessionId);
  if (!session) {
    vscode.window.showWarningMessage(
      'SciStack: Open a pipeline first — plotting a variable needs its database.'
    );
    return undefined;
  }
  return session;
}

/**
 * One status bar item, showing the session a Palette command would act on.
 *
 * Not one item per database: three open databases would crowd out everything
 * else in the bar. The previous code created a NEW item on every open and
 * never disposed the old one, so stale database names accumulated there.
 */
function updateStatusBar(): void {
  const active = sessions.active ?? sessions.all()[0];
  if (!active) {
    statusItem?.dispose();
    statusItem = null;
    return;
  }
  if (!statusItem) {
    statusItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 100);
    statusItem.command = 'scistack.switchSession';
  }
  const others = sessions.size - 1;
  statusItem.text = `$(database) SciStack: ${active.label}` + (others > 0 ? ` (+${others})` : '');
  statusItem.tooltip = others > 0
    ? `${active.dbPath}\nClick to switch between ${others + 1} open databases`
    : active.dbPath;
  statusItem.show();
}

export function deactivate() {
  sessions?.disposeAll();
  statusItem?.dispose();
  statusItem = null;
}
