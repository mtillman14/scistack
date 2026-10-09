/**
 * "SciStack: Import Project Bundle…" (.claude/plan-portability.md Stage 8).
 *
 * An import makes a NEW project, with its own database, so it runs in its
 * own short-lived process (`python -m scistack_gui.bundle_cli import`, the
 * same code as `scistack import`), never inside an open session's server:
 * DuckDB allows one writer per file and a session's process already holds
 * its own. The import itself runs none of the bundle's code. Opening the
 * new project does (discovery imports its package), so that is the trust
 * prompt: "Trust and Open", or leave it closed.
 *
 * Dialog order: bundle → (bundle-info) → folder → schema → key map for
 * exporter keys you do not have → PathInput roots → history → import →
 * report → trust + open.
 */

import * as path from 'path';
import { spawn } from 'child_process';
import * as vscode from 'vscode';
import {
  BundleInfo,
  ImportReport,
  InstallResult,
  buildImportArgs,
  buildInfoArgs,
  buildInstallArgs,
  describeInstall,
  formatImportReport,
  keysNeedingAChoice,
  parseCliJson,
} from './bundleImportCore';
import { resolvePythonPath } from './session';

function runCli(
  pythonPath: string,
  args: string[],
  cwd: string | undefined,
  log: vscode.OutputChannel,
): Promise<string> {
  log.appendLine(`[bundle] ${pythonPath} ${args.join(' ')}`);
  return new Promise((resolve, reject) => {
    const proc = spawn(pythonPath, args, { cwd });
    let stdout = '';
    proc.stdout.on('data', (d) => (stdout += d.toString()));
    proc.stderr.on('data', (d) => log.append(d.toString()));
    proc.on('error', reject);
    proc.on('close', (code) => {
      log.appendLine(`[bundle] exited with code ${code}`);
      resolve(stdout);
    });
  });
}

const DROP = '(drop it — no counterpart in my schema)';
const KEEP_ROOT = "Keep the exporter's folder";
const CHOOSE_ROOT = 'Choose my folder…';
const LATER_ROOT = 'Decide later (edit the entities file)';

export async function importProjectBundle(
  log: vscode.OutputChannel,
  openProject: (dbPath: string) => Promise<void>,
): Promise<void> {
  const picked = await vscode.window.showOpenDialog({
    canSelectFiles: true,
    canSelectMany: false,
    filters: { 'SciStack project bundle': ['scistack'] },
    title: 'Import a SciStack project bundle',
  });
  if (!picked || picked.length === 0) return;
  const bundle = picked[0].fsPath;

  const interpreter = await resolvePythonPath();
  if (!interpreter) {
    vscode.window.showErrorMessage('SciStack: no Python interpreter found (set scistack.pythonPath).');
    return;
  }
  const python = interpreter.path;
  log.appendLine(`[bundle] import ${bundle} with ${python} (${interpreter.source})`);

  const infoAnswer = parseCliJson<{ info: BundleInfo }>(
    await runCli(python, buildInfoArgs(bundle), undefined, log),
  );
  if (!infoAnswer.ok) {
    vscode.window.showErrorMessage(`SciStack: cannot read ${path.basename(bundle)}: ${infoAnswer.error}`);
    return;
  }
  const info = infoAnswer.info;

  // Where: a parent folder + a new folder name (import never reuses a project).
  const parent = await vscode.window.showOpenDialog({
    canSelectFiles: false,
    canSelectFolders: true,
    canSelectMany: false,
    openLabel: 'Create the project in here',
    title: `Where should the new project "${info.package ?? 'project'}" go?`,
    defaultUri: vscode.workspace.workspaceFolders?.[0]?.uri,
  });
  if (!parent || parent.length === 0) return;
  const folderName = await vscode.window.showInputBox({
    prompt: 'New project folder name (must not hold a project yet)',
    value: info.package ?? path.basename(bundle, '.scistack'),
    validateInput: (v) => (v.trim() && !/[\\/]/.test(v) ? null : 'A folder name, without separators'),
  });
  if (!folderName) return;
  const into = path.join(parent[0].fsPath, folderName.trim());

  // Schema: suggest the exporter's.
  const schemaInput = await vscode.window.showInputBox({
    prompt: "Your schema keys, top-down (the exporter's are suggested)",
    value: info.schema_keys.join(', '),
    validateInput: (v) => (v.split(',').map((s) => s.trim()).filter(Boolean).length ? null : 'At least one key'),
  });
  if (schemaInput === undefined) return;
  const schema = schemaInput.split(',').map((s) => s.trim()).filter(Boolean);

  const keyMap: Record<string, string | null> = {};
  const free = schema.filter((k) => !info.schema_keys.includes(k));
  for (const key of keysNeedingAChoice(info.schema_keys, schema)) {
    const choice = await vscode.window.showQuickPick([...free, DROP], {
      title: `The exporter's key "${key}" is not in your schema`,
      placeHolder: `Which of your keys is "${key}"?`,
    });
    if (choice === undefined) return;
    if (choice === DROP) keyMap[key] = null;
    else {
      keyMap[key] = choice;
      free.splice(free.indexOf(choice), 1);
    }
  }

  // PathInput roots: the recipient's own copy of the raw files (never copied).
  const pathRoots: Record<string, string> = {};
  for (const p of info.path_inputs) {
    const was = p.root_folders.length ? p.root_folders.join(', ') : '(project root)';
    const choice = await vscode.window.showQuickPick([CHOOSE_ROOT, KEEP_ROOT, LATER_ROOT], {
      title: `PathInput "${p.name}": ${p.templates.join(' | ')}`,
      placeHolder: `Raw files are not in the bundle. The exporter's folder: ${was}`,
    });
    if (choice === undefined) return;
    if (choice !== CHOOSE_ROOT) continue;
    const folder = await vscode.window.showOpenDialog({
      canSelectFiles: false,
      canSelectFolders: true,
      canSelectMany: false,
      title: `Your folder for "${p.name}" (${p.templates.join(' | ')})`,
    });
    if (folder && folder.length > 0) pathRoots[p.name] = folder[0].fsPath;
  }

  let importHistory = false;
  if (info.has_history) {
    const choice = await vscode.window.showQuickPick(
      [
        { label: 'Import history', detail: 'Loaded live when the bundle carries its data and the schema is kept; archived otherwise', yes: true },
        { label: 'Skip history', detail: 'The new database holds only your own results', yes: false },
      ],
      { title: `History (${info.has_data ? 'with' : 'without'} data)` },
    );
    if (choice === undefined) return;
    importHistory = choice.yes;
  }

  const sameSchema = schema.join('\u0000') === info.schema_keys.join('\u0000');
  const args = buildImportArgs({
    bundle,
    into,
    schema: sameSchema ? undefined : schema,
    keyMap,
    pathRoots,
    importHistory,
  });
  const answer = await vscode.window.withProgress(
    { location: vscode.ProgressLocation.Notification, title: `Importing ${path.basename(bundle)}…` },
    async () => parseCliJson<{ report: ImportReport }>(await runCli(python, args, undefined, log)),
  );
  if (!answer.ok) {
    vscode.window.showErrorMessage(`SciStack import failed: ${answer.error}`);
    return;
  }
  const report = answer.report;

  const doc = await vscode.workspace.openTextDocument({
    language: 'markdown',
    content: formatImportReport(report),
  });
  await vscode.window.showTextDocument(doc, { preview: false });

  const INSTALL = 'Trust, Install and Open';
  const OPEN = 'Trust and Open';
  const open = await vscode.window.showWarningMessage(
    `Imported "${report.package}" into ${report.root}. Opening it loads the bundle's code, `
    + 'which runs its Python module-level code. Installing what it needs runs those '
    + "packages' code too (only missing packages; on any conflict nothing is installed). "
    + 'Only continue if you trust it.',
    { modal: true },
    INSTALL,
    OPEN,
  );
  if (open !== INSTALL && open !== OPEN) return;

  if (open === INSTALL) {
    const answer = await vscode.window.withProgress(
      { location: vscode.ProgressLocation.Notification, title: `Installing what ${report.package} needs…` },
      async () => parseCliJson<{ install: InstallResult }>(
        await runCli(python, buildInstallArgs(report.root), undefined, log),
      ),
    );
    if (!answer.ok) {
      vscode.window.showWarningMessage(`SciStack: install did not run: ${answer.error}`);
    } else {
      const msg = describeInstall(answer.install);
      if (answer.install.status === 'installed' || answer.install.status === 'nothing') {
        vscode.window.showInformationMessage(`SciStack: ${msg}`);
      } else {
        vscode.window.showWarningMessage(`SciStack: ${msg}`);
      }
    }
  }

  // The project root is the innermost workspace folder holding the database
  // (sessionCore.projectRootForDb), so the new folder joins the workspace
  // first unless it already is one.
  const folders = vscode.workspace.workspaceFolders ?? [];
  if (!folders.some((f) => path.resolve(f.uri.fsPath) === path.resolve(report.root))) {
    vscode.workspace.updateWorkspaceFolders(folders.length, 0, { uri: vscode.Uri.file(report.root) });
    log.appendLine(`[bundle] added ${report.root} to the workspace`);
  }
  await openProject(report.db_path);
}
