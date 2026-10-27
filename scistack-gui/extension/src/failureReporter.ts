/**
 * Puts a session's run failures where VS Code users look for errors: the
 * Problems panel (one Error per failure reason, at the user's line that
 * raised) and an error notification with "Show failures" / "Open log".
 *
 * One per session; disposing it (session close) clears its Problems
 * entries. The rules live in the vscode-free `runFailures.ts`.
 */

import * as path from 'path';
import * as vscode from 'vscode';
import { LogSink } from './sessionCore';
import {
  DiagnosticEntry,
  entriesByFile,
  failureNotice,
  nextDiagnosticState,
  readRunFailures,
} from './runFailures';

const SHOW_FAILURES = 'Show failures';
const OPEN_LOG = 'Open log';

export class FailureReporter implements vscode.Disposable {
  private readonly collection: vscode.DiagnosticCollection;
  private state = new Map<string, DiagnosticEntry[]>();

  constructor(
    name: string,
    private readonly log: LogSink,
    /** `scistack.log` beside the database (scidb.log.log_path_for). */
    private readonly logPath: string,
    /** Ask the canvas to open its failures panel on this node. */
    private readonly showFailures: (nodeId: string | undefined) => void,
  ) {
    this.collection = vscode.languages.createDiagnosticCollection(`scistack:${name}`);
  }

  onRunDone(params: Record<string, unknown>): void {
    const failures = readRunFailures(params.failures);
    this.state = nextDiagnosticState(this.state, failures);
    this.collection.clear();
    let total = 0;
    for (const [file, entries] of entriesByFile(this.state)) {
      this.collection.set(
        vscode.Uri.file(file),
        entries.map(e => {
          const line = Math.max(0, e.line - 1);
          const d = new vscode.Diagnostic(
            new vscode.Range(line, 0, line, Number.MAX_SAFE_INTEGER),
            e.message,
            vscode.DiagnosticSeverity.Error,
          );
          d.source = 'SciStack';
          return d;
        }),
      );
      total += entries.length;
    }
    this.log.appendLine(
      `[failures] run_done ${params.run_id}: ${Object.keys(failures.by_node).length} ` +
      `node(s) covered, ${total} Problems entr${total === 1 ? 'y' : 'ies'} now shown`,
    );

    const notice = failureNotice(failures, {
      cancelled: Boolean(params.cancelled),
      stopped: Boolean(params.stopped),
    });
    if (!notice) return;
    void vscode.window
      .showErrorMessage(notice.message, SHOW_FAILURES, OPEN_LOG)
      .then(async picked => {
        if (picked === SHOW_FAILURES) {
          this.showFailures(notice.nodeId);
        } else if (picked === OPEN_LOG) {
          try {
            const doc = await vscode.workspace.openTextDocument(this.logPath);
            const editor = await vscode.window.showTextDocument(doc, { preview: false });
            const end = doc.lineCount - 1;
            editor.revealRange(new vscode.Range(end, 0, end, 0), vscode.TextEditorRevealType.InCenter);
          } catch (err) {
            this.log.appendLine(`[failures] could not open ${this.logPath}: ${err}`);
          }
        }
      });
  }

  dispose(): void {
    this.collection.dispose();
  }
}

// Copy of scidb.log.LOG_FILE_NAME (the owner); test_log_file_name_parity.py
// fails if the two drift.
export const LOG_FILE_NAME = 'scistack.log';

export function logPathFor(dbPath: string): string {
  return path.join(path.dirname(dbPath), LOG_FILE_NAME);
}
