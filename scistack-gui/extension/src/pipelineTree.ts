/**
 * The Activity Bar "SciStack" view (GitHub #28): one root per open
 * session; under it the steps in pipeline order with the verdict glyph and
 * the reason chip text; expand a step for its reasons and output variables;
 * "Exclusions (N)" lists what is left out, why and since when.
 *
 * Click a step or a reason → reveal the canvas and open the data view
 * filtered there (the webview's `reveal_step` notification). No session →
 * the welcome view ("No pipeline open" + Open Pipeline, package.json).
 *
 * Fed by the extension host: every `get_pipeline` reply the canvas fetches
 * is relayed here (dagPanel.ts), so the tree shows exactly the verdicts the
 * canvas shows. Rules live in pipelineTreeCore.ts.
 */

import * as vscode from 'vscode';
import { exclusionRows, stepRows, type GraphPayload } from './pipelineTreeCore';

type Item =
  | { kind: 'session'; sessionId: string; label: string }
  | { kind: 'step'; sessionId: string; index: number }
  | { kind: 'reason'; sessionId: string; step: number; index: number }
  | { kind: 'output'; sessionId: string; step: number; index: number }
  | { kind: 'exclusions'; sessionId: string }
  | { kind: 'exclusion'; sessionId: string; index: number };

const STATES_FOR_REASON: Record<string, string[]> = {
  failed: ['failed'],
  missing_input: ['missing_input'],
  not_run: ['not_run', 'new'],
  code_changed: ['outdated'],
  inputs_changed: ['outdated'],
};

class PipelineTreeProvider implements vscode.TreeDataProvider<Item> {
  private readonly graphs = new Map<string, { label: string; graph: GraphPayload }>();
  private readonly changed = new vscode.EventEmitter<Item | undefined>();
  readonly onDidChangeTreeData = this.changed.event;
  private channel: vscode.OutputChannel | null = null;

  setChannel(channel: vscode.OutputChannel): void {
    this.channel = channel;
  }

  update(sessionId: string, label: string, graph: GraphPayload): void {
    this.graphs.set(sessionId, { label, graph });
    const rows = stepRows(graph);
    this.channel?.appendLine(
      `[tree] ${label}: ${rows.length} step(s), ${rows.filter(r => r.needsRunning).length} need running`,
    );
    this.changed.fire(undefined);
  }

  /** Keep only the sessions still open. */
  prune(openIds: string[]): void {
    let dropped = false;
    for (const id of [...this.graphs.keys()]) {
      if (!openIds.includes(id)) {
        this.graphs.delete(id);
        dropped = true;
      }
    }
    if (dropped) this.changed.fire(undefined);
  }

  getChildren(item?: Item): Item[] {
    if (!item) {
      return [...this.graphs.entries()].map(([sessionId, g]) => ({ kind: 'session', sessionId, label: g.label }));
    }
    const g = this.graphs.get(item.sessionId)?.graph;
    if (!g) return [];
    const rows = stepRows(g);
    if (item.kind === 'session') {
      const steps: Item[] = rows.map((_r, index) => ({ kind: 'step', sessionId: item.sessionId, index }));
      return [...steps, { kind: 'exclusions', sessionId: item.sessionId }];
    }
    if (item.kind === 'step') {
      const row = rows[item.index];
      if (!row) return [];
      return [
        ...row.reasons.map((_r, index) => ({ kind: 'reason' as const, sessionId: item.sessionId, step: item.index, index })),
        ...row.outputs.map((_o, index) => ({ kind: 'output' as const, sessionId: item.sessionId, step: item.index, index })),
      ];
    }
    if (item.kind === 'exclusions') {
      return exclusionRows(g).map((_e, index) => ({ kind: 'exclusion', sessionId: item.sessionId, index }));
    }
    return [];
  }

  getTreeItem(item: Item): vscode.TreeItem {
    const g = this.graphs.get(item.sessionId)?.graph ?? {};
    const rows = stepRows(g);
    const C = vscode.TreeItemCollapsibleState;
    switch (item.kind) {
      case 'session': {
        const t = new vscode.TreeItem(item.label, C.Expanded);
        t.contextValue = 'scistackSession';
        t.iconPath = new vscode.ThemeIcon('database');
        return t;
      }
      case 'step': {
        const row = rows[item.index];
        const t = new vscode.TreeItem(row?.label ?? '?', row && (row.reasons.length || row.outputs.length) ? C.Collapsed : C.None);
        t.description = row?.description;
        t.tooltip = row?.tooltip;
        t.iconPath = new vscode.ThemeIcon(row?.needsRunning ? 'circle-filled' : 'pass-filled');
        t.command = {
          command: 'scistack.revealStep',
          title: 'Show in the data view',
          arguments: [item.sessionId, row?.nodeId, row?.outputs.map(o => o.variable) ?? [], STATES_FOR_REASON[row?.reasons[0]?.kind ?? ''] ?? []],
        };
        return t;
      }
      case 'reason': {
        const row = rows[item.step];
        const reason = row?.reasons[item.index];
        const t = new vscode.TreeItem(reason?.text ?? '', C.None);
        t.command = {
          command: 'scistack.revealStep',
          title: 'Show where',
          arguments: [item.sessionId, row?.nodeId, row?.outputs.map(o => o.variable) ?? [], STATES_FOR_REASON[reason?.kind ?? ''] ?? []],
        };
        return t;
      }
      case 'output': {
        const row = rows[item.step];
        const out = row?.outputs[item.index];
        const t = new vscode.TreeItem(out?.variable ?? '', C.None);
        t.description = out?.description;
        t.iconPath = new vscode.ThemeIcon('symbol-variable');
        t.command = {
          command: 'scistack.revealStep',
          title: 'Show in the data view',
          arguments: [item.sessionId, row?.nodeId, out ? [out.variable] : [], []],
        };
        return t;
      }
      case 'exclusions': {
        const n = exclusionRows(g).length;
        const t = new vscode.TreeItem(`Exclusions (${n})`, n ? C.Collapsed : C.None);
        t.iconPath = new vscode.ThemeIcon('circle-slash');
        return t;
      }
      case 'exclusion': {
        const e = exclusionRows(g)[item.index];
        const t = new vscode.TreeItem(e?.label ?? '', C.None);
        t.description = e?.description;
        return t;
      }
    }
  }
}

/** The one tree (one view, one root per session). */
export const pipelineTree = new PipelineTreeProvider();
