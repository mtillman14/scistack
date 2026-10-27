/**
 * Problems-panel entries and the end-of-run notice from a run's failures.
 *
 * vscode-free so it runs under `npm test`. The wire shape is scifor's
 * failure record (`scifor/failures.py` → `FailureGroup.to_dict`), grouped by
 * the backend (`scistack_gui/services/run_failures.py`):
 * `run_done.failures = {by_node: {nodeId: [group]}, other: [group]}`, where
 * `by_node` lists EVERY node the run covered — an empty list means the node
 * ran clean. So this module never decides which entries a rerun replaces:
 * it assigns what the backend sent. See docs/claude/error-surfacing.md.
 */

export interface WireFrame {
  file: string;
  line: number;
  function: string;
  code: string;
  origin: 'user' | 'framework' | 'library';
}

export interface WireFailureGroup {
  reason: string;
  fn_name: string;
  exc_type: string;
  message: string;
  no_data: boolean;
  count: number;
  combos: string[];
  user_frame: WireFrame | null;
  /** Who can fix it (scifor.failures.classify_blame). */
  blame?: string;
  probable?: boolean;
  /** Display text for `blame`, worded by scistacklog.journal (never copied here). */
  blame_label?: string;
  node_id?: string | null;
  target?: string;
}

export interface WireRunFailures {
  by_node: Record<string, WireFailureGroup[]>;
  other: WireFailureGroup[];
}

export interface DiagnosticEntry {
  file: string;
  /** 1-based, as Python reports it. */
  line: number;
  message: string;
}

/** Key for groups the run could not attach to a node. */
export const OTHER_KEY = '';

/** Read `run_done.failures` defensively (older servers send nothing). */
export function readRunFailures(raw: unknown): WireRunFailures {
  const value = (raw ?? {}) as Partial<WireRunFailures>;
  return {
    by_node: value.by_node && typeof value.by_node === 'object' ? value.by_node : {},
    other: Array.isArray(value.other) ? value.other : [],
  };
}

export function diagnosticMessage(g: WireFailureGroup): string {
  const example = g.combos?.length ? `, e.g. ${g.combos[0]}` : '';
  const label = g.blame_label ? `${g.blame_label}. ` : '';
  return `${label}${g.fn_name}: ${g.reason} — ${g.count} combo(s) failed${example}`;
}

function entriesFor(groups: WireFailureGroup[]): DiagnosticEntry[] {
  return groups
    .filter(g => !g.no_data && g.user_frame)
    .map(g => ({
      file: g.user_frame!.file,
      line: g.user_frame!.line,
      message: diagnosticMessage(g),
    }));
}

/**
 * Next per-node state: every node in `by_node` is ASSIGNED its new entries
 * (possibly none); nodes the run did not cover keep theirs. Node-less
 * groups replace the previous node-less ones.
 */
export function nextDiagnosticState(
  prev: ReadonlyMap<string, DiagnosticEntry[]>,
  failures: WireRunFailures,
): Map<string, DiagnosticEntry[]> {
  const next = new Map(prev);
  for (const [nodeId, groups] of Object.entries(failures.by_node)) {
    next.set(nodeId, entriesFor(groups));
  }
  next.set(OTHER_KEY, entriesFor(failures.other));
  for (const [key, entries] of next) {
    if (!entries.length) next.delete(key);
  }
  return next;
}

/** Flatten the per-node state into the per-file shape the collection takes. */
export function entriesByFile(
  state: ReadonlyMap<string, DiagnosticEntry[]>,
): Map<string, DiagnosticEntry[]> {
  const out = new Map<string, DiagnosticEntry[]>();
  for (const entries of state.values()) {
    for (const e of entries) {
      const list = out.get(e.file) ?? [];
      list.push(e);
      out.set(e.file, list);
    }
  }
  return out;
}

/** Every failed group of the run, node-tagged ones first. */
export function allGroups(failures: WireRunFailures): WireFailureGroup[] {
  return [...Object.values(failures.by_node).flat(), ...failures.other].filter(
    g => !g.no_data,
  );
}

export interface FailureNotice {
  message: string;
  /** The node "Show failures" opens; undefined when no group has one. */
  nodeId: string | undefined;
}

/** The error notification for a finished run, or null when nothing failed
 *  (or the user cancelled — they already know). */
export function failureNotice(
  failures: WireRunFailures,
  opts: { cancelled?: boolean; stopped?: boolean },
): FailureNotice | null {
  if (opts.cancelled) return null;
  const groups = allGroups(failures);
  if (!groups.length) return null;
  const combos = groups.reduce((n, g) => n + (g.count || 0), 0);
  const fns = [...new Set(groups.map(g => g.fn_name).filter(Boolean))];
  const first = groups[0].reason.length > 140
    ? groups[0].reason.slice(0, 139) + '…'
    : groups[0].reason;
  const where = groups[0].user_frame
    ? ` (${basename(groups[0].user_frame.file)}:${groups[0].user_frame.line})`
    : '';
  const stopped = opts.stopped ? ' — stopped at the first failure' : '';
  const more = groups.length > 1 ? ` (+${groups.length - 1} more reason(s))` : '';
  const nodeGroup = groups.find(g => g.node_id);
  return {
    message:
      `SciStack: ${combos} combo(s) failed in ${fns.join(', ') || 'the run'}${stopped}: ` +
      `${groups[0].blame_label ? groups[0].blame_label + ' — ' : ''}${first}${where}${more}`,
    nodeId: nodeGroup?.node_id ?? undefined,
  };
}

function basename(file: string): string {
  const parts = file.split(/[\\/]/);
  return parts[parts.length - 1] || file;
}
