/**
 * The vscode-free half of "SciStack: Import Project Bundle…"
 * (.claude/plan-portability.md Stage 8): building the
 * `python -m scistack_gui.bundle_cli` command line, reading its one-line
 * JSON answer, and rendering the import report. The dialogs and the process
 * spawn live in bundleImport.ts.
 *
 * Every decision about the import itself is Python's: this only collects
 * the user's answers and passes them as the CLI's flags, so the terminal
 * (`scistack import`) and this command run one implementation.
 */

/** What the dialogs collected; each maps onto one CLI flag. */
export interface ImportChoices {
  bundle: string;
  into: string;
  /** Recipient schema keys; undefined = keep the exporter's. */
  schema?: string[];
  /** Exporter key -> recipient key, or null to drop it. */
  keyMap?: Record<string, string | null>;
  /** PathInput name -> the recipient's folder. */
  pathRoots?: Record<string, string>;
  importHistory: boolean;
}

/** `bundle-info --json`'s `info` (scidb.bundle.preview). */
export interface BundleInfo {
  path: string;
  package: string | null;
  schema_keys: string[];
  database: string | null;
  options: Record<string, boolean>;
  sections: Record<string, { files: number; bytes: number }>;
  has_history: boolean;
  has_data: boolean;
  path_inputs: { name: string; templates: string[]; root_folders: string[] }[];
  environment: {
    python: { exported: string | null; here: string };
    missing: string[];
    different: { name: string; exported: string; here: string }[];
    install: string | null;
  } | null;
}

/** `import --json`'s `report` (scidb.bundle.ImportReport.to_dict). */
export interface ImportReport {
  root: string;
  db_path: string;
  package: string;
  schema_keys: string[];
  created: string[];
  sections: Record<string, Record<string, unknown>>;
  warnings: string[];
}

export const BUNDLE_CLI_MODULE = 'scistack_gui.bundle_cli';

export function buildInfoArgs(bundle: string): string[] {
  return ['-m', BUNDLE_CLI_MODULE, 'bundle-info', bundle, '--json'];
}

export function buildImportArgs(c: ImportChoices): string[] {
  const args = ['-m', BUNDLE_CLI_MODULE, 'import', c.bundle, '--into', c.into, '--json'];
  if (c.schema && c.schema.length > 0) args.push('--schema', ...c.schema);
  for (const [from, to] of Object.entries(c.keyMap ?? {})) {
    args.push('--map', `${from}=${to ?? ''}`);
  }
  for (const [name, folder] of Object.entries(c.pathRoots ?? {})) {
    args.push('--path-root', `${name}=${folder}`);
  }
  args.push(c.importHistory ? '--history' : '--no-history');
  return args;
}

/**
 * The CLI prints its answer as the LAST line of stdout (bundle_cli's
 * contract); anything before it is stray output from imported libraries.
 */
export function parseCliJson<T>(stdout: string): { ok: true } & T | { ok: false; error: string } {
  const lines = stdout.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  for (let i = lines.length - 1; i >= 0; i--) {
    if (!lines[i].startsWith('{')) continue;
    try {
      return JSON.parse(lines[i]);
    } catch {
      break;
    }
  }
  return { ok: false, error: `no JSON answer from ${BUNDLE_CLI_MODULE}; output was:\n${stdout.slice(-2000)}` };
}

/** Exporter keys the recipient schema does not have: each needs a choice. */
export function keysNeedingAChoice(exporter: string[], recipient: string[]): string[] {
  return exporter.filter((k) => !recipient.includes(k));
}

/** The report view: a Markdown document. */
export function formatImportReport(r: ImportReport): string {
  const out: string[] = [];
  out.push(`# Imported \`${r.package}\``, '');
  out.push(`- Project folder: \`${r.root}\``);
  out.push(`- Database: \`${r.db_path}\``);
  out.push(`- Schema: ${r.schema_keys.map((k) => `\`${k}\``).join(' → ')}`, '');

  const schema = (r.sections.schema ?? {}) as {
    key_map?: Record<string, string | null>;
    dropped?: [string, string][];
    flagged?: [string, string][];
  };
  const renamed = Object.entries(schema.key_map ?? {}).filter(([a, b]) => a !== b);
  if (renamed.length > 0) {
    out.push('## Schema keys', '');
    for (const [a, b] of renamed) out.push(`- \`${a}\` → ${b === null ? '_dropped_' : `\`${b}\``}`);
    out.push('');
  }
  if ((schema.dropped ?? []).length + (schema.flagged ?? []).length > 0) {
    out.push('## Review', '');
    for (const [where, key] of schema.dropped ?? []) {
      out.push(`- **dropped** ${where}: referred to \`${key}\`, which has no counterpart`);
    }
    for (const [where, why] of schema.flagged ?? []) out.push(`- ${where}: ${why}`);
    out.push('');
  }

  const env = r.sections.env as BundleInfo['environment'] | undefined;
  if (env && env.install) {
    out.push('## Environment', '');
    if (env.missing.length > 0) out.push(`- Missing: ${env.missing.join(', ')}`);
    for (const d of env.different) out.push(`- ${d.name}: exported ${d.exported}, here ${d.here}`);
    out.push('', 'Install with:', '', '```', env.install, '```', '');
  }

  const history = r.sections.history as { live?: boolean; archived?: string | null } | undefined;
  if (history) {
    out.push('## History', '');
    if (history.live) out.push('- Loaded live, with its data.');
    else if (history.archived) {
      out.push(
        `- Archived to \`${history.archived}\` (not in the live database: its data was not in`
        + ' the bundle, or the schema changed). Kept for reproduction checks.',
      );
    } else out.push('- Not imported.');
    out.push('');
  }

  out.push('## Sections', '');
  for (const [name, section] of Object.entries(r.sections).sort()) {
    if (name === 'schema' || name === 'env') continue;
    const parts = Object.entries(section ?? {}).map(([k, v]) =>
      Array.isArray(v) || (v && typeof v === 'object')
        ? `${k}: ${Array.isArray(v) ? v.length : Object.keys(v as object).length}`
        : `${k}: ${String(v)}`,
    );
    out.push(`- **${name}** ${parts.join(', ') || '_nothing to report_'}`);
  }
  out.push('');

  if (r.warnings.length > 0) {
    out.push('## Warnings', '');
    for (const w of r.warnings) out.push(`- ${w}`);
    out.push('');
  }
  out.push(`## Created (${r.created.length})`, '');
  for (const c of r.created) out.push(`- \`${c}\``);
  out.push('');
  return out.join('\n');
}
