/**
 * serverArgs — build the `python -m scistack_gui.server` argv.
 *
 * Split out of `pythonProcess.ts` so the argv can be unit-tested under
 * `node --test` (nothing here imports `vscode`).
 *
 * The extension deliberately never passes `--module` or `--project`. Both
 * used to be filled in by a "How should SciStack discover your pipeline
 * code?" QuickPick shown before every open; that step is gone, so the
 * server always takes `bootstrap.open_or_create_project`'s auto-discovery
 * branch. Which directory that scans is decided by
 * `config.resolve_project_root`, and with no `--project` the answer is
 * `--project-root` — see the rule list in that docstring. Callers must
 * therefore pass `projectRoot`, and with several databases open it must be
 * the folder containing THIS database (`sessionCore.projectRootForDb`), not
 * `workspaceFolders[0]`, or the second session discovers the first
 * project's code.
 */

export interface ServerArgsOptions {
  /** Path to the .duckdb file to open or create. */
  dbPath: string;
  /** Top-down schema keys. Only meaningful when creating a new database. */
  schemaKeys?: string[];
  /** The workspace folder this database belongs to, i.e. "its project". */
  projectRoot?: string;
  /**
   * Where the file sink should write. Only the plot-only session passes it:
   * with no database there is no `scidb.log` to sit beside, and scattering
   * one into whichever folder a CSV happens to live in is worse than
   * putting it in the extension's own storage.
   */
  logFile?: string;
}

export function buildServerArgs({
  dbPath,
  schemaKeys,
  projectRoot,
  logFile,
}: ServerArgsOptions): string[] {
  const args = ['-m', 'scistack_gui.server', '--db', dbPath];
  if (schemaKeys && schemaKeys.length > 0) {
    args.push('--schema-keys', schemaKeys.join(','));
  }
  // The workspace folder is what the user thinks of as "the project", and
  // it is the server's only way to know: a .duckdb usually lives in a
  // datasets folder, so without this a new scistack.toml + entities file
  // would be written next to the data instead of in the project.
  if (projectRoot) {
    args.push('--project-root', projectRoot);
  }
  if (logFile) {
    args.push('--log-file', logFile);
  }
  return args;
}

/**
 * Argv for a **plot-only** server: no database, no project, no discovery.
 *
 * Right-click ▸ Plot CSV needs neither — `plot_service.get_source` builds a
 * `CsvSource` and every plot entry point is already written as
 * `db_connection(..., needed=not csv_path)`. What used to block it was
 * startup, not plotting: `--db` was required, so a CSV could only be
 * plotted by borrowing a pipeline session's server, and closing that
 * pipeline took the CSV tab's backend with it.
 */
export function buildPlotOnlyServerArgs(options: { logFile?: string } = {}): string[] {
  const args = ['-m', 'scistack_gui.server', '--plot-only'];
  if (options.logFile) {
    args.push('--log-file', options.logFile);
  }
  return args;
}
