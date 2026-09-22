/**
 * Tracks which MATLAB runs currently own the DuckDB database.
 *
 * Why this exists: the GUI's Python server deliberately drops its DuckDB
 * file lock between requests (see `scistack_gui/db.py`) so that MATLAB can
 * open the same database. MATLAB then holds that lock for the whole run.
 * Any GUI request issued in that window can only fail — and the DB
 * file-watcher in `extension.ts` is otherwise happy to fire a `dag_updated`
 * on every WAL write MATLAB makes *during* the run, i.e. to generate
 * exactly those doomed requests, repeatedly.
 *
 * So: mark a run in flight when we dispatch it to MATLAB, note (rather than
 * act on) DB changes while it is, and replay one refresh when MATLAB lets
 * go. Deliberately free of any `vscode` import so it can be unit-tested
 * under `node --test` (see tsconfig.test.json).
 */

export class MatlabRunTracker {
  private inFlight = new Set<string>();
  /**
   * The subset of in-flight runs that occupy the window's ONE MathWorks
   * MATLAB — see `noteSharedEngine`.
   */
  private sharedEngine = new Set<string>();
  private refreshPending = false;
  private finishedCallbacks: (() => void)[] = [];

  /** Mark a MATLAB run as owning the database from now until its run_done. */
  begin(runId: string): void {
    this.inFlight.add(runId);
  }

  /**
   * Record that this run went to the **shared** MATLAB — the MathWorks
   * terminal, or the clipboard destined for it — rather than to this
   * session's own sidecar.
   *
   * The distinction exists because only one of the two tiers is shared
   * between databases:
   *
   * - **sidecar** — `scistack_gui.matlab_sidecar._sidecar` is a *process*
   *   singleton, and every session has its own Python server process, so
   *   every session already has its own MATLAB. Two databases running
   *   through sidecars are as independent as two Python runs and must not
   *   block each other.
   * - **terminal / clipboard** — the MathWorks extension owns one MATLAB
   *   per VS Code window, and a SciStack script points it at one database
   *   with `configure_database` before doing anything else.
   *
   * Without this split the gate was exactly backwards: a sidecar run held
   * the mark for its whole duration (Python pushes a real `run_done`) and
   * blocked the other database pointlessly, while a terminal run — the one
   * that genuinely shares an engine — cleared it milliseconds after
   * dispatch.
   */
  noteSharedEngine(runId: string): void {
    if (this.inFlight.has(runId)) this.sharedEngine.add(runId);
  }

  /** Whether a run is currently occupying the window's shared MATLAB. */
  get sharedEngineActive(): boolean {
    return this.sharedEngine.size > 0;
  }

  /**
   * Clear a run's mark. Safe to call for every run_done — Python runs are
   * simply absent from the set. Returns whether this was a tracked MATLAB
   * run, and fires the finished callbacks once the last one clears.
   */
  end(runId: string | undefined): boolean {
    if (!runId) return false;
    this.sharedEngine.delete(runId);
    const wasTracked = this.inFlight.delete(runId);
    if (wasTracked && this.inFlight.size === 0) {
      this.finishedCallbacks.forEach(cb => cb());
    }
    return wasTracked;
  }

  /** Whether any MATLAB run currently holds the database. */
  get isActive(): boolean {
    return this.inFlight.size > 0;
  }

  /**
   * Called by the DB file-watcher. Returns true when the caller should
   * refresh the DAG now; false when MATLAB owns the database, in which case
   * the change is remembered for {@link takeDeferredRefresh}.
   */
  noteDbChange(): boolean {
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
  takeDeferredRefresh(): boolean {
    if (!this.refreshPending) return false;
    this.refreshPending = false;
    return true;
  }

  /** Register a callback fired when the LAST in-flight MATLAB run ends. */
  onAllFinished(callback: () => void): void {
    this.finishedCallbacks.push(callback);
  }
}
