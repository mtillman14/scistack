/**
 * Decides whether a MATLAB "Run" click should be treated as connecting to
 * MATLAB rather than dispatching a run.
 *
 * Why this matters: `runInMatlabTerminal` (matlabTerminal.ts) calls
 * `matlab.openCommandWindow` and sends the script the moment a `MATLAB`
 * terminal object exists, but on the very first click of a VS Code session
 * that terminal has just been created and the MATLAB process behind it has
 * not finished starting — so nothing actually runs. `handleMatlabRun`
 * (dagPanel.ts) used to call this "dispatched" and report it as a
 * successful run regardless (see plan-matlab-terminal-run-tracking.md,
 * problem P2). Gating on terminal existence turns that specific case into
 * a "connect, don't run yet" prompt instead of a false success.
 *
 * This is a heuristic, not a true connection check — the MathWorks
 * extension exposes no API for MATLAB's actual readiness, only whether a
 * terminal object exists. A run dispatched to a terminal that exists but
 * whose MATLAB process is still starting (e.g. the user clicks Run again
 * immediately after connecting) is not caught by this and remains covered
 * only by the deferred Stage 2 fix (run markers written by MATLAB itself).
 *
 * Deliberately free of any `vscode` import so it can be unit-tested under
 * `node --test` (see tsconfig.test.json).
 */
export function needsMatlabConnectionPrompt(
  matlabExtensionAvailable: boolean,
  matlabTerminalAlreadyOpen: boolean,
): boolean {
  return matlabExtensionAvailable && !matlabTerminalAlreadyOpen;
}

/**
 * Which OTHER database currently owns MATLAB, if any.
 *
 * There is one MATLAB process per VS Code window, and a SciStack run
 * configures it against one database (`configure_database` in the generated
 * script) before doing anything else. Two sessions dispatching at once
 * therefore do not run in parallel — the second `configure_database`
 * repoints the engine mid-run, and the first run's remaining `for_each`
 * calls write into the *other* project's database.
 *
 * Nothing downstream can detect that: both writes are well-formed, they
 * just land in the wrong place. So it is refused here, by name, before the
 * script is generated.
 *
 * Returns the label of the database holding MATLAB, or undefined when the
 * engine is free. A session never blocks itself: several MATLAB runs
 * against ONE database are the existing, supported case (MatlabRunTracker
 * already counts them).
 */
export function matlabHolder(
  sessions: readonly { id: string; label: string; matlabBusy: boolean }[],
  selfId: string,
): string | undefined {
  return sessions.find((s) => s.id !== selfId && s.matlabBusy)?.label;
}
