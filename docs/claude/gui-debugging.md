# Debugging a run from the GUI

Added 2026-11-04. A prototype: `.claude/plan-distribute-replaces-combine.md` Stage 8.
Check it by hand (`docs/gui-manual-testing-todo.md`), since nothing here can run outside VS Code.

## Why a raised error did not stop VS Code

scifor catches the exception so one failing combination does not end the run. VS Code therefore
treats it as handled. "Raised Exceptions" stops, but also inside every library. The session
attached with `justMyCode: false`.

## What happens now

| Piece | Where | What it does |
|---|---|---|
| Library code | `debuggerStatus.attachConfiguration` | `justMyCode: true`, plus `rules` marking SciStack's packages as library, including on an editable install |
| The stop | `dagPanel.applyStopOnError` | Sends `setExceptionBreakpoints` with `userUnhandled`: stop when an error leaves user code into library code, which is where scifor catches it |
| Schema key in the Call Stack | `scifor.foreach._combo_named_thread` | Names the thread after the combination, only while `debugpy` is loaded |
| Schema key in a notification | `dagPanel.watchForStops`, `noteRunProgress` | Reads `stopped` events; the combination comes from `run_progress` `combo_start` |

Turn the stop off with `scistack.debugStopOnError`. Continue moves on to the next combination, so
a run with many failures stops at each one. `on_error` is unaffected.

## Open questions for the manual check

- **VS Code re-sending its checkboxes.** It sends its own `setExceptionBreakpoints` at session start
  and when breakpoints change. We re-send after both and before each Run. A change in the Breakpoints
  panel's checkboxes could still override it until the next Run.
- **Library functions.** `pandas.read_csv` wired directly has no user frame, so `userUnhandled` has
  nothing to fire on. The failure reports as usual.
- **justMyCode.** Breakpoints set inside library code no longer stop, by design of `justMyCode`.

## Diagnostics

The SciStack output channel logs `[debugger] stop on error: ...` or `could not set stop on error`.
If nothing stops, check that line first, then VS Code's Breakpoints panel.
