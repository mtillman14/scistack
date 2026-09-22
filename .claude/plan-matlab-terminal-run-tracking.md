# Plan: knowing when a MathWorks-terminal MATLAB run starts and ends

> **SUPERSEDED and DONE (2026-09-22).** Both stages below were finally built,
> but not as written here — see `.claude/plan-matlab-run-completion.md` for
> the plan that shipped and `docs/claude/matlab-run-completion.md` for how it
> works. The substantive differences:
>
> * **The liveness signal is the DuckDB lock holder, not a heartbeat or a
>   timestamp hint.** `db.py` already parsed the holder's PID out of DuckDB's
>   conflict message, so "is MATLAB still running" is answerable with no
>   cooperation from MATLAB at all. That was not noticed when this plan was
>   written, and it is what made Stage 2 tractable.
> * **The watcher lives in Python, not the extension host.** Terminal runs
>   now finish through the same `run_done` notification the sidecar already
>   emitted, so the frontend and `MatlabRunTracker` needed no new concepts.
> * **Markers sit beside the database** (`<stem>.runs/`), not in the OS temp
>   directory — per-database, and they survive MATLAB running on another
>   machine.
> * **`onCleanup` is a net, not the mechanism.** Under `run('<file>.m')` the
>   script shares the caller's workspace, so the cleanup object does not fire
>   at script end; the deterministic report is an explicit `finish` call on
>   both the success and the catch paths.
> * The per-run script filename (item 7 below) was fixed as *per session*
>   during the multi-session work, then narrowed no further — two runs in one
>   database are serialised by the engine gate.
>
> Kept for the failure-mode table and the "deliberately not doing" note about
> `external_db_access`, both of which still hold.


## Why

When SciStack dispatches a script to the MathWorks MATLAB terminal (Tier 2
— the default in VS Code, and the user's actual setup), it drops the text
off and hears nothing back. Two consequences, which are worth separating
because they have different severity and different fixes:

**P1 — the papercut.** Python doesn't know MATLAB is using the database, so
a GUI request that hits the lock can't tell a 200 ms write apart from a
20-minute run. It backs off for `ACQUIRE_RETRY_TIMEOUT` (5 s) before
reporting. Clicking around during a run costs ~5 s per click.

**P2 — the lie, and the more important one.** `dagPanel.ts::handleMatlabRun`
synthesizes `run_done { success: true }` the moment the script is handed
over:

```ts
const tier = await this.dispatchMatlabCommand(command, runId, undefined);
if (tier !== 'sidecar') { finish(true); }
```

So the Runs console reports success before MATLAB has executed a line, and
reports success even when MATLAB then fails. `MatlabRunTracker`'s
deferred-refresh logic is also defeated by this: begin/end span the
hand-off, not the run, so the DB file-watcher still fires during terminal
runs. This is pre-existing and was explicitly accepted in
`plan-matlab-pipeline-execution.md` ("completion detection stays exactly as
fragile as it already is"), but P1's fix makes it worth revisiting: both
problems are the same missing signal.

The sidecar tier has neither problem — Python owns that process and knows
exactly when it starts and stops.

## Design principle

**Prefer signals that self-heal over state that must be correctly torn
down.** The failure that matters most here is the GUI believing MATLAB owns
the database when it doesn't, and locking itself out indefinitely. That is
strictly worse than today's 5-second wait. So:

- Nothing may *prevent* the GUI from trying to open the database. A hint
  may only change **how long it waits**, never **whether it attempts**.
- Any "MATLAB is busy" state must have an escape that does not depend on
  MATLAB cooperating — because MATLAB can be Ctrl-C'd, crash, or be closed.

This rules out the obvious-looking approach of reusing `external_db_access`
for terminal runs (see "Deliberately not doing" below).

## Stage 1 — Adaptive backoff (fixes P1)

Small, self-contained, no MATLAB-side changes.

**`extension/src/dagPanel.ts`** — after a successful terminal dispatch,
notify Python: `matlab_terminal_run_started { run_id }`. Fire-and-forget;
a failure here must not fail the run.

**`scistack_gui/db.py`** — a `_matlab_expected_until` timestamp (not a
boolean, so it cannot be permanently true). While it is in the future,
`acquire_db_connection` uses a short backoff (~0.5 s) instead of 5 s.

Clearing, in order of preference:
1. **A successful acquire clears it.** This is the real completion signal —
   MATLAB releases the lock via `scidb.close_database` when the script ends,
   and the OS releases it if MATLAB dies. To avoid clearing during the
   window between dispatch and MATLAB actually calling
   `configure_database`, only clear once at least one conflict has been
   observed under the hint.
2. **A ceiling** (default ~30 min, configurable) as the backstop.

Why 0.5 s rather than 0: MATLAB's own short writes genuinely do clear in
well under a second, and reporting an error where waiting 200 ms would have
worked is a regression. This is "5 s → 0.5 s", not "5 s → instant".

**Worst case if the hint is wrong in either direction:** stale hint → one
request reports a conflict a little eagerly, and the next successful acquire
clears it. Missing hint → today's 5 s behavior. Neither can wedge anything.

## Stage 2 — Run markers (fixes P2)

**`scimatlab/src/scimatlab/matlab/+scidb/run_marker.m`** (new). The marker
writing belongs in the MATLAB layer, not inlined into generated text by the
GUI (CLAUDE.md NOTE 3) — the GUI generator should call
`scidb.run_marker(...)`, and the file format should have one owner.

Use **`onCleanup`**, not the existing try/catch. `onCleanup` fires when its
variable goes out of scope — on normal completion, on error, and (unlike a
catch block) on most Ctrl-C interrupts, which is exactly the case a
try/catch misses.

Markers live in the OS temp dir, not next to the database: MATLAB runs on
the same machine as the extension host in this tier, and the project may be
on a slow UNC share.

- `scistack_run_<run_id>.started` — written at the **very top** of the
  script, before the pyenv preamble. That preamble `rethrow`s on failure,
  and a misconfigured `pyenv` is the single most common way these scripts
  die, so without this we cannot distinguish "never launched" from "died in
  setup".
- `scistack_run_<run_id>.done` — run_id, ok/error, message, timestamp.

**`extension/src/`** — poll for the marker (the existing
`createFileSystemWatcher` only covers workspace folders; the temp dir is
outside them). Polling ~1 s is simpler and more portable than `fs.watch`
here, and these files are short-lived. On marker: emit the **real**
`run_done` with MATLAB's actual success/failure, and call
`matlabRuns.end(runId)` — at which point Stage 3 of the previous plan (the
deferred DAG refresh) finally works for terminal runs too.

**`matlabTerminal.ts`** — also fix the stable-filename bug found here: it
writes every script to `os.tmpdir()/scistack_run.m` and then sends
`run('<path>')`. Two runs dispatched close together and the second
overwrites the first before MATLAB has read it. Scope the script file by
run_id like the markers.

**Ceiling behavior.** If no marker arrives within the ceiling, emit
`run_done` with an explicit *completion unknown* state — never
`success: true`. This is a UI change: MATLAB nodes currently go straight
from "⏳ Running…" to done, and will now genuinely sit in "Running…" until
the marker. A killed MATLAB would spin until the ceiling, so the node needs
a "stop waiting" affordance. **This is the main UX regression risk in the
plan** and the reason Stage 2 is bigger than it first looks.

## Deliberately not doing

**Do not reuse `external_db_access` for terminal runs.** It is the natural-
looking move once Stage 1 gives a start signal, and it is a trap: it marks
the database *explicitly owned* and refuses acquisition outright. Paired
with an end signal that depends on MATLAB cooperating (Stage 2's marker,
which Ctrl-C and crashes can skip), a missed end locks the GUI out of its
own database until restart. The sidecar tier can use it safely only because
Python owns that process and its `finally` always runs.

Stage 1's timestamp hint gets ~90% of the benefit with none of that risk.

## Failure modes

| # | Failure | Effect | Mitigation |
| --- | --- | --- | --- |
| 1 | Hint set, MATLAB never actually runs (user closed terminal) | Brief over-eager conflict reports | Cleared by first successful acquire; ceiling backstop |
| 2 | Hint cleared too early (dispatched, MATLAB not yet at `configure_database`) | Back to 5 s waits | Require one observed conflict before clearing |
| 3 | Ctrl-C in MATLAB | try/catch skipped, no `.done` | `onCleanup` covers most interrupts; ceiling covers the rest |
| 4 | MATLAB crashes / killed | No `.done` | `.started` present + lock free ⇒ almost certainly dead; ceiling |
| 5 | Script dies in the pyenv preamble | No `.done` | `.started` written before the preamble distinguishes this |
| 6 | Stale markers from a previous session | Wrong run reported done | run_id in the filename + timestamp newer than dispatch |
| 7 | Two runs dispatched quickly | Second clobbers the first's script | Per-run_id script filename (pre-existing bug, fixed here) |
| 8 | Tier 4 (clipboard) run pasted manually | Marker for an untracked run_id | Ignore unknown run_ids |
| 9 | Remote/SSH VS Code, or MATLAB on another machine | Temp dir not shared; markers never seen | Detect and degrade to current dispatch-is-done behavior |
| 10 | Ceiling hit on a genuinely long run | Node reports "completion unknown" mid-run | Generous, configurable ceiling; never report false success |
| 11 | Pipeline steps open/close the DB between steps | Hint clears and re-arms repeatedly | Harmless — whenever the DB is free the GUI *should* use it |

## Testing

- **Python** (no MATLAB needed): the hint state machine — arm, clear on
  success-after-conflict, don't clear on success-before-conflict, ceiling
  expiry. Unit tests in `tests/test_db_lifecycle.py`.
- **Generated script** (no MATLAB needed): string assertions that
  `scidb.run_marker` appears before the preamble and under `onCleanup`,
  matching the existing style in `tests/test_matlab.py`.
- **Extension** (`node --test`): marker parsing, stale-marker rejection,
  unknown-run_id rejection, ceiling — as pure functions alongside
  `matlabRunTracker.test.ts`.
- **Manual, needs the user's MATLAB**: normal completion, a failing script,
  Ctrl-C mid-run, killing MATLAB outright, and two runs dispatched back to
  back.

## Recommendation

Stage 1 is a few hours and fixes the papercut with no MATLAB-side changes
and no UX risk. Stage 2 is the larger piece: it fixes a correctness problem
(the GUI reporting success for runs it knows nothing about), touches the
MATLAB layer, and carries a real UX regression risk around runs that never
report completion.

Suggested order: Stage 1 now if the 5 s waits are irritating in practice;
Stage 2 when the false "done" actually costs something — e.g. the first
time a MATLAB run fails and the GUI shows it as green.

## Interim fix (2026-09-03) — connection-gate heuristic

P2 first bit the user in its most visible form: the *very first* Run click
on a MATLAB node in a session only opens the MATLAB terminal (starting the
connection) — MATLAB hasn't executed a line — yet `handleMatlabRun`
reported it as a successful run.

Rather than build Stage 2 immediately, shipped a narrower, GUI-only heuristic
that fixes this specific cold-start case without touching MATLAB:

- `matlabConnectionGate.ts` (new, vscode-free, unit-tested) —
  `needsMatlabConnectionPrompt(extensionAvailable, terminalAlreadyOpen)`.
- `matlabTerminal.ts` — new `isMatlabTerminalOpen()`, checks
  `vscode.window.terminals` for a `MATLAB`-named terminal.
- `dagPanel.ts` — new `gateOnMatlabConnection()`, called at the top of both
  `handleMatlabRun` and `handleMatlabPipelineRun`, before `beginMatlabRun`.
  When gated: prompts "Connect now?", optionally calls
  `matlab.openCommandWindow` alone (no script sent), and resolves the click
  as `run_done { success: false, cancelled: true }` — the existing
  `cancelled` status in `RunLogContext.tsx`, distinct from both success and
  error, so it doesn't read as a successful *or* a failed run.

**What this does not fix** (still needs Stage 2): a real run dispatched
after MATLAB is already connected that then fails, or a click sent to a
terminal that exists but whose MATLAB process is still mid-startup — both
still synthesize `success: true` on dispatch. `isMatlabTerminalOpen()` is a
heuristic (terminal object exists), not a true readiness signal — the
MathWorks extension has no public API for that. Revisit Stage 2 the next
time a real run's failure shows green.
