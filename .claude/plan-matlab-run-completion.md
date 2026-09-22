# Closing the MATLAB terminal run-tracking gap

**Goal.** For a MATLAB run dispatched to the MathWorks terminal, answer two
questions the GUI currently cannot:

1. **Is MATLAB still running this?**
2. **Did it succeed or fail?**

**Status:** ALL STAGES IMPLEMENTED 2026-09-22 (uncommitted). Doc:
`docs/claude/matlab-run-completion.md`. Manual-testing item 0m in
`docs/gui-manual-testing-todo.md`. Supersedes
`.claude/plan-matlab-terminal-run-tracking.md` entirely.

| Stage | State |
|---|---|
| 0 Diagnostics | done — marker-lifecycle logging, `SciStack: Show MATLAB Run State` |
| 1 Marker, MATLAB side | done — `+scidb/run_marker.m`, emitted by both generators |
| 2 Watcher, Python side | done — `matlab_run_watch.py`, 3 new RPC rows |
| 3 Liveness / unknown | done — `db.probe_lock_holder`, `pid_alive`, `classify` |
| 4 Adaptive backoff | done — `ACQUIRE_RETRY_TIMEOUT_TRACKED` |
| 5 Spend the signal | done — real deferred refresh, exact engine gate, ✕ stop-waiting |

**Departure from the plan as written:** `onCleanup` turned out NOT to fire at
script end. The GUI dispatches with `run('<file>.m')`, which evaluates in the
caller's workspace, so the cleanup variable outlives the script and fires only
when cleared or replaced. The deterministic report is therefore an explicit
`finish` call on both the success and the catch paths; `onCleanup` is kept as
a net for Ctrl-C, and reports that run as interrupted at the START of the next
run. The window in between is covered by leg B, which needs nothing from
MATLAB.

---

## 1. What is wrong today

`dagPanel.ts::handleMatlabRun` synthesises the answer:

```ts
const tier = await this.dispatchMatlabCommand(command, runId, undefined);
if (tier !== 'sidecar') { finish(true); }
```

`finish(true)` fires the moment the script text is handed to the terminal.
So the Runs console reports **success before MATLAB has executed a line**,
and reports success when MATLAB then fails. Three further things are broken
by the same missing signal, and all three are fixed by fixing it once:

* `MatlabRunTracker`'s begin/end spans the hand-off, not the run, so the
  DuckDB file-watcher still fires `dag_updated` *during* terminal runs —
  the very requests that can only fail (`docs/claude/gui-multi-session.md`
  §8).
* the cross-session MATLAB engine gate (`refuseIfMatlabBusyElsewhere`) can
  only cover the dispatch window.
* a GUI request that hits the lock cannot tell a 200 ms write from a
  20-minute run, so it backs off `ACQUIRE_RETRY_TIMEOUT` (5 s) before
  reporting. Clicking around during a run costs ~5 s per click.

The sidecar tier has none of these problems: Python owns that process.

### "Exit code" — what we can and cannot promise

In the terminal tier there is **no exit code**. MATLAB stays alive; the
*script* ends. So "success" means *the script ran to completion with no
uncaught error*, and "failure" carries MATLAB's `MException` identifier and
message — strictly more useful than an integer. A real process exit code
exists only in the sidecar tier (`matlab -batch`), which already reports it.
Worth stating plainly so the Runs console's wording does not over-promise.

---

## 2. Two independent legs

The design deliberately does not rest on MATLAB cooperating, because MATLAB
can be Ctrl-C'd, wedged, or killed.

| Leg | Signal | Answers | Needs MATLAB to cooperate? |
|---|---|---|---|
| **A — markers** | a file the script writes | **success/failure** + message | yes (one call at each end) |
| **B — lock holder** | who holds the DuckDB file, and is that PID alive | **still running?** | **no** |

Leg B is the discovery that makes this tractable. `db.py` already parses the
holder out of DuckDB's conflict message — both spellings, POSIX and Windows
— into `DatabaseLockedError.holder` / `.pid`. A failed open therefore
already tells us *which process* owns the database. Asking the OS whether
that PID is alive costs nothing and cannot be defeated by MATLAB dying.

So:

* `.done` marker present → authoritative answer to Q2.
* no `.done`, lock held by a live PID → **still running** (Q1).
* no `.done`, lock free or holder PID gone → **MATLAB stopped without
  reporting** — never reported as success.

### Why not a heartbeat (for now)

MATLAB's `for_each` already has a per-combination progress loop
(`+scifor/for_each.m`, `PROGRESS_MIN_INTERVAL_S`), so a heartbeat hook is
available and would give per-iteration liveness. Deferred: leg B answers Q1
without touching the lowest MATLAB layer, and the run holds the DuckDB lock
for its whole length (`configure_database` … `close_database`). Revisit if
runs that legitimately release the lock mid-way turn out to matter.

---

## 3. Decisions (user, 2026-09-22)

| Question | Decision |
|---|---|
| Completion genuinely undeterminable | **A distinct "unknown" state.** Never success, never a bare failure. `RunLogContext` already carries `running \| cancelling \| cancelled \| done \| error`; this adds one. |
| Marker location | **Beside the database** — `<db>.runs/`, the same convention as `scidb.log` (`scidb.log.log_path_for`). Already per-database, so two sessions cannot collide, and it survives MATLAB running on a different machine as long as the project path is shared. |
| Heartbeat | **Deferred** — leg B instead. |

---

## 4. Who owns what

Per CLAUDE.md NOTE 3, this is not a GUI-layer problem: "did this run
finish, and did it work" is a statement about a scidb run.

| Piece | Layer | Why |
|---|---|---|
| marker format + writer | `scimatlab` (`+scidb/run_marker.m`) | MATLAB is what writes it; one owner for the format |
| marker reader + watcher | `scistack_gui` Python server | it already owns run state and emits `run_output`/`run_done`; it is also the only layer that can ask about the DuckDB lock |
| lock-holder liveness | `scistack_gui/db.py` | the conflict parsing already lives there |
| dispatch + the "unknown" UI | extension host / frontend | genuinely GUI |

**The payoff of putting the watcher in Python:** terminal runs then finish
through the *same* `run_done` notification the sidecar tier already uses.
The frontend, `MatlabRunTracker` and the engine gate need no new concepts —
they just start receiving a signal that was previously synthesised.

`.started` must be written **before the pyenv preamble** (a misconfigured
`pyenv` is the commonest way these scripts die), so it cannot use `py.*` and
is plain `fopen`/`fprintf` MATLAB. That rules out reusing a Python writer.

---

## 5. Stages

### Stage 0 — Diagnostics first (CLAUDE.md NOTE 2)

Nothing here is observable today, which is why the gap survived this long.

* `scidb.log`: one line per marker transition — dispatched, `.started` seen,
  `.done` seen (with status), watcher timeout, lock probe result with the
  holder PID and whether it is alive.
* New command **SciStack: Show MATLAB Run State** — for every tracked run:
  run_id, database, dispatch time, marker path, whether each marker exists,
  last lock probe. This is what to ask for when a node is stuck.
* The existing preamble timing (`[SciStack][timing] script_start`) already
  brackets the dispatch; the markers extend that record to the whole run.

### Stage 1 — The marker, MATLAB side

`scimatlab/src/scimatlab/matlab/+scidb/run_marker.m` (new). One owner for
the format; the GUI generator calls it rather than inlining text.

* `scidb.run_marker('begin', dir, run_id)` — writes `<run_id>.started`
  (run_id, MATLAB PID via `feature('getpid')`, ISO timestamp), at the very
  top of the script, before the preamble.
* `scidb.run_marker('finish', dir, run_id, ok, identifier, message)` —
  writes `<run_id>.done`.

**Use `onCleanup`, not the existing try/catch.** `onCleanup` fires on normal
completion, on error, *and* on most Ctrl-C interrupts — which a catch block
misses, and Ctrl-C is exactly the case that otherwise leaves a run unknown
forever. The existing try/catch stays for `close_database`; the cleanup
object is what guarantees a `.done`.

One line of JSON per file, so the reader is `json.loads` and nothing else.

`api/matlab_command.py` emits the calls in all three generators
(single-run, pipeline, and the third at ~line 863).

**Free side effect:** a clipboard-tier run — the script pasted by hand —
writes the same markers, so it reports properly too. Today it is a black
hole.

### Stage 2 — The watcher, Python side

* new RPC `matlab_terminal_run_started { run_id, marker_dir }`, declared as
  a `Handler` row like every other method. Called by `dagPanel.ts` right
  after a successful terminal/clipboard dispatch, fire-and-forget.
* a watcher thread per tracked run: poll the marker directory (~1 s).
  Polling rather than a file-system watcher because the directory may be a
  network share and these files are short-lived and few.
* on `.done`: emit the **real** `run_done { success, error }` on that
  run_id — the same frame the sidecar emits.
* stale-marker rejection: the marker's timestamp must be newer than the
  dispatch, and the run_id must be one we are tracking (an unknown run_id
  is ignored, not reported).

### Stage 3 — Liveness, and the "unknown" verdict

* `db.py`: `probe_lock_holder()` → `(pid, holder, alive)` using the existing
  conflict parsing plus a `pid_alive()` check (`os.kill(pid, 0)` on POSIX,
  `OpenProcess` via `ctypes` on Windows — or `psutil` if already a
  dependency; check before adding one).
* the watcher consults it when no `.done` has arrived:
  * lock held by a live PID → still running, keep waiting, no ceiling
    pressure. **This is the answer to Q1.**
  * lock free, or the holder PID is gone → wait one grace period (a run
    that just closed the database may be mid-`onCleanup`), then emit
    `run_done { success: false, unknown: true, reason }`.
* a ceiling (configurable, default generous) as the last backstop.

### Stage 4 — Adaptive backoff (the old plan's Stage 1)

Now nearly free, because Stage 2 already tells Python a terminal run is in
flight. While a run is tracked and the lock is held by a live PID,
`acquire_db_connection` backs off ~0.5 s instead of 5 s before reporting a
conflict.

**Design constraint, carried over verbatim from the old plan:** a hint may
change *how long* the GUI waits, never *whether it attempts*. And
`external_db_access` must **not** be reused for terminal runs — it marks the
database explicitly owned and refuses acquisition outright, so a missed end
signal locks the GUI out of its own database until restart. Leg B's
liveness check does not have that failure mode because a dead PID is
self-evident.

### Stage 5 — Spend the signal

Everything that was working around the missing signal now gets it:

* `MatlabRunTracker.begin`/`end` span the real run, so the deferred DAG
  refresh works for terminal runs — delete the "dispatch is done"
  compromise in `handleMatlabRun` / `handleMatlabPipelineRun`.
* `refuseIfMatlabBusyElsewhere` becomes exact rather than dispatch-window
  only: `sharedEngineActive` now stays true for the run's real duration.
  `docs/claude/gui-multi-session.md` §8 can be rewritten from "known limit"
  to "how it works".
* the node's "⏳ Running…" becomes true rather than decorative, so it needs
  a **Stop waiting** affordance for the unknown case (the main UX change).

---

## 6. Failure modes

| # | Failure | Effect | Mitigation |
|---|---|---|---|
| 1 | Ctrl-C mid-run | `.done` may be skipped by a catch block | `onCleanup` covers most interrupts; leg B covers the rest |
| 2 | MATLAB killed / crashes | no `.done` | holder PID gone ⇒ "unknown", not success |
| 3 | Script dies in the pyenv preamble | no `.done`, no work done | `.started` written *before* the preamble distinguishes "never launched" from "died in setup" |
| 4 | Stale markers from a previous session | wrong run reported done | run_id in the filename + timestamp newer than dispatch |
| 5 | Clipboard run never pasted | `.started` never appears | lock never taken ⇒ "unknown" after the grace period |
| 6 | MATLAB on another machine | markers land on the shared project path (the decision above), but the PID is meaningless there | detect a remote workspace; fall back to marker-only, and say so in the run state |
| 7 | Two runs dispatched quickly | second clobbers the first's script file | script filename already per-session (`sessionSlug`); **narrow it to per-run** in Stage 1 |
| 8 | Marker directory not writable | no markers ever | `.started` write failure is logged and reported at dispatch, not silently |
| 9 | Run legitimately releases the lock mid-way | looks dead while alive | grace period; heartbeat if this proves real |
| 10 | Watcher thread leaks on a run that never ends | one thread per stuck run | ceiling always terminates the watcher |

---

## 7. Testing

* **Python, no MATLAB needed:** marker parsing, stale/unknown rejection, the
  state machine (running → done / unknown), `pid_alive`, the backoff hint.
  `tests/test_matlab_run_markers.py` (new), `tests/test_db_lifecycle.py`.
* **Generated script, no MATLAB needed:** string assertions that
  `scidb.run_marker('begin', …)` precedes the preamble and that the finish
  call is under `onCleanup` — the existing style in `tests/test_matlab.py`.
* **Extension, `node --test`:** the dispatch→RPC handoff and the "unknown"
  rendering decision as pure functions.
* **MATLAB suite:** `run_marker.m` round-trips a file the Python reader
  parses — one fixture, both sides, per the hash-recipe precedent.
* **Manual (needs the user's MATLAB), the whole point:** normal completion;
  a script that errors; Ctrl-C mid-run; killing MATLAB outright; a script
  pasted from the clipboard; two runs back to back; and a run left to finish
  while clicking around the GUI (should no longer cost 5 s per click).
