# Knowing when a MATLAB run ends, and whether it worked

How the GUI finds out what happened to a MATLAB run it dispatched to the
MathWorks terminal — a question it could not answer at all until
2026-09-22. Plan: `.claude/plan-matlab-run-completion.md`.

---

## 1. What it replaces

`dagPanel.ts::handleMatlabRun` used to synthesise the answer:

```ts
const tier = await this.dispatchMatlabCommand(command, runId, undefined);
if (tier !== 'sidecar') { finish(true); }
```

`finish(true)` fired the moment the script text reached the terminal. The
Runs console therefore reported **success before MATLAB had executed a
line**, and reported success when MATLAB then failed. Three other things
were broken by the same missing signal, and all three came right when it
was fixed:

* `MatlabRunTracker`'s begin/end spanned the hand-off, not the run, so the
  `.duckdb` file-watcher fired `dag_updated` *during* terminal runs — the
  very requests that can only fail;
* the cross-session MATLAB engine gate could only cover the dispatch window;
* `acquire_db_connection` could not tell a 200 ms MATLAB write from a
  20-minute run, so every click during a run cost 5 s before reporting.

The sidecar tier never had any of this: Python owns that process.

### "Exit code" is the wrong word here

In the terminal tier there is no exit code. MATLAB stays alive; the
*script* ends. So **success means the script ran to completion with no
uncaught error**, and failure carries MATLAB's `MException` identifier and
message — more useful than an integer. A real process exit code exists only
in the sidecar tier (`matlab -batch`), which already reports one.

---

## 2. Two signals, because MATLAB can die

| Leg | Signal | Answers | Needs MATLAB to cooperate? |
|---|---|---|---|
| **A — markers** | files the script writes (`scidb.run_markers`) | **how it ended** | yes |
| **B — lock holder** | who holds the DuckDB file, and is that PID alive (`db.probe_lock_holder`) | **is it still going** | **no** |

| `.done` | lock | verdict |
|---|---|---|
| present | — | success / failure, with MATLAB's own message |
| absent | held by a live process | still running — keep waiting |
| absent | free, or holder gone | **unknown** — never success |

Leg B is what makes this tractable. `db.py` already parsed the holder out
of DuckDB's conflict message — both the POSIX and the Windows spelling —
into `DatabaseLockedError.holder` / `.pid`. A failed open therefore already
told us *which process* owns the database; asking the OS whether that PID
exists costs nothing and cannot be defeated by MATLAB dying.

`probe_lock_holder` opens a throwaway **read-only** connection: DuckDB
allows many readers or one writer, so it succeeds exactly when nothing is
writing. It touches neither `_db` nor the refcount, so a watcher thread can
call it at any time. It reports `free` when *we* hold the database — this
process is not an answer to "is MATLAB still running".

### Why "unknown" is its own verdict

A marker lost to a network share is not a broken analysis. Reporting
failure would send the user to debug working code; reporting success is the
original bug. `RunLogContext` gained a fifth status (`unknown`, rendered
`?` in amber) and the run's row says what was and was not kept.

---

## 3. The markers

`scidb.run_markers` owns the directory and the format; `scimatlab`'s
`+scidb/run_marker.m` writes them; the GUI server reads them. In scidb
rather than in the GUI because "did this run finish, and did it work" is a
statement about a scidb run, not about a webview (CLAUDE.md NOTE 3).

* `<db stem>.runs/<run_id>.started` — written at the **very top of the
  generated script, before the pyenv preamble**. That ordering is the whole
  point: the preamble rethrows, and a misconfigured `pyenv` is the commonest
  way these scripts die, so a marker written after it could never tell
  "MATLAB never launched the script" from "the script died in setup".
* `<db stem>.runs/<run_id>.done` — `ok`, `interrupted`, MATLAB's
  `identifier` and `message`.

Beside the database, matching `scidb.log` and `<stem>.layout.json`:
per-database by construction, and it still works when MATLAB runs on a
different machine as long as the project path is shared.

One line of JSON, written to a temp file and renamed, so a polling reader
sees a complete file or none. The reader tolerates a half-written file
anyway (unparseable means "look again"), and **`ok` absent is not success**.

### onCleanup, and exactly what it does not cover

The script calls `finish` on its success path and again in its `catch`,
where MATLAB's own identifier and message are known. `begin` also returns an
`onCleanup` object as a best-effort net for the path a catch cannot see:
Ctrl-C, which MATLAB does not make catchable.

**Be precise about when that fires**, because the obvious reading is wrong:
the GUI dispatches with `run('<file>.m')`, which evaluates the script in the
*caller's* workspace, so the cleanup variable outlives the script. It fires
when that variable is cleared or replaced — which the script does itself on
success, and which the *next* run's `begin` does for a run that was
interrupted. A Ctrl-C'd run is therefore reported as interrupted at the
start of the following run, not when it was interrupted.

The window in between is covered by leg B, which needs nothing from MATLAB.

---

## 4. The watcher

`scistack_gui.matlab_run_watch`, one daemon thread per tracked run. The
thread observes; `classify()` decides, as a pure function so the verdicts
are tested without threads or MATLAB.

`classify` checks the report **first**, deliberately: a run that finished
between two polls has released the database, so the liveness half would
otherwise read the act of finishing as "stopped without reporting".

Grace periods, each earning its place:

| Constant | Default | Why |
|---|---|---|
| `POLL_INTERVAL_S` | 1 s | imperceptible next to a MATLAB run |
| `STARTUP_GRACE_S` | 120 s | MATLAB must parse the script and load `pyenv`, which is slow cold on Windows |
| `IDLE_GRACE_S` | 20 s | a run that has just called `close_database` is briefly lock-free while its marker is written |
| `CEILING_S` | 12 h | backstop; exceeding it on a real run reports "unknown" for work that is fine, so it is generous |

Verdicts reach the frontend as the **same** `run_output` / `run_done` frames
the sidecar tier already emits, so nothing downstream needed a new concept —
the signal simply became real. `run_done` gained one field, `unknown`.

---

## 5. What the signal bought

* **The deferred DAG refresh works.** `MatlabRunTracker` now spans the real
  run, so the file-watcher stops firing doomed refreshes mid-run.
* **The cross-session engine gate is exact** (`docs/claude/gui-multi-session.md`
  §7), not dispatch-window-only.
* **Clicking during a run is ~10× faster.** `db.ACQUIRE_RETRY_TIMEOUT_TRACKED`
  (0.5 s) replaces the 5 s backoff while a watched run holds the database.
  Deliberately not zero: if the hint is ever wrong, a 200 ms MATLAB write
  that would have succeeded must not start reporting failures. The hint may
  change how long we *wait*, never whether we *attempt*.
* **Clipboard runs stopped being a black hole.** A script pasted by hand
  writes the same markers, so it reports like any other run.
* **The canvas repaints when the run ends** (2026-09-22). See below — this
  one did not arrive with the rest and had to be added.

### The completion path had to announce that records changed

`_finish` delivered the verdict and stopped there. Every *in-process* run path
ends with `api/run._notify_records_changed()` — canvas refetch, Plot Studio
cache drop — but `_start_matlab_run` returns `host_execution_required` and
exits without spawning a thread, so on this side there was no caller. Measured
on a real session: **4 `dag_updated` messages in 55 minutes against 9 runs,
none of them after a run.** Node colours stayed as they were until something
else happened to refresh the canvas, and Plot Studio kept serving pre-run
frames.

`_finish` now calls it, on **every** verdict. The message means "records may
have changed", which is true of a run that failed halfway (it saved what it
got to) and of one classified unknown. Announcing too often costs a refetch;
announcing too rarely was the bug. It is wrapped, because a failed
announcement must never look like a failed run — a missed refresh is a stale
canvas, not lost data.

The verdict is pushed **before** the refresh, so a refetch cannot race the
run's own result. `test_every_verdict_announces_that_records_changed` asserts
the ordering, not just the presence.

`_notify_records_changed` gained a third job in the same pass:
`scidb.state.clear_discovery_cache()`. That function's docstring has said
"For tests, and after a run" since it was written, and only tests called it.
A loader's discovery cache holds "files on disk minus locations already
realized" — a run changes the second half while the filesystem sits still, so
nothing but the 5-second TTL ever refreshed it. Putting the call there fixes
every run path at once.

**No routing gap here**, which was worth confirming rather than assuming:
`matlab_run_watch` pushes through `notify.push_message` (JSON-RPC) while
`api/run` binds `ws.push_message` at import, but `ws.push_message` delegates
to the JSON-RPC one when the extension host is driving. The two converge.
(Contrast `gui-notification-routing.md` §"A third source", where they do not.)

### The UX change this forces

A MATLAB node used to flick to green almost instantly. It now sits on
"⏳ Running in MATLAB…" for the real duration, which is correct and will feel
slower. Because a node can now genuinely wait, it has a **✕ stop waiting**
control — which does *not* stop MATLAB (nothing here can reach into that
terminal); it stops the GUI waiting, for when MATLAB was closed or
interrupted and will never report back.

---

## 6. Diagnostics

**SciStack: Show MATLAB Run State** prints, per tracked run: run_id, label,
database, marker directory, whether each marker exists, MATLAB's PID,
seconds since dispatch, and the last lock probe. "Why is this node still
spinning?" has no other answer — the run is in another process, behind a
terminal nothing can query.

`scidb.log` carries the same transitions: tracking started, `.started` seen
with the PID, the verdict and why.

Stale markers (a MATLAB killed mid-run leaves its `.started` behind) are
swept at server startup, older than a week.

---

## 7. Deliberately not done

**`external_db_access` is still not reused for terminal runs.** It marks the
database explicitly owned and refuses acquisition outright; paired with an
end signal MATLAB can skip, a missed end would lock the GUI out of its own
database until restart. Leg B has no such failure mode — a dead PID is
self-evident — which is why the backoff hint only shortens a wait.

**No heartbeat.** MATLAB's `for_each` has a per-combination progress loop
(`+scifor/for_each.m`) that could touch the marker, giving per-iteration
liveness. Not needed: the run holds the DuckDB lock from
`configure_database` to `close_database`, so leg B already answers "still
going" without touching the lowest MATLAB layer. Revisit if runs that
legitimately release the lock mid-way turn out to matter.
