# Logging architecture (scistacklog)

*Written 2026-07-07 alongside the logging redesign (plan: `.claude/logging-redesign-plan.md`); rewritten 2026-10-26 for the journal/debug split (plan: `.claude/scientist-facing-log-plan.md`). Read this before touching log levels, formats, handlers, or the journal anywhere in the stack.*

## Two audiences, two files

| File | Reader | Content | Written by |
|---|---|---|---|
| `scistack.log` (**journal**) | the scientist | one block per run: what ran, counts, duration, each failure labelled with **who can fix it**; data notes; failed GUI actions | `Log.journal` only (via `Log.run`, `Log.journal_note`, `Log.journal_json`) |
| `scistack-debug.log` | whoever debugs SciStack | everything, **DEBUG by default**, `run=<id>` on every line inside a run | every `Log.*` call / layer logger |

Both sit beside the database. `scidb.log.attach_log_file(db)` sets both (`log_path_for` = journal, `debug_log_path_for` = debug; the debug name is derived by `scistacklog.debug_path_for`). `configure_database` (Python and MATLAB), the GUI server and the `scidb` CLI all call it.

**Both goals, one emit stream.** Audience is a separate axis from level: a scientist never has to read a level-filtered framework log, and a debugger never has to re-run with `-v`. The debug file is a **superset**: every journal block is also logged as one INFO record (`[journal]`, layer `scistack`) to the debug file and the console. A journal block names `run=<id>`; grepping the debug file for that token gives the whole trace of that run.

## Topology

```
Log.debug/info/warn/error(msg, *args, layer=..., exc_info=...)   ← scistacklog facade
        │  emits on logging.getLogger(layer)
        ▼
layer loggers (LAYERS): scidb, scifor, sciduck, scihist, scilineage, scistack, scistack_gui, …, matlab
   • pinned to level DEBUG, propagate=True  ← NEVER filter here (caplog contract)
        │
        ├── console handler (stderr, singleton)        level: INFO default
        │      HH:MM:SS [layer] message   (level prefix only at WARN+)
        └── debug-file handler (_CappedFileHandler)     level: DEBUG default
               YYYY-MM-DD HH:MM:SS.mmm LEVEL [layer] run=<id> message   (run= only inside a run)
               rolls to .1…​.5 past DEBUG_MAX_BYTES (10 MB); a failed rename
               (Windows, another process has it open) is given up for the session

Log.run(name) ─► JournalRun ─► on exit, if a summary or an error reached it:
Log.journal(event) ─► scistacklog.journal.render(event) ─► append to scistack.log
                                                          └► Log.info("[journal]\n…", layer="scistack")
```

- **Levels live on the handlers, never on loggers.** caplog sees everything.
- `_RecordTagger` adds `layer`, `levelshort` and `runtag` (from the `_RUN` contextvar).
- `set_path(debug, journal_path=None)`; `get_path()` is the **debug** file (MATLAB tests `fileread` it), `get_journal_path()` the journal.

## The journal

- **`Log.run(name)`**: only the OUTERMOST run on a context creates a `JournalRun`; nested runs share it, so `scidb.for_each` → `scifor.for_each` (and EachOf alternatives) are one block. Written on exit only if a summary or an error reached it, so a dry run or a pipeline step that was only registered writes nothing.
- **`scifor.failures.journal_run` / `journaled`**: the decorator on both `scifor.for_each` and `scidb.for_each`. It classifies an escaping exception (`run_failure` → blame) before `Log.run` sees it; an `on_error="stop"` exception calls `run.fail(None)` because the summary already holds it. An exception nobody classified is recorded neutrally ("The run failed"), never as "internal".
- What feeds a block: scifor's summary event (`journal.add_summary`, same dict as the `summary` progress event), scidb's `count("saved", n)` (in `_save_results`) and `count("up_to_date", n)` (skip_computed).
- **Rendering** (`scistacklog/journal.py`): plain dicts in, text out, never raises. `blame_headline` is the ONE wording of each blame category. It is also sent to the GUI as `blame_label` on every failure group, so the TS code holds no copy.
- `Log.journal_note(text, key=)`: once per key per process. Used by scistackplotdb's field-coverage check (one note per variable + column set, instead of a WARN per column per load).
- `Log.journal_json(text)`: the MATLAB entry. `+scifor/for_each.m`'s `write_journal` builds the same event and calls it after the outputs are built (or, with `error`, when building them fails).
- GUI actions: `server._journal_action_failure` writes `action_failed` for a failed non-read RPC, and puts ` [run=<id>]` on the debug-file `RPC << … FAILED` line. Read RPCs (`get_*`/`list_*`) are left out: they repeat on every refresh.

## Level policy (debug file and console)

Unchanged triage rule: **INFO only if O(1) per user-visible operation**; per-iteration / per-column / internals = DEBUG; WARN = recoverable anomaly worth seeing once (first occurrence of each failure reason, with traceback); ERROR = failed operation, with traceback. The INFO/DEBUG split now decides only what the **console** shows and what a deliberately quieted debug file keeps. The scientist's view is the journal, so there was no reason to demote the GUI's 747 INFO call sites (a sweep was considered and not done).

**Naming: snake_case operation names, never `Step N`.** Grep tags `[timing]`, `[batch_save]`, `[journal]`, `[seam_fallback]`, `[run]` are stable.

## Instrumentation helpers

- `Log.step(name)`: `→`/`←` at DEBUG, ERROR + traceback on failure.
- `Log.timer(name, live=)` / `Log.timings(...)`: one `[timing] … TOTAL=` INFO line + DEBUG phase table (now in the debug file by default).
- `Log.trial(label)`: demote INFO/WARN to DEBUG for discarded work.

## Per-host behavior

| Host | Console | Notes |
|---|---|---|
| Python script | INFO narrative + journal blocks on stderr | `configure_database` attaches both files |
| `scidb` CLI | **WARN** unless `-v` | errors: one `Error: …` line to stderr + Log.error to file |
| GUI (JSON-RPC server) | DEBUG (stderr → VS Code Output Channel) | no `logging.basicConfig`; run console fed by `_RunLogRelay` on `scifor`/`scidb` loggers only (the `[journal]` mirror is layer `scistack`, so it is not relayed). The extension's "Open log" opens the journal. |
| MATLAB | same files | `+scidb/Log.m` gates client-side on `get_level()` (min of sinks), which is now **DEBUG**: every MATLAB `Log.debug` crosses the bridge. Watch `TestForEachTimingInstrumentation` for the cost. MATLAB tests read `scidb.Log.get_path()` = the debug file. |

Config: `SCIDB_LOG_LEVEL` (both sinks, at attach), `Log.set_level(level, sink)`, CLI `-v`.

## Testing contracts

- `scistacklog/tests/test_log.py`: formats, sink independence, caplog. Its fixture sets the file to INFO, because these tests pin what each level lets through. `test_default_levels_info_console_debug_file` pins the real defaults.
- `scistacklog/tests/test_journal.py`: two files, superset, one block per outermost run, `run=` token, neutral unclassified errors, renderer, notes, MATLAB events, rollover, single-writer source guard.
- `scifor/tests/test_blame.py`: every blame category from real runs, wire round trip, seam (precise / probable / pass-through), AST guard that the combine step is inside the seam, run-level error in the block.
- `scidb/tests/test_log_shim.py`: shim identity, `attach_log_file` paths, `[timing]` in the debug file, internals in the debug file and never the journal.

## Known trade-offs

- MATLAB: an EachOf input writes one block per alternative, and MATLAB-originated debug lines carry no `run=` token (the Python run context does not span MATLAB code).
- MATLAB: a built-in passed directly as the function (`@readtable`) that fails is classified `internal` (there is no MATLAB equivalent of `call_user_function`'s marker frame).
- A GUI action failure whose exception carries no marker is rendered with hedged wording ("If the message doesn't say what to change, this is likely a SciStack bug"), because the GUI's ~90 deliberate `raise ValueError` refusals are not marked yet. `layout_service.EdgeCycleError` is the first that is.
- The debug file at DEBUG costs disk and some time per record; rollover caps the disk.
