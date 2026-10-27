# Logging architecture (scistacklog)

*Written 2026-07-07 alongside the logging redesign (plan: `.claude/logging-redesign-plan.md`). Read this before touching log levels, formats, or handlers anywhere in the stack.*

## The two goals

1. **Pipeline documentation**: a run's log answers "what ran, what failed and why, what was skipped" — at the default level, in both the console and `scistack.log`.
2. **Framework debugging**: at DEBUG, the file becomes a nested execution trace with per-step durations, per-iteration detail, and `[timing]` phase breakdowns.

The default file is deliberately NOT "everything": both sinks default to INFO, and DEBUG is opt-in. Compensation so the INFO file still answers "why did it fail": the first occurrence of each distinct failure reason logs at WARN **with traceback**, and the end-of-run summary aggregates all failures by reason.

## Topology

```
Log.debug/info/warn/error(msg, *args, layer=..., exc_info=...)   ← scistacklog facade
        │  emits on logging.getLogger(layer)
        ▼
layer loggers: scidb, scifor, sciduck, scihist, scilineage,
               scistack, scistack_gui, matlab        (LAYERS tuple)
   • pinned to level DEBUG, propagate=True  ← NEVER filter here (caplog contract)
   • module loggers (scidb.discover, scilineage.hashing, …) are children — free coverage
        │
        ├── console handler (stderr, singleton)   level: INFO default
        │      format: HH:MM:SS [layer] message   (level prefix only at WARN+)
        └── file handler (scistack.log, singleton)   level: INFO default
               format: YYYY-MM-DD HH:MM:SS.mmm LEVEL [layer] message
               one record per line; WARN not WARNING; FileHandler(delay=True)
```

- **Levels live on the handlers, never on loggers.** Records always propagate to root, so pytest `caplog` sees everything regardless of sink levels. The default `layer="scidb"` emits on logger `"scidb"` — the historical caplog contract.
- `Log.attach()` is idempotent and lazy (first emit / `set_path`); nothing happens at import. `bridge_python_logging()` is a deprecated alias kept for the GUI.
- `scidb/src/scidb/log.py` is a **re-export shim** — MATLAB's `py.scidb.log.Log.*` delegation and `from scidb.log import Log` imports are public contracts.
- `set_path` swaps the FileHandler (`delay=True` → the file may legitimately never exist if everything was level-suppressed).

## Level policy (the triage rule)

**INFO only if O(1) per user-visible operation** (a for_each run, one save/load call, one CLI invocation). Per-iteration / per-column / per-phase / internals = DEBUG. WARN = recoverable anomaly worth seeing once (includes first-occurrence iteration failures, with traceback). ERROR = a failed operation, always with traceback to the file.

INFO inventory (keep it small): for_each banner + inputs/options, periodic progress, run summary + per-reason failure lines, `skip_computed: N/M`, `filtered N non-existent combos`, `loaded N inputs`, `saved -> record_id` (direct saves), capped `[save]` lines, `[batch_save]` Preparing/Completed (test-pinned), `[draft]` endpoint notices, `[provenance] recorded run_id`, all `[timing]` summaries, `configure_database` run-context header.

**Naming: snake_case operation names, never `Step N` / `4a_` prefixes** (user feedback, firm). Ordering is conveyed by log sequence and nesting. Subsystem tags like `[timing]`, `[batch_save]` are stable grep targets — keep them.

## Instrumentation helpers (scistacklog)

- `with Log.step("resolve_empty_lists", layer=...):` — `→ name` / `← name done in Xs` at DEBUG; on exception an ERROR with duration + traceback, then re-raise. Wired around scidb for_each's three phases (`for_each_prepare`, `delegate_to_scifor`, `for_each_save`), so a failed run's log always ends with the error in context.
- `with Log.timer("save_batch(X)", extra="114 items") as t: / t.phase("commit")` — one `[timing] name: …TOTAL=Xs (phase=Ys, …)` INFO line + DEBUG per-phase table. Use for new hot-path timing instead of hand-rolled dicts. (`database.py`'s save_batch kept its accumulator dict — same output format — because per-row accumulation doesn't fit the context-manager shape.)

## Per-host behavior

| Host | Console policy | Notes |
|---|---|---|
| Python script | INFO narrative on stderr | `configure_database` sets file path + header |
| `scidb` CLI | **WARN** unless `-v` (stdout=results, stderr=diagnostics) | `-v` → DEBUG both sinks; errors: one `Error: …` line to stderr + Log.error to file |
| GUI (JSON-RPC server) | DEBUG console (stderr → VS Code Output Channel) | **No `logging.basicConfig`** — a root handler would double-print every layer record. Frontend run console is fed by `_RunLogRelay` (a scoped handler on the `scifor`/`scidb` loggers in `api/run.py`), NOT stdout; stdout redirect only carries dry-run output. Never attach the relay to `scistack_gui` (feedback loop via `emit()`'s own debug line). |
| MATLAB | Same file; console = Python stderr (not `evalc`-capturable) | `+scidb/Log.m` gates client-side via appdata cache (min of both sinks) and tags `layer='matlab'`. MATLAB tests must assert on `scistack.log` content (`fileread(char(scidb.Log.get_path()))`), not `evalc`. Timing tests set `set_level('DEBUG','file')` so archives keep phase tables. |

Config surface: `SCIDB_LOG_LEVEL` env var (both sinks, read once at attach), `Log.set_level(level, sink="console"|"file"|"both")`, CLI `-v`. Nothing else.

## scifor specifics

- scifor stays scidb-free; it depends only on `scistacklog` and calls `Log.*(..., layer="scifor")`.
- Periodic progress: one INFO line per **outermost-iterated-key transition** (`progress: subject=s06 (6/12) — 57/120 combos (47.5%) …`), guarded by `_PROGRESS_START_DELAY_S` (5s — fast runs emit none) and `_PROGRESS_MIN_INTERVAL_S` (2s, measured elapsed-to-elapsed from loop start). Module constants, monkeypatched in tests.
- Failure aggregation: `failure_reasons: dict[reason → [combo_str]]`, reason = `f"{type(e).__name__}: {e}"`; summary caps combos at 5 with `(+N more)`. Same logic ported to MATLAB `+scifor/for_each.m` (`record_iteration_failure` with `containers.Map`).
- `_progress_fn` gained a final `{"event": "summary", current, total, completed, failed, skipped, cancelled, failure_reasons}` event (`current` kept for positional consumers like the GUI). scidb consumes it for the authoritative `run summary:` line, folding in `skip_computed_count` from `_ForEachState`.
- `_log_fn` is deprecated and ignored everywhere (kept in signatures so call sites don't break).
- Dry-run output is `print()` to stdout by design — it is the requested result, not logging. Same for `[stat draft]` payloads and `save_batch(profile=True)` tables.

## Testing contracts

- Tests asserting per-iteration `[run]`/`[skip]`/`[recompute]` use `caplog.at_level(logging.DEBUG, logger="scifor"|"scidb")`, filtered by `record.name` when counting (scifor and scidb records both propagate to root).
- `scistacklog/tests/test_log.py` pins formats (file-line regex, time-only console), dual-sink independence, caplog-at-ERROR-sinks, idempotence, MATLAB call shapes, step/timer behavior.
- `scidb/tests/test_log_shim.py` pins the shim identity, run-context header, `[timing]`-at-INFO, internals-absent-at-INFO.
- `scifor/tests/test_logging.py` pins the summary/progress/WARN-once behavior and the no-scidb-import + no-`/tmp/scihist_diag.log` guarantees.

## Known trade-offs / future work

- MATLAB users don't see the console narrative in the command window reliably (Python stderr routing varies); the file is the source of truth there.
- `save_batch`'s timing stays hand-rolled (accumulator pattern); migrate to `Log.timer` only with a restructure that can be properly verified.
- A pre-existing bug (2 combos → 4 fn calls with `generates_file=True`) was found during this work — tracked separately, not logging-related.
