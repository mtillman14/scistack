# SciStack Logging Redesign

## Context

Core scistack functionality is stable; logging is now the bottleneck for usefulness. Logs must serve two goals: **(1)** document pipeline progress/status for users, **(2)** provide debug information for framework errors. The top-level README (line 235) still promises progress logging as a *future* feature even though partial infrastructure exists.

**Current state (surveyed):**
- `scidb/src/scidb/log.py` has a custom `Log` class: `[HH:MM:SS.mmm] [LEVEL] message` (no date, no layer) written to `scidb.log` next to the DB via open/append/close per line; every message also mirrored to stdlib logger `"scidb"` (pytest caplog depends on this). MATLAB `+scidb/Log.m` delegates to it, so both languages share one file.
- **scifor emits progress via raw `print()`** (banner, `[run]`, `[skip]`, done footer in `scifor/src/scifor/foreach.py`) — duplicated into the file via a `_log_fn=Log.info` callback, so every line is written twice by two mechanisms. It also dumps error diagnostics to a hardcoded `/tmp/scihist_diag.log`.
- Progress narrative is drowned: a 7000-iteration run produces ~7200 log lines, >95% per-iteration `[run]` lines and per-column DEBUG dumps. No progress counter, no end-of-run failure/skip aggregation.
- `bridge_python_logging` covers only `scistack_gui`/`scihist`/`sciduck`; `scistack` and `scilineage` loggers are unbridged.

**User decisions (locked):**
1. **Dual-sink split** — concise console narrative; file gets full detail.
2. **Keep the `Log` facade, stdlib logging inside** — MATLAB delegation and call sites survive.
3. **Per-iteration `[run]`/`[skip]` demoted to DEBUG everywhere** — replaced at default level by periodic progress + end-of-run summary with reasons.
4. **Extract the facade into its own lowest-level package** so scifor (and any layer) can use it directly; `scidb.log` stays as a re-export shim so MATLAB and existing imports don't change.
5. **One record per line** in the file (no hard-wrapping); long value lists truncated. Console timestamps are time-only (no date). Periodic progress keyed to schema-key iteration, not raw %. Subsystem tags (`[timing]`, `[batch_save]`) kept.

**Design note reconciling 1 & 3:** both sinks default to INFO; `-v` / `SCIDB_LOG_LEVEL=DEBUG` / `Log.set_level` opt into full DEBUG detail (per-iteration lines, timing phase tables) in the file. So "file captures everything" is the opted-in debugging mode; the default file stays a concise run document. Compensation so the default file still answers "what failed and why": first occurrence of each distinct failure reason logs at WARN **with traceback**, and the end-of-run summary aggregates all failures by reason at INFO.

---

## 0. New package: the logging layer (working name `scistacklog`)

New lowest-level package holding the `Log` facade, layer registry, formatters, and filter — so every layer (including scifor, which stays scidb-free) can import it, and standalone scifor users get working console/file logging without scidb.

- Layout mirrors siblings: `scistacklog/src/scistacklog/__init__.py` (exports `Log`, `LAYERS`), `scistacklog/pyproject.toml`, `scistacklog/tests/test_log.py`, `scistacklog/README.md`. No dependencies.
- **`scidb/src/scidb/log.py` becomes a re-export shim** (`from scistacklog import Log`): MATLAB's `py.scidb.log.Log.*` delegation and every existing `from scidb.log import Log` import keep working with zero changes.
- `scifor` and `scidb` (and later scistack-gui) add the package as a dependency; scifor calls `Log.debug(..., layer="scifor")` directly instead of a bare stdlib logger.
- **Naming**: `scilog`/`scilogger` are taken on PyPI (no network in this sandbox to verify alternatives). Candidates to check before Stage 1: `scistacklog`, `scistack-log` (dist name; import name can stay `scistacklog`), `scilogging`. Folder/import rename before Stage 1 is trivial; the plan uses `scistacklog` as placeholder.

## 1. Formats and layer naming

**Logger name = layer.** Top-level loggers: `scidb`, `scifor`, `sciduck`, `scihist`, `scilineage`, `scistack`, `scistack_gui`, `matlab`. Existing module loggers (`scidb.discover`, `scilineage.hashing`, `sciduck`, …) are already children — **no renames needed**, and `caplog.at_level(..., logger="scidb")` keeps working. Layer derived by a `logging.Filter`: `record.layer = record.name.split(".", 1)[0]`. Subsystem tags (`[timing]`, `[batch_save]`) stay as message prefixes (they're grep targets for the MATLAB timing archives); redundant hardcoded `[scidb]`/`[scifor]` prefixes inside messages are removed.

**File format** (datefmt `%Y-%m-%d %H:%M:%S` + ms). **One record per line** — grep/awk and the CLI treat line = record. Long value lists are truncated in the message (`subject=12 values [s01,…,s12]` — first/last few values shown):
```
2026-07-07 14:32:06.001 INFO  [scifor] for_each(compute_psd) — 120 iterations: subject=12 values [s01,…,s12], session=10 values [1,…,10]
2026-07-07 14:32:41.310 INFO  [scifor] progress: subject=s06 (6/12) — 57/120 combos (47.5%), completed=57, failed=0, elapsed=35.2s
2026-07-07 14:33:15.940 WARN  [scifor] iteration failed: subject=s03, session=2 — ValueError: bad channel count (traceback follows)
2026-07-07 14:33:20.008 INFO  [scifor] for_each(compute_psd) done in 74.0s: completed=114, failed=3, total=120
2026-07-07 14:33:20.009 INFO  [scifor] failed: 3 × "ValueError: bad channel count" — subject=s03 session=2, ... (+N more)
2026-07-07 14:33:20.100 INFO  [scidb] [timing] save_batch(PSD): 114 items, 12 schemas, 0.412s
```
`WARN` (not `WARNING`) via a Formatter subclass rewriting `levelname` locally — no global `addLevelName`.

**Console format** — **time-only timestamp (no date)** + layer; level shown only at WARN+ (one Formatter with two format strings on `levelno >= WARNING`):
```
14:32:06 [scifor] for_each(compute_psd) — 120 iterations: subject=12 values [s01,…,s12], session=10 values [1,…,10]
14:33:15 [scifor] WARN: iteration failed: subject=s03, session=2 — ValueError: bad channel count
```

**Console stream = stderr**: the GUI's JSON-RPC owns stdout; the CLI convention is stdout=results/stderr=diagnostics. One policy safe in every host.

## 2. `Log` facade rework — new `scistacklog` package (shimmed from `scidb/src/scidb/log.py`)

Rewrite internals on stdlib logging; keep and extend the classmethod surface:

```python
class Log:
    DEBUG = 0; INFO = 1; WARN = 2; ERROR = 3      # numeric contract unchanged (MATLAB appdata cache)

    debug/info/warn/error(msg, *args, layer="scidb", exc_info=False)
                                                   # exc_info on all levels: scifor logs per-iteration
                                                   # tracebacks at DEBUG and first-occurrence ones at WARN
    set_level(level, sink="both")                  # 'console' | 'file' | 'both'; sets HANDLER levels only
    get_level(sink=None)                           # None → min(console, file)  (MATLAB cache semantics)
    set_path(path)                                 # swap FileHandler(path, mode='a', delay=True); None detaches
    get_path()
    attach()                                       # idempotent: build handlers/formatters, attach to all layer
                                                   # loggers, pin logger levels to DEBUG, propagate=True,
                                                   # read SCIDB_LOG_LEVEL once
    bridge_python_logging()                        # kept as thin alias for attach() (GUI calls it)
```

Key rules:
- Emission: `logging.getLogger(layer).log(...)` — byte-identical caplog behavior for default layer.
- **Levels enforced on handlers, never on loggers** — loggers pinned DEBUG + propagate=True so caplog always sees records.
- Class-level handler singletons + identity check in `attach()` → idempotent, no duplicate lines.
- Delete the `_ScidbLogHandler` bridge (hierarchy replaces it; `scistack` + `scilineage` now covered).
- Lazy `attach()` on first emit/`set_path`; nothing at import time. `configure_database` (`database.py:472-475`) and CLI (`inspect/cli.py:617-618`) need no changes — `set_path` signature unchanged.
- Positional call `Log.info("x")` must keep working (MATLAB call shape).

## 3. scifor: print() → logging — `scifor/src/scifor/foreach.py`

scifor stays scidb-free but now depends on `scistacklog`: it calls `Log.debug/info/warn/error(..., layer="scifor")` directly. Standalone scifor gets console output automatically via lazy `attach()` (no scidb needed, no separate `enable_console_logging` helper).

Migration (foreach.py; same pattern in `csv_export.py`, `merge.py`):
- Banner (325-333) + inputs/metadata/options (337-363) → single/few INFO records; **delete the duplicated `_log_fn` calls**.
- `[dry-run]` header (366-371) → INFO; per-iteration dry-run lines (`_print_dry_run_iteration`) **stay `print()` to stdout** — dry-run output is the requested result, not logging (also keeps `test_foreach_standalone.py:282` green).
- `[run]` (508), `[done]`, `[empty-combo]` → `Log.debug(..., layer="scifor")` (all scifor emissions below use the facade with `layer="scifor"`).
- `[skip]` at filter failure (437-448) and fn raise (558-568) → `Log.debug(..., exc_info=True)` per iteration **plus** first-occurrence-per-reason `Log.warn(..., exc_info=True)`; reason key `f"{type(e).__name__}: {e}"`.
- `traceback.print_exc()` + stderr prints → removed (exc_info carries traceback); `ColumnFunctionError` hard path → `Log.error(..., exc_info=True)` then re-raise.
- **`/tmp/scihist_diag.log` writes (442-445, 551-565) deleted entirely.**
- Cancel message (386-392) → INFO. Done footer (616-632) → INFO summary.

**Periodic progress (INFO):** keyed to **schema-key iteration, not raw %** — emit one line each time the outermost schema key's value changes (e.g. in `[subject, trial]`, one line per new subject, at that subject's first trial): `progress: subject=s06 (6/12) — 57/120 combos (47.5%), completed=57, failed=0, elapsed=35.2s`. Guard rails for degenerate shapes: suppress if <`_PROGRESS_MIN_INTERVAL_S` (2s) since the last progress line (prevents flooding when the outer key is huge or is the only key), and skip the very first transition within `_PROGRESS_START_DELAY_S` (5s) so fast runs emit none. Constants module-level and monkeypatchable in tests.

**End-of-run summary (INFO):** accumulate `failure_reasons: dict[str, list[str]]` (reason → metadata strings) at the two failure sites; emit done line + one `failed:` line per distinct reason, combos capped at 5 with `(+N more)`.

**Callbacks:** `_progress_fn` unchanged (GUI depends on events) + new final `{"event": "summary", completed, failed, total, failure_reasons}` event. `_log_fn` kept but no longer called for migrated lines (deprecated in docstring); **`scidb/foreach.py:541` stops passing `_log_fn=Log.info`** (kills the double-write). scidb's `_progress` hook consumes `summary` and logs one authoritative INFO line folding in skip_computed: `for_each(fn) run summary: completed=114, failed=3, skipped=6 (skip_computed, up to date), total=126` — this is the README-promised line.

## 4. Level triage (existing call sites)

**Rule: INFO only if O(1) per user-visible operation (a for_each run, one save_batch, one CLI invocation). Per-iteration / per-column / per-phase / internals = DEBUG. WARN = recoverable anomaly worth seeing once; ERROR = failed operation.**

- `scidb/foreach.py`: ~40 `[scidb] Step N` narrative lines → named `Log.step` wrappers / DEBUG lines (numbering dropped per §4b convention; same for scifor's `Step 7.5/8/9` `_log_fn` lines, which are deleted or renamed in Stage 2); `skip_computed: N/M skipped` stays INFO; per-combo `[skip]`/`[recompute]` (977-979, 1067-1069) → DEBUG + delete paired prints; batch-save `Log.error` + traceback stays ERROR.
- `scidb/database.py`: `[timing]` one-line summaries (1306-1310, 1671, 2707, 2909, 2964) **stay INFO** (grep targets); per-phase tables already DEBUG; per-column/54-column dumps → DEBUG.
- `sciduckdb/sciduckdb.py`: lock ACQUIRED/RELEASED → DEBUG.

## 4b. Debugging & performance instrumentation (goal 2, first-class)

What the redesign already preserves for debugging: DEBUG file mode (`-v` / `SCIDB_LOG_LEVEL=DEBUG`) captures every internal line; tracebacks travel via `exc_info`; caplog sees everything regardless of sink levels. What's new to make debugging *systematic* rather than ad-hoc:

**`Log.step()` — internal flow tracing (in scistacklog).** Context manager giving every layer a uniform entry/exit narrative with coarse timing:
```python
with Log.step("save_batch(PSD)", layer="scidb"):   # DEBUG: "→ save_batch(PSD)"
    ...                                             # DEBUG: "← save_batch(PSD) done in 7.752s"
                                                    # on exception: ERROR "✗ save_batch(PSD) failed after 1.2s: ValueError: ..." + exc_info, then re-raise
```
**Naming convention — no more `Step N` / `Step Na` numbering anywhere.** Numbered steps aren't extensible (inserting a phase renumbers everything and stale numbers mislead). Steps are named by their operation in snake_case, scoped by the layer tag the formatter already provides: e.g. `resolve_empty_lists`, `expand_combos`, `skip_computed_check`, `delegate_to_scifor`, `wrap_batch_bridge`, `provenance_stamp`. Ordering is conveyed by the log's own sequence and nesting, not by numbers. The same applies to timing phase names: `4a_canonical_hash`/`6e_commit` → `canonical_hash`/`commit` (the timer preserves emission order; greps target the `[timing]` prefix, not phase names, so the archive workflow is unaffected).

With that convention, scidb's for_each phases, provenance save/load, scilineage hashing, scihist merge, and the MATLAB bridge conversions each get `Log.step` wrappers at their public operation boundaries. At default (INFO) these are silent; at DEBUG the file reads as a nested execution trace with per-step durations — flow status and bottleneck localization in one mechanism.

**`Log.timer()` — phase timing (in scistacklog).** Extracts the hand-rolled `timings[...] = perf_counter()-t` pattern from `database.py` into a shared helper:
```python
with Log.timer("save_batch(PSD)", layer="scidb") as t:
    with t.phase("4a_canonical_hash"): ...
    with t.phase("6e_commit"): ...
# emits INFO  "[timing] save_batch(PSD): TOTAL=7.752s (4a_canonical_hash=7.058s, 6e_commit=0.029s, ...)"
# and DEBUG per-phase table lines (same format as today — timing-logs grep workflow unchanged)
```
`database.py` save_batch/load/find_record/load_all_as_df migrate to it (mechanical — same output, `[timing]` prefix preserved); it then becomes trivially adoptable in other hot paths (batched provenance helpers, sciduckdb query paths, scifor combo loop) whenever a bottleneck is suspected, instead of re-implementing timing dicts each time.

**Error legibility.** Any exception crossing a user-facing API boundary (for_each, save, load, CLI dispatch) is logged `ERROR` with full traceback to the file before propagating — a failed run's log always ends with the error and its context, not just a Python traceback lost in the terminal. (`Log.step` provides this for free wherever it wraps.)

**Run-context header.** `configure_database` logs one INFO line with scistack package versions, DB path, Python/MATLAB host, and PID — so every log file is self-describing when debugging a report from a colleague or an old archived run.

**Tests:** scistacklog tests for `step`/`timer` output format and exception path; scidb regression test that the migrated `[timing]` summary is byte-compatible with the old grep targets.

## 5. MATLAB parity — `scimatlab/src/scimatlab/matlab/+scidb/Log.m`

- `debug/info/warn/err` pass `pyargs('layer', 'matlab')`; appdata level-gating cache untouched (suppressed lines never cross the bridge).
- `set_level(level[, sink])` — optional second arg forwarded via pyargs; cache stores `get_level()` (min of sinks).
- Timing tests (`TestSaveTimingInstrumentation.m`, `TestForEachTimingInstrumentation.m`): add `scidb.Log.set_level('DEBUG', 'file')` in setup so archived logs keep the DEBUG phase tables; archive-by-path mechanism unaffected.

## 6. Config surface (minimal)

1. New: `SCIDB_LOG_LEVEL` env var, read once in `attach()`, applies to both sinks.
2. CLI `-v` → `Log.set_level("DEBUG")` (existing code, unchanged); CLI additionally sets console sink to WARN when not verbose (stderr stays quiet; file keeps the INFO narrative).
3. Programmatic: `Log.set_level(level, sink=...)`. No new `configure_database` kwarg.
4. GUI `scistack-gui/scistack_gui/server.py:26-30`: **remove `logging.basicConfig`** (root handler → would double-print everything with propagate=True); replace with `Log.set_level("DEBUG", sink="console")` (console is stderr, satisfying JSON-RPC-clean-stdout).

## 7. Tests

**Update:**
- `scidb/tests/test_batch_save_regression.py` — only the `"result row(s)"` assertions (message moves to logger `"scifor"` at DEBUG); `[batch_save]` assertions unchanged.
- `scihist/tests/test_pipeline_visibility.py`, `test_skip_computed.py`, `test_foreach.py`, `test_merge.py` — capsys `[skip]`/`[run]` counting → `caplog.at_level(logging.DEBUG)` record filtering.
- `scifor/tests/test_foreach_standalone.py` — `[skip]` (981) and stderr-label (787) → caplog; `[dry-run]` (282) unchanged.
- scidb inspect CLI tests untouched (stdout rendering isn't logging).

**New (regression, per CLAUDE.md NOTE 2):**
- `scistacklog/tests/test_log.py`: file-line regex `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} (DEBUG|INFO |WARN |ERROR) \[[a-z_]+\] ` (one record per line); dual-sink level independence; `set_path` re-pointing mid-run; `attach()` idempotence (no duplicates); caplog sees DEBUG when both sinks are ERROR; `layer=` routing; positional `Log.info("x")`; console format is time-only.
- `scidb/tests/test_log_shim.py`: `from scidb.log import Log` still resolves to the scistacklog facade (MATLAB delegation path contract).
- `scifor/tests/test_logging.py`: summary aggregates reasons with counts/combos; progress fires on outermost-key transitions (and respects the min-interval guard) under monkeypatched thresholds; `[run]`/`[skip]` absent at INFO / present at DEBUG; `"scidb" not in sys.modules` after scifor run; `/tmp/scihist_diag.log` never created.
- scidb: `[timing]` summary present in file at default level.

## 8. Docs

- `README.md:235` — replace the "future logging" promise with actual behavior.
- New `scistacklog/README.md`; `scidb/README.md`, `scifor/README.md` — logging sections (format, sinks, levels, `SCIDB_LOG_LEVEL`).
- New `docs/claude/logging-architecture.md` — layer/logger map, handler topology, sink defaults, triage rule, caplog contract, GUI/MATLAB constraints.

## 9. Stages (each lands + tests green alone; user runs all tests — no Python in assistant env)

| Stage | Content | User verification |
|---|---|---|
| 1 | Create `scistacklog` package (facade on stdlib) + `test_log.py`; `scidb/log.py` → re-export shim + `test_log_shim.py`; scidb/scifor pyproject deps; MATLAB timing tests get `set_level('DEBUG','file')`. No call-site changes. | Confirm final package name against PyPI; `bash run_tests.sh`; MATLAB `runtests` on `scimatlab/tests/matlab/scidb` (confirm timing-logs archive has new format, `[timing]` greps work, `py.scidb.log.Log` still resolves) |
| 2 | scifor print→Log facade, schema-key progress + summary, diag-file removal, drop `_log_fn=Log.info` (scidb/foreach.py:541), scidb consumes `summary` event; update scifor/scihist tests; `test_logging.py`. | `bash run_tests.sh` (scifor, scihist, scidb, scistack-gui); MATLAB `TestForEachTimingInstrumentation` |
| 3 | Level triage + instrumentation: `Log.step`/`Log.timer` added to scistacklog; scidb Step-N narrative → `Log.step` wrappers (delete paired prints), per-combo lines → DEBUG, sciduck locks → DEBUG, per-column dumps → DEBUG; `database.py` timing dicts → `Log.timer`; run-context header in `configure_database`; API-boundary ERROR logging; update `test_batch_save_regression.py` + remaining capsys tests; new step/timer tests. | `bash run_tests.sh`; manual: run an example pipeline, eyeball console vs `scidb.log` at default and `SCIDB_LOG_LEVEL=DEBUG` (file should read as a nested execution trace with durations); grep `[timing]` on the archived log still works |
| 4 | Hosts: GUI basicConfig removal + console-DEBUG; CLI console-WARN default; `SCIDB_LOG_LEVEL`; `+scidb/Log.m` layer tag + sink arg. | scistack-gui tests + JSON-RPC smoke (stdout must stay pure JSON); scidb CLI tests; full MATLAB pass |
| 5 | Docs (READMEs + `docs/claude/logging-architecture.md`). | Review |

## Failure modes checked

- **caplog**: handler-level filtering only; loggers pinned DEBUG, propagate=True.
- **Double emission**: sole root `basicConfig` (GUI) removed in Stage 4; `attach()` idempotent.
- **MATLAB overhead**: client-side gating cache unchanged.
- **File contention**: one FileHandler with stdlib lock replaces open/append/close race window.
- **timing-logs archives**: `[timing]` INFO summaries survive defaults; timing tests raise file sink to DEBUG for phase tables.

## Critical files

`scistacklog/src/scistacklog/__init__.py` (new) · `scidb/src/scidb/log.py` (→ shim) · `scifor/src/scifor/foreach.py` · `scidb/src/scidb/foreach.py` · `scidb/src/scidb/database.py` · `scimatlab/src/scimatlab/matlab/+scidb/Log.m` · `scistack-gui/scistack_gui/server.py` · `scidb/src/scidb/inspect/cli.py`

*(On approval, per CLAUDE.md convention, this plan will also be copied to `.claude/logging-redesign-plan.md` in the workspace.)*
