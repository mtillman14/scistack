# scistacklog

The shared logging facade for every scistack layer. This is the lowest-level
package in the stack — it has no dependencies, and every other layer (scifor,
scidb, sciduckdb, …) emits through it or through a plain
`logging.getLogger("<layer>")` child logger that it covers.

## Design

Two audiences, so two files, fed by one emit stream:

- **journal** (`scistack.log` beside the database) — for the scientist. One
  block per run: what ran, the counts, how long, and each failure labelled
  with who can fix it. Nothing about the framework's internals.

  ```
  ── 2026-10-22 17:55:18  clean_asia  (run=a3f2c1d9) ─────────────────────
     finished with 34 failures: ✓ 0 completed   ✗ 34 failed   – 0 no data   2m 14s

     ✗ Error in your function clean_asia — 34 combos
       AttributeError: 'StringArray' object has no attribute 'split'
       at C:\lab\clean_asia.py:42 in parse_level
           levels = col.split(",")
       combos: subject=2204674; subject=2204675; subject=2204676; subject=2204677; subject=2204678 (+29 more)

     Failed combos saved nothing; completed combos were kept.
  ```

- **debug file** (`scistack-debug.log` beside it) — for whoever debugs
  SciStack. Everything, at DEBUG by default, capped at 10 MB × 5. Every line
  written during a run carries the `run=<id>` the journal block names.

  ```
  2026-10-22 17:55:18.001 INFO  [scifor] run=a3f2c1d9 for_each(clean_asia) — 34 iterations: subject=…
  2026-10-22 17:55:18.120 DEBUG [scifor] run=a3f2c1d9 failure_from_exception: AttributeError in clean_asia -> blame=user_code via …
  ```

- **console** (stderr) — the narrative for the person watching, at INFO.
  Time-only timestamps; level prefix only at WARN and above.

The debug file is a superset of the journal: each block is also logged there
(as one `[journal]` record). `scidb.configure_database()` points both files
beside the database (`scidb.log.attach_log_file`); directly:

```python
from scistacklog import Log

Log.set_path("proj/scistack-debug.log", journal_path="proj/scistack.log")
Log.set_level("INFO", sink="file")   # a quieter debug file
Log.set_level("DEBUG")               # both sinks
```

or set the `SCIDB_LOG_LEVEL` environment variable (applies to both sinks),
or pass `-v` to the `scidb` CLI.

### Writing to the journal

```python
with Log.run("compute_psd") as run:      # one block per outermost run
    run.add_summary(summary_event)       # scifor's end-of-run summary
    run.count("saved", 114)
Log.journal_note("PSD: 3 columns hold text", key="psd-text")   # once per key
```

Who-can-fix-it (`blame`) is decided by `scifor.failures.classify_blame` and
arrives in the event; `scistacklog.journal.blame_headline` is the one wording.
See `docs/claude/logging-architecture.md`.

## Usage

```python
from scistacklog import Log

Log.info("for_each(compute_psd) — 120 iterations", layer="scifor")
Log.debug("combo detail: %s", combo, layer="scifor")
Log.error("save failed", layer="scidb", exc_info=True)
```

`layer` must be one of `scistacklog.LAYERS`
(`scidb`, `scifor`, `sciduck`, `scihist`, `scilineage`, `scistack`,
`scistack_gui`, `matlab`).

## Timing a hot path

```python
with Log.timer("save_batch(PSD)", extra="114 items") as t:
    with t.phase("canonical_hash"): ...
    with t.phase("commit"): ...
```

One INFO summary on exit — `[timing] save_batch(PSD): 114 items, TOTAL=0.412s
(canonical_hash=0.310s, commit=0.102s)` — plus a per-phase table at DEBUG.
`TOTAL=` appears on the summary line and nowhere else; MATLAB's timing archives
grep for it.

**`live=True` for anything that can run for minutes.** A summary is a
post-mortem, and an operation that has not finished yet has no post-mortem: a
25-minute figure save (scistack.log 2026-09-11) logged nothing at all, and a save
that timed out logged nothing ever. A live timer announces each phase as it
starts and as it ends, and enables `t.note(...)` for progress inside a long
phase:

```python
with Log.timer("save_figure", layer="scistack_gui", live=True) as t:
    with t.phase("resolve", extra="2 figure(s)"):
        t.note("figure %d/%d", 1, 2)
```

```
[timing] save_figure: resolve started — 2 figure(s)
[timing] save_figure: figure 1/2
[timing] save_figure: resolve done in 771.402s
[timing] save_figure: TOTAL=773.918s (resolve=771.402s, render_and_write=2.511s)
```

`t.note` is silent on a quiet timer, so the same instrumented code serves a
fast path without flooding it.

## Contracts

- **caplog**: level filtering happens on the two handlers, never on the
  loggers — every record propagates to the root logger, so pytest's `caplog`
  captures everything regardless of sink levels. `Log.*(...)` with the
  default layer emits on logger `"scidb"`, unchanged from the historical
  implementation.
- **MATLAB**: `scimatlab`'s `+scidb/Log.m` delegates to this class via
  `py.scidb.log.Log.*` (a re-export shim in scidb). The numeric level scale
  (`DEBUG=0 … ERROR=3`) and positional call shapes are frozen for that
  bridge.
