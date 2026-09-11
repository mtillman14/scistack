# scistacklog

The shared logging facade for every scistack layer. This is the lowest-level
package in the stack — it has no dependencies, and every other layer (scifor,
scidb, sciduckdb, …) emits through it or through a plain
`logging.getLogger("<layer>")` child logger that it covers.

## Design

One `Log` call, two destinations with independent levels:

- **console** (stderr) — a concise pipeline narrative for the person running
  the pipeline. Time-only timestamps; level prefix only at WARN and above.

  ```
  14:32:06 [scifor] for_each(compute_psd) — 120 iterations: subject=12 values [s01,…,s12]
  14:33:15 [scifor] WARN: iteration failed: subject=s03, session=2 — ValueError: bad channel count
  ```

- **file** (`scidb.log` next to the database, pointed via `Log.set_path`,
  done automatically by `scidb.configure_database()`) — the run document.
  One record per line: date + millisecond timestamp, level, originating layer.

  ```
  2026-07-07 14:32:06.001 INFO  [scifor] for_each(compute_psd) — 120 iterations: subject=12 values [s01,…,s12]
  2026-07-07 14:33:20.100 INFO  [scidb] [timing] save_batch(PSD): 114 items, 12 schemas, 0.412s
  ```

Both sinks default to INFO. DEBUG detail (per-iteration lines, timing phase
tables, framework internals) is opt-in:

```python
from scistacklog import Log

Log.set_level("DEBUG", sink="file")   # full detail in the file only
Log.set_level("DEBUG")                # both sinks
```

or set the `SCIDB_LOG_LEVEL` environment variable (applies to both sinks),
or pass `-v` to the `scidb` CLI.

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
25-minute figure save (scidb.log 2026-09-11) logged nothing at all, and a save
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
