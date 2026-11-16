# Run report

*Written 2026-11-15. Plan: `.claude/plan-run-report-popup.md` (GitHub issue #17).
Owner: `scidb/provenance/run_report.py`. Tests: `scidb/tests/test_run_report.py`,
`scidb/tests/test_run_summary.py`.*

## The question

After a function ran, what did the run do? Each input and output at every
schema-key combination (size, shape, value, status), the settings used, the
exclusions in force, NaNs and other missing data. The point is verifying a run
that "succeeded", so the report is read **from the database afterwards** (user
decision): it describes what the run left behind.

Failures keep their own surfaces (node badge, FailuresPanel, Problems,
RemedyDialog; `error-surfacing.md`). The report lists failed and no-data
locations as rows and links to them. There is no second failure popup.

## Derived vs stored

| What | Where it comes from |
|---|---|
| The run's calls, their input and output records, identity-bearing options (`distribute`, `as_table`, `for_columns`, `across_variants`, level), function hash and stored source, constants, PathInput specs, selectors | the graph: `_run` → `_run_invocation` → `_invocation` / `_invocation_input` / `_invocation_output` |
| Counts, timing, cancelled / stopped, `locations=`, `on_error`, `skip_computed`, `save`, failure reasons, output skips | **`_run_summary`** (new), written by the run |
| Failed and no-data locations of this run | `_run_outcome.run_id` (new column) |
| Exclusions in force | the append-only override history, as of the run's start |
| Marks, locations still missing | `scidb.provenance.expectation` |
| Shapes, sizes, NaN / blank / empty / constant cells | read from the run's records when asked: `scidb.inspect.cells.record_cells` + `describe_value` (cell kinds are `scicanonicalhash._cells`') |

No shape or size is stored.

## `_run_summary` (owner `scidb/provenance/run_summary.py`)

One row per `for_each` execution, **including a run that saved nothing** (which
writes no `_run` row). `run_id` is the `_run` row's when there is one, so the
two join. `group_id` is the journal run (`run=<id>` in the debug log; EachOf
alternatives share it). History bundle; emptied by Clear data.

`RunRecorder`:

* **Python:** `scidb.run.foreach._run_concrete` creates it before prepare,
  chains `recorder.wrap(...)` into the caller's `progress_fn` (it reads
  scifor's `combo_skip` and `summary` events), the save calls
  `recorder.saved(run_id, n)` (`foreach_save._save_results`, via
  `state.run_recorder`), and a `finally` after the save calls `finish`, so a
  stopped, cancelled or failed run is recorded too. A dry run records nothing.
  It replaced the `OutcomeCollector` that `_for_each_execute` used to hold.
* **MATLAB:** `scimatlab.bridge.for_each_prepare` creates it and notes
  `scifor.matlab_loop.last_token()`. `matlab_loop.assemble` keeps the finished
  loop's events in the Python loop's shapes; `for_each_save` takes the events
  of the loop planned after that mark (`take_run_events_after`), feeds the
  recorder and finishes it. No `.m` file changed. A cleanup call with no
  finished loop records nothing. `on_error` is not known on this path.
* `finish` never raises (a WARN), and then sends one **`run_recorded`**
  progress event with the `run_id`. That is how a caller learns which report
  to open: the GUI run thread (kept off `run_progress`) and a pipeline's step
  report (`entry["run_ids"]`).

`RunSummary.status_line()` and `.verdict` are THE wording of the counts.

## The report (two calls, by cost)

| `Inspector` | CLI | Cost |
|---|---|---|
| `run_reports(fn, outputs=, limit=)` | `scidb run-report FN --list` | one query |
| `run_report(run_id)` | `scidb run-report FN [--run ID]` | queries only |
| `run_report_data(run_id, status=, limit=)` | `... --locations [--status S] [--csv PATH]` | loads the run's records, one load per variable |

`run_report`: header, settings, input and output cards (record counts, input
variants, new vs saved again unchanged), exclusions, marks, attention items
that need no data. `run_report_data`: per variable its columns (dtypes,
shapes, cell counts); per combination each input's and output's shape, size,
inline scalar value and findings; attention items only the data can raise.

**Attention** (`Attention.text` is the wording; `problem` or `note`):
cancelled, stopped, failed reasons, no data, outputs skipped, draft, computed
but nothing saved, more than one function version, locations still missing
(`expectation()`), suspect cells and differing shapes (a problem on an output,
a note on an input), NaN the function introduced vs carried from an input.

**"What this run wrote":** an invocation's output slots append across runs, so
per (invocation, type, location) the report takes the record with the newest
`_record_save` at or before the run's `_run.timestamp`. An older run's report
therefore keeps describing its own records. Both timestamps are local-clock
ISO strings from the same process.

## Limits

* Already-up-to-date combinations are counted, not listed: the run did not
  touch them. The report says so in `notes`.
* Excluded and not-selected combinations are not rows; exclusions are listed
  with reasons, without a per-exclusion count.
* A run that saved nothing has no inputs or outputs to describe.
* A combination whose inputs have no record is left out before the loop
  (prepare), so it is not in `total` and is not a "no data" row. "No data"
  counts only combinations the loop reached and found nothing for.
* Exclusion times are stored as local wall time (an aware datetime bound to a
  TIMESTAMP column); `_override_time_utc` converts them to compare with the
  run's UTC start.
* Input variants are not resolved above 20,000 input records.
* A pooled input with several current variants is not flagged.

## GUI

* `services/run_report_service.py`: calls `Inspector` only. One handler,
  `run_report`, with `part` = `list` / `summary` / `data` (`api/run.py`).
* `run_done.reports = [{node_id, function_name, run_ids}]` in run order, only
  entries with a stored run (`reports_wire`). Python single-node run: from
  `run_recorded`. Pipeline run: the step report's `tag` + `run_ids`. MATLAB
  (terminal watcher and sidecar): `matlab_reports_since(dispatched_at,
  node_id)` reads the MATLAB rows stored since dispatch, because the bridge
  runs in MATLAB's process and nothing tells the GUI their ids.
* `context/RunReportContext.tsx` decides when the window opens;
  `components/RunReport/` draws it. Rules in `runReportTypes.ts` (unit-tested):
  `shouldAutoOpen` (something computed or already up to date, and the switch
  on), `tabsOf` (one tab per node, on its last stored run), `tabTone`,
  `shouldAutoRead` (records read without asking up to 500 computed
  combinations).
* Right-click a function node → **📋 Run report…**. The header tick **Open
  after every run** is kept in the browser's localStorage (per VS Code
  install, not per project).
* The window has no backdrop, so the canvas stays usable.

## Diagnostics to grep

| Where | Line |
|---|---|
| scidb | `[run_summary] run=<id> group=<id> fn=… total=… computed=… wrote in N ms` |
| scidb | `[run_report] run=<id> fn=…: N invocation(s) … graph N ms, cards N ms` and `[run_report] data run=<id> … load N ms` |
| scidb | `[cells] record_cells(X): N of M record(s) from K loaded row(s)` |
| scifor (MATLAB) | `[matlab-events] token N: kept K skip event(s) + summary` / `events taken (mark M)` |
| bridge | `[bridge] for_each_save: handle=N: no finished loop, run not recorded` |
| GUI | `[run_thread] run recorded as <id>`; `[run_report] part=… in N ms`; `[run_report] MATLAB run dispatched … stored N run(s)` |
| browser console | `[RunReport] run_done <id>: N tab(s), M summary(ies) read, auto-open=…` |
