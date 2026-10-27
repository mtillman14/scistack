# Error surfacing: failure records, on_error, and where failures show up

*Written 2026-10-26. Plan: `.claude/plan-error-surfacing-26-10-26.md`.*

`for_each` catches every per-combo exception (skip-and-continue): one bad
subject must not cost the other 32 their run. Before this work the only trace
of *why* a combo failed was a WARN + traceback in `scistack.log`; the GUI got
`str(e)` and printed `⚠ N combo(s) failed — see log above`. Now every failure
is a **record** that carries the line that raised, and that record is shown
where a VS Code user looks for errors. Skip-and-continue is still the default.
`on_error="stop"` behaves like vanilla Python: the run stops and the original
exception is raised.

## 1. The record: `scifor/failures.py` (one owner)

| Name | What |
|---|---|
| `ComboFailure` | one failed combo: `fn_name`, `metadata`, `stage` (`prepare_inputs` / `call` / `run`), `exc_type`, `message` (capped 2000 chars), `frames` (trimmed, ≤50, outermost first), `user_frame`, `no_data` |
| `FailureGroup` | every combo with one `reason` (`"{exc_type}: {message}"`, the same grouping key the log always used); `example` is the first occurrence |
| `frame_origin(file)` | **the only frame classifier**: `framework` (a SciStack package dir, found with `importlib.util.find_spec`, never imported), `library` (stdlib / site-packages), else `user`. An AST test bans `traceback.extract_tb` anywhere else |
| `failure_from_exception` | builds a record; walks `__cause__`/`__context__` to the first exception that reaches user code (for_columns' `ColumnFunctionError` is raised `from` the first per-column error for exactly this) |
| `groups_from_wire` | reads groups back from JSON (MATLAB's or Python's), undoing `jsonencode` quirks: a 1-element array written as an object, `[]` for none, a 1-element cellstr as a bare string |
| `validate_on_error`, `StoppedRun`, `stopped_run_of` | the `on_error` option (§3) |

**Trimming:** the framework frames that lead *into* the user's function
(scifor's loop, scidb's wrapper) are dropped. Everything from the first
non-framework frame inward is kept, library frames included (they are shown
collapsed). A traceback with no user frame (a failure in scifor's own input
preparation) is kept whole, because there the framework frames are the story.

**Cost:** only the *first* combo of each reason extracts a traceback. Later
combos only add their combo string to the group. `combo_skip` progress events
carry the small form `{reason, no_data, user_frame}`. Tracebacks are sent once,
in the summary.

### Wire shape (`FailureGroup.to_dict`)

```
{reason, fn_name, exc_type, message, no_data, count, combos[:20], user_frame, example: ComboFailure.to_dict()}
```

The scifor `summary` progress event carries `failures: [group...]` and
`stopped: bool`. `failure_reasons` was removed (beta: no shim).

## 2. Who reads it

```
scifor summary event ─┬─ scidb pipeline step report: entry.failures / entry.stopped / entry.tag
                      └─ scistack_gui.services.run_failures.RunFailures  (GUI owner)
                              │  drops no-data groups (HERE only), tags node_id, console text
                              ▼
                      run_done.failures = {by_node: {node_id: [group]}, other: [group]}
                              │  by_node lists EVERY covered node; [] = ran clean
              ┌───────────────┴──────────────────┐
  frontend NodeFailuresContext            extension FailureReporter
  (rules: DAG/runFailures.ts)             (rules: runFailures.ts)
  - red "✗ N combos failed" badge         - Problems panel: one Error per reason
  - FailuresPanel (traceback,               at user_frame; disposed with the session
    click → reveal_in_editor)             - error notification: Show failures / Open log
```

Because `by_node` names every node the run covered, **neither consumer decides
which entries a rerun replaces**. Each one assigns `state[node] = by_node[node]`,
so the two cannot diverge. (They have no shared-code path: the extension and
the frontend compile separately.)

**Node attribution:**
- Single-node Python run: the `node_id` the click sent.
- Pipeline: `StepSpec.tag`, an opaque label that is never replayed and never
  identity. `build_backend_pipeline` sets it to the node id, and the run report
  echoes it as `entry.tag`. Two nodes may share a function and a step name, so
  neither of those can be the key.
- Single-node MATLAB run: the extension passes `node_id` to
  `watch_matlab_terminal_run`.
- MATLAB pipeline run: not attributed yet; its groups go to `other`, so they
  appear in Problems and the notification but get no badge.

**Report survives a stop:** `Pipeline._run` publishes `last_run_report` *before*
the step loop, and `run_pipeline(report_sink=...)` fills the caller's list in a
`finally`.

## 3. `on_error="continue" | "stop"`

- **Owner:** scifor (`validate_on_error`). scidb and scihist forward it, and it
  is in the signature-parity guards (both languages).
- **Not identity:** it is NOT in `scidb.RunOptions`, which is identity-bearing.
  Stopping early never changes what a run computes.
- **NoDataError never stops a run.**
- **Python scifor:** on the first genuine failure the loop breaks. The summary
  (with `stopped: True`) and the output table are built, then the **original**
  exception is re-raised with `scifor_stopped = StoppedRun(failure,
  partial_result, completed)` attached and an `add_note` line. The caller's
  traceback therefore ends in the user's code, as in vanilla Python. EachOf
  merges earlier alternatives' rows into the partial result.
- **scidb:** catches it, saves `stopped.partial_result` (the completed combos;
  data is never dropped), then re-raises. The run-summary log line moved into
  a `finally`.
- **GUI:** the node's `runOptions.on_error`, read ONLY through
  `run_failures.on_error_from_config` (single-node run, compiled pipeline,
  Python and MATLAB export, MATLAB command generation). The sidebar shows it as
  Run Options → **Stop at first error**.
- **MATLAB:** `+scifor/for_each.m` accepts `on_error` (validated by Python) and
  rethrows the original after the summary. `+scidb/for_each.m` passes the
  private `_defer_stop`: scifor returns the completed combos and hands the
  error to `scidb.internal.iteration_tally`, and scidb saves, then
  `take_stop` + `rethrow`. MATLAB **standalone** scifor cannot attach the
  partial table to an MException, so its results are not returned on a stop.

## 4. MATLAB parity

- `+scifor/failure_from_mexception.m`: same fields as `ComboFailure.to_dict`,
  plus `combo` (the metadata string). `err.stack` is reversed to outermost-first.
- `+scifor/frame_origin.m`: the one MATLAB classifier (scimatlab's MATLAB dir →
  framework, `matlabroot` → library).
- `record_iteration_failure` keeps the first record per reason in
  `failure_examples`. `failure_groups` builds the wire groups; the summary
  struct gains `failures`, `stopped`, `stop_error`.
- `iteration_tally` accumulates the groups (capped at 200). The generated
  script's `catch` passes `iteration_tally('failures')` to `scidb.run_marker`,
  which splices a `"failures"` array into the `.done` JSON.
  `run_markers.read_done` → `DoneMarker.failures` (via `groups_from_wire`) →
  `matlab_run_watch._finish` → the same `RunFailures` → `run_done.failures`.
- Guard without MATLAB: `scimatlab/tests/test_matlab_failure_record_shape.py`
  (field names parsed from the .m source vs the Python dicts).
- **Not covered:** the sidecar tier (`matlab -batch`) does not read the
  marker's failures.

## 5. Diagnostics to grep

| Where | Line |
|---|---|
| scifor WARN (first per reason) | `iteration failed: <combo> — <fn> raised: <msg> [at <file>:<line> in <fn>]` |
| scifor INFO summary | `failed: N × "<reason>" — <combos> [at <file>:<line> in <fn>]` |
| scifor stop | `for_each(<fn>): on_error=stop — stopped at <Type> at …; K combo(s) completed before it, re-raising the original exception` |
| scifor DEBUG | `failure_from_exception: no user frame in <Type> traceback` (a classifier miss) |
| scidb | `for_each(<fn>): on_error=stop — saving K completed combo(s) before re-raising <Type>`, and `run summary: … stopped (on_error=stop)` |
| GUI | `[run_failures] <target>: N failure group(s) … on node <id>`; `[run_thread] Run options: … on_error=…` |
| extension output | `[failures] run_done <id>: N node(s) covered, M Problems entries now shown` |
| frontend console.debug | `[NodeFailures] run_done <id>: covered N node(s), M group(s)` |

## 6. Tests

`scifor/tests/test_failures.py`, `scifor/tests/test_blame.py`, `scidb/tests/test_on_error_stop.py`,
`scistack-gui/tests/test_run_failures.py`,
`scimatlab/tests/test_matlab_failure_record_shape.py`,
`scistack-gui/extension/src/runFailures.test.ts`,
`scistack-gui/frontend/src/components/DAG/runFailures.test.ts`.
GUI manual checks: `docs/gui-manual-testing-todo.md` §0zzzzzzl.

## 7. Who can fix it: `blame` (added 2026-10-26)

Plan: `.claude/scientist-facing-log-plan.md`. Every `ComboFailure` (and
`FailureGroup.to_dict`) carries `blame`, `probable` and, on groups,
`blame_label`, the display text worded by `scistacklog.journal.blame_headline`.
The journal (`scistack.log`, see logging-architecture.md), the FailuresPanel
label, the Problems entry and the error notification all show that label.
None of them keeps its own copy of the wording.

**One classifier: `scifor.failures.classify_blame(exc, frames, no_data=)`**,
rules in order:

| blame | rule | headline |
|---|---|---|
| `no_data` | NoDataError | No data |
| marker's own (`setup`, or `user_value` for `UserValueError`) | the exception itself (not its `__context__`) is an `ActionableError` | Problem with your setup or data / Fixable in your function F |
| `user_code` | innermost non-library frame is the user's, **or** is `call_user_function` (a library function wired up as the function, e.g. `pandas.read_csv`) | Error in your function F |
| `internal` | otherwise | SciStack internal error, not caused by your code |

- **`ActionableError`** (`scifor.failures`): a deliberate, actionable refusal. `scidb.SciStackError`, scifor's `SchemaKeyTypeError`, and the GUI's `layout_service.EdgeCycleError` subclass it. `scidb.OutputSaveError` computes `blame`/`probable` from its `causes`.
- **`call_user_function(fn, ...)`**: every for_each call of the user's function goes through it (`_call_fn`, `_run_column_iteration`); its frame is how the classifier sees where the user's function starts.
- **User-value seam**: SciStack steps that handle the user's values run inside `user_value_seam(stage, fn_name=, fn=, output=, diagnose=)`, or pass their exception through `as_user_value_error`. Today that covers scifor's combine step (`_results_to_output_dataframe`, AST-guarded in `test_blame.py`), scidb's per-output save (`_save_seam_error`), and MATLAB's flatten-mode `vertcat` (`+scifor/for_each.m` `user_value_error` / `describe_table_kinds`). An unmarked exception becomes a `UserValueError` with the original as `__cause__`:
  - **precise** when `diagnose` names the cause (`describe_value_kinds`: the output's values differ in kind across combos, naming the combos on each side). The diagnosis runs only after the step has failed, so a working run is never refused.
  - **probable** otherwise: hedged wording ("Probably fixable…", "may be a SciStack bug: report run=…") and a `[seam_fallback] stage=… fn=…` WARN. **Each `[seam_fallback]` in a real log is a missing check: add it to the diagnosis, plus a regression test.**
- `ForColumnsError` is a `UserValueError` (still a `ValueError`).
- MATLAB (`+scifor/failure_from_mexception.m` `classify_blame`): `scifor:NoData` → no_data; `scistack:userValue[:probable]` → user_value; innermost non-library frame user → user_code; framework frame + `scifor:`/`scidb:`/`scihist:`/`scistack:` identifier → setup; else internal. `failure_groups` asks Python for `blame_label`.
- `groups_from_wire`: a group without `blame` gets the frame rule's answer (`user_frame` → user_code, else internal), logged at DEBUG.

**Limit:** blame says where an error was *raised*, not its root cause. That is why
`user_code` reads "Error in your function F" and always shows the line.

Diagnostics: `failure_from_exception: <Type> in <fn> -> blame=<b> via <rule>`
(scifor DEBUG, MATLAB `failure_from_mexception: … via …`); `[seam_fallback]` (WARN).
