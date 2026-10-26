# Plan: make run errors obvious (structured failures, GUI surfacing, on_error)

Date: 2026-10-26. Scope: options 1, 2, 3 from the error-handling discussion.
Out of scope: debugger break-on-user-exception (option 4), persisting failures
to the DB (option 5).

## Problem

`scifor.for_each` catches every per-combo exception (skip-and-continue). The
only trace of *why* a combo failed is a WARN + traceback in `scidb.log` (first
occurrence per reason). The GUI receives `run_progress.error = str(e)` (no
type, no traceback, no file:line) and prints `⚠ N combo(s) failed — see log
above`. The verdict is honest (failed), but the cause is hidden.

## Current owners (verified)

- `scifor/src/scifor/foreach.py:63` `_record_iteration_failure` — builds the
  reason string `"{Type}: {msg}"`, groups combos by reason, WARN first occurrence.
- `scifor/src/scifor/foreach.py:~1150` — `summary` progress event carries
  `failure_reasons: dict[str, list[str]]`.
- `scifor/src/scifor/foreach.py:~1045` — `combo_skip` progress event carries `error: str(e)`.
- `scidb/src/scidb/foreach.py:1024` calls scifor; `_save_results` (line 3738)
  runs AFTER the loop returns, so completed results exist only in memory until then.
- `scidb/src/scidb/pipeline.py:1318` — pipeline step report tallies the summary.
- `scistack-gui/scistack_gui/api/run.py:611` — relays progress, tallies the verdict.
- `scistack-gui/frontend/src/components/DAG/FunctionNode.tsx:178` — consumes `run_progress`.
- MATLAB: `scimatlab/.../+scifor/for_each.m:769` `failure_reasons` map + `summary_fn`.

## Stage 1 — scifor owns the failure record (NEW: `scifor/src/scifor/failures.py`)

One owner for "what failed, where". Everything else is a consumer.

```python
@dataclass(frozen=True)
class FailureFrame:
    file: str; line: int; function: str; code: str
    origin: Literal["user", "framework", "library"]   # library = stdlib/site-packages

@dataclass(frozen=True)
class ComboFailure:
    fn_name: str
    metadata: dict[str, str]
    stage: Literal["prepare_inputs", "call", "outputs"]
    exc_type: str            # qualified, e.g. "KeyError"
    message: str             # capped (2000 chars)
    frames: tuple[FailureFrame, ...]   # trimmed: from the user fn's entry frame inward, capped at 50
    user_frame: FailureFrame | None    # innermost origin=="user" frame — the line to jump to
    no_data: bool
    @property
    def reason(self) -> str  # "{exc_type}: {message}" — SAME grouping key as today
    def to_dict(self) -> dict  # JSON-safe; the wire format for every consumer

@dataclass
class FailureGroup:
    reason: str; no_data: bool; count: int
    combos: list[dict[str, str]]       # capped at _SUMMARY_COMBOS_MAX
    example: ComboFailure              # first occurrence, with traceback

def failure_from_exception(exc, *, fn_name, metadata, stage) -> ComboFailure
def frame_origin(filename) -> origin   # SINGLE owner of "is this frame ours?"
```

- `frame_origin` classifies by package directory (`scifor`, `scidb`, `scihist`,
  `scilineage`, `sciduckdb`, `scistacklog`, `scistack_gui` `__file__` dirs →
  framework; `sysconfig` stdlib/purelib/platlib → library; everything else →
  user). It uses path prefixes, not substring matching (the workspace is case-insensitive:
  normalise with `os.path.normcase`). AST/unit test: no other module classifies frames.
- `_record_iteration_failure` builds a `ComboFailure` and accumulates `FailureGroup`s.
  `failure_reasons: dict[str, list[str]]` is REPLACED (beta: no deprecation shim).
- WARN line on first occurrence gains `at {user_frame.file}:{line} in {function}`
  and keeps the full `exc_info` traceback (the log stays complete).
- Progress events:
  - `combo_skip`: `failure: {reason, no_data, user_frame}` (small; no traceback per combo).
  - `summary`: `failures: [FailureGroup.to_dict(), ...]` (example carries frames).
- `scidb/pipeline.py:1318` copies `failures` into the step report entry.

Logging: DEBUG line when `frame_origin` finds no user frame (tells us the
classifier missed, e.g. a user module living inside a framework dir).

Tests (`scifor/tests/test_failures.py`):
- user fn raising → `user_frame` is the raising line in the test file; framework frames trimmed.
- user fn calling into pandas which raises → `user_frame` is the user call site, pandas frames origin=library.
- failure in input prep (malformed spec) → `stage="prepare_inputs"`, `user_frame` None, no crash.
- NoDataError → `no_data=True`, never in WARN.
- grouping: 10 combos same reason → 1 group, count 10, combos capped, one example.
- `to_dict` round-trips through `json.dumps`; message/frame caps hold.
- summary event shape test (consumers depend on it).

## Stage 2 — `on_error: "continue" | "stop"` (scifor owns, scidb/scihist forward)

- `scifor.for_each(..., on_error="continue")`. Validated at entry (bad value → ValueError).
- `"stop"`: on the first failure with `no_data=False`, stop the loop, emit the
  `summary` event (so the GUI tallies stay correct, with `stopped=True`), then
  raise `IterationStopped(failure, partial_result)` `from` the original exception.
  `NoDataError` never stops a run.
- scidb catches `IterationStopped`, **saves the completed combos' results**
  (never drop data, see memory "Never drop data silently"), then re-raises the
  ORIGINAL exception with `exc.scifor_failure = failure` attached. The caller's
  traceback ends in the user's own code, which matches vanilla Python.
  Standalone scifor (no DB) re-raises the original the same way.
- Kwarg parity: add to scidb.for_each, scihist.for_each, pipeline step options,
  and the plain-Python exporter (memory "Export matches preview"), following the
  owners in `docs/claude/for-each-kwarg-parity.md`.
- Logging: INFO `on_error=stop: stopping after {fn}@{combo} ({reason}); saved N completed`.

Tests:
- stop: 3rd of 5 combos raises → loop stops, 2 results saved, original exception type
  propagates with `.scifor_failure`, `__cause__`/traceback reaches the user frame.
- stop + NoDataError combos → run continues past them.
- continue: unchanged behaviour (existing `test_function_error_skips` still passes).
- parity test: scifor/scidb/scihist signatures all accept `on_error`; exporter emits it.

## Stage 3 — GUI backend consumes the record

- `api/run.py` `_progress_fn`: relay `failure` on `combo_skip`, collect summary
  `failures` per target, and include `failures: [{node_id, fn_name, ...group}]` in
  `run_done`. Pipeline run: same, from the step report.
- Replace `"see log above"` console line with: reason, `user_frame` file:line,
  and the first 3 combos for each group (the console works with no new UI).
- `on_error` is a per-node run option stored in `_node_config` (same path as
  `distribute`/`as_table`; see `docs/claude/gui-run-options-flow.md`), with a
  per-run override on the Run action. Both single-node and pipeline builds read it
  from that one owner.
- A stopped run gets verdict `failed` with `stopped: true` (not cancelled).

Tests (scistack-gui, pytest): run_done carries grouped failures for a raising fn;
pipeline run aggregates per step; `on_error` node config reaches scifor in both
single-node and pipeline paths (assert on the scifor INFO line, the existing diagnostic style).

## Stage 4 — Frontend + extension surfacing

1. **Node badge**: red badge on FunctionNode with failed-combo count (live from
   `combo_skip`, final from `run_done`). Cleared at the next run start of that node.
2. **Failures panel**: click the badge → popup grouped by reason: exc type, message,
   count, sample combos, example traceback with user frames expanded and
   framework/library frames collapsed.
3. **Click-to-source**: user frames post `openSource {file, line}` to the
   extension → `vscode.window.showTextDocument` at that line.
4. **Problems panel**: extension `DiagnosticCollection("scistack")` puts one
   Error diagnostic per group at `user_frame`
   (`"{fn}: {reason} — {count} combo(s), e.g. {combo}"`). Replaced on the next run
   of that function, and cleared on a successful run.
5. **Notification**: a failed run shows `vscode.window.showErrorMessage` with
   "Show failures" (opens panel) and "Open log" buttons.
6. **Run option UI**: "On error: continue / stop" toggle beside the other run options.

Frontend bundle must be rebuilt (memory "Frontend bundle rebuild trap").
Tests: vitest for badge-count reducer + failures grouping/collapse logic;
extension test for diagnostics build/replace/clear. Add §-entries to the GUI
manual testing doc.

## Stage 5 — MATLAB parity

- `+scifor/for_each.m` `record_iteration_failure` builds the same record as a struct
  from `MException.stack` (user frame = innermost frame outside the scimatlab
  `+scifor/+scidb/+scihist` dirs; one MATLAB owner function `failure_from_mexception.m`).
- `summary_fn` / run-completion markers carry `failures` in the same JSON shape
  (`to_dict` is the schema; a Python-side validator test checks MATLAB output against it).
- `on_error` supported in MATLAB for_each (`'stop'` → save completed, rethrow original).
- GUI consumes it unchanged (same wire shape).

## Docs

- New `docs/claude/error-surfacing.md`: the failure record, its owner, the wire
  shape, on_error semantics, consumers, and the diagnostic lines to grep.
- Update `docs/claude/scifor-for-each-internals.md` §Error handling and
  `scihist-for-each-internals.md` §Error handling.

## Open questions

1. `on_error="stop"` saving completed combos (proposed) vs. discarding them.
2. Should the Problems panel entries persist after the GUI closes? (Proposed: no,
   they're cleared with the session.)

## Commit plan

One commit per stage on main. The user runs pytest/vitest between stages (I don't).
